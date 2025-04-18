package workmgr

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"path"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/kvstore"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/rst/sync/internal/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var shouldReportIdleStatus bool = true
var shouldReportActiveStatus bool = true

var beeSyncReceivedWorkRequest = expvar.NewInt("beesync_received_work_requests")
var beeSyncQueuedWork = expvar.NewInt("beesync_queued_work")
var beeSyncActiveWork = expvar.NewInt("beesync_active_work")

type Config struct {
	WorkJournalPath string `mapstructure:"journal-db"`
	JobStorePath    string `mapstructure:"job-db"`
	// ActiveWorkQueueSize is the number of active work requests stored in the active work queue
	// (channel) and corresponding active work map. We don't just pull everything into these data
	// structures to ensure we can handles bursts of potentially millions of requests without
	// running out of memory. Because it is more efficient to update active work, it is beneficial
	// set this as high as possible, ideally so all work requests assigned to the node can fit
	// inside the ActiveWorkQueueSize.
	ActiveWorkQueueSize int `mapstructure:"active-work-queue-size"`
	NumWorkers          int `mapstructure:"num-workers"`
}

type Manager struct {
	ready   bool
	readyMu sync.RWMutex
	log     *zap.Logger
	// When shutting down workers must be shutdown first so the manager can handle their results and
	// store them in the database.
	workerCtx    context.Context
	workerCancel context.CancelFunc
	workerWG     *sync.WaitGroup
	// When shutting down the manager context should be cancelled last once it is safe to cleanup
	// all resources including closing DBs and connections.
	mgrCtx               context.Context
	mgrCancel            context.CancelFunc
	mgrWG                *sync.WaitGroup
	config               Config
	remoteStorageTargets *rst.ClientStore
	beeRemoteClient      *beeremote.Client
	// The workJournal has a work entry for each work request and its result. It uses an
	// automatically incremented submission ID as the key for each entry, allowing it to be used as
	// a journal to recreate the work queue(s) after a restart. This ensures requests are always
	// processed in stable chronological order (FIFO) based on when they were submitted, regardless
	// if the order we receive job IDs is not sequential or clocks change. Submission IDs are base36
	// encoded strings padded to a fixed width of 13 characters ensuring we can encode the max value
	// of an int64 in base36. Each entry stores one work request and its response. There may be
	// multiple entries for the same job if this worker node was assigned multiple work requests for
	// the same job.
	//
	// IMPORTANT: A workEntry will be locked the entire time a worker is handling the associated
	// work request. This prevents execution of the same request by multiple workers. As parts of
	// the request are completed, the worker will periodically update BadgerDB with the results
	// (without releasing the lock). If callers simply wish to query the progress of the request use
	// the read only GetEntry or GetEntries methods. If a caller wishes to cancel a request early,
	// first lock the activeWork map, check if the associated workEntry is already active and cancel
	// its context if so. If the workEntry is not yet active, only while the activeWork map is still
	// locked should the caller attempt to lock the journal entry. This ensures the workEntry
	// doesn't become active before the caller has a chance to remove it from the journal.
	//
	// Note we don't just set a flag in the work entry to coordinate access (which would allow us to
	// release the lock), because if the application crashes this would still be set when the
	// applications restarts which would cause problems. We also don't want to copy and release the
	// entry while its being processed because it would be better to block another goroutine than
	// risk two goroutines acting on the same entry concurrently. This should only ever happen if
	// there is a bug.
	workJournal *kvstore.MapStore[workEntry]
	// The jobStore keeps a mapping of job IDs to submission IDs in the journal for each of their
	// work requests. The inner map is a map of work request IDs to their submission ID in the
	// workJournal. This allows the worker node to handle multiple work request for a single job.
	// The jobID + request ID + submissionID make up the workIdentifier in the activeWork Map used
	// to coordinate access to the workJournal entries.
	//
	// IMPORTANT: Entries in the jobStore should not be locked for an extended period of time. The
	// intent is to allow external callers to quickly lookup the associated work entry in the
	// journal for a particular workIdentifier.
	jobStore *kvstore.MapStore[map[string]string]
	// When the manage loop sends work to workers via the activeWorkQueue, it also adds it to the
	// activeWork map with the workIdentifier as the key, allowing the state of in progress work to
	// be updated (i.e., cancelled) by cancelling the associated workContext. Lock activeWorkMu
	// before accessing the map.
	activeWork map[workIdentifier]workContext
	// Used to coordinate shared access to the activeWork map. DO NOT obtain a lock on a journal
	// entry then attempt to lock the activeWorkMu, this can cause deadlock. The correct approach is
	// to first lock the activeWorkMu then the journal entry. Only worker goroutines should lock
	// journal entries without first locking activeWorkMu.
	activeWorkMu sync.RWMutex
	// The manage loop picks up work from the workJournal and sends it to the worker goroutines
	// using the activeWorkQueue while simultaneously adding it to the activeWork map.
	activeWorkQueue chan workAssignment
}

func NewAndStart(log *zap.Logger, config Config, beeRemoteClient *beeremote.Client, mountPoint filesystem.Provider) (*Manager, error) {

	log = log.With(zap.String("component", path.Base(reflect.TypeOf(Manager{}).PkgPath())))
	workerCtx, workerCancel := context.WithCancel(context.Background())
	mgrCtx, mgrCancel := context.WithCancel(context.Background())

	m := &Manager{
		log:                  log,
		workerCtx:            workerCtx,
		workerCancel:         workerCancel,
		workerWG:             &sync.WaitGroup{},
		mgrCtx:               mgrCtx,
		mgrCancel:            mgrCancel,
		mgrWG:                &sync.WaitGroup{},
		config:               config,
		remoteStorageTargets: rst.NewClientStore(mountPoint),
		beeRemoteClient:      beeRemoteClient,
		activeWork:           make(map[workIdentifier]workContext),
		activeWorkMu:         sync.RWMutex{},
		activeWorkQueue:      make(chan workAssignment, config.ActiveWorkQueueSize),
	}

	// If anything goes wrong we want to execute all deferred functions
	// immediately to cleanup anything that did get initialized correctly. If we
	// startup normally then we'll handoff the deferred functions for manage to
	// call when shutting down.
	executeDefersImmediately := true
	deferredFuncs := []func() error{}
	defer func() {
		if executeDefersImmediately {
			// Deferred function calls should happen LIFO.
			for i := len(deferredFuncs) - 1; i >= 0; i-- {
				if err := deferredFuncs[i](); err != nil {
					m.log.Error("encountered an error aborting WorkMgr startup", zap.Error(err))
				}
			}
		}
	}()

	// Setup work journal:
	workJournalOpts := badger.DefaultOptions(m.config.WorkJournalPath)
	workJournalOpts = workJournalOpts.WithLogger(logger.NewBadgerLoggerBridge("workJournal", m.log))
	workJournal, closeWorkJournal, err := kvstore.NewMapStore[workEntry](workJournalOpts)
	if err != nil {
		return nil, fmt.Errorf("unable to setup work journal: %w", err)
	}
	deferredFuncs = append(deferredFuncs, closeWorkJournal)
	m.workJournal = workJournal

	// Setup job store:
	jobStoreOpts := badger.DefaultOptions(m.config.JobStorePath)
	jobStoreOpts = jobStoreOpts.WithLogger(logger.NewBadgerLoggerBridge("jobStore", m.log))
	jobStore, closeJobStore, err := kvstore.NewMapStore[map[string]string](jobStoreOpts)
	if err != nil {
		return nil, fmt.Errorf("unable to setup job store: %w", err)
	}
	deferredFuncs = append(deferredFuncs, closeJobStore)
	m.jobStore = jobStore

	// We successfully completed initialization so lets handoff the deferred
	// functions to the manage loop.
	executeDefersImmediately = false
	go m.manage(deferredFuncs)
	return m, nil
}

func (m *Manager) IsReady() bool {
	m.readyMu.RLock()
	defer m.readyMu.RUnlock()
	return m.ready
}

// UpdateConfig() applies some new configuration to the manager. This is the standard method to
// apply any configuration that needs to be set after the manager is initialized, notably the RST
// and BeeRemote client configuration are set automatically by BeeRemote through the gRPC server. It
// will check if the current configuration can be updated to the proposed configuration, and return
// an error if the configuration update is invalid/not allowed.
//
// IMPORTANT: Currently once the initial RST config is set, it cannot be dynamically updated without
// first restarting BeeSync.
//
// TODO: https://github.com/ThinkParQ/bee-remote/issues/29
// Allow RST configuration to be updated dynamically.
func (m *Manager) UpdateConfig(rstConfigs []*flex.RemoteStorageTarget, beeRemoteConfig *flex.BeeRemoteNode) error {

	err := m.remoteStorageTargets.UpdateConfig(m.mgrCtx, rstConfigs)
	if err != nil {
		return err
	}

	// If there are existing RSTs, verify the configuration did not change:
	err = m.beeRemoteClient.UpdateConfig(beeRemoteConfig)
	if err != nil {
		return err
	}

	m.readyMu.Lock()
	defer m.readyMu.Unlock()
	m.ready = true
	return nil
}

// Once manage is started it will not exit until the context is cancelled. If an error happens it
// will continue to retry the operation indefinitely.
func (m *Manager) manage(deferredFuncs []func() error) {

	m.mgrWG.Add(1)
	defer func() {
		// Deferred function calls should happen LIFO.
		for i := len(deferredFuncs) - 1; i >= 0; i-- {
			if err := deferredFuncs[i](); err != nil {
				m.log.Error("encountered an error aborting WorkMgr startup", zap.Error(err))
			}
		}
		m.mgrWG.Done()
	}()

	// completedWork is how workers signal when they are no longer working on a request. It may have
	// been completed successfully or cancelled, but either way it should be removed from the active
	// work map and new request(s) can be pulled to the active work queue and map.
	completedWork := make(chan workIdentifier, 4096)

	for i := 1; i <= m.config.NumWorkers; i++ {
		log := m.log.With(zap.String("goroutine", strconv.Itoa(i)))

		w := &worker{
			log:                  log,
			workQueue:            m.activeWorkQueue,
			completedWork:        completedWork,
			remoteStorageTargets: m.remoteStorageTargets,
			workJournal:          m.workJournal,
			jobStore:             m.jobStore,
			beeRemoteClient:      m.beeRemoteClient,
		}
		go w.run(m.workerCtx, m.workerWG)
	}

	m.log.Info("finished startup")
	var nextExpectedSubmissionID = ""
	var err error

	pollForWorkTicker := time.NewTicker(1500 * time.Millisecond)
	defer pollForWorkTicker.Stop()
	for {
		select {
		case <-m.mgrCtx.Done():
			return
		case <-pollForWorkTicker.C:
			// Add new work to the queue on a fixed interval. This is meant to allow us to add work
			// to the queue in batches, which should be more efficient than always only adding one
			// item to the queue. Provided a large enough queue workers should always be busy.
			nextExpectedSubmissionID, err = m.pullInNewWork(nextExpectedSubmissionID)
			if err != nil {
				// Keep trying if there was an error.
				continue
			}
		case completion := <-completedWork:
			m.activeWorkMu.Lock()
			delete(m.activeWork, completion)
			// Locking is expensive, complete/delete as many items as possible while holding the lock.
			// we hold the lock. While unless there are a large number of workers, it is unlikely
			// completedWork will constantly have items to complete. To be safe set an arbitrary max
			// number of items to delete in one go, to avoid holding on to the lock for too long
			// which in the extreme would cause workers to become idle if we can't pull in work.
		completeWork:
			for i := 0; i < 1024; i++ {
				select {
				case completion := <-completedWork:
					delete(m.activeWork, completion)
				default:
					break completeWork
				}
			}
			m.activeWorkMu.Unlock()
		}
	}
}

// pullInNewWork will startAtSubmissionID and add work from the work journal to the active
// work map and active work queue, up to the configured ActiveWorkQueueSize. It returns the ID of
// the last entry it was able to add + 1 (the ID of the next expected submission ID). If the journal
// is empty it always returns an empty string. If there are any errors it always returns an empty
// string. It is idempotent and will not re-add items to the work map or queue that already exist in
// the work map. This means it is safe to call it with a previously provided submission ID (or an
// empty string), for example if it returns an error and the caller isn't sure what entries were
// actually added to the active work queue.
func (m *Manager) pullInNewWork(startAtSubmissionID string) (string, error) {

	m.activeWorkMu.Lock()
	defer m.activeWorkMu.Unlock()

	nextItem, cleanupNext, err := m.workJournal.GetEntries(kvstore.WithStartingKey(startAtSubmissionID))
	if err != nil {
		return "", fmt.Errorf("unable to get work journal entries: %w", err)
	}
	defer cleanupNext()

	// Record the last valid submission ID so we can tell the caller what the
	// nextExpectedSubmissionID is.
	lastSubmissionID := ""

	// Completely fill up the active work queue then record the last submission ID.
	// Take into account if there are already entries in the work queue.
	for i := len(m.activeWorkQueue); i <= m.config.ActiveWorkQueueSize; i++ {
		entry, err := nextItem()
		if err != nil {
			return "", fmt.Errorf("unable to get work journal entry: %w", err)
		}
		if entry == nil {
			break // No more entries are left in the work journal.
		}

		workIdentifier := workIdentifier{
			submissionID:  entry.Key,
			jobID:         entry.Entry.Value.WorkRequest.JobId,
			workRequestID: entry.Entry.Value.WorkRequest.RequestId,
		}

		_, ok := m.activeWork[workIdentifier]
		if ok {
			// Check based on the work identifier if there is already an existing workContext in the
			// activeWork map. If so skip adding it to the map or queue again as we could block a worker
			// when it tries to lock the journal entry if another worker is already handling the WR.
			continue
		}

		workCtx, workCtxCancel := context.WithCancel(m.workerCtx)
		activeWork := workAssignment{
			ctx:            workCtx,
			workIdentifier: workIdentifier,
		}

		m.activeWork[activeWork.workIdentifier] = workContext{
			ctx:    workCtx,
			cancel: workCtxCancel,
		}
		m.activeWorkQueue <- activeWork
		lastSubmissionID = entry.Key
		shouldReportIdleStatus = true
		if shouldReportActiveStatus {
			m.log.Info("worker node is no longer idle")
			shouldReportActiveStatus = false
		}
		beeSyncQueuedWork.Add(1)
	}

	nextExpectedSubmissionID := ""
	if lastSubmissionID != "" {
		lastSubmissionIDInt64, err := strconv.ParseInt(lastSubmissionID, 10, 64)
		if err != nil {
			// This is likely only to happen if somehow we end up with unexpected entries in the DB.
			// For example if someone manually made an entry, or there is a bug somewhere else with
			// how DB keys are generated.
			return "", fmt.Errorf("unable to cast last submission ID to an integer '%s': %w", lastSubmissionID, err)
		}
		lastSubmissionIDInt64++
		// Submission IDs are base36 encoded strings padded to a fixed width of 13 characters
		// ensuring we can encode the max value of an int64 in base36.
		nextExpectedSubmissionID = fmt.Sprintf("%013s", strconv.FormatInt(int64(lastSubmissionIDInt64), 36))
	} else if shouldReportIdleStatus && len(m.activeWork) == 0 {
		m.log.Info("worker node is idle")
		shouldReportIdleStatus = false
		shouldReportActiveStatus = true
	}

	return nextExpectedSubmissionID, nil
}

func (m *Manager) SubmitWorkRequest(wr *flex.WorkRequest) (*flex.Work, error) {
	if !m.IsReady() {
		return nil, status.Errorf(codes.FailedPrecondition, "%s", ErrNotReady)
	}

	// First create or get the existing entry for this job and lock it to prevent anyone else from
	// adding an entry for this WR. We also ensure there isn't an existing entry for the WR.
	_, job, commitAndReleaseJob, err := m.jobStore.CreateAndLockEntry(
		wr.GetJobId(),
		kvstore.WithAllowExisting(true),
		kvstore.WithValue(make(map[string]string)),
	)
	if err != nil && !errors.Is(err, kvstore.ErrEntryAlreadyExistsInDB) {
		return nil, fmt.Errorf("unable to create new entry or get existing entry for job ID %s: %w", wr.GetJobId(), err)
	}

	if _, ok := job.Value[wr.GetRequestId()]; ok {
		if err := commitAndReleaseJob(); err != nil {
			m.log.Error("unable to release job entry", zap.Error(err), zap.Any("jobID", wr.GetJobId()))
		}
		return nil, status.Errorf(codes.AlreadyExists, "%s", fmt.Errorf("already handling work request ID %s for job ID %s (refusing to create duplicate entries)", wr.GetRequestId(), wr.GetJobId()))
	}

	defer func() {
		if err := commitAndReleaseJob(); err != nil {
			m.log.Error("unable to release job entry", zap.Error(err), zap.Any("jobID", wr.GetJobId()))
		}
	}()

	// Have the MapStore auto generate the submission ID based. This will be a base 36 encoded
	// string padded to a fixed width of 13 characters. Generally we should not need to worry about
	// manually generating IDs, except for pullInNewWork() which needs to increment the ID.
	workEntryKey, workEntry, commitAndReleaseWork, err := m.workJournal.CreateAndLockEntry("")
	if err != nil {
		return nil, fmt.Errorf("unable to create work journal entry for job ID %s work request ID %s: %w", wr.GetJobId(), wr.GetRequestId(), err)
	}
	defer func() {
		if err := commitAndReleaseWork(); err != nil {
			m.log.Error("unable to release work journal entry", zap.Error(err), zap.Any("jobID", wr.GetJobId()))
		}
	}()

	workEntry.Value.WorkRequest = &workRequest{
		WorkRequest: wr,
	}

	workResult := newWorkFromRequest(workEntry.Value.WorkRequest)

	workEntry.Value.WorkResult = workResult
	job.Value[wr.GetRequestId()] = workEntryKey

	beeSyncReceivedWorkRequest.Add(1)
	return workResult.Work, nil
}

// UpdateWork will attempt to modify the state of the specified work request if it exists on this
// node. If the request does not exist on this node (i.e., it was already cancelled), it will return
// nil, and an error with the gRPC status codes.NotFound. If the request cannot be updated
// (typically because it was already completed), it will return a response with the latest actual
// state and an error (note this is unlikely unless the worker was blocked from sending the response
// to BeeRemote). When UpdateWork() returns a valid work result (even if it also returns an error),
// no other response will be sent to BeeRemote so the caller must handle updating the request in
// BeeRemote as needed (i.e., ownership of the request was handed off to UpdateWork and worker
// goroutines will do nothing else with it). If it only returns an error, this generally indicates
// some transient issue (for example no disk space) and the update request should be retried later.
//
// Note this operates synchronously instead of taking an asynchronous approach. There are two big
// reasons for this: (1) if the request is in active work, it may take awhile for a worker to pickup
// the request, see it was cancelled, update it, and return a response. (2) it adds overhead for
// BeeRemote to sit and poll waiting to verify the request is updated.
//
// IMPORTANT: This is not well suited for "bulk" updates and should not (for example) be used to
// cancel all work requests on this node.
func (m *Manager) UpdateWork(update *flex.UpdateWorkRequest) (*flex.Work, error) {
	if !m.IsReady() {
		return nil, status.Errorf(codes.FailedPrecondition, "%s", ErrNotReady)
	}

	if update.GetNewState() != flex.UpdateWorkRequest_CANCELLED {
		return nil, fmt.Errorf("refusing to update work request to an unsupported state (job ID %s, request ID %s, new state: %s)", update.GetJobId(), update.GetRequestId(), update.GetNewState())
	}
	// Right now the only state change we support is cancelling the request.

	jobEntry, releaseJob, err := m.jobStore.GetAndLockEntry(update.GetJobId())
	if err != nil {
		if errors.Is(err, kvstore.ErrEntryNotInDB) {
			m.log.Debug("no work requests for the specified job ID found on this worker node", zap.Any("jobID", update.GetJobId()), zap.Any("requestID", update.GetRequestId()))
			return nil, status.Errorf(codes.NotFound, "work request %s for job ID %s was not found on this worker node", update.GetRequestId(), update.GetJobId())
		}
		return nil, status.Errorf(codes.Internal, "error looking up the specified job ID %s: %s", update.GetJobId(), err.Error())
	}

	// By default we automatically release the job using a defer. However if the update request is
	// successful we may want to bypass the defer so we can delete it instead.
	releaseJobWithDefer := true
	defer func() {
		if releaseJobWithDefer {
			err := releaseJob()
			if err != nil {
				m.log.Warn("error releasing job entry", zap.Error(err), zap.Any("jobID", update.GetJobId()))
			}
		}

	}()

	submissionID, ok := jobEntry.Value[update.GetRequestId()]
	if !ok {
		m.log.Debug("worker node has request(s) for the specified job ID, but the specific request was not found", zap.Any("jobID", update.GetJobId()), zap.Any("requestID", update.GetRequestId()))
		return nil, nil
	}

	workIdentifier := workIdentifier{
		submissionID:  submissionID,
		jobID:         update.GetJobId(),
		workRequestID: update.GetRequestId(),
	}

	m.activeWorkMu.Lock()
	activeWork, ok := m.activeWork[workIdentifier]
	if ok {
		// Request is already in the queue. We need to cancel it but don't delete it from the active
		// work map. If it was already picked up (or when it gets picked up) the worker goroutine
		// will tell manage() it was completed which handles removing it from the map (technically
		// nothing would break if we did it here though).
		activeWork.cancel()
		// Since the request was already active it is safe to unlock the mutex. If the request is
		// already being processed by a worker it may take some time to cancel and obtain the lock.
		m.activeWorkMu.Unlock()
	}

	// At this point we wait indefinitely to get the lock on the journal entry. If we cancelled the
	// work context we would end up with an "orphaned" entry that never has a response sent back to
	// BeeRemote because workers just stop processing requests when the context is cancelled and
	// don't send a response. Since ownership of work requests is always with either BeeRemote or
	// BeeSync, BeeRemote will never try to check on long-running requests BeeSync already accepted.
	// If for some reason the application was killed at this point after a restart this work request
	// would get picked back up, but otherwise there is no mechanism to check for orphaned requests.
	workEntry, releaseEntry, err := m.workJournal.GetAndLockEntry(submissionID)
	if !ok {
		// Documenting the rationale for how we handle locking here:
		//
		// If the request was not yet active then we have to leave mutex locked until we are able to
		// get a lock on the entry. Otherwise the manage() loop might race with us and add it to
		// active work and a worker could pick it up before we get a chance to lock it. Workers
		// leave the journal entries locked the entire time they are being processed, so we could be
		// blocked here for a very very long time.
		//
		// Even after we lock the journal entry it may still get added to active work because
		// pullInNewWork uses the RO GetEntries. However the worker would see the journal entry no
		// longer exists and just move on. This is unlikely enough it is not worth adding extra
		// complexity to prevent it from ever happening.
		//
		// There is the risk of deadlock if something else locked the journal entry and is also
		// trying to lock active work. We don't do this anywhere today, but is something that could
		// be overlooked in the future. This risk has been clearly documented on activeWorkMu to
		// minimize the risk of this happening.
		//
		// So the only real downside is that we have to block manage() from pulling in new work
		// until a (potentially expensive) I/O operation completes. Blocking a mutex on IO is not
		// great. However realistically this should never be an issue provided a large enough
		// ActiveWorkQueueSize it is unlikely we'd ever block long enough the queue would become
		// empty and there were idle workers which is where we'd see an actual performance impact.
		m.activeWorkMu.Unlock()
	}
	if err != nil {
		return nil, fmt.Errorf("error getting journal entry while updating work request: %w", err)
	}

	// Make a copy of the workResult because we will cleanup the DB entries before returning a
	// workResult. Don't return a workResult if there are any issues cleaning up.
	workResult := proto.Clone(workEntry.Value.WorkResult).(*flex.Work)

	var jobReleaseErr error
	// If this is the last/only work request for this job, delete its entry.
	if len(jobEntry.Value) == 1 {
		releaseJobWithDefer = false
		jobReleaseErr = releaseJob(kvstore.WithDeleteEntry(true))
	} else {
		delete(jobEntry.Value, update.GetRequestId())
	}

	var journalReleaseErr error
	if jobReleaseErr != nil {
		// If there was an error releasing the job entry, don't cleanup the journal entry. We may
		// get a chance to try again later.
		journalReleaseErr = releaseEntry()
	} else {
		journalReleaseErr = releaseEntry(kvstore.WithDeleteEntry(true))
	}

	if jobReleaseErr != nil || journalReleaseErr != nil {
		return nil, fmt.Errorf("error releasing job entry: %w, error releasing journal entry: %w", jobReleaseErr, journalReleaseErr)
	}

	// Update the response to indicate the job was cancelled. Ensure the message includes meaningful
	// details from the previous state if relevant.
	switch workResult.GetStatus().GetState() {
	case flex.Work_CREATED:
		// This shouldn't happen unless there is a bug. However we still want to allow artifacts to
		// be cleaned up if needed.
		workResult.GetStatus().SetState(flex.Work_CANCELLED)
		workResult.GetStatus().SetMessage("cancelled created work request (warning: nodes should not have work requests in an created state, likely there is a bug somewhere)")
	case flex.Work_SCHEDULED:
		workResult.GetStatus().SetState(flex.Work_CANCELLED)
		workResult.GetStatus().SetMessage("cancelled scheduled work request")
	case flex.Work_CANCELLED:
		// If a worker was already handling this work request the state should be cancelled.
	case flex.Work_FAILED:
		workResult.GetStatus().SetState(flex.Work_CANCELLED)
		workResult.GetStatus().SetMessage(workResult.GetStatus().GetMessage() + "; cancelled failed work request")
	case flex.Work_ERROR:
		workResult.GetStatus().SetState(flex.Work_CANCELLED)
		workResult.GetStatus().SetMessage(workResult.GetStatus().GetMessage() + "; cancelled work request that had an error")
	default:
		// This could happen if the request was already completed, or is in a state we don't yet
		// support. Don't change the state to force it to be added here if needed. Note responses
		// for completed requests are always sent to BeeRemote, even if the context was cancelled.
		workResult.GetStatus().SetMessage(workResult.GetStatus().GetMessage() + "; unable to cancel work request in the current state")
		return workResult, fmt.Errorf("unable to cancel work request in the current state: %s", workResult.GetStatus().GetState())
	}

	return workResult, nil
}

func (m *Manager) Stop() {
	m.log.Info("stopping workers")
	m.workerCancel()
	// Wait until all workers are stopped before shutting down the manager which will automatically
	// cleanup all shared resources (DB, RST/BeeRemote clients, etc).
	m.workerWG.Wait()
	m.log.Info("stopped all workers, attempting stop manager")
	m.mgrCancel()
	m.mgrWG.Wait()
	m.log.Info("stopped manager")
}
