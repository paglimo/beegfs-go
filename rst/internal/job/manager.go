package job

import (
	"context"
	"encoding/gob"
	"fmt"
	"path"
	"reflect"
	"sort"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/thinkparq/bee-remote/internal/worker"
	"github.com/thinkparq/bee-remote/internal/workermgr"
	"github.com/thinkparq/gobee/kvstore"
	"github.com/thinkparq/gobee/logger"
	"github.com/thinkparq/gobee/rst"
	"github.com/thinkparq/gobee/types"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Register custom types for serialization/deserialization via Gob when the
// package is initialized.
func init() {
	gob.Register(&Job{})
	gob.Register(&Segment{})
}

const (
	// This many jobs for each RST configured on a particular path is guaranteed
	// to be retained. At minimum this should be set to 1 so we always know the
	// last sync result for a particular RST.
	// TODO: Make this user configurable.
	minJobEntriesPerRST = 2
	// Once this threshold is exceeded older jobs will be deleted
	// (oldest-to-newest) until the number of jobs equals the
	// minJobEntriesPerRST The oldest job is determined by the lowest job ID.
	// TODO: Make this user configurable.
	maxJobEntriesPerRST = 4
)

type Config struct {
	PathDBPath         string `mapstructure:"pathDBPath"`
	PathDBCacheSize    int    `mapstructure:"pathDBCacheSize"`
	ResultsDBPath      string `mapstructure:"resultsDBPath"`
	ResultsDBCacheSize int    `mapstructure:"resultsDBCacheSize"`
	RequestQueueDepth  int    `mapstructure:"requestQueueDepth"`
	JournalPath        string `mapstructure:"journalPath"`
}

type Manager struct {
	log    *zap.Logger
	ctx    context.Context
	cancel context.CancelFunc
	config Config
	// Ready indicates the Manage() loop has been started and finished setting
	// up all MapStores and their backing databases. Methods besides the
	// Manage() loop must check the ready state before interacting with the
	// MapStores. Before the Manage() loop terminates and closes connections to
	// the databases it will update the ready state to ensure databases aren't
	// closed out from under other methods.
	ready bool
	// readyMu is used to coordinate updating the ready state. Manage() will
	// take a write lock before changing the ready state. Other methods should
	// take a read lock before checking the ready state.
	readyMu sync.RWMutex
	// JobRequests is where external callers should submit requests that can be
	// handled asynchronously by JobMgr. It is best suited for submitting jobs
	// when no user is waiting on a response (i.e., in response to FS events).
	// For interactive use cases (i.e., beegfs-ctl) the SubmitJobRequest method
	// can be used directly to immediately create a job and return a response.
	JobRequests chan<- *beeremote.JobRequest
	// jobRequests is where goroutines manged by JobMgr listen for requests.
	jobRequests <-chan *beeremote.JobRequest
	// JobUpdates is where external callers should submit requests to update existing jobs.
	JobUpdates chan<- *beeremote.UpdateJobRequest
	//jobUpdates is where goroutines manged by JobMgr listen for requests.
	jobUpdates <-chan *beeremote.UpdateJobRequest
	// WorkResponses is where work request updates from worker nodes should be sent.
	// These updates are sent to JobMgr via the gRPC server.
	WorkResponses chan<- *flex.WorkResponse
	// workResponses is where goroutines managed by JobMgr listen for work responses.
	workResponses <-chan *flex.WorkResponse
	// pathStore is the store where entries for file system paths with jobs
	// are kept. This store keeps a mapping of paths to Job(s). Note the inner
	// map is a map of Job IDs to jobs (not RST IDs to jobs) so we can retain
	// a configurable number of historical jobs for each path+RST combo.
	pathStore      *kvstore.MapStore[map[string]*Job]
	jobIDGenerator *badger.Sequence
	// A pointer to an initialized/started worker manager.
	workerManager *workermgr.Manager
}

// NewManager initializes and returns a new Job manager and channels used to submit and update job requests.
func NewManager(log *zap.Logger, config Config, workerManager *workermgr.Manager) *Manager {
	log = log.With(zap.String("component", path.Base(reflect.TypeOf(Manager{}).PkgPath())))
	ctx, cancel := context.WithCancel(context.Background())
	jobRequestChan := make(chan *beeremote.JobRequest, config.RequestQueueDepth)
	jobUpdatesChan := make(chan *beeremote.UpdateJobRequest, config.RequestQueueDepth)
	workResponsesChan := make(chan *flex.WorkResponse, config.RequestQueueDepth)
	return &Manager{
		log:           log,
		ctx:           ctx,
		cancel:        cancel,
		config:        config,
		ready:         false,
		workerManager: workerManager,
		JobRequests:   jobRequestChan,
		jobRequests:   jobRequestChan,
		JobUpdates:    jobUpdatesChan,
		jobUpdates:    jobUpdatesChan,
		WorkResponses: workResponsesChan,
		workResponses: workResponsesChan,
	}
}

// Start handles initializing all databases and starting a goroutine that
// handles job requests. It returns an error if there were any issues on setup,
// otherwise it returns nil to indicate the manager is ready to accept requests.
// Additional calls to manage while the Manager is already started will return
// an error. Use Stop() to shutdown a running manager.
func (m *Manager) Start() error {

	m.readyMu.Lock()
	if m.ready {
		return fmt.Errorf("an instance of job manager is already running")
	}

	// If anything goes wrong we want to execute all deferred functions
	// immediately to cleanup anything that did get initialized correctly. If we
	// startup normally then we don't want to execute deferred functions until
	// we're shutting down.
	executeDefersImmediately := true
	deferredFuncs := []func() error{}
	defer func() {
		if executeDefersImmediately {
			// Deferred function calls should happen LIFO.
			for i := len(deferredFuncs) - 1; i >= 0; i-- {
				if err := deferredFuncs[i](); err != nil {
					m.log.Error("encountered an error aborting JobMgr startup", zap.Error(err))
				}
			}
		}
	}()

	// We initialize databases in Manage() so we can ensure the DBs are closed properly when shutting down.
	pathDBOpts := badger.DefaultOptions(m.config.PathDBPath)
	pathDBOpts = pathDBOpts.WithLogger(logger.NewBadgerLoggerBridge("pathDB", m.log))
	pathDBOpts = pathDBOpts.WithLoggingLevel(badger.INFO)
	pathStore, closePathDB, err := kvstore.NewMapStore[map[string]*Job](pathDBOpts, m.config.PathDBCacheSize)
	if err != nil {
		return fmt.Errorf("unable to setup paths DB: %w", err)
	}
	deferredFuncs = append(deferredFuncs, closePathDB)
	m.pathStore = pathStore

	// TODO: Create a journal that also can be used to generate monotonically
	// increasing integers we can use as job IDs. The journal should also be
	// used to generate sequential job IDs.
	// Ref: https://dgraph.io/docs/badger/get-started/#monotonically-increasing-integers
	jobJournalDBOpts := badger.DefaultOptions(m.config.JournalPath)
	jobJournalDBOpts = jobJournalDBOpts.WithLogger(logger.NewBadgerLoggerBridge("journalDB", m.log))
	jobJournalDBOpts = jobJournalDBOpts.WithLoggingLevel(badger.INFO)
	jobJournal, err := badger.Open(jobJournalDBOpts)
	if err != nil {
		return err
	}

	deferredFuncs = append(deferredFuncs, func() error { return jobJournal.Close() })

	// TODO: Evaluate if a bandwidth of 1000 is appropriate.
	// Possibly this should be user configurable.
	jobIDGenerator, err := jobJournal.GetSequence([]byte("jobIDs"), 1000)
	if err != nil {
		return err
	}
	// Defers are LIFO which means this is called before closing the DB.
	deferredFuncs = append(deferredFuncs, func() error { return jobIDGenerator.Release() })
	m.jobIDGenerator = jobIDGenerator

	m.ready = true
	m.readyMu.Unlock()
	executeDefersImmediately = false

	// Start a separate goroutine that will handle closing the databases when
	// the Manager shuts down.
	go func() {
		defer func() {
			// Deferred function calls should happen LIFO.
			for i := len(deferredFuncs) - 1; i >= 0; i-- {
				if err := deferredFuncs[i](); err != nil {
					m.log.Error("encountered an error shutting down JobMgr", zap.Error(err))
				}
			}
		}()

		m.log.Info("now accepting job requests and work responses")
		// TODO: https://github.com/ThinkParQ/bee-remote/issues/9. Use a pool of
		// goroutines to process job requests and work responses.
		for {
			select {
			case <-m.ctx.Done():
				m.log.Info("shutting down because the app is shutting down")
				m.readyMu.Lock()
				m.ready = false
				m.readyMu.Unlock()
				return
			case jobRequest := <-m.jobRequests:
				response, err := m.SubmitJobRequest(jobRequest)
				if err != nil {
					m.log.Error("error submitting job request", zap.Error(err), zap.Any("jobRequest", jobRequest))
				} else {
					m.log.Debug("submitted job request", zap.Any("response", response))
				}
			case jobUpdate := <-m.jobUpdates:
				response, err := m.UpdateJob(jobUpdate)
				if err != nil {
					m.log.Error("error updating job request", zap.Error(err), zap.Any("jobUpdate", jobUpdate))
				} else {
					m.log.Debug("updated job request", zap.Any("response", response))
				}
			case workResponse := <-m.workResponses:
				err := m.updateJobResults(workResponse)
				// TODO: Once we are using a journal to keep track of job
				// results, it needs to be updated depending if the update was
				// successful or not.
				if err != nil {
					m.log.Error("error updating job results", zap.Error(err), zap.Any("workResponse", workResponse))
				} else {
					m.log.Debug("processed work response", zap.Any("workResponse", workResponse))
				}
			}
		}
	}()
	return nil
}

func (m *Manager) GetJobs(request *beeremote.GetJobsRequest) (*beeremote.GetJobsResponse, error) {

	m.readyMu.RLock()
	defer m.readyMu.RUnlock()
	if !m.ready {
		return nil, fmt.Errorf("unable to get jobs (JobMgr is not ready)")
	}

	getJobResponse := func(job *Job) *beeremote.JobResponse {
		workRequests := make([]*flex.WorkRequest, 0)
		workResults := make([]*beeremote.JobResponse_WorkResult, 0)
		if request.GetIncludeWorkRequests() {
			workRequests = rst.RecreateWorkRequests(job.Get(), job.GetSegments())
		}

		if request.GetIncludeWorkResults() {
			for reqID, wr := range job.WorkResults {
				workResult := &beeremote.JobResponse_WorkResult{
					RequestId:    reqID,
					Status:       wr.Status(),
					AssignedNode: wr.AssignedNode,
					AssignedPool: string(wr.AssignedPool),
				}
				workResults = append(workResults, workResult)
			}
		}
		return &beeremote.JobResponse{
			Job:          job.Get(),
			WorkRequests: workRequests,
			WorkResults:  workResults,
		}
	}

	switch query := request.Query.(type) {
	case *beeremote.GetJobsRequest_ByJobIdAndPath:
		if query.ByJobIdAndPath.JobID == "" || query.ByJobIdAndPath.Path == "" {
			return nil, fmt.Errorf("to query by job ID and path, both must be specified")
		}

		pathEntry, err := m.pathStore.GetEntry(query.ByJobIdAndPath.Path)
		if err != nil {
			return nil, err
		}

		job, ok := pathEntry.Value[query.ByJobIdAndPath.JobID]
		if !ok {
			return nil, fmt.Errorf("job ID %s does not exist for path %s", query.ByJobIdAndPath.JobID, query.ByJobIdAndPath.Path)
		}

		return &beeremote.GetJobsResponse{
			Response: []*beeremote.JobResponse{
				getJobResponse(job),
			},
		}, nil

	case *beeremote.GetJobsRequest_ByExactPath:
		pathEntry, err := m.pathStore.GetEntry(query.ByExactPath)
		if err != nil {
			return nil, err
		}

		jobResponses := []*beeremote.JobResponse{}
		for _, job := range pathEntry.Value {
			jobResponses = append(jobResponses, getJobResponse(job))
		}
		return &beeremote.GetJobsResponse{
			Response: jobResponses,
		}, nil

	case *beeremote.GetJobsRequest_ByPathPrefix:
		// For a path prefix first get all paths that match the prefix. Then get all the jobs for
		// each path. If someone wanted to get all jobs they could provide a prefix of "/".

		pathItems := make([]*kvstore.BadgerItem[map[string]*Job], 0)
		nextEntry, cleanupEntries, err := m.pathStore.GetEntries(kvstore.WithKeyPrefix(query.ByPathPrefix))
		if err != nil {
			return nil, err
		}
		defer cleanupEntries()

		// TODO: https://github.com/ThinkParQ/bee-remote/issues/13 - If there were enough entries in
		// the database that matched the path prefix we could run out of memory.
		for {
			entry, err := nextEntry()
			if err != nil {
				return nil, err
			}
			if entry == nil {
				break
			}
			pathItems = append(pathItems, entry)
		}

		jobResponses := []*beeremote.JobResponse{}
		for _, pathItem := range pathItems {
			for _, job := range pathItem.Entry.Value {
				jobResponses = append(jobResponses, getJobResponse(job))
			}
		}
		return &beeremote.GetJobsResponse{
			Response: jobResponses,
		}, nil
	}

	return nil, fmt.Errorf("no query parameters provided (to get all jobs use '/' with the prefix path query type)")
}

// Takes a single job request and attempts to create and schedule a job for it.
// Returns an error if the job could not be created. There are two main reasons
// we may not be able to create a job:
//
// (1) The provided request is invalid. For example the job type doesn't support
// the specified RST or the RST doesn't exist.
//
// (2) Some temporary condition makes the job request currently impossible. For
// example there is already an active job for the RST/path combo or there was an
// internal error retrieving or saving DB entries.
//
// Once the job is created if there were any issues allocating, scheduling or
// running the job these will be indicated in the job response or by inspecting
// the job status later on. If there was a temporary issue such as a network
// error trying to contact the RST, the job will still be created but the status
// will indicate the error.
func (m *Manager) SubmitJobRequest(jr *beeremote.JobRequest) (*beeremote.JobResponse, error) {

	m.readyMu.RLock()
	defer m.readyMu.RUnlock()
	if !m.ready {
		return nil, fmt.Errorf("unable to get jobs (JobMgr is not ready)")
	}

	job, err := New(m.jobIDGenerator, jr)
	if err != nil {
		return nil, fmt.Errorf("unable to generate job from job request: %w", err)
	}

	// Initialize reference types:
	job.WorkResults = make(map[string]worker.WorkResult)

	// TODO: Consider adding a CreateOrGetEntry method to the MapStore to
	// simplify/optimize this. Or adding a flag to the existing method that will
	// get the entry if it already exists.
	pathEntry, commitAndReleasePath, err := m.pathStore.CreateAndLockEntry(job.Request.GetPath())
	if err == kvstore.ErrEntryAlreadyExistsInDB || err == kvstore.ErrEntryAlreadyExistsInCache {
		pathEntry, commitAndReleasePath, err = m.pathStore.GetAndLockEntry(job.Request.GetPath())
		if err != nil {
			return nil, fmt.Errorf("unable to get existing entry for path %s while creating job ID %s: %w", job.Request.GetPath(), job.GetId(), err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("unable to create new entry for path %s while creating jobID %s: %w", job.Request.GetPath(), job.GetId(), err)
	} else {
		// If we actually create the entry then we must initialize the value field.
		pathEntry.Value = make(map[string]*Job)
	}
	defer func() {
		if err := commitAndReleasePath(); err != nil {
			m.log.Error("unable to release path entry", zap.Error(err))
		}
	}()

	if len(pathEntry.Value) != 0 {

		// Set true if we can't start a new job due to a conflict.
		jobConflict := false
		// Add jobs in a terminal state to a map based on their RST ID.
		// We limit the number of historical jobs kept for each RST.
		terminalJobsByRST := make(map[string][]*Job, 0)

		for _, existingJob := range pathEntry.Value {
			if existingJob.Request.GetRemoteStorageTarget() == job.Request.GetRemoteStorageTarget() && !existingJob.InTerminalState() {
				// We found an active job for this RST. We shouldn't try to start a new job but we also shouldn't clean up the active job.
				jobConflict = true
			} else if existingJob.InTerminalState() {
				// Found a job in a terminal state, add it to the list to check if it should be cleaned up:
				terminalJobsByRST[existingJob.Request.GetRemoteStorageTarget()] = append(terminalJobsByRST[existingJob.Request.GetRemoteStorageTarget()], existingJob)
			}
			// Otherwise we found a job for a different RST not in a terminal state. Don't touch it.
		}

		for _, jobsForRST := range terminalJobsByRST {
			// It would be inefficient if we only cleaned up one entry at a
			// time so we set a max number of entries before we start GC.
			if len(jobsForRST) > maxJobEntriesPerRST {
				// Sort the slice so we can delete old jobs based on the lowest
				// job ID until we reach minJobEntriesPerRST.
				sort.Slice(jobsForRST, func(i, j int) bool { return jobsForRST[i].GetId() < jobsForRST[j].GetId() })
				for _, gcJob := range jobsForRST[:minJobEntriesPerRST+1] {
					delete(pathEntry.Value, gcJob.GetId())
				}
			}
		}

		if jobConflict {
			// Initially we would add a cancelled job entry even if there was a
			// conflict. The goal was to avoid potentially spamming the logs
			// with rejected jobs. But typically users will submit jobs
			// interactively giving us a chance to immediately return an error.
			// And generally jobs submitted non-interactively are generated
			// based on events. And our event handling functionality should know
			// how to never create a job when there is already one running.
			//
			// A benefit of this approach is that we don't end up with a job
			// with a higher ID that would be retained longer than the active
			// job. We want the job with the highest ID for a particular RST
			// to always be the latest job result for that path+RST combo.
			return nil, fmt.Errorf("rejecting job request because the specified path entry %s already has an active job for RST %s (cancel or wait for it to complete first)", job.Request.GetPath(), job.Request.GetRemoteStorageTarget())
		}

	}

	rstClient, ok := m.workerManager.RemoteStorageTargets[job.Request.GetRemoteStorageTarget()]
	if !ok {
		return nil, fmt.Errorf("rejecting job because the requested RST does not exist: %s", job.Request.GetRemoteStorageTarget())
	}

	jobSubmission, retry, err := job.GenerateSubmission(rstClient)
	if err != nil && !retry {
		return nil, fmt.Errorf("unable to create job due to an unrecoverable error generating the submission: %w", err)
	} else if err != nil {
		// If an temporary error occurred that can be retried still go forward
		// with creating the job and just leave the results empty.
		job.GetStatus().State = flex.RequestStatus_ERROR
		job.GetStatus().Message = err.Error()
		pathEntry.Value[job.GetId()] = job
		// TODO: Push to a queue of jobs that need to be periodically retried by
		// a separate go routine. Note error is not considered a terminal state
		// so subsequent job requests for the same path/RST will be rejected
		// until this one is cancelled. This handler will need to periodically
		// retry GenerateSubmission() until there is no error then submit the job
		// submission to worker manager.
		return &beeremote.JobResponse{
			Job:          job.Get(),
			WorkRequests: rst.RecreateWorkRequests(job.Get(), job.GetSegments()),
			WorkResults:  getWorkResultsForResponse(job.WorkResults),
		}, nil
	}

	// At this point we have a properly formed job request that should be
	// runnable barring any ephemeral issues. Errors from this point onwards
	// should be indicated by the job status.
	pathEntry.Value[job.GetId()] = job
	job.WorkResults, job.Status = m.workerManager.SubmitJob(jobSubmission)

	// TODO: A crash here could mean there are WRs scheduled to worker nodes but no record of the
	// job or which nodes they were assigned to because the updated pathEntry hasn't been committed
	// to the DB. If we had a journal mechanism we could reply the journal and broadcast to all
	// workers they should cancel those jobs to cleanup any remnants and retry the job creation.
	// However if we don't have a journal, is there a more simpler way we can ensure this never
	// happens? Or is it so unlikely its not worth trying to solve?

	m.log.Debug("created job", zap.Any("job", job))

	return &beeremote.JobResponse{
		Job:          job.Get(),
		WorkRequests: rst.RecreateWorkRequests(job.Get(), job.GetSegments()),
		WorkResults:  getWorkResultsForResponse(job.WorkResults),
	}, nil
}

// UpdateJob will attempt to update the specified job(s) and any associated work requests. If a path
// is specified then all jobs running for that path will be updated. If a path and job ID is
// specified, then only that job is updated. UpdateJob is idempotent, so it can be called multiple
// times to verify a job is updated, for example if there was an issue cancelling some of a job's
// work requests.
//
// An error is only returned if an internal error (such as a DB connection issue) occurs updating
// any of the specified job(s). Otherwise errors updating the state of one or more jobs (for example
// moving a job to an invalid state or being unable to cancel work requests on a worker node) will
// result in the UpdateJobResponse being !ok and details are captured by the status message for a
// particular job. Individual responses can be reviewed immediately by examining the
// UpdateJobResponse, or later by querying the database.
//
// Note if an internal error occurs updating a job, that job will not be included in the
// UpdateJobsResponse. Because multiple jobs can be updated at once it is possible for UpdateJob to
// return an UpdateJobResponse and an error. At this time internal errors are not recorded on the
// status of the job or saved in the database as this would likely lead to inconsistent behavior
// considering most internal errors are likely database issues that would cause the status not to be
// actually updated. As a result while the actual status of the job may be indeterminate, the status
// recorded in the database should reflect the previous state, ensuring UpdateJob can be called
// multiple times once any internal issues are resolved. Generally internal errors should just be
// logged or returned to the user when handling interactive requests, and the typical recovery path
// is to retry.
//
// When deleteCompletedJobs != true, when updating jobs by path, completed jobs are silently
// skipped. When updating an individual job the response will be !ok and the response message will
// indicate the delete completed jobs was not set (the job message will not be changed, consistent
// with how it is handled for paths).
//
// Allowed state changes:
//
// UNASSIGNED/SCHEDULED/RUNNING/STALLED/PAUSED/FAILED => CANCEL CANCELLED => DELETE COMPLETED =>
// DELETE (only if deleteCompletedJobs==true)
//
// The status message field on a job will reflect if a request was made to move a job to an invalid
// state.
func (m *Manager) UpdateJob(jobUpdate *beeremote.UpdateJobRequest) (*beeremote.UpdateJobResponse, error) {

	m.readyMu.RLock()
	defer m.readyMu.RUnlock()
	if !m.ready {
		return nil, fmt.Errorf("unable to get jobs (JobMgr is not ready)")
	}

	if jobUpdate.NewState == flex.NewState_UNCHANGED {
		return nil, fmt.Errorf("job update requested by the job state is unchanged (possibly this indicates a bug in the caller)")
	}

	// We handle things a little differently depending if the job update was by
	// path or job ID. A path can update multiple jobs, a job ID at most one.
	// Both methods utilize a shared updateJobState for all job updates except
	// deletions. Job deletions are handled here because we have slightly
	// different handling depending if were working with a path or single job.
	if query := jobUpdate.GetByExactPath(); query != "" {
		pathEntry, releasePathEntry, err := m.pathStore.GetAndLockEntry(query)
		if err != nil {
			return nil, fmt.Errorf("error retrieving jobs for path %s: %w", query, err)
		}

		// Collect any errors and responses resulting from the job update(s).
		var multiErr types.MultiError
		response := &beeremote.UpdateJobResponse{
			// If updating any job fails this should be set to false.
			Ok:        true,
			Message:   "inspect individual responses for results",
			Responses: make([]*beeremote.JobResponse, 0),
		}

		// If we've been asked to delete jobs, after we verify they aren't
		// running and their jobResults are deleted we'll add them for deletion.
		jobsSafeToDelete := make([]string, 0)

		for _, job := range pathEntry.Value {
			if jobUpdate.NewState == flex.NewState_DELETE {
				if !job.InTerminalState() {
					job.GetStatus().Message = "unable to delete job that has not reached a terminal state (cancel it first)"
					response.Ok = false
					// Still get the results as it would be confusing to return empty results otherwise.
					// They may also be needed to troubleshoot why the job is not in a terminal state.
					response.Responses = append(response.Responses, &beeremote.JobResponse{
						Job:          job.Get(),
						WorkRequests: rst.RecreateWorkRequests(job.Get(), job.GetSegments()),
						WorkResults:  getWorkResultsForResponse(job.WorkResults),
					})
					continue
				}

				// By default only delete jobs in a terminal state other than completed:
				if job.GetStatus().State == flex.RequestStatus_COMPLETED && !jobUpdate.DeleteCompletedJobs {
					continue
				}
				jobsSafeToDelete = append(jobsSafeToDelete, job.GetId())
				job.GetStatus().Message = "job scheduled for deletion"
				response.Responses = append(response.Responses, &beeremote.JobResponse{
					Job:          job.Get(),
					WorkRequests: rst.RecreateWorkRequests(job.Get(), job.GetSegments()),
					WorkResults:  make([]*beeremote.JobResponse_WorkResult, 0),
				})
				continue
			}

			// Attempt to apply the requested update. If anything goes wrong the overall response
			// should be !ok. If the response is already !ok don't update it.
			response.Ok = m.updateJobState(job, jobUpdate.NewState) && response.Ok
			response.Responses = append(response.Responses, &beeremote.JobResponse{
				Job:          job.Get(),
				WorkRequests: rst.RecreateWorkRequests(job.Get(), job.GetSegments()),
				WorkResults:  getWorkResultsForResponse(job.WorkResults),
			})
		}

		for _, deletedJobID := range jobsSafeToDelete {
			delete(pathEntry.Value, deletedJobID)
		}

		if jobUpdate.NewState == flex.NewState_DELETE && len(pathEntry.Value) == 0 {
			// Only clean up the path entry if there are no more jobs left for it.
			// We want to delete the entry before releasing the lock, otherwise its
			// possible someone else slips in a new job for this path and we would
			// loose it. This is why we use the DeleteEntry release flag here.
			err = releasePathEntry(kvstore.DeleteEntry)
			if err != nil {
				multiErr.Errors = append(multiErr.Errors, fmt.Errorf("unable to delete path entry: %w", err))
				// If an error happens here we don't want to actually return the
				// response since we don't know if the path entry was actually
				// deleted.
				return nil, &multiErr
			}
		} else {
			err = releasePathEntry()
			if err != nil {
				multiErr.Errors = append(multiErr.Errors, fmt.Errorf("unable to commit and release path entry: %w", err))
				// If an error happens here we don't want to actually return a
				// response since we don't know if the path entry was actually
				// updated. Most likely it still reflects the previous state.
				return nil, &multiErr
			}
		}

		// An error here indicates there was one or more internal errors updating
		// the job state(s). We'll still return whatever responses we did get, but
		// indicate !ok and return the error as the response message.
		if len(multiErr.Errors) > 0 {
			response.Ok = false
			response.Message = multiErr.Error()
			return response, &multiErr
		}
		return response, nil

	} else if query := jobUpdate.GetByIdAndPath(); query != nil {

		// Aggregate errors since we might have multiple errors cleaning up.
		var multiErr types.MultiError

		pathEntry, releasePathEntry, err := m.pathStore.GetAndLockEntry(query.Path)
		if err != nil {
			multiErr.Errors = append(multiErr.Errors, fmt.Errorf("error retrieving jobs for path %s while searching for job ID %s : %w", query.Path, query.JobID, err))
			return nil, &multiErr
		}

		job, ok := pathEntry.Value[query.JobID]
		if !ok {
			multiErr.Errors = append(multiErr.Errors, fmt.Errorf("job ID %s does not exist for path %s", query.JobID, query.Path))
			if err = releasePathEntry(); err != nil {
				multiErr.Errors = append(multiErr.Errors, fmt.Errorf("an error occurred while releasing path entry %s: %w", query.Path, err))
			}
			return nil, &multiErr
		}

		if jobUpdate.NewState == flex.NewState_DELETE {
			if !job.InTerminalState() || (job.GetStatus().State == flex.RequestStatus_COMPLETED && !jobUpdate.DeleteCompletedJobs) {

				var responseMessage string

				if job.GetStatus().State == flex.RequestStatus_COMPLETED && !jobUpdate.DeleteCompletedJobs {
					// Note we don't set a message here to be consistent with how this is handled
					// when updating jobs by path.
					//status.Message = "refusing to delete completed job (deleted completed jobs flag is not set)"
					responseMessage = "refusing to delete completed job (deleted completed jobs flag is not set)"
				} else {
					job.GetStatus().Message = "unable to delete job that has not reached a terminal state (cancel it first)"
					responseMessage = "unable to delete job that has not reached a terminal state (cancel it first)"
				}

				response := &beeremote.UpdateJobResponse{
					Ok:      false,
					Message: responseMessage,
					Responses: []*beeremote.JobResponse{
						{
							Job:          job.Get(),
							WorkRequests: rst.RecreateWorkRequests(job.Get(), job.GetSegments()),
							WorkResults:  getWorkResultsForResponse(job.WorkResults),
						},
					},
				}

				if err = releasePathEntry(); err != nil {
					multiErr.Errors = append(multiErr.Errors, fmt.Errorf("an error occurred while releasing path entry %s: %w", query.Path, err))
				}
				if len(multiErr.Errors) > 0 {
					return nil, &multiErr
				}
				return response, nil
			}

			// We need to build the response before releasing anything.
			// This is just discarded if anything goes wrong.
			job.GetStatus().Message = "job scheduled for deletion"
			response := &beeremote.UpdateJobResponse{
				Ok:      true,
				Message: "success",
				Responses: []*beeremote.JobResponse{
					{
						Job:          job.Get(),
						WorkRequests: rst.RecreateWorkRequests(job.Get(), job.GetSegments()),
						WorkResults:  make([]*beeremote.JobResponse_WorkResult, 0),
					},
				},
			}

			delete(pathEntry.Value, query.JobID)
			// Only clean up the path entry if there are no more jobs left for it:
			if len(pathEntry.Value) == 0 {
				if err = releasePathEntry(kvstore.DeleteEntry); err != nil {
					return nil, fmt.Errorf("error removing path %s after deleting job ID %s (no jobs remain for the path): %w", query.Path, query.JobID, err)
				}
			} else {
				if err = releasePathEntry(); err != nil {
					return nil, fmt.Errorf("error releasing entry for path %s after updating job ID %s: %w", query.Path, query.JobID, err)
				}
			}
			return response, nil
		}

		ok = m.updateJobState(job, jobUpdate.NewState)
		if err = releasePathEntry(); err != nil {
			multiErr.Errors = append(multiErr.Errors, fmt.Errorf("error releasing entry for path %s: %w", query.Path, err))
		}
		if len(multiErr.Errors) > 0 {
			return nil, &multiErr
		}

		return &beeremote.UpdateJobResponse{
			Ok:      ok,
			Message: "inspect response for results",
			Responses: []*beeremote.JobResponse{
				{
					Job:          job.Get(),
					WorkRequests: rst.RecreateWorkRequests(job.Get(), job.GetSegments()),
					WorkResults:  getWorkResultsForResponse(job.WorkResults),
				},
			},
		}, nil

	} else {
		return nil, fmt.Errorf("unable to update job (no job ID or path specified)")
	}
}

// updateJobState takes an already locked job and and will attempt to apply newState. The job will
// be directly updated with the result of the operation. It is the callers responsibility to release
// the lock on the job. It returns true only if the new state was definitively applied. If it
// returns false the status and message of the job and its results should be inspected for
// additional details.
//
// IMPORTANT: This should only be used when the state of a job's work requests need to be modified
// on the assigned worker nodes. To update the job state in reaction to job results returned by a
// worker node use updateJobResults().
func (m *Manager) updateJobState(job *Job, newState flex.NewState) bool {

	ok := false
	if newState == flex.NewState_CANCEL {
		// If we're unable to definitively cancel on any node, ok is
		// set to false and the state of the job is unmodified.
		var updatedResults map[string]worker.WorkResult
		job.WorkResults, ok = m.workerManager.UpdateJob(workermgr.JobUpdate{
			JobID:       job.GetId(),
			WorkResults: job.WorkResults,
			NewState:    newState,
		})

		if !ok {
			job.GetStatus().State = flex.RequestStatus_FAILED
			job.GetStatus().Message = "error cancelling job (review work results for details)"
		} else {
			job.GetStatus().State = flex.RequestStatus_CANCELLED
			job.GetStatus().Message = "job cancelled"
		}
		m.log.Debug("updated job", zap.Any("job", job), zap.Any("workResults", updatedResults))
	}

	return ok
}

// updateJobResults processes work responses from worker nodes for outstanding
// work requests and handles updating the job results. Once all work requests
// have reached a terminal state, it handles moving the overall job state to a
// terminal state, including finishing the job by completing or cancelling
// multipart uploads (or equivalent for the specified job type). It only returns
// an error if there was an internal problem updating the results or job,
// otherwise the result of the update are reflected in the Job status and
// message. It may update the job status and also return an error for some
// corner cases.
func (m *Manager) updateJobResults(workResponse *flex.WorkResponse) error {

	pathEntry, commitAndRelease, err := m.pathStore.GetAndLockEntry(workResponse.Path)
	if err != nil {
		return status.Errorf(codes.NotFound, "no jobs found for path %s", workResponse.Path)
	}
	defer commitAndRelease()
	job, ok := pathEntry.Value[workResponse.JobId]
	if !ok {
		return status.Errorf(codes.NotFound, "path %s exists but does not have job ID %s", workResponse.Path, workResponse.JobId)
	}

	// Update the results entry.
	entryToUpdate, ok := job.WorkResults[workResponse.RequestId]
	if !ok {
		return fmt.Errorf("received a work response for an unknown work request: %s", workResponse)
	}
	entryToUpdate.WorkResponse = workResponse
	job.WorkResults[workResponse.RequestId] = entryToUpdate

	allSameState := true
	for _, workResult := range job.WorkResults {
		if !workResult.InTerminalState() && !workResult.RequiresUserIntervention() {
			// Don't do anything else if all work requests haven't reached a terminal state or aren't failed.
			return nil
		}
		// Verify all work requests have reached the same terminal state.
		// We'll compare against the work response we just received.
		if entryToUpdate.Status().State != workResult.Status().State {
			allSameState = false
		}
	}

	// TODO: Consider the scenarios where all work requests are in a terminal
	// state, but they aren't in the same state. Depending on how worker nodes
	// are implemented this could be possible if some WRs completed, then a user
	// cancelled the job. Perhaps we treat the overall job state as cancelled in
	// this scenario?
	if !allSameState {
		job.GetStatus().State = flex.RequestStatus_FAILED
		job.GetStatus().Message = "all work requests have reached a terminal state, but not all work requests are in the same state (this likely indicates a bug and the job will need to be cancelled to cleanup)"
		return nil
	}

	rst, ok := m.workerManager.RemoteStorageTargets[job.Request.GetRemoteStorageTarget()]
	if !ok {
		// We shouldn't return an error here. The caller (a worker node) can't do anything about
		// this. Most likely a user updated the configuration and removed the RST so there is
		// nothing we can do unless they were to add it back.
		job.GetStatus().State = flex.RequestStatus_FAILED
		job.GetStatus().Message = fmt.Sprintf("unable to complete job because the RST no longer exists: %s (add it back or manually cleanup any artifacts from this job)", job.Request.GetRemoteStorageTarget())
		return nil
	}

	// If everything has the same state, the state of the entry we just updated will match every other request.
	switch entryToUpdate.Status().State {
	case flex.RequestStatus_CANCELLED:
		if err := job.Complete(rst, true); err != nil {
			job.GetStatus().State = flex.RequestStatus_FAILED
			job.GetStatus().Message = "error cancelling job: " + err.Error()
		} else {
			job.GetStatus().State = flex.RequestStatus_CANCELLED
			job.GetStatus().Message = "successfully cancelled job"
		}
	case flex.RequestStatus_COMPLETED:
		if err := job.Complete(rst, false); err != nil {
			job.GetStatus().State = flex.RequestStatus_FAILED
			job.GetStatus().Message = "error completing job: " + err.Error()
		} else {
			job.GetStatus().State = flex.RequestStatus_COMPLETED
			job.GetStatus().Message = "successfully completed job"
		}
	case flex.RequestStatus_ERROR:
		// TODO: Automatically retry errors (also requires changing RequiresUserIntervention()). For
		// now treat jobs with an error the same as failed jobs.
		job.GetStatus().State = flex.RequestStatus_ERROR
		job.GetStatus().Message = "job cannot continue without user intervention (see work results for details)"
	case flex.RequestStatus_FAILED:
		// Something that went wrong that requires user intervention. We don't know what so don't
		// try to complete or abort the request as it may make it more difficult to recover.
		job.GetStatus().State = flex.RequestStatus_FAILED
		job.GetStatus().Message = "job cannot continue without user intervention (see work results for details)"
	default:
		job.GetStatus().State = entryToUpdate.Status().State
		job.GetStatus().Message = "all work requests have reached a terminal state, but the state is unknown (ignoring and updating job state anyway, this is likely a bug and may cause unexpected behavior)"
		// We return an error here because this is an internal problem that
		// shouldn't happen and hopefully a test will catch it. Most likely some
		// new terminal states were added and this function needs to be updated.
		// Since we have valid entries and all work requests are in the same
		// state it makes sense to update the job state to reflect this as well.
		return fmt.Errorf("all work requests have reached a terminal state, but the state is unknown (ignoring and updating job state anyway, this is likely a bug and may cause unexpected behavior)")
	}
	m.log.Debug("job result", zap.Any("job", job))
	return nil
}

func (m *Manager) Stop() {

	m.readyMu.RLock()
	defer m.readyMu.RUnlock()
	if !m.ready {
		return // Nothing to do.
	}

	m.cancel()
}
