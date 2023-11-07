package job

import (
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"path"
	"reflect"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/thinkparq/bee-remote/internal/worker"
	"github.com/thinkparq/gobee/kvstore"
	"github.com/thinkparq/gobee/logger"
	"github.com/thinkparq/gobee/types"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
)

// Register custom types for serialization/deserialization via Gob when the
// package is initialized.
func init() {
	gob.Register(&SyncJob{})
	gob.Register(&SyncSegment{})
	gob.Register(&MockJob{})
}

var (
	// When running tests testMode can be set to true in individual test cases
	// to allow on-disk artifacts such as databases to be automatically cleaned
	// up after the test.
	testMode = false
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
	// JobRequests is where external callers should submit requests to be handled by JobMgr.
	JobRequests chan<- *beeremote.JobRequest
	// jobRequests is where goroutines manged by JobMgr listen for requests.
	jobRequests <-chan *beeremote.JobRequest
	// JobUpdates is where external callers should submit requests to update existing jobs.
	JobUpdates chan<- *beeremote.UpdateJobRequest
	//jobUpdates is where goroutines manged by JobMgr listen for requests.
	jobUpdates <-chan *beeremote.UpdateJobRequest
	// pathStore is the store where entries for file system paths with jobs
	// are kept. This store keeps a mapping of paths to Job(s).
	pathStore *kvstore.MapStore[Job]
	// jobResultsStore is the store where the entries for jobs with work
	// requests currently being handled by WorkerMgr are kept. This store keeps
	// a mapping of JobIDs to to their WorkResult(s) (i.e., a JobResult).
	jobResultsStore *kvstore.MapStore[worker.WorkResult]
	jobIDGenerator  *badger.Sequence
	// A pointer to an initialized/started worker manager.
	workerManager *worker.Manager
}

// NewManager initializes and returns a new Job manager and channels used to submit and update job requests.
func NewManager(log *zap.Logger, config Config, workerManager *worker.Manager) *Manager {
	log = log.With(zap.String("component", path.Base(reflect.TypeOf(Manager{}).PkgPath())))
	ctx, cancel := context.WithCancel(context.Background())
	jobRequestChan := make(chan *beeremote.JobRequest, config.RequestQueueDepth)
	jobUpdatesChan := make(chan *beeremote.UpdateJobRequest, config.RequestQueueDepth)
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
	pathStore, closePathDB, err := kvstore.NewMapStore[Job](pathDBOpts, m.config.PathDBCacheSize, testMode)
	if err != nil {
		return fmt.Errorf("unable to setup paths DB: %w", err)
	}
	deferredFuncs = append(deferredFuncs, closePathDB)
	m.pathStore = pathStore

	jobResultsDBOpts := badger.DefaultOptions(m.config.ResultsDBPath)
	jobResultsDBOpts = jobResultsDBOpts.WithLogger(logger.NewBadgerLoggerBridge("jobResultsDB", m.log))
	jobResultsStore, closeJobResultsStoreDB, err := kvstore.NewMapStore[worker.WorkResult](jobResultsDBOpts, m.config.ResultsDBCacheSize, testMode)
	if err != nil {
		return fmt.Errorf("unable to setup job results DB: %w", err)
	}
	deferredFuncs = append(deferredFuncs, closeJobResultsStoreDB)
	m.jobResultsStore = jobResultsStore

	// TODO: Create a journal that also can be used to generate monotonically
	// increasing integers we can use as job IDs. The journal should also be
	// used to generate sequential job IDs.
	// Ref: https://dgraph.io/docs/badger/get-started/#monotonically-increasing-integers

	var journalDBPath string
	if testMode {
		journalDBPath, err = os.MkdirTemp(m.config.JournalPath, "journalDBTestMode")
		if err != nil {
			return err
		}
	} else {
		journalDBPath = m.config.JournalPath
	}

	jobJournalDBOpts := badger.DefaultOptions(journalDBPath)
	jobJournalDBOpts = jobJournalDBOpts.WithLogger(logger.NewBadgerLoggerBridge("journalDB", m.log))
	jobJournal, err := badger.Open(jobJournalDBOpts)
	if err != nil {
		return err
	}
	if testMode {
		deferredFuncs = append(deferredFuncs, func() error {
			var multiErr types.MultiError
			err := jobJournal.Close()
			if err != nil {
				multiErr.Errors = append(multiErr.Errors, err)
			}
			err = os.RemoveAll(journalDBPath)
			if err != nil {
				multiErr.Errors = append(multiErr.Errors, err)
			}
			if len(multiErr.Errors) > 0 {
				return &multiErr
			}
			return nil
		})
	} else {
		deferredFuncs = append(deferredFuncs, func() error { return jobJournal.Close() })
	}

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
				m.processNewJobRequest(jobRequest)
			case jobUpdate := <-m.jobUpdates:
				switch jobUpdate.NewState {
				case flex.NewState_CANCEL:
					m.cancelJobRequest(jobUpdate)
				default:
					m.log.Warn("unsupported job updated requested", zap.Any("jobUpdate", jobUpdate))
				}
			case wr := <-m.workerManager.WorkResponses:
				// TODO:
				m.log.Info("workResponse", zap.Any("workResponse", wr))
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

	switch query := request.Query.(type) {
	case *beeremote.GetJobsRequest_JobID:
		// If we got a JobID first query job results then use the path from the
		// results metadata to get the path entry and return at most one job.
		resultsEntry, err := m.jobResultsStore.GetEntry(query.JobID)
		if err != nil {
			return nil, err
		}
		exactPath, ok := resultsEntry.Metadata["path"]
		if !ok {
			return nil, fmt.Errorf("found job results for job ID %s but the entry's metadata doesn't contain the path (unable to perform reverse lookup of job in path store)", query.JobID)
		}

		pathEntry, err := m.pathStore.GetEntry(exactPath)
		if err != nil {
			return nil, err
		}

		job, ok := pathEntry.Value[query.JobID]
		if !ok {
			return nil, fmt.Errorf("found job results for job ID %s but there is no corresponding entry in the path store (perhaps the job finished and is being cleaned up?)", query.JobID)
		}

		workRequests := ""
		if request.GetIncludeWorkRequests() {
			workRequests = job.GetWorkRequests()
		}

		workResults := []*beeremote.JobResponse_WorkResult{}

		if request.GetIncludeWorkResults() {
			for _, wr := range resultsEntry.Value {
				workResult := &beeremote.JobResponse_WorkResult{
					RequestId:    wr.RequestID,
					Status:       wr.GetStatus(),
					AssignedNode: wr.AssignedNode,
					AssignedPool: string(wr.AssignedPool),
				}
				workResults = append(workResults, workResult)
			}
		}

		return &beeremote.GetJobsResponse{
			Response: []*beeremote.JobResponse{
				{
					Job:          job.Get(),
					WorkRequests: workRequests,
					WorkResults:  workResults,
				},
			},
		}, nil

	case *beeremote.GetJobsRequest_ExactPath:
		// If we got an exact path first get the jobs for the path entry then
		// lookup all the job results if requested.
		pathEntry, err := m.pathStore.GetEntry(query.ExactPath)
		if err != nil {
			return nil, err
		}

		jobResponses := []*beeremote.JobResponse{}

		for jobID, job := range pathEntry.Value {
			workResults := []*beeremote.JobResponse_WorkResult{}

			workRequests := ""
			if request.GetIncludeWorkRequests() {
				workRequests = job.GetWorkRequests()
			}

			if request.GetIncludeWorkResults() {
				resultsEntry, err := m.jobResultsStore.GetEntry(jobID)
				if err != nil {
					return nil, err
				}

				for _, wr := range resultsEntry.Value {
					workResult := &beeremote.JobResponse_WorkResult{
						RequestId:    wr.RequestID,
						Status:       wr.GetStatus(),
						AssignedNode: wr.AssignedNode,
						AssignedPool: string(wr.AssignedPool),
					}
					workResults = append(workResults, workResult)
				}
			}
			jobResponse := beeremote.JobResponse{
				Job:          job.Get(),
				WorkRequests: workRequests,
				WorkResults:  workResults,
			}

			jobResponses = append(jobResponses, &jobResponse)
		}
		return &beeremote.GetJobsResponse{
			Response: jobResponses,
		}, nil

	case *beeremote.GetJobsRequest_PathPrefix:
		// If we got a path prefix first get all jobs for that path prefix.
		// If someone wanted to get all jobs they could provide a prefix of "/".

		pathItems, err := m.pathStore.GetEntries(query.PathPrefix)
		if err != nil {
			return nil, err
		}

		jobResponses := []*beeremote.JobResponse{}

		for _, pathItem := range pathItems {

			for jobID, job := range pathItem.Entry.Value {
				workResults := []*beeremote.JobResponse_WorkResult{}

				workRequests := ""
				if request.GetIncludeWorkRequests() {
					workRequests = job.GetWorkRequests()
				}

				if request.GetIncludeWorkResults() {
					resultsEntry, err := m.jobResultsStore.GetEntry(jobID)
					if err != nil {
						return nil, err
					}

					for _, wr := range resultsEntry.Value {
						workResult := &beeremote.JobResponse_WorkResult{
							RequestId:    wr.RequestID,
							Status:       wr.GetStatus(),
							AssignedNode: wr.AssignedNode,
							AssignedPool: string(wr.AssignedPool),
						}
						workResults = append(workResults, workResult)
					}
				}
				jobResponse := beeremote.JobResponse{
					Job:          job.Get(),
					WorkRequests: workRequests,
					WorkResults:  workResults,
				}

				jobResponses = append(jobResponses, &jobResponse)
			}
		}
		return &beeremote.GetJobsResponse{
			Response: jobResponses,
		}, nil
	}

	return nil, fmt.Errorf("no query parameters provided (to get all jobs use '/' with the prefix path query type)")
}

func (m *Manager) processNewJobRequest(jr *beeremote.JobRequest) {

	job, err := New(m.jobIDGenerator, jr)
	if err != nil {
		m.log.Error("unable to generate job from job request", zap.Error(err), zap.Any("jobRequest", jr))
		return
	}

	// TODO: Consider adding a CreateOrGetEntry method to the MapStore to
	// simplify/optimize this. Or adding a flag to the existing method that will
	// get the entry if it already exists.
	pathEntry, commitAndReleasePath, err := m.pathStore.CreateAndLockEntry(job.GetPath())
	if err == kvstore.ErrEntryAlreadyExistsInDB || err == kvstore.ErrEntryAlreadyExistsInCache {
		pathEntry, commitAndReleasePath, err = m.pathStore.GetAndLockEntry(job.GetPath())
		if err != nil {
			m.log.Error("unable to get existing path entry", zap.Error(err), zap.Any("path", job.GetPath()), zap.Any("jobID", job.GetID()))
			return
		}
	} else if err != nil {
		m.log.Error("unable to create new path entry", zap.Error(err), zap.Any("path", job.GetPath()), zap.Any("jobID", job.GetID()))
		return
	}
	defer func() {
		if err := commitAndReleasePath(); err != nil {
			m.log.Error("unable to release path entry", zap.Error(err))
		}
	}()

	jobResultEntry, commitAndReleaseJobResults, err := m.jobResultsStore.CreateAndLockEntry(job.GetID())
	if err != nil {
		m.log.Error("unable to create a job result entry", zap.Error(err), zap.Any("jobID", job.GetID()))
		// Mostly likely an error here either means there was an existing entry,
		// or an issue accessing the DB. In theory we should never have an
		// existing entry for a new Job unless we have duplicate Job IDs. The
		// way things are setup we could end up with a job result entry but not
		// job entry for a particular Job ID (if we crashed at precisely the
		// right time as the deferred commitAndRelease funcs are being called).
		// But whenever processNewJobRequest is called it always generated a new
		// job ID. Either way something has gone horribly wrong, lets bail out.
		return
	}
	defer func() {
		if err := commitAndReleaseJobResults(); err != nil {
			m.log.Error("unable to release job results", zap.Error(err))
		}
	}()

	// TODO: More complex handling of existing jobs for this path. In some cases
	// we do support multiple jobs running for the same path. For example if the
	// job is configured with different RSTs.
	if len(pathEntry.Value) != 0 {
		for _, existingJob := range pathEntry.Value {
			status := existingJob.GetStatus().Status
			if status != flex.RequestStatus_CANCELLED && status != flex.RequestStatus_COMPLETED {
				m.log.Warn("rejecting job request because the specified entry already has an active job (cancel or wait for it to complete first)", zap.Any("path", job.GetPath()))
				newStatus := &flex.RequestStatus{
					Status:  flex.RequestStatus_CANCELLED,
					Message: "rejecting job request because the specified entry already has an active job (cancel or wait for it to complete first)",
				}
				job.SetStatus(newStatus)
				pathEntry.Value[job.GetID()] = job

				// Even if we don't submit a job we should still add a job results entry.
				// This allows reverse lookup of jobs by ID.
				jobResultEntry.Metadata["path"] = job.GetPath()
				jobResultEntry.Value = make(map[string]worker.WorkResult)
				return
			}
		}
	}

	pathEntry.Value[job.GetID()] = job
	workResults, allScheduled := m.workerManager.SubmitJob(job.Allocate())

	// If we crashed here there could be WRs scheduled to worker nodes but we
	// have no record which nodes they were assigned to. To handle this after a
	// crash we'll replay the request journal and broadcast to all workers they
	// should cancel and work requests for jobs in the journal.

	jobResultEntry.Metadata["path"] = job.GetPath()
	jobResultEntry.Value = workResults

	newStatus := flex.RequestStatus{}
	if !allScheduled {
		newStatus.Status = flex.RequestStatus_FAILED
		newStatus.Message = "error initially scheduling work requests"
	} else {
		newStatus.Status = flex.RequestStatus_SCHEDULED
		newStatus.Message = "job scheduled"
	}

	job.SetStatus(&newStatus)
	pathEntry.Value[job.GetID()] = job

	m.log.Debug("created job", zap.Any("job", job), zap.Any("workResults", workResults))
}

// cancelJobRequest will attempt to cancel the specified job and any associated
// work requests. The caller should check the database to see the effect of any
// changes. If a path is specified then all jobs running for that path will be
// cancelled. If a job ID is specified, then only that job is cancelled.
// Specifying both a path and Job ID will result in an error. cancelJobRequest
// is idempotent, so it can be called multiple times to verify a job is fully
// cancelled, for example if there was an error cancelling some work requests.
func (m *Manager) cancelJobRequest(jobUpdate *beeremote.UpdateJobRequest) {

	if jobUpdate.NewState != flex.NewState_CANCEL {
		m.log.Warn("cancel job request was invoked with a new state other than cancelled (probably this is a bug)", zap.Any("newState", jobUpdate.NewState))
	}

	if jobUpdate.GetJobID() != "" && jobUpdate.GetPath() != "" {
		m.log.Warn("updating a job by both path and job ID is not allowed (only one should be specified)", zap.Any("jobUpdate", jobUpdate))
		return

	} else if path := jobUpdate.GetPath(); path != "" {
		pathEntry, releasePathEntry, err := m.pathStore.GetAndLockEntry(path)
		if err != nil {
			m.log.Warn("unable to get jobs for the specified path", zap.Error(err), zap.Any("jobUpdate", jobUpdate))
			return
		}

		updatedJobs := make(map[string]Job)

		for _, job := range pathEntry.Value {

			resultsEntry, releaseResultsEntry, err := m.jobResultsStore.GetAndLockEntry(job.GetID())
			if err != nil {
				if err == badger.ErrKeyNotFound {
					newStatus := &flex.RequestStatus{
						Status:  flex.RequestStatus_CANCELLED,
						Message: "job cancelled by user",
					}
					job.SetStatus(newStatus)
					continue
				}
				m.log.Warn("unknown error getting results for job", zap.Error(err), zap.Any("jobUpdate", jobUpdate))
				continue
			}
			// IMPORTANT: Ensure releaseResultsEntry() is called if we get a valid resultsEntry.
			ju := worker.JobUpdate{
				JobID:       jobUpdate.JobID,
				WorkResults: resultsEntry.Value,
				NewState:    jobUpdate.NewState,
			}
			// If we're unable to definitively cancel on any node, allCancelled is
			// set to false and the state of the job is unmodified.
			updatedResults, allCancelled := m.workerManager.UpdateJob(ju)

			newStatus := &flex.RequestStatus{}
			if !allCancelled {
				newStatus.Status = flex.RequestStatus_FAILED
				newStatus.Message = "error cancelling job (review work results for details)"
			} else {
				newStatus.Status = flex.RequestStatus_CANCELLED
				newStatus.Message = "job cancelled"
			}

			job.SetStatus(newStatus)
			updatedJobs[job.GetID()] = job
			resultsEntry.Value = updatedResults
			err = releaseResultsEntry()
			if err != nil {
				m.log.Warn("unable to commit and release results entry", zap.Error(err), zap.Any("jobUpdate", jobUpdate))
			}
			m.log.Debug("updated job", zap.Any("job", job), zap.Any("workResults", updatedResults))
		}

		pathEntry.Value = updatedJobs
		err = releasePathEntry()
		if err != nil {
			m.log.Warn("unable to commit and release path entry", zap.Error(err), zap.Any("jobUpdate", jobUpdate))
		}
	} else if jobUpdate.GetJobID() != "" {
		// TODO: implement.
		m.log.Warn("updating jobs based on job ID alone is not currently supported", zap.Any("jobUpdate", jobUpdate))
	} else {
		m.log.Warn("unable to update job (no path specified)", zap.Any("jobUpdate", jobUpdate))
	}

}

func (m *Manager) Stop() {
	m.cancel()
}
