package job

import (
	"context"
	"encoding/gob"
	"fmt"
	"path"
	"reflect"

	"github.com/dgraph-io/badger/v4"
	"github.com/thinkparq/bee-remote/internal/worker"
	"github.com/thinkparq/gobee/kvstore"
	beegfs "github.com/thinkparq/protobuf/beegfs/go"
	beeremote "github.com/thinkparq/protobuf/beeremote/go"
	"go.uber.org/zap"
)

func init() {
	gob.Register(&SyncJob{})
	gob.Register(&SyncSegment{})
}

type Config struct {
	PathDBPath         string `mapstructure:"pathDBPath"`
	PathDBCacheSize    int    `mapstructure:"pathDBCacheSize"`
	ResultsDBPath      string `mapstructure:"resultsDBPath"`
	ResultsDBCacheSize int    `mapstructure:"resultsDBCacheSize"`
	RequestQueueDepth  int    `mapstructure:"requestQueueDepth"`
	JournalPath        string `mapstructure:"journalPath"`
}

type Manager struct {
	log     *zap.Logger
	ctx     context.Context
	cancel  context.CancelFunc
	config  Config
	errChan chan<- error
	// jobRequests is where external callers should submit requests to be handled by JobMgr.
	jobRequests <-chan *beeremote.JobRequest
	// jobUpdates is where external callers should submit requests to update existing jobs.
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
func NewManager(log *zap.Logger, config Config, workerManager *worker.Manager, errCh chan<- error) (*Manager, chan<- *beeremote.JobRequest, chan<- *beeremote.UpdateJobRequest) {
	log = log.With(zap.String("component", path.Base(reflect.TypeOf(Manager{}).PkgPath())))
	ctx, cancel := context.WithCancel(context.Background())
	jobRequestChan := make(chan *beeremote.JobRequest, config.RequestQueueDepth)
	jobUpdatesChan := make(chan *beeremote.UpdateJobRequest, config.RequestQueueDepth)
	return &Manager{
		log:           log,
		config:        config,
		errChan:       errCh,
		workerManager: workerManager,
		ctx:           ctx,
		cancel:        cancel,
		jobRequests:   jobRequestChan,
		jobUpdates:    jobUpdatesChan,
	}, jobRequestChan, jobUpdatesChan
}

func (m *Manager) Manage() {

	// We initialize databases in Manage() so we can ensure the DBs are closed properly when shutting down.
	pathDBOpts := badger.DefaultOptions(m.config.PathDBPath)
	pathStore, closePathDB, err := kvstore.NewMapStore[Job](pathDBOpts, m.config.PathDBCacheSize)
	if err != nil {
		m.errChan <- fmt.Errorf("unable to setup paths DB: %w", err)
	}
	defer closePathDB()
	m.pathStore = pathStore

	jobResultsDBOpts := badger.DefaultOptions(m.config.ResultsDBPath)
	jobResultsStore, closeJobResultsStoreDB, err := kvstore.NewMapStore[worker.WorkResult](jobResultsDBOpts, m.config.ResultsDBCacheSize)
	if err != nil {
		m.errChan <- fmt.Errorf("unable to setup job results DB: %w", err)
		return
	}
	defer closeJobResultsStoreDB()
	m.jobResultsStore = jobResultsStore

	// TODO: Create a journal that also can be used to generate monotonically
	// increasing integers we can use as job IDs. The journal should also be
	// used to generate sequential job IDs.
	// Ref: https://dgraph.io/docs/badger/get-started/#monotonically-increasing-integers
	jobJournal, err := badger.Open(badger.DefaultOptions(m.config.JournalPath))
	if err != nil {
		m.errChan <- err
		return
	}
	defer jobJournal.Close()
	// TODO: Evaluate if a bandwidth of 1000 is appropriate.
	// Possibly this should be user configurable.
	jobIDGenerator, err := jobJournal.GetSequence([]byte("jobIDs"), 1000)
	if err != nil {
		m.errChan <- err
		return
	}
	defer jobIDGenerator.Release() // Defers are LIFO which means this is called before closing the DB.
	m.jobIDGenerator = jobIDGenerator

	// TODO: https://github.com/ThinkParQ/bee-remote/issues/9. Use a pool of
	// goroutines to process job requests and work responses.
	m.log.Info("now accepting job requests and work responses")
	for {
		select {
		case <-m.ctx.Done():
			m.log.Info("shutting down because the app is shutting down")
			return
		case jobRequest := <-m.jobRequests:
			m.processNewJobRequest(jobRequest)
		case jobUpdate := <-m.jobUpdates:
			switch jobUpdate.NewState {
			case beegfs.NewState_CANCEL:
				m.cancelJobRequest(jobUpdate)
			default:
				m.log.Warn("unsupported job updated requested", zap.Any("jobUpdate", jobUpdate))
			}
		case wr := <-m.workerManager.WorkResponses:
			// TODO:
			m.log.Info("workResponse", zap.Any("workResponse", wr))
		}
	}
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
	defer commitAndReleasePath()

	// TODO: More complex handling of existing jobs for this path. In some cases
	// we do support multiple jobs running for the same path. For example if the
	// job is configured with different RSTs.
	if len(pathEntry.Value) != 0 {
		for _, existingJob := range pathEntry.Value {
			status := existingJob.GetStatus().Status
			if status != beegfs.RequestStatus_CANCELLED && status != beegfs.RequestStatus_COMPLETED {
				m.log.Warn("rejecting job request because the specified entry already has an active job (cancel or wait for it to complete first)", zap.Any("path", job.GetPath()))
				newStatus := &beegfs.RequestStatus{
					Status:  beegfs.RequestStatus_CANCELLED,
					Message: "rejecting job request because the specified entry already has an active job (cancel or wait for it to complete first)",
				}
				job.SetStatus(newStatus)
				pathEntry.Value[job.GetID()] = job
				return
			}
		}
	}

	pathEntry.Value[job.GetID()] = job
	workResults := m.workerManager.SubmitJob(job.Allocate())

	// If we crashed here there could be WRs scheduled to worker nodes but we
	// have no record which nodes they were assigned to. To handle this after a
	// crash we'll replay the request journal and broadcast to all workers they
	// should cancel and work requests for jobs in the journal.

	jobResultEntry, commitAndReleaseJobResults, err := m.jobResultsStore.CreateAndLockEntry(job.GetID())
	if err != nil {
		m.log.Error("unable to create a job result entry", zap.Error(err), zap.Any("jobID", job.GetID()))
	}
	defer commitAndReleaseJobResults()

	for _, result := range workResults {
		jobResultEntry.Value[result.RequestID] = result
	}
}

// cancelJobRequest will attempt to cancel the specified job and any associated
// work requests. The caller should check the database to see the effect of any
// changes. If a path is specified then all jobs running for that path will be
// cancelled. If a job ID is specified, then only that job is cancelled.
// Specifying both a path and Job ID will result in an error. cancelJobRequest
// is idempotent, so it can be called multiple times to verify a job is fully
// cancelled, for example if there was an error cancelling some work requests.
func (m *Manager) cancelJobRequest(jobUpdate *beeremote.UpdateJobRequest) {

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
					newStatus := &beegfs.RequestStatus{
						Status:  beegfs.RequestStatus_CANCELLED,
						Message: "job cancelled by user",
					}
					job.SetStatus(newStatus)
					continue
				}
				m.log.Warn("unknown error getting results for job", zap.Error(err), zap.Any("jobUpdate", jobUpdate))
				continue
			}

			// If we're unable to definitively cancel on any node, allCancelled is
			// set to true and the state of the job is unmodified.
			allCancelled := true
			updatedResults := make(map[string]worker.WorkResult)
			for _, workResult := range resultsEntry.Value {

				// If the WR was never assigned we can just cancel it.
				if workResult.AssignedPool == "" && workResult.AssignedNode == "" {
					workResult.Status = beegfs.RequestStatus_CANCELLED
					updatedResults[workResult.RequestID] = workResult
					continue
				}

				// Otherwise send a message to the worker node to ensure it is cancelled:
				updateWorkRequest := &beegfs.UpdateWorkRequest{
					JobID:     jobUpdate.JobID,
					RequestID: workResult.RequestID,
					NewState:  jobUpdate.NewState,
				}

				newWorkResult, err := m.workerManager.UpdateWorkRequest(workResult.AssignedPool, workResult.AssignedNode, updateWorkRequest)
				if err != nil {
					m.log.Warn("error communicating with worker node to update work request", zap.Error(err), zap.Any("jobUpdate", jobUpdate), zap.Any("workResult", workResult))
					allCancelled = false
					continue
				}
				if newWorkResult.Status != beegfs.RequestStatus_CANCELLED {
					m.log.Warn("unable to cancel job (some work requests are still active)", zap.Any("workResult", newWorkResult))
					allCancelled = false
				}
				updatedResults[newWorkResult.RequestID] = newWorkResult
			}

			if allCancelled {
				newStatus := &beegfs.RequestStatus{
					Status:  beegfs.RequestStatus_CANCELLED,
					Message: "job cancelled by user",
				}
				job.SetStatus(newStatus)
			}
			updatedJobs[job.GetID()] = job
			resultsEntry.Value = updatedResults
			releaseResultsEntry()
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
