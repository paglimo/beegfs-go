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
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Register custom types for serialization/deserialization via Gob when the
// package is initialized.
func init() {
	gob.Register(&Job{})
	gob.Register(&Segment{})
}

type Config struct {
	PathDBPath          string `mapstructure:"pathDBPath"`
	PathDBCacheSize     int    `mapstructure:"pathDBCacheSize"`
	RequestQueueDepth   int    `mapstructure:"requestQueueDepth"`
	MinJobEntriesPerRST int    `mapstructure:"minJobEntriesPerRST"`
	MaxJobEntriesPerRST int    `mapstructure:"maxJobEntriesPerRST"`
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
	// WorkResults is where work results from worker nodes should be sent.
	// These updates are sent to JobMgr via the gRPC server.
	WorkResults chan<- *flex.Work
	// workResults is where goroutines managed by JobMgr listen for work results.
	workResults <-chan *flex.Work
	// pathStore is the store where entries for file system paths with jobs
	// are kept. This store keeps a mapping of paths to Job(s). Note the inner
	// map is a map of Job IDs to jobs (not RST IDs to jobs) so we can retain
	// a configurable number of historical jobs for each path+RST combo.
	pathStore *kvstore.MapStore[map[string]*Job]
	// A pointer to an initialized/started worker manager.
	workerManager *workermgr.Manager
}

// NewManager initializes and returns a new Job manager and channels used to submit and update job requests.
func NewManager(log *zap.Logger, config Config, workerManager *workermgr.Manager) *Manager {
	log = log.With(zap.String("component", path.Base(reflect.TypeOf(Manager{}).PkgPath())))
	ctx, cancel := context.WithCancel(context.Background())
	jobRequestChan := make(chan *beeremote.JobRequest, config.RequestQueueDepth)
	jobUpdatesChan := make(chan *beeremote.UpdateJobRequest, config.RequestQueueDepth)
	workResultsChan := make(chan *flex.Work, config.RequestQueueDepth)
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
		WorkResults:   workResultsChan,
		workResults:   workResultsChan,
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
			case workResult := <-m.workResults:
				err := m.UpdateWork(workResult)
				// TODO: https://github.com/ThinkParQ/bee-remote/issues/11
				// Once we are using a journal to keep track of job results, it needs to be updated
				// depending if the update was successful or not (this will likely be unneeded
				// depending on the outcome of #11.
				if err != nil {
					m.log.Error("error updating job with work result", zap.Error(err), zap.Any("workResult", workResult))
				} else {
					m.log.Debug("processed work result", zap.Any("workResult", workResult))
				}
			}
		}
	}()
	return nil
}

func (m *Manager) GetJobs(ctx context.Context, request *beeremote.GetJobsRequest, responses chan<- *beeremote.GetJobsResponse) error {

	defer close(responses)

	m.readyMu.RLock()
	defer m.readyMu.RUnlock()
	if !m.ready {
		return fmt.Errorf("unable to get jobs (JobMgr is not ready)")
	}

	getJobResults := func(job *Job) *beeremote.JobResult {
		workRequests := make([]*flex.WorkRequest, 0)
		workResults := make([]*beeremote.JobResult_WorkResult, 0)
		if request.GetIncludeWorkRequests() {
			workRequests = rst.RecreateWorkRequests(job.Get(), job.GetSegments())
		}

		if request.GetIncludeWorkResults() {
			for _, wr := range job.WorkResults {
				workResult := &beeremote.JobResult_WorkResult{
					Work:         wr.WorkResult,
					AssignedNode: wr.AssignedNode,
					AssignedPool: string(wr.AssignedPool),
				}
				workResults = append(workResults, workResult)
			}
		}
		return &beeremote.JobResult{
			Job:          job.Get(),
			WorkRequests: workRequests,
			WorkResults:  workResults,
		}
	}

	switch query := request.Query.(type) {
	case *beeremote.GetJobsRequest_ByJobIdAndPath:
		if query.ByJobIdAndPath.JobId == "" || query.ByJobIdAndPath.Path == "" {
			return fmt.Errorf("to query by job ID and path, both must be specified")
		}

		pathEntry, err := m.pathStore.GetEntry(query.ByJobIdAndPath.Path)
		if err != nil {
			return err
		}

		job, ok := pathEntry.Value[query.ByJobIdAndPath.JobId]
		if !ok {
			return fmt.Errorf("job ID %s does not exist for path %s", query.ByJobIdAndPath.JobId, query.ByJobIdAndPath.Path)
		}

		responses <- &beeremote.GetJobsResponse{
			Path: query.ByJobIdAndPath.Path,
			Results: []*beeremote.JobResult{
				getJobResults(job),
			},
		}
		return nil

	case *beeremote.GetJobsRequest_ByExactPath:
		pathEntry, err := m.pathStore.GetEntry(query.ByExactPath)
		if err != nil {
			return err
		}

		jobResults := []*beeremote.JobResult{}
		for _, job := range pathEntry.Value {
			jobResults = append(jobResults, getJobResults(job))
		}

		responses <- &beeremote.GetJobsResponse{
			Path:    query.ByExactPath,
			Results: jobResults,
		}
		return nil

	case *beeremote.GetJobsRequest_ByPathPrefix:
		// For path prefix use iterate over each matching path and split the jobs for each path into
		// separate responses. This ensures we don't run out of memory and can efficiently stream
		// back as many jobs as the user wants (including all jobs if the prefix was "/").
		nextEntry, cleanupEntries, err := m.pathStore.GetEntries(kvstore.WithKeyPrefix(query.ByPathPrefix))
		if err != nil {
			return err
		}
		defer cleanupEntries()

	sendResponses:
		for {
			select {
			case <-ctx.Done():
				break sendResponses
			default:
				entry, err := nextEntry()
				if err != nil {
					return err
				}
				if entry == nil {
					break sendResponses
				}

				respForPath := &beeremote.GetJobsResponse{
					Path:    entry.Key,
					Results: make([]*beeremote.JobResult, 0, len(entry.Entry.Value)),
				}

				for _, job := range entry.Entry.Value {
					respForPath.Results = append(respForPath.Results, getJobResults(job))
				}
				responses <- respForPath
			}
		}
		return nil
	}

	return fmt.Errorf("no query parameters provided (to get all jobs use '/' with the prefix path query type)")
}

// Takes a single job request and attempts to create and schedule a job for it. Returns an error if
// the job could not be created or there was a scheduling error. There are two main reasons we may
// not be able to create a job:
//
// (1) The provided request is invalid. For example the job type doesn't support the specified RST
// or the RST doesn't exist.
//
// (2) Some temporary condition makes the job request currently impossible. For example there is
// already an active job for the RST/path combo or there was an internal error retrieving or saving
// DB entries.
//
// If there was a scheduling error the job is still created, but the state will be CANCELLED if all
// work requests that may have been started are definitively cancelled and it is safe to submit
// another job, or FAILED if there were any problems cleaning up that require manual intervention.
// If the response is not nil the status of the overall job and individual work requests should be
// reviewed to troubleshoot as errors are more general to guide the user on broad recovery steps.
func (m *Manager) SubmitJobRequest(jr *beeremote.JobRequest) (*beeremote.JobResult, error) {

	m.readyMu.RLock()
	defer m.readyMu.RUnlock()
	if !m.ready {
		return nil, fmt.Errorf("unable to get jobs (JobMgr is not ready)")
	}

	job, err := New(jr)
	if err != nil {
		return nil, fmt.Errorf("unable to generate job from job request: %w", err)
	}

	// Initialize reference types:
	job.WorkResults = make(map[string]worker.WorkResult)

	// TODO: https://github.com/ThinkParQ/gobee/issues/10
	// Add a flag to `CreateAndLockEntry()` that will get the entry if it already exists so the
	// caller doesn't have to check for an error then call `GetAndLockEntry()` (likely still
	// returning an appropriate error so the caller can adjust behavior as needed).
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
		// If there was an error creating the job and there are no other jobs for this path, clean
		// it up when we return.
		if len(pathEntry.Value) == 0 {
			if err := commitAndReleasePath(kvstore.DeleteEntry); err != nil {
				m.log.Error("unable to release and cleanup path entry with no jobs", zap.Error(err))
			}
		} else if err := commitAndReleasePath(); err != nil {
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
			} else if existingJob.Job.Id == job.Id {
				// We use a randomly generated UUID as the job ID. A collision is highly unlikely so
				// most likely something changed and we have a bug somewhere.
				return nil, fmt.Errorf("rejecting job request because the generated job ID (%s) is the same as a existing job ID (%s) (probably there is a bug somewhere)", job.Id, existingJob.Job.Id)
			}
			// Otherwise we found a job for a different RST not in a terminal state. Don't touch it.
		}

		for _, jobsForRST := range terminalJobsByRST {
			// It would be inefficient if we only cleaned up one entry at a
			// time so we set a max number of entries before we start GC.
			if len(jobsForRST) > m.config.MaxJobEntriesPerRST {
				// Sort the slice so we can delete old jobs based on the lowest
				// job ID until we reach minJobEntriesPerRST.
				sort.Slice(jobsForRST, func(i, j int) bool { return jobsForRST[i].GetId() < jobsForRST[j].GetId() })
				for _, gcJob := range jobsForRST[:m.config.MinJobEntriesPerRST+1] {
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

	jobSubmission, retry, err := job.GenerateSubmission(m.ctx, rstClient)
	if err != nil && !retry {
		return nil, fmt.Errorf("a fatal error occurred generating the job submission, please check the job or RST configuration before trying again: %w", err)
	} else if err != nil {
		// TODO: https://github.com/ThinkParQ/bee-remote/issues/27. If we decide to implement a
		// method to allow jobs to be resumed after an error/failure occurs, it could make sense to
		// create the job but leave the state as error or failed (depending on the results of #27),
		// allowing the user to easily resume jobs impacted by some transient issue in the
		// environment. For now just return an error. This is the original approach, cleanup
		// depending on the results of #27:
		//
		// job.GetStatus().State = beeremote.Job_FAILED job.GetStatus().Message = err.Error()
		// pathEntry.Value[job.GetId()] = job return &beeremote.JobResponse{
		//  Job:          job.Get(),
		//  WorkRequests: rst.RecreateWorkRequests(job.Get(), job.GetSegments()),
		//  WorkResults:  getWorkResultsForResponse(job.WorkResults),
		// }, nil
		return nil, fmt.Errorf("an transient error occurred generating the job submission, please try again: %w", err)
	}

	// At this point we have a properly formed job request that should be runnable barring any
	// transient issues. Details errors from this point onwards should be indicated by the job
	// status. We don't plan to allow jobs to be submitted asynchronously, so if there are any
	// errors scheduling also return an error to the caller so they know to fix whatever prevented
	// the job from being scheduled before trying again.
	pathEntry.Value[job.GetId()] = job
	job.WorkResults, job.Status, err = m.workerManager.SubmitJob(jobSubmission)

	// TODO: https://github.com/ThinkParQ/bee-remote/issues/11
	// Decide if this concern is valid enough to do anything about.
	//
	// A crash here could mean there are WRs scheduled to worker nodes but no record of the
	// job or which nodes they were assigned to because the updated pathEntry hasn't been committed
	// to the DB. If we had a journal mechanism we could reply the journal and broadcast to all
	// workers they should cancel those jobs to cleanup any remnants and retry the job creation.
	// However if we don't have a journal, is there a more simpler way we can ensure this never
	// happens? Or is it so unlikely its not worth trying to solve?

	m.log.Debug("created job", zap.Any("job", job))

	return &beeremote.JobResult{
		Job:          job.Get(),
		WorkRequests: rst.RecreateWorkRequests(job.Get(), job.GetSegments()),
		WorkResults:  getProtoWorkResults(job.WorkResults),
	}, err
}

// UpdateJob will attempt to update the specified job(s) and any associated work requests. If a path
// is specified then all jobs running for that path will be updated. If a path and job ID is
// specified, then only that job is updated. UpdateJob is idempotent, so it can be called multiple
// times to verify a job is updated, for example if there was an issue cancelling some of a job's
// work requests.
//
// An error is only returned if an internal error (such as a DB connection issue) occurs updating
// any of the specified job(s). If the state of one or more jobs could not be updated then the
// response will be !ok. The response message will indicate any jobs who's state remains unchanged
// because the new state was invalid, for example trying to delete a job that is not complete. The
// status message on individual jobs is updated if the job state was updated, but not to the desired
// new state, for example if a job was cancelled but the work requests could not be cancelled. Note
// in some cases the response may be ok but the message contains warnings, for example trying to
// delete a completed job when the force update flag is not set.
//
// When force update != true, when updating jobs by path or ID, completed jobs are ignored and do
// not affect if the response is ok, but a warning will always be logged in the response message.
//
// Note if an internal error occurs updating a job, no response is returned. At this time internal
// errors are not recorded on the status of the job or saved in the database as this would likely
// lead to inconsistent behavior considering most internal errors are likely database issues that
// would cause the status not to be actually updated. As a result while the actual status of the job
// may be indeterminate, the status recorded in the database should reflect the previous state,
// ensuring UpdateJob can be called multiple times once any internal issues are resolved. Generally
// internal errors should just be logged or returned to the user when handling interactive requests,
// and the typical recovery path is to retry.
//
// Allowed state changes:
//
//	UNASSIGNED/SCHEDULED/RUNNING/STALLED/PAUSED/FAILED => CANCEL
//	CANCELLED => DELETE / CANCEL
//	COMPLETED => DELETE / CANCEL // only if ForceUpdate==true
func (m *Manager) UpdateJob(jobUpdate *beeremote.UpdateJobRequest) (*beeremote.UpdateJobResponse, error) {

	m.readyMu.RLock()
	defer m.readyMu.RUnlock()
	if !m.ready {
		return nil, fmt.Errorf("unable to get jobs (JobMgr is not ready)")
	}

	if jobUpdate.NewState == beeremote.UpdateJobRequest_UNSPECIFIED {
		return nil, fmt.Errorf("no new job state specified (probably this indicates a bug in the caller)")
	}

	// The response will collect the results of all the job updates.
	response := &beeremote.UpdateJobResponse{
		// If updating any job fails this should be set to false.
		Ok: true,
		// If the state of any job remains unchanged, this message will indicate why.
		Message: "",
		Results: make([]*beeremote.JobResult, 0),
	}

	// We handle things a little differently depending if the job update was by path or job ID. A
	// path can update multiple jobs, a job ID at most one. Both methods utilize a shared
	// updateJobState for all job updates.
	if query := jobUpdate.GetByExactPath(); query != "" {
		pathEntry, releasePathEntry, err := m.pathStore.GetAndLockEntry(query)
		if err != nil {
			return nil, fmt.Errorf("error retrieving jobs for path %s: %w", query, err)
		}
		// Don't use a defer to release the path entry, we want to control how it is released
		// depending if it should be deleted. Be careful not to add a return without first releasing
		// the entry!

		// If we've been asked to delete jobs, after we verify they are in a terminal state and
		// deletion is allowed, we'll mark them for deletion.
		var jobsSafeToDelete []string
		if jobUpdate.NewState == beeremote.UpdateJobRequest_DELETED {
			jobsSafeToDelete = make([]string, 0)
		}

		for _, job := range pathEntry.Value {
			// Attempt to apply the requested update.
			success, safeToDelete, newMessage := m.updateJobState(job, jobUpdate.NewState, jobUpdate.ForceUpdate)
			// If anything goes wrong the overall response should be !ok. If the response is already
			// !ok from some other job, don't overwrite it:
			response.Ok = success && response.Ok
			if newMessage != "" {
				response.Message = response.Message + newMessage + ";"
			}
			// Only if the user requested a deletion and the job is safe to delete do we mark it for deletion:
			if jobUpdate.NewState == beeremote.UpdateJobRequest_DELETED && safeToDelete {
				jobsSafeToDelete = append(jobsSafeToDelete, job.GetId())
			}
			response.Results = append(response.Results, &beeremote.JobResult{
				Job:          job.Get(),
				WorkRequests: rst.RecreateWorkRequests(job.Get(), job.GetSegments()),
				WorkResults:  getProtoWorkResults(job.WorkResults),
			})
		}

		if jobUpdate.NewState == beeremote.UpdateJobRequest_DELETED {
			for _, deletedJobID := range jobsSafeToDelete {
				delete(pathEntry.Value, deletedJobID)
			}
		}
		if jobUpdate.NewState == beeremote.UpdateJobRequest_DELETED && len(pathEntry.Value) == 0 {
			// Only clean up the path entry if there are no more jobs left for it.
			// We want to delete the entry before releasing the lock, otherwise its
			// possible someone else slips in a new job for this path and we would
			// loose it. This is why we use the DeleteEntry release flag here.
			err = releasePathEntry(kvstore.DeleteEntry)
			if err != nil {
				// If an error happens here we don't want to actually return the response since we
				// don't know if the path entry was actually deleted.
				return nil, fmt.Errorf("no jobs should remain for path %s but was unable to remove the path entry (try again): %w", query, err)
			}
		} else {
			err = releasePathEntry()
			if err != nil {
				// If an error happens here we don't want to actually return a response since we
				// don't know if the path entry was actually updated. Most likely it still reflects
				// the previous state.
				return nil, fmt.Errorf("unable to commit and release entry for path %s (try again): %w", query, err)
			}
		}

		return response, nil

	} else if query := jobUpdate.GetByIdAndPath(); query != nil {

		if query.JobId == "" || query.Path == "" {
			return nil, fmt.Errorf("to update by job ID and path, both must be specified")
		}

		pathEntry, releasePathEntry, err := m.pathStore.GetAndLockEntry(query.Path)
		if err != nil {
			return nil, fmt.Errorf("error retrieving jobs for path %s while searching for job ID %s : %w", query.Path, query.JobId, err)
		}

		job, ok := pathEntry.Value[query.JobId]
		if !ok {
			var multiErr types.MultiError
			multiErr.Errors = append(multiErr.Errors, fmt.Errorf("job ID %s does not exist for path %s", query.JobId, query.Path))
			if err = releasePathEntry(); err != nil {
				multiErr.Errors = append(multiErr.Errors, fmt.Errorf("unable to commit and release path entry %s: %w", query.Path, err))
			}
			return nil, &multiErr
		}

		// Attempt to apply the requested update.
		success, safeToDelete, newMessage := m.updateJobState(job, jobUpdate.NewState, jobUpdate.ForceUpdate)
		response.Ok = success
		if newMessage != "" {
			response.Message = response.Message + newMessage + ";"
		}
		// Only if the user asked to delete the job and it is safe to delete should we delete it.
		if jobUpdate.NewState == beeremote.UpdateJobRequest_DELETED && safeToDelete {
			delete(pathEntry.Value, query.JobId)
		}
		// Only clean up the path entry if there are no more jobs left for it:
		if len(pathEntry.Value) == 0 {
			if err = releasePathEntry(kvstore.DeleteEntry); err != nil {
				return nil, fmt.Errorf("after deleting job ID %s no jobs should remain for path %s but was unable to remove the path entry (try again): %w", query.JobId, query.Path, err)
			}
		} else {
			if err = releasePathEntry(); err != nil {
				return nil, fmt.Errorf("unable to commit and release entry for path %s after updating job ID %s (try again): %w", query.Path, query.JobId, err)
			}
		}
		response.Results = append(response.Results, &beeremote.JobResult{
			Job:          job.Get(),
			WorkRequests: rst.RecreateWorkRequests(job.Get(), job.GetSegments()),
			WorkResults:  getProtoWorkResults(job.WorkResults),
		})

		return response, nil

	} else {
		return nil, fmt.Errorf("unable to update job (no job ID or path specified)")
	}
}

// updateJobState takes an already locked job and and will attempt to apply newState. The job will
// be directly updated with the result of the operation. It is the callers responsibility to release
// the lock on the job. It returns success only if the new state was definitively applied OR if the
// update was forced. If it returns !success, the job state was updated (but not to the desired
// state), and the job and/or work request status messages will be updated directly. If it returns
// !success but the job state was not updated (perhaps because the new state is invalid), it returns
// a message that should be included in the overall UpdateJobResponse to help a user understand why
// the new state could not be applied. For example if a job was deleted but hasn't yet reached a
// terminal state.
//
// If the update was forced it will always return success and safeToDelete to ensure the caller can
// always delete the job from the internal DB, at the risk of not fully cleaning up the job (e.g.,
// leaving orphaned work requests on worker nodes or incomplete multipart uploads). If there were
// any issues forcing the update the job status will indicate this. Generally callers should first
// try to update a job and only force the update if absolutely necessary.
//
// By default completed jobs are not updated and always return success, !safeToDelete and a warning
// message. As a safety hatch completed jobs can be forcibly updated by specifying forceUpdate.
//
// IMPORTANT: This should only be used when the state of a job's work requests need to be modified
// on the assigned worker nodes. To update the job state in reaction to job results returned by a
// worker node use updateJobResults().
func (m *Manager) updateJobState(job *Job, newState beeremote.UpdateJobRequest_NewState, forceUpdate bool) (success bool, safeToDelete bool, message string) {

	if job.Status.State == beeremote.Job_COMPLETED && !forceUpdate {
		return true, false, fmt.Sprintf("rejecting update for completed job ID %s (use the force update flag to override)", job.Id)
	}

	if newState == beeremote.UpdateJobRequest_DELETED {
		// When returning ensure the timestamp on the job state is updated.
		defer func() {
			job.GetStatus().Updated = timestamppb.Now()
		}()
		if !job.InTerminalState() {
			return false, false, fmt.Sprintf("unable to delete job %s because it has not reached a terminal state (cancel it first)", job.Id)
		}
		job.GetStatus().Message = "job scheduled for deletion"
		return true, true, ""
	}

	if newState == beeremote.UpdateJobRequest_CANCELLED {
		// When returning ensure the timestamp on the job state is updated.
		defer func() {
			job.GetStatus().Updated = timestamppb.Now()
		}()
		// If the job is already failed we don't need to update the work requests unless the update was forced:
		if job.GetStatus().State != beeremote.Job_FAILED || forceUpdate {
			// If we're unable to definitively cancel on any node, success is set to false and the
			// state of the job is failed.
			job.WorkResults, success = m.workerManager.UpdateJob(workermgr.JobUpdate{
				JobID:       job.GetId(),
				WorkResults: job.WorkResults,
				NewState:    flex.UpdateWorkRequest_CANCELLED,
			})
			if !success {
				if !forceUpdate {
					job.GetStatus().State = beeremote.Job_UNKNOWN
					job.GetStatus().Message = "unable to cancel job: unable to confirm work request(s) are no longer running on worker nodes (review work results for details and try again later)"
					return false, false, ""
				} else {
					job.GetStatus().Message = "canceling job: unable to confirm work request(s) are no longer running on worker nodes (cancelling anyway because this is a forced update)"
				}
			} else {
				job.GetStatus().State = beeremote.Job_CANCELLED
				job.GetStatus().Message = "verified work requests are cancelled on all worker nodes"
			}
		}

		rstClient, ok := m.workerManager.RemoteStorageTargets[job.Request.GetRemoteStorageTarget()]
		if !ok {
			if forceUpdate {
				job.GetStatus().State = beeremote.Job_CANCELLED
				job.GetStatus().Message += job.GetStatus().Message + "; unable to request the RST abort this job because the specified RST no longer exists (ignoring because this is a forced update)"
				return true, true, ""
			}
			job.GetStatus().State = beeremote.Job_FAILED
			job.GetStatus().Message += job.GetStatus().Message + "; unable to request the RST abort this job because the specified RST no longer exists (add it back or force the update to cancel the job anyway)"
			return false, false, ""
		}

		err := job.Complete(m.ctx, rstClient, true)
		if err != nil {
			if forceUpdate {
				job.GetStatus().State = beeremote.Job_CANCELLED
				job.GetStatus().Message += job.GetStatus().Message + "; error requesting the RST abort this job (ignoring because this is a forced update): " + err.Error()
				return true, true, ""
			}
			job.GetStatus().State = beeremote.Job_FAILED
			job.GetStatus().Message += job.GetStatus().Message + "; error requesting the RST abort this job (try again or force the update to cancel the job anyway): " + err.Error()
			return false, false, ""
		}

		job.GetStatus().State = beeremote.Job_CANCELLED
		job.GetStatus().Message += job.GetStatus().Message + "; successfully requested the RST abort this job"
		m.log.Debug("successfully updated job", zap.Any("job", job))
		return true, true, ""
	}

	return false, false, fmt.Sprintf("unable to update job %s, new state %s is not supported", job.Id, newState)
}

// UpdateWork processes work results from worker nodes for outstanding work requests and handles
// updating the job results. Once all work requests have reached a terminal state, it handles moving
// the overall job state to a terminal state, including finishing the job by completing or
// cancelling multipart uploads (or equivalent for the specified job type). It only returns an error
// if there was an internal problem updating the results or job, otherwise the result of the update
// are reflected in the Job status and message. It may update the job status and also return an
// error for some corner cases.
func (m *Manager) UpdateWork(workResult *flex.Work) error {

	pathEntry, commitAndRelease, err := m.pathStore.GetAndLockEntry(workResult.Path)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return status.Errorf(codes.NotFound, "no jobs found for path %s", workResult.Path)
		}
		return status.Errorf(codes.Internal, "internal database error retrieving path entry (try again later): %s: %s", workResult.Path, err)
	}
	defer commitAndRelease()
	job, ok := pathEntry.Value[workResult.JobId]
	if !ok {
		return status.Errorf(codes.NotFound, "path %s exists but does not have job ID %s", workResult.Path, workResult.JobId)
	}

	// Update the results entry.
	entryToUpdate, ok := job.WorkResults[workResult.RequestId]
	if !ok {
		return status.Errorf(codes.NotFound, "received a work response for an unknown work request: %s", workResult)
	}
	entryToUpdate.WorkResult = workResult
	job.WorkResults[workResult.RequestId] = entryToUpdate

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

	// From here on out we'll modify the job state, ensure the timestamp is also updated.
	defer func() {
		job.GetStatus().Updated = timestamppb.Now()
	}()

	// This might happen if WRs could complete on some nodes, but not on other nodes. This could be
	// due to some worker nodes having an issue uploading their segments to the RST, or a user
	// cancelling the job partway through while some WRs are complete and others are still running.
	if !allSameState {
		job.GetStatus().State = beeremote.Job_UNKNOWN
		job.GetStatus().Message = "all work requests have reached a terminal state, but not all work requests are in the same state (inspect individual work requests to determine possible next steps)"
		return nil
	}

	rst, ok := m.workerManager.RemoteStorageTargets[job.Request.GetRemoteStorageTarget()]
	if !ok {
		// We shouldn't return an error here. The caller (a worker node) can't do anything about
		// this. Most likely a user updated the configuration and removed the RST so there is
		// nothing we can do unless they were to add it back.
		job.GetStatus().State = beeremote.Job_FAILED
		job.GetStatus().Message = fmt.Sprintf("unable to complete job because the RST no longer exists: %s (add it back or manually cleanup any artifacts from this job)", job.Request.GetRemoteStorageTarget())
		return nil
	}

	// If everything has the same state, the state of the entry we just updated will match every other request.
	switch entryToUpdate.Status().State {
	case flex.Work_CANCELLED:
		if err := job.Complete(m.ctx, rst, true); err != nil {
			job.GetStatus().State = beeremote.Job_FAILED
			job.GetStatus().Message = "error cancelling job: " + err.Error()
		} else {
			job.GetStatus().State = beeremote.Job_CANCELLED
			job.GetStatus().Message = "successfully cancelled job"
		}
	case flex.Work_COMPLETED:
		if err := job.Complete(m.ctx, rst, false); err != nil {
			job.GetStatus().State = beeremote.Job_FAILED
			job.GetStatus().Message = "error completing job: " + err.Error()
		} else {
			job.GetStatus().State = beeremote.Job_COMPLETED
			job.GetStatus().Message = "successfully completed job"
		}
	case flex.Work_FAILED:
		// Something that went wrong that requires user intervention. We don't know what so don't
		// try to complete or abort the request as it may make it more difficult to recover.
		job.GetStatus().State = beeremote.Job_FAILED
		job.GetStatus().Message = "job cannot continue without user intervention (see work results for details)"
	default:
		job.GetStatus().State = beeremote.Job_UNKNOWN
		job.GetStatus().Message = "all work requests have reached a terminal state, but the state is unknown (this is likely a bug and will cause unexpected behavior)"
		// We return an error here because this is an internal problem that shouldn't happen and
		// hopefully a test will catch it. Most likely some new terminal states were added and this
		// function needs to be updated.
		return fmt.Errorf("all work requests have reached a terminal state, but the state is unknown (this is likely a bug and will cause unexpected behavior): %s", entryToUpdate.Status().State)
	}
	m.log.Debug("job result", zap.Any("job", job))
	return nil
}

func (m *Manager) GetRSTConfig() ([]*flex.RemoteStorageTarget, error) {
	m.readyMu.RLock()
	defer m.readyMu.RUnlock()
	if !m.ready {
		return nil, fmt.Errorf("unable to get RST config (JobMgr is not ready)")
	}
	rstConfigs := []*flex.RemoteStorageTarget{}
	for _, client := range m.workerManager.RemoteStorageTargets {
		rstConfigs = append(rstConfigs, client.GetConfig())
	}
	return rstConfigs, nil
}

func (m *Manager) Stop() {

	m.readyMu.Lock()
	defer m.readyMu.Unlock()
	if !m.ready {
		return // Nothing to do.
	}

	m.cancel()
	m.ready = false
}
