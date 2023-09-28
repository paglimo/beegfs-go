package worker

import (
	"context"
	"fmt"
	"path"
	"reflect"
	"sync"

	"github.com/thinkparq/gobee/types"
	beegfs "github.com/thinkparq/protobuf/beegfs/go"
	"go.uber.org/zap"
)

// The WorkerManager handles mapping WorkRequests to the appropriate node type.
type Manager struct {
	log    *zap.Logger
	ctx    context.Context
	cancel context.CancelFunc
	// nodePools allows us to define a different pool for each type of worker
	// node. Currently we only support one Pool per NodeType, however in the
	// future nodePools could be modified to support multiple Pools for each
	// NodeType and the Pool struct extended to include additional selection
	// criteria.
	nodePools map[NodeType]*Pool
	// jobSubmissions allows external callers to submit a job and associated work requests.
	jobSubmissions <-chan JobSubmission
	// jobResults is where the results of each job submission will be returned.
	jobResults chan<- JobResult
	// workStore is the store where the entries for jobs with work requests
	// currently being handled by WorkerMgr are kept.
	workStore *workStore
	// errChan allows the manager to return an unrecoverable error for upstream
	// handling. It should typically only be used if an unrecoverable error
	// occurs when first starting the manager, for example if the database is
	// not accessible.
	errChan chan<- error
}

// JobSubmission is used to submit a Job and its associated work requests to be
// executed on one or more workers. There are a few reasons we don't submit work
// requests directly: (1) we only have to do a single database update when
// tracking what workers are handling each request in a particular job, and (2)
// we can ensure requests in a particular job can be assigned out at once,
// potentially allowing smaller jobs (as determined by number of requests) to be
// ordered ahead of larger jobs.
type JobSubmission struct {
	JobID        string
	WorkRequests []WorkRequest
}

// JobResult is used to return the results of the work requests for a particular job.
type JobResult struct {
	JobID       string
	WorkResults []WorkResult
}

func NewManager(log *zap.Logger, errCh chan<- error, config []Config) (*Manager, chan<- JobSubmission, <-chan JobResult) {
	log = log.With(zap.String("component", path.Base(reflect.TypeOf(Manager{}).PkgPath())))
	ctx, cancel := context.WithCancel(context.Background())

	jobSubmissionsChan := make(chan JobSubmission)
	jobResultsChan := make(chan JobResult)

	nodePools := make(map[NodeType]*Pool, 0)
	workers, err := newWorkerNodesFromConfig(config)
	if err != nil {
		log.Warn("encountered one or more errors configuring workers", zap.Error(err))
	}

	for _, worker := range workers {
		if _, ok := nodePools[worker.GetNodeType()]; !ok {
			nodePools[worker.GetNodeType()] = &Pool{
				nodeType: worker.GetNodeType(),
				nodes:    make([]*Node, 0),
				next:     0,
				mu:       new(sync.Mutex),
			}
		}
		nodePools[worker.GetNodeType()].nodes = append(nodePools[worker.GetNodeType()].nodes, worker)
	}

	return &Manager{
		log:            log,
		ctx:            ctx,
		cancel:         cancel,
		nodePools:      nodePools,
		jobSubmissions: jobSubmissionsChan,
		jobResults:     jobResultsChan,
		errChan:        errCh,
	}, jobSubmissionsChan, jobResultsChan
}

func (m *Manager) Manage() {

	// TODO: Remove once we allow dynamic configuration updates since it is okay
	// if we startup with bad configuration (it can be fixed later).
	if len(m.nodePools) == 0 {
		m.errChan <- fmt.Errorf("no valid workers could be configured")
		return
	}

	// TODO: Allow the path to the workResults.db to be user configurable.
	// We initialize the work store in Manage() so we can ensure the DB is
	// closed properly when shutting down.
	workStore, closeWorkStore, err := newWorkStore("/tmp/workResults.db")
	if err != nil {
		m.errChan <- fmt.Errorf("unable to setup work store: %s", err)
		return
	}
	defer closeWorkStore()
	m.workStore = workStore

	// TODO: https://github.com/ThinkParQ/bee-remote/issues/7. Use a pool of
	// goroutines to accept jobSubmissions and receive responses from worker
	// nodes.
	for {
		select {
		case <-m.ctx.Done():
			m.log.Info("shutting down because the app is shutting down")
			return
		case js := <-m.jobSubmissions:

			entry, commitAndReleaseEntry, err := m.workStore.createAndLockEntry(js.JobID)
			if err != nil {
				// TODO: Consider how we want to handle updates to existing jobs.
				// Ideally we allow new/updated job submissions to come through the
				// same channel. Then we can get rid of the error checking if the
				// job was already submitted and just handle the request depending
				// if this is a new/existing job. Alternatively it may be simpler to
				// have one channel for new jobs and another for updated jobs
				// forcing the caller to be deliberate about what they want to do.
				//
				// For now essentially panic:
				m.errChan <- err
				continue
			}

			// Use a multi error to collect errors from all work requests.
			var multiErr types.MultiError

			// Iterate over the work requests in the job submission and attempt
			// to schedule them while simultaneously adding them to the
			// workStoreEntry.
			for _, wr := range js.WorkRequests {
				// If an error occurs return it as the message in the work response status.
				var err error
				// WorkerID will be empty if an error happens.
				workerID := ""

				pool, ok := m.nodePools[wr.getNodeType()]
				if !ok {
					multiErr.Errors = append(multiErr.Errors, fmt.Errorf("no pools available for requested node type: %s", wr.getNodeType()))
					wr.setStatus(beegfs.RequestStatus_FAILED, err.Error())
				} else {
					workerID, err = pool.assignToLeastBusyWorker(wr)
					if err != nil {
						multiErr.Errors = append(multiErr.Errors, err)
						wr.setStatus(beegfs.RequestStatus_UNASSIGNED, err.Error())
					}
				}

				fullStatus := wr.getStatus()
				entry.results[wr.getRequestID()] = WorkResult{
					RequestID:  wr.getRequestID(),
					Status:     fullStatus.Status,
					Message:    fullStatus.Message,
					AssignedTo: workerID,
				}
			}

			// If any errors happened lets check if we can retry them.
			if len(multiErr.Errors) != 0 {

				// TODO: https://github.com/ThinkParQ/bee-remote/issues/7. For
				// now treat all failures as fatal. The final implementation
				// should include logic that handles any errors that can be
				// retried by WorkerMgr, for example if there aren't any nodes
				// available in the pool yet, or a request fails on one node but
				// others are available.
				//
				// Note since we don't store the full work requests in the DB
				// (only the results) we have to retry immediately (perhaps with
				// a backoff). We can't just store the work request in the
				// workStore and try again later. This means we need to be
				// careful not to run ourselves out of memory if some extended
				// condition prevents scheduling requests because we can't just
				// keep an infinite backlog of outstanding work requests.
				// Probably the correct/easiest way to handle this is to use a
				// buffered channel and the size of the channel determines how
				// many outstanding work requests we'll allow.
				isFatal := true // Force all errors to be fatal for now.
				for _, r := range entry.results {
					if r.Status == beegfs.RequestStatus_FAILED {
						// If any requests failed just cancel any requests that did started.
						isFatal = true
					}
					// TODO: Evaluate if we need to handle any other state differently.
				}

				if isFatal {
					for _, r := range entry.results {
						if r.Status == beegfs.RequestStatus_ASSIGNED {
							// TODO: https://github.com/ThinkParQ/bee-remote/issues/7
							// Request the WR be cancelled. Probably using a new method
							// on pool that can be used to forward a request directly
							// to the specified node.
							continue
						}
					}
					// If a fatal error happened let JobMgr know immediately.
					m.jobResults <- getResults(entry)
					m.log.Error("unable to assign job", zap.Any("jobID", js.JobID), zap.Error(err))
				} else {
					m.log.Info("unable to assign all requests for job (retrying)", zap.Any("jobID", js.JobID), zap.Any("assignedWRtoNodes", entry.results))
				}
			} else {
				m.log.Info("finished assigning work requests for job", zap.Any("jobID", js.JobID), zap.Any("assignedWRtoNodes", entry.results))
			}
			// Release the lock on the workStoreEntry.
			if err = commitAndReleaseEntry(); err != nil {
				m.log.Error("unable to commit work store entry to database", zap.Error(err))
			}
		}
	}
}

func (m *Manager) Stop() {
	m.cancel()
}

// Pool defines a pool of workers and methods for automatically assigning work
// requests to the least busy node in the pool.
type Pool struct {
	// What type of workers are in this pool. Pools are generally organized into
	// a map based on their NodeType.
	nodeType NodeType
	// All nodes in a particular pool should be the same underlying type,
	// otherwise when assigning work requests, nodes will reject any requests
	// they do not support.
	nodes []*Node
	// Next is the index of the next worker that should be assigned a request.
	next int
	// The mutex should be locked when interacting with the pool.
	mu *sync.Mutex
}

// assignToLeastBusyWorker assigns the work request to the least busy node in
// the pool. It returns the ID of the assigned node, or an error.
func (p *Pool) assignToLeastBusyWorker(wr WorkRequest) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	poolSize := len(p.nodes)

	if poolSize == 0 {
		return "", fmt.Errorf("unable to assign work request to the %s node pool (no workers in pool)", p.nodeType)
	}

	// Don't retry more times than the number of workers in the pool. It could
	// be all workers are disconnected.
	for i := 0; i < poolSize; i++ {
		if p.nodes[p.next].GetState() == CONNECTED {
			assignedWorker := p.nodes[p.next].ID
			err := p.nodes[p.next].Send(wr)

			// TODO: https://github.com/ThinkParQ/bee-remote/issues/7.
			// Implement a more advanced mechanism to get the least busy worker
			// in the pool. For now we'll just assign work requests round robin
			// so just advance the next cursor wrapping around if needed.
			// However this will usually lead to imbalanced utilization as work
			// requests are expected to take varying times to complete.
			p.next = (p.next + 1) % poolSize
			if err != nil {
				// TODO: Evaluate the error message and if we should take this
				// worker out of the pool and potentially retry the send to a
				// different worker if we're 100% positive it failed.
				return "", err
			}
			return assignedWorker, nil
		}
		// If that worker is disconnected try the next.
		p.next = (p.next + 1) % poolSize
	}

	return "", fmt.Errorf("no workers are available in the %s pool (no workers are connected)", p.nodeType)
}
