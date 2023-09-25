package worker

import (
	"context"
	"fmt"
	"path"
	"reflect"
	"sync"

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
	// workResponses is where the results of each work request will be returned.
	workResponses chan<- *beegfs.WorkResponse
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

func NewManager(log *zap.Logger, errCh chan<- error, config []Config) (*Manager, chan<- JobSubmission, <-chan *beegfs.WorkResponse) {
	log = log.With(zap.String("component", path.Base(reflect.TypeOf(Manager{}).PkgPath())))
	ctx, cancel := context.WithCancel(context.Background())

	requestChan := make(chan JobSubmission)
	responseChan := make(chan *beegfs.WorkResponse)

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
		jobSubmissions: requestChan,
		workResponses:  responseChan,
		errChan:        errCh,
	}, requestChan, responseChan
}

func (m *Manager) Manage() {

	// TODO: Remove once we allow dynamic configuration updates since it is okay
	// if we startup with bad configuration (it can be fixed later).
	if len(m.nodePools) == 0 {
		m.errChan <- fmt.Errorf("no valid workers could be configured")
		return
	}

	for {
		select {
		case <-m.ctx.Done():
			m.log.Info("shutting down because the app is shutting down")
			return
		case s := <-m.jobSubmissions:

			// assignedNodes maps request IDs to their assigned worker IDs.
			assignedNodes := make(map[string]string)

			for _, wr := range s.WorkRequests {

				// If an error occurs this status should be set to tell JobMgr how to react.
				status := beegfs.RequestStatus_UNKNOWN
				// If an error occurs return it as the message in the work response status.
				var err error

				pool, ok := m.nodePools[wr.getNodeType()]
				if !ok {
					status = beegfs.RequestStatus_FAILED
					err = fmt.Errorf("no pools available for requested node type: %s", wr.getNodeType())
				} else {
					var workerID string
					workerID, err = pool.assignToLeastBusyWorker(wr)
					if err != nil {
						status = beegfs.RequestStatus_UNASSIGNED
					}
					// WorkerID will be empty if an error happened.
					assignedNodes[wr.getRequestID()] = workerID
				}

				// If anything went wrong let JobMgr know immediately:
				if err != nil {
					m.log.Warn("unable to schedule work request", zap.Error(err))
					m.workResponses <- &beegfs.WorkResponse{
						JobId:     wr.getJobID(),
						RequestId: wr.getRequestID(),
						Status: &beegfs.RequestStatus{
							Status:  status,
							Message: err.Error(),
						},
					}
				}
			}

			m.log.Info("finished assigning work requests for job", zap.Any("jobID", s.JobID), zap.Any("assignedWRtoNodes", assignedNodes))
			// TODO: Add map[s.JobID]assignedNodes to the database.
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
			//
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
