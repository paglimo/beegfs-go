package worker

import (
	"context"
	"path"
	"reflect"

	beegfs "github.com/thinkparq/protobuf/beegfs/go"
	"go.uber.org/zap"
)

// The WorkerManager handles mapping WorkRequests to the appropriate node type.
type Manager struct {
	log    *zap.Logger
	ctx    context.Context
	cancel context.CancelFunc
	// nodePool allows us to define a different pool for each type of worker node.
	nodePool map[NodeType]*Pool
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

func NewManager(log *zap.Logger, errCh chan<- error) (*Manager, chan<- JobSubmission, <-chan *beegfs.WorkResponse) {
	log = log.With(zap.String("component", path.Base(reflect.TypeOf(Manager{}).PkgPath())))
	ctx, cancel := context.WithCancel(context.Background())

	requestChan := make(chan JobSubmission)
	responseChan := make(chan *beegfs.WorkResponse)

	return &Manager{
		log:            log,
		ctx:            ctx,
		cancel:         cancel,
		nodePool:       make(map[NodeType]*Pool),
		jobSubmissions: requestChan,
		workResponses:  responseChan,
		errChan:        errCh}, requestChan, responseChan
}

func (m *Manager) Manage() {

	// TODO: Use Config package to setup node pools.
	beeSyncNode := BeeSyncNode{
		id:       "beesync-node-01",
		Hostname: "localhost",
		Port:     1234,
	}
	beeSyncNodePool := []Worker{&beeSyncNode}
	m.nodePool["beesync"] = &Pool{workers: beeSyncNodePool}

	for {
		select {
		case <-m.ctx.Done():
			m.log.Info("shutting down because the app is shutting down")
			return
		case s := <-m.jobSubmissions:

			// assignedNodes maps request IDs to their assigned worker IDs.
			assignedNodes := make(map[string]string)

			for _, wr := range s.WorkRequests {
				// TODO: The whole "getNodeType()" approach is not consistent
				// with how we handle interface types elsewhere. We should get
				// rid of getNodeType() (and the BaseRequest) and just add a
				// type switch here. We should still use a typed constant to
				// lookup a particular node pool once we determine the type.
				if wr.getNodeType() == Unknown {
					m.log.Error("cannot assign request (no node type specified)")
				} else {
					workerID, err := m.nodePool[wr.getNodeType()].assignToLeastBusyWorker(wr)
					if err != nil {
						// TODO: Handle the error instead of returning.
						m.log.Error("error assigning request", zap.Error(err))
					}
					assignedNodes[wr.getRequestID()] = workerID
				}
			}
			m.log.Info("finished assigning work requests for job", zap.Any("jobID", s.JobID), zap.Any("assignedNodes", assignedNodes))
			// TODO: Add map[s.JobID]assignedNodes to the database.
		}
	}
}

func (m *Manager) Stop() {
	m.cancel()
}

// Pool defines a pool of workers and methods for automatically assigning
// work requests to the least busy node in the pool.
type Pool struct {
	workers []Worker
}

// assignToLeastBusyWorker assigns the work request to the least busy node in
// the pool. It returns the ID of the assigned node, or an error.
func (p Pool) assignToLeastBusyWorker(wr WorkRequest) (string, error) {
	// TODO: Logic to get the least busy worker.
	err := p.workers[0].Send(wr)

	if err != nil {
		return "", err
	}

	return p.workers[0].GetID(), nil
}
