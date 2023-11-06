package worker

import (
	"fmt"
	"path"
	"reflect"
	"sync"

	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
)

type ManagerConfig struct {
}

// The WorkerManager handles mapping WorkRequests to the appropriate node type.
type Manager struct {
	log *zap.Logger
	// The wait group is incremented for each node that is being managed.
	// This is how we ensure all nodes are disconnected before shutting down.
	nodeWG *sync.WaitGroup
	// nodePools allows us to define a different pool for each type of worker
	// node. Currently we only support one Pool per NodeType, however in the
	// future nodePools could be modified to support multiple Pools for each
	// NodeType and the Pool struct extended to include additional selection
	// criteria.
	nodePools map[NodeType]*Pool
	// WorkResponses is where individual results from each worker node are sent.
	WorkResponses <-chan *flex.WorkResponse
	config        ManagerConfig
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

type JobUpdate struct {
	JobID       string
	WorkResults map[string]WorkResult
	NewState    flex.NewState
}

func NewManager(log *zap.Logger, managerConfig ManagerConfig, workerConfigs []Config) *Manager {
	log = log.With(zap.String("component", path.Base(reflect.TypeOf(Manager{}).PkgPath())))

	workResponsesChan := make(chan *flex.WorkResponse)

	nodePools := make(map[NodeType]*Pool, 0)
	nodes, err := newWorkerNodesFromConfig(log, workResponsesChan, workerConfigs)
	if err != nil {
		log.Warn("encountered one or more errors configuring workers", zap.Error(err))
	}

	for _, node := range nodes {
		if _, ok := nodePools[node.worker.GetNodeType()]; !ok {
			nodePools[node.worker.GetNodeType()] = &Pool{
				nodeType: node.worker.GetNodeType(),
				nodes:    make([]*Node, 0),
				nodeMap:  map[string]*Node{},
				next:     0,
				mu:       new(sync.Mutex),
			}
		}
		nodePools[node.worker.GetNodeType()].nodeMap[node.ID] = node
		nodePools[node.worker.GetNodeType()].nodes = append(nodePools[node.worker.GetNodeType()].nodes, node)
	}

	workerManager := &Manager{
		log:           log,
		nodeWG:        new(sync.WaitGroup),
		nodePools:     nodePools,
		WorkResponses: workResponsesChan,
		config:        managerConfig,
	}

	return workerManager
}

func (m *Manager) Manage() error {
	// TODO: Remove once we allow dynamic configuration updates since it is okay
	// if we startup with bad configuration (it can be fixed later).
	if len(m.nodePools) == 0 {
		return fmt.Errorf("no valid workers could be configured")
	}
	// Bring all node pools online:
	for _, pool := range m.nodePools {
		pool.HandleAll(m.nodeWG)
	}
	return nil
}

// SubmitJob is used to execute the work requests for a job across one or more
// worker nodes. It returns a map of the work results and a bool indicating if
// any of the work results were unable to be scheduled.
func (m *Manager) SubmitJob(js JobSubmission) (map[string]WorkResult, bool) {

	workResults := make(map[string]WorkResult)
	// If we're unable to schedule any of the work requests this is set to false
	// so we know to cancel and WRs that were scheduled.
	allScheduled := true

	// Iterate over the work requests in the job submission and attempt
	// to schedule them while simultaneously adding them to the work results.
	for _, workRequest := range js.WorkRequests {
		// WorkerID will be empty if an error happens.
		workerID := ""

		// If an error occurs return it as the message in the work response status.
		var err error

		pool, ok := m.nodePools[workRequest.getNodeType()]
		if !ok {
			err := fmt.Errorf("%s: %w", workRequest.getNodeType(), ErrNoPoolsForNodeType)
			workRequest.setStatus(flex.RequestStatus_FAILED, err.Error())
			allScheduled = false
		} else {
			var workResponse *flex.WorkResponse
			workerID, workResponse, err = pool.assignToLeastBusyWorker(workRequest)
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
			// jobResultStore and try again later. This means we need to be
			// careful not to run ourselves out of memory if some extended
			// condition prevents scheduling requests because we can't just
			// keep an infinite backlog of outstanding work requests.
			// Probably the correct/easiest way to handle this is to use a
			// buffered channel and the size of the channel determines how
			// many outstanding work requests we'll allow.
			if err != nil {
				workRequest.setStatus(flex.RequestStatus_UNASSIGNED, err.Error())
				allScheduled = false
			} else {
				workRequest.setStatus(workResponse.Status.GetStatus(), workResponse.GetStatus().Message)
				if workResponse.Status.Status != flex.RequestStatus_SCHEDULED {
					allScheduled = false
				}
			}
		}

		workResult := WorkResult{
			RequestID:    workRequest.getRequestID(),
			Status:       workRequest.getStatus().Status,
			Message:      workRequest.getStatus().Message,
			AssignedNode: workerID,
			AssignedPool: workRequest.getNodeType(),
		}
		workResults[workResult.RequestID] = workResult
	}

	if !allScheduled {
		m.log.Warn("unable to assign all work requests for job (attempting to cancel)", zap.Any("jobID", js.JobID), zap.Any("workResults", workResults))
		jobUpdate := JobUpdate{
			JobID:       js.JobID,
			WorkResults: workResults,
			NewState:    flex.NewState_CANCEL,
		}
		var allCancelled bool
		workResults, allCancelled = m.UpdateJob(jobUpdate)
		if !allCancelled {
			m.log.Warn("unable to cancel some outstanding work requests", zap.Any("jobID", js.JobID), zap.Any("workResults", workResults))
		}
	} else {
		m.log.Debug("finished assigning work requests for job", zap.Any("jobID", js.JobID), zap.Any("workResults", workResults))
	}
	return workResults, allScheduled
}

// UpdateJob takes a jobUpdate containing work results for outstanding work
// requests and a new state. It attempts to communicate with the worker node
// running the work requests to set the new state and returns the updated
// WorkResults and a bool indicating if all work requests were updated to the
// requested state (true) or false if any updates failed or resulted in a
// different state than what was requested.
func (m *Manager) UpdateJob(jobUpdate JobUpdate) (map[string]WorkResult, bool) {

	newResults := make(map[string]WorkResult)

	// If we're unable to definitively update the state if the work request on
	// any node to the requested state, allUpdated is set to false.
	allUpdated := true

	for _, workResult := range jobUpdate.WorkResults {

		// If the WR was never assigned we can just cancel it.
		if workResult.AssignedPool == "" || workResult.AssignedNode == "" {
			workResult.Status = flex.RequestStatus_CANCELLED
			newResults[workResult.RequestID] = workResult
			continue
		}

		pool, ok := m.nodePools[workResult.AssignedPool]
		if !ok {
			workResult.Status = flex.RequestStatus_UNKNOWN
			workResult.Message = ErrNoPoolsForNodeType.Error()
			newResults[workResult.RequestID] = workResult
			allUpdated = false
			newResults[workResult.RequestID] = workResult
			continue
		}

		resp, err := pool.updateWorkRequestOnNode(jobUpdate.JobID, workResult, jobUpdate.NewState)
		if err != nil {
			workResult.Status = flex.RequestStatus_UNKNOWN
			workResult.Message = err.Error()
			newResults[workResult.RequestID] = workResult
			allUpdated = false
			newResults[workResult.RequestID] = workResult
			continue
		}

		workResult.Status = resp.Status.Status
		workResult.Message = resp.Status.GetMessage()

		if jobUpdate.NewState == flex.NewState_CANCEL && workResult.Status != flex.RequestStatus_CANCELLED {
			allUpdated = false
		}

		newResults[workResult.RequestID] = workResult
	}
	return newResults, allUpdated
}

func (m *Manager) Stop() {
	// Disconnect all nodes before we stop the Manage() loop.
	// This ensures we can finish writing work requests/results to the DB.
	for _, pool := range m.nodePools {
		pool.StopAll()
	}
}
