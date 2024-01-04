package workermgr

import (
	"fmt"
	"path"
	"reflect"
	"sync"

	"github.com/thinkparq/bee-remote/internal/rst"
	"github.com/thinkparq/bee-remote/internal/worker"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
)

// Configuration that should apply to all nodes.
type Config struct {
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
	nodePools map[worker.Type]*Pool
	config    Config
	// WorkerManager maintains the list of RSTs because it is responsible for
	// keeping the configuration on all worker nodes in sync. This is exported
	// so other components like JobMgr can also reference it as needed.
	// TODO: Allow RST configuration to be dynamically updated. This is complicated
	// because we'll have to add locking and figure out how to handle when there
	// are existing jobs for a changed/removed RST.
	RemoteStorageTargets map[string]rst.Client
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
	WorkRequests []worker.WorkRequest
}

type JobUpdate struct {
	JobID       string
	WorkResults map[string]worker.WorkResult
	NewState    flex.NewState
}

func NewManager(log *zap.Logger, managerConfig Config, workerConfigs []worker.Config, rstConfigs []*flex.RemoteStorageTarget) (*Manager, error) {
	log = log.With(zap.String("component", path.Base(reflect.TypeOf(Manager{}).PkgPath())))

	rstMap := make(map[string]rst.Client)
	for _, config := range rstConfigs {
		rst, err := rst.New(config)
		if err != nil {
			return nil, fmt.Errorf("encountered an error setting up remote storage target: %w", err)
		}
		rstMap[config.Id] = rst
	}

	nodePools := make(map[worker.Type]*Pool, 0)
	nodes, err := worker.NewWorkerNodesFromConfig(log, workerConfigs)
	if err != nil {
		log.Warn("encountered one or more errors configuring workers", zap.Error(err))
	}

	for _, n := range nodes {
		if _, ok := nodePools[n.GetNodeType()]; !ok {
			nodePools[n.GetNodeType()] = &Pool{
				nodeType: n.GetNodeType(),
				nodes:    make([]worker.Worker, 0),
				nodeMap:  map[string]worker.Worker{},
				next:     0,
				mu:       new(sync.Mutex),
				// TODO: If/when we allow dynamic configuration this won't work.
				// We would need to pass a reference to the actual RST clients
				// and provide methods to get their configuration.
				workerConfig: &flex.WorkerNodeConfigRequest{Rsts: rstConfigs},
			}
		}
		nodePools[n.GetNodeType()].nodeMap[n.GetID()] = n
		nodePools[n.GetNodeType()].nodes = append(nodePools[n.GetNodeType()].nodes, n)
	}

	workerManager := &Manager{
		log:                  log,
		nodeWG:               new(sync.WaitGroup),
		nodePools:            nodePools,
		config:               managerConfig,
		RemoteStorageTargets: rstMap,
	}

	return workerManager, nil
}

func (m *Manager) Start() error {
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

// SubmitJob executes the work requests for a job across one or more worker
// nodes. If any requests can not be scheduled, it attempts to cancel all
// requests. It returns a map of individual work results and and the overall
// status of the job submission. If all requests have the same state, that is
// the overall status. Otherwise the overall status is failed if one or more
// requests cannot be cancelled after an initial failure.
func (m *Manager) SubmitJob(js JobSubmission) (map[string]worker.WorkResult, *flex.RequestStatus) {

	workResults := make(map[string]worker.WorkResult)
	// If we're unable to schedule any of the work requests this is set to false
	// so we know to cancel any WRs that were scheduled.
	allScheduled := true

	// Iterate over the work requests in the job submission and attempt
	// to schedule them while simultaneously adding them to the work results.
	for _, workRequest := range js.WorkRequests {
		// WorkerID will be empty if an error happens.
		workerID := ""

		var workResult worker.WorkResult
		// If an error occurs return it as the message in the work response status.
		var err error

		pool, ok := m.nodePools[workRequest.GetNodeType()]
		if !ok {
			err := fmt.Errorf("%s: %w", workRequest.GetNodeType(), ErrNoPoolsForNodeType)
			workRequest.SetStatus(flex.RequestStatus_FAILED, err.Error())
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
				// If there was a failure assemble a minimal work response.
				allScheduled = false
				workResult.WorkResponse = &flex.WorkResponse{
					JobId:     workRequest.GetJobID(),
					RequestId: workRequest.GetRequestID(),
					Status: &flex.RequestStatus{
						State:   flex.RequestStatus_UNASSIGNED,
						Message: err.Error(),
					},
				}
			} else {
				if workResponse.Status.State != flex.RequestStatus_SCHEDULED {
					allScheduled = false
				}
				workResult.WorkResponse = workResponse
			}
		}

		workResult.AssignedNode = workerID
		workResult.AssignedPool = workRequest.GetNodeType()
		workResults[workResult.WorkResponse.RequestId] = workResult
	}

	var status flex.RequestStatus

	if !allScheduled {
		status.State = flex.RequestStatus_CANCELLED
		status.Message = "cancelled because one or more work requests could not be scheduled"

		jobUpdate := JobUpdate{
			JobID:       js.JobID,
			WorkResults: workResults,
			NewState:    flex.NewState_CANCEL,
		}
		var allCancelled bool
		workResults, allCancelled = m.UpdateJob(jobUpdate)
		if !allCancelled {
			status.State = flex.RequestStatus_FAILED
			status.Message = "failed because one or more work requests could not be cancelled after initial scheduling failure"
		}
	} else {
		status.State = flex.RequestStatus_SCHEDULED
		status.Message = "finished scheduling work requests"
	}
	return workResults, &status
}

// UpdateJob takes a jobUpdate containing work results for outstanding work
// requests and a new state. It attempts to communicate with the worker node
// running the work requests to set the new state and returns the updated
// WorkResults and a bool indicating if all work requests were updated to the
// requested state (true) or false if any updates failed or resulted in a
// different state than what was requested. Historical messages for each
// work result will be retained separated by a semicolon for troubleshooting.
// For example if initially a scheduling failure occurred but then the job
// was cancelled, the message from the initial failure and result of the
// cancellation request may be required to understand what happened.
//
// TODO: Support job updates besides just cancellations.
func (m *Manager) UpdateJob(jobUpdate JobUpdate) (map[string]worker.WorkResult, bool) {

	newResults := make(map[string]worker.WorkResult)

	// If we're unable to definitively update the state if the work request on
	// any node to the requested state, allUpdated is set to false.
	allUpdated := true

	for reqID, workResult := range jobUpdate.WorkResults {

		// If the WR was never assigned we can just cancel it.
		if workResult.AssignedPool == "" || workResult.AssignedNode == "" {
			workResult.Status().State = flex.RequestStatus_CANCELLED
			workResult.Status().Message = workResult.Status().Message + "; cancelling because the request is not assigned to a pool or node"
			newResults[reqID] = workResult
			continue
		}

		pool, ok := m.nodePools[workResult.AssignedPool]
		if !ok {
			workResult.Status().State = flex.RequestStatus_UNKNOWN
			workResult.Status().Message = workResult.Status().Message + "; " + ErrNoPoolsForNodeType.Error()
			newResults[reqID] = workResult
			allUpdated = false
			continue
		}

		resp, err := pool.updateWorkRequestOnNode(jobUpdate.JobID, workResult, jobUpdate.NewState)
		if err != nil {
			workResult.Status().State = flex.RequestStatus_UNKNOWN
			workResult.Status().Message = workResult.Status().Message + "; " + err.Error()
			newResults[reqID] = workResult
			allUpdated = false
			continue
		}

		workResult.Status().State = resp.Status.State
		workResult.Status().Message = workResult.Status().Message + "; " + resp.Status.GetMessage()

		if jobUpdate.NewState == flex.NewState_CANCEL && workResult.Status().State != flex.RequestStatus_CANCELLED {
			allUpdated = false
		}

		newResults[reqID] = workResult
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
