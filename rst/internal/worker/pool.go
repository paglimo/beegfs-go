package worker

import (
	"fmt"
	"sync"
	"time"

	"github.com/thinkparq/gobee/types"
	"github.com/thinkparq/protobuf/go/flex"
)

// A Pool defines a pool of workers and methods for automatically assigning work
// requests to the least busy node in the pool and updating outstanding work
// requests.
type Pool struct {
	// What type of workers are in this pool. Pools are generally organized into
	// a map based on their NodeType.
	nodeType NodeType
	// All nodes in a particular pool should be the same underlying type,
	// otherwise when assigning work requests, nodes will reject any requests
	// they do not support.
	nodes []*Node
	// Node map should contain the same entries as nodes.
	// We use a map for quick lookup of a particular node.
	nodeMap map[string]*Node
	// Next is the index of the next worker that should be assigned a request.
	next int
	// The mutex should be locked when interacting with the pool.
	mu *sync.Mutex
}

func (p *Pool) HandleAll(wg *sync.WaitGroup) {
	for _, node := range p.nodes {
		go node.Handle(wg)
	}
}

func (p *Pool) StopAll() {
	for _, node := range p.nodes {
		go node.Stop()
	}
}

// assignToLeastBusyWorker assigns the work request to the least busy node in
// the pool. It returns the ID of the assigned node and the response, or an error.
func (p *Pool) assignToLeastBusyWorker(wr WorkRequest) (string, *flex.WorkResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	poolSize := len(p.nodes)

	if poolSize == 0 {
		return "", nil, fmt.Errorf("unable to assign work request to the %s node pool: %w", p.nodeType, ErrNoWorkersInPool)
	}

	var multiErr types.MultiError

	// If no workers are connected we'll wait a bit then retry.
	// When first starting it can take time for all nodes to connect.
	for i := 0; i <= 3; i++ {
		// Don't retry more times than the number of workers in the pool. It could
		// be all workers are disconnected.
		for i := 0; i < poolSize; i++ {
			if p.nodes[p.next].GetState() == CONNECTED {
				assignedWorker := p.nodes[p.next].ID
				resp, err := p.nodes[p.next].worker.SubmitWorkRequest(wr)

				if err != nil {
					errWithWorker := fmt.Errorf("node: %s - error: %w", assignedWorker, err)
					multiErr.Errors = append(multiErr.Errors, errWithWorker)
					// If that worker is disconnected or there was an error
					// sending try the next.
					p.next = (p.next + 1) % poolSize
					continue
				}

				// TODO: https://github.com/ThinkParQ/bee-remote/issues/7.
				// Implement a more advanced mechanism to get the least busy worker
				// in the pool. For now we'll just assign work requests round robin
				// so just advance the next cursor wrapping around if needed.
				// However this will usually lead to imbalanced utilization as work
				// requests are expected to take varying times to complete.
				//
				// Ideally move to a weighted system that takes into consideration
				// the size of the work request.
				p.next = (p.next + 1) % poolSize
				return assignedWorker, resp, nil
			}

		}
		// If no workers are connected sleep and retry.
		time.Sleep(1 * time.Second)
	}

	if len(multiErr.Errors) > 0 {
		return "", nil, fmt.Errorf("unable to assign to the %s pool: %w (%s)", p.nodeType, ErrFromAllWorkers, &multiErr)
	}

	return "", nil, fmt.Errorf("unable to assign to the %s pool: %w", p.nodeType, ErrNoWorkersConnected)
}

// updateWorkRequest on node takes a jobID and a work result representing a
// single outstanding work request for the job and attempts set a new state on
// the remote worker node. It returns the work response from the remote node or
// an error if the node was unable to apply the new state or a network/local error
// occurred preventing the remote node form being updated.
func (p *Pool) updateWorkRequestOnNode(jobID string, workResult WorkResult, newState flex.NewState) (*flex.WorkResponse, error) {

	p.mu.Lock()
	defer p.mu.Unlock()

	// TODO: https://github.com/ThinkParQ/bee-remote/issues/7.
	// If the work request was cancelled then account for this
	// once we have a weighting system to determine how new
	// work requests are assigned out.

	node, ok := p.nodeMap[workResult.AssignedNode]
	if !ok {
		return nil, ErrWorkerNotInPool
	}

	updateRequest := &flex.UpdateWorkRequest{
		JobID:     jobID,
		RequestID: workResult.RequestID,
		NewState:  newState,
	}

	return node.worker.UpdateWorkRequest(updateRequest)
}
