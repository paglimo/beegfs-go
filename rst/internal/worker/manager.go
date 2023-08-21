package worker

import (
	"context"

	beegfs "github.com/thinkparq/protobuf/beegfs/go"
)

// The WorkerManager handles mapping WorkRequests to the appropriate node type.
type Manager struct {
	// nodePool allows us to define a different pool for each type of worker node.
	nodePool  map[NodeType]Pool
	Requests  <-chan WorkRequest
	Responses chan<- *beegfs.WorkResponse
}

func (m *Manager) Manage(ctx context.Context) {

	for {
		select {
		case <-ctx.Done():
			return
		case req := <-m.Requests:
			err := m.nodePool[req.getNodeType()].assignToLeastBusyWorker(req)
			if err != nil {
				// TODO: Handle the error instead of returning.
				return
			}
		}
	}
}

// Pool defines a pool of workers and methods for automatically assigning
// work requests to the least busy node in the pool.
type Pool struct {
	workers []Worker
}

func (p Pool) assignToLeastBusyWorker(wr WorkRequest) error {
	// TODO: Logic to get the least busy worker.
	return p.workers[0].Send(wr)
}
