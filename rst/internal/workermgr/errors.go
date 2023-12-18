package workermgr

import "errors"

var (
	ErrNoWorkersConnected = errors.New("no workers are connected")
	ErrFromAllWorkers     = errors.New("all workers returned errors")
	ErrNoWorkersInPool    = errors.New("no workers in pool")
	ErrNoPoolsForNodeType = errors.New("no pools available for the requested node type")
	ErrWorkerNotInPool    = errors.New("pool for the requested node type does not contain a worker with the specified ID")
)
