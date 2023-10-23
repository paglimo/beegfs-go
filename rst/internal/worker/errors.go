package worker

import "errors"

var (
	ErrNoWorkersConnected = errors.New("no workers are connected")
	ErrNoWorkersInPool    = errors.New("no workers in pool")
	ErrNoPoolsForNodeType = errors.New("no pools available for the requested node type")
)
