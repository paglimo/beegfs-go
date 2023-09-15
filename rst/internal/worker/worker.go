package worker

import (
	beegfs "github.com/thinkparq/protobuf/beegfs/go"
)

// Supported worker nodes are organized into pools based on their NodeType. In
// addition to implementing the Worker interface, expand the list of constants
// below when adding new worker node types.
type NodeType string

const (
	Unknown NodeType = "unknown"
	BeeSync NodeType = "beesync"
)

// All worker nodes must implement the Worker interface.
type Worker interface {
	Connect() error
	Send(WorkRequest) error
	Recv() <-chan *beegfs.WorkResponse
	Disconnect() error
	GetID() string
}

// WorkRequest represents an interface for work requests that can be processed
// by different types of worker nodes.
type WorkRequest interface {
	// getJobID() returns the job id.
	getJobID() string
	// getRequestID() returns the request ID. Note request IDs are only
	// guaranteed to be unique for a particular job.
	getRequestID() string
	// getStatus() returns the status of the request.
	getStatus() beegfs.RequestStatus
	// getNodeType() is implemented by BaseWR.
	getNodeType() NodeType
}

// BaseRequest contains common fields for all work requests and must be embedded
// by all actual work request types.
type BaseRequest struct {
	NodeType
}

func (r *BaseRequest) getNodeType() NodeType {
	if r.NodeType == "" {
		return Unknown
	}
	return r.NodeType
}
