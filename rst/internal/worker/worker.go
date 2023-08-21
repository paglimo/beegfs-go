package worker

import (
	beegfs "github.com/thinkparq/protobuf/beegfs/go"
)

// Supported worker nodes are organized into pools based on their NodeType. In
// addition to implementing the Worker interface, expand the list of constants
// below when adding new worker node types.
type NodeType string

const (
	BeeSync NodeType = "beesync"
)

// All worker nodes must implement the Worker interface.
type Worker interface {
	Connect() error
	Send(WorkRequest) error
	Recv() <-chan *beegfs.WorkResponse
	Disconnect() error
}

// WorkRequest represents an interface for work requests that can be processed
// by different types of worker nodes.
type WorkRequest interface {
	// getNodeType() is implemented by BaseWR.
	getNodeType() NodeType
}

// BaseRequest is the base work request all work request types must embed.
type BaseRequest struct {
	NodeType
}

func (r *BaseRequest) getNodeType() NodeType {
	return r.NodeType
}
