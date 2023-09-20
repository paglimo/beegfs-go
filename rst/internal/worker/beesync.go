package worker

import (
	"fmt"

	beegfs "github.com/thinkparq/protobuf/beegfs/go"
	bs "github.com/thinkparq/protobuf/beesync/go"
)

// BeeSyncNode is a concrete implementation of a Worker.
type BeeSyncNode struct {
	BeeSyncConfig
}

// Verify BeeSyncNode satisfies the Interface.
var _ Interface = &BeeSyncNode{}

func newBeeSyncNode(config BeeSyncConfig) *BeeSyncNode {
	return &BeeSyncNode{
		BeeSyncConfig: config,
	}
}

func (n *BeeSyncNode) Connect() error {
	return nil // TODO
}

func (n *BeeSyncNode) Send(wr WorkRequest) error {

	request, ok := wr.(*SyncRequest)
	if !ok {
		return fmt.Errorf("received an invalid request for BeeSync node type: %s", request)
	}
	// TODO: Actually send the request to the node.
	fmt.Printf("sent request ID %s for job ID %s to %s:%d\n", wr.getRequestID(), wr.getJobID(), n.Hostname, n.Port)
	return nil
}

func (n *BeeSyncNode) Recv() <-chan *beegfs.WorkResponse {
	return nil // TODO
}

func (n *BeeSyncNode) Disconnect() error {
	return nil // TODO
}

func (n *BeeSyncNode) GetNodeType() NodeType {
	return BeeSync
}

// SyncRequests are handled by BeeSync nodes.
type SyncRequest struct {
	*bs.SyncRequest
}

// SyncRequest satisfies the WorkRequest interface.
var _ WorkRequest = &SyncRequest{}

func (r *SyncRequest) getJobID() string {
	return r.Metadata.GetId()
}

func (r *SyncRequest) getRequestID() string {
	return r.GetRequestId()
}

func (r *SyncRequest) getStatus() beegfs.RequestStatus {
	return *r.Metadata.GetStatus()
}

func (r *SyncRequest) getNodeType() NodeType {
	return BeeSync
}
