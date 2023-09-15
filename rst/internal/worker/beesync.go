package worker

import (
	"fmt"

	beegfs "github.com/thinkparq/protobuf/beegfs/go"
	bs "github.com/thinkparq/protobuf/beesync/go"
)

// BeeSyncNode is a concrete implementation of a Worker.
type BeeSyncNode struct {
	id       string
	Hostname string
	Port     int
}

// BeeSyncNode satisfies the Worker interface.
var _ Worker = &BeeSyncNode{}

func (n *BeeSyncNode) GetID() string {
	return n.id
}

func (n *BeeSyncNode) Connect() error {
	return nil // TODO
}

func (n *BeeSyncNode) Send(wr WorkRequest) error {

	request, ok := wr.(*SyncRequest)
	if !ok {
		return fmt.Errorf("received an invalid request for BeeSync node type: %s", request)
	}
	// TODO: Send actually send the request to the node.
	fmt.Printf("node ID %s handled request ID %s for job ID %s\n", n.GetID(), wr.getRequestID(), wr.getJobID())
	return nil
}

func (n *BeeSyncNode) Recv() <-chan *beegfs.WorkResponse {
	return nil // TODO
}

func (n *BeeSyncNode) Disconnect() error {
	return nil // TODO
}

// SyncRequests are handled by BeeSync nodes.
type SyncRequest struct {
	BaseRequest
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
