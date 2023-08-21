package worker

import (
	"fmt"

	beegfs "github.com/thinkparq/protobuf/beegfs/go"
	beesync "github.com/thinkparq/protobuf/beesync/go"
)

// BeeSyncNode is a concrete implementation of a Worker.
type BeeSyncNode struct {
	Hostname string
	Port     int8
}

// BeeSyncNode satisfies the Worker interface.
var _ Worker = &BeeSyncNode{}

// BeeSyncRequest satisfies the WorkRequest interface. BeeSync nodes are
// responsible for handling SyncRequests.
type SyncRequest struct {
	BaseRequest
	beesync.WorkRequest
}

func (n *BeeSyncNode) Connect() error {
	return nil // TODO
}

func (n *BeeSyncNode) Send(wr WorkRequest) error {

	request, ok := wr.(*SyncRequest)
	if !ok {
		return fmt.Errorf("received an invalid request for BeeSync node type: %s", request)
	}
	// TODO: Send request the node
	return nil
}

func (n *BeeSyncNode) Recv() <-chan *beegfs.WorkResponse {
	return nil // TODO
}

func (n *BeeSyncNode) Disconnect() error {
	return nil // TODO
}
