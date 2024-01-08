package worker

import (
	"fmt"

	"github.com/thinkparq/protobuf/go/beesync"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc"
)

type BeeSyncNode struct {
	*baseNode
	conn   *grpc.ClientConn
	client beesync.BeeSyncClient
}

var _ Worker = &BeeSyncNode{}
var _ grpcClientHandler = &BeeSyncNode{}

func newBeeSyncNode(baseNode *baseNode) Worker {

	beeSyncNode := &BeeSyncNode{baseNode: baseNode}
	beeSyncNode.baseNode.grpcClientHandler = beeSyncNode
	return beeSyncNode
}

func (n *BeeSyncNode) connect(config *flex.WorkerNodeConfigRequest, wrUpdates *flex.UpdateWorkRequests) (bool, error) {
	var err error
	n.conn, err = getGRPCClientConnection(n.config)
	if err != nil {
		return false, err
	}

	n.client = beesync.NewBeeSyncClient(n.conn)

	configureResp, err := n.client.UpdateConfig(n.rpcCtx, config)
	if err != nil {
		return true, err
	}

	// If we could send the message but the node didn't update the configuration
	// correctly probably we can't recover with a simple retry so consider fatal.
	if configureResp.Result != flex.WorkerNodeConfigResponse_SUCCESS {
		return false, fmt.Errorf("%s configure update on node with message %s", configureResp.Result, configureResp.Message)
	}

	updateWRResp, err := n.client.UpdateWorkRequests(n.rpcCtx, wrUpdates)
	if err != nil {
		return true, err
	}

	// If we could send the message but the node couldn't update the WRs,
	// probably we can't recover with a simply retry so consider fatal.
	if !updateWRResp.Success {
		return false, fmt.Errorf("updating work requests on node failed with message %s", updateWRResp.Message)
	}

	return false, nil
}

func (n *BeeSyncNode) disconnect() error {

	if n.conn != nil {
		if err := n.conn.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (n *BeeSyncNode) SubmitWorkRequest(wr *flex.WorkRequest) (*flex.WorkResponse, error) {
	n.rpcWG.Add(1)
	defer n.rpcWG.Done()
	if n.GetState() != ONLINE {
		return nil, fmt.Errorf("unable to submit work request to an offline node")
	}

	resp, err := n.client.SubmitWorkRequest(n.rpcCtx, wr)
	if err != nil {
		select {
		case n.rpcErr <- struct{}{}:
		default:
		}
		return nil, err
	}
	return resp, nil
}

func (n *BeeSyncNode) UpdateWorkRequest(updateRequest *flex.UpdateWorkRequest) (*flex.WorkResponse, error) {
	n.rpcWG.Add(1)
	defer n.rpcWG.Done()
	if n.GetState() != ONLINE {
		return nil, fmt.Errorf("unable to submit work request to an offline node")
	}

	resp, err := n.client.UpdateWorkRequest(n.rpcCtx, updateRequest)
	if err != nil {
		select {
		case n.rpcErr <- struct{}{}:
		default:
		}
		return nil, err
	}
	return resp, nil
}
