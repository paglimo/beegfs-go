package worker

import (
	"fmt"

	"github.com/stretchr/testify/mock"
	"github.com/thinkparq/protobuf/go/flex"
)

type MockNode struct {
	*baseNode
	mock.Mock
}

var _ Worker = &MockNode{}
var _ grpcClientHandler = &MockNode{}

func newMockNode(baseNode *baseNode) Worker {
	mockNode := &MockNode{baseNode: baseNode}
	mockNode.baseNode.grpcClientHandler = mockNode
	for _, e := range baseNode.config.MockConfig.Expectations {
		mockNode.Mock.On(e.MethodName, e.Args...).Return(e.ReturnArgs...)
	}
	return mockNode
}

func (n *MockNode) connect(config *flex.WorkerNodeConfigRequest, wrUpdates *flex.UpdateWorkRequests) (bool, error) {
	args := n.Called()
	return args.Bool(0), args.Error(1)
}

func (n *MockNode) heartbeat(request *flex.HeartbeatRequest) (*flex.HeartbeatResponse, error) {
	args := n.Called()
	return args.Get(0).(*flex.HeartbeatResponse), args.Error(1)
}

func (n *MockNode) disconnect() error {
	args := n.Called()
	return args.Error(1)
}

func (n *MockNode) SubmitWorkRequest(wr *flex.WorkRequest) (*flex.WorkResponse, error) {
	n.rpcWG.Add(1)
	defer n.rpcWG.Done()
	if n.GetState() != ONLINE {
		return nil, fmt.Errorf("unable to submit work request to an offline node")
	}
	args := n.Called(wr)

	if args.Error(1) != nil {
		select {
		case n.rpcErr <- args.Error(1):
		default:
		}
		return nil, args.Error(1)
	}

	// Echo the work response back to the caller with whatever status was set
	// using MockExpectations. It is very very important we copy the status here
	// and return a pointer to a new status (not reuse the status from the
	// expectation), otherwise all test requests will share the same status
	// which causes very confusing test failures.
	status := &flex.RequestStatus{
		State:   args.Get(0).(*flex.RequestStatus).GetState(),
		Message: args.Get(0).(*flex.RequestStatus).GetMessage(),
	}

	return &flex.WorkResponse{
		JobId:     wr.GetJobId(),
		RequestId: wr.GetRequestId(),
		Status:    status,
	}, args.Error(1)
}

func (n *MockNode) UpdateWorkRequest(updateRequest *flex.UpdateWorkRequest) (*flex.WorkResponse, error) {
	n.rpcWG.Add(1)
	defer n.rpcWG.Done()
	if n.GetState() != ONLINE {
		return nil, fmt.Errorf("unable to submit work request to an offline node")
	}
	args := n.Called(updateRequest)

	if args.Error(1) != nil {
		select {
		case n.rpcErr <- args.Error(1):
		default:
		}
		return nil, args.Error(1)
	}

	// Echo the work response back to the caller with whatever status was set
	// using MockExpectations. It is very very important we copy the status here
	// and return a pointer to a new status (not reuse the status from the
	// expectation), otherwise all test requests will share the same status
	// which causes very confusing test failures.
	status := &flex.RequestStatus{
		State:   args.Get(0).(*flex.RequestStatus).GetState(),
		Message: args.Get(0).(*flex.RequestStatus).GetMessage(),
	}
	return &flex.WorkResponse{
		JobId:     updateRequest.GetJobId(),
		RequestId: updateRequest.GetRequestId(),
		Status:    status,
	}, args.Error(1)
}
