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

func (w *MockNode) connect(config *flex.WorkerNodeConfigRequest, wrUpdates *flex.UpdateWorkRequests) (bool, error) {
	args := w.Called()
	return args.Bool(0), args.Error(1)
}

func (w *MockNode) disconnect() error {
	args := w.Called()
	return args.Error(1)
}

func (n *MockNode) SubmitWorkRequest(wr WorkRequest) (*flex.WorkResponse, error) {
	n.rpcWG.Add(1)
	defer n.rpcWG.Done()
	if n.GetState() != ONLINE {
		return nil, fmt.Errorf("unable to submit work request to an offline node")
	}
	args := n.Called(wr)

	if args.Error(1) != nil {
		select {
		case n.rpcErr <- struct{}{}:
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
		Status:  args.Get(0).(*flex.RequestStatus).GetStatus(),
		Message: args.Get(0).(*flex.RequestStatus).GetMessage(),
	}

	return &flex.WorkResponse{
		JobId:     wr.GetJobID(),
		RequestId: wr.GetRequestID(),
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
		case n.rpcErr <- struct{}{}:
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
		Status:  args.Get(0).(*flex.RequestStatus).GetStatus(),
		Message: args.Get(0).(*flex.RequestStatus).GetMessage(),
	}
	return &flex.WorkResponse{
		JobId:     updateRequest.GetJobID(),
		RequestId: updateRequest.GetRequestID(),
		Status:    status,
	}, args.Error(1)
}

type MockRequest struct {
	RequestID string
	Path      string
	Segment   string
	Metadata  *flex.JobMetadata
}

var _ WorkRequest = &MockRequest{}

func (wr *MockRequest) GetJobID() string {
	return wr.Metadata.GetId()
}

func (wr *MockRequest) GetRequestID() string {
	return wr.RequestID
}

func (r *MockRequest) GetStatus() flex.RequestStatus {
	return *r.Metadata.GetStatus()
}

func (r *MockRequest) SetStatus(status flex.RequestStatus_Status, message string) {

	newStatus := &flex.RequestStatus{
		Status:  status,
		Message: message,
	}

	r.Metadata.Status = newStatus
}

func (r *MockRequest) GetNodeType() Type {
	return Mock
}
