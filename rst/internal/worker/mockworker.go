package worker

import (
	"github.com/stretchr/testify/mock"
	"github.com/thinkparq/protobuf/go/flex"
)

type MockWorker struct {
	mock.Mock
}

var _ Worker = &MockWorker{}

func newMockWorker(config MockConfig) *MockWorker {
	beeTestWorker := &MockWorker{}
	for _, e := range config.Expectations {
		beeTestWorker.Mock.On(e.MethodName, e.Args...).Return(e.ReturnArgs...)
	}
	return beeTestWorker
}

func (w *MockWorker) Connect() (bool, error) {
	args := w.Called()
	return args.Bool(0), args.Error(1)
}

func (w *MockWorker) SubmitWorkRequest(wr WorkRequest) (*flex.WorkResponse, error) {
	args := w.Called(wr)

	// Echo the work response back to the caller with whatever status was set
	// using MockExpectations.
	response := &flex.WorkResponse{
		JobId:     wr.getJobID(),
		RequestId: wr.getRequestID(),
		Status:    args.Get(0).(*flex.RequestStatus),
	}

	return response, args.Error(1)
}

func (w *MockWorker) UpdateWorkRequest(updateRequest *flex.UpdateWorkRequest) (*flex.WorkResponse, error) {
	args := w.Called(updateRequest)

	return &flex.WorkResponse{
		JobId:     updateRequest.GetJobID(),
		RequestId: updateRequest.GetRequestID(),
		Status:    args.Get(0).(*flex.RequestStatus),
	}, args.Error(1)
}

// When setting up expectations the test function can provide an initialized
// channel that can then be used to simulate responses from this node.
func (w *MockWorker) NodeStream(updateRequests *flex.UpdateWorkRequests) <-chan *flex.WorkResponse {
	args := w.Called(updateRequests)
	return args.Get(0).(chan *flex.WorkResponse)
}

func (w *MockWorker) Disconnect() error {
	args := w.Called()
	return args.Error(1)
}

func (w *MockWorker) GetNodeType() NodeType {
	return Mock
}

type MockRequest struct {
	RequestID string
	Path      string
	Segment   string
	Metadata  *flex.JobMetadata
}

var _ WorkRequest = &MockRequest{}

func (wr *MockRequest) getJobID() string {
	return wr.Metadata.GetId()
}

func (wr *MockRequest) getRequestID() string {
	return wr.RequestID
}

func (r *MockRequest) getStatus() flex.RequestStatus {
	return *r.Metadata.GetStatus()
}

func (r *MockRequest) setStatus(status flex.RequestStatus_Status, message string) {

	newStatus := &flex.RequestStatus{
		Status:  status,
		Message: message,
	}

	r.Metadata.Status = newStatus
}

func (r *MockRequest) getNodeType() NodeType {
	return Mock
}
