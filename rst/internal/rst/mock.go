package rst

import (
	"github.com/stretchr/testify/mock"
	"github.com/thinkparq/protobuf/go/flex"
)

// MockClient can be used to mock the behavior of any real client type.
//
// To test directly (for example the RST package tests):
// rstClient := &rst.MockClient{}
// mockClient.On("RecommendedSegments",fileSize).Return(rst.S3, segmentCount, partsPerSegment)
//
// To test indirectly use the Mock RST type when initializing WorkerMgr:
// rsts := []*flex.RemoteStorageTarget{{Id: "0", Type: &flex.RemoteStorageTarget_Mock{}}}
// wm, err := workermgr.NewManager(logger, workermgr.Config{}, []worker.Config{}, rsts)
//
// Then use type assertion to get at the underlying mock client to setup expectations:
// mockClient, _ := workerManager.RemoteStorageTargets["0"].(*rst.MockClient)
// mockClient.On("RecommendedSegments", int64(1<<20)).Return(rst.S3, 4, 2)
type MockClient struct {
	mock.Mock
}

var _ Client = &MockClient{}

func (rst *MockClient) RecommendedSegments(fileSize int64) (Type, int64, int32) {
	args := rst.Called(fileSize)

	clientType, ok := args.Get(0).(Type)
	if !ok {
		panic("client type used for test is invalid")
	}

	return clientType, int64(args.Int(1)), int32(args.Int(2))
}

func (rst *MockClient) CreateUpload(path string) (uploadID string, err error) {
	args := rst.Called()
	return args.String(0), args.Error(1)
}

func (rst *MockClient) AbortUpload(uploadID string, path string) error {
	args := rst.Called()
	return args.Error(1)
}

func (rst *MockClient) FinishUpload(uploadID string, path string, parts []*flex.WorkResponse_Part) error {
	args := rst.Called()
	return args.Error(1)
}

func (rst *MockClient) UploadPart(uploadID string, part int32, path string) (string, error) {
	args := rst.Called()
	return args.String(0), args.Error(2)
}
