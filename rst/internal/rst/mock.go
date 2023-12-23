package rst

import "github.com/stretchr/testify/mock"

// MockClient can be used to mock the behavior of any real client type by
// specifying the client type when setting up the expectations. For example:
//
// rstClient := &rst.MockClient{}
// mockClient.On("RecommendedSegments", fileSize).Return(rst.S3, segmentCount, partsPerSegment)
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

func (rst *MockClient) CreateUpload() (uploadID string, err error) {
	args := rst.Called()
	return args.String(0), args.Error(1)
}

func (rst *MockClient) FinishUpload() error {
	args := rst.Called()
	return args.Error(1)
}
