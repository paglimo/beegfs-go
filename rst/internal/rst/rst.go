package rst

import (
	"fmt"

	"github.com/thinkparq/protobuf/go/flex"
)

type Type string

const (
	S3      Type = "s3"
	MockRST Type = "mockrst"
)

type Client interface {
	CreateUpload() (uploadID string, err error)
	FinishUpload() error
	RecommendedSegments(fileSize int64) (segType Type, numberOfSegments int64, partsPerSegment int32)
}

func New(config *flex.RemoteStorageTarget) (Client, error) {

	switch config.Type {
	case &flex.RemoteStorageTarget_S3_{}:
		return newS3(config)
	default:
		// Note we don't currently use New() to setup Mock RSTs. We could add
		// this if it would be helpful, but its just as easy to directly declare
		// an instance and setup expectations (see mock.go).
		return nil, fmt.Errorf("unknown/unsupported RST type: %s", config.Type)
	}
}
