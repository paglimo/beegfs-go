package rst

import "github.com/thinkparq/protobuf/go/flex"

type S3Client struct {
	config *flex.RemoteStorageTarget
}

var _ Client = &S3Client{}

func newS3(config *flex.RemoteStorageTarget) (Client, error) {

	rst := &S3Client{
		config: config,
	}

	// TODO (current): Setup a client and also figure out where/how to tear it down (if needed).

	return rst, nil
}
func (rst *S3Client) RecommendedSegments(fileSize int64) (Type, int64, int32) {
	if rst.config.Policies.FastStartMaxSize <= fileSize {
		return S3, 1, 0
	}
	// TODO: Arbitrary selection for now. We should be smarter and take int
	// consideration file size and number of workers for this RST type.
	return S3, 4, 0
}

func (rst *S3Client) CreateUpload() (uploadID string, err error) {

	// TODO
	return "", nil
}

func (rst *S3Client) FinishUpload() error {
	// TODO
	return nil
}
