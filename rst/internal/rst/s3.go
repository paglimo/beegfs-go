package rst

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3Config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/thinkparq/bee-remote/internal/filesystem"
	"github.com/thinkparq/protobuf/go/flex"
)

type S3Client struct {
	config *flex.RemoteStorageTarget
	client *s3.Client
}

var _ Client = &S3Client{}

func newS3(rstConfig *flex.RemoteStorageTarget) (Client, error) {

	resolver := aws.EndpointResolverWithOptionsFunc(func(string, string, ...any) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:       rstConfig.GetS3().GetPartitionId(),
			URL:               rstConfig.GetS3().GetEndpointUrl(),
			SigningRegion:     rstConfig.GetS3().GetRegion(),
			HostnameImmutable: true,
		}, nil
	})

	cfg, err := s3Config.LoadDefaultConfig(
		context.TODO(),
		s3Config.WithRegion(rstConfig.GetS3().GetRegion()),
		s3Config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(rstConfig.GetS3().GetAccessKey(), rstConfig.GetS3().GetSecretKey(), "")),
		s3Config.WithEndpointResolverWithOptions(resolver),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to load config for RST client: %w", err)
	}

	rst := &S3Client{
		config: rstConfig,
		client: s3.NewFromConfig(cfg),
	}

	return rst, nil
}

func (rst *S3Client) GetType() Type {
	return S3
}

func (rst *S3Client) RecommendedSegments(fileSize int64) (int64, int32) {
	if rst.config.Policies.FastStartMaxSize <= fileSize {
		return 1, 0
	}
	// TODO: Arbitrary selection for now. We should be smarter and take int
	// consideration file size and number of workers for this RST type.
	return 4, 0
}

func (rst *S3Client) CreateUpload(path string) (uploadID string, err error) {

	createMultipartUploadInput := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(rst.config.GetS3().GetBucket()),
		// TODO: Transform path as needed based on policies.
		Key: aws.String(path),
	}

	result, err := rst.client.CreateMultipartUpload(context.TODO(), createMultipartUploadInput)
	if err != nil {
		return "", err
	}
	return *result.UploadId, nil
}

func (rst *S3Client) AbortUpload(uploadID string, path string) error {
	abortMultipartUploadInput := &s3.AbortMultipartUploadInput{
		UploadId: aws.String(uploadID),
		Bucket:   &rst.config.GetS3().Bucket,
		Key:      aws.String(path),
	}

	_, err := rst.client.AbortMultipartUpload(context.TODO(), abortMultipartUploadInput)
	return err
}

func (rst *S3Client) FinishUpload(uploadID string, path string, parts []*flex.WorkResponse_Part) error {

	completedParts := make([]types.CompletedPart, len(parts))
	for i, part := range parts {
		completedParts[i] = types.CompletedPart{
			PartNumber: &part.PartNumber,
			ETag:       &part.EntityTag,
		}
	}

	completeMultipartUploadInput := &s3.CompleteMultipartUploadInput{
		Bucket:   &rst.config.GetS3().Bucket,
		Key:      aws.String(path),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}

	_, err := rst.client.CompleteMultipartUpload(context.TODO(), completeMultipartUploadInput)
	return err
}

// TODO: This is a WIP used for initial testing. For example it doesn't take
// into account what offset in the file should be uploaded. Ideally this just
// takes a segment instead of part if we can make it a common type.
func (rst *S3Client) UploadPart(uploadID string, part int32, path string) (string, error) {

	file, err := filesystem.MountPoint.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	uploadPartReq := &s3.UploadPartInput{
		Bucket:     &rst.config.GetS3().Bucket,
		Key:        aws.String(path),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(part),
		Body:       file,
	}

	uploadPartResp, err := rst.client.UploadPart(context.TODO(), uploadPartReq)
	if err != nil {
		return "", err
	}
	return *uploadPartResp.ETag, nil
}
