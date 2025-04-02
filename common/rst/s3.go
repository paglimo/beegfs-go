package rst

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3Config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"go.uber.org/zap"

	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type S3Client struct {
	config *flex.RemoteStorageTarget
	// TODO: https://github.com/thinkparq/gobee/issues/28
	// Rework client into an `s3Provider` interface type.
	client     *s3.Client
	mountPoint filesystem.Provider
}

var _ Provider = &S3Client{}

func newS3(ctx context.Context, rstConfig *flex.RemoteStorageTarget, mountPoint filesystem.Provider) (Provider, error) {

	resolver := aws.EndpointResolverWithOptionsFunc(func(string, string, ...any) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:       rstConfig.GetS3().GetPartitionId(),
			URL:               rstConfig.GetS3().GetEndpointUrl(),
			SigningRegion:     rstConfig.GetS3().GetRegion(),
			HostnameImmutable: true,
		}, nil
	})

	cfg, err := s3Config.LoadDefaultConfig(
		ctx,
		s3Config.WithRegion(rstConfig.GetS3().GetRegion()),
		s3Config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(rstConfig.GetS3().GetAccessKey(), rstConfig.GetS3().GetSecretKey(), "")),
		s3Config.WithEndpointResolverWithOptions(resolver),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to load config for RST client: %w", err)
	}

	r := &S3Client{
		config:     rstConfig,
		client:     s3.NewFromConfig(cfg),
		mountPoint: mountPoint,
	}

	return r, nil
}

func (r *S3Client) GetConfig() *flex.RemoteStorageTarget {
	return proto.Clone(r.config).(*flex.RemoteStorageTarget)
}

type GenerateWorkRequestsFunc func(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) ([]*flex.WorkRequest, bool, error)

// GenerateWorkRequest performs preliminary checks and generates the work requests.
func (r *S3Client) GenerateWorkRequests(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) ([]*flex.WorkRequest, bool, error) {
	request := job.GetRequest()

	var generate GenerateWorkRequestsFunc
	if request.HasSync() {
		generate = r.generateSyncJobWorkRequest
	} else if request.HasDirSync() {
		generate = r.generateDirSyncJobWorkRequests
	} else {
		return nil, false, ErrUnsupportedOpForRST
	}

	return generate(ctx, lastJob, job, availableWorkers)
}

type ExecuteWorkRequestsFunc func(ctx context.Context, workRequest *flex.WorkRequest, part *flex.Work_Part) error

// ExecuteWorkRequestPart attempts to carry out the specified part of the provided requests. The
// part is updated directly with the results.
func (r *S3Client) ExecuteWorkRequestPart(ctx context.Context, workRequest *flex.WorkRequest, part *flex.Work_Part) error {
	var execute ExecuteWorkRequestsFunc
	if workRequest.HasSync() {
		execute = r.executeSyncJobWorkRequestPart
	} else if workRequest.HasDirSync() {
		execute = r.executeDirSyncJobWorkRequestPart
	} else {
		return fmt.Errorf("operation is not supported by this RST type")
	}

	return execute(ctx, workRequest, part)
}

type CompleteWorkRequestsFunc func(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error

// CompleteWorkRequests completes the job request once all the job's work requests are complete.
func (r *S3Client) CompleteWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {
	request := job.GetRequest()

	var complete CompleteWorkRequestsFunc
	if request.HasSync() {
		complete = r.completeSyncWorkRequests
	} else if request.HasDirSync() {
		complete = r.completeDirSyncWorkRequests
	} else {
		return ErrReqAndRSTTypeMismatch
	}

	return complete(ctx, job, workResults, abort)
}

// activeJobAlreadyExists is intended to be used with GenerateWorkRequests to determine whether
// there is an active job that would be completed by this job.
func activeJobAlreadyExists(request *beeremote.JobRequest, lastJob *beeremote.Job) bool {
	if lastJob == nil {
		return false
	}

	lastRequest := lastJob.GetRequest()
	if request.HasSync() && lastRequest.HasSync() {
		if request.GetSync().Operation != lastRequest.GetSync().Operation {
			return false
		}

	} else if request.HasDirSync() && lastRequest.HasDirSync() {
		if request.GetDirSync().Operation != lastRequest.GetDirSync().Operation {
			return false
		}
	} else {
		return false
	}

	state := lastJob.Status.State
	if state == beeremote.Job_COMPLETED || state == beeremote.Job_CANCELLED || state == beeremote.Job_FAILED {
		return false
	}
	return true
}

/*

SyncJob specific functions

*/

type generateSyncJobWorkRequestFunc func(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) ([]*flex.WorkRequest, bool, error)

func (r *S3Client) generateSyncJobWorkRequest(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) ([]*flex.WorkRequest, bool, error) {
	if job.GetExternalId() != "" {
		return nil, false, ErrJobAlreadyHasExternalID
	}

	request := job.GetRequest()
	syncJob := request.GetSync()

	var generate generateSyncJobWorkRequestFunc
	switch syncJob.Operation {
	case flex.SyncJob_UPLOAD:
		generate = r.generateSyncJobWorkRequest_Upload
	case flex.SyncJob_DOWNLOAD:
		generate = r.generateSyncJobWorkRequest_Download
	default:
		return nil, false, ErrUnsupportedOpForRST
	}

	return generate(ctx, lastJob, job, availableWorkers)
}

// ExecuteWorkRequestPart() attempts to carry out the specified part of the provided requests.
// The part is updated directly with the results.
func (r *S3Client) executeSyncJobWorkRequestPart(ctx context.Context, request *flex.WorkRequest, part *flex.Work_Part) error {
	syncJob := request.GetSync()

	var err error
	switch syncJob.Operation {
	case flex.SyncJob_UPLOAD:
		err = r.Upload(ctx, request.Path, syncJob.RemotePath, request.ExternalId, part)
	case flex.SyncJob_DOWNLOAD:
		err = r.Download(ctx, request.Path, syncJob.RemotePath, part)
	}
	if err != nil {
		return err
	}

	part.Completed = true
	return nil
}

type completeSyncWorkRequestsFunc func(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error

func (r *S3Client) completeSyncWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {
	request := job.GetRequest()
	syncJob := request.GetSync()

	var complete completeSyncWorkRequestsFunc
	switch syncJob.Operation {
	case flex.SyncJob_UPLOAD:
		complete = r.completeSyncWorkRequests_Upload
	case flex.SyncJob_DOWNLOAD:
		complete = r.completeSyncWorkRequests_Download
	}
	return complete(ctx, job, workResults, abort)
}

func (r *S3Client) generateSyncJobWorkRequest_Upload(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) ([]*flex.WorkRequest, bool, error) {
	request := job.GetRequest()
	syncJob := request.GetSync()

	if syncJob.RemotePath == "" {
		key := request.Path
		syncJob.SetRemotePath(key)
	}

	// The most likely reason for an stat error is the path wasn't found because the request is for
	// a file in BeeGFS that doesn't exist or was removed after the request was submitted. Less
	// likely there was some transient network/other issue preventing communication to BeeGFS that
	// could be retried (generally the client should retry most issues anyway). Until there is a
	// reason to add more complex error handling just treat all stat errors as fatal.
	stat, err := r.mountPoint.Lstat(request.Path)
	if err != nil {
		return nil, true, err
	}
	fileSize := stat.Size()
	mtime := stat.ModTime()
	job.SetStartMtime(timestamppb.New(mtime))

	objectFileSize, objectMtime, err := r.GetObjectMetadata(ctx, syncJob.RemotePath, false)
	if err != nil {
		return nil, true, fmt.Errorf("error retrieving remote object's metadata: %w", err)
	}

	if fileSize == objectFileSize && mtime.Equal(objectMtime) {
		if syncJob.StubOnly {
			// File is already synced so just create a stub to finish the migration
			store, err := config.NodeStore(ctx)
			if err != nil {
				return nil, true, fmt.Errorf("upload successful but failed to create stub file: %w", err)
			}
			mappings, err := util.GetMappings(ctx)
			if err != nil && !errors.Is(err, util.ErrMappingRSTs) {
				return nil, true, fmt.Errorf("upload successful but failed to create stub file: %w", err)
			}

			err = createStubFile(ctx, r.mountPoint, mappings, store, request.Path, syncJob.RemotePath, request.RemoteStorageTarget, true)
			if err != nil {
				return nil, true, fmt.Errorf("file is already synced but failed to create stub file: %w", err)
			}
		}

		return nil, true, ErrJobAlreadyComplete
	}
	if activeJobAlreadyExists(request, lastJob) {
		return nil, true, ErrJobAlreadyExists
	}

	if stat.Mode().Type()&fs.ModeSymlink != 0 {
		// TODO: https://github.com/ThinkParQ/bee-remote/issues/25
		// Support symbolic links.
		return nil, true, fmt.Errorf("unable to upload symlink: %w", rst.ErrFileTypeUnsupported)
	}
	if !stat.Mode().IsRegular() {
		return nil, true, fmt.Errorf("%w", rst.ErrFileTypeUnsupported)
	}

	store, err := config.NodeStore(ctx)
	if err != nil {
		return nil, true, err
	}
	mappings, err := util.GetMappings(ctx)
	if err != nil && !errors.Is(err, util.ErrMappingRSTs) {
		return nil, true, fmt.Errorf("unable to proceed without entity mappings: %w", err)
	}
	if isStub, err := isFileStubbed(ctx, mappings, store, request.Path); err != nil {
		return nil, true, fmt.Errorf("unable to determine stub file status: %w", err)
	} else if isStub {
		if syncJob.StubOnly {
			// This is a migration so if the stub url matches return already complete
			err = verifyStubFileRstUrlMatches(r.mountPoint, request.Path, syncJob.RemotePath, request.RemoteStorageTarget)
			if err != nil {
				return nil, true, fmt.Errorf("failed to complete migration! File is already a stub: %w", err)
			} else {
				return nil, true, ErrJobAlreadyComplete
			}
		}
		return nil, true, fmt.Errorf("unable to upload stub file: %w", rst.ErrFileTypeUnsupported)
	}

	segCount, partsPerSegment := r.recommendedSegments(fileSize)
	if segCount > 1 {
		externalID, err := r.CreateUpload(ctx, syncJob.RemotePath, mtime)
		if err != nil {
			return nil, true, fmt.Errorf("error creating multipart upload: %w", err)
		}
		job.SetExternalId(externalID)
	}

	workRequests := RecreateWorkRequests(job, generateSegments(fileSize, segCount, partsPerSegment))
	return workRequests, true, nil
}

func (r *S3Client) generateSyncJobWorkRequest_Download(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) ([]*flex.WorkRequest, bool, error) {
	request := job.GetRequest()
	syncJob := request.GetSync()

	objectFileSize, objectMtime, err := r.GetObjectMetadata(ctx, syncJob.RemotePath, true)
	if err != nil {
		return nil, true, err
	}
	job.SetStartMtime(timestamppb.New(objectMtime))

	fileExists := false
	stat, err := r.mountPoint.Lstat(request.Path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, true, err
		}
	} else {
		fileExists = true
	}

	overwrite := syncJob.Overwrite
	if syncJob.StubOnly {
		store, err := config.NodeStore(ctx)
		if err != nil {
			return nil, true, err
		}
		mappings, err := util.GetMappings(ctx)
		if err != nil && !errors.Is(err, util.ErrMappingRSTs) {
			return nil, true, fmt.Errorf("unable to proceed without entity mappings: %w", err)
		}
		if fileExists {
			if isStub, err := isFileStubbed(ctx, mappings, store, request.Path); err != nil {
				return nil, true, fmt.Errorf("unable to determine stub status: %w", err)
			} else if isStub {
				err = verifyStubFileRstUrlMatches(r.mountPoint, request.Path, syncJob.RemotePath, request.RemoteStorageTarget)
				if err != nil {
					if !overwrite {
						return nil, true, fmt.Errorf("unable to create stub file: %w", err)
					}
				} else {
					return nil, true, ErrJobAlreadyComplete
				}

			} else if !overwrite {
				return nil, true, fmt.Errorf("unable to create stub file: %w", fs.ErrExist)
			}
		}

		err = createStubFile(ctx, r.mountPoint, mappings, store, request.Path, syncJob.RemotePath, request.RemoteStorageTarget, overwrite)
		if err != nil {
			return nil, true, err
		}
		return nil, true, ErrJobAlreadyComplete
	}

	if fileExists {
		store, err := config.NodeStore(ctx)
		if err != nil {
			return nil, true, err
		}
		mappings, err := util.GetMappings(ctx)
		if err != nil && !errors.Is(err, util.ErrMappingRSTs) {
			return nil, true, fmt.Errorf("unable to proceed without entity mappings: %w", err)
		}

		if isStub, err := isFileStubbed(ctx, mappings, store, request.Path); err != nil {
			return nil, true, fmt.Errorf("unable to determine stub status: %w", err)
		} else if isStub {
			if err := clearStubFlag(ctx, mappings, store, request.Path); err != nil {
				return nil, true, fmt.Errorf("unable to clear stub file flag: %w", err)
			}
			overwrite = true
		} else {
			fileSize := stat.Size()
			mtime := stat.ModTime()
			if fileSize == objectFileSize && mtime.Equal(objectMtime) {
				return nil, true, ErrJobAlreadyComplete
			}
			if activeJobAlreadyExists(request, lastJob) {
				return nil, true, ErrJobAlreadyExists
			}
		}
	}

	segCount, partsPerSegment := r.recommendedSegments(objectFileSize)
	err = r.mountPoint.CreatePreallocatedFile(request.Path, objectFileSize, overwrite)
	if err != nil {
		return nil, false, err
	}

	workRequests := RecreateWorkRequests(job, generateSegments(objectFileSize, segCount, partsPerSegment))
	return workRequests, true, nil
}

func (r *S3Client) completeSyncWorkRequests_Upload(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {
	request := job.GetRequest()
	syncJob := request.GetSync()

	stat, err := r.mountPoint.Lstat(request.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("unable to complete job requests: %w", err)
		} else if !abort {
			// Ignore errors when aborting.
			return fmt.Errorf("unable to set local file mtime as part of completing job requests: %w", err)
		}
	}
	mtime := stat.ModTime()
	job.SetStopMtime(timestamppb.New(mtime))

	if job.ExternalId != "" {
		if abort {
			// When aborting there is no reason to check the mtime below and it may not have
			// been set correctly anyway given the error check is skipped above.
			return r.AbortUpload(ctx, job.ExternalId, request.Path)
		} else {
			// TODO: https://github.com/thinkparq/gobee/issues/29
			// There could be lots of parts. Look for ways to optimize this. Like if we could
			// determine the total number of parts and make an appropriately sized slice up front.
			// Or if we could just pass the RST method the unmodified map since it potentially has
			// to iterate over the slice to convert it to the type it expects anyway. The drawback
			// with the latter approach is the RST package would import a BeeRemote package and the
			// goal has been to break this out into a standalone package to reuse for BeeSync.
			partsToFinish := make([]*flex.Work_Part, 0)
			for _, r := range workResults {
				partsToFinish = append(partsToFinish, r.Parts...)
			}
			// If there was an error finishing the upload we should return that and not worry
			// about checking if the file was modified.
			if err := r.FinishUpload(ctx, job.ExternalId, request.Path, partsToFinish); err != nil {
				return err
			}
		}
	}
	// Skip checking the file was modified if we were told to abort since the mtime may not have
	// been set correctly anyway given the error check is skipped above.
	if !abort {
		start := job.GetStartMtime().AsTime()
		stop := job.GetStopMtime().AsTime()
		if !start.Equal(stop) {
			return fmt.Errorf("successfully completed all work requests but the file appears to have been modified (mtime at job start: %s / mtime at job completion: %s)",
				start.Format(time.RFC3339), stop.Format(time.RFC3339))
		}
	}

	if syncJob.StubOnly {
		store, err := config.NodeStore(ctx)
		if err != nil {
			return fmt.Errorf("upload successful but failed to create stub file: %w", err)
		}
		mappings, err := util.GetMappings(ctx)
		if err != nil && !errors.Is(err, util.ErrMappingRSTs) {
			return fmt.Errorf("upload successful but failed to create stub file: %w", err)
		}

		err = createStubFile(ctx, r.mountPoint, mappings, store, request.Path, syncJob.RemotePath, request.RemoteStorageTarget, true)
		if err != nil {
			return fmt.Errorf("upload successful but failed to create stub file: %w", err)
		}
	}
	return nil
}

func (r *S3Client) completeSyncWorkRequests_Download(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {
	request := job.GetRequest()
	syncJob := request.GetSync()

	_, mtime, err := r.GetObjectMetadata(ctx, syncJob.RemotePath, true)
	if err != nil {
		return err
	}

	job.SetStopMtime(timestamppb.New(mtime))
	absPath := filepath.Join(r.mountPoint.GetMountPath(), request.Path)
	if err = os.Chtimes(absPath, mtime, mtime); err != nil {
		return fmt.Errorf("failed to update download's mtime: %w", err)
	}

	// Skip checking the file was modified if we were told to abort since the mtime may not have
	// been set correctly anyway given the error check is skipped above.
	if !abort {
		start := job.GetStartMtime().AsTime()
		stop := job.GetStopMtime().AsTime()
		if !start.Equal(stop) {
			return fmt.Errorf("successfully completed all work requests but the file appears to have been modified (mtime at job start: %s / mtime at job completion: %s)",
				start.Format(time.RFC3339), stop.Format(time.RFC3339))
		}
	}
	return nil
}

func (r *S3Client) recommendedSegments(fileSize int64) (int64, int32) {

	if fileSize <= r.config.Policies.FastStartMaxSize || r.config.Policies.FastStartMaxSize == 0 {
		return 1, 1
	} else if fileSize/4 < 5242880 {
		// Each part must be at least 5MB except for the last part which can be any size.
		// Regardless of the FastStartMaxSize ensure we don't try to use a multipart upload when it is not valid.
		// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
		return 1, 1
	}
	// TODO: https://github.com/thinkparq/gobee/issues/7
	// Arbitrary selection for now. We should be smarter and take into
	// consideration file size and number of workers for this RST type.
	return 4, 1
}

// GetObjectMetadata returns the object's size in bytes, modification time if it exists.
func (r *S3Client) GetObjectMetadata(ctx context.Context, key string, keyMustExist bool) (int64, time.Time, error) {
	headObjectInput := &s3.HeadObjectInput{
		Bucket: aws.String(r.config.GetS3().Bucket),
		Key:    aws.String(key),
	}

	resp, err := r.client.HeadObject(ctx, headObjectInput)
	if err != nil {
		var apiErr smithy.APIError
		if !keyMustExist && errors.As(err, &apiErr) {
			if apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchKey" {
				return 0, time.Time{}, nil
			}
		}
		return 0, time.Time{}, err
	}

	beegfsMtime, ok := resp.Metadata["beegfs-mtime"]
	if !ok {
		return *resp.ContentLength, *resp.LastModified, nil
	}

	mtime, err := time.Parse(time.RFC3339, beegfsMtime)
	if err != nil {
		return *resp.ContentLength, *resp.LastModified, fmt.Errorf("unable to parse s3 object's beegfs-mtime metadata")
	}

	return *resp.ContentLength, mtime, nil
}

func (r *S3Client) CreateUpload(ctx context.Context, path string, mtime time.Time) (uploadID string, err error) {

	createMultipartUploadInput := &s3.CreateMultipartUploadInput{
		Bucket:   aws.String(r.config.GetS3().Bucket),
		Key:      aws.String(path),
		Metadata: map[string]string{"beegfs-mtime": mtime.Format(time.RFC3339)},
	}

	result, err := r.client.CreateMultipartUpload(ctx, createMultipartUploadInput)
	if err != nil {
		return "", err
	}
	return *result.UploadId, nil
}

func (r *S3Client) AbortUpload(ctx context.Context, uploadID string, path string) error {
	abortMultipartUploadInput := &s3.AbortMultipartUploadInput{
		UploadId: aws.String(uploadID),
		Bucket:   aws.String(r.config.GetS3().Bucket),
		Key:      aws.String(path),
	}

	_, err := r.client.AbortMultipartUpload(ctx, abortMultipartUploadInput)
	return err
}

// FinishUpload will automatically sort parts by number if they are not already in order.
func (r *S3Client) FinishUpload(ctx context.Context, uploadID string, path string, parts []*flex.Work_Part) error {

	completedParts := make([]types.CompletedPart, len(parts))
	for i, part := range parts {
		completedParts[i] = types.CompletedPart{
			PartNumber:     aws.Int32(part.PartNumber),
			ETag:           aws.String(part.EntityTag),
			ChecksumSHA256: aws.String(part.ChecksumSha256),
		}
	}

	sort.Slice(completedParts, func(i, j int) bool {
		return *completedParts[i].PartNumber < *completedParts[j].PartNumber
	})

	completeMultipartUploadInput := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(r.config.GetS3().Bucket),
		Key:      aws.String(path),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}

	_, err := r.client.CompleteMultipartUpload(ctx, completeMultipartUploadInput)
	return err
}

// Upload attempts to upload the specified part of the provided file path. If an upload ID is
// provided it will treat the provided part as one part in a larger multi-part upload. If the upload
// ID is empty then it will not perform a multi-part upload and just do PutObject, but this also
// requires the provided part number to be "1". When not performing a multi-part upload it still
// honors the provided offset start/stop range and does not check to verify this range covers the
// entirety of the specified file. If the upload is successful the part will be updated directly
// with the results (such as the etag), otherwise an error will be returned.
func (r *S3Client) Upload(ctx context.Context, path string, remotePath string, uploadID string, part *flex.Work_Part) error {

	filePart, sha256sum, err := r.mountPoint.ReadFilePart(path, part.OffsetStart, part.OffsetStop)

	if err != nil {
		// S3 allows uploading empty (zero byte) files. Only allow this if the offset start/stop are
		// actually zero.
		if errors.Is(err, io.EOF) && part.OffsetStart == 0 && part.OffsetStop == 0 {
			filePart = bytes.NewReader([]byte{})
		} else {
			return err
		}
	}
	part.ChecksumSha256 = sha256sum

	if uploadID == "" {
		// This should catch most issues where the user intended to perform a multi-part upload, but
		// did not generate an upload ID first or if multiple parts were generated inadvertently.
		if part.PartNumber != 1 {
			return fmt.Errorf("only multi-part uploads can have a part number other than 1 (did you intend to create a multi-part upload first?)")
		}

		stat, err := r.mountPoint.Lstat(path)
		if err != nil {
			return err
		}
		mtime := stat.ModTime()

		resp, err := r.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:         aws.String(r.config.GetS3().Bucket),
			Key:            aws.String(remotePath),
			Body:           filePart,
			ChecksumSHA256: aws.String(part.ChecksumSha256),
			// Could a local mtime match and allow for a continue/resume feature...
			//	- If there was a previous failure and the mtime still match then continue from the last byte write
			Metadata: map[string]string{"beegfs-mtime": mtime.Format(time.RFC3339)},
		})

		if err != nil {
			return err
		}
		part.EntityTag = *resp.ETag
		return nil
	}

	uploadPartReq := &s3.UploadPartInput{
		Bucket:         aws.String(r.config.GetS3().Bucket),
		Key:            aws.String(remotePath),
		UploadId:       aws.String(uploadID),
		PartNumber:     aws.Int32(part.PartNumber),
		Body:           filePart,
		ChecksumSHA256: aws.String(part.ChecksumSha256),
	}

	resp, err := r.client.UploadPart(ctx, uploadPartReq)
	if err != nil {
		return err
	}
	part.EntityTag = *resp.ETag
	return nil
}

func (r *S3Client) Download(ctx context.Context, path string, remotePath string, part *flex.Work_Part) error {
	if part.OffsetStart == part.OffsetStop {
		// There are no bytes to write to the file. Most likely this is an empty file.
		return nil
	}

	filePart, err := r.mountPoint.WriteFilePart(path, part.OffsetStart, part.OffsetStop)
	if err != nil {
		return err
	}
	defer filePart.Close()

	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(r.config.GetS3().Bucket),
		Key:    aws.String(remotePath),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", part.OffsetStart, part.OffsetStop)),
	}

	resp, err := r.client.GetObject(ctx, getObjectInput)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	copiedBytes, err := io.Copy(filePart, resp.Body)
	if err != nil {
		return err
	}
	if copiedBytes != part.OffsetStop-part.OffsetStart+1 {
		return fmt.Errorf("%w (expected: %d, actual: %d)", ErrPartialPartDownload, part.OffsetStop-part.OffsetStart+1, copiedBytes)
	}
	return nil
}

/*

DirSyncJob specific functions

*/

func (r *S3Client) generateDirSyncJobWorkRequests(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) ([]*flex.WorkRequest, bool, error) {
	request := job.GetRequest()
	dirSync := request.GetDirSync()

	stat, err := r.mountPoint.Stat(request.Path)
	if err != nil {
		return nil, false, err
	}
	if !stat.IsDir() {
		return nil, false, fmt.Errorf("local path must be a directory! This is probably a bug")
	}

	var walkChan <-chan *WalkResponse
	switch dirSync.Operation {
	case flex.DirSyncJob_UPLOAD:
		walkChan, err = r.walkDirectory(ctx, request.Path, 128, true)
	case flex.DirSyncJob_DOWNLOAD:
		walkChan, err = r.walkPrefix(ctx, dirSync.RemotePath, 128, true)
	default:
		return nil, false, ErrUnsupportedOpForRST
	}
	if err != nil {
		return nil, true, fmt.Errorf("unable to determine job request file count: %w", err)
	}

	segments, err := r.getWalkSegments(ctx, walkChan, 1000) // What size should minSplit be?
	if err != nil {
		return nil, true, err
	}

	return RecreateWorkRequests(job, segments), true, nil
}

func (r *S3Client) executeDirSyncJobWorkRequestPart(ctx context.Context, request *flex.WorkRequest, part *flex.Work_Part) error {
	dirSync := request.GetDirSync()

	var err error
	switch dirSync.Operation {
	case flex.DirSyncJob_UPLOAD:
		err = r.executeDirSyncJobWorkRequestPart_Upload(ctx, request, part.OffsetStart, part.OffsetStop)
	case flex.DirSyncJob_DOWNLOAD:
		err = r.executeDirSyncJobWorkRequestPart_Download(ctx, request, part.OffsetStart, part.OffsetStop)
	}
	if err != nil {
		return err
	}

	part.SetCompleted(true)
	return nil
}

func (r *S3Client) completeDirSyncWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {
	return nil
}

func (r *S3Client) executeDirSyncJobWorkRequestPart_Upload(ctx context.Context, request *flex.WorkRequest, offsetStart, offsetStop int64) error {
	job := request.GetDirSync()

	chanSize := 2048
	walkChan, err := r.walkDirectory(ctx, request.Path, chanSize, false)
	if err != nil {
		return err
	}

	jobRequestCfgChan, _, err := rst.StreamSubmitJobRequests(ctx, r.mountPoint, chanSize, true)
	if err != nil {
		return fmt.Errorf("unable to stream job requests: %w", err)
	}

	defer close(jobRequestCfgChan)
	for count := int64(0); count <= offsetStop; count++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case walkResp, ok := <-walkChan:
			if !ok {
				return nil
			}
			if count < offsetStart {
				continue
			}

			if walkResp.FatalErr {
				log, _ := config.GetLogger()
				log.Error("fatal error occurred while walking path or prefix", zap.Error(walkResp.Err))
				return walkResp.Err
			}

			cfg := &rst.JobRequestCfg{
				RSTID:     request.RemoteStorageTarget,
				Path:      walkResp.Path,
				Download:  false,
				StubOnly:  job.StubOnly,
				Overwrite: job.Overwrite,
				Force:     job.Force,
				JobType:   rst.JobTypeSync,
			}
			jobRequestCfgChan <- cfg
		}
	}

	return nil
}

func (r *S3Client) executeDirSyncJobWorkRequestPart_Download(ctx context.Context, request *flex.WorkRequest, offsetStart, offsetStop int64) error {
	job := request.GetDirSync()
	store, err := config.NodeStore(ctx)
	if err != nil {
		return err
	}
	mappings, err := util.GetMappings(ctx)
	if err != nil {
		if !errors.Is(err, util.ErrMappingRSTs) {
			return fmt.Errorf("unable to proceed without entity mappings: %w", err)
		}
	}

	chanSize := 2048
	walkChan, err := r.walkPrefix(ctx, job.RemotePath, chanSize, false)
	if err != nil {
		return fmt.Errorf("unable to determine job request object count: %w", err)
	}

	var jobRequestCfgChan chan<- *rst.JobRequestCfg
	jobRequestCfgChan, _, err = rst.StreamSubmitJobRequests(ctx, r.mountPoint, chanSize, true)
	if err != nil {
		return fmt.Errorf("unable to stream job requests: %w", err)
	}

	for count := int64(0); count <= offsetStop; count++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case walkResp, ok := <-walkChan:
			if !ok {
				return nil
			}
			if count < offsetStart {
				continue
			}

			if walkResp.FatalErr {
				return walkResp.Err
			}

			destPath := mapRemoteToLocalPath(request.Path, walkResp.Path, job.Flatten)
			pathDir := filepath.Dir(destPath)
			if err := r.mountPoint.CreateDir(pathDir); err != nil {
				return err
			}

			// Only create a stub file there's no file. Otherwise allow the job to be created so the
			// decision making and failures are on the sync job request. This is important because it
			// allows the individual files to reflect failures rather than the directory.
			if _, err := r.mountPoint.Lstat(destPath); err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					return err
				}
				err = createStubFile(ctx, r.mountPoint, mappings, store, destPath, walkResp.Path, request.RemoteStorageTarget, job.Overwrite)
				if err != nil {
					return err
				}
				if job.StubOnly {
					continue
				}
			}

			cfg := &rst.JobRequestCfg{
				RSTID:      request.RemoteStorageTarget,
				Path:       destPath,
				RemotePath: walkResp.Path,
				Download:   true,
				StubOnly:   job.StubOnly,
				Overwrite:  job.Overwrite,
				Force:      job.Force,
				JobType:    rst.JobTypeSync,
			}
			jobRequestCfgChan <- cfg
		}
	}

	return nil
}

type WalkResponse struct {
	Path     string
	Err      error
	FatalErr bool
}

// walkPrefix lists objects in an S3 bucket that match the specified prefix, returning results via
// the *WalkPrefixResponse channel.Results can also be filtered using file globbing in the pattern
// which includes double start for directory recursion.
func (r *S3Client) walkPrefix(ctx context.Context, prefix string, chanSize int, forCountOnly bool) (<-chan *WalkResponse, error) {
	prefix = strings.TrimLeft(prefix, "/") // valid s3 prefixes do not start with a '/' (e.g. myfolder/*/subdir?[a-z])
	if _, err := filepath.Match(prefix, ""); err != nil {
		return nil, fmt.Errorf("invalid prefix %s: %w", prefix, err)
	}

	prefixWithoutPattern := stripGlobPattern(prefix)
	usePattern := prefix != prefixWithoutPattern

	respChan := make(chan *WalkResponse, chanSize)
	go func() {
		defer close(respChan)

		select {
		case <-ctx.Done():
			return
		default:
			var err error
			var output *s3.ListObjectsV2Output
			input := &s3.ListObjectsV2Input{
				Bucket: aws.String(r.config.GetS3().Bucket),
				Prefix: aws.String(prefixWithoutPattern),
			}

			objectPaginator := s3.NewListObjectsV2Paginator(r.client, input)
			for objectPaginator.HasMorePages() {
				output, err = objectPaginator.NextPage(ctx)
				if err != nil {
					var noBucket *types.NoSuchBucket
					if errors.As(err, &noBucket) {
						err := fmt.Errorf("s3 bucket %s does not exist: %s", r.config.GetS3().Bucket, noBucket.Error())
						respChan <- &WalkResponse{Err: err, FatalErr: true}
					}
					break
				} else if forCountOnly {
					for range output.Contents {
						respChan <- nil
					}
				} else {
					for _, content := range output.Contents {
						if usePattern {
							if match, _ := filepath.Match(prefix, *content.Key); !match {
								continue
							}
						}
						respChan <- &WalkResponse{Path: *content.Key}
					}
				}
			}
		}
	}()

	return respChan, nil
}

func (r *S3Client) walkDirectory(ctx context.Context, pattern string, chanSize int, forCountOnly bool) (<-chan *WalkResponse, error) {
	respChan := make(chan *WalkResponse, chanSize)
	walkFunc := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if d.IsDir() {
				return nil
			}

			if forCountOnly {
				respChan <- nil
				return nil
			}

			inMountPath, err := r.mountPoint.GetRelativePathWithinMount(path)
			if err != nil {
				// An error at this point is unlikely given previous steps also get relative paths.
				// To be safe note this in the error that is returned and just return the absolute
				// path instead. This shouldn't be a security issue since the user should have
				// access to this path if they were able to start a job request for it.
				inMountPath = path
				respChan <- &WalkResponse{
					Path:     inMountPath,
					Err:      fmt.Errorf("unable to determine relative path: %w", err),
					FatalErr: true,
				}
				return nil
			}

			respChan <- &WalkResponse{Path: inMountPath}
		}
		return nil
	}

	go func() {
		defer close(respChan)
		r.mountPoint.WalkDir(stripGlobPattern(pattern), walkFunc)
	}()
	return respChan, nil
}

func (r *S3Client) getWalkSegments(ctx context.Context, walkChan <-chan *WalkResponse, minSplit int) ([]*flex.WorkRequest_Segment, error) {
	count, err := r.walkCount(ctx, walkChan)
	if err != nil {
		return nil, fmt.Errorf("unable to determine job request file count: %w", err)
	}

	maxWorkers := numWorkers()
	numWorkers := int(count/minSplit) + 1
	if numWorkers > maxWorkers {
		numWorkers = maxWorkers
	}
	batchSize := int(count / numWorkers)

	segments := make([]*flex.WorkRequest_Segment, 0)
	worker := 0
	for ; worker < numWorkers-1; worker++ {
		start := worker * batchSize
		segment := &flex.WorkRequest_Segment{
			OffsetStart: int64(start),
			OffsetStop:  int64(start + batchSize - 1),
		}
		segments = append(segments, segment)
	}
	segment := &flex.WorkRequest_Segment{
		OffsetStart: int64(worker * batchSize),
		OffsetStop:  math.MaxInt64,
	}
	segments = append(segments, segment)

	return segments, nil
}

func (r *S3Client) walkCount(ctx context.Context, walkChan <-chan *WalkResponse) (int, error) {
	count := 0
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case _, ok := <-walkChan:
			if !ok {
				return count, nil
			}
			count += 1
		}
	}
}

// MapRemoteToLocalPath joins a base path with a remote path. If flatten is true, it replaces
// directory separators in the remote path with underscores.
func mapRemoteToLocalPath(path string, remotePath string, flatten bool) string {
	if flatten {
		remotePath = strings.Replace(remotePath, "/", "_", -1)
	}

	remoteDir := filepath.Dir(remotePath)
	remoteBase := filepath.Base(remotePath)
	combinedDir := filepath.Join(path, remoteDir)
	combinedPath := filepath.Join(combinedDir, remoteBase)
	return combinedPath
}

func numWorkers() int {
	count := runtime.GOMAXPROCS(0)
	if count > 1 {
		return count - 1
	}
	return 1
}
