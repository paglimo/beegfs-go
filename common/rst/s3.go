package rst

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3Config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg"
	"github.com/thinkparq/beegfs-go/common/filesystem"

	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
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

// GenerateWorkRequest prepares work requests
func (r *S3Client) GenerateWorkRequests(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) ([]*flex.WorkRequest, bool, error) {
	request := job.GetRequest()
	if !request.HasSync() {
		return nil, false, ErrReqAndRSTTypeMismatch
	}
	if job.GetExternalId() != "" {
		return nil, false, ErrJobAlreadyHasExternalID
	}

	if request.JobBuilder {
		workRequests := RecreateWorkRequests(job, nil)
		return workRequests, true, nil
	}

	sync := request.GetSync()
	if sync.RemotePath == "" {
		sync.SetRemotePath(sanitizePrefix(request.Path))
	}

	switch sync.Operation {
	case flex.SyncJob_UPLOAD:
		return r.generateSyncJobWorkRequest_Upload(ctx, lastJob, job, availableWorkers)
	case flex.SyncJob_DOWNLOAD:
		return r.generateSyncJobWorkRequest_Download(ctx, lastJob, job, availableWorkers)
	}
	return nil, false, ErrUnsupportedOpForRST
}

// ExecuteWorkRequestPart attempts to carry out the specified part of the provided requests. The
// part is updated directly with the results.
func (r *S3Client) ExecuteWorkRequestPart(ctx context.Context, workRequest *flex.WorkRequest, part *flex.Work_Part) error {
	if !workRequest.HasSync() {
		return ErrReqAndRSTTypeMismatch
	}
	sync := workRequest.GetSync()

	var err error
	switch sync.Operation {
	case flex.SyncJob_UPLOAD:
		err = r.Upload(ctx, workRequest.Path, sync.RemotePath, workRequest.ExternalId, part)
	case flex.SyncJob_DOWNLOAD:
		err = r.Download(ctx, workRequest.Path, sync.RemotePath, part)
	}
	if err != nil {
		return err
	}

	part.Completed = true
	return nil

}

// ExecuteJobBuilderRequest walks either the local directory or remote prefix and sends job requests
// for each file or object.
func (r *S3Client) ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionChan chan<- *beeremote.JobRequest) error {
	defer close(jobSubmissionChan)
	if !workRequest.HasSync() {
		return ErrReqAndRSTTypeMismatch
	}

	sync := workRequest.GetSync()
	walkChanSize := len(jobSubmissionChan)

	var err error
	var walkChan <-chan *WalkResponse
	switch sync.Operation {
	case flex.SyncJob_DOWNLOAD:
		if sync.RemotePath == "" {
			sync.SetRemotePath(sanitizePrefix(workRequest.Path))
			workRequest.SetPath("/")
		}
		walkChan, err = r.walkPrefix(ctx, sync.RemotePath, walkChanSize)
	case flex.SyncJob_UPLOAD:
		walkChan, err = WalkLocalDirectory(ctx, r.mountPoint, workRequest.Path, walkChanSize)
	default:
		return ErrUnsupportedOpForRST
	}
	if err != nil {
		return err
	}

	return r.executeSyncJobBuilderRequest(ctx, workRequest, walkChan, jobSubmissionChan)
}

// CompleteWorkRequests completes the job request once all the job's work requests are complete.
func (r *S3Client) CompleteWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {
	request := job.GetRequest()
	if !request.HasSync() {
		return ErrReqAndRSTTypeMismatch
	}
	if request.JobBuilder {
		return nil
	}

	syncJob := request.GetSync()
	switch syncJob.Operation {
	case flex.SyncJob_UPLOAD:
		return r.completeSyncWorkRequests_Upload(ctx, job, workResults, abort)
	case flex.SyncJob_DOWNLOAD:
		return r.completeSyncWorkRequests_Download(ctx, job, workResults, abort)
	default:
		return ErrUnsupportedOpForRST
	}
}

// TODO: Investigate whether there's a way to offload this tasks back to
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
	} else {
		return false
	}

	state := lastJob.Status.State
	if state == beeremote.Job_OFFLOADED || state == beeremote.Job_COMPLETED || state == beeremote.Job_CANCELLED || state == beeremote.Job_FAILED {
		return false
	}
	return true
}

func (r *S3Client) generateSyncJobWorkRequest_Upload(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) ([]*flex.WorkRequest, bool, error) {
	request := job.GetRequest()
	sync := request.GetSync()

	mappings := &util.Mappings{}
	store := &beemsg.NodeStore{}
	lockedInfo, err := r.getLockedInfo(ctx, mappings, store, sync, request.Path, sync.RemotePath, request.StubLocal)
	if err != nil {
		return nil, true, err
	}
	job.SetStartMtime(lockedInfo.Mtime)

	filemode := fs.FileMode(lockedInfo.Mode)
	if filemode.Type()&fs.ModeSymlink != 0 {
		// TODO: https://github.com/ThinkParQ/bee-remote/issues/25
		// Support symbolic links.
		return nil, true, fmt.Errorf("unable to upload symlink: %w", rst.ErrFileTypeUnsupported)
	}
	if !filemode.IsRegular() {
		return nil, true, fmt.Errorf("%w", rst.ErrFileTypeUnsupported)
	}

	alreadySynced := lockedInfo.Size == lockedInfo.RemoteSize && lockedInfo.Mtime.AsTime().Equal(lockedInfo.RemoteMtime.AsTime())
	if alreadySynced {
		if request.StubLocal {

			// TODO: Get rid of theses
			store, err = config.NodeStore(ctx)
			if err != nil {
				return nil, true, err
			}
			mappings, err = util.GetMappings(ctx)
			if err != nil && !errors.Is(err, util.ErrMappingRSTs) {
				return nil, true, err
			}

			if err := CreateOffloadedDataFile(ctx, r.mountPoint, mappings, store, request.Path, sync.RemotePath, request.RemoteStorageTarget, true); err != nil {
				return nil, true, fmt.Errorf("file is already synced but failed to create stub file: %w", err)
			}
			return nil, true, ErrJobAlreadyOffloaded
		}
		return nil, true, ErrJobAlreadyComplete
	}

	if activeJobAlreadyExists(request, lastJob) {
		return nil, true, ErrJobAlreadyExists
	}

	isStub := lockedInfo.StubUrlRstId > 0
	if isStub {
		if request.StubLocal {
			if request.RemoteStorageTarget == lockedInfo.StubUrlRstId && sync.RemotePath == lockedInfo.StubUrlPath {
				return nil, true, ErrJobAlreadyOffloaded
			}
			return nil, true, fmt.Errorf("failed to complete migration! File is already a stub")
		}
		return nil, true, fmt.Errorf("unable to upload stub file: %w", rst.ErrFileTypeUnsupported)
	}

	segCount, partsPerSegment := r.recommendedSegments(lockedInfo.Size)
	if segCount > 1 {
		externalID, err := r.CreateUpload(ctx, sync.RemotePath, lockedInfo.Mtime.AsTime())
		if err != nil {
			return nil, true, fmt.Errorf("error creating multipart upload: %w", err)
		}
		job.SetExternalId(externalID)
	}

	workRequests := RecreateWorkRequests(job, generateSegments(lockedInfo.Size, segCount, partsPerSegment))
	return workRequests, true, nil
}

func (r *S3Client) generateSyncJobWorkRequest_Download(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) ([]*flex.WorkRequest, bool, error) {
	request := job.GetRequest()
	sync := request.GetSync()

	mappings := &util.Mappings{}
	store := &beemsg.NodeStore{}
	lockedInfo, err := r.getLockedInfo(ctx, mappings, store, sync, request.Path, sync.RemotePath, request.StubLocal)
	if err != nil {
		return nil, true, err
	}
	job.SetStartMtime(lockedInfo.RemoteMtime)

	overwrite := sync.Overwrite
	if request.StubLocal {
		if lockedInfo.Exists {
			isStub := lockedInfo.StubUrlRstId > 0
			if isStub {
				if request.RemoteStorageTarget == lockedInfo.StubUrlRstId && sync.RemotePath == lockedInfo.StubUrlPath {
					return nil, true, ErrJobAlreadyOffloaded
				} else if !overwrite {
					return nil, true, fmt.Errorf("unable to create stub file: %w", err)
				}
			} else if !overwrite {
				return nil, true, fmt.Errorf("unable to create stub file: %w", fs.ErrExist)
			}
		}

		err = CreateOffloadedDataFile(ctx, r.mountPoint, mappings, store, request.Path, sync.RemotePath, request.RemoteStorageTarget, overwrite)
		if err != nil {
			return nil, true, err
		}
		return nil, true, ErrJobAlreadyOffloaded
	}

	if lockedInfo.Exists {
		isStub := lockedInfo.StubUrlRstId > 0
		alreadySynced := lockedInfo.Size == lockedInfo.RemoteSize && lockedInfo.Mtime.AsTime().Equal(lockedInfo.RemoteMtime.AsTime())
		if isStub {

			// TODO: Get rid of theses
			store, err = config.NodeStore(ctx)
			if err != nil {
				return nil, true, err
			}
			mappings, err = util.GetMappings(ctx)
			if err != nil && !errors.Is(err, util.ErrMappingRSTs) {
				return nil, true, err
			}

			if err := entry.ClearFileDataState(ctx, mappings, store, request.Path); err != nil {
				return nil, true, fmt.Errorf("unable to clear stub file flag: %w", err)
			}
			overwrite = true
		} else if alreadySynced {
			return nil, true, ErrJobAlreadyComplete
		} else if activeJobAlreadyExists(request, lastJob) {
			return nil, true, ErrJobAlreadyExists
		}
	}

	segCount, partsPerSegment := r.recommendedSegments(lockedInfo.RemoteSize)
	err = r.mountPoint.CreatePreallocatedFile(request.Path, lockedInfo.RemoteSize, overwrite)
	if err != nil {
		return nil, false, err
	}

	workRequests := RecreateWorkRequests(job, generateSegments(lockedInfo.RemoteSize, segCount, partsPerSegment))
	return workRequests, true, nil
}

func (r *S3Client) executeSyncJobBuilderRequest(ctx context.Context, request *flex.WorkRequest, walkChan <-chan *WalkResponse, jobSubmissionChan chan<- *beeremote.JobRequest) error {
	sync := request.GetSync()
	inMountPath := request.Path
	remotePath := sync.RemotePath

	store, err := config.NodeStore(ctx)
	if err != nil {
		return fmt.Errorf("upload successful but failed to create stub file: %w", err)
	}
	mappings, err := util.GetMappings(ctx)
	if err != nil && !errors.Is(err, util.ErrMappingRSTs) {
		return fmt.Errorf("upload successful but failed to create stub file: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case walkResp, ok := <-walkChan:
			if !ok {
				return nil
			}
			if walkResp.FatalErr {
				return walkResp.Err
			}

			switch sync.Operation {
			case flex.SyncJob_DOWNLOAD:
				remotePath = sanitizePrefix(walkResp.Path)
				inMountPath = MapRemoteToLocalPath(request.Path, remotePath, sync.Flatten)
				if err := r.mountPoint.CreateDir(filepath.Dir(inMountPath)); err != nil {
					return err
				}
			case flex.SyncJob_UPLOAD:
				inMountPath = walkResp.Path
				remotePath = sanitizePrefix(inMountPath)
			}

			// TODO: If getLockedInfo needs to get the entry then it could be passed back as a pointer to avoid duplicate checks
			//  - Subsequent commands that need entry should just check for nil first...
			lockedInfo, err := r.getLockedInfo(ctx, mappings, store, sync, inMountPath, remotePath, request.StubLocal)
			if err != nil {
				return err
			}

			if request.StubLocal && !lockedInfo.Exists && sync.Operation == flex.SyncJob_DOWNLOAD {
				err = CreateOffloadedDataFile(ctx, r.mountPoint, mappings, store, inMountPath, remotePath, request.RemoteStorageTarget, false)
				if err != nil {
					return fmt.Errorf("unable to create stub file: %w", err)
				}
				lockedInfo.StubUrlRstId = request.RemoteStorageTarget
				lockedInfo.StubUrlPath = remotePath
			}

			jobRequest := &beeremote.JobRequest{
				Path:                inMountPath,
				RemoteStorageTarget: request.RemoteStorageTarget,
				StubLocal:           request.StubLocal,
				// TODO: this should be reflective of the original request
				// Force:               request.Force,
				Force: false,
				Type: &beeremote.JobRequest_Sync{
					Sync: &flex.SyncJob{
						RemotePath: remotePath,
						Operation:  sync.Operation,
						Overwrite:  sync.Overwrite,
						LockedInfo: lockedInfo,
					}}}
			jobSubmissionChan <- jobRequest
		}
	}
}

func (r *S3Client) completeSyncWorkRequests_Upload(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {
	request := job.GetRequest()
	sync := request.GetSync()

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

	if request.StubLocal {
		store, err := config.NodeStore(ctx)
		if err != nil {
			return fmt.Errorf("upload successful but failed to create stub file: %w", err)
		}
		mappings, err := util.GetMappings(ctx)
		if err != nil && !errors.Is(err, util.ErrMappingRSTs) {
			return fmt.Errorf("upload successful but failed to create stub file: %w", err)
		}

		err = CreateOffloadedDataFile(ctx, r.mountPoint, mappings, store, request.Path, sync.RemotePath, request.RemoteStorageTarget, true)
		if err != nil {
			return fmt.Errorf("upload successful but failed to create stub file: %w", err)
		}
		job.GetStatus().SetState(beeremote.Job_OFFLOADED)
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

func (r *S3Client) getLockedInfo(
	ctx context.Context,
	mappings *util.Mappings,
	store *beemsg.NodeStore,
	sync *flex.SyncJob,
	path string,
	remotePath string,
	offload bool,
) (*flex.SyncJob_SyncJobLockedInfo, error) {

	lockedInfo := sync.GetLockedInfo()
	if lockedInfo == nil {
		lockedInfo = &flex.SyncJob_SyncJobLockedInfo{}
	}

	if !lockedInfo.Locked {
		var err error
		if store == nil || len(store.GetNodes()) == 0 {
			store, err = config.NodeStore(ctx)
			if err != nil {
				return nil, err
			}
		}
		if mappings == nil || mappings.NodeToTargets.Len() == 0 {
			mappings, err = util.GetMappings(ctx)
			if err != nil && !errors.Is(err, util.ErrMappingRSTs) {
				return nil, err
			}
		}

		// TODO: Remove if the read-write and read-only methods can do these checks
		entry, err := entry.GetEntry(ctx, mappings, entry.GetEntriesCfg{}, path)
		if err != nil {
			if !errors.Is(err, beegfs.OpsErr_PATHNOTEXISTS) {
				return nil, err
			}
		} else {
			// TODO: need to add request.Force...
			// ignoreReaders := request.Force || sync.Operation == flex.SyncJob_UPLOAD
			// ignoreWriters := request.Force
			ignoreReaders := false || sync.Operation == flex.SyncJob_UPLOAD
			ignoreWriters := false
			err := CheckEntry(entry.Entry, ignoreReaders, ignoreWriters)
			if err != nil {
				return nil, err
			}
		}

		operation := sync.GetOperation()
		switch operation {
		case flex.SyncJob_DOWNLOAD:
			// TODO: Get file read-write lock
			//   Should read-write lock check for open files? If so, remove the GetEntry()/CheckEntry above
		case flex.SyncJob_UPLOAD:
			// TODO: Get file read-only lock
			//  Should read-only lock check for files open with write privilege? If so, remove the GetEntry()/CheckEntry above
		default:
			return nil, ErrUnsupportedOpForRST
		}
		lockedInfo.SetLocked(true)

		// The most likely reason for an stat error is the path wasn't found because the request is for
		// a file in BeeGFS that doesn't exist or was removed after the request was submitted. Less
		// likely there was some transient network/other issue preventing communication to BeeGFS that
		// could be retried (generally the client should retry most issues anyway). Until there is a
		// reason to add more complex error handling just treat all stat errors as fatal.
		stat, err := r.mountPoint.Lstat(path)
		if err != nil {
			if !os.IsNotExist(err) {
				return nil, err
			}
			if operation == flex.SyncJob_UPLOAD {
				return nil, err
			}
		} else {
			lockedInfo.Exists = true
			lockedInfo.Size = stat.Size()
			lockedInfo.Mtime = timestamppb.New(stat.ModTime())
			lockedInfo.Mode = uint32(stat.Mode())
		}

		remoteSize, remoteMtime, err := r.GetObjectMetadata(ctx, remotePath, operation == flex.SyncJob_DOWNLOAD)
		if err != nil {
			return nil, fmt.Errorf("error retrieving remote object's metadata: %w", err)
		}
		lockedInfo.RemoteSize = remoteSize
		lockedInfo.RemoteMtime = timestamppb.New(remoteMtime)

		if lockedInfo.Exists && !fs.FileMode(lockedInfo.Mode).IsDir() {
			if lockedInfo.StubUrlRstId, lockedInfo.StubUrlPath, err = GetOffloadedUrlParts(ctx, r.mountPoint, mappings, store, path); err != nil {
				return nil, fmt.Errorf("unable to get rst url parts: %w", err)
			}
		}
	}

	return lockedInfo, nil
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

func (r *S3Client) ObjectExists(ctx context.Context, key string) (bool, error) {
	headObjectInput := &s3.HeadObjectInput{
		Bucket: aws.String(r.config.GetS3().Bucket),
		Key:    aws.String(key),
	}

	_, err := r.client.HeadObject(ctx, headObjectInput)
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			if apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchKey" {
				return false, nil
			}
		}
		return false, err
	}
	return true, nil
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

// walkPrefix lists objects in an S3 bucket that match the specified prefix, returning results via
// the *WalkPrefixResponse channel.Results can also be filtered using file globbing in the pattern
// which includes double start for directory recursion.
func (r *S3Client) walkPrefix(ctx context.Context, prefix string, chanSize int) (<-chan *WalkResponse, error) {
	prefix = sanitizePrefix(prefix)
	if _, err := filepath.Match(prefix, ""); err != nil {
		return nil, fmt.Errorf("invalid prefix %s: %w", prefix, err)
	}

	prefixWithoutPattern := StripGlobPattern(prefix)
	usePattern := prefix != prefixWithoutPattern

	walkChan := make(chan *WalkResponse, chanSize)
	go func() {
		defer close(walkChan)
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
						walkChan <- &WalkResponse{Err: err, FatalErr: true}
					}
					return
				}

				for _, content := range output.Contents {
					if usePattern {
						if match, _ := filepath.Match(prefix, *content.Key); !match {
							continue
						}
					}
					walkChan <- &WalkResponse{Path: *content.Key}
				}
			}
		}
	}()

	return walkChan, nil
}

func sanitizePrefix(prefix string) string {
	// Valid s3 prefixes do not start with a '/' (e.g. myfolder/*/subdir?[a-z])
	return strings.TrimLeft(prefix, "/")
}
