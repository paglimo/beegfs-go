package job

import (
	"fmt"
	"strconv"

	"github.com/thinkparq/bee-remote/internal/worker"
	"github.com/thinkparq/bee-remote/internal/workermgr"
	"github.com/thinkparq/gobee/filesystem"
	"github.com/thinkparq/gobee/rst"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

// SyncJob implements all methods needed for BeeRemote to handle sync jobs.
type SyncJob struct {
	*baseJob
}

// Verify SyncJob implements the Job interface.
var _ Job = &SyncJob{}

// Allocate breaks the file into segments taking into consideration file size,
// operation type, and transfer method (S3, POSIX, etc.). It uses these segments
// to return a WorkSubmission containing work requests that can be distributed
// across multiple BeeSync nodes.
//
// We intentionally don't store individual work requests requests on-disk as
// they contain a lot of duplicate information (considering each BeeSync node
// needs a copy of the request). Instead we just store the segments and ensure
// Allocate() is idempotent. Allocate() will not regenerate segments after they
// are initially generated and can be used to regenerate the original
// WorkSubmission. This allows Allocate() to be called multiple times to check
// on the status of outstanding work requests (e.g., after an app crash or
// because a user requests this).
//
// IMPORTANT: After initially generating segments subsequent calls to allocate
// will not recheck the size of the file to determine if the generated segments
// are still valid, the original segments will always be returned. This is to
// discourage misuse of the Allocate() function as a method to determine if a
// file has changed and it is safe to resume a job or if it should be cancelled.
func (j *SyncJob) Allocate(client rst.Client) (workermgr.JobSubmission, bool, error) {

	if j.Segments == nil {
		op := j.Request.GetSync().Operation
		if op != flex.SyncJob_UPLOAD && op != flex.SyncJob_DOWNLOAD {
			return workermgr.JobSubmission{}, false, fmt.Errorf("unable to allocate job: %w", ErrUnknownJobOp)
		}

		switch client.GetType() {
		case rst.S3:
		default:
			return workermgr.JobSubmission{}, false, fmt.Errorf("unable to allocate job: %w", ErrIncompatibleNodeAndRST)
		}

		stat, err := filesystem.MountPoint.Stat(j.GetPath())
		if err != nil {
			// The most likely reason for an error is the path wasn't found because
			// we got a job request for a file in BeeGFS that didn't exist or was
			// removed after the job request was submitted. Less likely there was
			// some transient network/other issue preventing us from talking to
			// BeeGFS that could actually be retried. Until there is a reason to add
			// more complex error handling lets just treat all stat errors as fatal.
			return workermgr.JobSubmission{}, false, err
		}
		fileSize := stat.Size()

		segCount, partsPerSegment := client.RecommendedSegments(fileSize)
		if segCount > 1 && op == flex.SyncJob_UPLOAD {
			uploadID, err := client.CreateUpload(j.GetPath())
			if err != nil {
				return workermgr.JobSubmission{}, true, err
			}
			j.ExternalId = uploadID
		}

		// If the file is empty then set bytesPerSegment to 1 so we don't have
		// to do anything special to the logic below. If we don't do this then
		// OffsetStop would be -1 which is confusing and may break elsewhere.
		var bytesPerSegment int64 = 1
		if fileSize != 0 {
			bytesPerSegment = fileSize / segCount
		}
		extraBytesForLastSegment := fileSize % segCount
		j.Segments = make([]*Segment, 0)

		// Based on the RST type generate the appropriate BeeSync segments.
		// We have to use a int64 counter for byte ranges inside the file
		// and a int32 counter for the parts. This is probably slightly
		// faster/cleaner than constantly recasting each iteration.
		for i64, i32 := int64(0), int32(1); i64 < segCount; i64, i32 = i64+1, i32+1 {
			offsetStop := (i64+1)*bytesPerSegment - 1
			if i64 == segCount-1 {
				// If the number of bytes cannot be divided evenly into the
				// number of segments, just add the extra bytes to the last
				// segment. S3 multipart uploads allow the last part to be any
				// size.
				offsetStop += extraBytesForLastSegment
			}

			segment := &Segment{
				segment: flex.WorkRequest_Segment{
					OffsetStart: i64 * bytesPerSegment,
					OffsetStop:  offsetStop,
					PartsStart:  (i32-1)*partsPerSegment + 1,
					PartsStop:   i32 * partsPerSegment,
				},
			}
			j.Segments = append(j.Segments, segment)
		}
	}

	return workermgr.JobSubmission{
		JobID:        j.GetId(),
		WorkRequests: j.GetWorkRequests(),
	}, false, nil
}

func (j *SyncJob) GetWorkRequests() []*flex.WorkRequest {

	workRequests := make([]*flex.WorkRequest, 0)
	for i, s := range j.Segments {
		wr := flex.WorkRequest{
			JobId:      j.GetID(),
			RequestId:  strconv.Itoa(i),
			ExternalId: j.ExternalId,
			Path:       j.GetPath(),
			Status:     proto.Clone(j.Status()).(*flex.RequestStatus),
			Segment:    &s.segment,
			Type: &flex.WorkRequest_Sync{
				Sync: j.Request.GetSync(),
			},
			RemoteStorageTarget: j.Request.RemoteStorageTarget,
		}
		workRequests = append(workRequests, &wr)
	}
	return workRequests
}

func (j *SyncJob) Complete(client rst.Client, results map[string]worker.WorkResult, abort bool) error {

	op := j.Request.GetSync().Operation
	if op != flex.SyncJob_UPLOAD && op != flex.SyncJob_DOWNLOAD {
		return fmt.Errorf("unable to complete job: %w", ErrUnknownJobOp)
	}

	switch client.GetType() {
	case rst.S3:
	default:
		return fmt.Errorf("unable to complete job: %w", ErrIncompatibleNodeAndRST)
	}

	if op == flex.SyncJob_UPLOAD && j.ExternalId != "" {
		if abort {
			return client.AbortUpload(j.ExternalId, j.GetPath())
		}
		// TODO: There could be lots of parts. Look for ways to optimize this.
		// Like if we could determine the total number of parts and make an
		// appropriately sized slice up front. Or if we could just pass the RST
		// method the unmodified map since it potentially has to iterate over
		// the slice to convert it to the type it expects anyway. The drawback
		// with the latter approach is the RST package would import a BeeRemote
		// package and the goal has been to break this out into a standalone
		// package to reuse for BeeSync.
		partsToFinish := make([]*flex.WorkResponse_Part, 0)
		for _, r := range results {
			partsToFinish = append(partsToFinish, r.WorkResponse.CompletedParts...)
		}
		return client.FinishUpload(j.ExternalId, j.GetPath(), partsToFinish)
	}
	return nil // Nothing to do.
}

// GobEncode encodes the SyncJob into a byte slice for gob serialization. Refer
// to the GobEncode method on baseJob if you are implementing a new job type.
func (j *SyncJob) GobEncode() ([]byte, error) {

	if j.baseJob == nil {
		return nil, fmt.Errorf("cannot encode a nil job (most likely this is a bug somewhere else)")
	}
	return j.baseJob.GobEncode()
}

// GobDecode decodes a byte slice into the SyncJob. Refer to the GobDecode
// method on baseJob if you are implementing a new job type.
func (j *SyncJob) GobDecode(data []byte) error {

	if j.baseJob == nil {
		j.baseJob = &baseJob{}
	}

	return j.baseJob.GobDecode(data)
}
