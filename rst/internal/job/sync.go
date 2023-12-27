package job

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"strconv"

	"github.com/thinkparq/bee-remote/internal/filesystem"
	"github.com/thinkparq/bee-remote/internal/rst"
	"github.com/thinkparq/bee-remote/internal/worker"
	"github.com/thinkparq/bee-remote/internal/workermgr"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/beesync"
	"google.golang.org/protobuf/proto"
)

// SyncJob implements all methods needed for BeeRemote to handle sync jobs. Its
// embeds all the protocol buffer define types needed for BeeRemote to associate
// what work needs to be done with how it is being carried out.
type SyncJob struct {
	*baseJob
	// Segments aren't populated until Allocate() is called.
	Segments []*SyncSegment
}

// Verify SyncJob implements the Job interface.
var _ Job = &SyncJob{}

func (j *SyncJob) GetRSTID() string {
	return j.GetRequest().GetSync().RemoteStorageTarget
}

func (j *SyncJob) GetWorkRequests() string {

	var output string
	for i, segment := range j.Segments {
		output += fmt.Sprintf("{request_id: %d, segment: %s}", i, segment.segment.String())
	}

	return output
}

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

		uploadID := ""
		rstType, segCount, partsPerSegment := client.RecommendedSegments(fileSize)

		if j.Request.GetSync().Operation == beesync.SyncJob_UNKNOWN {
			return workermgr.JobSubmission{}, false, ErrUnknownJobOp
		} else if segCount > 1 && j.Request.GetSync().Operation == beesync.SyncJob_UPLOAD {
			uploadID, err = client.CreateUpload()
			if err != nil {
				return workermgr.JobSubmission{}, true, err
			}
		}

		bytesPerSegment := fileSize / segCount
		extraBytesForLastSegment := fileSize % segCount
		j.Segments = make([]*SyncSegment, 0)

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

			segment := &SyncSegment{
				segment: beesync.Segment{
					OffsetStart: i64 * bytesPerSegment,
					OffsetStop:  offsetStop,
				},
			}
			switch rstType {
			case rst.S3:
				segment.segment.Method = &beesync.Segment_S3_{
					S3: &beesync.Segment_S3{
						MultipartId: uploadID,
						PartsStart:  (i32-1)*partsPerSegment + 1,
						PartsStop:   i32 * partsPerSegment,
					},
				}
			default:
				return workermgr.JobSubmission{}, false, fmt.Errorf("BeeSync nodes do not support %s remote storage targets", rstType)
			}
			j.Segments = append(j.Segments, segment)
		}
	}

	workRequests := make([]worker.WorkRequest, 0)

	for i, s := range j.Segments {
		wr := worker.SyncRequest{
			SyncRequest: &beesync.SyncRequest{
				RequestId: strconv.Itoa(i),
				Metadata:  j.GetMetadata(),
				Path:      j.GetPath(),
				Job:       j.Request.GetSync(),
				Segment:   &s.segment,
			},
		}
		workRequests = append(workRequests, &wr)
	}
	return workermgr.JobSubmission{
		JobID:        j.Metadata.GetId(),
		WorkRequests: workRequests,
	}, false, nil
}

// GobEncode encodes the SyncJob into a byte slice for gob serialization. The
// method serializes the JobResponse using the protobuf marshaller and the
// Segments using gob. It prefixes the serialized JobResponse data with its
// length to handle variable-length slices during deserialization. It is
// primarily used when storing SyncJobs in the database.
//
// IMPORTANT: If you are using this as a reference to implement custom
// encoding/decoding of a new type, don't forget the to update the package
// init() function to add `gob.Register(&MyType{})`.
func (j *SyncJob) GobEncode() ([]byte, error) {

	// We use the proto.Marshal function because gob doesn't work properly with
	// the oneof type field in the JobRequest struct.
	jobResponseData, err := proto.Marshal(j.Job)
	if err != nil {
		return nil, err
	}

	// TODO: Determine if we also need to use protobuf to encode the WorkRequests.
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	err = enc.Encode(j.Segments)
	if err != nil {
		return nil, err
	}

	workRequestsData := buf.Bytes()

	// Prefix the serialized JobResponseData with the length as a uint16.
	// We'll use this when decoding.
	jobResponseLength := make([]byte, 2)
	binary.BigEndian.PutUint16(jobResponseLength, uint16((len(jobResponseData))))

	combinedData := append(jobResponseLength, jobResponseData...)
	combinedData = append(combinedData, workRequestsData...)
	return combinedData, nil
}

// GobDecode decodes a byte slice into the SyncJob. The method first extracts
// the length prefix to determine the size of the JobResponse data. It then
// decodes JobResponse using the protobuf unmarshaller and Segments using gob.
// It is primarily used when retrieving SyncJobs from the database.
//
// IMPORTANT: If you are using this as a reference to implement custom
// encoding/decoding of a new type, don't forget the to update the package
// init() function to add `gob.Register(&MyType{})`.
func (j *SyncJob) GobDecode(data []byte) error {

	jobResponseLength := binary.BigEndian.Uint16(data[:2])
	jobResponseData := data[2 : 2+jobResponseLength]
	workRequestData := data[2+jobResponseLength:]

	if j.baseJob == nil {
		j.baseJob = &baseJob{
			Job: &beeremote.Job{},
		}
	}

	if j.Segments == nil {
		j.Segments = make([]*SyncSegment, 0)
	}

	err := proto.Unmarshal(jobResponseData, j.Job)
	if err != nil {
		return err
	}

	dec := gob.NewDecoder(bytes.NewReader(workRequestData))
	err = dec.Decode(&j.Segments)
	if err != nil {
		return err
	}
	return nil
}

// SyncSegment is a wrapper around the protobuf defined BeeSync Segment type to
// allow encoding/decoding to work properly through encoding/gob.
type SyncSegment struct {
	segment beesync.Segment
}

func (r *SyncSegment) GobEncode() ([]byte, error) {
	segmentData, err := proto.Marshal(&r.segment)
	if err != nil {
		return nil, err
	}

	return segmentData, nil

}

func (r *SyncSegment) GobDecode(data []byte) error {
	err := proto.Unmarshal(data, &r.segment)

	return err
}
