package job

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"strconv"

	"github.com/thinkparq/bee-remote/internal/worker"
	"github.com/thinkparq/bee-remote/internal/workermgr"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/beesync"
	"github.com/thinkparq/protobuf/go/flex"
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
func (j *SyncJob) Allocate(rst *flex.RemoteStorageTarget) workermgr.JobSubmission {

	// TODO: Stat the file to determine how to generate segments. Consider if we
	// should store the file size anywhere so if Allocate() is called a second
	// time and the file size changes we can handle it.

	//rst := j.Request.GetSync().RemoteStorageTarget

	// TODO: Actually generate segments.
	if j.Segments == nil {
		j.Segments = make([]*SyncSegment, 0)
		j.Segments = append(j.Segments, &SyncSegment{
			segment: beesync.Segment{
				OffsetStart: 0,
				OffsetStop:  10,
				Method: &beesync.Segment_S3_{
					S3: &beesync.Segment_S3{
						MultipartId: "mpartid-01",
						PartsStart:  0,
						PartsStop:   1024,
					},
				},
			},
		})
		j.Segments = append(j.Segments, &SyncSegment{
			segment: beesync.Segment{
				OffsetStart: 10,
				OffsetStop:  20,
				Method: &beesync.Segment_S3_{
					S3: &beesync.Segment_S3{
						MultipartId: "mpartid-02",
						PartsStart:  1024,
						PartsStop:   2048,
					},
				},
			},
		})
	}

	workRequests := make([]worker.WorkRequest, 0)

	for i, s := range j.Segments {
		fmt.Printf("request: %+v\n", s.segment.GetMethod())

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
	}
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
