package job

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"

	"github.com/thinkparq/bee-remote/internal/worker"
	beegfs "github.com/thinkparq/protobuf/beegfs/go"
	br "github.com/thinkparq/protobuf/beeremote/go"
	bs "github.com/thinkparq/protobuf/beesync/go"
	"google.golang.org/protobuf/proto"
)

// SyncJob implements all methods needed for BeeRemote to handle sync jobs. Its
// embeds all the protocol buffer define types needed for BeeRemote to associate
// what work needs to be done with how it is being carried out.
type SyncJob struct {
	// By directly storing the protobuf defined response we can quickly return
	// responses to users about the current status of their jobs.
	br.JobResponse
	// Segments aren't populated until Allocate() is called.
	Segments []*SyncSegment
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
func (j *SyncJob) Allocate() worker.JobSubmission {

	// TODO: Stat the file to determine how to generate segments. Consider if we
	// should store the file size anywhere so if Allocate() is called a second
	// time and the file size changes we can handle it.

	// TODO: Actually generate segments.
	if j.Segments == nil {
		j.Segments = make([]*SyncSegment, 0)
		j.Segments = append(j.Segments, &SyncSegment{
			segment: bs.Segment{
				OffsetStart: 0,
				OffsetStop:  10,
				Method: &bs.Segment_S3_{
					S3: &bs.Segment_S3{
						MultipartId: "mpartid-01",
						PartsStart:  0,
						PartsStop:   1024,
					},
				},
			},
		})
		j.Segments = append(j.Segments, &SyncSegment{
			segment: bs.Segment{
				OffsetStart: 10,
				OffsetStop:  20,
				Method: &bs.Segment_S3_{
					S3: &bs.Segment_S3{
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
			SyncRequest: &bs.SyncRequest{
				RequestId: fmt.Sprint(i),
				Metadata:  j.GetMetadata(),
				Path:      j.GetPath(),
				Job:       j.Request.GetSync(),
				Segment:   &s.segment,
			},
		}
		workRequests = append(workRequests, &wr)

	}
	return worker.JobSubmission{
		JobID:        j.Metadata.GetId(),
		WorkRequests: workRequests,
	}
}

// GetPath returns the path to the BeeGFS entry this job is running against.
func (j *SyncJob) GetPath() string {
	return j.Request.GetPath()
}

// GetID returns the job ID.
func (j *SyncJob) GetID() string {
	return j.Metadata.GetId()
}

func (j *SyncJob) GetStatus() *beegfs.RequestStatus {
	return j.Metadata.GetStatus()
}

func (j *SyncJob) SetStatus(status *beegfs.RequestStatus) {
	j.Metadata.Status = status
}

// GobEncode encodes the SyncJob into a byte slice for gob serialization. The
// method serializes the JobResponse using protobufs and the WorkRequests using
// gob. It prefixes the serialized WorkRequests data with its length to handle
// variable-length slices during deserialization. It is primarily used when
// storing SyncJobs in the database.
func (j *SyncJob) GobEncode() ([]byte, error) {

	// We use the proto.Marshal function because gob doesn't work properly with
	// the oneof type field in the JobRequest struct.
	jobResponseData, err := proto.Marshal(&j.JobResponse)
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
// the length prefix to determine the size of the WorkRequests data. It then
// decodes WorkRequests using gob and JobResponse using protobufs. It is
// primarily used when retrieving SyncJobs from the database.
func (j *SyncJob) GobDecode(data []byte) error {

	jobResponseLength := binary.BigEndian.Uint16(data[:2])
	jobResponseData := data[2 : 2+jobResponseLength]
	workRequestData := data[2+jobResponseLength:]

	err := proto.Unmarshal(jobResponseData, &j.JobResponse)
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
	segment bs.Segment
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
