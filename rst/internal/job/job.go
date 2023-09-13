package job

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"

	beegfs "github.com/thinkparq/protobuf/beegfs/go"
	br "github.com/thinkparq/protobuf/beeremote/go"
	bs "github.com/thinkparq/protobuf/beesync/go"
	"google.golang.org/protobuf/proto"
)

// Job represents an interface for tasks that can be managed by BeeRemote.
type Job interface {
	// Allocate handles setting up WorkRequests that can be executed by
	// WorkerMgr on the appropriate work node(s) based on the job type.
	Allocate()
	// GetPath should return the BeeGFS path typically used as the key when
	// retrieving Jobs from the DB.
	GetPath() string
}

// New is the standard way to generate a Job from a JobRequest.
func New(jobRequest *br.JobRequest) (Job, error) {
	switch jobRequest.Type.(type) {
	case *br.JobRequest_Sync:

		job := &SyncJob{}
		job.Request = jobRequest
		job.Metadata = &beegfs.JobMetadata{
			Id: "TODO",
			Status: &beegfs.RequestStatus{
				Status:  beegfs.RequestStatus_UNASSIGNED,
				Message: "created",
			},
		}
		return job, nil
	}
	return nil, fmt.Errorf("bad job request")
}

// Verify SyncJob implements the Job interface.
var _ Job = &SyncJob{}

// SyncJob implements all methods needed for BeeRemote to handle sync jobs. Its
// embeds all the protocol buffer define types needed for BeeRemote to associate
// what work needs to be done forwith how it is being carried out.
type SyncJob struct {
	br.JobResponse
	WorkRequests map[string]*SyncRequest
}

// Allocate generates WorkRequests taking into consideration file size,
// operation type, and transfer method (S3, POSIX, etc.)
func (j *SyncJob) Allocate() {

	// TODO: Actually implement.
	if j.WorkRequests == nil {
		j.WorkRequests = make(map[string]*SyncRequest)
		j.WorkRequests["test-beesync-node-1"] = &SyncRequest{
			segment: bs.Segment{
				OffsetStart: 0,
				OffsetStop:  10,
				Method: &bs.Segment_S3_{
					S3: &bs.Segment_S3{
						MultipartId: "mpartid",
						PartsStart:  0,
						PartsStop:   1024,
					},
				},
			},
		}
		fmt.Printf("new allocation for job ID %s, name %s\n", j.Metadata.Id, j.Request.Name)
	} else {
		fmt.Printf("already allocated job ID %s, name %s\n", j.Metadata.Id, j.Request.Name)
	}

	for _, req := range j.WorkRequests {
		fmt.Printf("request: %+v\n", req.segment.GetMethod())
	}
}

// GetPath returns the path to the BeeGFS entry this job is running against.
func (j *SyncJob) GetPath() string {
	return j.Request.GetSync().Entry.Path
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

	err = enc.Encode(j.WorkRequests)
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
	err = dec.Decode(&j.WorkRequests)
	if err != nil {
		return err
	}
	return nil
}

type SyncRequest struct {
	segment bs.Segment
}

func (r *SyncRequest) GobEncode() ([]byte, error) {
	segmentData, err := proto.Marshal(&r.segment)
	if err != nil {
		return nil, err
	}

	return segmentData, nil

}

func (r *SyncRequest) GobDecode(data []byte) error {
	err := proto.Unmarshal(data, &r.segment)

	return err
}
