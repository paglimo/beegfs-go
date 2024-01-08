package job

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/thinkparq/bee-remote/internal/rst"
	"github.com/thinkparq/bee-remote/internal/worker"
	"github.com/thinkparq/bee-remote/internal/workermgr"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

// Job represents an interface for tasks that can be managed by BeeRemote.
// Common methods like Status() are implemented by baseJob. Methods that provide
// functionality specific to a particular job type (like Allocate()) must be
// implemented by the various concrete job types.
type Job interface {
	// Allocate creates a JobSubmission that can be executed by WorkerMgr on the
	// appropriate work node(s) based on the job type. When an error occurs
	// retry will be false if the job request is invalid, typically because the
	// RST and job type are incompatible. Otherwise it will be true if an
	// ephemeral issue occurred, typically an issue contacting the RST to setup
	// any prerequisites such as creating a multipart upload.
	Allocate(rst.Client) (jobSubmission workermgr.JobSubmission, retry bool, err error)
	// GetWorkRequests is used to generate work requests for a job based on its
	// segments. Because WorkRequests duplicate a lot of the information
	// contained in the Job they are not stored on-disk. Instead they are
	// initially generated when Allocate() is called to create a JobSubmission,
	// and can be subsequently recreated as needed for troubleshooting.
	GetWorkRequests() []*flex.WorkRequest
	// Complete should always be called before moving the job status to a
	// terminal state. If the job completed successfully and all work results
	// are complete then abort should be false, otherwise it can be set to true
	// to cancel the job. The actions it takes depends on the job and RST type.
	// For example completing or aborting a multipart upload for a sync job to
	// an S3 target.
	Complete(client rst.Client, results map[string]worker.WorkResult, abort bool) error
	// GetPath should return the BeeGFS path typically used as the key when
	// retrieving Jobs from the DB.
	GetPath() string
	// GetID should return the job ID generated when the job was created.
	GetID() string
	// GetRST should return the ID of the RST this job was created for.
	// This is used to determine if a job already exists for a particular
	// RST and should be rejected. An empty string should be returned
	// if RST has no meaning for a particular job type.
	GetRSTID() string
	// Status returns a pointer to the status of the overall job. Because it
	// returns a pointer the status and/or message can be updated directly. This
	// allows you to modify one but not the other field (commonly message can
	// change but status should not). The state should encompass the results for
	// individual work requests. For example if some WRs are finished and others
	// are still running the state would be RUNNING.
	Status() *flex.RequestStatus
	// InTerminalState returns true if the job has reached a state it would not
	// continue without user interaction. Notably this indicates there are no active
	// work requests and the job is safe to be deleted without leaving orphaned
	// requests on worker nodes.
	InTerminalState() bool
	// Get returns the protocol buffer defined message representing a single Job.
	Get() *beeremote.Job
}

// baseJob defines the fields and methods that apply to all job types.
// Individual jobs should embed a pointer to baseJob.
type baseJob struct {
	// By directly storing the protobuf defined Job we can quickly return
	// responses to users about the current status of their jobs.
	*beeremote.Job
	// Segments aren't populated until Allocate() is called.
	Segments []*Segment
}

func (j *baseJob) Get() *beeremote.Job {
	return j.Job
}

func (j *baseJob) GetRSTID() string {
	return j.GetRequest().RemoteStorageTarget
}

// GetPath returns the path to the BeeGFS entry this job is running against.
func (j *baseJob) GetPath() string {
	return j.Request.GetPath()
}

// GetID returns the job ID.
func (j *baseJob) GetID() string {
	return j.GetId()
}

func (j *baseJob) Status() *flex.RequestStatus {
	return j.GetStatus()
}

// InTerminalState() indicates the job is no longer active, cannot be restarted,
// and will not conflict with a new job. This should mirror the
// InTerminalState() method for WorkResults.
func (j *baseJob) InTerminalState() bool {
	status := j.Status()
	return status.State == flex.RequestStatus_COMPLETED || status.State == flex.RequestStatus_CANCELLED
}

// New is the standard way to generate a Job from a JobRequest.
func New(jobSeq *badger.Sequence, jobRequest *beeremote.JobRequest) (Job, error) {

	jobID, err := jobSeq.Next()
	if err != nil {
		return nil, err
	}

	baseJob := &baseJob{
		Job: &beeremote.Job{
			Id:      fmt.Sprint(jobID),
			Request: jobRequest,
			Status: &flex.RequestStatus{
				State:   flex.RequestStatus_UNASSIGNED,
				Message: "created",
			},
		},
	}

	switch jobRequest.Type.(type) {
	case *beeremote.JobRequest_Sync:
		job := &SyncJob{baseJob: baseJob}
		return job, nil
	case *beeremote.JobRequest_Mock:
		job := &MockJob{baseJob: baseJob}
		return job, nil
	}
	return nil, fmt.Errorf("bad job request")
}

// GobEncode encodes the job into a byte slice for gob serialization. The method
// serializes the JobResponse using the protobuf marshaller and the Segments
// slice field using gob and individual segments using the protobuf
// unmarshaller. It prefixes the serialized Job field with its length to handle
// variable-length slices during deserialization. It is primarily used when
// storing jobs in the database.
//
// IMPORTANT: Concrete jobs (like SyncJobs) must still implement GobEncode and
// GobDecode methods. However unless the job has additional fields that require
// special handling these methods can simply call the GobEncode/GobDecode
// methods of the base job. Don't forget the to update the Job package init()
// function to add `gob.Register(&MyType{})` for any new types.
func (j *baseJob) GobEncode() ([]byte, error) {

	// We use the proto.Marshal function because gob doesn't work properly with
	// the oneof type field in the Job.JobRequest message.
	jobData, err := proto.Marshal(j.Job)
	if err != nil {
		return nil, err
	}

	// We use Gob to encode the slice of segments. The segment.GobEncode method
	// will be called for each element to properly encode it using the protobuf
	// marshaller.
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	err = enc.Encode(j.Segments)
	if err != nil {
		return nil, err
	}

	segmentsData := buf.Bytes()

	// Prefix the serialized jobData with the length as a uint16.
	// We'll use this when decoding.
	jobFieldLength := make([]byte, 2)
	binary.BigEndian.PutUint16(jobFieldLength, uint16((len(jobData))))

	combinedData := append(jobFieldLength, jobData...)
	combinedData = append(combinedData, segmentsData...)
	return combinedData, nil
}

// GobDecode decodes a byte slice into a baseJob. It first extracts the length
// prefix to determine the size of the Job field. It then decodes the Job field
// using the protobuf unmarshaller and Segments slice field using gob an
// individual segments using the protobuf unmarshaller. It is primarily used
// when retrieving Jobs from the database.
//
// IMPORTANT: Concrete jobs (like SyncJobs) must still implement GobEncode and
// GobDecode methods. However unless the job has additional fields that require
// special handling these methods can simply call the GobEncode/GobDecode
// methods of the base job. Don't forget the to update the Job package init()
// function to add `gob.Register(&MyType{})` for any new types.
func (j *baseJob) GobDecode(data []byte) error {

	jobFieldLength := binary.BigEndian.Uint16(data[:2])
	jobData := data[2 : 2+jobFieldLength]
	segmentsData := data[2+jobFieldLength:]

	if j.Job == nil {
		j.Job = &beeremote.Job{}
	}

	if j.Segments == nil {
		j.Segments = make([]*Segment, 0)
	}

	err := proto.Unmarshal(jobData, j.Job)
	if err != nil {
		return err
	}

	dec := gob.NewDecoder(bytes.NewReader(segmentsData))
	err = dec.Decode(&j.Segments)
	if err != nil {
		return err
	}
	return nil
}

// Segment is a wrapper around the protobuf defined Segment type to
// allow encoding/decoding to work properly through encoding/gob.
type Segment struct {
	segment flex.WorkRequest_Segment
}

func (s *Segment) GobEncode() ([]byte, error) {
	segmentData, err := proto.Marshal(&s.segment)
	if err != nil {
		return nil, err
	}

	return segmentData, nil

}

func (s *Segment) GobDecode(data []byte) error {
	err := proto.Unmarshal(data, &s.segment)

	return err
}
