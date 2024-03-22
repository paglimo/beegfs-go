package job

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"slices"

	"github.com/dgraph-io/badger/v4"
	"github.com/thinkparq/bee-remote/internal/worker"
	"github.com/thinkparq/bee-remote/internal/workermgr"
	"github.com/thinkparq/gobee/filesystem"
	"github.com/thinkparq/gobee/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

// Job represents a task that can be managed by BeeRemote.
type Job struct {
	// By directly storing the protobuf defined Job we can quickly return
	// responses to users about the current status of their jobs.
	*beeremote.Job
	// Segments aren't populated until GetSegments() is called.
	Segments []*Segment
	// Mapping of request IDs to their results. We intentionally don't store individual work
	// requests on-disk as they contain a lot of duplicate information that can be regenerated as
	// needed from the job request and segments.
	WorkResults map[string]worker.WorkResult
	// IMPORTANT: When adding fields update the gob serialization and deserialization methods.
}

// Get() returns the protocol buffer defined message representing a single Job.
//
// IMPORTANT: This method returns a reference to a Job instance. Modifying the returned object
// directly affects the original Job. This approach should not be used when you need a deep copy of
// the Job for modifications or initializations of individual fields on a new instance, such as
// status of a derived work request. For those cases, use proto.Clone() to create a deep copy of the
// Job or particular field (like status), ensuring that changes do not impact the original instance.
func (j *Job) Get() *beeremote.Job {
	return j.Job
}

// GetSegments() is used anywhere we need a slice of the protobuf defined segments. Notably for use
// with rst.RecreateWorkRequests(). This is needed because in many places we can't directly use
// j.Segments because the entries are the a wrapper type around WorkRequest_Segment so they can be
// stored in the DB. It would be nice to optimize all of this, but for most jobs this should only
// need to be called once when the job is created.
func (j *Job) GetSegments() []*flex.WorkRequest_Segment {
	segments := make([]*flex.WorkRequest_Segment, 0, len(j.Segments))
	for _, s := range j.Segments {
		segments = append(segments, s.segment)
	}
	return segments
}

// InTerminalState() returns true the job cannot cannot be restarted, and there are no active work
// requests that would conflict with a new job. Jobs in this state are safe to be deleted without
// leaving orphaned requests on worker nodes. This should mirror the InTerminalState() method for
// WorkResults.
func (j *Job) InTerminalState() bool {
	status := j.GetStatus()
	return status.State == flex.RequestStatus_COMPLETED || status.State == flex.RequestStatus_CANCELLED
}

// GenerateSubmission creates a JobSubmission containing one or more work requests that can be
// executed by WorkerMgr on the appropriate work node(s) based on the job type. Requests are
// generated based on the RST taking into consideration file size, worker node availability,
// operation type, and transfer method (S3, POSIX, etc.). When an error occurs retry will be false
// if the job request is invalid, typically because the RST and job type are incompatible. Otherwise
// it will be true if an ephemeral issue occurred, typically an issue contacting the RST to setup
// any prerequisites such as creating a multipart upload.
//
// We intentionally don't store individual work requests requests on-disk as they contain a lot of
// duplicate information (considering each BeeSync node needs a copy of the request). Instead we
// just store the segments and ensure GenerateSubmission() is idempotent. As a result
// GenerateSubmission() will not regenerate segments after they are initially generated and can be
// used to regenerate the original WorkSubmission. This allows GenerateSubmission() to be called
// multiple times to check on the status of outstanding work requests (e.g., after an app crash or
// because a user requests this).
//
// IMPORTANT: After initially generating segments subsequent calls to GenerateSubmission will not
// recheck the size of the file to determine if the generated segments are still valid, the original
// segments will always be returned. This is to discourage misuse of the GenerateSubmission()
// function as a method to determine if a file has changed and it is safe to resume a job or if it
// should be cancelled. It also ensures even if the file changes the original job submission can be
// recreated for troubleshooting.
func (j *Job) GenerateSubmission(rstClient rst.Client) (workermgr.JobSubmission, bool, error) {

	var workRequests []*flex.WorkRequest

	if j.Segments == nil {
		stat, err := filesystem.MountPoint.Stat(j.Request.GetPath())
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
		var canRetry bool
		workRequests, canRetry, err = rstClient.GenerateRequests(j.Get(), fileSize, 0)
		if err != nil {
			return workermgr.JobSubmission{}, canRetry, err
		}

		j.Segments = make([]*Segment, 0, len(workRequests))
		for _, wr := range workRequests {
			seg := proto.Clone(wr.Segment).(*flex.WorkRequest_Segment)
			j.Segments = append(j.Segments, &Segment{segment: seg})
		}

	} else {
		workRequests = rst.RecreateWorkRequests(j.Get(), j.GetSegments())
	}

	return workermgr.JobSubmission{
		JobID:        j.GetId(),
		WorkRequests: workRequests,
	}, false, nil
}

// Complete should always be called before moving the job status to a terminal state. If the job
// completed successfully and all work results are complete then abort should be false, otherwise it
// can be set to true to cancel the job. The actions it takes depends on the job and RST type. For
// example completing or aborting a multipart upload for a sync job to an S3 target. Note this is
// largely just a wrapper around the rst.Client CompleteRequests method to handle converting between
// data types used by the Job and the RST packages.
func (j *Job) Complete(client rst.Client, abort bool) error {
	workResponses := make([]*flex.WorkResponse, 0, len(j.WorkResults))
	for _, r := range j.WorkResults {
		workResponses = append(workResponses, r.WorkResponse)
	}
	return client.CompleteRequests(j.Get(), workResponses, abort)
}

// New is the standard way to generate a Job from a JobRequest.
func New(jobSeq *badger.Sequence, jobRequest *beeremote.JobRequest) (*Job, error) {

	jobID, err := jobSeq.Next()
	if err != nil {
		return nil, err
	}

	newJob := &Job{
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
		return newJob, nil
	case *beeremote.JobRequest_Mock:
		return newJob, nil
	}
	return nil, fmt.Errorf("bad job request")
}

// GobEncode encodes the job into a byte slice for gob serialization. The method serializes the
// JobResponse using the protobuf marshaller and the Segments slice field using gob and individual
// segments using the protobuf unmarshaller. It prefixes the serialized Job field with its length to
// handle variable-length slices during deserialization. It is primarily used when storing jobs in
// the database.
func (j *Job) GobEncode() ([]byte, error) {

	// We use the proto.Marshal function because gob doesn't work properly with
	// the oneof type field in the Job.JobRequest message.
	jobData, err := proto.Marshal(j.Job)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	// We use Gob to encode the slice of segments. The segment.GobEncode method will be called for
	// each element to properly encode it using the protobuf marshaller.
	if err := enc.Encode(j.Segments); err != nil {
		return nil, err
	}

	// WorkResults contain proto defined WorkResponses, but these don't use a oneof type so they can
	// be encoded with Gob directly.
	if err := enc.Encode(j.WorkResults); err != nil {
		return nil, err
	}

	// Prefix the serialized jobData with the length as a uint16.
	// We'll use this when decoding.
	jobFieldLength := make([]byte, 2)
	binary.BigEndian.PutUint16(jobFieldLength, uint16((len(jobData))))
	return slices.Concat(jobFieldLength, jobData, buf.Bytes()), nil
}

// GobDecode decodes a byte slice into a baseJob. It first extracts the length prefix to determine
// the size of the Job field. It then decodes the Job field using the protobuf unmarshaller and
// Segments slice field using gob and individual segments using the protobuf unmarshaller. Remaining
// fields do not require special handling and are just decoded using gob. It is primarily used when
// retrieving Jobs from the database.
func (j *Job) GobDecode(data []byte) error {

	jobFieldLength := binary.BigEndian.Uint16(data[:2])
	jobData := data[2 : 2+jobFieldLength]
	remainingData := data[2+jobFieldLength:]

	if j.Job == nil {
		j.Job = &beeremote.Job{}
	}

	if j.Segments == nil {
		j.Segments = make([]*Segment, 0)
	}

	if j.WorkResults == nil {
		j.WorkResults = make(map[string]worker.WorkResult)
	}

	if err := proto.Unmarshal(jobData, j.Job); err != nil {
		return err
	}

	dec := gob.NewDecoder(bytes.NewReader(remainingData))
	if err := dec.Decode(&j.Segments); err != nil {
		return nil
	}
	return dec.Decode(&j.WorkResults)
}

// Segment is a wrapper around the protobuf defined Segment type to
// allow encoding/decoding to work properly through encoding/gob.
type Segment struct {
	segment *flex.WorkRequest_Segment
}

func (s *Segment) GobEncode() ([]byte, error) {
	segmentData, err := proto.Marshal(s.segment)
	if err != nil {
		return nil, err
	}

	return segmentData, nil

}

func (s *Segment) GobDecode(data []byte) error {
	if s.segment == nil {
		s.segment = &flex.WorkRequest_Segment{}
	}

	err := proto.Unmarshal(data, s.segment)

	return err
}
