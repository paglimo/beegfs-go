package job

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/thinkparq/bee-remote/internal/rst"
	"github.com/thinkparq/bee-remote/internal/worker"
	"github.com/thinkparq/bee-remote/internal/workermgr"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// Job represents an interface for tasks that can be managed by BeeRemote.
type Job interface {
	// Allocate creates a JobSubmission that can be executed by WorkerMgr on the
	// appropriate work node(s) based on the job type. When an error occurs
	// retry will be false if the job request is invalid, typically because the
	// RST and job type are incompatible. Otherwise it will be true if an
	// ephemeral issue occurred, typically an issue contacting the RST to setup
	// any prerequisites such as creating a multipart upload.
	Allocate(rst.Client) (jobSubmission workermgr.JobSubmission, retry bool, err error)
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
	// GetStatus returns a pointer to the status of the overall job. Because it
	// returns a pointer the status and/or message can be updated directly
	// without using SetStatus(). This is helpful if you want to modify one but
	// not the other field (commonly message can change but status should not).
	GetStatus() *flex.RequestStatus
	// SetStatus sets the overall status for the job. This should encompass the
	// results for individual work requests. For example if some WRs are
	// finished and others are still running the state would be RUNNING.
	SetStatus(*flex.RequestStatus)
	// InTerminalState returns true if the job has reached a state it would not
	// continue without user interaction. Notably this indicates there are no active
	// work requests and the job is safe to be deleted without leaving orphaned
	// requests on worker nodes.
	InTerminalState() bool
	// Get returns the protocol buffer defined message representing a single Job.
	Get() *beeremote.Job
	// GetWorkRequests returns a string representation of the original work
	// requests generated for this job. It is primarily intended for
	// troubleshooting. This shouldn't just return all fields from the original
	// work request, only whatever is unique for that particular work request
	// type. For example even though work request for a SyncJob includes the
	// path and other details needed to run the job, only the request ID and a
	// particular segment need to be returned as the other fields can be
	// determined by looking elsewhere in the JobResponse.
	GetWorkRequests() string
}

// baseJob defines the fields and methods that apply to all job types.
// Individual jobs should embed a pointer to baseJob so Jobs can be retrieved
// from the database and updated directly using methods like
// Manager.updateJobState() and baseJob.SetStatus() instead of making a copy and
// having to remember to update the database entry with the updated Job.
type baseJob struct {
	// By directly storing the protobuf defined Job we can quickly return
	// responses to users about the current status of their jobs.
	*beeremote.Job
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
	return j.Metadata.GetId()
}

func (j *baseJob) GetStatus() *flex.RequestStatus {
	return j.Metadata.GetStatus()
}

func (j *baseJob) SetStatus(status *flex.RequestStatus) {
	j.Metadata.Status = status
}

// InTerminalState() indicates the job is no longer active, cannot be restarted,
// and will not conflict with a new job. This should mirror the
// InTerminalState() method for WorkResults.
func (j *baseJob) InTerminalState() bool {
	status := j.GetStatus()
	return status.Status == flex.RequestStatus_COMPLETED || status.Status == flex.RequestStatus_CANCELLED
}

// New is the standard way to generate a Job from a JobRequest.
func New(jobSeq *badger.Sequence, jobRequest *beeremote.JobRequest) (Job, error) {

	jobID, err := jobSeq.Next()
	if err != nil {
		return nil, err
	}

	jobMetadata := &flex.JobMetadata{
		Id: fmt.Sprint(jobID),
		Status: &flex.RequestStatus{
			Status:  flex.RequestStatus_UNASSIGNED,
			Message: "created",
		},
	}

	baseJob := &baseJob{
		Job: &beeremote.Job{
			Request:  jobRequest,
			Metadata: jobMetadata,
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
