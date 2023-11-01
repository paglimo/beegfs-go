package job

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/thinkparq/bee-remote/internal/worker"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// Job represents an interface for tasks that can be managed by BeeRemote.
type Job interface {
	// Allocate creates a JobSubmission that can be executed by WorkerMgr on the
	// appropriate work node(s) based on the job type.
	Allocate() worker.JobSubmission
	// GetPath should return the BeeGFS path typically used as the key when
	// retrieving Jobs from the DB.
	GetPath() string
	// GetID should return the job ID generated when the job was created.
	GetID() string
	// GetStatus should return the overall status of the job.
	GetStatus() *flex.RequestStatus
	// SetStatus sets the overall status for the job. This should encompass the
	// results for individual work requests. For example if some WRs are
	// finished and others are still running the state would be RUNNING.
	SetStatus(*flex.RequestStatus)
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

// New is the standard way to generate a Job from a JobRequest.
func New(jobSeq *badger.Sequence, jobRequest *beeremote.JobRequest) (Job, error) {
	switch jobRequest.Type.(type) {
	case *beeremote.JobRequest_Sync:

		jobID, err := jobSeq.Next()
		if err != nil {
			return nil, err
		}

		job := &SyncJob{}
		job.Request = jobRequest
		job.Metadata = &flex.JobMetadata{
			Id: fmt.Sprint(jobID),
			Status: &flex.RequestStatus{
				Status:  flex.RequestStatus_UNASSIGNED,
				Message: "created",
			},
		}
		return job, nil
	}
	return nil, fmt.Errorf("bad job request")
}
