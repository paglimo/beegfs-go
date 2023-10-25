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
