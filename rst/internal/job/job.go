package job

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/thinkparq/bee-remote/internal/worker"
	beegfs "github.com/thinkparq/protobuf/beegfs/go"
	br "github.com/thinkparq/protobuf/beeremote/go"
)

// Job represents an interface for tasks that can be managed by BeeRemote.
type Job interface {
	// Allocate creates a JobSubmission that can be executed by WorkerMgr on the
	// appropriate work node(s) based on the job type.
	Allocate() worker.JobSubmission
	// GetPath should return the BeeGFS path typically used as the key when
	// retrieving Jobs from the DB.
	GetPath() string
}

// New is the standard way to generate a Job from a JobRequest.
func New(jobSeq *badger.Sequence, jobRequest *br.JobRequest) (Job, error) {
	switch jobRequest.Type.(type) {
	case *br.JobRequest_Sync:

		jobID, err := jobSeq.Next()
		if err != nil {
			return nil, err
		}

		job := &SyncJob{}
		job.Request = jobRequest
		job.Metadata = &beegfs.JobMetadata{
			Id: fmt.Sprint(jobID),
			Status: &beegfs.RequestStatus{
				Status:  beegfs.RequestStatus_UNASSIGNED,
				Message: "created",
			},
		}
		return job, nil
	}
	return nil, fmt.Errorf("bad job request")
}
