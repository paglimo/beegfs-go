package job

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

func TestInTerminalState(t *testing.T) {
	baseJob := &baseJob{
		Job: &beeremote.Job{
			Request: &beeremote.JobRequest{},
			Metadata: &flex.JobMetadata{
				Status: &flex.RequestStatus{
					Status: flex.RequestStatus_COMPLETED,
				},
			},
		},
	}
	job := &MockJob{baseJob: baseJob}
	assert.True(t, job.InTerminalState())

	job.SetStatus(&flex.RequestStatus{
		Status: flex.RequestStatus_CANCELLED,
	})
	assert.True(t, job.InTerminalState())

	job.SetStatus(&flex.RequestStatus{
		Status: flex.RequestStatus_RUNNING,
	})
	assert.False(t, job.InTerminalState())
}
