package job

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

func TestInTerminalState(t *testing.T) {
	job := &Job{
		Job: &beeremote.Job{
			Request: &beeremote.JobRequest{},
			Status: &flex.RequestStatus{
				State: flex.RequestStatus_COMPLETED,
			},
		},
	}
	assert.True(t, job.InTerminalState())

	job.GetStatus().State = flex.RequestStatus_CANCELLED
	assert.True(t, job.InTerminalState())

	job.GetStatus().State = flex.RequestStatus_RUNNING
	assert.False(t, job.InTerminalState())
}
