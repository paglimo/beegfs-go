package job

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/protobuf/go/beeremote"
)

func TestInTerminalState(t *testing.T) {
	job := &Job{
		Job: &beeremote.Job{
			Request: &beeremote.JobRequest{},
			Status: &beeremote.Job_Status{
				State: beeremote.Job_COMPLETED,
			},
		},
	}
	assert.True(t, job.InTerminalState())

	job.GetStatus().State = beeremote.Job_CANCELLED
	assert.True(t, job.InTerminalState())

	job.GetStatus().State = beeremote.Job_RUNNING
	assert.False(t, job.InTerminalState())
}
