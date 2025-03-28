package job

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/protobuf/go/beeremote"
)

func TestInTerminalState(t *testing.T) {
	job := &Job{
		Job: beeremote.Job_builder{
			Request: &beeremote.JobRequest{},
			Status: beeremote.Job_Status_builder{
				State: beeremote.Job_COMPLETED,
			}.Build(),
		}.Build(),
	}
	assert.True(t, job.InTerminalState())

	job.GetStatus().SetState(beeremote.Job_CANCELLED)
	assert.True(t, job.InTerminalState())

	job.GetStatus().SetState(beeremote.Job_RUNNING)
	assert.False(t, job.InTerminalState())
}
