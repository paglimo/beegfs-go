package job

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/bee-remote/internal/worker"
	"github.com/thinkparq/protobuf/go/flex"
)

func TestGetWorkResultsForResponse(t *testing.T) {

	testMap := map[string]worker.WorkResult{
		"0": {
			RequestID:    "0",
			Status:       5,
			Message:      "message0",
			AssignedNode: "node0",
			AssignedPool: worker.BeeSync,
		},
		"1": {
			RequestID:    "1",
			Status:       6,
			Message:      "message1",
			AssignedNode: "node1",
			AssignedPool: worker.BeeSync,
		},
	}

	for _, r := range getWorkResultsForResponse(testMap) {

		reqID, err := strconv.Atoi(r.RequestId)
		require.NoError(t, err)
		expectedStatus := flex.RequestStatus_Status(reqID + 5)

		assert.Equal(t, expectedStatus, r.Status.Status)
		assert.Equal(t, "message"+r.RequestId, r.Status.Message)
		assert.Equal(t, "node"+r.RequestId, r.AssignedNode)
		assert.Equal(t, r.AssignedPool, string(worker.BeeSync))

	}
}
