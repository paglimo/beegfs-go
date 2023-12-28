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
			AssignedNode: "node0",
			AssignedPool: worker.BeeSync,
			WorkResponse: &flex.WorkResponse{
				RequestId: "0",
				Status: &flex.RequestStatus{
					Status:  5,
					Message: "message0",
				},
			},
		},
		"1": {
			AssignedNode: "node1",
			AssignedPool: worker.BeeSync,
			WorkResponse: &flex.WorkResponse{
				RequestId: "1",
				Status: &flex.RequestStatus{
					Status:  6,
					Message: "message1",
				},
			},
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
