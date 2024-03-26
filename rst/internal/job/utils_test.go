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
				Path:      "/foo",
				RequestId: "0",
				Status: &flex.WorkResponse_Status{
					State:   5,
					Message: "message0",
				},
			},
		},
		"1": {
			AssignedNode: "node1",
			AssignedPool: worker.BeeSync,
			WorkResponse: &flex.WorkResponse{
				Path:      "/bar",
				RequestId: "1",
				Status: &flex.WorkResponse_Status{
					State:   6,
					Message: "message1",
				},
			},
		},
	}

	for _, r := range getWorkResultsForResponse(testMap) {

		reqID, err := strconv.Atoi(r.Response.GetRequestId())
		require.NoError(t, err)
		expectedStatus := flex.WorkResponse_State(reqID + 5)

		assert.Equal(t, expectedStatus, r.Response.GetStatus().State)
		assert.Equal(t, "message"+r.Response.GetRequestId(), r.Response.GetStatus().Message)
		assert.Equal(t, "node"+r.Response.GetRequestId(), r.AssignedNode)
		assert.Equal(t, r.AssignedPool, string(worker.BeeSync))

	}
}
