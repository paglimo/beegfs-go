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
			WorkResult: &flex.Work{
				Path:      "/foo",
				RequestId: "0",
				Status: &flex.Work_Status{
					State:   5,
					Message: "message0",
				},
			},
		},
		"1": {
			AssignedNode: "node1",
			AssignedPool: worker.BeeSync,
			WorkResult: &flex.Work{
				Path:      "/bar",
				RequestId: "1",
				Status: &flex.Work_Status{
					State:   6,
					Message: "message1",
				},
			},
		},
	}

	for _, result := range getProtoWorkResults(testMap) {

		reqID, err := strconv.Atoi(result.Work.GetRequestId())
		require.NoError(t, err)
		expectedStatus := flex.Work_State(reqID + 5)

		assert.Equal(t, expectedStatus, result.Work.GetStatus().State)
		assert.Equal(t, "message"+result.Work.GetRequestId(), result.Work.GetStatus().Message)
		assert.Equal(t, "node"+result.Work.GetRequestId(), result.AssignedNode)
		assert.Equal(t, result.AssignedPool, string(worker.BeeSync))

	}
}
