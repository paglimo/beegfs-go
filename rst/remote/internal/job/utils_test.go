package job

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/bee-remote/remote/internal/worker"
	"github.com/thinkparq/protobuf/go/flex"
)

func TestGetWorkResultsForResponse(t *testing.T) {

	testMap := map[string]worker.WorkResult{
		"0": {
			AssignedNode: "node0",
			AssignedPool: worker.BeeSync,
			WorkResult: flex.Work_builder{
				Path:      "/foo",
				RequestId: "0",
				Status: flex.Work_Status_builder{
					State:   5,
					Message: "message0",
				}.Build(),
			}.Build(),
		},
		"1": {
			AssignedNode: "node1",
			AssignedPool: worker.BeeSync,
			WorkResult: flex.Work_builder{
				Path:      "/bar",
				RequestId: "1",
				Status: flex.Work_Status_builder{
					State:   6,
					Message: "message1",
				}.Build(),
			}.Build(),
		},
	}

	for _, result := range getProtoWorkResults(testMap) {

		reqID, err := strconv.Atoi(result.GetWork().GetRequestId())
		require.NoError(t, err)
		expectedStatus := flex.Work_State(reqID + 5)

		assert.Equal(t, expectedStatus, result.GetWork().GetStatus().GetState())
		assert.Equal(t, "message"+result.GetWork().GetRequestId(), result.GetWork().GetStatus().GetMessage())
		assert.Equal(t, "node"+result.GetWork().GetRequestId(), result.GetAssignedNode())
		assert.Equal(t, result.GetAssignedPool(), string(worker.BeeSync))

	}
}
