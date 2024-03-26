package job

import (
	"github.com/thinkparq/bee-remote/internal/worker"
	"github.com/thinkparq/protobuf/go/beeremote"
)

// Converts the internal of one or more WorkResults to the corresponding gRPC
// messages.
func getWorkResultsForResponse(workResults map[string]worker.WorkResult) []*beeremote.JobResponse_WorkResult {
	workResultsForResponse := []*beeremote.JobResponse_WorkResult{}
	for _, wr := range workResults {
		workResult := &beeremote.JobResponse_WorkResult{
			Response:     wr.WorkResponse,
			AssignedNode: wr.AssignedNode,
			AssignedPool: string(wr.AssignedPool),
		}
		workResultsForResponse = append(workResultsForResponse, workResult)
	}
	return workResultsForResponse
}
