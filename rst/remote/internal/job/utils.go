package job

import (
	"github.com/thinkparq/bee-remote/remote/internal/worker"
	"github.com/thinkparq/protobuf/go/beeremote"
)

// Converts the internal WorkResults to the corresponding gRPC messages.
func getProtoWorkResults(workResults map[string]worker.WorkResult) []*beeremote.JobResult_WorkResult {
	workResultsForResponse := []*beeremote.JobResult_WorkResult{}
	for _, wr := range workResults {
		workResult := &beeremote.JobResult_WorkResult{
			Work:         wr.WorkResult,
			AssignedNode: wr.AssignedNode,
			AssignedPool: string(wr.AssignedPool),
		}
		workResultsForResponse = append(workResultsForResponse, workResult)
	}
	return workResultsForResponse
}
