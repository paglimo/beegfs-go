package worker

import "github.com/thinkparq/protobuf/go/flex"

// WorkRequest represents an interface for work requests that can be processed
// by different types of worker nodes.
type WorkRequest interface {
	// GetJobID() returns the job id.
	GetJobID() string
	// GetRequestID() returns the request ID. Note request IDs are only
	// guaranteed to be unique for a particular job.
	GetRequestID() string
	// GetStatus() returns the status of the request.
	GetStatus() flex.RequestStatus
	// SetStatus() sets the request status and message.
	SetStatus(flex.RequestStatus_Status, string)
	// GetNodeType() returns the type of node this request should run on.
	GetNodeType() Type
}

// WorkResult carries status and node assignment for a particular WorkRequest.
// It is setup so it can be copied when needed, either in JobResults or when
// saving WorkResults to disk. The requests may not necessarily be completed and
// the statuses of the results will reflect this.
type WorkResult struct {
	RequestID string
	// The last known status of the request. IMPORTANT: Don't rely on status
	// when submitting an UpdateWorkRequest. Instead check if the assigned node
	// and pool aren't empty to decide if an update can be requested. Because
	// this is the last known status, if a state change is requested we should
	// always always send the request to the worker node to ensure it is
	// updated. Update requests are expected to be idempotent so if the WR is
	// already in the requested state no errors will happen.
	Status  flex.RequestStatus_Status
	Message string
	// AssignedNode is the ID of the node running this work request or "" if it is unassigned.
	AssignedNode string
	// AssignedPool is the type of the node pool running this work request or "" if it is unassigned.
	AssignedPool Type
}

func (wr *WorkResult) GetStatus() *flex.RequestStatus {
	return &flex.RequestStatus{
		Status:  wr.Status,
		Message: wr.Message,
	}
}
