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
	SetStatus(flex.RequestStatus_State, string)
	// GetNodeType() returns the type of node this request should run on.
	GetNodeType() Type
}

// WorkResult carries status and node assignment for a particular WorkRequest.
// It is setup so it can be copied when needed, either in JobResults or when
// saving WorkResults to disk. The requests may not necessarily be completed and
// the statuses of the results will reflect this.
type WorkResult struct {
	// AssignedNode is the ID of the node running this work request or "" if it is unassigned.
	AssignedNode string
	// AssignedPool is the type of the node pool running this work request or "" if it is unassigned.
	AssignedPool Type
	// Embed the latest work response from the assigned node (or generated
	// response if assignment failed). This includes the status along with any
	// other details like remaining/completed parts needed to finish the job.
	//
	// Note because WorkResponse doesn't include any protobuf specific fields
	// (like a oneof), testing shows we don't need to implement custom
	// GobEncode/GobDecode methods. A test verifies this and checks if the
	// definition of a WorkResponse ever changes which might break things.
	WorkResponse *flex.WorkResponse
}

// Used to access the status of the work request. Because it returns a pointer
// the status and/or message can be updated directly without overwriting the
// entire status. This is helpful if you want to modify one but not the other
// field (commonly message can change but status should not, or we want to keep
// a message history).
//
// IMPORTANT: Because this is the last known status, when handling UpdateJob
// requests don't assume the WorkRequest status is current. Instead always send
// the request to the worker node to ensure it is updated. Update requests are
// expected to be idempotent so if the WR is already in the requested state no
// errors will happen.
func (wr *WorkResult) Status() *flex.RequestStatus {
	return wr.WorkResponse.GetStatus()
}

// InTerminalState() indicates the work request is no longer active, cannot be
// restarted, and will not conflict with a new work request. This should mirror
// the InTerminalState() method for jobs.
func (wr *WorkResult) InTerminalState() bool {
	return wr.Status().State == flex.RequestStatus_COMPLETED || wr.Status().State == flex.RequestStatus_CANCELLED
}
