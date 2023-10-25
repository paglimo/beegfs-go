package worker

import (
	"fmt"

	"github.com/thinkparq/protobuf/go/beesync"
	"github.com/thinkparq/protobuf/go/flex"
)

// BeeSyncWorker is a concrete implementation of a worker node.
type BeeSyncWorker struct {
	BeeSyncConfig
}

// Verify BeeSyncWorker satisfies the Worker interface.
var _ Worker = &BeeSyncWorker{}

func newBeeSyncWorker(config BeeSyncConfig) *BeeSyncWorker {
	return &BeeSyncWorker{
		BeeSyncConfig: config,
	}
}

func (w *BeeSyncWorker) Connect() (bool, error) {
	return true, nil // TODO
}

func (w *BeeSyncWorker) SubmitWorkRequest(wr WorkRequest) (*flex.WorkResponse, error) {

	request, ok := wr.(*SyncRequest)
	if !ok {
		return nil, fmt.Errorf("received an invalid request for BeeSync node type: %s", request)
	}
	// TODO: Actually send the request to the node.
	fmt.Printf("sent request ID %s for job ID %s to %s:%d\n", wr.getRequestID(), wr.getJobID(), w.Hostname, w.Port)
	return &flex.WorkResponse{
		JobId:     wr.getJobID(),
		RequestId: wr.getRequestID(),
		Status: &flex.RequestStatus{
			Status:  flex.RequestStatus_SCHEDULED,
			Message: "scheduled",
		},
	}, nil
}

func (w *BeeSyncWorker) UpdateWorkRequest(updateRequest *flex.UpdateWorkRequest) (*flex.WorkResponse, error) {

	// TODO: Actually send the request to the node.

	if updateRequest.NewState == flex.NewState_CANCEL {
		return &flex.WorkResponse{
			JobId:     updateRequest.JobID,
			RequestId: updateRequest.RequestID,
			Status: &flex.RequestStatus{
				Status:  flex.RequestStatus_CANCELLED,
				Message: "cancelled by user",
			},
		}, nil
	}
	return nil, fmt.Errorf("unsupported new state requested for work request: %s", updateRequest.NewState)
}

func (w *BeeSyncWorker) NodeStream(updateRequests *flex.UpdateWorkRequests) <-chan *flex.WorkResponse {
	return nil // TODO
}

func (w *BeeSyncWorker) Disconnect() error {
	return nil // TODO
}

func (w *BeeSyncWorker) GetNodeType() NodeType {
	return BeeSync
}

// SyncRequests are handled by BeeSync nodes.
type SyncRequest struct {
	*beesync.SyncRequest
}

// SyncRequest satisfies the WorkRequest interface.
var _ WorkRequest = &SyncRequest{}

func (wr *SyncRequest) getJobID() string {
	return wr.Metadata.GetId()
}

func (wr *SyncRequest) getRequestID() string {
	return wr.GetRequestId()
}

func (r *SyncRequest) getStatus() flex.RequestStatus {
	return *r.Metadata.GetStatus()
}

func (r *SyncRequest) setStatus(status flex.RequestStatus_Status, message string) {

	newStatus := &flex.RequestStatus{
		Status:  status,
		Message: message,
	}

	r.Metadata.Status = newStatus
}

func (r *SyncRequest) getNodeType() NodeType {
	return BeeSync
}
