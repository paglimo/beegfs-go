package job

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/bee-remote/internal/worker"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap/zaptest"
)

const (
	mapStoreTestPath  = "/tmp"
	journalDBTestPath = "/tmp"
)

func TestManage(t *testing.T) {
	testMode = true

	logger := zaptest.NewLogger(t)
	workerMgrConfig := worker.ManagerConfig{}
	workResponsesChan := make(chan *flex.WorkResponse)
	workerConfigs := []worker.Config{
		{
			ID:                             "0",
			Name:                           "test-node-0",
			Type:                           worker.Mock,
			MaxReconnectBackOff:            5,
			MaxWaitForResponseAfterConnect: 5,
			MockConfig: worker.MockConfig{
				Expectations: []worker.MockExpectation{
					{
						MethodName: "Connect",
						ReturnArgs: []interface{}{false, nil},
					},
					{
						MethodName: "SubmitWorkRequest",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							&flex.RequestStatus{
								Status:  flex.RequestStatus_SCHEDULED,
								Message: "test expects a scheduled request",
							},
							nil,
						},
					},
					{
						MethodName: "UpdateWorkRequest",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							&flex.RequestStatus{
								Status:  flex.RequestStatus_CANCELLED,
								Message: "test expects a cancelled request",
							},
							nil,
						},
					},
					{
						MethodName: "NodeStream",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{workResponsesChan},
					},
					{
						MethodName: "Disconnect",
						ReturnArgs: []interface{}{nil},
					},
				},
			},
		},
	}
	workerManager := worker.NewManager(logger, workerMgrConfig, workerConfigs)
	require.NoError(t, workerManager.Start())

	jobMgrConfig := Config{
		PathDBPath:         mapStoreTestPath,
		PathDBCacheSize:    1024,
		ResultsDBPath:      mapStoreTestPath,
		ResultsDBCacheSize: 1024,
		JournalPath:        journalDBTestPath,
	}

	jobManager := NewManager(logger, jobMgrConfig, workerManager)
	require.NoError(t, jobManager.Start())

	// When we initially submit a job the state should be scheduled:
	testJobRequest := beeremote.JobRequest{
		Path:     "/test/myfile",
		Name:     "test job 1",
		Priority: 3,
		Type:     &beeremote.JobRequest_Mock{Mock: &beeremote.MockJob{NumTestSegments: 4}},
	}
	jobManager.JobRequests <- &testJobRequest
	time.Sleep(2 * time.Second)
	getJobRequestsByPrefix := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_PathPrefix{
			PathPrefix: "/",
		},
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}
	getJobsResponse, err := jobManager.GetJobs(getJobRequestsByPrefix)
	require.NoError(t, err)
	assert.Equal(t, flex.RequestStatus_SCHEDULED, getJobsResponse.Response[0].Job.Metadata.Status.Status)

	assert.Len(t, getJobsResponse.Response[0].WorkResults, 4)
	for _, wr := range getJobsResponse.Response[0].WorkResults {
		assert.Equal(t, flex.RequestStatus_SCHEDULED, wr.Status.Status)
	}

	scheduledJobID := getJobsResponse.Response[0].Job.Metadata.Id

	// If we try to submit another job for the same path it should be cancelled but the original job should be unaffected:
	jobManager.JobRequests <- &testJobRequest
	time.Sleep(2 * time.Second)
	getJobRequestsByPath := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_ExactPath{
			ExactPath: "/test/myfile",
		},
	}
	getJobsResponse, err = jobManager.GetJobs(getJobRequestsByPath)
	require.NoError(t, err)
	for _, response := range getJobsResponse.Response {
		// Note the order responses are returned is not guaranteed.
		if response.Job.Metadata.Id == scheduledJobID {
			assert.Equal(t, flex.RequestStatus_SCHEDULED, response.Job.Metadata.Status.Status)
		} else {
			assert.Equal(t, flex.RequestStatus_CANCELLED, response.Job.Metadata.Status.Status)
		}
	}

	// If we cancel a job the state of the job and work requests should update:
	updateJobRequest := beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_Path{
			Path: "/test/myfile",
		},
		NewState: flex.NewState_CANCEL,
	}
	jobManager.JobUpdates <- &updateJobRequest
	time.Sleep(2 * time.Second)

	getJobRequestsByID := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_JobID{
			JobID: scheduledJobID,
		},
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}
	getJobsResponse, err = jobManager.GetJobs(getJobRequestsByID)
	require.NoError(t, err)
	assert.Equal(t, flex.RequestStatus_CANCELLED, getJobsResponse.Response[0].Job.Metadata.Status.Status)

	for _, wr := range getJobsResponse.Response[0].WorkResults {
		assert.Equal(t, flex.RequestStatus_CANCELLED, wr.Status.Status)
	}

}

func TestUpdateJobRequestDelete(t *testing.T) {
	testMode = true

	logger := zaptest.NewLogger(t)
	workerMgrConfig := worker.ManagerConfig{}
	workResponsesChan := make(chan *flex.WorkResponse)
	workerConfigs := []worker.Config{
		{
			ID:                             "0",
			Name:                           "test-node-0",
			Type:                           worker.Mock,
			MaxReconnectBackOff:            5,
			MaxWaitForResponseAfterConnect: 5,
			MockConfig: worker.MockConfig{
				Expectations: []worker.MockExpectation{
					{
						MethodName: "Connect",
						ReturnArgs: []interface{}{false, nil},
					},
					{
						MethodName: "SubmitWorkRequest",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							&flex.RequestStatus{
								Status:  flex.RequestStatus_SCHEDULED,
								Message: "test expects a scheduled request",
							},
							nil,
						},
					},
					{
						MethodName: "UpdateWorkRequest",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							&flex.RequestStatus{
								Status:  flex.RequestStatus_CANCELLED,
								Message: "test expects a cancelled request",
							},
							nil,
						},
					},
					{
						MethodName: "NodeStream",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{workResponsesChan},
					},
					{
						MethodName: "Disconnect",
						ReturnArgs: []interface{}{nil},
					},
				},
			},
		},
	}
	workerManager := worker.NewManager(logger, workerMgrConfig, workerConfigs)
	require.NoError(t, workerManager.Start())

	jobMgrConfig := Config{
		PathDBPath:         mapStoreTestPath,
		PathDBCacheSize:    1024,
		ResultsDBPath:      mapStoreTestPath,
		ResultsDBCacheSize: 1024,
		JournalPath:        journalDBTestPath,
	}

	jobManager := NewManager(logger, jobMgrConfig, workerManager)
	require.NoError(t, jobManager.Start())

	// Submit two jobs for testing:
	testJobRequest1 := beeremote.JobRequest{
		Path:     "/test/myfile",
		Name:     "test job 1",
		Priority: 3,
		Type:     &beeremote.JobRequest_Mock{Mock: &beeremote.MockJob{NumTestSegments: 4}},
	}
	testJobRequest2 := beeremote.JobRequest{
		Path:     "/test/myfile2",
		Name:     "test job 2",
		Priority: 3,
		Type:     &beeremote.JobRequest_Mock{Mock: &beeremote.MockJob{NumTestSegments: 2}},
	}
	jobManager.JobRequests <- &testJobRequest1
	jobManager.JobRequests <- &testJobRequest2
	time.Sleep(2 * time.Second)

	////////////////////////////////////
	// First test deleting jobs by path:
	////////////////////////////////////
	// If we delete a job that has not yet reached a terminal state, nothing should happen:
	deleteJobRequest1 := beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_Path{
			Path: "/test/myfile",
		},
		NewState: flex.NewState_DELETE,
	}
	jobManager.JobUpdates <- &deleteJobRequest1
	time.Sleep(2 * time.Second)

	getJobRequestsByPath1 := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_ExactPath{
			ExactPath: "/test/myfile",
		},
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}
	getJobsResponse1, err := jobManager.GetJobs(getJobRequestsByPath1)
	require.NoError(t, err)
	assert.Equal(t, flex.RequestStatus_SCHEDULED, getJobsResponse1.Response[0].Job.Metadata.Status.Status)
	assert.Equal(t, "unable to delete job that has not reached a terminal state (cancel it first)", getJobsResponse1.Response[0].Job.Metadata.Status.Message)

	assert.Len(t, getJobsResponse1.Response[0].WorkResults, 4)
	for _, wr := range getJobsResponse1.Response[0].WorkResults {
		assert.Equal(t, flex.RequestStatus_SCHEDULED, wr.Status.Status)
	}
	jobID1 := getJobsResponse1.Response[0].Job.Metadata.Id

	// Cancel then delete the job:
	cancelJobRequest1 := beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_Path{
			Path: "/test/myfile",
		},
		NewState: flex.NewState_CANCEL,
	}
	jobManager.JobUpdates <- &cancelJobRequest1
	jobManager.JobUpdates <- &deleteJobRequest1
	time.Sleep(2 * time.Second)

	// Verify the job was deleted:
	getJobRequestsByID := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_JobID{
			JobID: jobID1,
		},
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}
	getJobsResponse1, err = jobManager.GetJobs(getJobRequestsByID)
	assert.ErrorIs(t, err, badger.ErrKeyNotFound)
	assert.Nil(t, getJobsResponse1)

	////////////////////////////////
	// Then test deleting by job ID:
	////////////////////////////////
	getJobRequestsByPath2 := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_ExactPath{
			ExactPath: "/test/myfile2",
		},
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}
	getJobsResponse2, err := jobManager.GetJobs(getJobRequestsByPath2)
	require.NoError(t, err)
	jobID2 := getJobsResponse2.Response[0].Job.Metadata.Id

	// If we delete a job that has not yet reached a terminal state, nothing should happen:
	deleteJobRequest2 := beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_JobID{
			JobID: jobID2,
		},
		NewState: flex.NewState_DELETE,
	}
	jobManager.JobUpdates <- &deleteJobRequest2
	time.Sleep(2 * time.Second)

	getJobsResponse2, err = jobManager.GetJobs(getJobRequestsByPath2)
	require.NoError(t, err)
	assert.Equal(t, flex.RequestStatus_SCHEDULED, getJobsResponse2.Response[0].Job.Metadata.Status.Status)
	assert.Equal(t, "unable to delete job that has not reached a terminal state (cancel it first)", getJobsResponse2.Response[0].Job.Metadata.Status.Message)

	assert.Len(t, getJobsResponse2.Response[0].WorkResults, 2)
	for _, wr := range getJobsResponse2.Response[0].WorkResults {
		assert.Equal(t, flex.RequestStatus_SCHEDULED, wr.Status.Status)
	}

	// Cancel the delete the job:
	cancelJobRequest2 := beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_JobID{
			JobID: jobID2,
		},
		NewState: flex.NewState_CANCEL,
	}
	jobManager.JobUpdates <- &cancelJobRequest2
	jobManager.JobUpdates <- &deleteJobRequest2
	time.Sleep(2 * time.Second)

	getJobsResponse2, err = jobManager.GetJobs(getJobRequestsByPath2)
	assert.ErrorIs(t, err, badger.ErrKeyNotFound)
	assert.Nil(t, getJobsResponse2)
}

// Test fault conditions
// Schedule a job and one or more work requests fail == job should be failed.
// Cancel a job and one or more work request don't cancel == job should be failed.
func TestManageErrorHandling(t *testing.T) {
	testMode = true

	logger := zaptest.NewLogger(t)
	workerMgrConfig := worker.ManagerConfig{}
	workResponsesChan := make(chan *flex.WorkResponse)

	// This allows us to modify the expected status to what we expect in
	// different steps of the test after we initialize worker manager.
	expectedStatus := &flex.RequestStatus{
		Status:  flex.RequestStatus_CANCELLED,
		Message: "test expects a cancelled request",
	}

	workerConfigs := []worker.Config{
		{
			ID:                             "0",
			Name:                           "test-node-0",
			Type:                           worker.Mock,
			MaxReconnectBackOff:            5,
			MaxWaitForResponseAfterConnect: 5,
			MockConfig: worker.MockConfig{
				Expectations: []worker.MockExpectation{
					{
						MethodName: "Connect",
						ReturnArgs: []interface{}{false, nil},
					},
					{
						MethodName: "SubmitWorkRequest",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							expectedStatus,
							nil,
						},
					},
					{
						MethodName: "UpdateWorkRequest",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							expectedStatus,
							nil,
						},
					},
					{
						MethodName: "NodeStream",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{workResponsesChan},
					},
					{
						MethodName: "Disconnect",
						ReturnArgs: []interface{}{nil},
					},
				},
			},
		},
	}
	workerManager := worker.NewManager(logger, workerMgrConfig, workerConfigs)
	require.NoError(t, workerManager.Start())

	jobMgrConfig := Config{
		PathDBPath:         mapStoreTestPath,
		PathDBCacheSize:    1024,
		ResultsDBPath:      mapStoreTestPath,
		ResultsDBCacheSize: 1024,
		JournalPath:        journalDBTestPath,
	}

	jobManager := NewManager(logger, jobMgrConfig, workerManager)
	require.NoError(t, jobManager.Start())

	// When we initially submit a job the state should be failed if any work requests failed:
	testJobRequest := beeremote.JobRequest{
		Path:     "/test/myfile",
		Name:     "test job 1",
		Priority: 3,
		Type:     &beeremote.JobRequest_Mock{Mock: &beeremote.MockJob{NumTestSegments: 4}},
	}
	jobManager.JobRequests <- &testJobRequest
	time.Sleep(2 * time.Second)
	getJobRequestsByPrefix := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_PathPrefix{
			PathPrefix: "/",
		},
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}
	getJobsResponse, err := jobManager.GetJobs(getJobRequestsByPrefix)
	require.NoError(t, err)
	assert.Equal(t, flex.RequestStatus_FAILED, getJobsResponse.Response[0].Job.Metadata.Status.Status)

	// JobMgr should have cancelled all outstanding requests:
	assert.Len(t, getJobsResponse.Response[0].WorkResults, 4)
	for _, wr := range getJobsResponse.Response[0].WorkResults {
		assert.Equal(t, flex.RequestStatus_CANCELLED, wr.Status.Status)
	}

	scheduledJobID := getJobsResponse.Response[0].Job.Metadata.Id

	// The following sequence of events is unlikely in real work scenarios, but
	// verifies how JobMgr handles states. First try to cancel the failed job.
	// JobMgr should always attempt to verify the work requests are cancelled on
	// the worker nodes, even if they were previously cancelled (calls are
	// idempotent). This time cancelling the work requests fails for some
	// reason. As a result the Job status should remained failed, but the work
	// requests should also be marked as failed.
	expectedStatus.Status = flex.RequestStatus_FAILED
	expectedStatus.Message = "test expects a failed request"

	updateJobRequest := beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_Path{
			Path: "/test/myfile",
		},
		NewState: flex.NewState_CANCEL,
	}
	jobManager.JobUpdates <- &updateJobRequest
	time.Sleep(2 * time.Second)

	getJobRequestsByID := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_JobID{
			JobID: scheduledJobID,
		},
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}
	getJobsResponse, err = jobManager.GetJobs(getJobRequestsByID)
	require.NoError(t, err)
	assert.Equal(t, flex.RequestStatus_FAILED, getJobsResponse.Response[0].Job.Metadata.Status.Status)

	for _, wr := range getJobsResponse.Response[0].WorkResults {
		assert.Equal(t, flex.RequestStatus_FAILED, wr.Status.Status)
	}

	// Submit another request to cancel the job. This time the work requests are
	// cancelled so the job status and work requests should all be cancelled.
	expectedStatus.Status = flex.RequestStatus_CANCELLED
	expectedStatus.Message = "test expects a cancelled request"

	jobManager.JobUpdates <- &updateJobRequest
	time.Sleep(2 * time.Second)

	getJobRequestsByID = &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_JobID{
			JobID: scheduledJobID,
		},
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}
	getJobsResponse, err = jobManager.GetJobs(getJobRequestsByID)
	require.NoError(t, err)
	assert.Equal(t, flex.RequestStatus_CANCELLED, getJobsResponse.Response[0].Job.Metadata.Status.Status)

	for _, wr := range getJobsResponse.Response[0].WorkResults {
		assert.Equal(t, flex.RequestStatus_CANCELLED, wr.Status.Status)
	}
}
