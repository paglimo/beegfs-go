package job

import (
	"io/fs"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/bee-remote/internal/worker"
	"github.com/thinkparq/bee-remote/internal/workermgr"
	"github.com/thinkparq/gobee/filesystem"
	"github.com/thinkparq/gobee/rst"
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
	workerMgrConfig := workermgr.Config{}
	workerConfigs := []worker.Config{
		{
			ID:                  "0",
			Name:                "test-node-0",
			Type:                worker.Mock,
			MaxReconnectBackOff: 5,
			MockConfig: worker.MockConfig{
				Expectations: []worker.MockExpectation{
					{
						MethodName: "connect",
						ReturnArgs: []interface{}{false, nil},
					},
					{
						MethodName: "SubmitWorkRequest",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							&flex.RequestStatus{
								State:   flex.RequestStatus_SCHEDULED,
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
								State:   flex.RequestStatus_CANCELLED,
								Message: "test expects a cancelled request",
							},
							nil,
						},
					},
					{
						MethodName: "disconnect",
						ReturnArgs: []interface{}{nil},
					},
				},
			},
		},
	}
	remoteStorageTargets := []*flex.RemoteStorageTarget{{Id: "0", Type: &flex.RemoteStorageTarget_Mock{Mock: "test"}}, {Id: "1", Type: &flex.RemoteStorageTarget_Mock{Mock: "test"}}}
	workerManager, err := workermgr.NewManager(logger, workerMgrConfig, workerConfigs, remoteStorageTargets)
	require.NoError(t, err)
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
		Type:     &beeremote.JobRequest_Mock{Mock: &beeremote.MockJob{NumTestSegments: 4, Rst: "0"}},
	}

	_, err = jobManager.SubmitJobRequest(&testJobRequest)
	require.NoError(t, err)

	getJobRequestsByPrefix := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_PathPrefix{
			PathPrefix: "/",
		},
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}
	getJobsResponse, err := jobManager.GetJobs(getJobRequestsByPrefix)
	require.NoError(t, err)
	assert.Equal(t, flex.RequestStatus_SCHEDULED, getJobsResponse.Response[0].Job.Status.State)

	assert.Len(t, getJobsResponse.Response[0].WorkResults, 4)
	for _, wr := range getJobsResponse.Response[0].WorkResults {
		assert.Equal(t, flex.RequestStatus_SCHEDULED, wr.Status.State)
	}

	scheduledJobID := getJobsResponse.Response[0].Job.Id

	// If we try to submit another job for the same path with the same RST an error should be returned:
	jr, err := jobManager.SubmitJobRequest(&testJobRequest)
	assert.Nil(t, jr)
	assert.Error(t, err)

	// No job should be created:
	getJobRequestsByPath := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_ExactPath{
			ExactPath: "/test/myfile",
		},
	}
	getJobsResponse, err = jobManager.GetJobs(getJobRequestsByPath)
	require.NoError(t, err)
	assert.Len(t, getJobsResponse.Response, 1)
	assert.Equal(t, flex.RequestStatus_SCHEDULED, getJobsResponse.Response[0].Job.Status.State)

	// If we schedule a job for a different RST it should be scheduled:
	testJobRequest2 := beeremote.JobRequest{
		Path:     "/test/myfile",
		Name:     "test job 1",
		Priority: 3,
		Type:     &beeremote.JobRequest_Mock{Mock: &beeremote.MockJob{NumTestSegments: 4, Rst: "1"}},
	}
	jr, err = jobManager.SubmitJobRequest(&testJobRequest2)
	assert.NoError(t, err)
	assert.Equal(t, flex.RequestStatus_SCHEDULED, jr.Job.Status.State)

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
	assert.Equal(t, flex.RequestStatus_CANCELLED, getJobsResponse.Response[0].Job.Status.State)

	for _, wr := range getJobsResponse.Response[0].WorkResults {
		assert.Equal(t, flex.RequestStatus_CANCELLED, wr.Status.State)
	}

}

// Use interactive JobMgr methods to submit and update jobs for this test. The
// channels are mostly just asynchronous wrappers around these anyway, so using
// these directly lets us also test what they return under different conditions.
//
// TODO: Test updating multiple jobs, including when one job updates and the
// other has a problem.
func TestUpdateJobRequestDelete(t *testing.T) {
	testMode = true

	logger := zaptest.NewLogger(t)
	workerMgrConfig := workermgr.Config{}
	workerConfigs := []worker.Config{
		{
			ID:                  "0",
			Name:                "test-node-0",
			Type:                worker.Mock,
			MaxReconnectBackOff: 5,
			MockConfig: worker.MockConfig{
				Expectations: []worker.MockExpectation{
					{
						MethodName: "connect",
						ReturnArgs: []interface{}{false, nil},
					},
					{
						MethodName: "SubmitWorkRequest",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							&flex.RequestStatus{
								State:   flex.RequestStatus_SCHEDULED,
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
								State:   flex.RequestStatus_CANCELLED,
								Message: "test expects a cancelled request",
							},
							nil,
						},
					},
					{
						MethodName: "disconnect",
						ReturnArgs: []interface{}{nil},
					},
				},
			},
		},
	}
	remoteStorageTargets := []*flex.RemoteStorageTarget{{Id: "0", Type: &flex.RemoteStorageTarget_Mock{Mock: "test"}}, {Id: "1", Type: &flex.RemoteStorageTarget_Mock{Mock: "test"}}}
	workerManager, err := workermgr.NewManager(logger, workerMgrConfig, workerConfigs, remoteStorageTargets)
	require.NoError(t, err)
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
		Type:     &beeremote.JobRequest_Mock{Mock: &beeremote.MockJob{NumTestSegments: 4, Rst: "0"}},
	}
	testJobRequest2 := beeremote.JobRequest{
		Path:     "/test/myfile2",
		Name:     "test job 2",
		Priority: 3,
		Type:     &beeremote.JobRequest_Mock{Mock: &beeremote.MockJob{NumTestSegments: 2, Rst: "1"}},
	}

	_, err = jobManager.SubmitJobRequest(&testJobRequest1)
	require.NoError(t, err)

	// We only interact with the second job request by its job ID:
	submitJobResponse2, err := jobManager.SubmitJobRequest(&testJobRequest2)
	require.NoError(t, err)

	////////////////////////////////////
	// First test deleting jobs by path:
	////////////////////////////////////
	// If we delete a job that has not yet reached a terminal state, nothing should happen:
	deleteJobByPathRequest := beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_Path{
			Path: testJobRequest1.Path,
		},
		NewState: flex.NewState_DELETE,
	}
	deleteJobByPathResponse, err := jobManager.UpdateJob(&deleteJobByPathRequest)
	require.NoError(t, err)                     // Only internal errors should return an error.
	assert.False(t, deleteJobByPathResponse.Ok) // Response should not be okay.

	assert.Equal(t, flex.RequestStatus_SCHEDULED, deleteJobByPathResponse.Responses[0].Job.Status.State)
	assert.Equal(t, "unable to delete job that has not reached a terminal state (cancel it first)", deleteJobByPathResponse.Responses[0].Job.Status.Message)

	// Work results should all still be scheduled:
	assert.Len(t, deleteJobByPathResponse.Responses[0].WorkResults, 4)
	for _, wr := range deleteJobByPathResponse.Responses[0].WorkResults {
		assert.Equal(t, flex.RequestStatus_SCHEDULED, wr.Status.State)
	}

	// Cancel the job:
	cancelJobByPathRequest := beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_Path{
			Path: testJobRequest1.Path,
		},
		NewState: flex.NewState_CANCEL,
	}
	cancelJobByPathResponse, err := jobManager.UpdateJob(&cancelJobByPathRequest)
	require.NoError(t, err)
	assert.True(t, cancelJobByPathResponse.Ok)

	// Work results should all be cancelled:
	assert.Len(t, cancelJobByPathResponse.Responses[0].WorkResults, 4)
	for _, wr := range cancelJobByPathResponse.Responses[0].WorkResults {
		assert.Equal(t, flex.RequestStatus_CANCELLED, wr.Status.State)
	}

	// Then delete it:
	deleteJobByPathResponse, err = jobManager.UpdateJob(&deleteJobByPathRequest)
	assert.NoError(t, err)
	assert.True(t, deleteJobByPathResponse.Ok)
	assert.Equal(t, "job scheduled for deletion", deleteJobByPathResponse.Responses[0].Job.Status.Message)

	// Verify the job was fully deleted:
	getJobRequestsByPath := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_ExactPath{
			ExactPath: testJobRequest1.Path,
		},
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}
	getJobsResponse1, err := jobManager.GetJobs(getJobRequestsByPath)
	assert.ErrorIs(t, err, badger.ErrKeyNotFound)
	assert.Nil(t, getJobsResponse1)

	////////////////////////////////
	// Then test deleting by job ID:
	////////////////////////////////

	// If we delete a job that has not yet reached a terminal state, nothing should happen:
	deleteJobByIDRequest := beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_JobID{
			JobID: submitJobResponse2.Job.Id,
		},
		NewState: flex.NewState_DELETE,
	}
	updateJobByIDResponse, err := jobManager.UpdateJob(&deleteJobByIDRequest)
	require.NoError(t, err)                   // Only internal errors should return an error.
	assert.False(t, updateJobByIDResponse.Ok) // However the response should not be okay.

	require.NoError(t, err)
	assert.Equal(t, flex.RequestStatus_SCHEDULED, updateJobByIDResponse.Responses[0].Job.Status.State)
	assert.Equal(t, "unable to delete job that has not reached a terminal state (cancel it first)", updateJobByIDResponse.Responses[0].Job.Status.Message)

	assert.Len(t, updateJobByIDResponse.Responses[0].WorkResults, 2)
	for _, wr := range updateJobByIDResponse.Responses[0].WorkResults {
		assert.Equal(t, flex.RequestStatus_SCHEDULED, wr.Status.State)
	}

	// Cancel the job:
	cancelJobByIDRequest := beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_JobID{
			JobID: submitJobResponse2.Job.Id,
		},
		NewState: flex.NewState_CANCEL,
	}
	cancelJobByIDResponse, err := jobManager.UpdateJob(&cancelJobByIDRequest)
	require.NoError(t, err)
	assert.True(t, cancelJobByIDResponse.Ok)

	// Work requests should be cancelled:
	assert.Len(t, cancelJobByIDResponse.Responses[0].WorkResults, 2)
	for _, wr := range cancelJobByIDResponse.Responses[0].WorkResults {
		assert.Equal(t, flex.RequestStatus_CANCELLED, wr.Status.State)
	}

	// Then delete it:
	updateJobByIDResponse, err = jobManager.UpdateJob(&deleteJobByIDRequest)
	assert.NoError(t, err)
	assert.True(t, updateJobByIDResponse.Ok)
	assert.Equal(t, "job scheduled for deletion", updateJobByIDResponse.Responses[0].Job.Status.Message)

	// Verify the job was fully deleted:
	getJobRequestsByID := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_JobID{
			JobID: submitJobResponse2.Job.Id,
		},
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}
	getJobsResponse2, err := jobManager.GetJobs(getJobRequestsByID)
	assert.ErrorIs(t, err, badger.ErrKeyNotFound)
	assert.Nil(t, getJobsResponse2)

	////////////////////////////////
	// Test deleting completed jobs:
	////////////////////////////////

	response, err := jobManager.SubmitJobRequest(&testJobRequest1)
	require.NoError(t, err)
	require.NotNil(t, response)

	// TODO: Add tests once we handle completed jobs.
	// Test deleting jobs by path skips completed jobs silently unless deleteCompletedJobs==true.
	// Test deleting jobs by ID skips completed jobs not affecting the job status message, but the response is !ok.

}

// Test fault conditions
// Schedule a job and one or more work requests fail == job should be cancelled.
// Cancel a job and one or more work request don't cancel == job should be failed.
// Schedule a job and one or more work requests fail and refuse to cancel == job should be failed.
func TestManageErrorHandling(t *testing.T) {
	testMode = true

	logger := zaptest.NewLogger(t)
	workerMgrConfig := workermgr.Config{}

	// This allows us to modify the expected status to what we expect in
	// different steps of the test after we initialize worker manager.
	expectedStatus := &flex.RequestStatus{
		State:   flex.RequestStatus_CANCELLED,
		Message: "test expects a cancelled request",
	}

	workerConfigs := []worker.Config{
		{
			ID:                  "0",
			Name:                "test-node-0",
			Type:                worker.Mock,
			MaxReconnectBackOff: 5,
			MockConfig: worker.MockConfig{
				Expectations: []worker.MockExpectation{
					{
						MethodName: "connect",
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
						MethodName: "disconnect",
						ReturnArgs: []interface{}{nil},
					},
				},
			},
		},
	}

	remoteStorageTargets := []*flex.RemoteStorageTarget{{Id: "0", Type: &flex.RemoteStorageTarget_Mock{Mock: "test"}}}
	workerManager, err := workermgr.NewManager(logger, workerMgrConfig, workerConfigs, remoteStorageTargets)
	require.NoError(t, err)
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

	// When we initially submit a job the state should be cancelled if any work
	// requests aren't scheduled but were able to be cancelled:
	testJobRequest := beeremote.JobRequest{
		Path:     "/test/myfile",
		Name:     "test job 1",
		Priority: 3,
		Type:     &beeremote.JobRequest_Mock{Mock: &beeremote.MockJob{NumTestSegments: 4, Rst: "0"}},
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
	assert.Equal(t, flex.RequestStatus_CANCELLED, getJobsResponse.Response[0].Job.Status.State)

	// JobMgr should have cancelled all outstanding requests:
	assert.Len(t, getJobsResponse.Response[0].WorkResults, 4)
	for _, wr := range getJobsResponse.Response[0].WorkResults {
		assert.Equal(t, flex.RequestStatus_CANCELLED, wr.Status.State)
	}

	scheduledJobID := getJobsResponse.Response[0].Job.Id

	// The following sequence of events is unlikely in real work scenarios, but
	// verifies how JobMgr handles states. First try to cancel the already
	// cancelled job. JobMgr should always attempt to verify the work requests
	// are cancelled on the worker nodes, even if they were previously cancelled
	// (calls are idempotent). This time cancelling the work requests fails for
	// some reason. As a result the Job status is now failed along with the WRs.
	expectedStatus.State = flex.RequestStatus_FAILED
	expectedStatus.Message = "test expects a failed request"

	updateJobRequest := beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_Path{
			Path: "/test/myfile",
		},
		NewState: flex.NewState_CANCEL,
	}
	jobManager.UpdateJob(&updateJobRequest)

	getJobRequestsByID := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_JobID{
			JobID: scheduledJobID,
		},
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}
	getJobsResponse, err = jobManager.GetJobs(getJobRequestsByID)
	require.NoError(t, err)
	assert.Equal(t, flex.RequestStatus_FAILED, getJobsResponse.Response[0].Job.Status.State)

	for _, wr := range getJobsResponse.Response[0].WorkResults {
		assert.Equal(t, flex.RequestStatus_FAILED, wr.Status.State)
	}

	// Submit another request to cancel the job. This time the work requests are
	// cancelled so the job status and work requests should all be cancelled.
	expectedStatus.State = flex.RequestStatus_CANCELLED
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
	assert.Equal(t, flex.RequestStatus_CANCELLED, getJobsResponse.Response[0].Job.Status.State)

	for _, wr := range getJobsResponse.Response[0].WorkResults {
		assert.Equal(t, flex.RequestStatus_CANCELLED, wr.Status.State)
	}

	// If we submit a job the state should be failed if any work requests were
	// failed and unable to be cancelled.
	expectedStatus.State = flex.RequestStatus_FAILED
	expectedStatus.Message = "test expects a failed request"

	jobResponse, err := jobManager.SubmitJobRequest(&testJobRequest)
	require.NoError(t, err)
	assert.Equal(t, flex.RequestStatus_FAILED, jobResponse.Job.GetStatus().State)
	jobID := jobResponse.GetJob().Id

	// We should not be able to delete failed jobs:
	updateJobRequest = beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_JobID{
			JobID: jobID,
		},
		NewState: flex.NewState_DELETE,
	}
	updateJobResponse, err := jobManager.UpdateJob(&updateJobRequest)
	require.NoError(t, err)
	assert.Equal(t, flex.RequestStatus_FAILED, updateJobResponse.Responses[0].Job.Status.State)

	// We should reject new jobs while there is a job in a failed state:
	jobResponse, err = jobManager.SubmitJobRequest(&testJobRequest)
	require.Error(t, err)
	assert.Nil(t, jobResponse)

	// We should be able to cancel failed jobs:
	expectedStatus.State = flex.RequestStatus_CANCELLED
	expectedStatus.Message = "test expects a cancelled request"

	updateJobRequest = beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_JobID{
			JobID: jobID,
		},
		NewState: flex.NewState_CANCEL,
	}
	updateJobResponse, err = jobManager.UpdateJob(&updateJobRequest)
	require.NoError(t, err)
	assert.Equal(t, flex.RequestStatus_CANCELLED, updateJobResponse.Responses[0].Job.Status.State)
}

func TestAllocationFailure(t *testing.T) {
	// Set setup:
	testMode = true
	path := "/foo/bar"
	fileSize := int64(1 << 20)
	logger := zaptest.NewLogger(t)

	// We don't need a full worker manager for this test.
	remoteStorageTargets := []*flex.RemoteStorageTarget{{Id: "1", Type: &flex.RemoteStorageTarget_Mock{Mock: "test"}}}
	workerManager, err := workermgr.NewManager(logger, workermgr.Config{}, []worker.Config{}, remoteStorageTargets)
	require.NoError(t, err)

	mockClient, ok := workerManager.RemoteStorageTargets["1"].(*rst.MockClient)
	require.True(t, ok, "likely a change in the test broke this check")
	mockClient.On("GetType").Return(rst.S3)
	mockClient.On("RecommendedSegments", fileSize).Return(4, 2)

	jobMgrConfig := Config{
		PathDBPath:         mapStoreTestPath,
		PathDBCacheSize:    1024,
		ResultsDBPath:      mapStoreTestPath,
		ResultsDBCacheSize: 1024,
		JournalPath:        journalDBTestPath,
	}

	jobManager := NewManager(logger, jobMgrConfig, workerManager)
	require.NoError(t, jobManager.Start())

	filesystem.MountPoint, err = filesystem.New("mock")
	// Start with no files in the MockFS:

	require.NoError(t, err)
	jobRequest := &beeremote.JobRequest{
		Path:                path,
		RemoteStorageTarget: "1",
		Type: &beeremote.JobRequest_Sync{
			Sync: &flex.SyncJob{
				Operation: flex.SyncJob_UNKNOWN,
			},
		},
	}

	// Submit a request for an unknown operation:
	response, err := jobManager.SubmitJobRequest(jobRequest)
	assert.ErrorIs(t, err, ErrUnknownJobOp)
	assert.Nil(t, response)

	// Fix the operation:
	jobRequest.GetSync().Operation = flex.SyncJob_UPLOAD

	// Submit a job request for a file that doesn't exist:
	response, err = jobManager.SubmitJobRequest(jobRequest)
	assert.Error(t, err)
	var pathError *fs.PathError
	assert.ErrorAs(t, err, &pathError)
	assert.Nil(t, response)
}

func TestUpdateJobResults(t *testing.T) {
	testMode = true

	logger := zaptest.NewLogger(t)
	workerMgrConfig := workermgr.Config{}
	remoteStorageTargets := []*flex.RemoteStorageTarget{{Id: "0", Type: &flex.RemoteStorageTarget_Mock{Mock: "test"}}}
	workerConfigs := []worker.Config{
		{
			ID:                  "0",
			Name:                "test-node-0",
			Type:                worker.Mock,
			MaxReconnectBackOff: 5,
			MockConfig: worker.MockConfig{
				Expectations: []worker.MockExpectation{
					{
						MethodName: "connect",
						ReturnArgs: []interface{}{false, nil},
					},
					{
						MethodName: "SubmitWorkRequest",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							&flex.RequestStatus{
								State:   flex.RequestStatus_SCHEDULED,
								Message: "test expects a scheduled request",
							},
							nil,
						},
					},
					{
						MethodName: "disconnect",
						ReturnArgs: []interface{}{nil},
					},
				},
			},
		},
	}
	workerManager, err := workermgr.NewManager(logger, workerMgrConfig, workerConfigs, remoteStorageTargets)
	require.NoError(t, err)
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

	testJobRequest := &beeremote.JobRequest{
		Path:     "/test/myfile",
		Name:     "test job 1",
		Priority: 3,
		Type:     &beeremote.JobRequest_Mock{Mock: &beeremote.MockJob{NumTestSegments: 2, Rst: "0"}},
	}

	// Verify once all WRs are in the same terminal state the job state
	// transitions correctly:
	for _, expectedStatus := range []flex.RequestStatus_State{flex.RequestStatus_COMPLETED, flex.RequestStatus_CANCELLED} {

		js, err := jobManager.SubmitJobRequest(testJobRequest)
		require.NoError(t, err)

		// The first response should not finish the job:
		workResponse1 := &flex.WorkResponse{
			JobId:     js.Job.GetId(),
			RequestId: "0",
			Status: &flex.RequestStatus{
				State:   expectedStatus,
				Message: expectedStatus.String(),
			},
			RemainingParts: 0,
			CompletedParts: []*flex.WorkResponse_Part{},
		}

		err = jobManager.updateJobResults(workResponse1)
		require.NoError(t, err)

		getJobsRequest := &beeremote.GetJobsRequest{
			Query: &beeremote.GetJobsRequest_JobID{
				JobID: js.Job.GetId(),
			},
			IncludeWorkRequests: false,
			IncludeWorkResults:  true,
		}
		resp, err := jobManager.GetJobs(getJobsRequest)
		require.NoError(t, err)

		// Work result order is not guaranteed...
		for _, wr := range resp.Response[0].WorkResults {
			if wr.RequestId == "0" {
				require.Equal(t, expectedStatus, wr.Status.GetState())
			} else {
				require.Equal(t, flex.RequestStatus_SCHEDULED, wr.Status.GetState())
			}
		}
		require.Equal(t, flex.RequestStatus_SCHEDULED, resp.Response[0].Job.Status.GetState())

		// The second response should finish the job:
		workResponse2 := &flex.WorkResponse{
			JobId:     js.Job.GetId(),
			RequestId: "1",
			Status: &flex.RequestStatus{
				State:   expectedStatus,
				Message: expectedStatus.String(),
			},
			RemainingParts: 0,
			CompletedParts: []*flex.WorkResponse_Part{},
		}
		err = jobManager.updateJobResults(workResponse2)
		require.NoError(t, err)

		resp, err = jobManager.GetJobs(getJobsRequest)
		require.NoError(t, err)
		require.Equal(t, expectedStatus, resp.Response[0].WorkResults[0].Status.GetState())
		require.Equal(t, expectedStatus, resp.Response[0].WorkResults[1].Status.GetState())
		require.Equal(t, expectedStatus, resp.Response[0].Job.Status.GetState())
	}

	// Test if all WRs are in a terminal state but there is a mismatch the job
	// is failed:
	js, err := jobManager.SubmitJobRequest(testJobRequest)
	require.NoError(t, err)

	workResponse1 := &flex.WorkResponse{
		JobId:     js.Job.GetId(),
		RequestId: "0",
		Status: &flex.RequestStatus{
			State:   flex.RequestStatus_COMPLETED,
			Message: flex.RequestStatus_COMPLETED.String(),
		},
		RemainingParts: 0,
		CompletedParts: []*flex.WorkResponse_Part{},
	}

	workResponse2 := &flex.WorkResponse{
		JobId:     js.Job.GetId(),
		RequestId: "1",
		Status: &flex.RequestStatus{
			State:   flex.RequestStatus_CANCELLED,
			Message: flex.RequestStatus_CANCELLED.String(),
		},
		RemainingParts: 0,
		CompletedParts: []*flex.WorkResponse_Part{},
	}

	err = jobManager.updateJobResults(workResponse1)
	require.NoError(t, err)
	err = jobManager.updateJobResults(workResponse2)
	require.NoError(t, err)

	getJobsRequest := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_JobID{
			JobID: js.Job.GetId(),
		},
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}

	resp, err := jobManager.GetJobs(getJobsRequest)
	require.NoError(t, err)
	require.Equal(t, flex.RequestStatus_FAILED, resp.Response[0].Job.Status.GetState())

}
