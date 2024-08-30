package job

import (
	"context"
	"io/fs"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/bee-remote/internal/worker"
	"github.com/thinkparq/bee-remote/internal/workermgr"
	"github.com/thinkparq/gobee/filesystem"
	"github.com/thinkparq/gobee/kvstore"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap/zaptest"
)

const (
	testDBBasePath = "/tmp"
)

// Helper function to create a temporary path for testing under the provided
// path. Returns the full path that should be used for BadgerDB and a function
// that should be called (usually with defer) to cleanup after the test. Will
// fail the test if the cleanup function encounters any errors
func tempPathForTesting(path string) (string, func(t require.TestingT), error) {
	tempDBPath, err := os.MkdirTemp(path, "mapStoreTestMode")
	if err != nil {
		return "", nil, err
	}

	cleanup := func(t require.TestingT) {
		require.NoError(t, os.RemoveAll(tempDBPath), "error cleaning up after test")
	}

	return tempDBPath, cleanup, nil

}

func TestManage(t *testing.T) {
	tmpPathDBPath, cleanupPathDBPath, err := tempPathForTesting(testDBBasePath)
	require.NoError(t, err, "error setting up for test")
	defer cleanupPathDBPath(t)

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
						MethodName: "SubmitWork",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							&flex.Work_Status{
								State:   flex.Work_SCHEDULED,
								Message: "test expects a scheduled request",
							},
							nil,
						},
					},
					{
						MethodName: "UpdateWork",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							&flex.Work_Status{
								State:   flex.Work_CANCELLED,
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

	mountPoint := filesystem.NewMockFS()
	mountPoint.CreateWriteClose("/test/myfile", make([]byte, 0))

	remoteStorageTargets := []*flex.RemoteStorageTarget{{Id: 0, Type: &flex.RemoteStorageTarget_Mock{Mock: "test"}}, {Id: 1, Type: &flex.RemoteStorageTarget_Mock{Mock: "test"}}}
	workerManager, err := workermgr.NewManager(context.Background(), logger, workerMgrConfig, workerConfigs, remoteStorageTargets, &flex.BeeRemoteNode{}, mountPoint)
	require.NoError(t, err)
	require.NoError(t, workerManager.Start())

	jobMgrConfig := Config{
		PathDBPath: tmpPathDBPath,
	}

	jobManager := NewManager(logger, jobMgrConfig, workerManager)
	require.NoError(t, jobManager.Start())

	// When we initially submit a job the state should be scheduled:
	testJobRequest := beeremote.JobRequest{
		Path:                "/test/myfile",
		Name:                "test job 1",
		Priority:            3,
		Type:                &beeremote.JobRequest_Mock{Mock: &flex.MockJob{NumTestSegments: 4}},
		RemoteStorageTarget: 0,
	}

	_, err = jobManager.SubmitJobRequest(&testJobRequest)
	require.NoError(t, err)

	getJobRequestsByPrefix := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_ByPathPrefix{
			ByPathPrefix: "/",
		},
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}

	responses := make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByPrefix, responses)
	require.NoError(t, err)
	getJobsResponse := <-responses
	assert.Equal(t, beeremote.Job_SCHEDULED, getJobsResponse.Results[0].Job.Status.State)

	assert.Len(t, getJobsResponse.Results[0].WorkResults, 4)
	for _, wr := range getJobsResponse.Results[0].WorkResults {
		assert.Equal(t, flex.Work_SCHEDULED, wr.Work.Status.State)
	}

	scheduledJobID := getJobsResponse.Results[0].Job.Id

	// If we try to submit another job for the same path with the same RST an error should be returned:
	jr, err := jobManager.SubmitJobRequest(&testJobRequest)
	assert.Nil(t, jr)
	assert.Error(t, err)

	// No job should be created:
	getJobRequestsByPath := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_ByExactPath{
			ByExactPath: "/test/myfile",
		},
	}
	responses = make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByPath, responses)
	require.NoError(t, err)
	getJobsResponse = <-responses
	assert.Len(t, getJobsResponse.Results, 1)
	assert.Equal(t, beeremote.Job_SCHEDULED, getJobsResponse.Results[0].Job.Status.State)

	// If we schedule a job for a different RST it should be scheduled:
	testJobRequest2 := beeremote.JobRequest{
		Path:                "/test/myfile",
		Name:                "test job 1",
		Priority:            3,
		Type:                &beeremote.JobRequest_Mock{Mock: &flex.MockJob{NumTestSegments: 4}},
		RemoteStorageTarget: 1,
	}
	jr, err = jobManager.SubmitJobRequest(&testJobRequest2)
	assert.NoError(t, err)
	assert.Equal(t, beeremote.Job_SCHEDULED, jr.Job.Status.State)

	// If we cancel a job the state of the job and work requests should update:
	updateJobRequest := beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_ByExactPath{
			ByExactPath: "/test/myfile",
		},
		NewState: beeremote.UpdateJobRequest_CANCELLED,
	}
	jobManager.JobUpdates <- &updateJobRequest
	time.Sleep(2 * time.Second)

	getJobRequestsByID := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_ByJobIdAndPath{
			ByJobIdAndPath: &beeremote.GetJobsRequest_QueryIdAndPath{
				JobId: scheduledJobID,
				Path:  testJobRequest2.Path,
			},
		},
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}

	responses = make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByID, responses)
	getJobsResponse = <-responses
	require.NoError(t, err)
	assert.Equal(t, beeremote.Job_CANCELLED, getJobsResponse.Results[0].Job.Status.State)

	for _, wr := range getJobsResponse.Results[0].WorkResults {
		assert.Equal(t, flex.Work_CANCELLED, wr.Work.Status.State)
	}

}

// Use interactive JobMgr methods to submit and update jobs for this test. The channels are mostly
// just asynchronous wrappers around these anyway, so using these directly lets us also test what
// they return under different conditions.
//
// TODO: https://github.com/ThinkParQ/bee-remote/issues/37
// Test updating multiple jobs for the same path, including when one job updates and the other has a
// problem.
func TestUpdateJobRequestDelete(t *testing.T) {
	tmpPathDBPath, cleanupPathDBPath, err := tempPathForTesting(testDBBasePath)
	require.NoError(t, err, "error setting up for test")
	defer cleanupPathDBPath(t)

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
						MethodName: "SubmitWork",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							&flex.Work_Status{
								State:   flex.Work_SCHEDULED,
								Message: "test expects a scheduled request",
							},
							nil,
						},
					},
					{
						MethodName: "UpdateWork",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							&flex.Work_Status{
								State:   flex.Work_CANCELLED,
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
	mountPoint := filesystem.NewMockFS()
	mountPoint.CreateWriteClose("/test/myfile", make([]byte, 10))
	mountPoint.CreateWriteClose("/test/myfile2", make([]byte, 20))

	remoteStorageTargets := []*flex.RemoteStorageTarget{{Id: 0, Type: &flex.RemoteStorageTarget_Mock{Mock: "test"}}, {Id: 1, Type: &flex.RemoteStorageTarget_Mock{Mock: "test"}}}
	workerManager, err := workermgr.NewManager(context.Background(), logger, workerMgrConfig, workerConfigs, remoteStorageTargets, &flex.BeeRemoteNode{}, mountPoint)
	require.NoError(t, err)
	require.NoError(t, workerManager.Start())

	jobMgrConfig := Config{
		PathDBPath: tmpPathDBPath,
	}

	jobManager := NewManager(logger, jobMgrConfig, workerManager)
	require.NoError(t, jobManager.Start())

	// Submit two jobs for testing:
	testJobRequest1 := beeremote.JobRequest{
		Path:                "/test/myfile",
		Name:                "test job 1",
		Priority:            3,
		Type:                &beeremote.JobRequest_Mock{Mock: &flex.MockJob{NumTestSegments: 4}},
		RemoteStorageTarget: 0,
	}
	testJobRequest2 := beeremote.JobRequest{
		Path:                "/test/myfile2",
		Name:                "test job 2",
		Priority:            3,
		Type:                &beeremote.JobRequest_Mock{Mock: &flex.MockJob{NumTestSegments: 2}},
		RemoteStorageTarget: 1,
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
		Query: &beeremote.UpdateJobRequest_ByExactPath{
			ByExactPath: testJobRequest1.Path,
		},
		NewState: beeremote.UpdateJobRequest_DELETED,
	}
	deleteJobByPathResponse, err := jobManager.UpdateJob(&deleteJobByPathRequest)
	require.NoError(t, err)                     // Only internal errors should return an error.
	assert.False(t, deleteJobByPathResponse.Ok) // Response should not be okay.
	assert.Contains(t, deleteJobByPathResponse.Message, "because it has not reached a terminal state")

	// Status on the job should not change:
	assert.Equal(t, beeremote.Job_SCHEDULED, deleteJobByPathResponse.Results[0].Job.Status.State)
	assert.Equal(t, "finished scheduling work requests", deleteJobByPathResponse.Results[0].Job.Status.Message)

	// Work results should all still be scheduled:
	assert.Len(t, deleteJobByPathResponse.Results[0].WorkResults, 4)
	for _, wr := range deleteJobByPathResponse.Results[0].WorkResults {
		assert.Equal(t, flex.Work_SCHEDULED, wr.Work.Status.State)
	}

	// Cancel the job:
	cancelJobByPathRequest := beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_ByExactPath{
			ByExactPath: testJobRequest1.Path,
		},
		NewState: beeremote.UpdateJobRequest_CANCELLED,
	}
	cancelJobByPathResponse, err := jobManager.UpdateJob(&cancelJobByPathRequest)
	require.NoError(t, err)
	assert.True(t, cancelJobByPathResponse.Ok)

	// Work results should all be cancelled:
	assert.Len(t, cancelJobByPathResponse.Results[0].WorkResults, 4)
	for _, wr := range cancelJobByPathResponse.Results[0].WorkResults {
		assert.Equal(t, flex.Work_CANCELLED, wr.Work.Status.State)
	}

	// Then delete it:
	deleteJobByPathResponse, err = jobManager.UpdateJob(&deleteJobByPathRequest)
	assert.NoError(t, err)
	assert.True(t, deleteJobByPathResponse.Ok)
	assert.Equal(t, "job scheduled for deletion", deleteJobByPathResponse.Results[0].Job.Status.Message)

	// Verify the job was fully deleted:
	getJobRequestsByPath := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_ByExactPath{
			ByExactPath: testJobRequest1.Path,
		},
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}
	responses := make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByPath, responses)
	assert.ErrorIs(t, err, kvstore.ErrEntryNotInDB)

	////////////////////////////////
	// Then test deleting by job ID:
	////////////////////////////////

	// If we delete a job that has not yet reached a terminal state, nothing should happen:
	deleteJobByIDRequest := beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_ByIdAndPath{
			ByIdAndPath: &beeremote.UpdateJobRequest_QueryIdAndPath{
				JobId: submitJobResponse2.Job.Id,
				Path:  submitJobResponse2.Job.Request.Path,
			},
		},
		NewState: beeremote.UpdateJobRequest_DELETED,
	}
	updateJobByIDResponse, err := jobManager.UpdateJob(&deleteJobByIDRequest)
	require.NoError(t, err)                   // Only internal errors should return an error.
	assert.False(t, updateJobByIDResponse.Ok) // However the response should not be okay.
	assert.Contains(t, updateJobByIDResponse.Message, "because it has not reached a terminal state")

	// Status on the job should not change:
	assert.Equal(t, beeremote.Job_SCHEDULED, updateJobByIDResponse.Results[0].Job.Status.State)
	assert.Equal(t, "finished scheduling work requests", updateJobByIDResponse.Results[0].Job.Status.Message)

	assert.Len(t, updateJobByIDResponse.Results[0].WorkResults, 2)
	for _, wr := range updateJobByIDResponse.Results[0].WorkResults {
		assert.Equal(t, flex.Work_SCHEDULED, wr.Work.Status.State)
	}

	// Cancel the job:
	cancelJobByIDRequest := beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_ByIdAndPath{
			ByIdAndPath: &beeremote.UpdateJobRequest_QueryIdAndPath{
				JobId: submitJobResponse2.Job.Id,
				Path:  submitJobResponse2.Job.Request.Path,
			},
		},
		NewState: beeremote.UpdateJobRequest_CANCELLED,
	}
	cancelJobByIDResponse, err := jobManager.UpdateJob(&cancelJobByIDRequest)
	require.NoError(t, err)
	assert.True(t, cancelJobByIDResponse.Ok)

	// Work requests should be cancelled:
	assert.Len(t, cancelJobByIDResponse.Results[0].WorkResults, 2)
	for _, wr := range cancelJobByIDResponse.Results[0].WorkResults {
		assert.Equal(t, flex.Work_CANCELLED, wr.Work.Status.State)
	}

	// Then delete it:
	updateJobByIDResponse, err = jobManager.UpdateJob(&deleteJobByIDRequest)
	assert.NoError(t, err)
	assert.True(t, updateJobByIDResponse.Ok)
	assert.Equal(t, "job scheduled for deletion", updateJobByIDResponse.Results[0].Job.Status.Message)

	// Verify the job was fully deleted:
	getJobRequestsByID := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_ByJobIdAndPath{
			ByJobIdAndPath: &beeremote.GetJobsRequest_QueryIdAndPath{
				JobId: submitJobResponse2.Job.Id,
				Path:  submitJobResponse2.Job.Request.Path,
			},
		},
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}
	responses = make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByID, responses)
	assert.ErrorIs(t, err, kvstore.ErrEntryNotInDB)

	////////////////////////////////
	// Test deleting completed jobs:
	////////////////////////////////

	response, err := jobManager.SubmitJobRequest(&testJobRequest1)
	require.NoError(t, err)
	require.NotNil(t, response)
	// Complete the job by simulating a worker node updating the results.
	for i := range 4 {
		result := &flex.Work{
			Path:      response.Job.Request.Path,
			JobId:     response.Job.GetId(),
			RequestId: strconv.Itoa(i),
			Status: &flex.Work_Status{
				State:   flex.Work_COMPLETED,
				Message: "complete",
			},
			Parts: []*flex.Work_Part{},
		}
		err = jobManager.UpdateWork(result)
		require.NoError(t, err)
	}

	// Refuse to cancel completed jobs:
	updateJobByIDRequest := beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_ByIdAndPath{
			ByIdAndPath: &beeremote.UpdateJobRequest_QueryIdAndPath{
				JobId: response.Job.Id,
				Path:  response.Job.Request.Path,
			},
		},
		NewState: beeremote.UpdateJobRequest_CANCELLED,
	}
	cancelJobByIDResponse, err = jobManager.UpdateJob(&updateJobByIDRequest)
	require.NoError(t, err)
	assert.True(t, cancelJobByIDResponse.Ok)
	assert.Contains(t, cancelJobByIDResponse.Message, "rejecting update for completed job")

	// Refuse to delete completed jobs by ID and path, the overall response should be ok:
	updateJobByIDRequest.NewState = beeremote.UpdateJobRequest_DELETED
	deleteJobByIDResp, err := jobManager.UpdateJob(&updateJobByIDRequest)
	require.NoError(t, err)
	assert.True(t, deleteJobByIDResp.Ok)
	assert.Contains(t, deleteJobByIDResp.Message, "rejecting update for completed job")

	// Refuse to delete completed jobs by path, the overall response should be ok:
	updateJobByPathRequest := beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_ByExactPath{
			ByExactPath: response.Job.Request.Path,
		},
		NewState: beeremote.UpdateJobRequest_DELETED,
	}
	deleteJobByPathResp, err := jobManager.UpdateJob(&updateJobByPathRequest)
	require.NoError(t, err)
	assert.True(t, deleteJobByPathResp.Ok)
	assert.Contains(t, deleteJobByPathResp.Message, "rejecting update for completed job")

	// Status on the job should have not changed at any point:
	assert.Equal(t, beeremote.Job_COMPLETED, deleteJobByPathResp.Results[0].Job.Status.State)
	assert.Equal(t, "successfully completed job", deleteJobByPathResp.Results[0].Job.Status.Message)

	assert.Len(t, deleteJobByPathResp.Results[0].WorkResults, 4)
	for _, wr := range deleteJobByPathResp.Results[0].WorkResults {
		assert.Equal(t, flex.Work_COMPLETED, wr.Work.Status.State)
	}

	// Deleting completed jobs by job ID and path is allowed when the update is forced:
	updateJobByIDRequest.ForceUpdate = true
	deleteJobByIDResp, err = jobManager.UpdateJob(&updateJobByIDRequest)
	require.NoError(t, err)
	assert.True(t, deleteJobByIDResp.Ok)
	assert.Contains(t, deleteJobByIDResp.Message, "")
	assert.Len(t, deleteJobByIDResp.Results, 1)
	assert.Equal(t, beeremote.Job_COMPLETED, deleteJobByPathResp.Results[0].Job.Status.State)
	assert.Contains(t, deleteJobByIDResp.Results[0].Job.Status.Message, "job scheduled for deletion")

	responses = make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_ByExactPath{ByExactPath: "response.Job.Request.Path"},
	}, responses)
	assert.ErrorIs(t, kvstore.ErrEntryNotInDB, err)
}

// Test fault conditions
// Schedule a job and one or more work requests fail == job should be cancelled.
// Cancel a job and one or more work request don't cancel == job should be failed.
// Schedule a job and one or more work requests fail and refuse to cancel == job should be failed.
func TestManageErrorHandling(t *testing.T) {
	tmpPathDBPath, cleanupPathDBPath, err := tempPathForTesting(testDBBasePath)
	require.NoError(t, err, "error setting up for test")
	defer cleanupPathDBPath(t)

	logger := zaptest.NewLogger(t)
	workerMgrConfig := workermgr.Config{}

	// This allows us to modify the expected status to what we expect in
	// different steps of the test after we initialize worker manager.
	expectedStatus := &flex.Work_Status{
		State:   flex.Work_CANCELLED,
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
						MethodName: "SubmitWork",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							expectedStatus,
							nil,
						},
					},
					{
						MethodName: "UpdateWork",
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

	mountPoint := filesystem.NewMockFS()
	mountPoint.CreateWriteClose("/test/myfile", make([]byte, 30))

	remoteStorageTargets := []*flex.RemoteStorageTarget{{Id: 0, Type: &flex.RemoteStorageTarget_Mock{Mock: "test"}}}
	workerManager, err := workermgr.NewManager(context.Background(), logger, workerMgrConfig, workerConfigs, remoteStorageTargets, &flex.BeeRemoteNode{}, mountPoint)
	require.NoError(t, err)
	require.NoError(t, workerManager.Start())

	jobMgrConfig := Config{
		PathDBPath: tmpPathDBPath,
	}

	jobManager := NewManager(logger, jobMgrConfig, workerManager)
	require.NoError(t, jobManager.Start())

	// When we initially submit a job the state should be cancelled if any work
	// requests aren't scheduled but were able to be cancelled:
	testJobRequest := beeremote.JobRequest{
		Path:                "/test/myfile",
		Name:                "test job 1",
		Priority:            3,
		Type:                &beeremote.JobRequest_Mock{Mock: &flex.MockJob{NumTestSegments: 4}},
		RemoteStorageTarget: 0,
	}
	jobManager.JobRequests <- &testJobRequest
	time.Sleep(2 * time.Second)
	getJobRequestsByPrefix := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_ByPathPrefix{
			ByPathPrefix: "/",
		},
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}
	responses := make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByPrefix, responses)
	require.NoError(t, err)
	getJobsResponse := <-responses
	assert.Equal(t, beeremote.Job_CANCELLED, getJobsResponse.Results[0].Job.Status.State)

	// JobMgr should have cancelled all outstanding requests:
	assert.Len(t, getJobsResponse.Results[0].WorkResults, 4)
	for _, wr := range getJobsResponse.Results[0].WorkResults {
		assert.Equal(t, flex.Work_CANCELLED, wr.Work.Status.State)
	}

	scheduledJobID := getJobsResponse.Results[0].Job.Id

	// The following sequence of events is unlikely in real work scenarios, but verifies how JobMgr
	// handles states. First try to cancel the already cancelled job. JobMgr should always attempt
	// to verify the work requests are cancelled on the worker nodes, even if they were previously
	// cancelled (calls are idempotent). This time we cannot definitely cancel the requests so their
	// state is unknown for some reason. As a result the Job status is now unknown.
	expectedStatus.State = flex.Work_UNKNOWN
	expectedStatus.Message = "test expects an error communicating to the node"

	updateJobRequest := beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_ByExactPath{
			ByExactPath: "/test/myfile",
		},
		NewState: beeremote.UpdateJobRequest_CANCELLED,
	}
	jobManager.UpdateJob(&updateJobRequest)

	getJobRequestsByID := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_ByJobIdAndPath{
			ByJobIdAndPath: &beeremote.GetJobsRequest_QueryIdAndPath{
				JobId: scheduledJobID,
				Path:  testJobRequest.Path,
			},
		},
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}
	responses = make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByID, responses)
	require.NoError(t, err)
	getJobsResponse = <-responses
	assert.Equal(t, beeremote.Job_UNKNOWN, getJobsResponse.Results[0].Job.Status.State)

	for _, wr := range getJobsResponse.Results[0].WorkResults {
		assert.Equal(t, flex.Work_UNKNOWN, wr.Work.Status.State)
	}

	// Submit another request to cancel the job. This time the work requests are
	// cancelled so the job status and work requests should all be cancelled.
	expectedStatus.State = flex.Work_CANCELLED
	expectedStatus.Message = "test expects a cancelled request"

	jobManager.UpdateJob(&updateJobRequest)

	getJobRequestsByID = &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_ByJobIdAndPath{
			ByJobIdAndPath: &beeremote.GetJobsRequest_QueryIdAndPath{
				JobId: scheduledJobID,
				Path:  testJobRequest.Path,
			},
		},
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}
	responses = make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByID, responses)
	getJobsResponse = <-responses
	require.NoError(t, err)
	assert.Equal(t, beeremote.Job_CANCELLED, getJobsResponse.Results[0].Job.Status.State)

	for _, wr := range getJobsResponse.Results[0].WorkResults {
		assert.Equal(t, flex.Work_CANCELLED, wr.Work.Status.State)
	}

	// If we submit a job the state should be unknown if any work requests were
	// failed and unable to be cancelled.
	expectedStatus.State = flex.Work_FAILED
	expectedStatus.Message = "test expects a failed request"

	jobResponse, err := jobManager.SubmitJobRequest(&testJobRequest)
	assert.Error(t, err)
	assert.Equal(t, beeremote.Job_UNKNOWN, jobResponse.Job.GetStatus().State)
	jobID := jobResponse.GetJob().Id

	// We should not be able to delete jobs in an unknown state:
	updateJobRequest = beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_ByIdAndPath{
			ByIdAndPath: &beeremote.UpdateJobRequest_QueryIdAndPath{
				JobId: jobID,
				Path:  testJobRequest.Path,
			},
		},
		NewState: beeremote.UpdateJobRequest_DELETED,
	}
	updateJobResponse, err := jobManager.UpdateJob(&updateJobRequest)
	require.NoError(t, err)
	assert.Equal(t, beeremote.Job_UNKNOWN, updateJobResponse.Results[0].Job.Status.State)

	// We should reject new jobs while there is a job in an unknown state:
	jobResponse, err = jobManager.SubmitJobRequest(&testJobRequest)
	require.Error(t, err)
	assert.Nil(t, jobResponse)

	// We should be able to cancel jobs in an unknown state once the WRs can be cancelled:
	expectedStatus.State = flex.Work_CANCELLED
	expectedStatus.Message = "test expects a cancelled request"

	updateJobRequest = beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_ByIdAndPath{
			ByIdAndPath: &beeremote.UpdateJobRequest_QueryIdAndPath{
				JobId: jobID,
				Path:  testJobRequest.Path,
			},
		},
		NewState: beeremote.UpdateJobRequest_CANCELLED,
	}
	updateJobResponse, err = jobManager.UpdateJob(&updateJobRequest)
	require.NoError(t, err)
	assert.Equal(t, beeremote.Job_CANCELLED, updateJobResponse.Results[0].Job.Status.State)

	// Submit another jobs whose work requests cannot be scheduled and an error occurs cancelling
	// them so the overall job status is unknown:
	expectedStatus.State = flex.Work_UNKNOWN
	expectedStatus.Message = "test expects the work request status is unknown"

	jobResponse, err = jobManager.SubmitJobRequest(&testJobRequest)
	assert.Error(t, err)
	assert.Equal(t, beeremote.Job_UNKNOWN, jobResponse.Job.GetStatus().State)
	jobID = jobResponse.GetJob().Id

	// Even if we cannot contact the worker nodes to determine the WR statuses, we can still force
	// the job to be cancelled:
	updateJobRequest = beeremote.UpdateJobRequest{
		Query: &beeremote.UpdateJobRequest_ByIdAndPath{
			ByIdAndPath: &beeremote.UpdateJobRequest_QueryIdAndPath{
				JobId: jobID,
				Path:  testJobRequest.Path,
			},
		},
		NewState:    beeremote.UpdateJobRequest_CANCELLED,
		ForceUpdate: true,
	}

	updateJobResponse, err = jobManager.UpdateJob(&updateJobRequest)
	require.NoError(t, err)
	require.True(t, updateJobResponse.Ok)
	assert.Equal(t, beeremote.Job_CANCELLED, updateJobResponse.Results[0].Job.Status.State)
}

// This test verifies if we try to do an S3 upload for a file that doesn't exist with get the
// appropriate path error from the RST package.
func TestGenerateSubmissionFailure(t *testing.T) {
	// Set setup:
	tmpPathDBPath, cleanupPathDBPath, err := tempPathForTesting(testDBBasePath)
	require.NoError(t, err, "error setting up for test")
	defer cleanupPathDBPath(t)

	logger := zaptest.NewLogger(t)

	mountPoint := filesystem.NewMockFS()
	// Intentionally don't create any files in the MockFS.

	// We don't need a full worker manager for this test.
	remoteStorageTargets := []*flex.RemoteStorageTarget{{Id: 1, Type: &flex.RemoteStorageTarget_S3_{}}}
	workerManager, err := workermgr.NewManager(context.Background(), logger, workermgr.Config{}, []worker.Config{}, remoteStorageTargets, &flex.BeeRemoteNode{}, mountPoint)
	require.NoError(t, err)

	jobMgrConfig := Config{
		PathDBPath: tmpPathDBPath,
	}

	jobManager := NewManager(logger, jobMgrConfig, workerManager)
	require.NoError(t, jobManager.Start())

	require.NoError(t, err)
	jobRequest := &beeremote.JobRequest{
		Path:                "/foo/bar",
		RemoteStorageTarget: 1,
		Type: &beeremote.JobRequest_Sync{
			Sync: &flex.SyncJob{
				Operation: flex.SyncJob_UPLOAD,
			},
		},
	}

	// Submit a job request for a file that doesn't exist:
	response, err := jobManager.SubmitJobRequest(jobRequest)
	assert.Error(t, err)
	var pathError *fs.PathError
	assert.ErrorAs(t, err, &pathError)
	assert.Nil(t, response)
}

func TestUpdateJobResults(t *testing.T) {
	tmpPathDBPath, cleanupPathDBPath, err := tempPathForTesting(testDBBasePath)
	require.NoError(t, err, "error setting up for test")
	defer cleanupPathDBPath(t)

	logger := zaptest.NewLogger(t)
	workerMgrConfig := workermgr.Config{}
	remoteStorageTargets := []*flex.RemoteStorageTarget{{Id: 0, Type: &flex.RemoteStorageTarget_Mock{Mock: "test"}}}
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
						MethodName: "SubmitWork",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							&flex.Work_Status{
								State:   flex.Work_SCHEDULED,
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

	mountPoint := filesystem.NewMockFS()
	mountPoint.CreateWriteClose("/test/myfile", make([]byte, 15))

	workerManager, err := workermgr.NewManager(context.Background(), logger, workerMgrConfig, workerConfigs, remoteStorageTargets, &flex.BeeRemoteNode{}, mountPoint)
	require.NoError(t, err)
	require.NoError(t, workerManager.Start())

	jobMgrConfig := Config{
		PathDBPath: tmpPathDBPath,
	}

	jobManager := NewManager(logger, jobMgrConfig, workerManager)
	require.NoError(t, jobManager.Start())

	testJobRequest := &beeremote.JobRequest{
		Path:                "/test/myfile",
		Name:                "test job 1",
		Priority:            3,
		Type:                &beeremote.JobRequest_Mock{Mock: &flex.MockJob{NumTestSegments: 2}},
		RemoteStorageTarget: 0,
	}

	// Verify once all WRs are in the same terminal state the job state
	// transitions correctly:
	for _, expectedStatus := range []flex.Work_State{flex.Work_COMPLETED, flex.Work_CANCELLED} {

		js, err := jobManager.SubmitJobRequest(testJobRequest)
		require.NoError(t, err)

		// The first response should not finish the job:
		workResponse1 := &flex.Work{
			Path:      js.Job.Request.Path,
			JobId:     js.Job.GetId(),
			RequestId: "0",
			Status: &flex.Work_Status{
				State:   expectedStatus,
				Message: expectedStatus.String(),
			},
			Parts: []*flex.Work_Part{},
		}

		err = jobManager.UpdateWork(workResponse1)
		require.NoError(t, err)

		getJobsRequest := &beeremote.GetJobsRequest{
			Query: &beeremote.GetJobsRequest_ByJobIdAndPath{
				ByJobIdAndPath: &beeremote.GetJobsRequest_QueryIdAndPath{
					JobId: js.Job.GetId(),
					Path:  js.Job.Request.Path,
				},
			},
			IncludeWorkRequests: false,
			IncludeWorkResults:  true,
		}
		responses := make(chan *beeremote.GetJobsResponse, 1)
		err = jobManager.GetJobs(context.Background(), getJobsRequest, responses)
		require.NoError(t, err)
		resp := <-responses

		// Work result order is not guaranteed...
		for _, wr := range resp.Results[0].WorkResults {
			if wr.Work.GetRequestId() == "0" {
				require.Equal(t, expectedStatus, wr.Work.Status.GetState())
			} else {
				require.Equal(t, flex.Work_SCHEDULED, wr.Work.Status.GetState())
			}
		}
		require.Equal(t, beeremote.Job_SCHEDULED, resp.Results[0].Job.Status.State)

		// The second response should finish the job:
		workResponse2 := &flex.Work{
			Path:      js.Job.Request.Path,
			JobId:     js.Job.GetId(),
			RequestId: "1",
			Status: &flex.Work_Status{
				State:   expectedStatus,
				Message: expectedStatus.String(),
			},
			Parts: []*flex.Work_Part{},
		}
		err = jobManager.UpdateWork(workResponse2)
		require.NoError(t, err)

		responses = make(chan *beeremote.GetJobsResponse, 1)
		err = jobManager.GetJobs(context.Background(), getJobsRequest, responses)
		require.NoError(t, err)
		resp = <-responses
		require.Equal(t, expectedStatus, resp.Results[0].WorkResults[0].Work.Status.GetState())
		require.Equal(t, expectedStatus, resp.Results[0].WorkResults[1].Work.Status.GetState())
		switch expectedStatus {
		case flex.Work_COMPLETED:
			require.Equal(t, beeremote.Job_COMPLETED, resp.Results[0].Job.Status.GetState())
		case flex.Work_CANCELLED:
			require.Equal(t, beeremote.Job_CANCELLED, resp.Results[0].Job.Status.GetState())
		default:
			require.Fail(t, "received an unexpected status", "likely the test needs to be updated to add a new status compare the job status against")
		}

	}

	// Test if all WRs are in a terminal state but there is a mismatch the job
	// state is unknown:
	js, err := jobManager.SubmitJobRequest(testJobRequest)
	require.NoError(t, err)

	workResult1 := &flex.Work{
		Path:      js.Job.Request.Path,
		JobId:     js.Job.GetId(),
		RequestId: "0",
		Status: &flex.Work_Status{
			State:   flex.Work_COMPLETED,
			Message: flex.Work_COMPLETED.String(),
		},
		Parts: []*flex.Work_Part{},
	}

	workResult2 := &flex.Work{
		Path:      js.Job.Request.Path,
		JobId:     js.Job.GetId(),
		RequestId: "1",
		Status: &flex.Work_Status{
			State:   flex.Work_CANCELLED,
			Message: flex.Work_CANCELLED.String(),
		},
		Parts: []*flex.Work_Part{},
	}

	err = jobManager.UpdateWork(workResult1)
	require.NoError(t, err)
	err = jobManager.UpdateWork(workResult2)
	require.NoError(t, err)

	getJobsRequest := &beeremote.GetJobsRequest{
		Query: &beeremote.GetJobsRequest_ByJobIdAndPath{
			ByJobIdAndPath: &beeremote.GetJobsRequest_QueryIdAndPath{
				JobId: js.Job.GetId(),
				Path:  js.Job.Request.Path,
			},
		},
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}

	responses := make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobsRequest, responses)
	require.NoError(t, err)
	resp := <-responses
	require.Equal(t, beeremote.Job_UNKNOWN, resp.Results[0].Job.Status.State)

}
