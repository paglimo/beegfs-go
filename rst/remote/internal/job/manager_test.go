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
	"github.com/thinkparq/bee-remote/remote/internal/worker"
	"github.com/thinkparq/bee-remote/remote/internal/workermgr"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/kvstore"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"
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
							flex.Work_Status_builder{
								State:   flex.Work_SCHEDULED,
								Message: "test expects a scheduled request",
							}.Build(),
							nil,
						},
					},
					{
						MethodName: "UpdateWork",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							flex.Work_Status_builder{
								State:   flex.Work_CANCELLED,
								Message: "test expects a cancelled request",
							}.Build(),
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

	remoteStorageTargets := []*flex.RemoteStorageTarget{flex.RemoteStorageTarget_builder{Id: 0, Mock: proto.String("test")}.Build(), flex.RemoteStorageTarget_builder{Id: 1, Mock: proto.String("test")}.Build()}
	workerManager, err := workermgr.NewManager(context.Background(), logger, workerMgrConfig, workerConfigs, remoteStorageTargets, &flex.BeeRemoteNode{}, mountPoint)
	require.NoError(t, err)
	require.NoError(t, workerManager.Start())

	jobMgrConfig := Config{
		PathDBPath: tmpPathDBPath,
	}

	jobManager := NewManager(logger, jobMgrConfig, workerManager)
	require.NoError(t, jobManager.Start())

	// When we initially submit a job the state should be scheduled:
	testJobRequest := beeremote.JobRequest_builder{
		Path:                "/test/myfile",
		Name:                "test job 1",
		Priority:            3,
		Mock:                flex.MockJob_builder{NumTestSegments: 4}.Build(),
		RemoteStorageTarget: 0,
	}.Build()

	_, err = jobManager.SubmitJobRequest(testJobRequest)
	require.NoError(t, err)

	getJobRequestsByPrefix := beeremote.GetJobsRequest_builder{
		ByPathPrefix:        proto.String("/"),
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}.Build()

	responses := make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByPrefix, responses)
	require.NoError(t, err)
	getJobsResponse := <-responses
	assert.Equal(t, beeremote.Job_SCHEDULED, getJobsResponse.GetResults()[0].GetJob().GetStatus().GetState())

	assert.Len(t, getJobsResponse.GetResults()[0].GetWorkResults(), 4)
	for _, wr := range getJobsResponse.GetResults()[0].GetWorkResults() {
		assert.Equal(t, flex.Work_SCHEDULED, wr.GetWork().GetStatus().GetState())
	}

	scheduledJobID := getJobsResponse.GetResults()[0].GetJob().GetId()

	// If we try to submit another job for the same path with the same RST an error should be returned:
	jr, err := jobManager.SubmitJobRequest(testJobRequest)
	assert.Nil(t, jr)
	assert.Error(t, err)

	// No job should be created:
	getJobRequestsByPath := beeremote.GetJobsRequest_builder{
		ByExactPath: proto.String("/test/myfile"),
	}.Build()
	responses = make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByPath, responses)
	require.NoError(t, err)
	getJobsResponse = <-responses
	assert.Len(t, getJobsResponse.GetResults(), 1)
	assert.Equal(t, beeremote.Job_SCHEDULED, getJobsResponse.GetResults()[0].GetJob().GetStatus().GetState())

	// If we schedule a job for a different RST it should be scheduled:
	testJobRequest2 := beeremote.JobRequest_builder{
		Path:                "/test/myfile",
		Name:                "test job 1",
		Priority:            3,
		Mock:                flex.MockJob_builder{NumTestSegments: 4}.Build(),
		RemoteStorageTarget: 1,
	}.Build()
	jr, err = jobManager.SubmitJobRequest(testJobRequest2)
	assert.NoError(t, err)
	assert.Equal(t, beeremote.Job_SCHEDULED, jr.GetJob().GetStatus().GetState())

	// If we cancel a job the state of the job and work requests should update:
	updateJobRequest := beeremote.UpdateJobRequest_builder{
		ByExactPath: proto.String("/test/myfile"),
		NewState:    beeremote.UpdateJobRequest_CANCELLED,
	}.Build()
	jobManager.JobUpdates <- updateJobRequest
	time.Sleep(2 * time.Second)

	getJobRequestsByID := beeremote.GetJobsRequest_builder{
		ByJobIdAndPath: beeremote.GetJobsRequest_QueryIdAndPath_builder{
			JobId: scheduledJobID,
			Path:  testJobRequest2.GetPath(),
		}.Build(),
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}.Build()

	responses = make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByID, responses)
	getJobsResponse = <-responses
	require.NoError(t, err)
	assert.Equal(t, beeremote.Job_CANCELLED, getJobsResponse.GetResults()[0].GetJob().GetStatus().GetState())

	for _, wr := range getJobsResponse.GetResults()[0].GetWorkResults() {
		assert.Equal(t, flex.Work_CANCELLED, wr.GetWork().GetStatus().GetState())
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
							flex.Work_Status_builder{
								State:   flex.Work_SCHEDULED,
								Message: "test expects a scheduled request",
							}.Build(),
							nil,
						},
					},
					{
						MethodName: "UpdateWork",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							flex.Work_Status_builder{
								State:   flex.Work_CANCELLED,
								Message: "test expects a cancelled request",
							}.Build(),
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

	remoteStorageTargets := []*flex.RemoteStorageTarget{flex.RemoteStorageTarget_builder{Id: 0, Mock: proto.String("test")}.Build(), flex.RemoteStorageTarget_builder{Id: 1, Mock: proto.String("test")}.Build()}
	workerManager, err := workermgr.NewManager(context.Background(), logger, workerMgrConfig, workerConfigs, remoteStorageTargets, &flex.BeeRemoteNode{}, mountPoint)
	require.NoError(t, err)
	require.NoError(t, workerManager.Start())

	jobMgrConfig := Config{
		PathDBPath: tmpPathDBPath,
	}

	jobManager := NewManager(logger, jobMgrConfig, workerManager)
	require.NoError(t, jobManager.Start())

	// Submit two jobs for testing:
	testJobRequest1 := beeremote.JobRequest_builder{
		Path:                "/test/myfile",
		Name:                "test job 1",
		Priority:            3,
		Mock:                flex.MockJob_builder{NumTestSegments: 4}.Build(),
		RemoteStorageTarget: 0,
	}.Build()
	testJobRequest2 := beeremote.JobRequest_builder{
		Path:                "/test/myfile2",
		Name:                "test job 2",
		Priority:            3,
		Mock:                flex.MockJob_builder{NumTestSegments: 2}.Build(),
		RemoteStorageTarget: 1,
	}.Build()

	_, err = jobManager.SubmitJobRequest(testJobRequest1)
	require.NoError(t, err)

	// We only interact with the second job request by its job ID:
	submitJobResponse2, err := jobManager.SubmitJobRequest(testJobRequest2)
	require.NoError(t, err)

	////////////////////////////////////
	// First test deleting jobs by path:
	////////////////////////////////////
	// If we delete a job that has not yet reached a terminal state, nothing should happen:
	deleteJobByPathRequest := beeremote.UpdateJobRequest_builder{
		ByExactPath: proto.String(testJobRequest1.GetPath()),
		NewState:    beeremote.UpdateJobRequest_DELETED,
	}.Build()
	deleteJobByPathResponse, err := jobManager.UpdateJob(deleteJobByPathRequest)
	require.NoError(t, err)                          // Only internal errors should return an error.
	assert.False(t, deleteJobByPathResponse.GetOk()) // Response should not be okay.
	assert.Contains(t, deleteJobByPathResponse.GetMessage(), "because it has not reached a terminal state")

	// Status on the job should not change:
	assert.Equal(t, beeremote.Job_SCHEDULED, deleteJobByPathResponse.GetResults()[0].GetJob().GetStatus().GetState())
	assert.Equal(t, "finished scheduling work requests", deleteJobByPathResponse.GetResults()[0].GetJob().GetStatus().GetMessage())

	// Work results should all still be scheduled:
	assert.Len(t, deleteJobByPathResponse.GetResults()[0].GetWorkResults(), 4)
	for _, wr := range deleteJobByPathResponse.GetResults()[0].GetWorkResults() {
		assert.Equal(t, flex.Work_SCHEDULED, wr.GetWork().GetStatus().GetState())
	}

	// Cancel the job:
	cancelJobByPathRequest := beeremote.UpdateJobRequest_builder{
		ByExactPath: proto.String(testJobRequest1.GetPath()),
		NewState:    beeremote.UpdateJobRequest_CANCELLED,
	}.Build()
	cancelJobByPathResponse, err := jobManager.UpdateJob(cancelJobByPathRequest)
	require.NoError(t, err)
	assert.True(t, cancelJobByPathResponse.GetOk())

	// Work results should all be cancelled:
	assert.Len(t, cancelJobByPathResponse.GetResults()[0].GetWorkResults(), 4)
	for _, wr := range cancelJobByPathResponse.GetResults()[0].GetWorkResults() {
		assert.Equal(t, flex.Work_CANCELLED, wr.GetWork().GetStatus().GetState())
	}

	// Then delete it:
	deleteJobByPathResponse, err = jobManager.UpdateJob(deleteJobByPathRequest)
	assert.NoError(t, err)
	assert.True(t, deleteJobByPathResponse.GetOk())
	assert.Equal(t, "job scheduled for deletion", deleteJobByPathResponse.GetResults()[0].GetJob().GetStatus().GetMessage())

	// Verify the job was fully deleted:
	getJobRequestsByPath := beeremote.GetJobsRequest_builder{
		ByExactPath:         proto.String(testJobRequest1.GetPath()),
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}.Build()
	responses := make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByPath, responses)
	assert.ErrorIs(t, err, kvstore.ErrEntryNotInDB)

	////////////////////////////////
	// Then test deleting by job ID:
	////////////////////////////////

	// If we delete a job that has not yet reached a terminal state, nothing should happen:
	deleteJobByIDRequest := beeremote.UpdateJobRequest_builder{
		ByIdAndPath: beeremote.UpdateJobRequest_QueryIdAndPath_builder{
			JobId: submitJobResponse2.GetJob().GetId(),
			Path:  submitJobResponse2.GetJob().GetRequest().GetPath(),
		}.Build(),
		NewState: beeremote.UpdateJobRequest_DELETED,
	}.Build()
	updateJobByIDResponse, err := jobManager.UpdateJob(deleteJobByIDRequest)
	require.NoError(t, err)                        // Only internal errors should return an error.
	assert.False(t, updateJobByIDResponse.GetOk()) // However the response should not be okay.
	assert.Contains(t, updateJobByIDResponse.GetMessage(), "because it has not reached a terminal state")

	// Status on the job should not change:
	assert.Equal(t, beeremote.Job_SCHEDULED, updateJobByIDResponse.GetResults()[0].GetJob().GetStatus().GetState())
	assert.Equal(t, "finished scheduling work requests", updateJobByIDResponse.GetResults()[0].GetJob().GetStatus().GetMessage())

	assert.Len(t, updateJobByIDResponse.GetResults()[0].GetWorkResults(), 2)
	for _, wr := range updateJobByIDResponse.GetResults()[0].GetWorkResults() {
		assert.Equal(t, flex.Work_SCHEDULED, wr.GetWork().GetStatus().GetState())
	}

	// Cancel the job:
	cancelJobByIDRequest := beeremote.UpdateJobRequest_builder{
		ByIdAndPath: beeremote.UpdateJobRequest_QueryIdAndPath_builder{
			JobId: submitJobResponse2.GetJob().GetId(),
			Path:  submitJobResponse2.GetJob().GetRequest().GetPath(),
		}.Build(),
		NewState: beeremote.UpdateJobRequest_CANCELLED,
	}.Build()
	cancelJobByIDResponse, err := jobManager.UpdateJob(cancelJobByIDRequest)
	require.NoError(t, err)
	assert.True(t, cancelJobByIDResponse.GetOk())

	// Work requests should be cancelled:
	assert.Len(t, cancelJobByIDResponse.GetResults()[0].GetWorkResults(), 2)
	for _, wr := range cancelJobByIDResponse.GetResults()[0].GetWorkResults() {
		assert.Equal(t, flex.Work_CANCELLED, wr.GetWork().GetStatus().GetState())
	}

	// Then delete it:
	updateJobByIDResponse, err = jobManager.UpdateJob(deleteJobByIDRequest)
	assert.NoError(t, err)
	assert.True(t, updateJobByIDResponse.GetOk())
	assert.Equal(t, "job scheduled for deletion", updateJobByIDResponse.GetResults()[0].GetJob().GetStatus().GetMessage())

	// Verify the job was fully deleted:
	getJobRequestsByID := beeremote.GetJobsRequest_builder{
		ByJobIdAndPath: beeremote.GetJobsRequest_QueryIdAndPath_builder{
			JobId: submitJobResponse2.GetJob().GetId(),
			Path:  submitJobResponse2.GetJob().GetRequest().GetPath(),
		}.Build(),
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}.Build()
	responses = make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByID, responses)
	assert.ErrorIs(t, err, kvstore.ErrEntryNotInDB)

	////////////////////////////////
	// Test deleting completed jobs:
	////////////////////////////////

	response, err := jobManager.SubmitJobRequest(testJobRequest1)
	require.NoError(t, err)
	require.NotNil(t, response)
	// Complete the job by simulating a worker node updating the results.
	for i := range 4 {
		result := flex.Work_builder{
			Path:      response.GetJob().GetRequest().GetPath(),
			JobId:     response.GetJob().GetId(),
			RequestId: strconv.Itoa(i),
			Status: flex.Work_Status_builder{
				State:   flex.Work_COMPLETED,
				Message: "complete",
			}.Build(),
			Parts: []*flex.Work_Part{},
		}.Build()
		err = jobManager.UpdateWork(result)
		require.NoError(t, err)
	}

	// Refuse to cancel completed jobs:
	updateJobByIDRequest := beeremote.UpdateJobRequest_builder{
		ByIdAndPath: beeremote.UpdateJobRequest_QueryIdAndPath_builder{
			JobId: response.GetJob().GetId(),
			Path:  response.GetJob().GetRequest().GetPath(),
		}.Build(),
		NewState: beeremote.UpdateJobRequest_CANCELLED,
	}.Build()
	cancelJobByIDResponse, err = jobManager.UpdateJob(updateJobByIDRequest)
	require.NoError(t, err)
	assert.True(t, cancelJobByIDResponse.GetOk())
	assert.Contains(t, cancelJobByIDResponse.GetMessage(), "rejecting update for completed job")

	// Refuse to delete completed jobs by ID and path, the overall response should be ok:
	updateJobByIDRequest.SetNewState(beeremote.UpdateJobRequest_DELETED)
	deleteJobByIDResp, err := jobManager.UpdateJob(updateJobByIDRequest)
	require.NoError(t, err)
	assert.True(t, deleteJobByIDResp.GetOk())
	assert.Contains(t, deleteJobByIDResp.GetMessage(), "rejecting update for completed job")

	// Refuse to delete completed jobs by path, the overall response should be ok:
	updateJobByPathRequest := beeremote.UpdateJobRequest_builder{
		ByExactPath: proto.String(response.GetJob().GetRequest().GetPath()),
		NewState:    beeremote.UpdateJobRequest_DELETED,
	}.Build()
	deleteJobByPathResp, err := jobManager.UpdateJob(updateJobByPathRequest)
	require.NoError(t, err)
	assert.True(t, deleteJobByPathResp.GetOk())
	assert.Contains(t, deleteJobByPathResp.GetMessage(), "rejecting update for completed job")

	// Status on the job should have not changed at any point:
	assert.Equal(t, beeremote.Job_COMPLETED, deleteJobByPathResp.GetResults()[0].GetJob().GetStatus().GetState())
	assert.Equal(t, "successfully completed job", deleteJobByPathResp.GetResults()[0].GetJob().GetStatus().GetMessage())

	assert.Len(t, deleteJobByPathResp.GetResults()[0].GetWorkResults(), 4)
	for _, wr := range deleteJobByPathResp.GetResults()[0].GetWorkResults() {
		assert.Equal(t, flex.Work_COMPLETED, wr.GetWork().GetStatus().GetState())
	}

	// Deleting completed jobs by job ID and path is allowed when the update is forced:
	updateJobByIDRequest.SetForceUpdate(true)
	deleteJobByIDResp, err = jobManager.UpdateJob(updateJobByIDRequest)
	require.NoError(t, err)
	assert.True(t, deleteJobByIDResp.GetOk())
	assert.Contains(t, deleteJobByIDResp.GetMessage(), "")
	assert.Len(t, deleteJobByIDResp.GetResults(), 1)
	assert.Equal(t, beeremote.Job_COMPLETED, deleteJobByPathResp.GetResults()[0].GetJob().GetStatus().GetState())
	assert.Contains(t, deleteJobByIDResp.GetResults()[0].GetJob().GetStatus().GetMessage(), "job scheduled for deletion")

	responses = make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), beeremote.GetJobsRequest_builder{
		ByExactPath: proto.String("response.Job.Request.Path"),
	}.Build(), responses)
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
	expectedStatus := flex.Work_Status_builder{
		State:   flex.Work_CANCELLED,
		Message: "test expects a cancelled request",
	}.Build()

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

	remoteStorageTargets := []*flex.RemoteStorageTarget{flex.RemoteStorageTarget_builder{Id: 0, Mock: proto.String("test")}.Build()}
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
	testJobRequest := beeremote.JobRequest_builder{
		Path:                "/test/myfile",
		Name:                "test job 1",
		Priority:            3,
		Mock:                flex.MockJob_builder{NumTestSegments: 4}.Build(),
		RemoteStorageTarget: 0,
	}.Build()
	jobManager.JobRequests <- testJobRequest
	time.Sleep(2 * time.Second)
	getJobRequestsByPrefix := beeremote.GetJobsRequest_builder{
		ByPathPrefix:        proto.String("/"),
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}.Build()
	responses := make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByPrefix, responses)
	require.NoError(t, err)
	getJobsResponse := <-responses
	assert.Equal(t, beeremote.Job_CANCELLED, getJobsResponse.GetResults()[0].GetJob().GetStatus().GetState())

	// JobMgr should have cancelled all outstanding requests:
	assert.Len(t, getJobsResponse.GetResults()[0].GetWorkResults(), 4)
	for _, wr := range getJobsResponse.GetResults()[0].GetWorkResults() {
		assert.Equal(t, flex.Work_CANCELLED, wr.GetWork().GetStatus().GetState())
	}

	scheduledJobID := getJobsResponse.GetResults()[0].GetJob().GetId()

	// The following sequence of events is unlikely in real work scenarios, but verifies how JobMgr
	// handles states. First try to cancel the already cancelled job. JobMgr should always attempt
	// to verify the work requests are cancelled on the worker nodes, even if they were previously
	// cancelled (calls are idempotent). This time we cannot definitely cancel the requests so their
	// state is unknown for some reason. As a result the Job status is now unknown.
	expectedStatus.SetState(flex.Work_UNKNOWN)
	expectedStatus.SetMessage("test expects an error communicating to the node")

	updateJobRequest := beeremote.UpdateJobRequest_builder{
		ByExactPath: proto.String("/test/myfile"),
		NewState:    beeremote.UpdateJobRequest_CANCELLED,
	}.Build()
	jobManager.UpdateJob(updateJobRequest)

	getJobRequestsByID := beeremote.GetJobsRequest_builder{
		ByJobIdAndPath: beeremote.GetJobsRequest_QueryIdAndPath_builder{
			JobId: scheduledJobID,
			Path:  testJobRequest.GetPath(),
		}.Build(),
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}.Build()
	responses = make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByID, responses)
	require.NoError(t, err)
	getJobsResponse = <-responses
	assert.Equal(t, beeremote.Job_UNKNOWN, getJobsResponse.GetResults()[0].GetJob().GetStatus().GetState())

	for _, wr := range getJobsResponse.GetResults()[0].GetWorkResults() {
		assert.Equal(t, flex.Work_UNKNOWN, wr.GetWork().GetStatus().GetState())
	}

	// Submit another request to cancel the job. This time the work requests are
	// cancelled so the job status and work requests should all be cancelled.
	expectedStatus.SetState(flex.Work_CANCELLED)
	expectedStatus.SetMessage("test expects a cancelled request")

	jobManager.UpdateJob(updateJobRequest)

	getJobRequestsByID = beeremote.GetJobsRequest_builder{
		ByJobIdAndPath: beeremote.GetJobsRequest_QueryIdAndPath_builder{
			JobId: scheduledJobID,
			Path:  testJobRequest.GetPath(),
		}.Build(),
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}.Build()
	responses = make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByID, responses)
	getJobsResponse = <-responses
	require.NoError(t, err)
	assert.Equal(t, beeremote.Job_CANCELLED, getJobsResponse.GetResults()[0].GetJob().GetStatus().GetState())

	for _, wr := range getJobsResponse.GetResults()[0].GetWorkResults() {
		assert.Equal(t, flex.Work_CANCELLED, wr.GetWork().GetStatus().GetState())
	}

	// If we submit a job the state should be unknown if any work requests were
	// failed and unable to be cancelled.
	expectedStatus.SetState(flex.Work_FAILED)
	expectedStatus.SetMessage("test expects a failed request")

	jobResponse, err := jobManager.SubmitJobRequest(testJobRequest)
	assert.Error(t, err)
	assert.Equal(t, beeremote.Job_UNKNOWN, jobResponse.GetJob().GetStatus().GetState())
	jobID := jobResponse.GetJob().GetId()

	// We should not be able to delete jobs in an unknown state:
	updateJobRequest = beeremote.UpdateJobRequest_builder{
		ByIdAndPath: beeremote.UpdateJobRequest_QueryIdAndPath_builder{
			JobId: jobID,
			Path:  testJobRequest.GetPath(),
		}.Build(),
		NewState: beeremote.UpdateJobRequest_DELETED,
	}.Build()
	updateJobResponse, err := jobManager.UpdateJob(updateJobRequest)
	require.NoError(t, err)
	assert.Equal(t, beeremote.Job_UNKNOWN, updateJobResponse.GetResults()[0].GetJob().GetStatus().GetState())

	// We should reject new jobs while there is a job in an unknown state:
	jobResponse, err = jobManager.SubmitJobRequest(testJobRequest)
	require.Error(t, err)
	assert.Nil(t, jobResponse)

	// We should be able to cancel jobs in an unknown state once the WRs can be cancelled:
	expectedStatus.SetState(flex.Work_CANCELLED)
	expectedStatus.SetMessage("test expects a cancelled request")

	updateJobRequest = beeremote.UpdateJobRequest_builder{
		ByIdAndPath: beeremote.UpdateJobRequest_QueryIdAndPath_builder{
			JobId: jobID,
			Path:  testJobRequest.GetPath(),
		}.Build(),
		NewState: beeremote.UpdateJobRequest_CANCELLED,
	}.Build()
	updateJobResponse, err = jobManager.UpdateJob(updateJobRequest)
	require.NoError(t, err)
	assert.Equal(t, beeremote.Job_CANCELLED, updateJobResponse.GetResults()[0].GetJob().GetStatus().GetState())

	// Submit another jobs whose work requests cannot be scheduled and an error occurs cancelling
	// them so the overall job status is unknown:
	expectedStatus.SetState(flex.Work_UNKNOWN)
	expectedStatus.SetMessage("test expects the work request status is unknown")

	jobResponse, err = jobManager.SubmitJobRequest(testJobRequest)
	assert.Error(t, err)
	assert.Equal(t, beeremote.Job_UNKNOWN, jobResponse.GetJob().GetStatus().GetState())
	jobID = jobResponse.GetJob().GetId()

	// Even if we cannot contact the worker nodes to determine the WR statuses, we can still force
	// the job to be cancelled:
	updateJobRequest = beeremote.UpdateJobRequest_builder{
		ByIdAndPath: beeremote.UpdateJobRequest_QueryIdAndPath_builder{
			JobId: jobID,
			Path:  testJobRequest.GetPath(),
		}.Build(),
		NewState:    beeremote.UpdateJobRequest_CANCELLED,
		ForceUpdate: true,
	}.Build()

	updateJobResponse, err = jobManager.UpdateJob(updateJobRequest)
	require.NoError(t, err)
	require.True(t, updateJobResponse.GetOk())
	assert.Equal(t, beeremote.Job_CANCELLED, updateJobResponse.GetResults()[0].GetJob().GetStatus().GetState())
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
	remoteStorageTargets := []*flex.RemoteStorageTarget{flex.RemoteStorageTarget_builder{Id: 1, S3: &flex.RemoteStorageTarget_S3{}}.Build()}
	workerManager, err := workermgr.NewManager(context.Background(), logger, workermgr.Config{}, []worker.Config{}, remoteStorageTargets, &flex.BeeRemoteNode{}, mountPoint)
	require.NoError(t, err)

	jobMgrConfig := Config{
		PathDBPath: tmpPathDBPath,
	}

	jobManager := NewManager(logger, jobMgrConfig, workerManager)
	require.NoError(t, jobManager.Start())

	require.NoError(t, err)
	jobRequest := beeremote.JobRequest_builder{
		Path:                "/foo/bar",
		RemoteStorageTarget: 1,
		Sync: flex.SyncJob_builder{
			Operation: flex.SyncJob_UPLOAD,
		}.Build(),
	}.Build()

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
	remoteStorageTargets := []*flex.RemoteStorageTarget{flex.RemoteStorageTarget_builder{Id: 0, Mock: proto.String("test")}.Build()}
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
							flex.Work_Status_builder{
								State:   flex.Work_SCHEDULED,
								Message: "test expects a scheduled request",
							}.Build(),
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

	testJobRequest := beeremote.JobRequest_builder{
		Path:                "/test/myfile",
		Name:                "test job 1",
		Priority:            3,
		Mock:                flex.MockJob_builder{NumTestSegments: 2}.Build(),
		RemoteStorageTarget: 0,
	}.Build()

	// Verify once all WRs are in the same terminal state the job state
	// transitions correctly:
	for _, expectedStatus := range []flex.Work_State{flex.Work_COMPLETED, flex.Work_CANCELLED} {

		js, err := jobManager.SubmitJobRequest(testJobRequest)
		require.NoError(t, err)

		// The first response should not finish the job:
		workResponse1 := flex.Work_builder{
			Path:      js.GetJob().GetRequest().GetPath(),
			JobId:     js.GetJob().GetId(),
			RequestId: "0",
			Status: flex.Work_Status_builder{
				State:   expectedStatus,
				Message: expectedStatus.String(),
			}.Build(),
			Parts: []*flex.Work_Part{},
		}.Build()

		err = jobManager.UpdateWork(workResponse1)
		require.NoError(t, err)

		getJobsRequest := beeremote.GetJobsRequest_builder{
			ByJobIdAndPath: beeremote.GetJobsRequest_QueryIdAndPath_builder{
				JobId: js.GetJob().GetId(),
				Path:  js.GetJob().GetRequest().GetPath(),
			}.Build(),
			IncludeWorkRequests: false,
			IncludeWorkResults:  true,
		}.Build()
		responses := make(chan *beeremote.GetJobsResponse, 1)
		err = jobManager.GetJobs(context.Background(), getJobsRequest, responses)
		require.NoError(t, err)
		resp := <-responses

		// Work result order is not guaranteed...
		for _, wr := range resp.GetResults()[0].GetWorkResults() {
			if wr.GetWork().GetRequestId() == "0" {
				require.Equal(t, expectedStatus, wr.GetWork().GetStatus().GetState())
			} else {
				require.Equal(t, flex.Work_SCHEDULED, wr.GetWork().GetStatus().GetState())
			}
		}
		require.Equal(t, beeremote.Job_SCHEDULED, resp.GetResults()[0].GetJob().GetStatus().GetState())

		// The second response should finish the job:
		workResponse2 := flex.Work_builder{
			Path:      js.GetJob().GetRequest().GetPath(),
			JobId:     js.GetJob().GetId(),
			RequestId: "1",
			Status: flex.Work_Status_builder{
				State:   expectedStatus,
				Message: expectedStatus.String(),
			}.Build(),
			Parts: []*flex.Work_Part{},
		}.Build()
		err = jobManager.UpdateWork(workResponse2)
		require.NoError(t, err)

		responses = make(chan *beeremote.GetJobsResponse, 1)
		err = jobManager.GetJobs(context.Background(), getJobsRequest, responses)
		require.NoError(t, err)
		resp = <-responses
		require.Equal(t, expectedStatus, resp.GetResults()[0].GetWorkResults()[0].GetWork().GetStatus().GetState())
		require.Equal(t, expectedStatus, resp.GetResults()[0].GetWorkResults()[1].GetWork().GetStatus().GetState())
		switch expectedStatus {
		case flex.Work_COMPLETED:
			require.Equal(t, beeremote.Job_COMPLETED, resp.GetResults()[0].GetJob().GetStatus().GetState())
		case flex.Work_CANCELLED:
			require.Equal(t, beeremote.Job_CANCELLED, resp.GetResults()[0].GetJob().GetStatus().GetState())
		default:
			require.Fail(t, "received an unexpected status", "likely the test needs to be updated to add a new status compare the job status against")
		}

	}

	// Test if all WRs are in a terminal state but there is a mismatch the job
	// state is unknown:
	js, err := jobManager.SubmitJobRequest(testJobRequest)
	require.NoError(t, err)

	workResult1 := flex.Work_builder{
		Path:      js.GetJob().GetRequest().GetPath(),
		JobId:     js.GetJob().GetId(),
		RequestId: "0",
		Status: flex.Work_Status_builder{
			State:   flex.Work_COMPLETED,
			Message: flex.Work_COMPLETED.String(),
		}.Build(),
		Parts: []*flex.Work_Part{},
	}.Build()

	workResult2 := flex.Work_builder{
		Path:      js.GetJob().GetRequest().GetPath(),
		JobId:     js.GetJob().GetId(),
		RequestId: "1",
		Status: flex.Work_Status_builder{
			State:   flex.Work_CANCELLED,
			Message: flex.Work_CANCELLED.String(),
		}.Build(),
		Parts: []*flex.Work_Part{},
	}.Build()

	err = jobManager.UpdateWork(workResult1)
	require.NoError(t, err)
	err = jobManager.UpdateWork(workResult2)
	require.NoError(t, err)

	getJobsRequest := beeremote.GetJobsRequest_builder{
		ByJobIdAndPath: beeremote.GetJobsRequest_QueryIdAndPath_builder{
			JobId: js.GetJob().GetId(),
			Path:  js.GetJob().GetRequest().GetPath(),
		}.Build(),
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}.Build()

	responses := make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobsRequest, responses)
	require.NoError(t, err)
	resp := <-responses
	require.Equal(t, beeremote.Job_UNKNOWN, resp.GetResults()[0].GetJob().GetStatus().GetState())

}
