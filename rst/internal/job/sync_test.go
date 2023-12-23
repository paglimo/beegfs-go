package job

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/bee-remote/internal/filesystem"
	"github.com/thinkparq/bee-remote/internal/rst"
	"github.com/thinkparq/bee-remote/internal/worker"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/beesync"
)

func getTestSyncJob(path string, operation beesync.SyncJob_Operation) SyncJob {
	return SyncJob{
		baseJob: &baseJob{
			&beeremote.Job{
				Request: &beeremote.JobRequest{
					Path: path,
					Type: &beeremote.JobRequest_Sync{
						Sync: &beesync.SyncJob{
							Operation:           operation,
							RemoteStorageTarget: "1",
						},
					},
				},
			},
		},
	}
}

type test struct {
	fileSize        int64
	segmentCount    int
	partsPerSegment int
	operation       beesync.SyncJob_Operation
	expectations    map[string]expectation
}

type expectation struct {
	offsetStart int64
	offsetStop  int64
	partsStart  int32
	partsStop   int32
}

func TestAllocateS3(t *testing.T) {

	// Test setup:
	var err error
	filesystem.MountPoint, err = filesystem.New("mock")
	require.NoError(t, err)
	tests := []test{
		{
			// Test when the file size lets it be split into even segments:
			fileSize:        int64(1 << 20), // 20MB
			segmentCount:    2,
			partsPerSegment: 2,
			operation:       beesync.SyncJob_UPLOAD,
			expectations: map[string]expectation{
				"0": {
					offsetStart: 0,
					offsetStop:  524287,
					partsStart:  1,
					partsStop:   2,
				},
				"1": {
					offsetStart: 524288,
					offsetStop:  1048575,
					partsStart:  3,
					partsStop:   4,
				},
			},
		}, {
			// Test when the file size does not let it be split into even segments:
			fileSize:        int64(1 << 20), // 20MB
			segmentCount:    6,
			partsPerSegment: 4,
			operation:       beesync.SyncJob_DOWNLOAD,
			expectations: map[string]expectation{
				"0": {
					offsetStart: 0,
					offsetStop:  174761,
					partsStart:  1,
					partsStop:   4,
				},
				"1": {
					offsetStart: 174762,
					offsetStop:  349523,
					partsStart:  5,
					partsStop:   8,
				},
				"2": {
					offsetStart: 349524,
					offsetStop:  524285,
					partsStart:  9,
					partsStop:   12,
				},
				"3": {
					offsetStart: 524286,
					offsetStop:  699047,
					partsStart:  13,
					partsStop:   16,
				},
				"4": {
					offsetStart: 699048,
					offsetStop:  873809,
					partsStart:  17,
					partsStop:   20,
				},
				"5": {
					offsetStart: 873810,
					offsetStop:  1048575,
					partsStart:  21,
					partsStop:   24,
				},
			},
		},
	}

	// Run tests:
	for i, test := range tests {
		path := "/foo/bar" + strconv.Itoa(i)
		filesystem.MountPoint.CreateWriteClose(path, make([]byte, test.fileSize)) // 20MB

		rstClient := &rst.MockClient{}
		if test.operation == beesync.SyncJob_UPLOAD {
			rstClient.On("CreateUpload").Return("mpartid", nil)
		}
		rstClient.On("RecommendedSegments", test.fileSize).Return(rst.S3, test.segmentCount, test.partsPerSegment)

		syncJob := getTestSyncJob(path, test.operation)

		js, retry, err := syncJob.Allocate(rstClient)
		assert.NoError(t, err)
		assert.False(t, retry)
		assert.Len(t, js.WorkRequests, test.segmentCount)

		for _, wr := range js.WorkRequests {

			sr, ok := wr.(*worker.SyncRequest)
			if !ok {
				panic("unexpected work request in test")
			}

			e, ok := test.expectations[sr.RequestId]
			require.True(t, ok)
			assert.Equal(t, e.offsetStart, sr.Segment.OffsetStart)
			assert.Equal(t, e.offsetStop, sr.Segment.OffsetStop)
			assert.Equal(t, e.partsStart, sr.Segment.GetS3().PartsStart)
			assert.Equal(t, e.partsStop, sr.Segment.GetS3().PartsStop)
			if test.operation == beesync.SyncJob_UPLOAD {
				assert.Equal(t, "mpartid", sr.Segment.GetS3().MultipartId)
			} else {
				assert.Equal(t, "", sr.Segment.GetS3().MultipartId)
			}
		}
		filesystem.MountPoint.Remove(path)
	}
}
