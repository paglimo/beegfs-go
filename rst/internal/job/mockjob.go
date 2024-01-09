package job

import (
	"fmt"
	"strconv"

	"github.com/thinkparq/bee-remote/internal/worker"
	"github.com/thinkparq/bee-remote/internal/workermgr"
	"github.com/thinkparq/gobee/rst"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

// MockJob does not currently incorporate mock.Mock but rather simply returns
// static responses to most calls except allocate which generates TestSegments
// based on the Mock RSTs NumTestSegments configuration parameter.
type MockJob struct {
	*baseJob
}

var _ Job = &MockJob{}

func (j *MockJob) GetRSTID() string {
	return j.GetRequest().GetMock().Rst
}

func (j *MockJob) Allocate(rst rst.Client) (workermgr.JobSubmission, bool, error) {

	if len(j.Segments) == 0 {
		numTestSegments := 1
		if j.GetRequest().GetMock().NumTestSegments != 0 {
			numTestSegments = int(j.GetRequest().GetMock().NumTestSegments)
		}

		for i := 0; i < numTestSegments; i++ {
			s := &Segment{}
			j.Segments = append(j.Segments, s)
		}
	}

	return workermgr.JobSubmission{
		JobID:        j.GetId(),
		WorkRequests: j.GetWorkRequests(),
	}, false, nil
}

func (j *MockJob) GetWorkRequests() []*flex.WorkRequest {

	workRequests := make([]*flex.WorkRequest, 0)
	for i, s := range j.Segments {
		wr := flex.WorkRequest{
			JobId:      j.GetID(),
			RequestId:  strconv.Itoa(i),
			ExternalId: j.ExternalId,
			Path:       j.GetPath(),
			Status:     proto.Clone(j.Status()).(*flex.RequestStatus),
			Segment:    &s.segment,
			Type: &flex.WorkRequest_Mock{
				Mock: "mockjob",
			},
		}
		workRequests = append(workRequests, &wr)
	}
	return workRequests
}

func (j *MockJob) Complete(client rst.Client, results map[string]worker.WorkResult, abort bool) error {
	return nil
}

// GobEncode encodes the MockJob into a byte slice for gob serialization. Refer
// to the GobEncode method on baseJob if you are implementing a new job type.
func (j *MockJob) GobEncode() ([]byte, error) {

	if j.baseJob == nil {
		return nil, fmt.Errorf("cannot encode a nil job (most likely this is a bug somewhere else)")
	}
	return j.baseJob.GobEncode()
}

// GobDecode decodes a byte slice into the MockJob. Refer to the GobDecode
// method on baseJob if you are implementing a new job type.
func (j *MockJob) GobDecode(data []byte) error {

	if j.baseJob == nil {
		j.baseJob = &baseJob{}
	}

	return j.baseJob.GobDecode(data)
}
