package job

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"strconv"

	"github.com/thinkparq/bee-remote/internal/rst"
	"github.com/thinkparq/bee-remote/internal/worker"
	"github.com/thinkparq/bee-remote/internal/workermgr"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

// MockJob does not currently incorporate mock.Mock but rather simply returns
// static responses to most calls except allocate which generates TestSegments
// based on the Mock RSTs NumTestSegments configuration parameter.
type MockJob struct {
	*baseJob
	TestSegments []string
}

var _ Job = &MockJob{}

func (j *MockJob) GetRSTID() string {
	return j.GetRequest().GetMock().Rst
}

func (j *MockJob) Allocate(rst rst.Client) (workermgr.JobSubmission, bool, error) {

	if len(j.TestSegments) == 0 {
		numTestSegments := 1
		if j.GetRequest().GetMock().NumTestSegments != 0 {
			numTestSegments = int(j.GetRequest().GetMock().NumTestSegments)
		}

		for i := 0; i < numTestSegments; i++ {
			j.TestSegments = append(j.TestSegments, strconv.Itoa(i))
		}
	}

	workRequests := make([]worker.WorkRequest, 0)

	for i, s := range j.TestSegments {
		wr := worker.MockRequest{
			RequestID: strconv.Itoa(i),
			//Metadata:  proto.Clone(j.GetMetadata()).(*flex.JobMetadata),
			Metadata: &flex.JobMetadata{
				Id:         j.GetID(),
				Status:     proto.Clone(j.Status()).(*flex.RequestStatus),
				ExternalId: j.ExternalId,
			},
			Path:    j.GetPath(),
			Segment: s,
		}
		workRequests = append(workRequests, &wr)
	}

	return workermgr.JobSubmission{
		JobID:        j.GetId(),
		WorkRequests: workRequests,
	}, false, nil
}

func (j *MockJob) Complete(client rst.Client, results map[string]worker.WorkResult, abort bool) error {
	return nil
}

func (j *MockJob) GetWorkRequests() string {
	var output string
	for i, segment := range j.TestSegments {
		output += fmt.Sprintf("{request_id: %d, segment: %s}", i, segment)
	}
	return output
}

// GobEncode encodes the MockJob into a byte slice for gob serialization. The
// method serializes the JobResponse using the protobuf marshaller and the
// Segments using gob. It prefixes the serialized JobResponse data with its
// length to handle variable-length slices during deserialization. It is
// primarily used when storing MockJobs in the database.
//
// IMPORTANT: If you are using this as a reference to implement custom
// encoding/decoding of a new type, don't forget the to update the package
// init() function to add `gob.Register(&MyType{})`.
func (j *MockJob) GobEncode() ([]byte, error) {

	// We use the proto.Marshal function because gob doesn't work properly with
	// the oneof type field in the JobRequest struct.
	jobResponseData, err := proto.Marshal(j.Job)
	if err != nil {
		return nil, err
	}

	// TODO: Determine if we also need to use protobuf to encode the WorkRequests.
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	err = enc.Encode(j.TestSegments)
	if err != nil {
		return nil, err
	}

	workRequestsData := buf.Bytes()

	// Prefix the serialized JobResponseData with the length as a uint16.
	// We'll use this when decoding.
	jobResponseLength := make([]byte, 2)
	binary.BigEndian.PutUint16(jobResponseLength, uint16((len(jobResponseData))))

	combinedData := append(jobResponseLength, jobResponseData...)
	combinedData = append(combinedData, workRequestsData...)
	return combinedData, nil
}

// GobDecode decodes a byte slice into the MockJob. The method first extracts
// the length prefix to determine the size of the JobResponse data. It then
// decodes JobResponse using the protobuf unmarshaller and Segments using gob.
// It is primarily used when retrieving MockJobs from the database.
//
// IMPORTANT: If you are using this as a reference to implement custom
// encoding/decoding of a new type, don't forget the to update the package
// init() function to add `gob.Register(&MyType{})`.
func (j *MockJob) GobDecode(data []byte) error {

	jobResponseLength := binary.BigEndian.Uint16(data[:2])
	jobResponseData := data[2 : 2+jobResponseLength]
	workRequestData := data[2+jobResponseLength:]

	if j.baseJob == nil {
		j.baseJob = &baseJob{
			Job: &beeremote.Job{},
		}
	}

	if j.TestSegments == nil {
		j.TestSegments = make([]string, 0)
	}

	err := proto.Unmarshal(jobResponseData, j.Job)
	if err != nil {
		return err
	}

	dec := gob.NewDecoder(bytes.NewReader(workRequestData))
	err = dec.Decode(&j.TestSegments)
	if err != nil {
		return err
	}
	return nil
}
