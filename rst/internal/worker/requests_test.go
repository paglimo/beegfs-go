package worker

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/flex"
)

// We need to serialize and deserialize WorkResults so they can be stored in the
// DB. This test ensures Gob is able to encode and decode WorkResults that embed
// the protobuf defined WorkResponse. It also fails if there are any changes to
// the definition of the WorkResponse struct forcing the test to be updated.
func TestEncodeDecodeWorkResults(t *testing.T) {

	workResult := &WorkResult{
		AssignedNode: "test",
		AssignedPool: BeeSync,
		WorkResponse: &flex.WorkResponse{
			Path:      "/foo",
			JobId:     "1",
			RequestId: "2",
			Status: &flex.RequestStatus{
				State:   flex.RequestStatus_COMPLETED,
				Message: "test message",
			},
			Parts: []*flex.WorkResponse_Part{
				{
					PartNumber:     10,
					EntityTag:      "etag10",
					ChecksumSha256: "checksum10",
				},
				{
					PartNumber:     11,
					EntityTag:      "etag11",
					ChecksumSha256: "checksum11",
				},
			},
		},
	}

	var encodedWorkResult bytes.Buffer
	enc := gob.NewEncoder(&encodedWorkResult)
	require.NoError(t, enc.Encode(workResult))

	var decodedWorkResult = &WorkResult{}
	dec := gob.NewDecoder(&encodedWorkResult)
	require.NoError(t, dec.Decode(decodedWorkResult))
	assert.Equal(t, workResult, decodedWorkResult)

	expectedUserFields := map[string]reflect.Type{
		"Path":      reflect.TypeOf(""),
		"JobId":     reflect.TypeOf(""),
		"RequestId": reflect.TypeOf(""),
		"Status":    reflect.TypeOf(&flex.RequestStatus{}),
		"Parts":     reflect.TypeOf([]*flex.WorkResponse_Part{}),
	}

	workResponseType := reflect.TypeOf(flex.WorkResponse{})
	for expectedField, expectedType := range expectedUserFields {
		field, found := workResponseType.FieldByName(expectedField)
		assert.True(t, found, "a field was removed from the WorkResponse message (update test and verify encoding via Gob is not broken)")
		assert.Equal(t, field.Type, expectedType, "the type of a field was changed in the WorkResponse message (update test and verify encoding via Gob is not broken)")
	}

	// In addition to user fields, protobuf defined structs also have some
	// internal fields. We should check the expected fields still exist
	// otherwise we may miss changes to the struct when we verify the field
	// count hasn't changed:
	_, state := workResponseType.FieldByName("state")
	_, sizeCache := workResponseType.FieldByName("sizeCache")
	_, unknownFields := workResponseType.FieldByName("unknownFields")
	assert.True(t, state && sizeCache && unknownFields, "an expected internal protobuf field was not found (update test and verify encoding via Gob is not broken)")

	// Verify number of user defined + internal fields haven't changed:
	assert.Equal(t, 8, reflect.TypeOf(flex.WorkResponse{}).NumField(), "the number of fields in the WorkResponse message has changed (update test and verify encoding via Gob is not broken)")
}
