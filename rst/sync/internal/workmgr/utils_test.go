package workmgr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/protobuf/go/flex"
)

func TestNewWorkResponseFromRequest(t *testing.T) {

	workRequest := workRequest{
		&flex.WorkRequest{
			JobId:     "123",
			RequestId: "456",
			Segment: &flex.WorkRequest_Segment{
				OffsetStart: 0,
				OffsetStop:  12,
				PartsStart:  0,
				PartsStop:   10,
			},
		},
	}

	workResult := newWorkFromRequest(&workRequest)

	assert.Equal(t, "123", workResult.JobId)
	assert.Equal(t, "456", workResult.RequestId)
	assert.Equal(t, flex.Work_SCHEDULED, workResult.Status.State)
	assert.Equal(t, "worker node accepted work request", workResult.Status.Message)
	assert.Len(t, workResult.Parts, 11)
}

func TestGenerateParts(t *testing.T) {

	type expectation struct {
		partNum     int32
		offsetStart int64
		offsetStop  int64
	}

	type test struct {
		name     string
		segment  *flex.WorkRequest_Segment
		expected []expectation // map parts to a slice of tuples (offsetStart, offsetStop)
	}

	tests := []test{
		{
			name:     "test when offsets evenly divide into parts",
			segment:  &flex.WorkRequest_Segment{OffsetStart: 0, OffsetStop: 99, PartsStart: 1, PartsStop: 10},
			expected: []expectation{{1, 0, 9}, {2, 10, 19}, {3, 20, 29}, {4, 30, 39}, {5, 40, 49}, {6, 50, 59}, {7, 60, 69}, {8, 70, 79}, {9, 80, 89}, {10, 90, 99}, {-1, -1, -1}},
			// expected: [2]int64{{0, 9}, {10, 19}, {20, 29}, {30, 39}, {40, 49}, {50, 59}, {60, 69}, {70, 79}, {80, 89}, {90, 99}, {-1, -1}},
		},
		{
			name:     "test when offsets don't evenly divide into parts",
			segment:  &flex.WorkRequest_Segment{OffsetStart: 0, OffsetStop: 100, PartsStart: 1, PartsStop: 10},
			expected: []expectation{{1, 0, 9}, {2, 10, 19}, {3, 20, 29}, {4, 30, 39}, {5, 40, 49}, {6, 50, 59}, {7, 60, 69}, {8, 70, 79}, {9, 80, 89}, {10, 90, 100}, {-1, -1, -1}, {-1, -1, -1}},
		},
		{
			name:     "test when offsets do not start at zero and don't divide evenly into parts",
			segment:  &flex.WorkRequest_Segment{OffsetStart: 10, OffsetStop: 110, PartsStart: 2, PartsStop: 7},
			expected: []expectation{{2, 10, 25}, {3, 26, 41}, {4, 42, 57}, {5, 58, 73}, {6, 74, 89}, {7, 90, 110}, {-1, -1, -1}},
		},
	}

	for _, test := range tests {
		partsGenerator := generatePartsFromSegment(test.segment)
		for _, expected := range test.expected {
			partNumber, offsetStart, offsetStop := partsGenerator()
			assert.Equal(t, expected.partNum, partNumber, "PartNumber does not match for segment %v of test: %s", test.segment, test.name)
			assert.Equal(t, expected.offsetStart, offsetStart, "OffsetStart does not match for segment %v of test: %s", test.segment, test.name)
			assert.Equal(t, expected.offsetStop, offsetStop, "OffsetStop does not match for segment %v of test: %s", test.segment, test.name)
		}
	}

}
