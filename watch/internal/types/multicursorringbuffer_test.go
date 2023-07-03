package types

import (
	"testing"

	pb "git.beegfs.io/beeflex/bee-watch/api/proto/v1"
	"github.com/stretchr/testify/assert"
)

func TestMCRBGC(t *testing.T) {

	// testEvents := []*pb.Event{
	// 	{SeqId: 1},
	// 	{SeqId: 2},
	// 	{SeqId: 3},
	// 	{SeqId: 4},
	// 	{SeqId: 5},
	// 	{SeqId: 6},
	// 	{SeqId: 7},
	// 	{SeqId: 8},
	// 	{SeqId: 9},
	// 	{SeqId: 10},
	// 	{SeqId: 11},
	// 	{SeqId: 12},
	// }

	rb := NewMultiCursorRingBuffer(10)
	rb.AddCursor(1)
	rb.AddCursor(2)

	// If we push 12 events we should drop the two oldest:
	for i := 1; i <= 12; i++ {
		rb.Push(&pb.Event{SeqId: uint64(i)})
	}

	e, _ := rb.GetEvent(1)
	assert.Equal(t, uint64(3), e.SeqId)
	e, _ = rb.GetEvent(1)
	assert.Equal(t, uint64(4), e.SeqId)

	// TODO: Test to verify logic around determining oldestAckPosition,
	// and moving start to point at the oldest event works as expected.

}

func TestMCRBGetEventAndResetSendCursor(t *testing.T) {
	rb := NewMultiCursorRingBuffer(10)
	rb.AddCursor(1)
	rb.AddCursor(2)

	// If we push 12 events we should drop the two oldest:
	for i := 1; i <= 12; i++ {
		rb.Push(&pb.Event{SeqId: uint64(i)})
	}

	// Run twice so we can test resetting the send cursor:
	for i := 0; i <= 1; i++ {
		for j := 3; j <= 12; j++ {
			e, err := rb.GetEvent(1)
			assert.NoError(t, err)
			assert.Equal(t, uint64(j), e.SeqId)
		}

		e, _ := rb.GetEvent(1)
		assert.Nil(t, e)

		// Repeated calls with no new events should return nil:
		e, _ = rb.GetEvent(1)
		assert.Nil(t, e)
		// The cursor should point to the end of the buffer:
		assert.Equal(t, rb.end, rb.cursors[1].sendCursor)

		// Try resetting the send cursor:
		err := rb.ResetSendCursor(1)
		assert.NoError(t, err)
		assert.Equal(t, rb.cursors[1].ackCursor, rb.cursors[1].sendCursor)
		// Then re-run once as if we were resending lost events.
	}

}

func TestBinarySearch(t *testing.T) {
	rb := NewMultiCursorRingBuffer(10)

	// 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21...
	for i := 1; i <= 21; i = i + 2 {
		rb.Push(&pb.Event{SeqId: uint64(i)})
	}

	result := rb.binarySearchIndexOfSeqID(1, 6, 6)
	assert.Equal(t, 2, result)
	result = rb.binarySearchIndexOfSeqID(1, 6, 13)
	assert.Equal(t, 6, result)
	result = rb.binarySearchIndexOfSeqID(1, 6, 1)
	assert.Equal(t, -1, result)
	result = rb.binarySearchIndexOfSeqID(1, 6, 15)
	assert.Equal(t, -2, result)
}
