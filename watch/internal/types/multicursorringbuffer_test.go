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

	err := rb.AckEvent(1, 3)
	assert.NoError(t, err)

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

func TestAckEvent(t *testing.T) {
	rb := NewMultiCursorRingBuffer(10)
	rb.AddCursor(1)

	// If we push 12 events we should drop the two oldest:
	for i := 1; i <= 12; i++ {
		rb.Push(&pb.Event{SeqId: uint64(i)})
	}

	// Ack events that were already dropped and the cursor shouldn't move:
	currAckLocation := rb.cursors[1].ackCursor
	rb.AckEvent(1, 1)
	rb.AckEvent(2, 1)
	assert.Equal(t, currAckLocation, rb.cursors[1].ackCursor)

	// Now ack an actual event and the cursor should move:
	rb.AckEvent(1, 3)
	assert.Equal(t, 3, rb.cursors[1].ackCursor)

	// TODO (current location): Continue building out tests and verifying AckEvent().

}

func TestSearchIndexOfSeqID(t *testing.T) {
	rb := NewMultiCursorRingBuffer(10)

	// 23, 25, nil, 7, 9, 11, 13, 15, 17, 19, 21...
	for i := 1; i <= 25; i = i + 2 {
		rb.Push(&pb.Event{SeqId: uint64(i)})
	}

	// Search for a seqID that was dropped:
	idx, found := rb.searchIndexOfSeqID(3, 8, 8)
	assert.Equal(t, 4, idx)
	assert.False(t, found)

	// Search for a seqID that exists:
	idx, found = rb.searchIndexOfSeqID(3, 8, 7)
	assert.Equal(t, 3, idx)
	assert.True(t, found)

	// Search for a seqID that was dropped where the buffer wraps around:
	idx, found = rb.searchIndexOfSeqID(3, 1, 22)
	assert.Equal(t, 0, idx)
	assert.False(t, found)

	// Search for a seqID that exists after the buffer wraps around:
	idx, found = rb.searchIndexOfSeqID(3, 1, 23)
	assert.Equal(t, 0, idx)
	assert.True(t, found)

	// Search for a seqID that exists at the end of the buffer:
	idx, found = rb.searchIndexOfSeqID(3, 1, 25)
	assert.Equal(t, 1, idx)
	assert.True(t, found)

	// Search for a seqID that was already ack'd (lower than startIndex):
	idx, found = rb.searchIndexOfSeqID(4, 6, 7)
	assert.Equal(t, 4, idx)
	assert.False(t, found)

	// Search for a seqID that wasn't sent yet but has wrapped around in the buffer:
	idx, found = rb.searchIndexOfSeqID(4, 8, 25)
	assert.Equal(t, -1, idx)
	assert.False(t, found)

	// Search for a seqID that wasn't sent yet and doesn't exist:
	idx, found = rb.searchIndexOfSeqID(4, 8, 27)
	assert.Equal(t, -1, idx)
	assert.False(t, found)

	rb = NewMultiCursorRingBuffer(10)

	// 23, 25, 27, 29, nil, 11, 13, 15, 17, 19, 21...
	for i := 1; i <= 29; i = i + 2 {
		rb.Push(&pb.Event{SeqId: uint64(i)})
	}

	// Search for a sequence ID in a deeper buffer wraparound:
	idx, found = rb.searchIndexOfSeqID(5, 3, 27)
	assert.Equal(t, 2, idx)
}
