package types

import (
	"fmt"
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

	// Both cursors ack and next event should point at the next oldest:
	assert.Equal(t, 2, rb.cursors[1].sendCursor)
	assert.Equal(t, 2, rb.cursors[1].ackCursor)
	assert.Equal(t, 2, rb.cursors[2].sendCursor)
	assert.Equal(t, 2, rb.cursors[2].ackCursor)

	// Get 3 events for both subscribers,
	// but only acknowledge the events for subscriber 1:
	for i := 3; i <= 5; i++ {
		rb.GetEvent(1)
		rb.GetEvent(2)
		rb.AckEvent(1, uint64(i))
	}

	// Then push another event.
	// Because subscriber 2 hasn't ack'd anything, we should only free one buffer.
	rb.Push(&pb.Event{SeqId: uint64(13)})
	assert.Equal(t, 3, rb.cursors[2].sendCursor)
	assert.Equal(t, 3, rb.cursors[5].ackCursor)

	// TODO: After fixing collectGarbage() continue adding tests.
	// In particular around verifying logic to determine oldestAckCursor,
	// and that we always (a) clear the oldest event in the buffer even if we need to move an ackCursor,
	// and (b) free up as much space as possible but never go past oldestAckCursor.
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

	// Ack an event that wasn't sent yet and the cursor shouldn't move and we should get an error:
	err := rb.AckEvent(1, 3)
	assert.Equal(t, 2, rb.cursors[1].ackCursor)
	assert.Error(t, err)

	// Get an event:
	event, err := rb.GetEvent(1)
	assert.Equal(t, uint64(3), event.SeqId)
	assert.NoError(t, err)

	// Ack events that were already dropped and the cursor shouldn't move:
	currAckLocation := rb.cursors[1].ackCursor
	err = rb.AckEvent(1, 1)
	assert.NoError(t, err)
	rb.AckEvent(1, 2)
	assert.NoError(t, err)
	assert.Equal(t, currAckLocation, rb.cursors[1].ackCursor)

	// Ack an event that wasn't dropped and the ackCursor should move to the next spot:
	err = rb.AckEvent(1, 3)
	currAckLocation++
	assert.NoError(t, err)
	assert.Equal(t, currAckLocation, rb.cursors[1].ackCursor)

	// Get an event:
	event, err = rb.GetEvent(1)
	assert.Equal(t, uint64(4), event.SeqId)
	assert.NoError(t, err)

	// Then acknowledge it. The ackCursor should move to the next spot:
	err = rb.AckEvent(1, 4)
	currAckLocation++
	assert.NoError(t, err)
	assert.Equal(t, currAckLocation, rb.cursors[1].ackCursor)

	// Get the next four events:
	for i := 5; i <= 8; i++ {
		event, err = rb.GetEvent(1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(i), event.SeqId)
	}

	// Ack in between the ackCursor and sendCursor:
	err = rb.AckEvent(1, 6)
	currAckLocation += 2
	assert.NoError(t, err)
	assert.Equal(t, currAckLocation, rb.cursors[1].ackCursor)

	// Get the next three events causing the sendCursor to wrap around the buffer:
	for i := 9; i <= 11; i++ {
		event, err = rb.GetEvent(1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(i), event.SeqId)
	}

	// Ack the event in the last position of the buffer.
	// This should cause the ackCursor to wrap around to the front of the buffer.
	err = rb.AckEvent(1, 11)
	assert.NoError(t, err)
	assert.Equal(t, 0, rb.cursors[1].ackCursor)

	// Get the last event in the buffer:
	event, err = rb.GetEvent(1)
	assert.NoError(t, err)
	assert.Equal(t, uint64(12), event.SeqId)

	// Push more events but skip sequence IDs:
	rb.Push(&pb.Event{SeqId: uint64(14)})

	// Get that event so now there are no more events to send (sendCursor points at a nil buffer location):
	event, err = rb.GetEvent(1)
	assert.NoError(t, err)
	assert.Equal(t, uint64(14), event.SeqId)

	// Acknowledge that event.
	err = rb.AckEvent(1, 14)
	assert.NoError(t, err)
	assert.Equal(t, 2, rb.cursors[1].ackCursor)

	// Push another event skipping sequence IDs:
	rb.Push(&pb.Event{SeqId: uint64(16)})

	// Get that event so now there are no more events to send (sendCursor points at a nil buffer location):
	event, err = rb.GetEvent(1)
	assert.NoError(t, err)
	assert.Equal(t, uint64(16), event.SeqId)

	// Acknowledge the event that was skipped:
	err = rb.AckEvent(1, 15)
	assert.NoError(t, err)
	assert.Equal(t, 2, rb.cursors[1].ackCursor)

	fmt.Print("test")

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
