package types

import (
	"testing"

	pb "git.beegfs.io/beeflex/bee-watch/api/proto/v1"
	"github.com/stretchr/testify/assert"
)

func TestNewEventBuffer(t *testing.T) {

	b := NewEventRingBuffer(5)
	assert.Len(t, b.buffer, 5+1)
}

func TestEventBufferPushPop(t *testing.T) {

	// Ring buffer capacity small enough to ensure the buffer will overflow several times:
	capacity := 12

	testEvents := []*pb.Event{}

	// Create enough test events to overflow the buffer several times:
	var i uint64 = 0
	for ; i <= 80; i++ {
		testEvents = append(testEvents, &pb.Event{SeqId: i})
	}

	b := NewEventRingBuffer(capacity)
	for _, event := range testEvents {
		b.Push(event)
	}

	assert.False(t, b.IsEmpty())
	event := b.Peek()
	assert.Equal(t, testEvents[uint64(len(testEvents)-capacity)].SeqId, event.SeqId)

	// Only as many events as the capacity of the ring buffer should be left:
	i = uint64(len(testEvents) - capacity)
	for ; i < uint64(len(testEvents)); i++ {
		event := b.Pop()
		assert.Equal(t, i, event.SeqId)
	}

	// The ring buffer should now be empty and Pop() should return nil and an error:
	event = b.Pop()
	assert.Nil(t, event)
	assert.True(t, b.IsEmpty())
}
