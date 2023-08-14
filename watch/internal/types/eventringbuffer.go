package types

import (
	"sync"

	pb "github.com/thinkparq/protobuf/beewatch/go"
)

type EventRingBuffer struct {
	buffer []*pb.Event
	start  int
	end    int
	mutex  sync.RWMutex
}

// Returns an ring buffer used to store pointers to events.
// The usable size of the buffer (number of pointers that can be stored in total) is based on size.
// The actual capacity as reflected in memory use is size+1 due to how the buffer distinguishes between an empty and full buffer.
func NewEventRingBuffer(size int) *EventRingBuffer {
	return &EventRingBuffer{
		buffer: make([]*pb.Event, size+1),
		start:  0,
		end:    0,
	}
}

// Push adds an event to the ring buffer.
// If the capacity is exceeded the oldest event is overwritten.
func (b *EventRingBuffer) Push(event *pb.Event) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.buffer[b.end] = event
	b.end = (b.end + 1) % len(b.buffer)

	// If we wrapped around to the start of the buffer we need to update the start pointer to reflect the new oldest event:
	if b.end == b.start {
		b.start = (b.start + 1) % len(b.buffer)
	}
}

// Pop returns a pointer to the next event from the ring buffer and moves to the next event.
// The underlying buffer will also be set to nil ensuring the GC will free the memory for the event when nothing else is using it.
func (b *EventRingBuffer) Pop() (event *pb.Event) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// There are no more events in the buffer:
	if b.start == b.end {
		return nil
	}

	// Save the current event:
	event = b.buffer[b.start]

	// Clear the buffer (important so the garbage collector will free up memory):
	b.buffer[b.start] = nil

	// Advance the start pointer to the next event wrapping around to the start if needed:
	b.start = (b.start + 1) % len(b.buffer)

	return event
}

// RemoveUntil accepts a sequence ID.
// It then removes all events in the queue up to and including that ID.
// It is intended to be used when the events are not needed.
func (b *EventRingBuffer) RemoveUntil(id uint64) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	var event *pb.Event

	for {
		// There are no more events in the buffer:
		if b.start == b.end {
			return
		}

		// Save the current event:
		event = b.buffer[b.start]

		if event.SeqId <= id {
			// Clear the buffer (important so the garbage collector will free up memory):
			b.buffer[b.start] = nil

			// Advance the start pointer to the next event wrapping around to the start if needed:
			b.start = (b.start + 1) % len(b.buffer)
		} else {
			break
		}
	}
}

// Peek returns a pointer to the next event but does not advance to the next event.
func (b *EventRingBuffer) Peek() (event *pb.Event) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	if b.IsEmpty() {
		return nil
	}
	return b.buffer[b.start]
}

// IsEmpty returns if there are no more events in the buffer.
func (b *EventRingBuffer) IsEmpty() bool {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return b.start == b.end
}
