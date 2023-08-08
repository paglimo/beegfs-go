package types

// TODO: This is not actually in use anywhere.
// It was used for some early performance testing to evaluate performance of a ring buffer vs. a queue based data structure.
// Before using ensure to add unit tests and verify functionality works as expected.

import (
	"sync"

	pb "github.com/thinkparq/bee-protos/beewatch"
)

type EventQueue struct {
	events []*pb.Event
	mu     sync.Mutex
}

func NewEventQueue(length int) *EventQueue {

	return &EventQueue{
		events: make([]*pb.Event, length),
	}
}

// Push adds a new event to the back of the queue.
func (q *EventQueue) Push(e *pb.Event) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.events = append(q.events, e)
}

// Pop will return the next event in the queue.
// Once the queue is empty it will return nil.
func (q *EventQueue) Pop() *pb.Event {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.events) == 0 {
		return nil
	}
	event := q.events[0]
	q.events = q.events[1:]
	return event
}

// RemoveUntil accepts a sequence ID.
// It then removes all events in the queue up to and including that ID.
// It is intended to be used when the events are not needed.
func (q *EventQueue) RemoveUntil(id uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.events) > 0 && q.events[0].SeqId <= id {
		q.events = q.events[1:]
	}
}
