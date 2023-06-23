package subscriber

import (
	"sync"

	pb "git.beegfs.io/beeflex/bee-watch/api/proto/v1"
)

// Subscriber defines the methods all subscribers (such as gRPC) are expected to implement.
type Subscriber interface {
	connect() (retry bool, err error)
	send(*pb.Event) (err error)
	receive() chan *pb.Response
	// disconnect should be idempotent and not return an error even if called against an already disconnected subscriber.
	disconnect() (err error)
}

// SubscriberState should not be used directly.
// It should be set/inspected using the thread safe SubscriberSafeState methods.
type SubscriberState string

const (
	STATE_DISCONNECTED SubscriberState = "disconnected"
	STATE_CONNECTING   SubscriberState = "connecting"
	STATE_CONNECTED    SubscriberState = "connected"
	// STATE_DRAINING_IE indicates we've just connected (or reconnected) to a subscriber, and any events in the interrupted events
	// buffer are being sent to the subscriber. While in this state Enqueue() should not add new events to the queue or interrupted events buffer.
	STATE_DRAINING_IE SubscriberState = "draining_interrupted_events"
	// STATE_DRAINING_Q indicates an issue occurred with a connection and all events need to be drained from the queue to the interrupted events buffer.
	// While in this state Enqueue() should not add new events to the queue or interrupted events buffer.
	// This is to allow time for the current queue to be drained to the interrupted event buffer, ensuring events are buffered in order.
	STATE_DRAINING_Q SubscriberState = "draining_queue"
	// Disconnecting signals a subscriber needs to disconnect for some reason.
	STATE_DISCONNECTING SubscriberState = "disconnecting"
)

// The GetState() method provided by SubscriberSafeState are used to communicate the state of a subscriber to the outside world.
// External readers SHOULD NOT inspect the state directly, but instead use the thread safe GetState() method.
// The Handler.Handle() method is the ONLY place where SubscriberStates should change.
// All internal handler methods and subscriber methods such as connect(), send(), disconnect(), etc. should not affect the state.
type SubscriberSafeState struct {
	// State should only be accessed through the GetStateStatus() and SetStateStatus() methods to ensure thread safety.
	state SubscriberState
	// Status should only be accessed through the GetStateStatus() and SetStateStatus() methods to ensure thread safety.
	mutex sync.RWMutex
}

// GetState() is a thread safe mechanism to get the current state and status of a subscriber.
func (s *SubscriberSafeState) GetState() (state SubscriberState) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.state
}

// setState() is a thread safe mechanism to set the current state and status of a subscriber.
func (s *SubscriberSafeState) setState(state SubscriberState) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.state = state
}

// BaseSubscriber contains common fields used by all subscribers.
type BaseSubscriber struct {
	id        string
	name      string
	queueSize int
	SubscriberSafeState
	Subscriber
}

// This is a "comparable" view of the BaseSubscriber struct used for testing.
// When BaseSubscriber is updated it should also be updated with any fields that are a comparable type.
// Notably the queue, ctx, and log fields are not comparable and thus omitted.
type ComparableBaseSubscriber struct {
	id        string
	name      string
	queueSize int
	SubscriberSafeState
}

// Used for testing (notably TestNewSubscribersFromJson).
// This should be updated when ComparableBaseSubscriber is modified.
func newComparableBaseSubscriber(s *BaseSubscriber) ComparableBaseSubscriber {
	return ComparableBaseSubscriber{
		id:        s.id,
		name:      s.name,
		queueSize: s.queueSize,
		SubscriberSafeState: SubscriberSafeState{
			state: STATE_DISCONNECTED,
		},
	}
}

// newBaseSubscriber is intended to be used with newSubscriberFromConfig.
// It DOES NOT handle setting the Subscriber interface to a valid subscriber.
// Typically you would not use this directly and instead use newSubscriberFromConfig().
func newBaseSubscriber(id string, name string, queueSize int) *BaseSubscriber {

	return &BaseSubscriber{
		id:        id,
		name:      name,
		queueSize: queueSize,
		SubscriberSafeState: SubscriberSafeState{
			state: STATE_DISCONNECTED,
		},
	}
}
