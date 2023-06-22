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

// SubscriberState state is used to communicate the state of a subscriber to the outside world.
// IMPORTANT: BaseSubscriber.Manage() method is the ONLY place where SubscriberStates should change.
// All actual implementations such as connect(), send(), disconnect(), etc. should not affect the state.
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

// SubscriberStatus state is used internally to determine what state a subscriber should be placed in next.
// In other words it is the result of the last state change.
// IMPORTANT: BaseSubscriber.Manage() method is the ONLY place where SubscriberStates should change.
// All actual implementations such as connect(), send(), disconnect(), etc. should not affect the state.
type SubscriberStatus string

const (
	STATUS_OKAY              SubscriberStatus = "ok"
	STATUS_NONE              SubscriberStatus = "none"
	STATUS_LOCAL_DISCONNECT  SubscriberStatus = "local_disconnect"
	STATUS_REMOTE_DISCONNECT SubscriberStatus = "remote_disconnect"
	STATUS_SEND_ERROR        SubscriberStatus = "send_error"
	STATUS_CONNECT_ERROR     SubscriberStatus = "connect_error"
	STATUS_DISCONNECT_ERROR  SubscriberStatus = "disconnect_error"
)

type SubscriberStateStatus struct {
	// State should only be accessed through the GetStateStatus() and SetStateStatus() methods to ensure thread safety.
	state SubscriberState
	// Status should only be accessed through the GetStateStatus() and SetStateStatus() methods to ensure thread safety.
	status SubscriberStatus
	mutex  sync.RWMutex
}

// GetStateStatus() is a thread safe mechanism to get the current state and status of a subscriber.
func (s *SubscriberStateStatus) GetStateStatus() (state SubscriberState, status SubscriberStatus) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.state, s.status
}

// SetStateStatus() is a thread safe mechanism to set the current state and status of a subscriber.
func (s *SubscriberStateStatus) SetStateStatus(state SubscriberState, status SubscriberStatus) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.state = state
	s.status = status
}

// BaseSubscriber contains common fields used by all subscribers.
type BaseSubscriber struct {
	id        string
	name      string
	queueSize int
	SubscriberStateStatus
	Subscriber
}

// This is a "comparable" view of the BaseSubscriber struct used for testing.
// When BaseSubscriber is updated it should also be updated with any fields that are a comparable type.
// Notably the queue, ctx, and log fields are not comparable and thus omitted.
type ComparableBaseSubscriber struct {
	id        string
	name      string
	queueSize int
	SubscriberStateStatus
}

// Used for testing (notably TestNewSubscribersFromJson).
// This should be updated when ComparableBaseSubscriber is modified.
func newComparableBaseSubscriber(s *BaseSubscriber) ComparableBaseSubscriber {
	return ComparableBaseSubscriber{
		id:        s.id,
		name:      s.name,
		queueSize: s.queueSize,
		SubscriberStateStatus: SubscriberStateStatus{
			state:  STATE_DISCONNECTED,
			status: STATUS_OKAY,
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
		SubscriberStateStatus: SubscriberStateStatus{
			state:  STATE_DISCONNECTED,
			status: STATUS_OKAY,
		},
	}
}
