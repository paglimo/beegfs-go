package subscriber

import (
	"sync"

	pb "git.beegfs.io/beeflex/bee-watch/api/proto/v1"
)

// Subscriber defines the methods all subscribers (such as gRPC) are expected to implement.
type Subscriber interface {
	Connect() (retry bool, err error)
	Send(*pb.Event) (err error)
	Receive() chan *pb.Response
	// disconnect should be idempotent and not return an error even if called against an already disconnected subscriber.
	Disconnect() (err error)
}

// state should not be used directly.
// It should be set/inspected using the exported thread safe State methods.
type state string

const (
	DISCONNECTED state = "disconnected"
	CONNECTING   state = "connecting"
	CONNECTED    state = "connected"
	// STATE_FROZEN indicates the subscriber is undergoing a connection state change (disconnected->connected or connected->disconnected).
	// While in this state handlers should wait and not add new events to the queue or offline events buffer.
	// This is to allow time for the current queue to be drained to the offline event buffer, or for the buffer to be drained to the queue.
	// This is important to ensure events are always eventually sent in order.
	FROZEN state = "frozen"
	// Disconnecting signals a subscriber needs to disconnect for some reason.
	DISCONNECTING state = "disconnecting"
)

// The GetState() method provided by State are used to communicate the state of a subscriber to the outside world.
// External readers SHOULD NOT inspect the state directly, but instead use the thread safe GetState() method.
// The Handler.Handle() method is the ONLY place where SubscriberStates should change.
// All internal handler methods and subscriber methods such as connect(), send(), disconnect(), etc. should not affect the state.
type State struct {
	// State should only be accessed through the GetStateStatus() and SetStateStatus() methods to ensure thread safety.
	state state
	// Status should only be accessed through the GetStateStatus() and SetStateStatus() methods to ensure thread safety.
	mutex sync.RWMutex
}

// GetState() is a thread safe mechanism to get the current state and status of a subscriber.
func (s *State) GetState() state {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.state
}

// SetState() is a thread safe mechanism to set the current state and status of a subscriber.
func (s *State) SetState(newState state) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.state = newState
}

// BaseSubscriber contains common fields used by all subscribers.
type BaseSubscriber struct {
	Id   string
	Name string
	// queueSize is the number of events that will be buffered while this subscriber is connected.
	// Ideally we'll send events to subscribers as fast as they are received from the metadata service.
	// The queue gives us extra buffer in case of sudden bursts of events.
	// If the queueSize is exceeded then some mechanism upstream of the subscriber is suspected to buffer the events.
	// This may need to be set to a higher value for slower subscribers.
	QueueSize int
	State
	Subscriber
}

// This is a "comparable" view of the BaseSubscriber struct used for testing.
// When BaseSubscriber is updated it should also be updated with any fields that are a comparable type.
// Notably the queue, ctx, and log fields are not comparable and thus omitted.
type ComparableBaseSubscriber struct {
	Id        string
	Name      string
	QueueSize int
	State
}

// Used for testing (notably TestNewSubscribersFromJson).
// This should be updated when ComparableBaseSubscriber is modified.
func newComparableBaseSubscriber(s *BaseSubscriber) ComparableBaseSubscriber {
	return ComparableBaseSubscriber{
		Id:        s.Id,
		Name:      s.Name,
		QueueSize: s.QueueSize,
		State: State{
			state: s.state,
		},
	}
}

// newBaseSubscriber is intended to be used with newSubscriberFromConfig.
// It DOES NOT handle setting the Subscriber interface to a valid subscriber.
// Typically you would not use this directly and instead use newSubscriberFromConfig().
func newBaseSubscriber(id string, name string, queueSize int) *BaseSubscriber {

	return &BaseSubscriber{
		Id:        id,
		Name:      name,
		QueueSize: queueSize,
		State: State{
			state: DISCONNECTED,
		},
	}
}
