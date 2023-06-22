package subscriber

import (
	"context"
	"math/rand"
	"sync"
	"time"

	pb "git.beegfs.io/beeflex/bee-watch/api/proto/v1"
	"git.beegfs.io/beeflex/bee-watch/internal/types"
	"go.uber.org/zap"
)

const (
	// TODO: Make this configurable.
	// If we cannot connect to a subscriber we'll try to reconnect with an exponential backoff.
	// This is the maximum time in seconds between reconnect attempts to avoid increasing the backoff forever.
	maxReconnectBackoff = 60
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
	// Draining indicates an issue occurred with a connection and all events need to be drained from the queue to the interrupted events buffer.
	// While in this state Enqueue() should not add new events to the queue or interrupted events buffer.
	// This is to allow time for the current queue to be drained to the interrupted event buffer, ensuring events are buffered in order.
	STATE_DRAINING SubscriberState = "draining"
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

func (s *SubscriberStateStatus) GetStateStatus() (state SubscriberState, status SubscriberStatus) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.state, s.status
}

func (s *SubscriberStateStatus) SetStateStatus(state SubscriberState, status SubscriberStatus) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.state = state
	s.status = status
}

type BaseSubscriber struct {
	id   string
	name string
	SubscriberStateStatus
	interruptedEvents *types.EventRingBuffer // Used as a temp buffer if the connection with a subscriber is lost.
	queue             chan *pb.Event
	ctx               context.Context
	log               *zap.Logger
	Subscriber
}

// This is a "comparable" view of the BaseSubscriber struct used for testing.
// When BaseSubscriber is updated it should also be updated with any fields that are a comparable type.
// Notably the queue, ctx, and log fields are not comparable and thus omitted.
type ComparableBaseSubscriber struct {
	id   string
	name string
	SubscriberStateStatus
}

// Used for testing (notably TestNewSubscribersFromJson).
// This should be updated when ComparableBaseSubscriber is modified.
func newComparableBaseSubscriber(s *BaseSubscriber) ComparableBaseSubscriber {
	return ComparableBaseSubscriber{
		id:   s.id,
		name: s.name,
		SubscriberStateStatus: SubscriberStateStatus{
			state:  STATE_DISCONNECTED,
			status: STATUS_OKAY,
		},
	}
}

// newBaseSubscriber is intended to be used with newSubscriberFromConfig.
func newBaseSubscriber(id string, name string, queueSize int, log *zap.Logger) *BaseSubscriber {

	log = log.With(zap.String("subscriber", name))

	return &BaseSubscriber{
		id:   id,
		name: name,
		SubscriberStateStatus: SubscriberStateStatus{
			state:  STATE_DISCONNECTED,
			status: STATUS_OKAY,
		},
		interruptedEvents: types.NewEventRingBuffer(queueSize),
		queue:             make(chan *pb.Event, queueSize),
		ctx:               context.Background(),
		log:               log,
	}
}

func (s *BaseSubscriber) GetID() string {
	return s.id
}

func (s *BaseSubscriber) GetName() string {
	return s.name
}

// Manages the the connection with a particular Subscriber.
// It expects to be run as a Go routine.
// It handles connecting, disconnecting, and actually sending and receiving events to the subscriber.
// It also handles changing the state of subscribers (and this is the only place this should happen).
// It works as follows:
// * Attempt to connect, if an error occurs reconnect with an exponential backoff unless retry=false.
// * Once connected attempt to send() and receive().
//   - Any retry logic must be implemented in those methods. If an error occurs it will disconnect/reconnect.
//
// * If an error occurs or if a local or remote disconnect is requested, call disconnect().
//   - disconnect() is expected to be idempotent. In other words repeated attempts to disconnect an already
//     disconnected connection should not result in an error.
func (s *BaseSubscriber) Manage() {

	// If we can't connect to a subscriber we'll wait this long to try again.
	// This number increases exponentially with every retry for the same connection attempt.
	// It resets after a successful connection.
	var reconnectBackOff float64 = 1

connectLoop:
	for {

		// If we're in state disconnecting, something asked us to disconnect:
		if lastState, lastStatus := s.GetStateStatus(); lastState == STATE_DISCONNECTING {
		disconnectLoop:
			for {
				err := s.disconnect()
				if err != nil {
					s.SetStateStatus(STATE_DISCONNECTING, STATUS_DISCONNECT_ERROR)
					s.log.Error("encountered one or more errors disconnecting subscriber (ignoring)", zap.Error(err))

					select {
					case <-s.ctx.Done():
						return // We failed to disconnect but we were asked to shutdown so lets quit to avoid blocking.
					default:
						// Otherwise lets keep trying to disconnect.
						// The subscriber should handle if we're calling disconnect on an already disconnected subscriber.
						// So any error here means we shouldn't try to reconnect.
						continue disconnectLoop
					}
				}

				s.SetStateStatus(STATE_DISCONNECTED, STATUS_OKAY)
				if lastStatus == STATUS_LOCAL_DISCONNECT {
					// If the last status was a local disconnect, don't try to reconnect and stop managing the subscriber:
					return

				} else {
					// Otherwise we are now disconnected and ready to connect again:
					break disconnectLoop
				}

			}

		}

		// If we're disconnected we should try and connect. If we're already connecting continue trying.
		select {
		case <-s.ctx.Done():
			s.log.Info("not attempting to connect because the subscriber is shutting down")
			return
		case <-time.After(time.Second * time.Duration(reconnectBackOff)): // We use this instead of time.Ticker so we can change the duration.
			s.log.Info("connecting to subscriber")
			if state, _ := s.GetStateStatus(); state == STATE_DISCONNECTED || state == STATE_CONNECTING {
				s.SetStateStatus(STATE_CONNECTING, STATUS_OKAY)
				retry, err := s.connect()
				if err != nil {
					s.SetStateStatus(STATE_CONNECTING, STATUS_CONNECT_ERROR)
					if !retry {
						s.log.Error("unable to connect to subscriber (unable to retry)", zap.Error(err))
						return
					}

					// We'll retry to connect with an exponential back off. We'll add some jitter to avoid load spikes.
					reconnectBackOff *= 2 + rand.Float64()
					if reconnectBackOff > maxReconnectBackoff {
						reconnectBackOff = maxReconnectBackoff - rand.Float64()
					}

					s.log.Error("unable to connect to subscriber (retrying)", zap.Error(err), zap.Any("retry_in_seconds", reconnectBackOff))
					continue connectLoop
				}

				// If the subscriber disconnected for some reason, we may have been interrupted trying to send events.
				// Lets try to resend them before we enter the main connectedLoop:
				// First we need to set the status to draining to ensure Enqueue doesn't keep adding events:
				s.SetStateStatus(STATE_DRAINING, STATUS_OKAY)

				if !s.interruptedEvents.IsEmpty() {
					s.log.Info("sending interrupted events to subscriber")

					for !s.interruptedEvents.IsEmpty() {
						// We don't want to remove the event from the buffer until we're sure it was sent successfully:
						err := s.send(s.interruptedEvents.Peek())
						if err != nil {
							s.SetStateStatus(STATE_DISCONNECTING, STATUS_SEND_ERROR)
							s.log.Error("unable to send interrupted event", zap.Error(err), zap.Any("event_seq", s.interruptedEvents.Peek().SeqId))
							// We hit an error so all we can do is try to reconnect again.
							continue connectLoop

						} else {
							// Otherwise if the event was sent lets pop it from the event buffer.
							_ = s.interruptedEvents.Pop()
						}
					}
				}

				s.SetStateStatus(STATE_CONNECTED, STATUS_OKAY)
				reconnectBackOff = 1
				s.log.Info("connected to subscriber")
			} else {
				// This probably indicates a bug with our state transitions.
				// To avoid worse effects we won't try to connect unless we're already in a disconnected state.
				s.log.Warn("tried to connect a subscriber that was not already disconnected (this shouldn't happen and indicates a bug as the subscriber will never connect now)", zap.Any("current_state", s.state))
			}
		}

		// Once connected start sending events and listening for responses:
		if state, _ := s.GetStateStatus(); state == STATE_CONNECTED {

			// Start listening for responses from the subscriber:
			recvStream := s.receive()

			s.log.Info("beginning to stream events to subscriber")

		connectedLoop:
			for {
				select {
				case <-s.ctx.Done():
					// The context should only be cancelled if something local requested a disconnect.
					s.SetStateStatus(STATE_DRAINING, STATUS_LOCAL_DISCONNECT)
					s.log.Info("local disconnect requested")
					break connectedLoop
				case event := <-s.queue:
					if err := s.send(event); err != nil {
						s.SetStateStatus(STATE_DRAINING, STATUS_SEND_ERROR)
						s.log.Error("unable to send event", zap.Error(err))
						// We don't know if the event was sent successfully or not so lets mark it interrupted.
						// Subscribers are expected to handle duplicate events so we'll err on the side of caution.
						s.interruptedEvents.Push(event)
						break connectedLoop
					}
				case response, ok := <-recvStream:
					if !ok {
						// Note an error or a legitimate remote disconnect could result in a REMOTE_DISCONNECT.
						s.SetStateStatus(STATE_DRAINING, STATUS_REMOTE_DISCONNECT)
						s.log.Info("remote disconnect received")
						break connectedLoop
					}
					s.log.Debug("received response from subscriber", zap.Any("response", response))
					// TODO: https://linear.app/thinkparq/issue/BF-29/acknowledge-events-sent-to-all-subscribers-back-to-the-metadata-server
					// Also consider if we need to better handle what we do with recvStream when we break out of the connectedLoop.
					// Probably nothing because there is no expectation subscribers ack every event back to BeeWatch, or BW ack every event to meta.
					// If we're able to reconnect then we'll start reading the recvStream again.
					// If we're shutting down it doesn't matter since BeeWatch doesn't store any state on-disk.
					// Whatever ack's events back to meta will need to handle if a subscriber is removed, knowing to disregard events it hasn't ack'd.
				}
			}

			if currentState, currentStatus := s.GetStateStatus(); currentState == STATE_DRAINING {
			drainLoop:
				for {
					select {
					case event := <-s.queue:
						s.interruptedEvents.Push(event)
					default:
						s.SetStateStatus(STATE_DISCONNECTING, currentStatus)
						break drainLoop
					}
				}
			}
		}
	}
}

// Enqueue is called to add an event to the send queue for a particular subscriber.
func (s *BaseSubscriber) Enqueue(event *pb.Event) {

	// This is thread safe because getting the status will block if it is currently being updated.
	state, _ := s.GetStateStatus()

	if state == STATE_CONNECTED {
		s.queue <- event
		return
	} else if state != STATE_DRAINING {
		s.interruptedEvents.Push(event)
		return
	}

	// Otherwise block until the subscriber has finished draining any current events in the queue
	// so we can add the event (and subsequent events) in the right place in the interrupted events queue.
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			s.log.Info("unable to enqueue event because the subscriber is shutting down")
		case <-ticker.C:
			if state, _ := s.GetStateStatus(); state != STATE_DRAINING {
				s.interruptedEvents.Push(event)
				return
			}
		}
	}
}

// Stop is called to cancel the context associated with a particular subscriber.
// This will cause the Go routine handling the subscriber to attempt to cleanly disconnect.
func (bs *BaseSubscriber) Stop() {
	bs.ctx.Done()
}
