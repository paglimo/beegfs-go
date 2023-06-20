package subscriber

import (
	"context"
	"math/rand"
	"time"

	pb "git.beegfs.io/beeflex/bee-watch/api/proto/v1"
	"git.beegfs.io/beeflex/bee-watch/internal/types"
	"go.uber.org/zap"
)

// Subscriber defines the methods all subscribers (such as gRPC) are expected to implement.
type Subscriber interface {
	connect() (retry bool, err error)
	send(*pb.Event) (err error)
	receive() chan *pb.Response
	disconnect() (err error)
}

// IMPORTANT:
// The BaseSubscriber.Manage() method is the ONLY place where SubscriberStates should change.
// All actual implementations such as connect(), send(), disconnect(), etc. should not affect the state.
// The manager sets the set in response to the return values from these functions.
type SubscriberState string

const (
	DISCONNECTED      SubscriberState = "disconnected"
	CONNECTING        SubscriberState = "connecting"
	CONNECTED         SubscriberState = "connected"
	LOCAL_DISCONNECT  SubscriberState = "local_disconnect"
	REMOTE_DISCONNECT SubscriberState = "remote_disconnect"
	SEND_ERR          SubscriberState = "send_error"
	CONNECT_ERR       SubscriberState = "connect_err"
)

type BaseSubscriber struct {
	id                string
	name              string
	state             SubscriberState
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
	id    string
	name  string
	state SubscriberState
}

// Used for testing (notably TestNewSubscribersFromJson).
// This should be updated when ComparableBaseSubscriber is modified.
func newComparableBaseSubscriber(s BaseSubscriber) ComparableBaseSubscriber {
	return ComparableBaseSubscriber{
		id:    s.id,
		name:  s.name,
		state: s.state,
	}
}

// newBaseSubscriber is intended to be used with newSubscriberFromConfig.
func newBaseSubscriber(id string, name string, queueSize int, log *zap.Logger) *BaseSubscriber {

	log = log.With(zap.String("subscriber", name))

	return &BaseSubscriber{
		id:                id,
		name:              name,
		state:             DISCONNECTED,
		interruptedEvents: types.NewEventRingBuffer(2048), // TODO: This should be configurable.
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

	var reconnectBackOff float64 = 1

connectLoop:
	for {

		// First we'll handle if the result of the last connectLoop was we need to disconnect:
		if s.state == SEND_ERR || s.state == LOCAL_DISCONNECT || s.state == REMOTE_DISCONNECT {
		disconnectLoop:
			for {
				err := s.disconnect()
				if err != nil {
					s.log.Error("encountered one or more errors disconnecting subscriber (ignoring)", zap.Error(err))

					select {
					case <-s.ctx.Done():
						return // We failed to disconnect but we were asked to shutdown so lets quit to avoid blocking.
					default:
						continue disconnectLoop
					}
				}

				if s.state == LOCAL_DISCONNECT {
					s.state = DISCONNECTED
					return
				} else {
					s.state = DISCONNECTED
					break disconnectLoop
				}

			}

		}

		// Next check if we should connect/reconnect:
		select {
		case <-s.ctx.Done():
			s.log.Info("not attempting to connect because the subscriber is shutting down")
			return
		case <-time.After(time.Second * time.Duration(reconnectBackOff)):
			s.log.Info("connecting to subscriber")
			if s.state == DISCONNECTED {
				retry, err := s.connect()
				if err != nil {

					if !retry {
						s.log.Error("unable to connect to subscriber (unable to retry)", zap.Error(err))
						return
					}

					// TODO: Probably we don't want to keep increasing the timer forever.
					// We'll retry to connect with an exponential back off. We'll add some jitter to avoid load spikes.
					reconnectBackOff = float64(reconnectBackOff) * (2 + rand.Float64())
					s.log.Error("unable to connect to subscriber (retrying)", zap.Error(err), zap.Any("retry_backoff_seconds", reconnectBackOff))
					continue connectLoop
				}
				s.state = CONNECTED
				s.log.Info("connected to subscriber")
			} else {
				// This probably indicates a bug with our state transitions.
				// To avoid worse effects we won't try to connect unless we're already in a disconnected state.
				s.log.Warn("tried to connect a subscriber that was not already disconnected (this shouldn't happen and indicates a bug as the subscriber will never connect now)", zap.Any("current_state", s.state))
			}
		}

		// Once connected start sending events and listening for responses:
		if s.state == CONNECTED {

			// Start listening for responses from the subscriber:
			recvStream := s.receive()

			// If the subscriber disconnected for some reason, we may have been interrupted trying to send events.
			// Lets try to resend them before we enter the main connectedLoop:
			if !s.interruptedEvents.IsEmpty() {
				s.log.Info("sending interrupted events to subscriber")

				for !s.interruptedEvents.IsEmpty() {
					// We don't want to remove the event from the buffer until we're sure it was sent successfully:
					err := s.send(s.interruptedEvents.Peek())
					if err != nil {
						s.state = SEND_ERR
						s.log.Error("unable to send interrupted event", zap.Error(err), zap.Any("event_seq", s.interruptedEvents.Peek().SeqId))
						// We hit an error so all we can do is try to reconnect again.
						continue connectLoop

					} else {
						// Otherwise if the event was sent lets pop it from the event buffer.
						_ = s.interruptedEvents.Pop()
					}
				}
			}

			s.log.Info("beginning to stream events to subscriber")

		connectedLoop:
			for {
				select {
				case <-s.ctx.Done():
					// The context should only be cancelled if something local requested a disconnect.
					s.state = LOCAL_DISCONNECT
					s.log.Info("local disconnect requested")
					break connectedLoop
				case event := <-s.queue:
					if err := s.send(event); err != nil {
						s.state = SEND_ERR
						s.log.Error("unable to send event", zap.Error(err))
						// We don't know if the event was sent successfully or not so lets mark it interrupted.
						// Subscribers are expected to handle duplicate events so we'll err on the side of caution.
						s.interruptedEvents.Push(event)
						break connectedLoop
					}
				case response, ok := <-recvStream:
					if !ok {
						// Note an error or a legitimate remote disconnect could result in a REMOTE_DISCONNECT.
						s.state = REMOTE_DISCONNECT
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

		drainLoop:
			for {
				select {
				case event := <-s.queue:
					s.interruptedEvents.Push(event)
				default:
					break drainLoop
				}
			}

		}
	}
}

// Enqueue is called to add an event to the send queue for a particular subscriber.
func (s *BaseSubscriber) Enqueue(event *pb.Event) {

	// TODO (current location): Enqueue should only add events to s.queue when state == CONNECTED.
	// Otherwise it should just add them directly to the interruptedEvents buffer.
	// However that is not currently thread safe.
	// Options include:
	// (a) add a Mutex to EventRingBuffer
	// There is also some overhead due to locking, but it should be fairly minimal since we only
	// need to coordinate between the drainLoop and Enqueue (which shouldn't happen often).
	// But if we're wanting to keep events in sequential order, this doesn't provide that guarantee.
	// Especially if we implement the mutex properly inside the methods of EventRingBuffer.
	// We made it public we could use it to coordinate between drainLoop and Enqueue.
	// The connectedLoop could lock the mutex before changing the state of the subscriber.
	// Then Enqueue notices the state change but has to wait for us to flush all events from s.queue.
	// The mutex and state change also prevent Enqueue from endlessly adding new events to s.queue.
	//
	// (b) we the state of the subscriber to determine where the event should go.
	// This seems like a more clear cut approach, but I'm not sure how it would work yet.
	// Ideally we have a special drain state that Enqueue knows about, but the way we currently set
	// states the connectedLoop changes the state to affect the disconnectLoop.
	//
	// Whatever we do we also need to increase the size of s.queue.

	s.queue <- event
}

// Stop is called to cancel the context associated with a particular subscriber.
// This will cause the Go routine handling the subscriber to attempt to cleanly disconnect.
func (bs *BaseSubscriber) Stop() {
	bs.ctx.Done()
}
