package subscriber

import (
	"context"
	"math/rand"
	"time"

	pb "git.beegfs.io/beeflex/bee-watch/api/proto/v1"
	"go.uber.org/zap"
)

// Subscriber defines the interface all subscribers are expected to implement.
type Subscriber interface {
	// These methods are implemented by the base subscriber type:
	GetID() string
	GetName() string
	Manage()
	Enqueue(*pb.Event)
	Stop()
	// These methods must be implemented by a concrete subscriber type (such as gRPC):
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
	interruptedEvents []*pb.Event // Used as a temp buffer if the connection with a subscriber is lost.
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
		interruptedEvents: make([]*pb.Event, 1),
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
			if len(s.interruptedEvents) > 0 {
				err := s.send(s.interruptedEvents[0])
				if err != nil {
					s.state = SEND_ERR
					// A failed send() would have added the event to the back of the interrupted events queue.
					// To avoid sending duplicate events we'll drop it.
					s.interruptedEvents = s.interruptedEvents[:len(s.interruptedEvents)-1]
					s.log.Error("unable to send interrupted event", zap.Error(err), zap.Any("event_seq", s.interruptedEvents[0].SeqId), zap.Any("interrupted_event_count", len(s.interruptedEvents)))
					// We hit an error so all we can do is try to reconnect again.
					continue connectLoop

				} else {
					// Otherwise if the event was sent lets pop it from the queue.
					// Note a slice is not the most efficient data structure to use as a queue,
					// but since we don't expect this queue to be large since it should mostly be used as a
					// recovery mechanism, the simplicity probably outweighs the benefits of a better approach.
					s.interruptedEvents = s.interruptedEvents[1:]
				}
			}

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
						break connectedLoop
					}
				case response, ok := <-recvStream:
					if !ok {
						// Note an error or a legitimate remote disconnect could result in a REMOTE_DISCONNECT.
						s.state = REMOTE_DISCONNECT
						s.log.Info("remote disconnect received")
						break connectedLoop
					}
					s.log.Info("received response from subscriber", zap.Any("response", response))
					// TODO: https://linear.app/thinkparq/issue/BF-29/acknowledge-events-sent-to-all-subscribers-back-to-the-metadata-server
				}
			}
		}
	}
}

// Enqueue is called to add an event to the send queue for a particular subscriber.
func (s *BaseSubscriber) Enqueue(event *pb.Event) {
	s.queue <- event
}

// Stop is called to cancel the context associated with a particular subscriber.
// This will cause the Go routine handling the subscriber to attempt to cleanly disconnect.
func (bs *BaseSubscriber) Stop() {
	bs.ctx.Done()
}
