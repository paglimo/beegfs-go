package subscriber

import (
	"context"

	pb "git.beegfs.io/beeflex/bee-watch/api/proto/v1"
	"go.uber.org/zap"
)

// Subscriber defines the interface all subscribers are expected to implement.
type Subscriber interface {
	// These methods are implemented by the base subscriber type:
	GetID() string
	GetName() string
	Handle()
	Enqueue(*pb.Event)
	Stop()
	// These methods must be implemented by a concrete subscriber type (such as gRPC):
	connect() (retry bool, err error)
	send(*pb.Event) (retry bool, err error)
	receive() (retry bool, err error)
	disconnect() (retry bool, err error)
}

type SubscriberState string

const (
	DISCONNECTED SubscriberState = "disconnected"
	CONNECTED    SubscriberState = "connected"
	CONNECT_ERR  SubscriberState = "connect_err"
)

type BaseSubscriber struct {
	id    string
	name  string
	state SubscriberState
	queue chan *pb.Event
	ctx   context.Context
	log   *zap.Logger
	Subscriber
}

// This is a "comparable" view of the BaseSubscriber struct used for testing.
// When BaseSubscriber is updated it should also be updated with any fields that are a comparable type.
// Notably the queue, ctx, and log fields are not comparable and thus omitted.
type ComparableBaseSubscriber struct {
	id   string
	name string
}

// Used for testing (notably TestNewSubscribersFromJson).
func newComparableBaseSubscriber(s BaseSubscriber) ComparableBaseSubscriber {
	return ComparableBaseSubscriber{
		id:   s.id,
		name: s.name,
	}
}

// newBaseSubscriber is intended to be used with newSubscriberFromConfig.
func newBaseSubscriber(id string, name string, queueSize int, log *zap.Logger) *BaseSubscriber {
	return &BaseSubscriber{
		id:    id,
		name:  name,
		queue: make(chan *pb.Event, queueSize),
		ctx:   context.Background(),
		log:   log,
	}
}

func (s *BaseSubscriber) GetID() string {
	return s.id
}

func (s *BaseSubscriber) GetName() string {
	return s.name
}

// Handles the the connection with a particular Subscriber.
// It expects to be run as a Go routine.
// It handles connecting, disconnecting, and actually sending events to the subscriber.
// If an error occurs it will retry the method with an exponential backoff unless retry=false.
// If retry=false then the following recovery path will be attempted:
// * connect() -> not retried.
// * send() -> disconnect() -> connect() -> not retried.
// * disconnect() -> not retried.
func (s *BaseSubscriber) Handle() {

	// TODO: Actually implement the connection lifecycle managment.
	for {
		select {
		case <-s.ctx.Done():
			if s.state == CONNECTED {
				retry, err := s.disconnect()
				if err != nil {
					s.log.Info("unable to disconnect subscriber", zap.Error(err))
					if retry {
						continue // TODO: Make sure this allows us to retry the disconnect.
					}
				}
			}
			return
		case event := <-s.queue:
			retry, err := s.send(event)
			if err != nil {
				s.log.Error("unable to send event", zap.Error(err), zap.Bool("retryable", retry))
				// TODO: If retryable, attempt reresending the event with an exponential backoff.
				// Otherwise attempt to disconnect and reconnect.
				// Make sure not to block here if a shutdown is requested.
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
