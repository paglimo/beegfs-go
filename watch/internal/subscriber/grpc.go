package subscriber

import (
	"fmt"
	"io"
	"sync"
	"time"

	pb "git.beegfs.io/beeflex/bee-watch/api/proto/v1"
	"git.beegfs.io/beeflex/bee-watch/internal/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCSubscriber struct {
	BaseSubscriber
	Hostname      string
	Port          string
	AllowInsecure bool
	conn          *grpc.ClientConn
	client        pb.SubscriberClient
	stream        pb.Subscriber_ReceiveEventsClient
	recvStream    chan *pb.Response
	recvMutex     *sync.Mutex // Guarantee there is only ever one Go routine receiving responses.
}

var _ Subscriber = &GRPCSubscriber{} // Verify type satisfies interface.

// This is a "comparable" view of the GRPCSubscriber struct used for testing.
// When GRPCSubscriber is updated it should also be updated with any fields that are a comparable type.
type ComparableGRPCSubscriber struct {
	ComparableBaseSubscriber
	Hostname      string
	Port          string
	AllowInsecure bool
}

// Used for testing (notably TestNewSubscribersFromJson).
func newComparableGRPCSubscriber(s GRPCSubscriber) ComparableGRPCSubscriber {

	base := newComparableBaseSubscriber(s.BaseSubscriber)
	return ComparableGRPCSubscriber{
		ComparableBaseSubscriber: base,
		Hostname:                 s.Hostname,
		Port:                     s.Port,
		AllowInsecure:            s.AllowInsecure,
	}

}

func (s *GRPCSubscriber) connect() (retry bool, err error) {

	s.log.Info("connecting to subscriber")

	var opts []grpc.DialOption
	if s.AllowInsecure {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	// TODO: Handle if TLS should be used.

	s.conn, err = grpc.Dial(s.Hostname+":"+s.Port, opts...)
	if err != nil {
		return true, fmt.Errorf("unable to connect to the subscriber: %w", err)
	}

	s.client = pb.NewSubscriberClient(s.conn)
	s.log.Info("gRPC client initialized")

	// TODO: Evaluate if this causes problems to reuse the context here.
	// Because we connect and disconnect in two separate functions,
	// we need to ensure we're actually able to run out disconnect function before things shutdown.
	s.stream, err = s.client.ReceiveEvents(s.ctx)

	if err != nil {
		return true, fmt.Errorf("unable to setup gRPC client stream: %w", err)
	}

	s.log.Info("setup event stream")
	return false, nil
}

// Send attempts to transmit an event to a remote subscriber.
// It is expected to implement any logic for attempting to resend an event if the first attempt fails.
// If it returns an error it will add the event to an interrupted events queue for this subscriber (so it can be retried).
func (s *GRPCSubscriber) send(event *pb.Event) (err error) {

	if err := s.stream.Send(event); err != nil {
		// TODO: Is there ever a scenario where we'd want to retry to send the event?
		s.interruptedEvents = append(s.interruptedEvents, event)
		return fmt.Errorf("unable to send event to subscriber: %w", err)
	}

	return nil
}

// Receive starts a Go routine that receives events from the subscriber.
// It returns a channel where responses from the subscriber will be sent.
// Normally this channel is read by the base subscriber's Manage() method.
//
// For gRPC even after the connection state shifts from CONNECTED,
// there may still be responses we need to read until we get an io.EOF error.
// To facilitate this we actually setup the recvStream channel on the GRPCSubscriber struct,
// then return the same channel so the base subscriber's manage() function can use it.
func (s *GRPCSubscriber) receive() (recvStream chan *pb.Response) {

	// TODO (CURRENT LOCATION): This returns a "runtime error: invalid memory address or nil pointer dereference"
	// Probably I didn't initialize it properly
	//
	// Typically this mutex should not be necessary.
	// Receive() should only ever be called once for each connection to a subscriber.
	// However it guarantees there will only ever be one Go routine listening to subscriber responses.
	// It also guarantees we don't try and reinitialize an in-use channel until it is closed.
	if s.recvMutex.TryLock() {
		return s.recvStream
	}

	s.recvMutex.Lock()

	// If the channel was not yet initialized or closed we'll reinitialize it:
	s.recvStream = make(chan *pb.Response)

	go func() {
		defer close(s.recvStream)
		defer s.recvMutex.Unlock()
		for {
			in, err := s.stream.Recv()
			if err == io.EOF {
				return
			} else if err != nil {
				s.log.Error("failed to receive response", zap.Error(err))
				// TODO: Figure out if/how we want to handle this scenario better.
				// For example retry a few times or parse the error.
				// For now we'll treat it as a remote disconnect which will try to reconnect.
				// Ideally we don't log anything here (as is standard for the other methods).
				return
			}
			s.recvStream <- in
		}
	}()

	return s.recvStream
}

// Disconnect handles cleaning up all resources used for the gRPC connection.
// It can be called at any point in the lifecycle of a connection.
// For example even if a connection was only partially established, it can be used to cleanup.
// It will also attempt to receive any additional responses from the subscriber.
// For example final acknowledgement of events that were already sent.
func (s *GRPCSubscriber) disconnect() error {

	disconnectTimeout := 30
	var multiErr types.MultiError

	if err := s.stream.CloseSend(); err != nil {
		err = fmt.Errorf("an error occurred closing the send direction of the subscriber stream: %w", err)
		multiErr.Errors = append(multiErr.Errors, err)
	} // TODO: Handle the error if the stream was already closed.

	//
	if s.recvStream != nil {
	responseLoop:
		for {
			select {
			case response, ok := <-s.recvStream:
				if !ok {
					break // Subscriber has already disconnected.
				}
				s.log.Info("received response from subscriber", zap.Any("response", response))
				// TODO: https://linear.app/thinkparq/issue/BF-29/acknowledge-events-sent-to-all-subscribers-back-to-the-metadata-server
				continue responseLoop
			case <-time.After(time.Duration(disconnectTimeout)):
				// TODO: Make the disconnect timeout configurable.
				err := fmt.Errorf("subscriber failed to disconnect within the %ds timeout", disconnectTimeout)
				multiErr.Errors = append(multiErr.Errors, err)
				break responseLoop
			}
		}
	}

	if err := s.conn.Close(); err != nil {
		err = fmt.Errorf("an error ocurred closing the subscriber connection: %w", err)
		multiErr.Errors = append(multiErr.Errors, err)
	} // TODO: Handle the error if the connection was already closed.

	if len(multiErr.Errors) > 0 {
		return &multiErr
	}

	return nil
}
