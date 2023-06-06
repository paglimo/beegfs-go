package subscriber

import (
	"context"
	"io"
	"sync"

	pb "git.beegfs.io/beeflex/bee-watch/api/proto/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func Connect(ctx context.Context, wg *sync.WaitGroup, log *zap.Logger, remoteAddress string, eventBuffer <-chan *pb.Event) error {

	defer wg.Done()

	// TODO: Consider breaking out client setup into a separate function.

	log.Info("connecting to subscriber")

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.Dial(remoteAddress, opts...)
	if err != nil {
		return err
	}

	defer conn.Close()

	log.Info("connected to subscriber")

	client := pb.NewSubscriberClient(conn)

	log.Info("setup client")

	stream, err := client.ReceiveEvents(ctx)
	if err != nil {
		return err
	}

	log.Info("setup event stream")

	// Handle messages received from the subscriber.
	// TODO: Determine if we need to do anything else to gracefully shut this down.
	// I don't believe so because below we call stream.CloseSend() when the context is cancelled.
	// Which in theory tells the subscriber we're shutting down the stream so it'll close its end,
	// thus terminating this Go routine (should make sure this also works if the connection is broken).
	waitc := make(chan struct{})
	go func() {
		for {
			log.Info("waiting for events from subscriber")
			in, err := stream.Recv()
			if err == io.EOF {
				// read done
				close(waitc)
				return
			}
			if err != nil {
				log.Error("failed to receive control message", zap.Error(err))
				return
			}
			log.Info("subscriber acknowledged event", zap.Any("control", in))
		}
	}()

	// Send new events to the subscriber.
	log.Info("ready to send events to subscriber")
	for {
		select {
		case <-ctx.Done():
			log.Info("attempting to shutdown")
			stream.CloseSend()
			return nil
		case <-waitc:
			log.Info("subscriber closed connection")
			return nil
		case event := <-eventBuffer:

			if err := stream.Send(event); err != nil {
				log.Error("unable to send event to subscriber", zap.Error(err))
			}
			log.Info("sent event to subscriber")
		}
	}
}
