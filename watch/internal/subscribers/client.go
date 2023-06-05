package subscribers

import (
	"context"
	"io"
	"path"
	"reflect"
	"sync"

	pb "git.beegfs.io/beeflex/bee-watch/api/proto/v1"
	"git.beegfs.io/beeflex/bee-watch/internal/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type subscribers struct{}

func Connect(ctx context.Context, wg *sync.WaitGroup, log *zap.Logger, remoteAddress string, eventBuffer <-chan *types.Packet) error {

	defer wg.Done()

	// TODO: Consider breaking out client setup into a separate function.

	log = log.With(zap.String("component", path.Base(reflect.TypeOf(subscribers{}).PkgPath())))
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

	// TODO (CURRENT LOCATION): For some reason we're blocking here, figure out why.
	// Do we need to do something else on the server before we can use the stream?
	// Is there some other way we should set this up so it is non-blocking?

	stream, err := client.ReceiveEvents(ctx)
	if err == nil {
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
		case p := <-eventBuffer:
			// TODO: Is there any way to do this more efficiently?
			event := pb.Event{
				FormatVersionMajor: uint32(p.FormatVersionMajor),
				FormatVersionMinor: uint32(p.FormatVersionMinor),
				SeqId:              0, // TODO: Generate
				Size:               p.Size,
				DroppedSeq:         p.DroppedSeq,
				MissedSeq:          p.MissedSeq,
				Type:               pb.Event_Type(p.Type),
				Path:               p.Path,
				EntryId:            p.EntryId,
				ParentEntryId:      p.ParentEntryId,
				TargetPath:         p.TargetPath,
				TargetParentId:     p.TargetParentId,
			}

			if err := stream.Send(&event); err != nil {
				log.Error("unable to send event to subscriber", zap.Error(err))
			}
			log.Info("sent event to subscriber")
		}
	}
}
