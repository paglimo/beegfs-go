package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	bw "git.beegfs.io/beeflex/bee-watch/api/proto/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	lastSequenceIDFilename = "scratch"
)

var (
	logFile                  = flag.String("logFile", "", "log to a file instead of stdout")
	logDebug                 = flag.Bool("logDebug", false, "enable logging at the debug level")
	eventSubscriberInterface = flag.String("eventSubscriberInterface", "localhost:50052", "Where this subscriber will listen for events from BeeWatch nodes.")
	db                       = &MockDB{}
)

func main() {
	flag.Parse()
	log, err := getLogger()
	if err != nil {
		fmt.Println("Unable to initialize logger: ", err)
		os.Exit(1)
	}

	defer log.Sync() // Make sure we flush logs before shutting down.

	// We'll connect common OS signals to a context to cleanly shutdown goroutines:
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	// Simulate a database that would be used to persist events to disk:
	var waitDB sync.WaitGroup
	dbCtx, dbShutdown := context.WithCancel(context.Background())
	db = newDB(dbCtx, log)
	go db.Run(&waitDB)
	waitDB.Add(1)

	// Setup and start the gRPC server used to receive events from BeeWatch:
	var waitGRPC sync.WaitGroup
	var grpcServerOpts []grpc.ServerOption
	lis, err := net.Listen("tcp", *eventSubscriberInterface)
	if err != nil {
		log.Fatal("failed to setup listener for BeeWatch events", zap.Error(err))
	}
	grpcServer := grpc.NewServer(grpcServerOpts...)
	bw.RegisterSubscriberServer(grpcServer, NewEventSubscriberServer(log, ctx))

	// We'll run the gRPC server in a separate goroutine so we can catch signals
	// and coordinate shutdown in the main goroutine:
	go func() {
		defer waitGRPC.Done()
		log.Info("starting gRPC server")
		if grpcServer.Serve(lis); err != nil {
			log.Fatal("unable to serve gRPC requests", zap.Error(err))
		}
	}()
	waitGRPC.Add(1)

	// Wait here until we're signaled to shutdown:
	<-ctx.Done()

	// Coordinate shutdown ensuring to disconnect all subscribers before shutting down the database.
	log.Info("shutting down gRPC server")
	grpcServer.Stop()
	// Note we could also use grpcServer.GracefulStop() which would block until all RPCs are finished.
	// However its better to always test for the worst case scenario where streams aren't given time to close nicely.
	waitGRPC.Wait()

	log.Info("shutting down database")
	dbShutdown()
	waitDB.Wait()
	log.Info("all components stopped, exiting")
}

type EventSubscriberServer struct {
	log *zap.Logger
	ctx context.Context
	bw.UnimplementedSubscriberServer
}

func NewEventSubscriberServer(log *zap.Logger, ctx context.Context) *EventSubscriberServer {
	return &EventSubscriberServer{log: log, ctx: ctx}
}

func (s *EventSubscriberServer) ReceiveEvents(stream bw.Subscriber_ReceiveEventsServer) error {

	s.log.Info("gRPC client connected")

	errCh := make(chan error)

	// Recv() will block until the context of the stream is cancelled (or an even is received).
	// We don't need to run Recv() as a separate goroutine if we only use grpcServer.Stop() because it will close the connections
	// effectively cancelling the context of the stream causing Recv() to return with an error. However if we want to support
	// grpcServer.GracefulStop() then we need a goroutine so the handler ReceiveEvents() can watch for the server context to be
	// cancelled and return, also cancelling the context on the stream unblocking the receive call and terminating the goroutine.
	// Coordinating shutdown of a bidirectional streaming server is known to be a bit of a pain point:
	// * https://stackoverflow.com/questions/68218469/how-to-un-wedge-go-grpc-bidi-streaming-server-from-the-blocking-recv-call
	// * https://github.com/grpc/grpc-go/issues/2888
	// I have yet to find a more optimal/straightforward approach than something like this.
	go func() {
		for {
			in, err := stream.Recv()

			if err != nil {
				if status, ok := status.FromError(err); ok {
					if status.Code() == codes.Canceled {
						s.log.Info("gRPC stream was cancelled (probably the app or client is shutting down)", zap.Error(err))
						// If the context was cancelled ReceiveEvents is no longer listening to the channel.
						// Don't send anything else and just close the channel and return so we don't leak resources.
					}
				} else if err == io.EOF {
					s.log.Info("client closed the gRPC stream", zap.Error(err))
					errCh <- nil
				} else {
					s.log.Error("unknown error receiving from gRPC stream", zap.Error(err))
					errCh <- err
				}
				close(errCh)
				return
			}
			s.log.Debug("received event", zap.Any("event", in.SeqId))
			db.Add(in)
			if err = stream.Send(&bw.Response{CompletedSeq: in.SeqId}); err != nil {
				errCh <- err
				close(errCh)
				return
			}
		}
	}()

connectedLoop:
	for {
		select {
		case <-s.ctx.Done():
			s.log.Info("stopping receiving events because the app is shutting down")
			break connectedLoop
		case err, ok := <-errCh:
			if ok {
				return err
			}
			return nil
		}
	}

	return nil
}

type MockDB struct {
	ctx    context.Context
	log    *zap.Logger
	events chan *bw.Event
}

func newDB(ctx context.Context, log *zap.Logger) *MockDB {
	log = log.With(zap.String("component,", "database"))
	return &MockDB{
		ctx:    ctx,
		log:    log,
		events: make(chan *bw.Event),
	}
}

func (db *MockDB) Add(event *bw.Event) {
	db.events <- event
}

func (db *MockDB) Run(wg *sync.WaitGroup) {
	defer wg.Done()

	file, err := os.OpenFile(lastSequenceIDFilename, os.O_RDWR|os.O_CREATE, 0755)

	if err != nil {
		db.log.Fatal("unable to open file", zap.Error(err))
	}

	defer file.Close()

	data, err := io.ReadAll(file)

	if err != nil {
		db.log.Fatal("unable to read file", zap.Error(err))
	}

	file.Close()

	var lastSeq uint64 = 0
	dataCleaned := strings.TrimSpace(string(data))

	if dataCleaned != "" {
		l, err := strconv.Atoi(dataCleaned)

		if err != nil {
			db.log.Fatal("unable to parse file", zap.Error(err))
		} else {
			lastSeq = uint64(l)
			db.log.Info("using last sequence ID from file", zap.Any("lastSequenceID", lastSeq))
		}
	} else {
		db.log.Info("resetting lastSequenceID (file not found or empty)", zap.Any("lastSequenceIDFilename", lastSequenceIDFilename))
	}

readEvents:
	for {
		select {
		case <-db.ctx.Done():
			db.log.Info("writing out last sequence ID and shutting down", zap.Int("lastSeq", int(lastSeq)))
			break readEvents
		case event := <-db.events:

			if event.SeqId != lastSeq+1 && lastSeq != 0 {
				db.log.Error("warning: detected dropped event", zap.Any("expected", lastSeq+1), zap.Any("actual", event.SeqId))
			}

			lastSeq = event.SeqId
		}
	}

	file, err = os.OpenFile(lastSequenceIDFilename, os.O_RDWR|os.O_TRUNC, 0755)
	if err != nil {
		db.log.Error("unable to open file to write out sequence ID", zap.Error(err))
	}

	defer file.Close()

	_, err = file.WriteString(fmt.Sprint(lastSeq))
	if err != nil {
		db.log.Error("error writing out sequence ID", zap.Error(err))
	}

	err = file.Sync()
	if err != nil {
		db.log.Error("error syncing sequence ID file", zap.Error(err))
	}
}

// getLogger parses command line logging options and returns an appropriately configured zap.Logger.
func getLogger() (*zap.Logger, error) {

	var config zap.Config

	if *logDebug {
		config = zap.NewDevelopmentConfig()
	} else {
		config = zap.NewProductionConfig()
	}

	if *logFile != "" {
		logFile, err := os.OpenFile(*logFile, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			fmt.Println("unable to create log file: ", err)
			os.Exit(1)
		}

		config.OutputPaths = []string{logFile.Name()}
	}

	return config.Build()
}
