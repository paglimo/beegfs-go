package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	bw "git.beegfs.io/beeflex/bee-watch/api/proto/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	mockDBFilename = "scratch"
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
	var grpcServerOpts []grpc.ServerOption
	lis, err := net.Listen("tcp", *eventSubscriberInterface)
	if err != nil {
		log.Fatal("failed to setup listener for BeeWatch events", zap.Error(err))
	}
	genericGRPCServer := grpc.NewServer(grpcServerOpts...)
	eventSubscriberServer := NewEventSubscriberServer(log, ctx)
	bw.RegisterSubscriberServer(genericGRPCServer, eventSubscriberServer)

	// We'll run the gRPC server in a separate goroutine so we can catch signals
	// and coordinate shutdown in the main goroutine:
	go func() {
		log.Info("starting gRPC server")
		if genericGRPCServer.Serve(lis); err != nil {
			log.Fatal("unable to serve gRPC requests", zap.Error(err))
		}
	}()

	// Wait here until we're signaled to shutdown:
	<-ctx.Done()

	// Coordinate shutdown ensuring to disconnect all subscribers before shutting down the database.
	log.Info("shutting down gRPC server")
	genericGRPCServer.Stop()
	eventSubscriberServer.wg.Wait()
	// Note we don't use grpcServer.GracefulStop() because it will block until active RPCs are finished.
	// It will cancel the context associated with the stream, but stream.Recv() blocks so we can't use select.
	// To accommodate GracefulStop() we need ReceiveEvents() to start stream.Recv() in a separate goroutine.
	// That way ReceiveEvents() can watch for the context to be cancelled and return causing Recv() to return an error.
	// The trouble is this can lead to race conditions that often cause an event to be dropped.
	// Using Stop() is less convoluted and lets us always test the worst case scenario where streams don't close nicely.

	log.Info("shutting down database")
	dbShutdown()
	waitDB.Wait()
	log.Info("all components stopped, exiting")
}

type EventSubscriberServer struct {
	bw.UnimplementedSubscriberServer
	log *zap.Logger
	wg  sync.WaitGroup
	mu  sync.Mutex
}

func NewEventSubscriberServer(log *zap.Logger, ctx context.Context) *EventSubscriberServer {
	return &EventSubscriberServer{log: log}
}

func (s *EventSubscriberServer) ReceiveEvents(stream bw.Subscriber_ReceiveEventsServer) error {

	s.mu.Lock()
	s.wg.Add(1)
	s.mu.Unlock()
	defer s.wg.Done()

	s.log.Info("gRPC client connected")

	ctx := stream.Context()

	for {
		select {
		case <-ctx.Done():
			s.log.Info("stream was cancelled (is the server shutting down?)")
			return ctx.Err()
		default:
		}

		event, err := stream.Recv()

		if err == io.EOF {
			s.log.Info("client closed the stream")
			return nil
		} else if err != nil {
			s.log.Info("error receiving from client", zap.Error(err))
			continue
		}

		// TODO: Add logic to ignore duplicate events here.

		db.Add(event)
		if err = stream.Send(&bw.Response{CompletedSeq: event.SeqId}); err != nil {
			s.log.Error("error sending response", zap.Error(err), zap.Any("event", event.SeqId))
		}
	}
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

	file, err := os.OpenFile(mockDBFilename, os.O_RDWR|os.O_CREATE, 0755)

	if err != nil {
		db.log.Fatal("unable to open file", zap.Error(err))
	}

	defer file.Close()

	data, err := io.ReadAll(file)

	if err != nil {
		db.log.Fatal("unable to read file", zap.Error(err))
	}

	file.Close()

	var lastSeqID, lastDroppedSeq, lastMissedSeq uint64

	dataCleaned := strings.TrimSpace(string(data))

	if dataCleaned != "" {
		//l, err := strconv.Atoi(dataCleaned)
		_, err := fmt.Sscanf(dataCleaned, "%d,%d,%d", &lastSeqID, &lastDroppedSeq, &lastMissedSeq)
		if err != nil {
			db.log.Fatal("unable to parse file", zap.Error(err))
		} else {
			db.log.Info("using sequence IDs from file", zap.Any("lastSeqID", lastSeqID), zap.Any("lastDroppedSequence", lastDroppedSeq), zap.Any("lastMissedSeq", lastMissedSeq))
		}
	} else {
		db.log.Info("resetting sequence IDs (file not found or empty)", zap.Any("mockDBFilename", mockDBFilename))
		lastSeqID, lastDroppedSeq, lastMissedSeq = 0, 0, 0
	}

readEvents:
	for {
		select {
		case <-db.ctx.Done():
			break readEvents
		case event := <-db.events:

			if event.SeqId != lastSeqID+1 && lastSeqID != 0 {
				db.log.Error("warning: client dropped event(s) or sent events out of order", zap.Any("expectedSeqID", lastSeqID+1), zap.Any("actualSeqID", event.SeqId))
			}

			lastSeqID = event.SeqId

			if event.DroppedSeq != lastDroppedSeq {
				db.log.Error("warning: metadata service dropped event(s)", zap.Any("lastDroppedSeq", lastDroppedSeq), zap.Any("currentDroppedSeq", event.DroppedSeq))
				lastDroppedSeq = event.DroppedSeq
			}

			if event.MissedSeq != lastMissedSeq {
				db.log.Error("warning: metadata service missed event(s)", zap.Any("lastMisedSeq", lastMissedSeq), zap.Any("currentMissedSeq", event.MissedSeq))
				lastMissedSeq = event.MissedSeq
			}
		}
	}

	db.log.Info("writing out sequence IDs and shutting down", zap.Any("lastSeq", lastSeqID), zap.Any("lastDroppedSequence", lastDroppedSeq), zap.Any("lastMissedSequence", lastMissedSeq))

	file, err = os.OpenFile(mockDBFilename, os.O_RDWR|os.O_TRUNC, 0755)
	if err != nil {
		db.log.Error("unable to open file to write out sequence ID", zap.Error(err))
	}

	defer file.Close()

	_, err = file.WriteString(fmt.Sprintf("%d,%d,%d", lastSeqID, lastDroppedSeq, lastMissedSeq))
	if err != nil {
		db.log.Error("error writing out updated sequence IDs", zap.Error(err))
	}

	err = file.Sync()
	if err != nil {
		db.log.Error("error syncing database file", zap.Error(err))
	}

	db.log.Info("synchronized database to disk")
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
