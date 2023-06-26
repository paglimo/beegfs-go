package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	pb "git.beegfs.io/beeflex/bee-watch/api/proto/v1"
	"git.beegfs.io/beeflex/bee-watch/internal/eventlog"
	"git.beegfs.io/beeflex/bee-watch/internal/subscriber"
	"go.uber.org/zap"
)

var (
	socketPath = flag.String("socket", "/beegfs/meta_01_tgt_0101/socket/beegfs_eventlog", "The path to the BeeGFS event log socket")
	logFile    = flag.String("logFile", "", "log to a file instead of stdout")
	logDebug   = flag.Bool("logDebug", false, "enable logging at the debug level")
	// If a subscriber disconnects there is a brief cutover before we start buffering interrupted events for that subscriber using a ring buffer.
	// Currently the metadata service expects events to be read as fast as they are sent to the socket otherwise it will drop events.
	// Until we have the metadata service buffering events for us, we need the option to have BeeWatch implement the in-memory buffer.
	// Once the metadata service is buffering events for us this can be set to zero without dropping events.
	metaBufferSize = flag.Int("metaBufferSize", 976562, "buffer up to this many events before they can be added to subscriber buffers")
)

var subscriberConfigJson string = `
[
    {
        "type": "grpc",
        "id": "1",
        "name": "bee-remote",
        "hostname":"localhost",
		"port":"50052",
		"allow_insecure":true
    }
]
`

func main() {

	flag.Parse()

	log, err := getLogger()
	if err != nil {
		fmt.Println("Unable to initialize logger: ", err)
		os.Exit(1)
	}

	// We'll use a context to cleanly shutdown goroutines:
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	// We'll use a wait group to coordinate shutdown of all components:
	var wg sync.WaitGroup

	// Use a channel as a buffer to move events between threads:
	metaEventBuffer := make(chan *pb.Event, *metaBufferSize)

	// Create a unix domain socket and listen for incoming connections:
	socket, err := eventlog.New(ctx, log, *socketPath)
	if err != nil {
		log.Fatal("failed to listen for unix packets on socket path", zap.Error(err), zap.String("socket", *socketPath))
	}
	go socket.ListenAndServe(&wg, metaEventBuffer) // Don't move this away from the creation to ensure the socket is cleaned up.
	wg.Add(1)

	// Setup our subscriber manager:
	sm := subscriber.NewManager(log)
	err = sm.UpdateConfiguration(subscriberConfigJson)
	if err != nil {
		log.Fatal("unable to configure subscribers", zap.Error(err))
	}
	go sm.Manage(ctx, &wg, metaEventBuffer)
	wg.Add(1)

	wg.Wait()
	log.Info("all components stopped, exiting")
}

// getLogger parses command line logging options and returns an appropriately configured zap.Logger.
func getLogger() (*zap.Logger, error) {

	var config zap.Config
	config.InitialFields = map[string]interface{}{"serviceName": "bee-watch"}

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
