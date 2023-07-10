package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"git.beegfs.io/beeflex/bee-watch/internal/eventlog"
	"git.beegfs.io/beeflex/bee-watch/internal/subscribermgr"
	"git.beegfs.io/beeflex/bee-watch/internal/types"
	"go.uber.org/zap"
)

var (
	socketPath     = flag.String("socket", "/beegfs/meta_01_tgt_0101/socket/beegfs_eventlog", "The path to the BeeGFS event log socket")
	logFile        = flag.String("logFile", "", "log to a file instead of stdout")
	logDebug       = flag.Bool("logDebug", false, "enable logging at the debug level")
	enableSampling = flag.Bool("enableSampling", false, "output events per second")
	enablePProf    = flag.Int("enablePProf", 0, "specify a port where performance profiles will be made available on the localhost")
	// If we're targeting 500,000 EPS, then a buffer of 300,000,000 allows us to take up to 600s to drain offline events.
	metaBufferSize          = flag.Int("metaBufferSize", 300000000, "how many events to keep in memory if the BeeGFS metadata service sends events to BeeWatch faster than they can be sent to subscribers, or a subscriber is temporarily disconnected")
	metaBufferGCFrequency   = flag.Int("metaBufferGCFrequency", 100000, "after how many new events should unused buffer space be reclaimed automatically")
	metaBufferPollFrequency = flag.Int("metaBufferPollFrequency", 1, "how often subscribers should poll the metadata buffer for new events (causes more CPU utilization when idle)")
)

var subscriberConfigJson string = `
[
    {
        "type": "grpc",
        "id": 1,
        "name": "test-subscriber",
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

	if *enablePProf != 0 {
		go func() {
			http.ListenAndServe(fmt.Sprintf(":%d", *enablePProf), nil)
		}()
	}

	// We'll use a context to cleanly shutdown goroutines:
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	// We'll use a wait group to coordinate shutdown of all components:
	var wg sync.WaitGroup

	// Use a custom ring buffer to move events between the metadata socket and multiple subscribers:
	metaEventBuffer := types.NewMultiCursorRingBuffer(*metaBufferSize, *metaBufferGCFrequency)

	// Create a unix domain socket and listen for incoming connections from the metadata service:
	socket, err := eventlog.New(ctx, log, *socketPath, metaEventBuffer)
	if err != nil {
		log.Fatal("failed to listen for unix packets on socket path", zap.Error(err), zap.String("socket", *socketPath))
	}
	go socket.ListenAndServe(&wg) // Don't move this away from the creation to ensure the socket is cleaned up.
	wg.Add(1)

	if *enableSampling {
		go socket.Sample() // We don't care about adding this to the wg.
	}

	// Setup our subscriber manager:
	sm := subscribermgr.NewManager(log)
	err = sm.UpdateConfiguration(subscriberConfigJson, metaEventBuffer, *metaBufferPollFrequency)
	if err != nil {
		log.Fatal("unable to configure subscribers", zap.Error(err))
	}
	go sm.Manage(ctx, &wg)
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
