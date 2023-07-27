package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"git.beegfs.io/beeflex/bee-watch/internal/configmgr"
	"git.beegfs.io/beeflex/bee-watch/internal/eventlog"
	"git.beegfs.io/beeflex/bee-watch/internal/logger"
	"git.beegfs.io/beeflex/bee-watch/internal/subscribermgr"
	"git.beegfs.io/beeflex/bee-watch/internal/types"
	"go.uber.org/zap"
)

var (
	cfgFile = flag.String("cfgFile", "/etc/beegfs/bee-watch.conf", "The path to the BeeWatch configuration file.")
	//socketPath = flag.String("socket", "/beegfs/meta_01_tgt_0101/socket/beegfs_eventlog", "The path to the BeeGFS event log socket")
	//logFile        = flag.String("logFile", "", "log to a file instead of stdout")
	//logDebug       = flag.Bool("logDebug", false, "enable logging at the debug level")
	//enableSampling = flag.Bool("enableSampling", false, "output events per second")
	//enablePProf    = flag.Int("enablePProf", 0, "specify a port where performance profiles will be made available on the localhost")
	// If we're targeting 500,000 EPS, then a buffer of 300,000,000 allows us to take up to 600s to drain offline events.
	// Worst case we're looking at ~10KB per event. So if we allow up to 1M events memory should be around 1GB of memory utilization.
	//metaBufferSize        = flag.Int("metaBufferSize", 10000000, "how many events to keep in memory if the BeeGFS metadata service sends events to BeeWatch faster than they can be sent to subscribers, or a subscriber is temporarily disconnected")
	//metaBufferGCFrequency = flag.Int("metaBufferGCFrequency", 100000, "after how many new events should unused buffer space be reclaimed automatically")
	//metaBufferPollFrequency = flag.Int("metaBufferPollFrequency", 1, "how often subscribers should poll the metadata buffer for new events (causes more CPU utilization when idle)")
)

func main() {

	flag.Parse()

	// We initialize ConfigManager first because all components require the initial config to start up.
	cfgMgr, initialCfg, err := configmgr.New(*cfgFile)
	if err != nil {
		log.Fatalf("unable to get initial configuration: %s", err)
	}

	logger, err := logger.New(initialCfg)
	if err != nil {
		log.Fatalf("Unable to initialize logger: %s", err)
	}

	if initialCfg.Developer.PerfProfilePort != 0 {
		go func() {
			http.ListenAndServe(fmt.Sprintf(":%d", initialCfg.Developer.PerfProfilePort), nil)
		}()
	}

	// Create a channel to receive OS signals to coordinate graceful shutdown:
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	// We'll use a wait group to coordinate shutdown of all components including
	// verifying individual subscribers are disconnected:
	var wg sync.WaitGroup

	// Use a custom ring buffer to move events between the metadata socket and multiple subscribers:
	metaEventBuffer := types.NewMultiCursorRingBuffer(initialCfg.Metadata.SysFileEventBufferSize, initialCfg.Metadata.SysFileEventBufferGCFrequency)

	// Setup the subscriber manager:
	sm := subscribermgr.New(logger.Logger, metaEventBuffer, &wg)

	// Using a different context for subscribers is important so we can coordinate shutdown.
	// Notably we want to wait to disconnect until the buffer is empty.
	subscribersCtx, subscribersCancel := context.WithCancel(context.Background())
	defer subscribersCancel()
	go sm.Manage(subscribersCtx, &wg)
	wg.Add(1)
	cfgMgr.AddListener(sm)

	// Create a unix domain socket and listen for incoming connections from the metadata service.
	// The metadata service gets its own context so we can disconnect it first when shutting down.
	// We do this last so we don't start reading events from the metadata server until we're sure everything is ready.
	// If for some reason the subscriber configuration is bad it can be updated without restarting the app and potentially dropping events.
	metaCtx, metaCancel := context.WithCancel(context.Background())
	defer metaCancel()

	socket, err := eventlog.New(metaCtx, logger.Logger, initialCfg.Metadata.SysFileEventLogTarget, metaEventBuffer)
	if err != nil {
		logger.Fatal("failed to listen for unix packets on socket path", zap.Error(err), zap.String("socket", initialCfg.Metadata.SysFileEventLogTarget))
	}
	go socket.ListenAndServe(&wg) // Don't move this away from the creation to ensure the socket is cleaned up.
	wg.Add(1)

	if initialCfg.Developer.PerfLogIncomingEventRate {
		go socket.Sample() // We don't care about adding this to the wg. It'll just stop when the meta service is cancelled.
	}

	// Start accepting dynamic configuration updates:
	configCtx, configCancel := context.WithCancel(context.Background())
	go cfgMgr.Manage(configCtx, logger.Logger)

	<-sigs // Block here and wait for a signal to shutdown.
	logger.Info("shutdown signal received, no longer accepting events from the metadata service and waiting for outstanding events to be sent to subscribers before exiting (send another SIGINT or SIGTERM to shutdown immediately)")
	metaCancel()   // When shutting down first stop adding events to the metadata buffer.
	configCancel() // Stop accepting configuration updates.

shutdownLoop:
	for {
		select {
		case <-sigs:
			// If we get another signal we should shutdown immediately.
			logger.Warn("attempting to disconnect all subscribers and shutdown down immediately, outstanding events in the buffer may be lost")
			subscribersCancel()
			break shutdownLoop
		case <-time.After(1 * time.Second):
			// Otherwise wait for subscribers to send all events and the buffer to be empty:
			if metaEventBuffer.AllEventsAcknowledged() {
				subscribersCancel()
				break shutdownLoop
			}
		}
	}

	// Wait for subscribers to disconnect and all components to stop.
	// We will wait here indefinitely even if we get another signal so we can try and cleanup resources.
	// SIGKILL would be needed at this point if something gets blocked shutting down.
	wg.Wait()
	logger.Info("all components stopped, exiting")
}
