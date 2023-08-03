package main

import (
	"context"

	"github.com/spf13/pflag"

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
	"git.beegfs.io/beeflex/bee-watch/internal/logger"
	"git.beegfs.io/beeflex/bee-watch/internal/metadata"
	"git.beegfs.io/beeflex/bee-watch/internal/subscribermgr"
	"go.uber.org/zap"
)

func main() {

	// All application configuration (AppConfig) can be set using flags. The
	// default values specified here will be used as configuration defaults.
	// Note defaults for configuration specified using a slice is not set here.
	// Notably subscriber defaults are handled as part of initializing a
	// particular subscriber type.
	pflag.String("cfgFile", "", "The path to the a configuration file (can be omitted to set all configuration using flags and/or environment variables).")
	pflag.String("log.type", "stdout", "Where log messages should be sent ('stdout', 'journal', 'logfile').")
	pflag.String("log.file", "/var/log/bee-watch.log", "The path to the desired log file when logType is 'logfile'.")
	pflag.Bool("log.debug", false, "Enable logging at the debug level (will impact performance).")
	pflag.Bool("log.incomingEventRate", false, "output the rate of incoming events per second")
	pflag.String("metadata.eventLogTarget", "", "The path where the BeeGFS metadata service expected to log events to a unix socket (should match sysFileEventLogTarget in beegfs-meta.conf).")
	pflag.Int("metadata.eventBufferSize", 10000000, "How many events to keep in memory if the BeeGFS metadata service sends events to BeeWatch faster than they can be sent to subscribers, or a subscriber is temporarily disconnected.\nWorst case memory usage is approximately (10KB x sysFileEventBufferSize).")
	pflag.Int("metadata.eventBufferGCFrequency", 100000, "After how many new events should unused buffer space be reclaimed automatically. \nThis should be set taking into consideration the buffer size. \nMore frequent garbage collection will negatively impact performance, whereas less frequent garbage collection risks running out of memory and dropping events.")
	pflag.Int("handler.maxReconnectBackOff", 60, "When a connection cannot be made to a subscriber subscriber reconnection attempts will be made with an exponential back off. This is the maximum time in seconds between reconnection attempts to avoid increasing the back off timer forever.")
	pflag.Int("handler.waitForAckAfterConnect", 2, "When a subscriber connects/reconnects wait this long for the subscriber to acknowledge the sequence ID of the last event it received successfully. This prevents sending duplicate events if the connection was disrupted unexpectedly.")
	pflag.Int("handler.pollFrequency", 1, "How often subscribers should poll the metadata buffer for new events (causes more CPU utilization when idle).")
	pflag.String("subscribers", "", `Specify one or more subscribers separated by semicolons.
	The full list of subscribers should be enclosed in "double quotes".
	The parameters for each subscriber should be specified as key='value'.
	Include all required/desired parameters for the particular subscriber type you want to configure.
	Example: --subscribers="id=1,name='subscriber1',type='grpc';id=2,name='subscriber2',type='grpc'"`)
	// Hidden flags:
	pflag.Int("developer.perfProfilingPort", 0, "Specify a port where performance profiles will be made available on the localhost via pprof (0 disables performance profiling).")
	pflag.CommandLine.MarkHidden("developer.perfProfilingPort")
	pflag.Bool("developer.dumpConfig", false, "Dump the full configuration and immediately exit.")
	pflag.CommandLine.MarkHidden("developer.dumpConfig")

	pflag.CommandLine.SortFlags = false
	pflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		pflag.PrintDefaults()
		helpText := `
Further info:
	Except for subscribers, configuration may be set using a mix of flags, environment variables, and values from a TOML configuration file. 
	Configuration will be merged using the following precedence order (highest->lowest): (1) flags (2) environment variables (3) configuration file (4) defaults.
	Subscribers can only be specified using one of these options, and when set using environment variables or a configuration file, can be updated dynamically after the application starts without by sending a hangup signal (SIGHUP).
Using environment variables:
	To specify configuration using environment variables specify %sKEY=VALUE where KEY is the flag name you want to specify in all capitals replacing dots (.) with underscores (_).
	Examples: 
	export %sLOG_DEBUG=true
	export %sSUBSCRIBERS="id=1,name='subscriber1',type='grpc';id=2,name='subscriber2',type='grpc'"
`
		fmt.Fprintf(os.Stderr, helpText, configmgr.ConfigEnvVariablePrefix, configmgr.ConfigEnvVariablePrefix, configmgr.ConfigEnvVariablePrefix)
		os.Exit(0)
	}

	pflag.Parse()

	// We initialize ConfigManager first because all components require the initial config to start up.
	cfgMgr, initialCfg, err := configmgr.New(pflag.CommandLine)
	if err != nil {
		log.Fatalf("unable to get initial configuration: %s", err)
	}

	if initialCfg.Developer.DumpConfig {
		fmt.Printf("Dumping AppConfig and exiting...\n\n")
		fmt.Printf("%+v\n", initialCfg)
		fmt.Println(`
		WARNING: Configuration listed here for individual subscribers may not reflect their final configuration.
		Individual subscriber types may define their own custom defaults, or automatically override invalid user configuration.
		`)
		os.Exit(0)
	}

	logger, err := logger.New(initialCfg.Log)
	if err != nil {
		log.Fatalf("Unable to initialize logger: %s", err)
	}

	if initialCfg.Developer.PerfProfilingPort != 0 {
		go func() {
			http.ListenAndServe(fmt.Sprintf(":%d", initialCfg.Developer.PerfProfilingPort), nil)
		}()
	}

	// Create a channel to receive OS signals to coordinate graceful shutdown:
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	// We'll use a wait group to coordinate shutdown of all components including
	// verifying individual subscribers are disconnected:
	var wg sync.WaitGroup

	// Setup the Metadata manager:
	metaCtx, metaCancel := context.WithCancel(context.Background())
	defer metaCancel()

	metaMgr, metaCleanup, err := metadata.New(metaCtx, logger.Logger, initialCfg.Metadata)
	if err != nil {
		logger.Fatal("failed to listen for unix packets on socket path", zap.Error(err), zap.String("socket", initialCfg.Metadata.EventLogTarget))
	}
	defer metaCleanup()

	// Setup the subscriber manager:
	sm := subscribermgr.New(logger.Logger, metaMgr.EventBuffer, &wg)

	// Using a different context for subscribers is important so we can coordinate shutdown.
	// Notably we want to wait to disconnect until the buffer is empty.
	subscribersCtx, subscribersCancel := context.WithCancel(context.Background())
	defer subscribersCancel()
	go sm.Manage(subscribersCtx, &wg)
	wg.Add(1)
	cfgMgr.AddListener(sm)

	// We do this last so we don't start reading events from the metadata server until we're sure everything is ready.
	// If for some reason the subscriber configuration is bad it can be updated without restarting the app and potentially dropping events.
	go metaMgr.Manage(&wg)
	wg.Add(1)

	if initialCfg.Log.IncomingEventRate {
		go metaMgr.Sample() // We don't care about adding this to the wg. It'll just stop when the meta service is cancelled.
	}

	// Start accepting dynamic configuration updates:
	configCtx, configCancel := context.WithCancel(context.Background())
	go cfgMgr.Manage(configCtx, logger.Logger)

	<-sigs // Block here and wait for a signal to shutdown.
	logger.Info("shutdown signal received, no longer accepting events from the metadata service and waiting for outstanding events to be sent to subscribers before exiting (send another SIGINT or SIGTERM to shutdown immediately)")
	metaCancel() // When shutting down first stop adding events to the metadata buffer.

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
			if metaMgr.EventBuffer.AllEventsAcknowledged() {
				subscribersCancel()
				break shutdownLoop
			}
		}
	}

	configCancel() // Stop accepting configuration updates.
	// Wait for subscribers to disconnect and all components to stop.
	// We will wait here indefinitely even if we get another signal so we can try and cleanup resources.
	// SIGKILL would be needed at this point if something gets blocked shutting down.
	wg.Wait()
	logger.Info("all components stopped, exiting")
}
