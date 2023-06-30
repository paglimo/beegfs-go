package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
)

var (
	socketPath = flag.String("socket", "/beegfs/meta_01_tgt_0101/socket/beegfs_eventlog", "The path to the BeeGFS event log socket")
	logFile    = flag.String("logFile", "", "log to a file instead of stdout")
	logDebug   = flag.Bool("logDebug", false, "enable logging at the debug level")
	frequency  = flag.Duration("frequency", 1*time.Second, "how often an event should be sent to the socket (0 is as fast as possible)")
)

func main() {
	flag.Parse()

	log, err := getLogger()
	if err != nil {
		fmt.Println("Unable to initialize logger: ", err)
		os.Exit(1)
	}

	// Setup the socket:
	conn, err := net.Dial("unixpacket", *socketPath)

	if err != nil {
		log.Fatal("error listening on socket", zap.Error(err))
	}

	defer conn.Close()

	dummyEvent := []byte{0x1, 0x0, 0x0, 0x0, 0x5d, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xb, 0x0, 0x0, 0x0, 0xc, 0x0, 0x0, 0x0, 0x30, 0x2d, 0x36, 0x34, 0x34, 0x42, 0x46, 0x46, 0x31, 0x46, 0x2d, 0x31, 0x0, 0xc, 0x0, 0x0, 0x0, 0x30, 0x2d, 0x36, 0x34, 0x34, 0x43, 0x30, 0x30, 0x31, 0x42, 0x2d, 0x31, 0x0, 0x8, 0x0, 0x0, 0x0, 0x2f, 0x62, 0x61, 0x72, 0x2f, 0x66, 0x6f, 0x6f, 0x0, 0x4, 0x0, 0x0, 0x0, 0x2f, 0x66, 0x6f, 0x6f, 0x0, 0x4, 0x0, 0x0, 0x0, 0x72, 0x6f, 0x6f, 0x74, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}

	if *frequency == 0 {
		for {
			_, err := conn.Write(dummyEvent)
			if err != nil {
				log.Fatal("error writing to socket", zap.Error(err))
			}
		}
	} else {
		// We'll connect common OS signals to a context to cleanly shutdown even if we're waiting to send an event:
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
		defer cancel()

		ticker := time.NewTicker(*frequency)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Info("graceful shutdown requested")
				return
			case <-ticker.C:
				_, err := conn.Write(dummyEvent)
				if err != nil {
					log.Fatal("error writing to socket", zap.Error(err))
				}
			}
		}
	}
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
