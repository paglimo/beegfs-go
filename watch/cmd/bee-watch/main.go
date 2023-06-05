package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"git.beegfs.io/beeflex/bee-watch/internal/metasocket"
	"go.uber.org/zap"
)

var (
	socketPath = flag.String("socket", "/beegfs/meta_01_tgt_0101/socket/beegfs_eventlog", "The path to the BeeGFS event log socket")
)

func main() {

	flag.Parse()

	config := zap.NewProductionConfig()
	config.InitialFields = map[string]interface{}{"serviceName": "BeeWatch"}
	log, err := config.Build()

	if err != nil {
		fmt.Println("Unable to initialize logger: ", err)
		os.Exit(1)
	}

	// We'll use a context to cleanly shutdown goroutines:
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	// We'll use a wait group to coordinate shutdown of all components:
	var wg sync.WaitGroup

	// Create a unix domain socket and listen for incoming connections:
	socket, err := metasocket.New(ctx, log, *socketPath)
	if err != nil {
		log.Fatal("failed to listen for unix packets on socket path", zap.Error(err), zap.String("socket", *socketPath))
	}
	wg.Add(1)
	go socket.ListenAndServe(&wg) // Don't move this away from the creation to ensure the socket is cleaned up.

	// TODO: Setup a subscriber manager.
	// This watches for updates to the backend and sends them to subscribers.
	// It also handles removing events from the disk buffer once all subscribers have read them.

	wg.Wait()
	log.Info("all components stopped, exiting")
}
