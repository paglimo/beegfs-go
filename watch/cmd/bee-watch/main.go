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
	socketPath             = flag.String("socket", "/beegfs/meta_01_tgt_0101/socket/beegfs_eventlog", "The path to the BeeGFS event log socket")
	eventSubscriberAddress = flag.String("eventSubscriberInterface", "localhost:50052", "The host:port where a BeeWatch subscriber is listening.")
)

func main() {

	flag.Parse()

	config := zap.NewProductionConfig()
	config.InitialFields = map[string]interface{}{"serviceName": "bee-watch"}
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

	// Use a channel as a buffer to move events between threads:
	eventBuffer := make(chan *pb.Event)

	// Create a unix domain socket and listen for incoming connections:
	socket, err := eventlog.New(ctx, log, *socketPath)
	if err != nil {
		log.Fatal("failed to listen for unix packets on socket path", zap.Error(err), zap.String("socket", *socketPath))
	}
	wg.Add(1)
	go socket.ListenAndServe(&wg, eventBuffer) // Don't move this away from the creation to ensure the socket is cleaned up.

	// TODO: Rework this into a subscriber manager that can handle multiple subscribers.
	// This watches for new additions to the eventBuffer then sends them to subscribers.
	// It also handles removing events from the buffer once all subscribers have read them.
	wg.Add(1)
	err = subscriber.Connect(ctx, &wg, log, *eventSubscriberAddress, eventBuffer)
	if err != nil {
		log.Fatal("failed to connect to subscriber", zap.Error(err))
	}

	wg.Wait()
	log.Info("all components stopped, exiting")
}
