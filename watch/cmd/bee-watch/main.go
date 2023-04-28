package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"git.beegfs.io/beeflex/bee-watch/internal/types"
	"go.uber.org/zap"
)

var (
	socketPath = flag.String("socket", "/beegfs/meta_01_tgt_0101/socket/beegfs_eventlog", "The path to the BeeGFS event log socket")
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

	// TODO: Set a backend disk buffer.
	// Probably we just want to get rid of events and use a channel to send new events to the backend.

	// TODO: Setup a subscriber manager.
	// This watches for updates to the backend and sends them to subscribers.
	// It also handles removing events from the disk buffer once all subscribers have read them.

	// Cleanup old socket if needed:
	stat, err := os.Stat(*socketPath)
	if err == nil {
		if stat.IsDir() {
			os.Exit(1)
		}
		os.Remove(*socketPath)
	}

	// Create a unix domain socket and listen for incoming connections:
	socket, err := net.Listen("unixpacket", *socketPath)
	if err != nil {
		log.Fatal("failed to listen for unix packets on socket path", zap.Error(err), zap.String("socket", *socketPath))
	}
	defer socket.Close()

	// Clean up socket file on clean exit:
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		os.Remove(*socketPath)
		os.Exit(1)
	}()

	var events []types.Packet

	// Handle connections:
	for {

		log.Info("waiting for connection")
		conn, err := socket.Accept()
		if err != nil {
			log.Fatal("failed to accept connection", zap.Error(err))
		}
		defer conn.Close()
		log.Info("established connection")

		// Right now the meta service establishes and sends events over a single connection.
		// It expects that connection will remain active indefinitely and will indicate "broken pipe" otherwise.
		for {

			// TODO: Explore ways to optimize buffer allocation.
			// Buffer size is based on what is used in beegfs_file_event_log.hpp.
			// Presumably this is the max size a message could be given the max file path lengths in BeeGFS.
			// I thought we could maybe optimize by first reading the header (8 bytes) including the packet size,
			// then allocating an appropriately sized buffer to read the rest of the message.
			// However if we do this the meta will log a connection reset then resend the entire packet.
			// So we'd have to change the meta for this to work (and maybe Unix sockets aren't meant to be used like this).
			buf := make([]byte, 65536)
			bytesRead, err := conn.Read(buf)
			if err != nil {
				log.Error("error reading from connection", zap.Error(err))
				break
			}

			var packet types.Packet
			packet.Deserialize(buf)
			if bytesRead < int(packet.Size) {
				log.Error("expected packet size is smaller than the actual packet size", zap.Uint32("expected size", packet.Size), zap.Int("actual size", bytesRead))
				return
			}

			log.Info("Event: %s", zap.Any("event", packet))
			events = append(events, packet)
		}
		conn.Close()
	}
}
