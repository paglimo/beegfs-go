package eventlog

import (
	"context"
	"errors"
	"net"
	"os"
	"path"
	"reflect"
	"sync"

	pb "git.beegfs.io/beeflex/bee-watch/api/proto/v1"
	"go.uber.org/zap"
)

type MetaSocket struct {
	ctx        context.Context
	socket     net.Listener
	log        *zap.Logger
	socketPath string
	// Allocating a new buffer for every event has an immense impact on performance.
	// So we allocate it once and reuse it.
	//
	buffer []byte
}

// Create returns a Unix socket where the BeeGFS metadata service can send events.
// To avoid leaking resources, ListenAndServe MUST be called immediately after the socket is created.
func New(ctx context.Context, log *zap.Logger, socketPath string) (*MetaSocket, error) {

	// Cleanup old socket if needed:
	stat, err := os.Stat(socketPath)
	if err == nil {
		if stat.IsDir() {
			return nil, errors.New("the provided Unix socket path for sysFileEventLogTarget was an existing directory, but must be a new or existing file path")
		}
		if err = os.Remove(socketPath); err != nil {
			return nil, err
		}

	}

	socket, err := net.Listen("unixpacket", socketPath)

	if err != nil {
		return nil, err
	}

	log = log.With(zap.String("component", path.Base(reflect.TypeOf(MetaSocket{}).PkgPath())))

	return &MetaSocket{
		ctx:        ctx,
		socket:     socket,
		log:        log,
		socketPath: socketPath,
		buffer:     make([]byte, 65536),
	}, nil
}

// ListenAndServe should be called against an valid MetaSocket created using New().
// It requires a pointer to a WaitGroup that will be marked done when it exits.
//
// When it starts it will wait until it receives a connection.
// Once a connection is accepted it will read any bytes send over the connection and
// attempt to deserialize BeeGFS packets before sending them to any subscribers.
//
// If there is a problem accepting a connection it will continue trying to accept new connections.
// If there is an error reading from a connection it will be closed and a new connection will be required.
// If it receives a bad packet (length doesn't match bytes read) it will warn and still save the packet.
//
// When the context on the BeeGFSSocket is cancelled, ListenAndServe will attempt to shutdown cleanly.
// If it is currently reading/serializing a packet, the packet will be saved before the connection is closed.
// After exiting it will close the socket and attempt to delete the socket file.
func (b *MetaSocket) ListenAndServe(wg *sync.WaitGroup, metaEventBuffer chan<- *pb.Event) {

	defer wg.Done()
	defer b.socket.Close()

	// TODO: https://linear.app/thinkparq/issue/BF-43/add-support-for-new-metadata-fields-and-event-types-to-beewatch
	// This is not implemented yet in the meta service, so for now we'll have BeeWatch generate sequence IDs.
	// Remove once the BeeGFS metadata service starts sending us sequence IDs.
	var seqId uint64 = 0

	// Clean up socket path when exiting.
	defer func() {
		err := os.Remove(b.socketPath)
		if err != nil {
			b.log.Warn("unable to clean up metadata socket", zap.String("path", b.socketPath))
		}
	}()

	connections := make(chan net.Conn)
	eventStream := make(chan *pb.Event)

	for {
		go b.acceptConnection(connections)
		select {
		case <-b.ctx.Done():
			b.log.Info("attempting to shutdown metadata connection")
			return
		case conn := <-connections:

			if conn == nil {
				// Right now we don't do any complex error handling if we couldn't accept a connection.
				// The error has already been logged so just keep trying to accept connections.
				continue
			}

			go b.readConnection(conn, eventStream)

		readLoop:
			for {
				select {
				case <-b.ctx.Done():
					b.log.Info("attempting to close active metadata connection and shutdown")
					err := conn.Close()
					if err != nil {
						b.log.Error("unable to close metadata connection", zap.Error(err))
					}
					return
				case event := <-eventStream:
					if event == nil {
						// Something went wrong reading the packet. Possibly the connection is broken.
						// Lets try to close it and reconnect.
						err := conn.Close()
						if err != nil {
							b.log.Error("unable to close metadata connection", zap.Error(err))
						}
						break readLoop
					}
					// TODO: https://linear.app/thinkparq/issue/BF-43/add-support-for-new-metadata-fields-and-event-types-to-beewatch
					// This is not implemented yet in the meta service, so for now we'll have BeeWatch generate sequence IDs.
					// Remove once the BeeGFS metadata service starts sending us sequence IDs.
					seqId++
					event.SeqId = seqId
					metaEventBuffer <- event
				}
			}
		}
	}

}

// acceptConnection will block until it receives a connection.
// When it establishes a connection it will be returned on the provided channel.
// If there is an error accepting a connection it will return a nil connection for upstream handling.
func (b MetaSocket) acceptConnection(connections chan<- net.Conn) {

	b.log.Info("waiting for metadata connection")
	c, err := b.socket.Accept()

	if err != nil {
		// Handle if we're gracefully shutting down and the socket was closed.
		if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "use of closed network connection" {
			return
		}
		b.log.Error("failed to accept metadata connection", zap.Error(err))
		connections <- nil
		return
	}
	b.log.Info("established metadata connection")
	connections <- c
}

// readConnection will block until it receives a packet on the provided connection.
// When it reads a packet it will be deserialized and returned on the provided channel.
// If there is an error reading from the connection it will return a nil packet for upstream handling.
func (b MetaSocket) readConnection(conn net.Conn, eventStream chan<- *pb.Event) {
	// Right now the meta service establishes and sends events over a single connection.
	// It expects that connection will remain active indefinitely and will indicate "broken pipe" otherwise.

	for {
		bytesRead, err := conn.Read(b.buffer)
		if err != nil {
			// Handle if we're gracefully shutting down and the socket was closed.
			if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "use of closed network connection" {
				return
			}
			b.log.Error("error reading from metadata connection", zap.Error(err))
			eventStream <- nil
			return
		}

		event, err := deserialize(b.buffer, bytesRead)

		if err != nil {
			// Probably we received a malformed event packet. There really isn't much we can do here other than warn.
			// Hopefully this would only come up in development when network protocols and packet versions may be in flux.
			// TODO - Consider if we should do something besides warn here (panic or return a nil packet to reset the connection?)
			b.log.Warn("unable to correctly deserialize packet due to an error (ignoring)", zap.Error(err))
		} else if bytesRead < int(event.Size) {
			// In "theory" this shouldn't happen.
			// If the connection broke while we were reading from it, we should get an error earlier.
			// Likely If this happens the BeeGFS meta service is sending us malformed event packets.
			// TODO - Consider if we should do something besides warn here (panic or return a nil packet to reset the connection?)
			b.log.Warn("received a packet that is smaller than the expected packet size (ignoring)", zap.Uint32("expected size", event.Size), zap.Int("actual size", bytesRead))
		}

		eventStream <- event
	}
}
