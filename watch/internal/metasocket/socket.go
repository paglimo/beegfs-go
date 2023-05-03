package metasocket

import (
	"context"
	"net"
	"os"
	"path"
	"reflect"
	"sync"

	"git.beegfs.io/beeflex/bee-watch/internal/types"
	"go.uber.org/zap"
)

type MetaSocket struct {
	ctx        context.Context
	socket     net.Listener
	log        *zap.Logger
	socketPath string
}

// Create returns a Unix socket where the BeeGFS metadata service can send events.
// To avoid leaking resources, ListenAndServe MUST be called immediately after the socket is created.
func New(ctx context.Context, log *zap.Logger, socketPath string) (*MetaSocket, error) {

	// Cleanup old socket if needed:
	stat, err := os.Stat(socketPath)
	if err == nil {
		if stat.IsDir() {
			os.Exit(1)
		}
		os.Remove(socketPath)
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
	}, nil
}

// ListenAndServe should be called against an valid BeeGFSSocket created using New().
// It requires a pointer to a WaitGroup that will be marked done when it exits.
//
// When it starts it will wait until it receives a connection.
// Once a connection is accepted it will read any bytes send over the connection and
// attempt to deserialize BeeGFS packets before handing them off to persistent storage.
//
// If there is a problem accepting a connection it will continue trying to accept new connections.
// If there is an error reading from a connection it will be closed and a new connection will be required.
// If it receives a bad packet (length doesn't match bytes read) it will warn and still save the packet.
//
// When the context on the BeeGFSSocket is cancelled, ListenAndServe will attempt to shutdown cleanly.
// If it is currently reading/serializing a packet, the packet will be saved before the connection is closed.
// After exiting it will close the socket and attempt to delete the socket file.
func (b *MetaSocket) ListenAndServe(wg *sync.WaitGroup) {

	defer wg.Done()
	defer b.socket.Close()

	// Clean up socket path when exiting.
	defer func() {
		err := os.Remove(b.socketPath)
		if err != nil {
			b.log.Warn("unable to clean up socket", zap.String("path", b.socketPath))
		}
	}()

	connections := make(chan net.Conn)
	packets := make(chan *types.Packet)

	for {
		go b.acceptConnection(connections)
		select {
		case <-b.ctx.Done():
			b.log.Info("attempting to shutdown")
			return
		case conn := <-connections:

			if conn == nil {
				// Right now we don't do any complex error handling if we couldn't accept a connection.
				// The error has already been logged so just keep trying to accept connections.
				continue
			}

			for {
				go b.readConnection(conn, packets)
				select {
				case <-b.ctx.Done():
					b.log.Info("attempting to close active connection and shutdown")
					err := conn.Close()
					if err != nil {
						b.log.Error("unable to close connection", zap.Error(err))
					}
					return
				case packet := <-packets:
					if packet == nil {
						// Something went wrong reading the packet. Possibly the connection is broken.
						// Lets try to close it and reconnect.
						err := conn.Close()
						if err != nil {
							b.log.Error("unable to close connection", zap.Error(err))
						}
						break
					}
					// TODO: Save the packet to persistent storage.
					b.log.Info("Event: %s", zap.Any("event", packet))
				}
			}
		}
	}

}

// acceptConnection will block until it receives a connection.
// When it establishes a connection it will be returned on the provided channel.
// If there is an error accepting a connection it will return a nil connection for upstream handling.
func (b MetaSocket) acceptConnection(connections chan<- net.Conn) {

	b.log.Info("waiting for connection")
	c, err := b.socket.Accept()

	if err != nil {
		// Handle if we're gracefully shutting down and the socket was closed.
		if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "use of closed network connection" {
			return
		}
		b.log.Error("failed to accept connection", zap.Error(err))
		connections <- nil
		return
	}
	b.log.Info("established connection")
	connections <- c
}

// readConnection will block until it receives a packet on the provided connection.
// When it reads a packet it will be deserialized and returned on the provided channel.
// If there is an error reading from the connection it will return a nill packet for upstream handling.
func (b MetaSocket) readConnection(conn net.Conn, packets chan<- *types.Packet) {
	// Right now the meta service establishes and sends events over a single connection.
	// It expects that connection will remain active indefinitely and will indicate "broken pipe" otherwise.

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
		// Handle if we're gracefully shutting down and the socket was closed.
		if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "use of closed network connection" {
			return
		}
		b.log.Error("error reading from connection", zap.Error(err))
		packets <- nil
	}

	var p types.Packet
	p.Deserialize(buf)
	if bytesRead < int(p.Size) {
		// In "theory" this shouldn't happen.
		// If the connection broke while we were reading from it, we should get an error earlier.
		// Likely If this happens the BeeGFS meta service is sending us malformed packets.
		// TODO - Consider if we should do something besides warn here (panic?)
		b.log.Warn("packet that is smaller than the expected packet size", zap.Uint32("expected size", p.Size), zap.Int("actual size", bytesRead))
	}

	packets <- &p
}
