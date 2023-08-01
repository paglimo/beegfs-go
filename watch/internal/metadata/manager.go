// Package metadata reads from the event log published by the BeeGFS metadata service over a
// unix socket and provides methods for deserializing file system modification event packets.
package metadata

import (
	"context"
	"errors"
	"net"
	"os"
	"path"
	"reflect"
	"sync"
	"time"

	"git.beegfs.io/beeflex/bee-watch/internal/types"
	"go.uber.org/zap"
)

// The metadata.Manager manages a Unix socket where the BeeGFS metadata service
// can send events. It handles deserializing the events and writing them to a
// shared EventBuffer that other components can read from.
type Manager struct {
	// Shared EventBuffer where events are sent once they are deserialized.
	// Only the metadata manager should write to to the EventBuffer.
	// It is expected other components in the application may read from this
	// buffer by registering their own subscriber cursors.
	EventBuffer *types.MultiCursorRingBuffer
	ctx         context.Context
	log         *zap.Logger
	// This is the path where a socket will be created where the metadata server can send events.
	socketPath string
	socket     net.Listener
	// Allocating a new buffer for every event has an immense impact on performance.
	// So we allocate a buffer once and reuse it.
	buffer []byte
	// Sequence IDs are not implemented yet in the meta service, so for now we'll have BeeWatch generate sequence IDs.
	// TODO: https://linear.app/thinkparq/issue/BF-43/add-support-for-new-metadata-fields-and-event-types-to-beewatch
	// Remove once the BeeGFS metadata service starts sending us sequence IDs.
	seqId uint64
}

// Config defines the configuration for a metadata.Manager.
// It is currently only used with New() as the Manager does
// not support dynamic configuration updates at this time.
// If it ever does it should also be embedded in the manager.
type Config struct {
	EventLogTarget         string `mapstructure:"eventLogTarget"`
	EventBufferSize        int    `mapstructure:"eventBufferSize"`
	EventBufferGCFrequency int    `mapstructure:"eventBufferGCFrequency"`
}

// New creates a new Metadata manager that handles reading events from the
// specified EventLogTarget and publishing them to an EventBuffer.
// It is the callers responsibility to call the returned cleanup function to
// avoid leaking resources (typically using a defer statement).
// This allows us to initialize the metadata socket and buffer which are then
// used to initialize other components before we actually start receiving
// events from the metadata service. This helps avoid dropped events in case
// some other component was misconfigured.
func New(ctx context.Context, log *zap.Logger, config Config) (*Manager, func(), error) {

	// Cleanup old socket if needed:
	stat, err := os.Stat(config.EventLogTarget)
	if err == nil {
		if stat.IsDir() {
			return nil, nil, errors.New("the provided Unix socket path for sysFileEventLogTarget was an existing directory, but must be a new or existing file path")
		}
		if err = os.Remove(config.EventLogTarget); err != nil {
			return nil, nil, err
		}

	}

	socket, err := net.Listen("unixpacket", config.EventLogTarget)

	if err != nil {
		return nil, nil, err
	}

	log = log.With(zap.String("component", path.Base(reflect.TypeOf(Manager{}).PkgPath())))

	cleanup := func() {
		// This also handles deleting the file.
		err := socket.Close()
		if err != nil {
			log.Error("unable to close metadata socket", zap.Error(err))
		}
	}

	return &Manager{
		ctx:         ctx,
		socket:      socket,
		log:         log,
		socketPath:  config.EventLogTarget,
		EventBuffer: types.NewMultiCursorRingBuffer(config.EventBufferSize, config.EventBufferGCFrequency),
		buffer:      make([]byte, 65536),
		seqId:       0,
	}, cleanup, nil
}

// Manage starts listening for events from a metadata service and publishing them to the EventBuffer.
// It requires a WaitGroup that will be marked done when it exits.
//
// When it starts it will wait until it receives a connection.
// Once a connection is accepted it will read any bytes send over the connection and
// attempt to deserialize BeeGFS packets before sending them to any subscribers.
//
// If there is a problem accepting a connection it will continue trying to accept new connections.
// If there is an error reading from a connection it will close the connection and wait for a new one.
// If it receives a bad packet (length doesn't match bytes read) it will warn and just send the packet header.
//
// When Managers context is cancelled, Manage will attempt to shutdown cleanly.
// If it is currently reading/serializing a packet, the packet will be saved before the connection is closed.
// IMPORTANT: The caller must use the cleanup() function returned by New() to ensure resources are cleaned up.
// Manage handles shutting down the connection, but not cleaning up the socket.
func (m *Manager) Manage(wg *sync.WaitGroup) {

	// Defers are executed in LIFO order.
	// We want to ensure we get a chance to cleanup BEFORE we mark the wg as done.
	defer wg.Done()

	connections := make(chan net.Conn)

	for {
		go m.acceptConnection(connections)
		select {
		case <-m.ctx.Done():
			m.log.Info("no longer accepting new metadata connections because the app is shutting down")
			return
		case conn := <-connections:

			if conn == nil {
				// Right now we don't do any complex error handling if we couldn't accept a connection.
				// The error has already been logged so just keep trying to accept connections.
				continue
			}

			// Start a separate goroutine that reads from the connection.
			// It can cancel the context to request the connection be closed if anything goes wrong.
			connCtx, cancelConn := context.WithCancel(context.Background())
			// connMutex coordinates reading and closing the connection.
			// This is because we use one goroutine to wait for app shutdown and close the connection,
			// and another to actually read from the connection. This ensures we don't block shutdown when no events are ready.
			// It also ensures we are able to finish reading the last event and publish it to the buffer before disconnecting.
			var connMutex sync.Mutex
			go m.readConnection(conn, &connMutex, cancelConn)

			select {
			case <-m.ctx.Done():
				m.log.Info("attempting to close active metadata connection because the app is shutting down")
				connMutex.Lock()
				err := conn.Close()
				connMutex.Unlock()
				if err != nil {
					m.log.Error("unable to close metadata connection", zap.Error(err))
				}
				return
			case <-connCtx.Done():
				// Something went wrong reading the packet. Possibly the connection is broken.
				// Lets try to close it and reconnect.
				m.log.Info("attempting to close active metadata connection and reconnect")
				err := conn.Close()
				if err != nil {
					m.log.Error("unable to close metadata connection", zap.Error(err))
				}
			}
		}
	}

}

// acceptConnection will block until it receives a connection.
// When it establishes a connection it will be returned on the provided channel.
// If there is an error accepting a connection it will return a nil connection for upstream handling.
func (m *Manager) acceptConnection(connections chan<- net.Conn) {

	m.log.Info("waiting for metadata connection")
	c, err := m.socket.Accept()

	if err != nil {
		// Handle if we're gracefully shutting down and the socket was closed.
		if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "use of closed network connection" {
			return
		}
		m.log.Error("failed to accept metadata connection", zap.Error(err))
		connections <- nil
		return
	}
	m.log.Info("established metadata connection")
	connections <- c
}

// readConnection will read packets from the provided connection.
// When it reads a packet it will be deserialized and send to the metaEventBuffer.
// If there is an error reading from the connection it will return calling cancelCtx() for upstream handling.
func (m *Manager) readConnection(conn net.Conn, connMutex *sync.Mutex, cancelConn context.CancelFunc) {
	// Right now the meta service establishes and sends events over a single connection.
	// It expects that connection will remain active indefinitely and will indicate "broken pipe" otherwise.

	defer cancelConn()

	for {
		connMutex.Lock()
		bytesRead, err := conn.Read(m.buffer)
		connMutex.Unlock()
		if err != nil {
			// Handle if we're gracefully shutting down and the socket was closed.
			if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "use of closed network connection" {
				m.log.Debug("disconnected from metadata service")
				return
			}
			m.log.Error("error reading from metadata connection", zap.Error(err))
			return
		}

		event, err := deserialize(m.buffer, bytesRead)

		if err != nil {
			// Probably we received a malformed event packet. There really isn't much we can do here other than warn.
			// Hopefully this would only come up in development when network protocols and packet versions may be in flux.
			// TODO - Consider if we should do something besides warn here (panic or return a nil packet to reset the connection?)
			m.log.Warn("unable to correctly deserialize packet due to an error (ignoring)", zap.Error(err))
		} else if bytesRead < int(event.Size) {
			// In "theory" this shouldn't happen.
			// If the connection broke while we were reading from it, we should get an error earlier.
			// Likely If this happens the BeeGFS meta service is sending us malformed event packets.
			// TODO - Consider if we should do something besides warn here (panic or return a nil packet to reset the connection?)
			m.log.Warn("received a packet that is smaller than the expected packet size (ignoring)", zap.Uint32("expected size", event.Size), zap.Int("actual size", bytesRead))
		}

		// TODO: https://linear.app/thinkparq/issue/BF-43/add-support-for-new-metadata-fields-and-event-types-to-beewatch
		// This is not implemented yet in the meta service, so for now we'll have BeeWatch generate sequence IDs.
		// Remove once the BeeGFS metadata service starts sending us sequence IDs.
		m.seqId++
		event.SeqId = m.seqId
		m.EventBuffer.Push(event)

	}
}

func (m *Manager) Sample() {

	for {
		select {
		case <-m.ctx.Done():
			return
		default:
			// Since we're just reading we're not doing any locking.
			// This means our sampling may be slightly off.
			start := m.seqId
			startTime := time.Now()
			time.Sleep(time.Second)
			end := m.seqId
			endTime := time.Now()

			eventsReceived := end - start
			duration := endTime.Sub(startTime).Seconds()

			eventsPerSecond := float64(eventsReceived) / duration
			m.log.Info("incoming events per second", zap.Any("EPS", eventsPerSecond))
		}
	}
}
