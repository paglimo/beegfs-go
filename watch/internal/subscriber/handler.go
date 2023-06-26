package subscriber

import (
	"context"
	"math/rand"
	"time"

	pb "git.beegfs.io/beeflex/bee-watch/api/proto/v1"
	"git.beegfs.io/beeflex/bee-watch/internal/types"
	"go.uber.org/zap"
)

const (
	// TODO: Make this configurable.
	// If we cannot connect to a subscriber we'll try to reconnect with an exponential backoff.
	// This is the maximum time in seconds between reconnect attempts to avoid increasing the backoff forever.
	maxReconnectBackoff = 60
	// defaultInterruptedEventBufferSize is the number of events that can be buffered while this subscriber is disconnected.
	defaultInterruptedEventBufferSize = 976562
)

type Handler struct {
	// Interrupted events is a ring buffer used to store events if the connection with a subscriber is lost.
	// It should always be as large or larger than the queueSize to ensure the queue can be flushed to the buffer.
	interruptedEvents *types.EventRingBuffer
	queue             chan *pb.Event
	ctx               context.Context
	cancel            context.CancelFunc
	log               *zap.Logger
	*BaseSubscriber
}

func newHandler(log *zap.Logger, subscriber *BaseSubscriber) *Handler {
	log = log.With(zap.String("subscriberID", subscriber.id), zap.String("subscriberName", subscriber.name))

	ctx, cancel := context.WithCancel(context.Background())

	return &Handler{
		interruptedEvents: types.NewEventRingBuffer(defaultInterruptedEventBufferSize),
		queue:             make(chan *pb.Event, subscriber.queueSize),
		ctx:               ctx,
		cancel:            cancel,
		log:               log,
		BaseSubscriber:    subscriber,
	}
}

// Handles the connection with a particular Subscriber.
// It determines the next state a subscriber should transition to in response to external and internal factors.
// It is the only place that should update the state of the subscriber.
// TODO: Consider if we should add a mutex to handle to ensure only one instances can run at a time.
func (h *Handler) Handle() {

	for {
		select {
		case <-h.ctx.Done():
			h.log.Info("shutting down subscriber")
			// At this point we should already be disconnected, or disconnecting if there was an error.
			// In the future we may want to consider a mechanism if we were unable to disconnect, to retry a few times.
			// If the app is shutting down those resources should be cleaned up.
			// For now we'll just go ahead and shutdown.
			return
		default:

			// We look at the result of the last loop to tell us what needs to happen next.
			// If we're disconnected we should connect.
			// If we're connected we should start handling the connection.
			// Otherwise we presume we need to disconnect for some reason.

			if state := h.GetState(); state == STATE_DISCONNECTED {
				h.setState(STATE_CONNECTING)
				if h.connectLoop() {
					// If the subscriber disconnected for some reason there may be interrupted events
					// that need to be sent from while the subscriber was disconnected.
					// To ensure events are sent in order lets try to send them before entering the main connectedLoop()
					// First we need to set the status to draining to ensure Enqueue doesn't keep adding events:
					h.setState(STATE_DRAINING_IE)
					if h.drainInterruptedEvents() {
						h.setState(STATE_CONNECTED)
						h.connectedLoop()
						h.setState(STATE_DRAINING_Q)
						h.drainQueue()
					}
				}
			}

			// If the connection was lost for any reason, we should first disconnect before we reconnect or shutdown:
			h.setState(STATE_DISCONNECTING)
			if h.doDisconnect() {
				h.setState(STATE_DISCONNECTED)
			}
		}
	}
}

// doDisconnect() attempts to disconnect the subscriber.
// It only attempts once returning true on a clean disconnect and false otherwise.
// It subscribers disconnect method should be idempotent, so it can be called repeatedly.
// It should also return true if there is no active connection to this subscriber.
// It is up to the caller to determine how many times to recall doDisconnect() if it returns false.
// A new connection should not be attempted until doDisconnect() returns true.
func (h *Handler) doDisconnect() bool {
	h.log.Info("disconnecting subscriber")
	err := h.disconnect()
	if err != nil {
		h.log.Error("encountered one or more errors disconnecting subscriber (ignoring)", zap.Error(err))
		return false
	}

	return true
}

// connectLoop() attempts to connect to a subscriber.
// If the subscriber is not ready or there is an error it will attempt to reconnect with an exponential backoff.
// If it returns false there was an unrecoverable error and the caller should first call doDisconnect() before reconnecting.
func (h *Handler) connectLoop() bool {
	h.log.Info("connecting to subscriber")
	var reconnectBackOff float64 = 1
	for {
		select {
		case <-h.ctx.Done():
			h.log.Info("not attempting to connect because the subscriber is shutting down")
			return false
		case <-time.After(time.Second * time.Duration(reconnectBackOff)): // We use this instead of time.Ticker so we can change the duration.
			retry, err := h.connect()
			if err != nil {
				if !retry {
					h.log.Error("unable to connect to subscriber (unable to retry)", zap.Error(err))
					return false
				}

				// We'll retry to connect with an exponential back off. We'll add some jitter to avoid load spikes.
				reconnectBackOff *= 2 + rand.Float64()
				if reconnectBackOff > maxReconnectBackoff {
					reconnectBackOff = maxReconnectBackoff - rand.Float64()
				}

				h.log.Error("unable to connect to subscriber (retrying)", zap.Error(err), zap.Any("retry_in_seconds", reconnectBackOff))
				continue
			}

			h.log.Info("connected to subscriber")
			return true
		}
	}
}

func (h *Handler) drainInterruptedEvents() bool {

	h.log.Info("sending interrupted events to subscriber")

	for !h.interruptedEvents.IsEmpty() {
		// We don't want to remove the event from the buffer until we're sure it was sent successfully:
		err := h.send(h.interruptedEvents.Peek())
		if err != nil {
			h.log.Error("unable to send interrupted event to subscriber", zap.Error(err), zap.Any("event_seq", h.interruptedEvents.Peek().SeqId))
			// We hit an error so all we can do is try to reconnect again.
			return false

		} else {
			// Otherwise if the event was sent lets pop it from the event buffer.
			event := h.interruptedEvents.Pop()
			h.log.Debug("sent interrupted event to subscriber", zap.Any("event", event.SeqId))
		}
	}
	return true
}

// connectedLoop() handles sending events and receiving responses from the subscriber.
// It will do this until the connection breaks for any reason (gracefully or otherwise).
// Once it returns the connection must be disconnected and reconnected before connectedLoop() is called again.
// It does not return an error because the caller should react the same in all scenarios.
func (h *Handler) connectedLoop() {
	// Start listening for responses from the subscriber:
	recvStream := h.receive()

	h.log.Info("beginning regular bidirectional event stream")

	// Only used for debugging:
	var lastEvent = &pb.Event{SeqId: 0}

	for {
		select {
		case <-h.ctx.Done():
			// The context should only be cancelled if something local requested a disconnect.
			h.log.Info("local disconnect requested")
			return
		case event := <-h.queue:
			if h.log.Level() == zap.DebugLevel {
				lastEvent = event
				h.log.Debug("sending event", zap.Any("event", event.SeqId))
			}
			if err := h.send(event); err != nil {
				h.log.Error("unable to send event", zap.Error(err), zap.Any("event", event.SeqId))
				// We don't know if the event was sent successfully or not so lets mark it interrupted.
				// Subscribers are expected to handle duplicate events so we'll err on the side of caution.
				h.interruptedEvents.Push(event)
				return
			}
		case response, ok := <-recvStream:
			if !ok {
				// Note an error or a legitimate remote disconnect could result in a REMOTE_DISCONNECT.
				h.log.Debug("disconnect while sending event", zap.Any("lastEvent", lastEvent.SeqId))
				h.log.Info("remote disconnect received")
				return
			}
			h.log.Debug("received response from subscriber", zap.Any("response", response))
			// TODO: https://linear.app/thinkparq/issue/BF-29/acknowledge-events-sent-to-all-subscribers-back-to-the-metadata-server
			// Also consider if we need to better handle what we do with recvStream when we break out of the connectedLoop.
			// Probably nothing because there is no expectation subscribers ack every event back to BeeWatch, or BW ack every event to meta.
			// If we're able to reconnect then we'll start reading the recvStream again.
			// If we're shutting down it doesn't matter since BeeWatch doesn't store any state on-disk.
			// Whatever ack'h events back to meta will need to handle if a subscriber is removed, knowing to disregard events it hasn't ack'd.
		}
	}
}

// drainQueue() drains all events in the queue to the interrupted events buffer.
func (h *Handler) drainQueue() {
	h.log.Info("draining queue to the interrupted events buffer")
	for {
		select {
		case event := <-h.queue:
			h.log.Debug("draining event", zap.Any("event", event.SeqId))
			h.interruptedEvents.Push(event)
		default:
			return
		}
	}
}

// Enqueue is called to add an event to the send queue for a particular subscriber.
// If the subscriber is connected the event will be sent to a buffered channel.
//
//	The use of a buffered channel allows us to handle bursts in events without blocking.
//	If we get to far behind sending events to this subscriber it will block.
//	This is an intentional design decision to avoid dropping events for a connected subscriber.
//	The idea is to create an artificial bottleneck that will limit the number of events BeeWatch buffers.
//	Thus reducing memory consumption by pushing the buffering back upstream to the metadata service.
//
// If the subscriber is not connected the event will be added to a ring buffer.
//
//	If the subscriber fails to reconnect before the ring buffer is full, some events may be dropped.
//	When a subscriber connects/reconnects we will temporarily block adding new events to it's queue.
//	This allows the handler to send "interrupted events" generated while the subscriber was disconnected.
//	It also ensures events are sent in the order they were generated by the metadata service.
func (h *Handler) Enqueue(event *pb.Event) {

	// This is thread safe because getting the status will block if it is currently being updated.
	state := h.GetState()

	if state == STATE_CONNECTED {
		h.queue <- event
		return
	} else if state != STATE_DRAINING_IE && state != STATE_DRAINING_Q {
		h.interruptedEvents.Push(event)
		return
	}

	// If we're in either draining state we need to block and wait here to ensure events don't get out of order.
	// What we do next with the event will depend on the new state:
	// * If the state transitions back to connected, add the event to the queue.
	// * Otherwise add the event to the interrupted events buffer.
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			h.log.Info("unable to enqueue event because the subscriber is shutting down")
			return
		case <-ticker.C:
			if newState := h.GetState(); newState != STATE_DRAINING_IE && newState != STATE_DRAINING_Q {
				if newState == STATE_CONNECTED {
					h.queue <- event
				} else {
					h.interruptedEvents.Push(event)
				}
				return
			}
		}
	}
}

// Stop is called to cancel the context associated with a particular handler.
// This will cause the Go routine handling the subscriber to attempt to cleanly disconnect.
func (h *Handler) Stop() {
	h.log.Info("stopping handler")
	h.cancel()
}
