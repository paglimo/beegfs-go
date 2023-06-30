package subscribermgr

import (
	"context"
	"math/rand"
	"time"

	pb "git.beegfs.io/beeflex/bee-watch/api/proto/v1"
	"git.beegfs.io/beeflex/bee-watch/internal/subscriber"
	"git.beegfs.io/beeflex/bee-watch/internal/types"
	"go.uber.org/zap"
)

const (
	// TODO: Make this configurable.
	// If we cannot connect to a subscriber we'll try to reconnect with an exponential backoff.
	// This is the maximum time in seconds between reconnect attempts to avoid increasing the backoff forever.
	maxReconnectBackoff = 60
	// TODO: Possibly we want the ability to define this on a per subscriber basis.
	defaultUnackEventsBufferSize = 4096
	defaultQueueSize             = 1048576
	defaultOfflineBufferSize     = 300000000
)

type Handler struct {
	// unacknowledgedEvents is a ring buffer used to store events immediately after they are sent.
	// They are kept in this buffer until the event is acknowledged by the subscriber,
	// or the subscriber disconnects for any reason, then they are added to the front of the
	// offlineEvents buffer and resent when the subscriber reconnects.
	// Depending how subscribers handle events and the network we don't really need a large buffer here:
	// * From a network perspective:
	//   This should be the maximum number of events that can be "on the wire" at any moment.
	//   So if our stream breaks this is at most the number of events that could be dropped.
	// * From a subscriber perspective:
	//   So long as they are handling events as they are received, even if they aren't acknowledging each event,
	//   this buffer size shouldn't matter. If they need us to hold on-to events for some amount of time,
	//   perhaps they're processing events in batches, this may need to be set to their "batch size",
	//   or the max number of events that can be received between them sending each acknowledgement.
	unacknowledgedEvents *types.EventRingBuffer
	// Offline events is a ring buffer used to store events while a subscriber is not connected.
	// The use of a ring buffer ensures we don't use infinite memory and drop older events for newer ones.
	offlineEvents *types.EventRingBuffer
	// The queue is where new events are published while the subscriber is connected.
	// If the subscriber is unable to process events faster than they are published to this queue,
	// the overall rate at which new events are accepted will be throttled by the subscriber manager.
	queue  chan *pb.Event
	ctx    context.Context
	cancel context.CancelFunc
	log    *zap.Logger
	*subscriber.BaseSubscriber
}

func newHandler(log *zap.Logger, subscriber *subscriber.BaseSubscriber) *Handler {
	log = log.With(zap.String("subscriberID", subscriber.Id), zap.String("subscriberName", subscriber.Name))

	ctx, cancel := context.WithCancel(context.Background())

	// Ensure the subscriber has provided us sane values:
	if subscriber.QueueSize == 0 {
		log.Info("using default queue size for subscriber")
		subscriber.QueueSize = defaultQueueSize
	}

	if subscriber.OfflineBufferSize == 0 {
		log.Info("using default offline buffer size for subscriber")
		subscriber.OfflineBufferSize = defaultOfflineBufferSize
	}

	if subscriber.OfflineBufferSize < subscriber.QueueSize+defaultUnackEventsBufferSize {
		log.Info("offline buffer size cannot be less than combined queue and unacknowledged buffer size, setting equal")
		subscriber.OfflineBufferSize = subscriber.QueueSize + defaultUnackEventsBufferSize
	}

	return &Handler{
		// TODO: This probably doesn't need to be as large as the offlineEvents buffer.
		unacknowledgedEvents: types.NewEventRingBuffer(defaultUnackEventsBufferSize),
		offlineEvents:        types.NewEventRingBuffer(subscriber.OfflineBufferSize),
		queue:                make(chan *pb.Event, subscriber.QueueSize),
		ctx:                  ctx,
		cancel:               cancel,
		log:                  log,
		BaseSubscriber:       subscriber,
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

			if state := h.GetState(); state == subscriber.DISCONNECTED {
				h.SetState(subscriber.CONNECTING)
				if h.connectLoop() {
					// We need to start listening for responses from the subscriber before we do anything.
					// Some subscribers may block on receiving new events until they can send responses.
					doneReceiving, cancelReceive := h.receiveLoop()
					// If the subscriber disconnected for some reason there may be events in the offline event buffer.
					// To ensure events are sent in order lets try to send them before starting the main sendLoop()
					// First we need to set the status to frozen to ensure Enqueue doesn't keep adding events to the buffer:
					h.SetState(subscriber.FROZEN)
					if h.drainOfflineEvents() {
						h.SetState(subscriber.CONNECTED)
						doneSending, cancelSend := h.sendLoop()
						// If either the receive or send goroutines are done, we should fully disconnect:
						select {
						case <-doneReceiving:
							cancelSend()
							<-doneSending
						case <-doneSending:
							cancelReceive()
							<-doneReceiving
						}
						h.SetState(subscriber.FROZEN)
						h.drainOutstandingEvents()
					} else {
						// Make sure we stop receiving if we can't drain offline events:
						cancelReceive()
						<-doneReceiving
					}
				}
			}

			// If the connection was lost for any reason, we should first disconnect before we reconnect or shutdown:
			h.SetState(subscriber.DISCONNECTING)
			if h.doDisconnect() {
				h.SetState(subscriber.DISCONNECTED)
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
	err := h.Disconnect()
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
			retry, err := h.Connect()
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

func (h *Handler) receiveLoop() (<-chan struct{}, context.CancelFunc) {

	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	recvStream := h.Receive()
	h.log.Info("starting to receive responses from subscriber")

	go func() {
		defer close(done)
		defer cancel()
		for {
			select {
			case <-h.ctx.Done():
				// The context should only be cancelled if something local requested a disconnect.
				h.log.Info("stopping receive loop because the handler is shutting down")
				return
			case <-ctx.Done():
				h.log.Info("stopping receive loop because the context was cancelled")
				return
			case response, ok := <-recvStream:
				if !ok {
					// Note an error or a legitimate remote disconnect could result in a REMOTE_DISCONNECT.
					h.log.Info("stopping receive loop due to a remote disconnect")
					return
				}
				//h.log.Debug("received response from subscriber", zap.Any("response", response))
				h.unacknowledgedEvents.RemoveUntil(response.CompletedSeq)

				// TODO: https://linear.app/thinkparq/issue/BF-29/acknowledge-events-sent-to-all-subscribers-back-to-the-metadata-server
				// Also consider if we need to better handle what we do with recvStream when we break out of the connectedLoop.
				// Probably nothing because there is no expectation subscribers ack every event back to BeeWatch, or BW ack every event to meta.
				// If we're able to reconnect then we'll start reading the recvStream again.
				// If we're shutting down it doesn't matter since BeeWatch doesn't store any state on-disk.
				// Whatever ack'h events back to meta will need to handle if a subscriber is removed, knowing to disregard events it hasn't ack'd.
			}
		}
	}()

	return done, cancel
}

func (h *Handler) drainOfflineEvents() bool {

	if !h.offlineEvents.IsEmpty() {
		h.log.Info("sending offline events to subscriber")

		for !h.offlineEvents.IsEmpty() {
			select {
			case <-h.ctx.Done():
				// The context should only be cancelled if something local requested a disconnect.
				h.log.Info("local disconnect requested while sending offline events to subscriber")
				return false
			default:
				// We don't want to remove the event from the buffer until we're sure it was sent successfully:
				err := h.Send(h.offlineEvents.Peek())
				if err != nil {
					h.log.Error("unable to send offline event to subscriber", zap.Error(err), zap.Any("event_seq", h.offlineEvents.Peek().SeqId))
					// We hit an error so all we can do is try to reconnect again.
					return false

				} else {
					// Otherwise if the event was sent lets pop it from the event buffer.
					h.offlineEvents.Pop()
					//h.log.Debug("sent offline event to subscriber", zap.Any("event", event.SeqId))
				}
			}
		}
	}
	return true
}

// connectedLoop() handles sending events and receiving responses from the subscriber.
// It will do this until the connection breaks for any reason (gracefully or otherwise).
// Once it returns the connection must be disconnected and reconnected before connectedLoop() is called again.
// It does not return an error because the caller should react the same in all scenarios.
func (h *Handler) sendLoop() (<-chan struct{}, context.CancelFunc) {

	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	h.log.Info("beginning normal event stream")

	go func() {
		defer close(done)
		defer cancel()
		for {
			select {
			case <-h.ctx.Done():
				// The context should only be cancelled if something local requested a disconnect.
				h.log.Info("stopping send loop because the handler is shutting down")
				return
			case <-ctx.Done():
				h.log.Info("stopping send loop because the context was cancelled")
				return
			case event := <-h.queue:
				//h.log.Debug("sending event", zap.Any("event", event.SeqId))
				// We don't know if the event was sent successfully or not so lets push it to the unacknowledgedEvents event buffer.
				// Subscribers are expected to handle duplicate events so we'll err on the side of caution.
				h.unacknowledgedEvents.Push(event)
				if err := h.Send(event); err != nil {
					h.log.Error("unable to send event", zap.Error(err), zap.Any("event", event.SeqId))
					return
				}
			}
		}
	}()
	return done, cancel
}

// drainOutstandingEvents() drains all unacknowledged events and events in the queue to the offline events buffer.
func (h *Handler) drainOutstandingEvents() {

	h.log.Info("draining unacknowledged events to the offline events buffer")

drainUnacknowledged:
	for {
		if event := h.unacknowledgedEvents.Pop(); event != nil {
			h.offlineEvents.Push(event)
			continue drainUnacknowledged
		}
		break drainUnacknowledged
	}

	h.log.Info("draining queue to the offline events buffer")
	for {
		select {
		case event := <-h.queue:
			//h.log.Debug("draining event", zap.Any("event", event.SeqId))
			h.offlineEvents.Push(event)
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
//	This allows the handler to send "offline events" generated while the subscriber was disconnected.
//	It also ensures events are sent in the order they were generated by the metadata service.
func (h *Handler) Enqueue(event *pb.Event) {

	// This is thread safe because getting the status will block if it is currently being updated.
	state := h.GetState()

	if state == subscriber.CONNECTED {
		h.queue <- event
		return
	} else if state != subscriber.FROZEN {
		h.offlineEvents.Push(event)
		return
	}

	// If the subscriber is frozen we need to block and wait here to ensure events don't get out of order.
	// What we do next with the event will depend on the new state:
	// * If the state transitions back to connected, add the event to the queue.
	// * Otherwise add the event to the offline events buffer.
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			h.log.Info("unable to enqueue event because the subscriber is shutting down")
			return
		case <-ticker.C:
			if newState := h.GetState(); newState != subscriber.FROZEN {
				if newState == subscriber.CONNECTED {
					h.queue <- event
				} else {
					h.offlineEvents.Push(event)
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
