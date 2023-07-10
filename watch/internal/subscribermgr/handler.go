package subscribermgr

import (
	"context"
	"math/rand"
	"time"

	"git.beegfs.io/beeflex/bee-watch/internal/subscriber"
	"git.beegfs.io/beeflex/bee-watch/internal/types"
	"go.uber.org/zap"
)

const (
	// TODO: Make this configurable.
	// If we cannot connect to a subscriber we'll try to reconnect with an exponential backoff.
	// This is the maximum time in seconds between reconnect attempts to avoid increasing the backoff forever.
	maxReconnectBackoff = 60
)

type Handler struct {
	ctx                     context.Context
	cancel                  context.CancelFunc
	log                     *zap.Logger
	metaEventBuffer         *types.MultiCursorRingBuffer
	metaBufferPollFrequency int
	*subscriber.BaseSubscriber
}

func newHandler(log *zap.Logger, subscriber *subscriber.BaseSubscriber, metaEventBuffer *types.MultiCursorRingBuffer, metaBufferPollFrequency int) *Handler {
	log = log.With(zap.Int("subscriberID", subscriber.Id), zap.String("subscriberName", subscriber.Name))

	ctx, cancel := context.WithCancel(context.Background())

	return &Handler{
		ctx:                     ctx,
		cancel:                  cancel,
		log:                     log,
		metaEventBuffer:         metaEventBuffer,
		metaBufferPollFrequency: metaBufferPollFrequency,
		BaseSubscriber:          subscriber,
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
					h.SetState(subscriber.CONNECTED)

					// We need to start listening for responses from the subscriber before we do anything.
					// Some subscribers may block on receiving new events until they can send responses.
					// Some subscribers may also tell us the last event they successfully received so we can avoid sending duplicate events.
					doneReceiving, cancelReceive := h.receiveLoop()

					// Start sending events to this subscriber:
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
					h.SetState(subscriber.DISCONNECTING)
				}
			}

			// If the connection was lost for any reason, we should first disconnect before we reconnect or shutdown:
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
	// When we connect, some subscribers may acknowledge the last event they successfully received before a disconnect.
	// TODO: Consider if the time we wait should be configurable based per subscriber.
	select {
	case response, ok := <-recvStream:
		if ok {
			// Note an error or a legitimate remote disconnect could result in a REMOTE_DISCONNECT.
			h.log.Info("subscriber acknowledged last event received before disconnect", zap.Any("sequenceID", response.CompletedSeq))
			h.metaEventBuffer.AckEvent(h.Id, response.CompletedSeq)
		}
		// TODO: Test what happens if we have a REMOTE_DISCONNECT here. Are we safe to just ignore it? It would be better to explicitly handle.
	case <-time.After(2 * time.Second):
		h.log.Info("subscriber did not acknowledge last event received before disconnect, resending events from the last acknowledged event")
	}

	// Move the send cursor back to the last acknowledged event.
	// This may result in duplicate events being sent if the subscriber didn't tell us the last event they received.
	h.metaEventBuffer.ResetSendCursor(h.Id)

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
				h.metaEventBuffer.AckEvent(h.Id, response.CompletedSeq)

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
			case <-time.After(time.Duration(h.metaBufferPollFrequency) * time.Second):
				// Poll on a configurable period for new events to be added to the buffer.
			sendEvents:
				for {
					event, err := h.metaEventBuffer.GetEvent(h.Id)
					if err != nil {
						h.log.Error("unable to get the next event in the buffer", zap.Error(err))
						break sendEvents
					}

					// Continue sending events until there are no more events to send:
					if event != nil {
						if err := h.Send(event); err != nil {
							h.log.Error("unable to send event", zap.Error(err), zap.Any("event", event.SeqId))
							return
						}
					} else {
						break sendEvents
					}
				}
			}
		}
	}()
	return done, cancel
}

// Stop is called to cancel the context associated with a particular handler.
// This will cause the Go routine handling the subscriber to attempt to cleanly disconnect.
func (h *Handler) Stop() {
	h.log.Info("stopping handler")
	h.cancel()
}
