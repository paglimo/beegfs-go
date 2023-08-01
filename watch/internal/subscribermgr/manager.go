/*
Package subscribermgr provides functionality for managing multiple subscribers.

It accepts a JSON configuration with one or more subscribers, then creates a handler for each subscriber.
It also adds cursors to the metaEventBuffer so each subscriber has their own unique window into the shared ring buffer space.
When the app is shutting down is also coordinates shutting down all handlers.
*/
package subscribermgr

import (
	"context"
	"fmt"
	"path"
	"reflect"
	"sync"

	"git.beegfs.io/beeflex/bee-watch/internal/subscriber"
	"git.beegfs.io/beeflex/bee-watch/internal/types"
	"go.uber.org/zap"
)

type Manager struct {
	log             *zap.Logger
	handlers        []*Handler
	metaEventBuffer *types.MultiCursorRingBuffer
	wg              *sync.WaitGroup
}

func New(log *zap.Logger, metaEventBuffer *types.MultiCursorRingBuffer, wg *sync.WaitGroup) *Manager {

	log = log.With(zap.String("component", path.Base(reflect.TypeOf(Manager{}).PkgPath())))
	return &Manager{
		log:             log,
		handlers:        make([]*Handler, 0),
		metaEventBuffer: metaEventBuffer,
		wg:              wg,
	}
}

// UpdateConfiguration is intended to be used with ConfigMgr. It accepts a
// variadic parameter that must contain the desired handler configuration and a
// slice of subscribers to configure. This configuration should contain all
// subscribers including any changes to existing ones. Any subscribers found in
// the old configuration but not in the new will be removed.
func (sm *Manager) UpdateConfiguration(configs ...any) error {

	if len(configs) != 2 {
		return fmt.Errorf("invalid configuration provided (expected only handler and subscriber configuration)")
	}

	handlerConfig, ok1 := configs[0].(HandlerConfig)
	subscribersConfig, ok2 := configs[1].([]subscriber.Config)

	if !ok1 || !ok2 {
		return fmt.Errorf("invalid configuration provided (expected both handler and subscriber configuration)")
	}

	// TODO: https://linear.app/thinkparq/issue/BF-46/allow-configuration-updates-without-restarting-the-app
	// Consider if we want to do this better.
	// Fow now this is a fairly rudimentary way of updating subscribers while I flush out the rest of the implementation.
	// We'll just stop all subscribers, make the updates, then restart all subscribers.
	// Maybe this is "good enough", but we could be more deliberate in how we move from the old->new config.
	// Current issues include:
	// * We will always delete the metaEventBuffer cursors which will reset what events were ack'd/sent likely causing dropped events even if we just update a subscriber.
	// * We will always overwrite the last state of the subscriber, for example if it was unable to disconnect and the state is disconnecting.
	// It is possible a configuration change is needed to correct the state of the subscriber.
	//
	// For example we could do something like:
	// * Pause Run() before making any changes.
	// * Evaluate how to get from the oldConfig to the NewConfig:
	//   * If a subscriber exists in the old config but not the new one, stop it.
	//   * If a subscriber doesn't exist in the old config but does in the new config start it after adding it.
	// Note: There is no need to restart changed subscribers as they will automatically pickup
	// the new configuration (either when a new event comes, or they try to resend an existing event).
	//
	// PS: Be careful not to over engineer the final approach.

	newSubscribers, err := subscriber.NewSubscribersFromConfig(subscribersConfig)
	if err != nil {
		return err
	}

	// Don't stop old subscribers until we're sure the new configuration is valid:
	for _, h := range sm.handlers {
		h.Stop()
	}

	// We'll always recreate all handlers to ensure the configuration is updated.
	// However we only want to remove cursors when subscribers are removed.
	// This ensures we don't drop events while updating subscriber config.
	// We don't worry about adding cursors here because that happens in the handler.
	_, toRemove := evaluateAddedAndRemovedSubscribers(sm.handlers, subscribersConfig)
	for _, id := range toRemove {
		sm.metaEventBuffer.RemoveCursor(id)
	}

	var newHandlers []*Handler
	for _, s := range newSubscribers {
		newHandlers = append(newHandlers, newHandler(sm.log, s, sm.metaEventBuffer, handlerConfig))
	}

	sm.handlers = newHandlers
	for _, h := range sm.handlers {
		go h.Handle(sm.wg)
	}

	return nil
}

// evaluateAddedAndRemovedSubscribers takes a slice of current subscriber
// handlers and a slice of new subscriber configuration. It returns a slice of
// IDs that need new handlers, and a slice of IDs that no longer need handlers.
func evaluateAddedAndRemovedSubscribers(current []*Handler, new []subscriber.Config) (toAdd []int, toRemove []int) {

	currentMap := make(map[int]bool)
	newMap := make(map[int]bool)

	for _, c := range current {
		currentMap[c.ID] = true
	}

	for _, n := range new {
		if !currentMap[n.ID] {
			toAdd = append(toAdd, n.ID)
		}
		newMap[n.ID] = true
	}

	for _, c := range current {
		if !newMap[c.ID] {
			toRemove = append(toRemove, c.ID)
		}
	}

	return toAdd, toRemove
}

// Manage handles shutting down all subscribers when the app is shutting down.
func (sm *Manager) Manage(ctx context.Context, wg *sync.WaitGroup) {

	defer wg.Done()

	<-ctx.Done()
	sm.log.Info("shutting down subscriber handlers because the app is shutting down")
	for _, h := range sm.handlers {
		h.Stop()
	}

}
