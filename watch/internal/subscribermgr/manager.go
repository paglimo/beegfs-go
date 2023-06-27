package subscribermgr

import (
	"context"
	"path"
	"reflect"
	"sync"

	pb "git.beegfs.io/beeflex/bee-watch/api/proto/v1"
	"git.beegfs.io/beeflex/bee-watch/internal/subscriber"
	"go.uber.org/zap"
)

type Manager struct {
	log      *zap.Logger
	handlers []*Handler
}

func NewManager(log *zap.Logger) Manager {

	log = log.With(zap.String("component", path.Base(reflect.TypeOf(Manager{}).PkgPath())))
	return Manager{
		log:      log,
		handlers: make([]*Handler, 0),
	}
}

// Update subscribers takes a string containing JSON configuration specifying a list of subscribers to manage.
// This is the external mechanism external functions should call to dynamically add/update/remove subscribers.
// This configuration should contain all subscribers including any changes to existing ones.
// Any subscribers found in the old configuration but not in the new will be removed.
func (sm *Manager) UpdateConfiguration(jsonConfig string) error {

	// TODO: https://linear.app/thinkparq/issue/BF-46/allow-configuration-updates-without-restarting-the-app
	// Consider if we want to do this better.
	// Fow now this is a fairly rudimentary way of updating subscribers while I flush out the rest of the implementation.
	// We'll just stop all subscribers, make the updates, then restart all subscribers.
	// Maybe this is "good enough", but we could be more deliberate in how we move from the old->new config.
	// Likely issues include:
	// * ensuring we don't loose any events that are in the subscriber queues but not yet sent.
	// * not overwriting the last state of the subscriber, for example if it was unable to disconnect and the state is disconnecting.
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

	for _, h := range sm.handlers {
		h.Stop()
	}

	newSubscribers, err := subscriber.NewSubscribersFromJson(jsonConfig)
	if err != nil {
		return err
	}

	var newHandlers []*Handler
	for _, s := range newSubscribers {
		newHandlers = append(newHandlers, newHandler(sm.log, s))
	}

	sm.handlers = newHandlers
	for _, h := range sm.handlers {
		go h.Handle()
	}

	return nil
}

// Manage watches for new events and adds them to the queue for each subscriber.
// It also handles shutting down all subscribers when the app is shutting down.
func (sm *Manager) Manage(ctx context.Context, wg *sync.WaitGroup, eventBuffer <-chan *pb.Event) {

	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			sm.log.Info("shutting down subscriber handlers")
			for _, h := range sm.handlers {
				h.Stop()
			}
			return
		case event := <-eventBuffer:
			for _, h := range sm.handlers {
				h.Enqueue(event)
			}
		}

	}
}
