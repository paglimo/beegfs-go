package subscriber

import (
	"context"
	"path"
	"reflect"
	"sync"

	pb "git.beegfs.io/beeflex/bee-watch/api/proto/v1"
	"go.uber.org/zap"
)

type SubscriberManager struct {
	log         *zap.Logger
	subscribers []Subscriber
}

func New(log *zap.Logger) SubscriberManager {

	log = log.With(zap.String("component", path.Base(reflect.TypeOf(SubscriberManager{}).PkgPath())))
	return SubscriberManager{
		log:         log,
		subscribers: make([]Subscriber, 0),
	}
}

// Update subscribers takes a string containing JSON configuration specifying a list of subscribers to manage.
// This is the external mechanism external functions should call to dynamically add/update/remove subscribers.
// This configuration should contain all subscribers including any changes to existing ones.
// Any subscribers found in the old configuration but not in the new will be removed.
func (sm *SubscriberManager) UpdateConfiguration(jsonConfig string) error {

	// TODO: Consider if we want to do this better.
	// Fow now this is a fairly rudimentary way of updating subscribers while I flush out the rest of the implementation.
	// We'll just stop all subscribers, make the updates, then restart all subscribers.
	// Maybe this is "good enough", but we could be more deliberate in how we move from the old->new config.
	// Likely issues include ensuring we don't loose any events that are in the subscriber queues but not yet sent.
	// For example:
	// * Pause Run() before making any changes.
	// * Evaluate how to get from the oldConfig to the NewConfig:
	//   * If a subscriber exists in the old config but not the new one, stop it.
	//   * If a subscriber doesn't exist in the old config but does in the new config start it after adding it.
	// Note: There is no need to restart changed subscribers as they will automatically pickup
	// the new configuration (either when a new event comes, or they try to resend an existing event).
	//
	// PS: Be careful not to over engineer the final approach.

	for _, s := range sm.subscribers {
		s.Stop()
	}

	newSubscribers, err := newSubscribersFromJson(jsonConfig, sm.log)
	if err != nil {
		return err
	}

	sm.subscribers = newSubscribers
	for _, s := range sm.subscribers {
		s.Handle()
	}

	return nil
}

// Monitor watches for new events and adds them to the queue for each subscriber.
// It also handles shutting down all subscribers when the app is shutting down.
func (sm *SubscriberManager) Monitor(ctx context.Context, wg *sync.WaitGroup, eventBuffer <-chan *pb.Event) error {

	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			sm.log.Info("shutting down subscribers")
			for _, s := range sm.subscribers {
				s.Stop()
			}
			return nil
		case event := <-eventBuffer:
			for _, s := range sm.subscribers {
				s.Enqueue(event)
			}
		}

	}
}
