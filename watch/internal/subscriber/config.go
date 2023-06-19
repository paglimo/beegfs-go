package subscriber

import (
	"encoding/json"
	"fmt"

	"go.uber.org/zap"
)

// SubscriberConfig defines all the possible configuration options that could be set on any type of subscriber.
// Based on the selected "Type" only some fields will actually apply to a particular subscriber.
type SubscriberConfig struct {
	Type      string `json:"type"`
	ID        string `json:"id"`
	Name      string `json:"name"`
	QueueSize int    `json:"queue_size"`
	// Options for gRPC subscribers
	Hostname      string `json:"hostname"`
	Port          string `json:"port"`
	AllowInsecure bool   `json:"allow_insecure"` // If this is unset it will default to "false", ensuring insecure connections are not allowed by default.
}

// newSubscribersFromJson takes a string containing JSON defining the configuration for one or more subscribers.
// It attempts to unmarshal and initialize each subscriber and returns a slice of all the subscribers.
// It does not return an error if there are any invalid subscribers (or configuration/fields).
// It does return an error if it was unable to unmarshal the provided JSON due to a syntax/other error.
func newSubscribersFromJson(jsonConfig string, log *zap.Logger) ([]Subscriber, error) {

	var configs []SubscriberConfig

	if err := json.Unmarshal([]byte(jsonConfig), &configs); err != nil {
		return nil, fmt.Errorf("unable to unmarshal subscriber configuration: %w", err)
	}

	var newSubscribers []Subscriber
	for _, config := range configs {
		log := log.With(zap.String("subscriber_id", config.ID))
		s, err := newSubscriberFromConfig(config, log)

		if err != nil {
			log.Info("unable to add subscriber (ignoring)", zap.Error(err), zap.Any("config", config))
			continue
		}
		newSubscribers = append(newSubscribers, s)
	}

	return newSubscribers, nil

}

// newSubscriberFromConfig takes a SubscriberConfig and returns an initialized struct for the indicated subscriber type.
// It will return an error if the requested subscriber type is unknown.
func newSubscriberFromConfig(config SubscriberConfig, log *zap.Logger) (Subscriber, error) {

	// TODO: We should probably handle the default as part of global configuration.
	queueSize := config.QueueSize
	if queueSize == 0 {
		queueSize = 100
	}

	base := newBaseSubscriber(config.ID, config.Name, queueSize, log)

	switch config.Type {
	case "grpc":
		subscriber := newGRPCSubscriber(base, config.Hostname, config.Port, config.AllowInsecure)
		// In order to use the connect and disconnect methods from the specific GRPCSubscriber struct,
		// we need to ensure that the Subscriber interface in the BaseSubscriber is actually holding a GRPCSubscriber value.
		// If we don't do this we'll get a panic because BaseSubscriber doesn't actually implement these methods.
		subscriber.Subscriber = subscriber
		return subscriber, nil
	default:
		return nil, fmt.Errorf("unknown subscriber type: %s", config.Type)
	}

}
