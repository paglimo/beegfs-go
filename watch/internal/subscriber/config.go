package subscriber

import (
	"fmt"
)

// BaseConfig defines the configuration options that could be set on any type of subscriber.
// It embeds each type of subscriber so they can be initialized based on the selected "Type".
type BaseConfig struct {
	Type string `mapstructure:"type"`
	ID   int    `mapstructure:"id"`
	Name string `mapstructure:"name"`
	// All embedded subscriber types must specify `mapstructure:",squash"` to tell
	// Viper to squash the fields of the embedded struct into BaseConfig.
	// Without this viper.Unmarshal(&newConfig) will omit their configuration.
	// Across all embedded structs, the field tags must be unique for proper unmarshalling.
	// The standard is to prefix field tags with the subscriber type.
	GrpcConfig `mapstructure:",squash"` // Configuration options for type gRPC.
}

// GrpcConfig defines configuration options that only apply to gRPC subscribers.
type GrpcConfig struct {
	Hostname      string `mapstructure:"grpcHostname"`
	Port          string `mapstructure:"grpcPort"`
	AllowInsecure bool   `mapstructure:"grpcAllowInsecure"` // If this is unset it will default to "false", ensuring insecure connections are not allowed by default.
}

// NewSubscribersFromConfig is the standard way for initializing one or more subscribers.
// It takes a slice of BaseConfig defining the configuration for one or more subscribers.
// It attempts to initialize each subscriber and returns a slice of all the subscribers.
// It returns an error if there are any invalid subscribers (or configuration/fields).
func NewSubscribersFromConfig(configs []BaseConfig) ([]*BaseSubscriber, error) {

	var newSubscribers []*BaseSubscriber
	for _, config := range configs {
		s, err := newSubscriberFromConfig(config)

		if err != nil {
			return nil, err
		}
		newSubscribers = append(newSubscribers, s)
	}

	return newSubscribers, nil

}

// newSubscriberFromConfig takes a BaseConfig and returns an initialized struct for the indicated subscriber type.
// It will return an error if the requested subscriber type is unknown.
func newSubscriberFromConfig(config BaseConfig) (*BaseSubscriber, error) {

	base := &BaseSubscriber{
		Id:   config.ID,
		Name: config.Name,
		State: State{
			state: DISCONNECTED,
		},
	}

	switch config.Type {
	case "grpc":
		subscriber := newGRPCSubscriber(config.Hostname, config.Port, config.AllowInsecure)
		// In order to use the connect and disconnect methods from the specific GRPCSubscriber struct,
		// we need to ensure that the Subscriber interface in the BaseSubscriber is actually holding a GRPCSubscriber value.
		// If we don't do this we'll get a panic because BaseSubscriber doesn't actually implement these methods.
		//subscriber.Subscriber = subscriber
		base.Subscriber = subscriber
		return base, nil
	default:
		return nil, fmt.Errorf("unknown subscriber type: %s", config.Type)
	}

}
