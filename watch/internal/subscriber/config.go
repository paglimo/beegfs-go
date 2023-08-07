package subscriber

import (
	"fmt"
)

// Config defines the configuration options that could be set on any type of
// subscriber. It embeds the configuration for each type of subscriber to standardize
// and simplify unmarshalling configuration and initializing subscribers.
type Config struct {
	Type string `mapstructure:"type"`
	ID   int    `mapstructure:"id"`
	Name string `mapstructure:"name"`
	// All embedded subscriber types must specify `mapstructure:",squash"` to tell
	// Viper to squash the fields of the embedded struct into the subscriber Config.
	// Without this viper.Unmarshal(&newConfig) will omit their configuration.
	// Across all embedded structs, the field tags must be unique for proper unmarshalling.
	// The standard is to prefix field tags with the subscriber type.
	GrpcConfig `mapstructure:",squash"` // Configuration options for type gRPC.
}

// GrpcConfig defines configuration options that only apply to gRPC subscribers.
type GrpcConfig struct {
	Hostname string `mapstructure:"grpcHostname"`
	Port     string `mapstructure:"grpcPort"`
	// If AllowInsecure is unset it will default to "false", ensuring insecure
	// connections are not allowed by default.
	AllowInsecure         bool   `mapstructure:"grpcAllowInsecure"`
	DisconnectTimeout     int    `mapstructure:"grpcDisconnectTimeout"`
	SelfSignedTLSCertPath string `mapstructure:"grpcSelfSignedTLSCertPath"`
}

// NewSubscribersFromConfig is the standard way for initializing one or more subscribers.
// It takes a slice of Config defining the configuration for one or more subscribers.
// It attempts to initialize each subscriber and returns a slice of all the subscribers.
// It returns an error if there are any invalid subscribers (or configuration/fields).
func NewSubscribersFromConfig(configs []Config) ([]*Subscriber, error) {

	var newSubscribers []*Subscriber
	for _, config := range configs {
		s, err := newSubscriberFromConfig(config)

		if err != nil {
			return nil, err
		}
		newSubscribers = append(newSubscribers, s)
	}

	return newSubscribers, nil

}

// newSubscriberFromConfig takes a subscriber Config and returns an initialized
// struct for the indicated subscriber type. It will return an error if the
// requested subscriber type is unknown.
func newSubscriberFromConfig(config Config) (*Subscriber, error) {

	subscriber := &Subscriber{
		Config: config,
		State: State{
			state: DISCONNECTED,
		},
	}

	switch subscriber.Type {
	case "grpc":
		// In order to use the connect and disconnect methods from the specific
		// GRPCSubscriber struct, we need to ensure that the interface in the
		// Subscriber is actually holding a GRPCSubscriber value. If we don't do
		// this we'll get a panic because BaseSubscriber doesn't actually
		// implement these methods.
		subscriber.Interface = newGRPCSubscriber(subscriber.GrpcConfig)
		return subscriber, nil
	default:
		return nil, fmt.Errorf("unknown subscriber type: %s", config.Type)
	}
}
