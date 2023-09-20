package worker

import (
	"fmt"

	"github.com/thinkparq/gobee/types"
)

// Config defines the configuration options that could be set on any type of
// Worker. It embeds the configuration for each type of worker to standardize
// and simplify unmarshalling configuration and initializing workers.
type Config struct {
	ID   string   `mapstructure:"id"`
	Name string   `mapstructure:"name"`
	Type NodeType `mapstructure:"type"`
	// All embedded subscriber types must specify `mapstructure:",squash"` to tell
	// Viper to squash the fields of the embedded struct into the worker Config.
	// Without this viper.Unmarshal(&newConfig) will omit their configuration.
	// Across all embedded structs, the field tags must be unique for proper unmarshalling.
	// The standard is to prefix field tags with the subscriber type.
	BeeSyncConfig `mapstructure:",squash"` // Configuration options for type BeeSync.
}

type BeeSyncConfig struct {
	Hostname string `mapstructure:"beeSyncHostname"`
	Port     int    `mapstructure:"beeSyncPort"`
	// If AllowInsecure is unset it will default to "false", ensuring insecure
	// connections are not allowed by default.
	AllowInsecure         bool   `mapstructure:"beeSyncAllowInsecure"`
	DisconnectTimeout     int    `mapstructure:"beeSyncDisconnectTimeout"`
	SelfSignedTLSCertPath string `mapstructure:"beeSyncSelfSignedTLSCertPath"`
}

// newWorkersFromConfig is the standard way for initializing workers. It accepts
// a slice of Worker Config defining the configuration for one or more workers.
// It returns a slice of all workers that could be successfully initialized. If
// it was unable to initialize any workers due to bad configuration/fields it
// also returns an error indicating the invalid workers. It is up to the caller
// to decide to act on the configuration if an error is returned. For example
// the caller could choose to move forward with whatever good Workers were
// returned and log a warning about the misconfigured workers.
func newWorkersFromConfig(configs []Config) ([]*Worker, error) {

	var newWorkers []*Worker
	var multiErr types.MultiError

	for _, config := range configs {
		w, err := newWorkerFromConfig(config)
		if err != nil {
			multiErr.Errors = append(multiErr.Errors, err)
			continue
		}
		newWorkers = append(newWorkers, w)
	}

	if len(multiErr.Errors) != 0 {
		return newWorkers, &multiErr
	}

	return newWorkers, nil
}

func newWorkerFromConfig(config Config) (*Worker, error) {

	worker := &Worker{
		Config: config,
		State: State{
			state: DISCONNECTED,
		},
	}

	switch worker.Type {
	case BeeSync:
		// In order to use the connect and disconnect methods from the specific
		// BeeSync struct, we need to ensure that the interface in the Worker is
		// actually holding a BeeSync value. If we don't do this we'll get a
		// panic because the base Worker struct doesn't actually implement these
		// methods.
		worker.Interface = newBeeSyncNode(config.BeeSyncConfig)
		return worker, nil
	default:
		return nil, fmt.Errorf("unknown worker type: %s", config.Type)
	}
}
