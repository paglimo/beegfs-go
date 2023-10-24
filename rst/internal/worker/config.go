package worker

import (
	"context"
	"fmt"

	"github.com/thinkparq/gobee/types"
	beegfs "github.com/thinkparq/protobuf/beegfs/go"
	"go.uber.org/zap"
)

// Config defines the configuration options that could be set on any type of
// worker node. It embeds the configuration for each type of worker to
// standardize and simplify unmarshalling configuration and initializing
// workers.
type Config struct {
	ID                             string   `mapstructure:"id"`
	Name                           string   `mapstructure:"name"`
	Type                           NodeType `mapstructure:"type"`
	MaxReconnectBackOff            int      `mapstructure:"maxReconnectBackOff"`
	MaxWaitForResponseAfterConnect int      `mapstructure:"maxWaitForResponseAfterConnect"`
	// All embedded subscriber types must specify `mapstructure:",squash"` to tell
	// Viper to squash the fields of the embedded struct into the worker Config.
	// Without this viper.Unmarshal(&newConfig) will omit their configuration.
	// Across all embedded structs, the field tags must be unique for proper unmarshalling.
	// The standard is to prefix field tags with the subscriber type.
	BeeSyncConfig `mapstructure:",squash"` // Configuration options for type BeeSync.
}

// BeeSyncConfig contains configuration options specific to BeeSync worker nodes.
type BeeSyncConfig struct {
	Hostname string `mapstructure:"beeSyncHostname"`
	Port     int    `mapstructure:"beeSyncPort"`
	// If AllowInsecure is unset it will default to "false", ensuring insecure
	// connections are not allowed by default.
	AllowInsecure         bool   `mapstructure:"beeSyncAllowInsecure"`
	DisconnectTimeout     int    `mapstructure:"beeSyncDisconnectTimeout"`
	SelfSignedTLSCertPath string `mapstructure:"beeSyncSelfSignedTLSCertPath"`
}

// newWorkerNodesFromConfig is the standard way for initializing worker nodes.
// It accepts a slice of Worker Config defining the configuration for one or
// more worker nodes. It returns a slice of all nodes that could be successfully
// initialized. If it was unable to initialize any nodes due to bad
// configuration/fields it also returns an error indicating the invalid nodes.
// It is up to the caller to decide to act on the configuration if an error is
// returned. For example the caller could choose to move forward with whatever
// good Nodes were returned and log a warning about the misconfigured nodes.
func newWorkerNodesFromConfig(log *zap.Logger, workResponses chan<- *beegfs.WorkResponse, configs []Config) ([]*Node, error) {

	var newWorkers []*Node
	var multiErr types.MultiError

	for _, config := range configs {
		w, err := newWorkerNodeFromConfig(log, workResponses, config)
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

// newWorkerNodeFromConfig() is intended to be used with newWorkerNodesFromConfig().
// It accepts a single worker node configuration and returns either an
// initialized worker node or an error.
func newWorkerNodeFromConfig(log *zap.Logger, workResponses chan<- *beegfs.WorkResponse, config Config) (*Node, error) {

	log = log.With(zap.String("nodeID", config.ID), zap.String("nodeName", config.Name))
	ctx, cancel := context.WithCancel(context.Background())

	node := &Node{
		Config: config,
		State: State{
			state: DISCONNECTED,
		},
		ctx:           ctx,
		cancel:        cancel,
		log:           log,
		workResponses: workResponses,
	}

	switch node.Type {
	case BeeSync:
		// In order to use the connect and disconnect methods from the specific
		// BeeSync struct, we need to ensure that the interface in the Worker is
		// actually holding a BeeSync value. If we don't do this we'll get a
		// panic because the base Worker struct doesn't actually implement these
		// methods.
		node.worker = newBeeSyncWorker(config.BeeSyncConfig)
		return node, nil
	default:
		return nil, fmt.Errorf("unknown worker type: %s", config.Type)
	}
}
