package worker

import (
	"context"
	"fmt"

	"github.com/thinkparq/gobee/types"
	"github.com/thinkparq/protobuf/go/flex"
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
	MockConfig                             // Allow mocking worker node behavior from other packages. We could add a map structure tag here if we wanted to be able to setup expectations using a TOML file.
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

// Mock worker nodes represent simulated worker nodes. They are configured just
// like other worker node types, except their configuration defines how they
// should behave for a particular test. An entry must exist in Expectations for
// each method that would be called during a particular test run.
type MockConfig struct {
	Expectations []MockExpectation
}

// MockExpectation is a wrapper around a single call to setup expectations for
// Testify (i.e., `testObj.On("DoSomething", 123).Return(true, nil)`
// https://github.com/stretchr/testify#mock-package). This allows us to mock
// worker nodes from other packages which cannot directly modify worker nodes.
type MockExpectation struct {
	MethodName string
	Args       []interface{}
	ReturnArgs []interface{}
}

// newWorkerNodesFromConfig is the standard way for initializing worker nodes.
// It accepts a slice of Worker Config defining the configuration for one or
// more worker nodes. It returns a slice of all nodes that could be successfully
// initialized. If it was unable to initialize any nodes due to bad
// configuration/fields it also returns an error indicating the invalid nodes.
// It is up to the caller to decide to act on the configuration if an error is
// returned. For example the caller could choose to move forward with whatever
// good Nodes were returned and log a warning about the misconfigured nodes.
func newWorkerNodesFromConfig(log *zap.Logger, workResponses chan<- *flex.WorkResponse, configs []Config) ([]*Node, error) {

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
func newWorkerNodeFromConfig(log *zap.Logger, workResponses chan<- *flex.WorkResponse, config Config) (*Node, error) {

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
	case Mock:
		node.worker = newMockWorker(config.MockConfig)
		return node, nil
	default:
		return nil, fmt.Errorf("unknown worker type: %s", config.Type)
	}
}
