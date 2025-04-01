package config

import (
	"fmt"

	"github.com/thinkparq/beegfs-go/common/configmgr"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/rst/sync/internal/beeremote"
	"github.com/thinkparq/beegfs-go/rst/sync/internal/server"
	"github.com/thinkparq/beegfs-go/rst/sync/internal/workmgr"
)

// We use ConfigManager to handle configuration updates.
// Verify all interfaces that depend on AppConfig are satisfied.
var _ configmgr.Configurable = &AppConfig{}

type AppConfig struct {
	MountPoint string           `mapstructure:"mount-point"`
	WorkMgr    workmgr.Config   `mapstructure:"manager"`
	BeeRemote  beeremote.Config `mapstructure:"remote"`
	Server     server.Config    `mapstructure:"server"`
	Log        logger.Config    `mapstructure:"log"`
	Developer  struct {
		PerfProfilingPort int  `mapstructure:"perf-profiling-port"`
		DumpConfig        bool `mapstructure:"dump-config"`
	}
}

// NewEmptyInstance() returns an empty AppConfig for ConfigManager to use with
// when unmarshalling the configuration.
func (c *AppConfig) NewEmptyInstance() configmgr.Configurable {
	return new(AppConfig)
}

func (c *AppConfig) UpdateAllowed(newConfig configmgr.Configurable) error {
	// Currently all configuration allowed to be dynamically updated is provided by BeeRemote and
	// ConfigMgr is not involved with it.
	return nil
}
func (c *AppConfig) ValidateConfig() error {
	if c.WorkMgr.NumWorkers <= 0 {
		return fmt.Errorf("at least one worker is required to start (specified number of workers: %d)", c.WorkMgr.NumWorkers)
	}
	if c.WorkMgr.ActiveWorkQueueSize <= 0 {
		return fmt.Errorf("the active work queue size must at least be one (specified size: %d)", c.WorkMgr.ActiveWorkQueueSize)
	}
	return nil
}
