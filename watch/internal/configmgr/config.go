package configmgr

import (
	"fmt"

	"git.beegfs.io/beeflex/bee-watch/internal/logger"
	"git.beegfs.io/beeflex/bee-watch/internal/metadata"
	"git.beegfs.io/beeflex/bee-watch/internal/subscriber"
	"git.beegfs.io/beeflex/bee-watch/internal/subscribermgr"
	"git.beegfs.io/beeflex/bee-watch/internal/types"
)

// AppConfig defines all configuration supported by all application components.
// IMPORTANT: When updating/refactoring AppConfig these changes need to be
// manually applied to the pflags defined in main.go.
type AppConfig struct {
	CfgFile     string                      `mapstructure:"cfgFile"`
	Log         logger.Config               `mapstructure:"log"`
	Handler     subscribermgr.HandlerConfig `mapstructure:"handler"`
	Metadata    metadata.Config             `mapstructure:"metadata"`
	Subscribers []subscriber.Config         `mapstructure:"subscriber"`
	Developer   struct {
		PerfProfilingPort int  `mapstructure:"perfProfilingPort"`
		DumpConfig        bool `mapstructure:"dumpConfig"`
	}
}

// validateConfig checks we received sane configuration values. Any issues are
// returned as a MultiError specifying the problematic values. Note it only
// performs static checks, and will not (for example) catch if a file doesn't
// exist or we don't have permissions to access it.
func validateConfig(config AppConfig) error {

	// TODO: Consider moving validation checks into the respective packages where the config is defined.
	var multiErr types.MultiError

	switch config.Log.Type {
	case logger.LogFile:
		if config.Log.File == "" {
			multiErr.Errors = append(multiErr.Errors, fmt.Errorf("log.type is set to 'logfile' but no log.file was specified"))
		}
	case logger.StdOut:
	case logger.Syslog:
	default:
		multiErr.Errors = append(multiErr.Errors, fmt.Errorf("provided log.type is invalid: %s (valid types: %s)", config.Log.Type, logger.SupportedLogTypes))
	}

	if !(config.Log.Level == 1 || config.Log.Level == 3 || config.Log.Level == 5) {
		multiErr.Errors = append(multiErr.Errors, fmt.Errorf("the provided log.level is invalid (must be 1, 3, or 5)"))
	}

	if config.Metadata.EventLogTarget == "" {
		multiErr.Errors = append(multiErr.Errors, fmt.Errorf("no 'metadata.eventLogTarget' was specified"))
	}

	if len(config.Subscribers) == 0 && config.CfgFile == "" {
		multiErr.Errors = append(multiErr.Errors, fmt.Errorf("no subscribers were configured and no subscribers can be added later (no configuration file was specified)"))
	}

	if len(multiErr.Errors) > 0 {
		return &multiErr
	}
	return nil
}
