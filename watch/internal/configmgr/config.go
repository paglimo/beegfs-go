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
			multiErr.Errors = append(multiErr.Errors, fmt.Errorf("logType is set to 'logfile' but no log file path (log.file) was specified"))
		}
	case logger.StdOut:
	default:
		multiErr.Errors = append(multiErr.Errors, fmt.Errorf("provided LogType is invalid: %s (valid types: %s)", config.Log.Type, logger.SupportedLogTypes))
	}

	if config.Metadata.EventLogTarget == "" {
		multiErr.Errors = append(multiErr.Errors, fmt.Errorf("no 'metadata.eventLogTarget' was specified"))
	}

	if len(multiErr.Errors) > 0 {
		return &multiErr
	}
	return nil
}
