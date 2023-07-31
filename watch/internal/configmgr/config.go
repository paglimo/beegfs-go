package configmgr

import (
	"fmt"

	"git.beegfs.io/beeflex/bee-watch/internal/subscriber"
	"git.beegfs.io/beeflex/bee-watch/internal/types"
)

// AppConfig defines all configuration supported by all application components.
// Note when updating/refactoring AppConfig these changes need to be manually
// applied to the pflags defined in main.go.
type AppConfig struct {
	Log struct {
		Type              SupportedLogTypes `mapstructure:"type"`
		File              string            `mapstructure:"file"`
		Debug             bool              `mapstructure:"debug"`
		IncomingEventRate bool              `mapstructure:"incomingEventRate"`
	}
	Metadata struct {
		EventLogTarget         string `mapstructure:"eventLogTarget"`
		EventBufferSize        int    `mapstructure:"eventBufferSize"`
		EventBufferGCFrequency int    `mapstructure:"eventBufferGCFrequency"`
		EventPollFrequency     int    `mapstructure:"eventPollFrequency"`
	}
	Subscribers []subscriber.Config `mapstructure:"subscriber"`
	Developer   struct {
		PerfProfilingPort int  `mapstructure:"perfProfilingPort"`
		DumpConfig        bool `mapstructure:"dumpConfig"`
	}
}

type SupportedLogTypes string

const (
	StdOut  SupportedLogTypes = "stdout"
	LogFile SupportedLogTypes = "logfile"
)

// All SupportedLogTypes should also be added to this slice.
// It is used for printing help text, for example if an invalid type is specified.
var supportedLogTypes = []SupportedLogTypes{
	StdOut,
	LogFile,
}

// validateConfig checks we received sane configuration values. Any issues are
// returned as a MultiError specifying the problematic values. Note it only
// performs static checks, and will not (for example) catch if a file doesn't
// exist or we don't have permissions to access it.
func validateConfig(config AppConfig) error {

	var multiErr types.MultiError

	switch config.Log.Type {
	case LogFile:
		if config.Log.File == "" {
			multiErr.Errors = append(multiErr.Errors, fmt.Errorf("logType is set to 'logfile' but no log file path (log.file) was specified"))
		}
	case StdOut:
	default:
		multiErr.Errors = append(multiErr.Errors, fmt.Errorf("provided LogType is invalid: %s (valid types: %s)", config.Log.Type, supportedLogTypes))
	}

	if config.Metadata.EventLogTarget == "" {
		multiErr.Errors = append(multiErr.Errors, fmt.Errorf("no 'metadata.eventLogTarget' was specified"))
	}

	if len(multiErr.Errors) > 0 {
		return &multiErr
	}
	return nil
}
