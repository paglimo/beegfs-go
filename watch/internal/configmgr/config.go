package configmgr

import (
	"fmt"

	"git.beegfs.io/beeflex/bee-watch/internal/subscriber"
	"git.beegfs.io/beeflex/bee-watch/internal/types"
)

type AppConfig struct {
	Logging struct {
		LogType    SupportedLogTypes `mapstructure:"logType"`
		LogStdFile string            `mapstructure:"logStdFile"`
		LogDebug   bool              `mapstructure:"logDebug"`
	}
	Metadata struct {
		SysFileEventLogTarget         string `mapstructure:"sysFileEventLogTarget"`
		SysFileEventBufferSize        int    `mapstructure:"sysFileEventBufferSize"`
		SysFileEventBufferGCFrequency int    `mapstructure:"sysFileEventBufferGCFrequency"`
		SysFileEventPollFrequency     int    `mapstructure:"sysFileEventPollFrequency"`
	}
	Subscribers []subscriber.BaseConfig `mapstructure:"subscriber"`
	Developer   struct {
		PerfLogIncomingEventRate bool `mapstructure:"perfLogIncomingEventRate"`
		PerfProfilePort          int  `mapstructure:"perfProfilePort"`
	}
}

type SupportedLogTypes string

const (
	StdOut  SupportedLogTypes = "stdout"
	LogFile SupportedLogTypes = "logfile"
)

// validateConfig checks we received sane configuration values. Any issues are
// returned as a MultiError specifying the problematic values. Note it only
// performs static checks, and will not (for example) catch if a file doesn't
// exist or we don't have permissions to access it.
func validateConfig(config AppConfig) error {

	var multiErr types.MultiError

	switch config.Logging.LogType {
	case LogFile:
		if config.Logging.LogStdFile == "" {
			multiErr.Errors = append(multiErr.Errors, fmt.Errorf("logType is set to 'logfile' but no log file path (logStdFile) was specified"))
		}
	case StdOut:
	default:
		multiErr.Errors = append(multiErr.Errors, fmt.Errorf("provided LogType is invalid: %s", config.Logging.LogType))
	}

	if config.Metadata.SysFileEventLogTarget == "" {
		multiErr.Errors = append(multiErr.Errors, fmt.Errorf("no 'SysFileEventLogTarget' was specified"))
	}

	if len(multiErr.Errors) > 0 {
		return &multiErr
	}
	return nil
}
