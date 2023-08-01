// Package logger implements logging for the application. Its goal is to
// allowing logging capabilities be easily extended in the future while
// requiring minimal changes elsewhere in the application.
package logger

import (
	"fmt"
	"os"
	"sync"

	"go.uber.org/zap"
)

// Logger is a wrapper around zap.Logger.
// It primarily exists so we can allowing logging configuration to be updated dynamically in the future (BF-48).
type Logger struct {
	*zap.Logger
	configLock sync.RWMutex
}

type Config struct {
	Type              supportedLogTypes `mapstructure:"type"`
	File              string            `mapstructure:"file"`
	Debug             bool              `mapstructure:"debug"`
	IncomingEventRate bool              `mapstructure:"incomingEventRate"`
}

type supportedLogTypes string

const (
	StdOut  supportedLogTypes = "stdout"
	LogFile supportedLogTypes = "logfile"
)

// All SupportedLogTypes should also be added to this slice.
// It is used for printing help text, for example if an invalid type is specified.
var SupportedLogTypes = []supportedLogTypes{
	StdOut,
	LogFile,
}

// New parses command line logging options and returns an appropriately configured logger.
func New(newConfig Config) (*Logger, error) {

	logMgr := Logger{}
	logMgr.updateConfiguration(newConfig)

	return &logMgr, nil
}

// updateConfiguration sets the Logger's zap.Logger based on the provided
// AppConfig. Once BF-48 is resolved it can be exported so the logging
// configuration can be dynamically updated using ConfigMgr.
func (lm *Logger) updateConfiguration(newConfig Config) error {
	lm.configLock.Lock()
	defer lm.configLock.Unlock()

	var config zap.Config
	config.InitialFields = map[string]interface{}{"serviceName": "bee-watch"}

	if newConfig.Debug {
		config = zap.NewDevelopmentConfig()
	} else {
		config = zap.NewProductionConfig()
	}

	// TODO (BF-47): Better support multiple log types.
	if newConfig.Type == LogFile {
		logFile, err := os.OpenFile(newConfig.File, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			return fmt.Errorf("unable to create log file: %s", err)
		}

		config.OutputPaths = []string{logFile.Name()}
	}

	l, err := config.Build()
	if err != nil {
		return err
	}

	lm.Logger = l
	return nil
}
