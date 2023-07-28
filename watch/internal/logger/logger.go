// Package logger implements logging for the application. Its goal is to
// allowing logging capabilities be easily extended in the future while
// requiring minimal changes elsewhere in the application.
package logger

import (
	"fmt"
	"os"
	"sync"

	"git.beegfs.io/beeflex/bee-watch/internal/configmgr"
	"go.uber.org/zap"
)

// Logger is a wrapper around zap.Logger.
// It primarily exists so we can allowing logging configuration to be updated dynamically in the future (BF-48).
type Logger struct {
	*zap.Logger
	configLock sync.RWMutex
}

// New parses command line logging options and returns an appropriately configured logger.
func New(newConfig configmgr.AppConfig) (*Logger, error) {

	logMgr := Logger{}
	logMgr.updateConfiguration(newConfig)

	return &logMgr, nil
}

// updateConfiguration sets the Logger's zap.Logger based on the provided
// AppConfig. Once BF-48 is resolved it can be exported so the logging
// configuration can be dynamically updated using ConfigMgr.
func (lm *Logger) updateConfiguration(newConfig configmgr.AppConfig) error {
	lm.configLock.Lock()
	defer lm.configLock.Unlock()

	var config zap.Config
	config.InitialFields = map[string]interface{}{"serviceName": "bee-watch"}

	if newConfig.Log.Debug {
		config = zap.NewDevelopmentConfig()
	} else {
		config = zap.NewProductionConfig()
	}

	// TODO (BF-47): Better support multiple log types.
	if newConfig.Log.Type == configmgr.LogFile {
		logFile, err := os.OpenFile(newConfig.Log.File, os.O_RDWR|os.O_CREATE, 0755)
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
