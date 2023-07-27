// Package configmgr contains functionality for handling the application's configuration.
package configmgr

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path"
	"reflect"
	"sync"
	"syscall"

	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// ConfigManager is used to determine the initial configuration from flags and/or a config file.
// It also allows dynamically updating configuration for select application components.
// This works by updating the config file then sending the app a SIGHUP.
type ConfigManager struct {
	configFile       string
	listeners        []ConfigListener
	currentConfig    *AppConfig
	updateSignal     chan os.Signal
	updateInProgress *sync.RWMutex
}

// Components that support dynamic configuration updates can be added as a
// listener if they implement the ConfigListener interface.
type ConfigListener interface {
	UpdateConfiguration(AppConfig) error
}

// New attempts to read the provided configFile and parse the configuration.
// It it fails it immediately returns an error so the app doesn't start with bad configuration.
// If it succeeds it returns an initialized ConfigManager with the provided configuration.
// It also returns a copy of the initial AppConfig so it can be used to initialize other managers.
// When the app is ready to accept dynamic configuration updates the Manage() method can be called.
func New(configFile string) (*ConfigManager, AppConfig, error) {

	var mutex sync.RWMutex

	cfgMgr := &ConfigManager{
		configFile:       configFile,
		currentConfig:    nil,
		updateSignal:     make(chan os.Signal, 1),
		updateInProgress: &mutex,
	}

	err := cfgMgr.UpdateConfiguration()
	if err != nil {
		return nil, AppConfig{}, err
	}

	signal.Notify(cfgMgr.updateSignal, syscall.SIGHUP)

	return cfgMgr, *cfgMgr.currentConfig, nil
}

// Manage listens for an update signal (SIGHUP) and attempts to dynamically
// update the configuration. It requires a context that should be cancelled when
// it should terminate. It also requires a logger because the logger cannot be
// set on the ConfigManager struct since it is also responsible for handling log
// configuration.
//
// Configuration that can be updated dynamically: * Subscribers can be added,
// removed, and edited. Configuration that can not be updated dynamically: *
// Anything to do with the metadata socket or events buffer. If a configuration
// update results in bad configuration, it will warn and refuse to update the
// config.
func (cm *ConfigManager) Manage(ctx context.Context, log *zap.Logger) {

	log = log.With(zap.String("component", path.Base(reflect.TypeOf(ConfigManager{}).PkgPath())))

	for {
		// When we first start make sure all the managers have the latest configuration.
		cm.updateInProgress.Lock()
		err := cm.UpdateConfiguration()
		cm.updateInProgress.Unlock()
		if err != nil {
			log.Warn("new configuration was rejected because it is invalid", zap.Error(err))
		}

		select {
		case <-ctx.Done():
			log.Info("shutting down because the app is shutting down")
			return
		case <-cm.updateSignal:
			log.Info("configuration update requested")
			continue
		}
	}
}

// Any components that support dynamic configuration updates should be added as
// listeners once they're initialized.
func (cm *ConfigManager) AddListener(listener ConfigListener) {
	cm.listeners = append(cm.listeners, listener)
}

// UpdateConfiguration attempts to read the latest configuration from a file.
// If it fails it will return an error to be handled by the caller.
// It then propagates the configuration to other components in the app.
// It does this by calling UpdateConfiguration() on all configured listeners.
//
// TODO: Consider if some of this functionality could be broken out into a
// function that doesn't require CM for easier unit testing. For example do we
// need to compare the old/new configuration state? Do we want to warn if someone
// updates part of the configuration we don't support setting dynamically?
func (cm *ConfigManager) UpdateConfiguration() error {

	viper.SetConfigFile(cm.configFile)
	viper.SetEnvPrefix("BEEWATCH")
	viper.AutomaticEnv()

	// Any defaults not specified here will use the default value for that type:
	viper.SetDefault("logType", "stdout")
	viper.SetDefault("logStdFile", "/var/log/beegfs/bee-watch.log")
	viper.SetDefault("sysFileEventBufferSize", 10000000)
	viper.SetDefault("sysFileEventBufferGCFrequency", 100000)
	viper.SetDefault("sysFileEventPollFrequency", 1)

	err := viper.ReadInConfig()
	if err != nil {
		return fmt.Errorf("unable to read config from file: %s (does the file exist?)", err)
	}

	var newConfig AppConfig
	err = viper.Unmarshal(&newConfig)
	if err != nil {
		return fmt.Errorf("unable to parse configuration from file: %s (is the configuration valid?)", err)
	}

	if err = validateConfig(newConfig); err != nil {
		return err
	}

	// After initial startup some of the configuration is immutable:
	if cm.currentConfig != nil {
		if newConfig.Developer != cm.currentConfig.Developer {
			return fmt.Errorf("rejecting configuration update: unable to change developer configuration settings after startup")
		}
		if newConfig.Metadata != cm.currentConfig.Metadata {
			return fmt.Errorf("rejecting configuration update: unable to change metadata configuration settings after startup")
		}
		// TODO (BF-48): Allow logging configuration to be changed dynamically.
		if newConfig.Logging != cm.currentConfig.Logging {
			return fmt.Errorf("rejecting configuration update: unable to change logging configuration settings after startup")
		}
	}

	for _, mgr := range cm.listeners {
		err := mgr.UpdateConfiguration(newConfig)
		// TODO: The component should have logged a more meaningful message and handled the error.
		// Is there any general functionality we want to perform here? Rollback the config change?
		// If not we should just get rid of the error on the UpdateConfiguration() method.
		// It would be ideal if we rollback, otherwise the running config may not match the actual config.
		if err != nil {
			fmt.Printf("TODO: Handle: %s", err)
		}
	}

	cm.currentConfig = &newConfig

	return nil
}
