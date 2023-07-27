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

	"git.beegfs.io/beeflex/bee-watch/internal/types"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// ConfigManager is used to determine the initial configuration from flags and/or a config file.
// It also allows dynamically updating configuration for select application components.
// This works by updating the config file then sending the app a SIGHUP.
type ConfigManager struct {
	initialFlags     *pflag.FlagSet
	listeners        []ConfigListener
	currentConfig    *AppConfig
	updateSignal     chan os.Signal
	updateInProgress *sync.RWMutex
}

// Components that support dynamic configuration updates can be added as a
// listener if they implement the ConfigListener interface.
type ConfigListener interface {
	// UpdateConfiguration is run to provide the latest AppConfig to a
	// component. If the component is unable to update the configuration it
	// should return a meaningful error to help diagnose and correct the
	// configuration then wait for new configuration to be provided. The
	// component SHOULD NOT rollback to the previous version of the
	// configuration. The component SHOULD avoid data loss, for example if a bad
	// list of subscribers was provided, don't delete all subscribers and drop
	// events.
	UpdateConfiguration(AppConfig) error
}

// New attempts to read the provided configFile and parse the configuration.
// It it fails it immediately returns an error so the app doesn't start with bad configuration.
// If it succeeds it returns an initialized ConfigManager with the provided configuration.
// It also returns a copy of the initial AppConfig so it can be used to initialize other managers.
// When the app is ready to accept dynamic configuration updates the Manage() method can be called.
func New(flags *pflag.FlagSet) (*ConfigManager, AppConfig, error) {

	var mutex sync.RWMutex

	cfgMgr := &ConfigManager{
		initialFlags:     flags,
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

// UpdateConfiguration combines the following configuration sources. The
// precedence order is determined by Viper: (1) command line flags, (2)
// environmental variables, (3) a configuration file, (4) default values
// (specified as part of the pflag definitions in main.go).
//
// Before applying the configuration static validation checks will be performed.
// If any of its validation checks fail, the configuration will not be updated,
// and an error will be returned immediately to be handled by the caller.
//
// If validation succeeds it will attempt to propagate the configuration update
// to other components of the app that support dynamic configuration updates. It
// does this by calling UpdateConfiguration() on all configured listeners. If
// there is an error dynamically updating the configuration for any component,
// the component is expected to return a meaningful error. Any error(s) will be
// aggregated and returned to the caller for handling.
func (cm *ConfigManager) UpdateConfiguration() error {

	// Viper's precedence order means flags have the highest priority.
	// This means any configuration set using a flag is immutable.
	// We also get
	err := viper.BindPFlags(cm.initialFlags)
	if err != nil {
		return fmt.Errorf("unable to parse command line flags: %s", err)
	}

	// BeeWatch will support configuration using environment variables prefixed
	// with BEEWATCH_<VARIABLE>. These will be revaluated if
	// UpdateConfiguration() reruns, meaning they can be used to update the
	// configuration dynamically.
	viper.SetEnvPrefix("BEEWATCH")
	viper.AutomaticEnv()

	// Important we do this last as a cfgFile could be set as an environment variable.
	// We'll allow configuring BeeWatch entirely without a config file.
	if viper.GetString("cfgFile") != "" {
		viper.SetConfigFile(viper.GetString("cfgFile"))
	}

	err = viper.ReadInConfig()
	if err != nil {
		return fmt.Errorf("unable to read config from file: %s (does the file exist?)", err)
	}

	var newConfig AppConfig
	if err := viper.Unmarshal(&newConfig); err != nil {
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

	// Update any components whose configuration can be set dynamically.
	// If anything goes wrong the component is expected to handle the issue gracefully.
	// We'll just return the error, but we won't rollback the configuration.
	var multiErr types.MultiError
	for _, mgr := range cm.listeners {
		err := mgr.UpdateConfiguration(newConfig)
		if err != nil {
			multiErr.Errors = append(multiErr.Errors, err)
		}
	}

	if len(multiErr.Errors) > 0 {
		return &multiErr
	}

	cm.currentConfig = &newConfig
	return nil
}
