// Package configmgr contains functionality for handling the application's configuration.
package configmgr

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path"
	"reflect"
	"strings"
	"sync"
	"syscall"

	"git.beegfs.io/beeflex/bee-watch/internal/types"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const (
	envVariablePrefix = "BEEWATCH_"
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
// following precedence order is respected (1) command line flags, (2)
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

	// We don't want to use Viper to actually persist the configuration.
	// We mainly use Viper to simplify merging the configuration together,
	// following a predictable precedence order.
	v := viper.New()
	v.SetConfigType("toml")

	// Viper's precedence order means flags have the highest priority.
	// This means any configuration set using a flag is immutable.
	// We also get all of our defaults based on the flag setup.
	err := v.BindPFlags(cm.initialFlags)
	if err != nil {
		return fmt.Errorf("unable to parse command line flags: %s", err)
	}

	// Allow specifying subscribers using a --subscribers flag. We do this by
	// transforming the flags into TOML format. Then later on we can just use
	// viper.Unmarshal to get the corresponding Golang structs. This avoids
	// having to write a custom unmarshaller. Note the flag we use to specify
	// subscribers cannot be the same as what is in the TOML file (subscriber).
	// This is because we need a way to allow users to specify multiple
	// subscribers in a single string, that is different from the actual slice
	// of subscribers that is unmarshalled from the final configuration.
	subscribersFromFlags := v.GetString("subscribers")
	if len(subscribersFromFlags) > 0 {
		tomlString := parseTOMLSubscribersFromString(subscribersFromFlags)

		if err := v.ReadConfig(strings.NewReader(tomlString)); err != nil {
			return fmt.Errorf("unable to parse subscribers from command line flags: %s\nAre all flag values enclosed in \"double\" quotes (--flag=\"value\")? \nAre all strings within flag values contained in 'single' quotes (--flag=\"key='value'\"?)", err)
		}
	}

	// While Viper allows you to override values it reads from a config file
	// with environment variables, it does not appear to provide a way to just
	// read in configuration from environment variables.  We don't want to
	// require use of a config file, so this workaround searches for environment
	// variables that match our prefix then uses BindEnv to manually link the
	// desired environment variables that will later be used when we
	// call viper.Unmarshal(). This approach was adapted from:
	// https://renehernandez.io/snippets/bind-environment-variables-to-config-struct-with-viper/.
	// Note an alternative approach would be to use viper.AutomaticEnv() then
	// Viper.Get() would reevaluate environment variables every time Get() is
	// called. However we want also ConfigManager to handle all configuration
	// updates so we can check configuration before it is reapplied. This is
	// partially why we don't use Viper to persist the configuration and
	// require going through ConfigMgr instead of using Get() throughout the app.
	subscribersFromEnv := false
	for _, envVar := range os.Environ() {
		pair := strings.SplitN(envVar, "=", 2)
		key := pair[0]
		val := pair[1]

		if strings.HasPrefix(key, envVariablePrefix) {
			viperKey := strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(key, envVariablePrefix)), "_", ".")

			if viperKey == "subscribers" {
				// We do not want to allow subscribers to be specified multiple
				// ways, so we first check if they were also specified using flags.
				if len(subscribersFromFlags) > 0 {
					return fmt.Errorf("subscribers cannot be set using both flags and environment variables")
				}
				// Use the same approach as we do for flags to get a list of subscribers:
				subscribersFromEnv = true
				tomlString := parseTOMLSubscribersFromString(val)
				if err := v.ReadConfig(strings.NewReader(tomlString)); err != nil {
					return fmt.Errorf("unable to parse subscribers from environment variable: %s\nIs the value contained in \"double\" quotes?\nAre all strings within the value contained in 'single' quotes? \nExample: \"id=1,name='subscriber',type='grpc'\"", err)
				}
			} else if viperKey == "subscriber" {
				return fmt.Errorf("subscribers specified using environment variables should be specified using '%sSUBSCRIBERS=<LIST>' with one or more subscribers separated by a semicolon (the singular form '%sSUBSCRIBER' is not allowed)", envVariablePrefix, envVariablePrefix)
			} else {
				if err := v.BindEnv(viperKey, strings.ToUpper(key)); err != nil {
					return err
				}
			}
		}
	}

	// Important we do this last as a cfgFile could be set as a flag or
	// environment variable. We also want to allow configuring BeeWatch entirely
	// without a config file.
	if v.GetString("cfgfile") != "" {
		// First we read the config file into a separate Viper instance.
		// We mainly do this so we can check subscribers are only being set in one place.
		// Otherwise we'd have to define complicated precedence rules for merging subscribers.
		vFile := viper.New()
		vFile.SetConfigFile(v.GetString("cfgfile"))

		if err := vFile.ReadInConfig(); err != nil {
			if _, ok := err.(viper.ConfigFileNotFoundError); ok {
				return fmt.Errorf("configuration file at '%s' was not found (does it exist? are permissions set correctly?)", v.GetString("cfgFile"))
			}
			return fmt.Errorf("an unknown error occurred reading config file '%s' (do we have permissions to read it?): %s", v.GetString("cfgFile"), err)
		}
		subscribersFromFile := vFile.GetStringSlice("subscriber")

		if len(subscribersFromFile) > 0 && (len(subscribersFromFlags) > 0 || subscribersFromEnv) {
			return fmt.Errorf("subscribers cannot be set using a mix of flags, environment variables, and a configuration file (only one is allowed)")
		}
		// If all our checks pass we'll actually use the config file for the combined Viper instance.
		v.SetConfigFile(v.GetString("cfgfile"))

		// Merge the configuration set via flags and environment variables with the cfgFile.
		if err := v.MergeInConfig(); err != nil {
			return fmt.Errorf("an unknown error occurred merging configuration sources: %s", err)
		}
	}

	if v.GetBool("developer.dumpconfig") {
		fmt.Printf("Dumping final merged configuration from Viper: \n\n%s\n\n", v.AllSettings())
	}

	// Get everything out of our temporary Viper store and unmarshall it into a new AppConfig.
	var newConfig AppConfig
	if err := v.Unmarshal(&newConfig); err != nil {
		return fmt.Errorf("unable to parse configuration: %s \n\n(is the configuration valid?)", err)
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
		if newConfig.Log != cm.currentConfig.Log {
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

func parseTOMLSubscribersFromString(subscribers string) string {
	var tomlSubscribers []string
	for _, s := range strings.Split(subscribers, ";") {
		subscriber := strings.ReplaceAll(s, ",", "\n")
		tomlSubscribers = append(tomlSubscribers, "[[subscriber]]\n"+subscriber)
	}

	return strings.Join(tomlSubscribers, "\n")
}
