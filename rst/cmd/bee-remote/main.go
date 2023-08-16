package main

import (
	"fmt"
	"log"
	"os"

	"github.com/spf13/pflag"
	"github.com/thinkparq/bee-remote/internal/config"
	"github.com/thinkparq/gobee/configmgr"
)

const (
	envVarPrefix = "BEEREMOTE_"
)

func main() {
	// All application configuration (AppConfig) can be set using flags. The
	// default values specified here will be used as configuration defaults.
	// Note defaults for configuration specified using a slice are not set here.
	// Notably remote storage target defaults are handled as part of
	// initializing a particular RST type.
	pflag.String("cfgFile", "", "The path to the a configuration file (can be omitted to set all configuration using flags and/or environment variables). When Remote Storage Targets are configured using a file, they can be updated without restarting the application.")
	// Hidden flags:
	pflag.Int("developer.perfProfilingPort", 0, "Specify a port where performance profiles will be made available on the localhost via pprof (0 disables performance profiling).")
	pflag.CommandLine.MarkHidden("developer.perfProfilingPort")
	pflag.Bool("developer.dumpConfig", false, "Dump the full configuration and immediately exit.")
	pflag.CommandLine.MarkHidden("developer.dumpConfig")

	pflag.CommandLine.SortFlags = false
	pflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		pflag.PrintDefaults()
		helpText := `
Further info:
	Except for Remote Storage Targets, configuration may be set using a mix of flags, environment variables, and values from a TOML configuration file. 
	Configuration will be merged using the following precedence order (highest->lowest): (1) flags (2) environment variables (3) configuration file (4) defaults.
	Remote Storage Targets can only be specified using one of these options, and when set using a configuration file, can be updated dynamically after the application starts without by sending a hangup signal (SIGHUP).
Using environment variables:
	To specify configuration using environment variables specify %sKEY=VALUE where KEY is the flag name you want to specify in all capitals replacing dots (.) with underscores (_).
	Examples: 
	export %sLOG_DEBUG=true
	export %sREMOTE_STORAGE_TARGETS="id=1,name='rst1',type='s3';id=2,name='rst2',type='s3'"
`
		fmt.Fprintf(os.Stderr, helpText, envVarPrefix, envVarPrefix, envVarPrefix)
		os.Exit(0)
	}
	pflag.Parse()

	// We initialize ConfigManager first because all components require the initial config to start up.
	cfgMgr, err := configmgr.New(pflag.CommandLine, envVarPrefix, &config.AppConfig{}, config.SetRSTTypeHook())
	if err != nil {
		log.Fatalf("unable to get initial configuration: %s", err)
	}
	c := cfgMgr.Get()
	initialCfg, ok := c.(*config.AppConfig)
	if !ok {
		log.Fatalf("configuration manager returned invalid configuration (expected BeeWatch application configuration)")
	}

	if initialCfg.Developer.DumpConfig {
		fmt.Printf("Dumping AppConfig and exiting...\n\n")
		fmt.Printf("%+v\n", initialCfg)
		// TODO: Remove unless it later turns out this actually applies.
		// fmt.Println(`
		// WARNING: Configuration listed here for individual Remote Storage Targets may not reflect their final configuration.
		// Individual RST types may define their own custom defaults, or automatically override invalid user configuration.
		// `)
		os.Exit(0)
	}

}
