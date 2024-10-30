package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/spf13/pflag"
	"github.com/thinkparq/bee-remote/sync/internal/beeremote"
	"github.com/thinkparq/bee-remote/sync/internal/config"
	"github.com/thinkparq/bee-remote/sync/internal/server"
	"github.com/thinkparq/bee-remote/sync/internal/workmgr"
	"github.com/thinkparq/gobee/configmgr"
	"github.com/thinkparq/gobee/filesystem"
	"github.com/thinkparq/gobee/logger"
	"go.uber.org/zap"
)

const (
	envVarPrefix = "BEESYNC_"
)

// Set by the build process using ldflags.
var (
	binaryName = "unknown"
	version    = "unknown"
	commit     = "unknown"
	buildTime  = "unknown"
)

func main() {
	pflag.Bool("version", false, "Print the version then exit.")
	pflag.String("cfg-file", "", "The path to the a configuration file (can be omitted to set all configuration using flags and/or environment variables).")
	pflag.String("mount-point", "", "The path where BeeGFS is mounted.")
	pflag.String("log.type", "stderr", "Where log messages should be sent ('stderr', 'stdout', 'syslog', 'logfile').")
	pflag.String("log.file", "/var/log/beegfs/beegfs-sync.log", "The path to the desired log file when logType is 'log.file' (if needed the directory and all parent directories will be created).")
	pflag.Int8("log.level", 3, "Adjust the logging level (0=Fatal, 1=Error, 2=Warn, 3=Info, 4+5=Debug).")
	pflag.Int("log.max-size", 1000, "When log.type is 'logfile' the maximum size of the log.file in megabytes before it is rotated.")
	pflag.Int("log.num-rotated-files", 5, "When log.type is 'logfile' the maximum number old log.file(s) to keep when log.max-size is reached and the log is rotated.")
	pflag.Bool("log.developer", false, "Enable developer logging including stack traces and setting the equivalent of log.level=5 and log.type=stdout (all other log settings are ignored).")
	pflag.String("server.address", "127.0.0.1:9011", "The hostname:port where BeeSync should listen for work requests.")
	pflag.String("server.tls-cert-file", "/etc/beegfs/cert.pem", "Path to a certificate file that provides the identify of the BeeSync gRPC server.")
	pflag.String("server.tls-key-file", "/etc/beegfs/key.pem", "Path to a key file belonging to the certificate for the BeeSync gRPC server.")
	pflag.String("manager.journal-db", "/var/lib/beegfs/sync/journal.badger", "The path where a journal of all work requests assigned to this node will be kept.")
	pflag.String("manager.job-db", "/var/lib/beegfs/sync/job.badger", "The path where a database of all jobs with active work requests on this node is stored.")
	pflag.Int("manager.active-work-queue-size", 50000, "The number of work requests to keep in memory. Set this as high as possible, ideally large enough it can contain all work requests assigned to this node.")
	pflag.Int("manager.num-workers", runtime.GOMAXPROCS(0), "The number of workers used to execute work requests in parallel. By default this is automatically set to the number of CPUs.")
	pflag.String("remote.tls-ca-cert", "", "Use a CA certificate (signed or self-signed) for server verification. Leave empty to use the system's default certificate pool to verify the server.")
	pflag.Bool("remote.tls-disable-verification", false, "If TLS verification should be disabled when connecting to BeeGFS Remote (not recommended).")
	pflag.Bool("remote.tls-disable", false, "If TLS should be disabled when connecting to BeeGFS Remote (not recommended).")
	// Hidden flags:
	pflag.Int("developer.perf-profiling-port", 0, "Specify a port where performance profiles will be made available on the localhost via pprof (0 disables performance profiling).")
	pflag.CommandLine.MarkHidden("developer.perf-profiling-port")
	pflag.Bool("developer.dump-config", false, "Dump the full configuration and immediately exit.")
	pflag.CommandLine.MarkHidden("developer.dump-config")

	pflag.CommandLine.SortFlags = false
	pflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		pflag.PrintDefaults()
		helpText := `
Further info:
	Configuration may be set using a mix of flags, environment variables, and values from a TOML configuration file. 
	Configuration will be merged using the following precedence order (highest->lowest): (1) flags (2) environment variables (3) configuration file (4) defaults.
Using environment variables:
	To specify configuration using environment variables specify %sKEY=VALUE where KEY is the flag name you want to specify in all capitals replacing dots (.) with a double underscore (__) and hyphens (-) with an underscore (_).
	Examples: 
	export %sLOG__DEBUG=true
	export %sCONFIG_FILE=/etc/beegfs/bee-sync.toml
`
		fmt.Fprintf(os.Stderr, helpText, envVarPrefix, envVarPrefix, envVarPrefix)
		os.Exit(0)
	}
	pflag.Parse()

	if printVersion, _ := pflag.CommandLine.GetBool("version"); printVersion {
		fmt.Printf("%s %s (commit: %s, built: %s)\n", binaryName, version, commit, buildTime)
		os.Exit(0)
	}

	// We initialize ConfigManager first because all components require the initial config to start up.
	cfgMgr, err := configmgr.New(pflag.CommandLine, envVarPrefix, &config.AppConfig{})
	if err != nil {
		log.Fatalf("unable to get initial configuration: %s", err)
	}
	c := cfgMgr.Get()
	initialCfg, ok := c.(*config.AppConfig)
	if !ok {
		log.Fatalf("configuration manager returned invalid configuration (expected BeeSync application configuration)")
	}

	if initialCfg.Developer.DumpConfig {
		fmt.Printf("Dumping AppConfig and exiting...\n\n")
		fmt.Printf("%+v\n", initialCfg)
		os.Exit(0)
	}

	if initialCfg.Developer.PerfProfilingPort != 0 {
		go func() {
			http.ListenAndServe(fmt.Sprintf(":%d", initialCfg.Developer.PerfProfilingPort), nil)
		}()
	}

	logger, err := logger.New(initialCfg.Log)
	if err != nil {
		log.Fatalf("unable to initialize logger: %s", err)
	}
	defer logger.Sync()
	logger.Info("<=== BeeSync Initialized ===>")
	logger.Info("start-of-day", zap.String("application", binaryName), zap.String("version", version))
	logger.Debug("build details", zap.String("commit", commit), zap.String("built", buildTime))

	// Determine if we should use a real or mock mount point:
	mountPoint, err := filesystem.NewFromMountPoint(initialCfg.MountPoint)
	if err != nil {
		logger.Fatal("unable to access BeeGFS mount point", zap.Error(err))
	}

	// Create a channel to receive OS signals to coordinate graceful shutdown:
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	beeRemoteClient, err := beeremote.New(initialCfg.BeeRemote)
	if err != nil {
		logger.Fatal("failed to initialize BeeRemote client", zap.Error(err))
	}

	workMgr, err := workmgr.NewAndStart(logger.Logger, initialCfg.WorkMgr, beeRemoteClient, mountPoint)
	if err != nil {
		logger.Fatal("failed to initialize work manager", zap.Error(err))
	}

	jobServer, err := server.New(logger.Logger, initialCfg.Server, workMgr)
	if err != nil {
		logger.Fatal("failed to initialize BeeRemote server", zap.Error(err))
	}
	go jobServer.ListenAndServe()

	// Block and wait for a shutdown signal:
	<-sigs
	logger.Info("shutdown signal received")
	jobServer.Stop()
	workMgr.Stop()
	beeRemoteClient.Disconnect()
	logger.Info("shutdown all components, exiting")

}
