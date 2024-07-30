package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/pflag"
	"github.com/thinkparq/bee-remote/internal/config"
	"github.com/thinkparq/bee-remote/internal/job"
	"github.com/thinkparq/bee-remote/internal/server"
	"github.com/thinkparq/bee-remote/internal/workermgr"
	"github.com/thinkparq/gobee/configmgr"
	"github.com/thinkparq/gobee/filesystem"
	"github.com/thinkparq/gobee/logger"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
)

const (
	envVarPrefix = "BEEREMOTE_"
	// Note the concept of a BeeRemote nodeID will be used to support multiple BeeRemote nodes in the future.
	nodeID = "0"
)

func main() {
	// All application configuration (AppConfig) can be set using flags. The
	// default values specified here will be used as configuration defaults.
	// Note defaults for configuration specified using a slice are not set here.
	// Notably remote storage target defaults are handled as part of
	// initializing a particular RST type.
	pflag.String("cfgFile", "", "The path to the a configuration file (can be omitted to set all configuration using flags and/or environment variables). When Remote Storage Targets are configured using a file, they can be updated without restarting the application.")
	pflag.String("mountPoint", "", "The path where BeeGFS is mounted.")
	pflag.String("log.type", "stdout", "Where log messages should be sent ('stdout', 'syslog', 'logfile').")
	pflag.String("log.file", "/var/log/beeremote/beeremote.log", "The path to the desired log file when logType is 'log.file' (if needed the directory and all parent directories will be created).")
	pflag.Int8("log.level", 3, "Adjust the logging level (1=Warning+Error, 3=Info+Warning+Error, 5=Debug+Info+Warning+Error).")
	pflag.Int("log.maxSize", 1000, "Maximum size of the log.file in megabytes before it is rotated.")
	pflag.Int("log.numRotatedFiles", 5, "Maximum number old log.file(s) to keep when log.maxSize is reached and the log is rotated.")
	pflag.Bool("log.developer", false, "Enable developer logging including stack traces and setting the equivalent of log.level=5 and log.type=stdout (all other log settings are ignored).")
	pflag.String("server.address", "127.0.0.1:9010", "The hostname:port where BeeRemote should listen for job requests.")
	pflag.String("server.tlsCertificate", "/etc/beegfs/cert.pem", "Path to a certificate file.")
	pflag.String("server.tlsKey", "/etc/beegfs/key.pem", "Path to a key file.")
	pflag.String("job.pathDBPath", "/var/lib/beegfs/beeremotePathDB", "Path where the jobs database will be created/maintained.")
	pflag.Int("job.pathDBCacheSize", 4096, "How many entries from the database should be kept in-memory to speed up access. Entries are evicted first-in-first-out so actual utilization may be higher for any requests actively being modified.")
	pflag.Int("job.requestQueueDepth", 1024, "Number of requests that can be made to JobMgr before new requests are blocked.")
	pflag.Int("job.minJobEntriesPerRST", 2, "This many jobs for each RST configured for a particular path is guaranteed to be retained. At minimum this should be set to 1 so we always know the last sync result for an RST.")
	pflag.Int("job.maxJobEntriesPerRST", 4, "Once this threshold is exceeded, older jobs will be deleted (oldest-to-newest) until the number of jobs equals the minJobEntriesPerRST.")
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
		log.Fatalf("configuration manager returned invalid configuration (expected BeeRemote application configuration)")
	}

	if initialCfg.Developer.DumpConfig {
		fmt.Printf("Dumping AppConfig and exiting...\n\n")
		fmt.Printf("%+v\n", initialCfg)
		fmt.Println(`
		WARNING: Configuration listed here for individual Remote Storage Targets may not reflect their final configuration.
		Individual RST types may define their own custom defaults, or automatically override invalid user configuration.
		`)
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
	logger.Info("<=== BeeRemote Initialized ===>")

	// Determine if we should use a real or mock mount point:
	mountPoint, err := filesystem.NewFromMountPoint(initialCfg.MountPoint)
	if err != nil {
		logger.Fatal("unable to access BeeGFS mount point", zap.Error(err))
	}

	// Create a channel to receive OS signals to coordinate graceful shutdown:
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	// This context signals the application has received one of the above signals. It is used by the
	// main goroutine to understand when it should start a coordinated/ordered graceful shutdown of
	// all components. Generally it should not be used by individual components to coordinate their
	// shutdown, except if they require a context as part of their setup method to request
	// cancellation of any goroutines that may otherwise block shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-sigs
		cancel()
	}()

	workerManager, err := workermgr.NewManager(ctx, logger.Logger, initialCfg.WorkerMgr, initialCfg.Workers, initialCfg.RemoteStorageTargets, &flex.BeeRemoteNode{
		Id:      nodeID,
		Address: initialCfg.Server.Address,
	}, mountPoint)
	if err != nil {
		logger.Fatal("unable to initialize worker manager", zap.Error(err))
	}

	err = workerManager.Start()
	if err != nil {
		logger.Fatal("unable to start worker manager", zap.Error(err))
	}

	jobManager := job.NewManager(logger.Logger, initialCfg.Job, workerManager)
	err = jobManager.Start()
	if err != nil {
		logger.Fatal("unable to start job manager", zap.Error(err))
	}

	jobServer, err := server.New(logger.Logger, initialCfg.Server, jobManager)
	if err != nil {
		logger.Fatal("failed to initialize BeeRemote server", zap.Error(err))
	}
	go jobServer.ListenAndServe()

	// Block and wait for a shutdown signal:
	<-ctx.Done()
	logger.Info("shutdown signal received")
	jobServer.Stop()
	jobManager.Stop()
	workerManager.Stop()

	logger.Info("shutdown all components, exiting")
}
