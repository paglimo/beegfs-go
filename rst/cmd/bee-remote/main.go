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
	"github.com/thinkparq/gobee/beegfs/beegrpc"
	"github.com/thinkparq/gobee/configmgr"
	"github.com/thinkparq/gobee/filesystem"
	"github.com/thinkparq/gobee/logger"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
)

const (
	envVarPrefix = "BEEREMOTE_"
	// Note the concept of a BeeRemote nodeID will be used to support multiple BeeRemote nodes in the future.
	nodeID            = "0"
	FeatureLicenseStr = "io.beegfs.flex"
)

// Set by the build process using ldflags.
var (
	binaryName = "unknown"
	version    = "unknown"
	commit     = "unknown"
	buildTime  = "unknown"
)

func main() {
	// All application configuration (AppConfig) can be set using flags. The
	// default values specified here will be used as configuration defaults.
	// Note defaults for configuration specified using a slice are not set here.
	// Notably remote storage target defaults are handled as part of
	// initializing a particular RST type.
	pflag.Bool("version", false, "Print the version then exit.")
	pflag.String("cfg-file", "", "The path to the a configuration file (can be omitted to set all configuration using flags and/or environment variables). When Remote Storage Targets are configured using a file, they can be updated without restarting the application.")
	pflag.String("mount-point", "", "The path where BeeGFS is mounted.")
	pflag.String("log.type", "stderr", "Where log messages should be sent ('stderr', 'stdout', 'syslog', 'logfile').")
	pflag.String("log.file", "/var/log/beeremote/beeremote.log", "The path to the desired log file when logType is 'log.file' (if needed the directory and all parent directories will be created).")
	pflag.Int8("log.level", 3, "Adjust the logging level (0=Fatal, 1=Error, 2=Warn, 3=Info, 4+5=Debug).")
	pflag.Int("log.max-size", 1000, "Maximum size of the log.file in megabytes before it is rotated.")
	pflag.Int("log.num-rotated-files", 5, "Maximum number old log.file(s) to keep when log.max-size is reached and the log is rotated.")
	pflag.Bool("log.developer", false, "Enable developer logging including stack traces and setting the equivalent of log.level=5 and log.type=stdout (all other log settings are ignored).")
	pflag.String("management.address", "127.0.0.1:8010", "The hostname:port of the BeeGFS management service.")
	pflag.String("management.tls-ca-cert", "/etc/beegfs/cert.pem", "Use a CA certificate (signed or self-signed) for server verification.")
	pflag.Bool("management.tls-disable-verification", false, "Disable TLS verification for gRPC communication.")
	pflag.Bool("management.tls-disable", false, "Disable TLS for gRPC communication.")
	pflag.String("management.auth-file", "/etc/beegfs/conn.auth", "The file containing the connection authentication shared secret.")
	pflag.Bool("management.auth-disable", false, "Disable connection authentication.")
	pflag.String("server.address", "127.0.0.1:9010", "The hostname:port where BeeRemote should listen for job requests.")
	pflag.String("server.tls-cert-file", "/etc/beegfs/cert.pem", "Path to a certificate file that provides the identify of the BeeRemote gRPC server.")
	pflag.String("server.tls-key-file", "/etc/beegfs/key.pem", "Path to a key file belonging to the certificate for the BeeRemote gRPC server.")
	pflag.String("job.path-db-path", "/var/lib/beegfs/beeremotePathDB", "Path where the jobs database will be created/maintained.")
	pflag.Int("job.path-db-cache-size", 4096, "How many entries from the database should be kept in-memory to speed up access. Entries are evicted first-in-first-out so actual utilization may be higher for any requests actively being modified.")
	pflag.Int("job.request-queue-depth", 1024, "Number of requests that can be made to JobMgr before new requests are blocked.")
	pflag.Int("job.min-job-entries-per-rst", 2, "This many jobs for each RST configured for a particular path is guaranteed to be retained. At minimum this should be set to 1 so we always know the last sync result for an RST.")
	pflag.Int("job.max-job-entries-per-rst", 4, "Once this threshold is exceeded, older jobs will be deleted (oldest-to-newest) until the number of jobs equals the min-job-entries-per-rst.")
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
	Except for Remote Storage Targets, configuration may be set using a mix of flags, environment variables, and values from a TOML configuration file. 
	Configuration will be merged using the following precedence order (highest->lowest): (1) flags (2) environment variables (3) configuration file (4) defaults.
	Remote Storage Targets can only be configured using a TOML config file.
Using environment variables:
	To specify configuration using environment variables specify %sKEY=VALUE where KEY is the flag name you want to specify in all capitals replacing dots (.) with a double underscore (__) and hyphens (-) with an underscore (_).
	Examples: 
	export %sLOG__DEBUG=true
`
		// TODO (https://github.com/ThinkParQ/bee-remote/issues/29): Consider if RSTs should ever be
		// configurable using flags and environment variables. If we want to allow them to be
		// configured using CTL this may never make sense, and better to just store them in a
		// DB/similar and not even allow them to be configured using a config file.
		fmt.Fprintf(os.Stderr, helpText, envVarPrefix, envVarPrefix)
		os.Exit(0)
	}
	pflag.Parse()

	if printVersion, _ := pflag.CommandLine.GetBool("version"); printVersion {
		fmt.Printf("%s %s (commit: %s, built: %s)\n", binaryName, version, commit, buildTime)
		os.Exit(0)
	}

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
	logger.Info("start-of-day", zap.String("application", binaryName), zap.String("version", version))
	logger.Debug("build details", zap.String("commit", commit), zap.String("built", buildTime))
	// Determine if we should use a real or mock mount point:
	logger.Info("checking BeeGFS mount point")
	// If we hang here, probably BeeGFS itself is not reachable.
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

	// The mgmtd gRPC client expects the cert and auth file to already be read from their respective
	// sources (files in this case) and provided as a byte slice.
	var cert []byte
	if initialCfg.Management.TLSCaCert != "" {
		cert, err = os.ReadFile(initialCfg.Management.TLSCaCert)
		if err != nil && pflag.Lookup("management.tls-ca-cert").Changed {
			logger.Fatal("unable to read management TLS certificate", zap.Error(err))
		}
	}
	var authSecret []byte
	if !initialCfg.Management.AuthDisable {
		authSecret, err = os.ReadFile(initialCfg.Management.AuthFile)
		if err != nil {
			logger.Fatal("unable to read management secret from auth file", zap.Error(err))
		}
	}
	// The only thing currently requiring a mgmtd client is license verification. Immediately
	// disconnect client after determining the license status to discourage reuse without first
	// evaluating if a more robust strategy to manage long-term connections to the mgmtd is required
	// for future use cases (such as downloading configuration from mgmtd).
	if mgmtdClient, err := beegrpc.NewMgmtd(
		initialCfg.Management.Address,
		beegrpc.WithTLSDisable(initialCfg.Management.TLSDisable),
		beegrpc.WithTLSDisableVerification(initialCfg.Management.TLSDisableVerification),
		beegrpc.WithTLSCaCert(cert),
		beegrpc.WithAuthSecret(authSecret),
	); err != nil {
		// If the mgmtd is actually offline, usually we'll never get to this point. Startup will
		// hang earlier trying to determine the BeeGFS mount point. This is why we don't do any
		// extra retry logic here, the client will already try to reconnect and return an error
		// after some time. If we do get to this point it is more likely there is a configuration
		// error (for example the wrong mgmtd is configured on BeeRemote). The exception is if the
		// mgmtd just went offline and the client hasn't yet set targets probably-offline. But in
		// that scenario its probably better to refuse to startup prompting investigation.
		logger.Fatal("unable to initialize BeeGFS mgmtd client", zap.Error(err))
	} else {
		licenseDetails, err := mgmtdClient.VerifyLicense(ctx, FeatureLicenseStr)
		mgmtdClient.Cleanup()
		if licenseDetails != nil {
			logger.Info("downloaded license from BeeGFS management service", licenseDetails...)
		}
		if err != nil {
			logger.Fatal("unable to verify license", zap.Error(err))
		}
	}

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
