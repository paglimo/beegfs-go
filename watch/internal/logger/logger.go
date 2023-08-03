// Package logger implements logging for the application. Its goal is to
// allowing logging capabilities be easily extended in the future while
// requiring minimal changes elsewhere in the application.
package logger

import (
	"fmt"
	"log/syslog"
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
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
	Level             int8              `mapstructure:"level"`
	MaxSize           int               `mapstructure:"maxSize"`
	NumRotatedFiles   int               `mapstructure:"numRotatedFiles"`
	IncomingEventRate bool              `mapstructure:"incomingEventRate"`
	Developer         bool              `mapstructure:"developer"`
}

type supportedLogTypes string

const (
	StdOut  supportedLogTypes = "stdout"
	LogFile supportedLogTypes = "logfile"
	// The syslog type is the slowest logging option due to how zap log messages
	// need to be translated to syslog messages and severity levels.
	Syslog supportedLogTypes = "syslog"
)

// All SupportedLogTypes should also be added to this slice.
// It is used for printing help text, for example if an invalid type is specified.
var SupportedLogTypes = []supportedLogTypes{
	StdOut,
	LogFile,
	Syslog,
}

// New parses command line logging options and returns an appropriately configured logger.
func New(newConfig Config) (*Logger, error) {

	logMgr := Logger{}
	if err := logMgr.updateConfiguration(newConfig); err != nil {
		return nil, err
	}

	return &logMgr, nil
}

// updateConfiguration sets the Logger's zap.Logger based on the provided
// AppConfig. Once BF-48 is resolved it can be exported so the logging
// configuration can be dynamically updated using ConfigMgr.
func (lm *Logger) updateConfiguration(configs ...any) error {

	if len(configs) != 1 {
		return fmt.Errorf("invalid configuration provided (expected only logging configuration)")
	}

	newConfig, ok := configs[0].(Config)
	if !ok {
		return fmt.Errorf("invalid configuration provided (expected logging configuration)")
	}

	lm.configLock.Lock()
	defer lm.configLock.Unlock()

	// Use the opinionated Zap development configuration.
	// This notably gives us stack traces at warn and error levels.
	if newConfig.Developer {
		l, err := zap.NewDevelopmentConfig().Build()
		if err != nil {
			return err
		}
		lm.Logger = l
		return nil
	}

	// Otherwise build a production config based on the user settings:
	zapConfig := zap.NewProductionEncoderConfig()
	zapConfig.TimeKey = "timestamp"
	zapConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	// Then setup the encoder that will turn our log entries into byte slices.
	// For now just log in plaintext and don't expose an option to log using
	// JSON. We can always add an option later with zapcore.NewJSONEncoder() if
	// needed. IMPORTANT: If the encoding type ever changes then the way we
	// handle writing to syslog in SyslogWriteSyncer.Write() MUST be updated
	// accordingly.
	zapEncoder := zapcore.NewConsoleEncoder(zapConfig)

	// We'll map Zap levels to standard BeeGFS log levels. The use of an atomic
	// level means we can change this after the application has started.
	var zapLevel zap.AtomicLevel
	switch newConfig.Level {
	case 1:
		zapLevel = zap.NewAtomicLevelAt(zap.WarnLevel)
	case 3:
		zapLevel = zap.NewAtomicLevelAt(zap.InfoLevel)
	case 5:
		zapLevel = zap.NewAtomicLevelAt(zap.DebugLevel)
	}

	// zapcore.WriteSyncers are what handle writing the byte slices from the
	// encoder somewhere. This means we can easily add support for new types of
	// logging (i.e., log destinations) by simply swapping out the WriteSyncer.
	var logDestination zapcore.WriteSyncer
	switch newConfig.Type {
	case StdOut:
		logDestination = zapcore.AddSync(os.Stdout)
	case LogFile:
		// Just being able to write to the provided log file is not sufficient
		// if we want to rotate log files. Make sure the directory selected for
		// logging exists and we can write to it.
		if err := ensureLogsAreWritable(newConfig.File); err != nil {
			return err
		}

		logDestination = zapcore.AddSync(&lumberjack.Logger{
			Filename:   newConfig.File,
			MaxSize:    newConfig.MaxSize,
			MaxBackups: newConfig.NumRotatedFiles,
		})
	case Syslog:
		// By default we'll log at severity level info. Typically we'll be able
		// to parse out the log level and log at the appropriate severity level.
		// We'll use the process name as the prefix tag in case there are multiple
		// instances of BeeWatch running on the same server.
		l, err := NewSyslogWriteSyncer(syslog.LOG_INFO|syslog.LOG_LOCAL0, os.Args[0])
		if err != nil {
			return fmt.Errorf("unable to initialize syslog destination: %w", err)
		}
		logDestination = l
	default:
		return fmt.Errorf("unsupported log type: %s", newConfig.Type)
	}

	lm.Logger = zap.New(zapcore.NewCore(zapEncoder, logDestination, zapLevel))
	return nil
}
