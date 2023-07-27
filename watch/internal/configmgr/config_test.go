package configmgr

import (
	"testing"

	"git.beegfs.io/beeflex/bee-watch/internal/subscriber"
	"github.com/stretchr/testify/assert"
)

func TestGetConfig(t *testing.T) {

	expected := &AppConfig{
		Logging: struct {
			LogType    string "toml:\"logType\""
			LogStdFile string "toml:\"logStdFile\""
			LogDebug   bool   "toml:\"logDebug\""
		}{
			LogType:    "stdout",
			LogStdFile: "/var/log/bee-watch.log",
			LogDebug:   false},
		Metadata: struct {
			SysFileEventLogTarget         string "toml:\"sysFileEventLogTarget\""
			SysFileEventBufferSize        int    "toml:\"sysFileEventBufferSize\""
			SysFileEventBufferGCFrequency int    "toml:\"sysFileEventBufferGCFrequency\""
			SysFileEventPollFrequency     int    "toml:\"sysFileEventPollFrequency\""
		}{
			SysFileEventLogTarget:         "/beegfs/meta_01_tgt_0101/socket/beegfs_eventlog",
			SysFileEventBufferSize:        10000000,
			SysFileEventBufferGCFrequency: 100000,
			SysFileEventPollFrequency:     1},
		Subscribers: []subscriber.BaseConfig{
			{Type: "grpc", ID: 1, Name: "test-subscriber", GrpcConfig: subscriber.GrpcConfig{Hostname: "localhost", Port: "50052", AllowInsecure: true}},
			{Type: "grpc", ID: 2, Name: "test2", GrpcConfig: subscriber.GrpcConfig{Hostname: "host2", Port: "2", AllowInsecure: false}}},
		Developer: struct {
			PerfLogIncomingEventRate bool "toml:\"perfLogIncomingEventRate\""
			PerfProfilePort          int  "toml:\"perfProfilePort\""
		}{
			PerfLogIncomingEventRate: false,
			PerfProfilePort:          0,
		},
	}

	cfg, err := getAppConfigFromFile("sample_config.toml")
	assert.NoError(t, err)
	assert.Equal(t, expected, cfg)

	// Can be used to generate expected structs for testing.
	// fmt.Printf("%#v", cfg)
}
