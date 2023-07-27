package configmgr

import (
	"io"
	"os"

	"git.beegfs.io/beeflex/bee-watch/internal/subscriber"
	"github.com/pelletier/go-toml"
)

type AppConfig struct {
	Logging struct {
		LogType    string `toml:"logType"`
		LogStdFile string `toml:"logStdFile"`
		LogDebug   bool   `toml:"logDebug"`
	}
	Metadata struct {
		SysFileEventLogTarget         string `toml:"sysFileEventLogTarget"`
		SysFileEventBufferSize        int    `toml:"sysFileEventBufferSize"`
		SysFileEventBufferGCFrequency int    `toml:"sysFileEventBufferGCFrequency"`
		SysFileEventPollFrequency     int    `toml:"sysFileEventPollFrequency"`
	}
	Subscribers []subscriber.BaseConfig `toml:"subscriber"`
	Developer   struct {
		PerfLogIncomingEventRate bool `toml:"perfLogIncomingEventRate"`
		PerfProfilePort          int  `toml:"perfProfilePort"`
	}
}

func getAppConfigFromFile(configFile string) (*AppConfig, error) {

	file, err := os.Open(configFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	b, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var config AppConfig
	err = toml.Unmarshal(b, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}
