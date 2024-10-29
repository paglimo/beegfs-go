package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/bee-watch/internal/metadata"
)

func TestUpdateAllowed(t *testing.T) {
	currentConfig := AppConfig{
		Metadata: []metadata.Config{
			{
				EventLogTarget:         "/path",
				EventBufferSize:        100,
				EventBufferGCFrequency: 5,
			},
		},
	}
	newConfig := AppConfig{
		Metadata: []metadata.Config{
			{
				EventLogTarget:         "/path/changed",
				EventBufferSize:        100,
				EventBufferGCFrequency: 5,
			},
		},
	}
	newConfig2 := AppConfig{
		Metadata: []metadata.Config{
			{
				EventLogTarget:         "/path",
				EventBufferSize:        100,
				EventBufferGCFrequency: 5,
			},
			{
				EventLogTarget:         "/new/path",
				EventBufferSize:        100,
				EventBufferGCFrequency: 5,
			},
		},
	}
	assert.NoError(t, currentConfig.UpdateAllowed(&currentConfig))
	assert.Error(t, currentConfig.UpdateAllowed(&newConfig))
	assert.Error(t, currentConfig.UpdateAllowed(&newConfig2))
}
