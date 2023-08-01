package subscribermgr

import (
	"testing"

	"git.beegfs.io/beeflex/bee-watch/internal/subscriber"
	"git.beegfs.io/beeflex/bee-watch/internal/types"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestNewHandler(t *testing.T) {

	logger, _ := zap.NewDevelopment()

	var handlerConfig = HandlerConfig{
		MaxReconnectBackOff:            5,
		MaxWaitForResponseAfterConnect: 4,
		PollFrequency:                  3,
	}

	t.Run("verify handler is initialized correctly", func(t *testing.T) {
		subscriber := &subscriber.Subscriber{}
		metaEventBuffer := &types.MultiCursorRingBuffer{}
		handler := newHandler(logger, subscriber, metaEventBuffer, handlerConfig)
		assert.Equal(t, 3, handlerConfig.PollFrequency)
		assert.NotNil(t, handler.metaEventBuffer)
		assert.NotNil(t, handler.log)
		assert.NotNil(t, handler.Subscriber)
		assert.NotNil(t, handler.ctx)
		assert.NotNil(t, handler.cancel)
	})
}
