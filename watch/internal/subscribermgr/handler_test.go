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

func TestEvaluateChangedSubscribers(t *testing.T) {

	currentHandlers := []*Handler{
		{
			Subscriber: &subscriber.Subscriber{
				Config: subscriber.Config{
					ID: 1,
				},
			},
		},
		{
			Subscriber: &subscriber.Subscriber{
				Config: subscriber.Config{
					ID: 2,
				},
			},
		},
	}
	newSubscribers := []subscriber.Config{{ID: 1}, {ID: 3}}

	toAdd, toRemove := evaluateAddedAndRemovedSubscribers(currentHandlers, newSubscribers)
	assert.Equal(t, []int{3}, toAdd)
	assert.Equal(t, []int{2}, toRemove)
}
