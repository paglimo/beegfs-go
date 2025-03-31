package subscribermgr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/beegfs-go/watch/internal/subscriber"
	"github.com/thinkparq/beegfs-go/watch/internal/types"
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
		metaEventBuffer := types.NewMultiCursorRingBuffer(1024, 128)
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
	// newSubscribers := []*subscriber.Subscriber{{ID: 1}, {ID: 3}}
	newSubscribers := []*subscriber.Subscriber{
		{
			Config: subscriber.Config{
				ID: 1,
			},
		},
		{
			Config: subscriber.Config{
				ID: 3,
			},
		},
	}

	toAdd, toRemove, toVerify := evaluateAddedAndRemovedSubscribers(currentHandlers, newSubscribers)
	assert.Contains(t, toAdd, 3)
	assert.Contains(t, toRemove, 2)
	assert.Contains(t, toVerify, 1)
	assert.Len(t, toAdd, 1)
	assert.Len(t, toRemove, 1)
	assert.Len(t, toVerify, 1)
}
