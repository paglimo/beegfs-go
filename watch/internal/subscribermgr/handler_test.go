package subscribermgr

import (
	"testing"

	"git.beegfs.io/beeflex/bee-watch/internal/subscriber"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestNewHandler(t *testing.T) {

	logger, _ := zap.NewDevelopment()

	t.Run("verify defaults are set correctly", func(t *testing.T) {
		subscriber := &subscriber.BaseSubscriber{}
		handler := newHandler(logger, subscriber)
		assert.Equal(t, defaultQueueSize, handler.BaseSubscriber.QueueSize)
		assert.Equal(t, defaultOfflineBufferSize, handler.BaseSubscriber.OfflineBufferSize)
		assert.NotNil(t, handler.unacknowledgedEvents)
		assert.NotNil(t, handler.offlineEvents)
		assert.Equal(t, defaultQueueSize, cap(handler.queue))
		assert.NotNil(t, handler.log)
		assert.NotNil(t, handler.BaseSubscriber)
		assert.NotNil(t, handler.ctx)
		assert.NotNil(t, handler.cancel)
	})

	t.Run("verify provided values are set correctly", func(t *testing.T) {
		subscriber := &subscriber.BaseSubscriber{QueueSize: 1000, OfflineBufferSize: 5096}
		handler := newHandler(logger, subscriber)
		assert.Equal(t, 1000, handler.BaseSubscriber.QueueSize)
		assert.Equal(t, 5096, handler.BaseSubscriber.OfflineBufferSize)
		assert.NotNil(t, handler.unacknowledgedEvents)
		assert.NotNil(t, handler.offlineEvents)
		assert.Equal(t, 1000, cap(handler.queue))
		assert.NotNil(t, handler.log)
		assert.NotNil(t, handler.BaseSubscriber)
		assert.NotNil(t, handler.ctx)
		assert.NotNil(t, handler.cancel)
	})

	t.Run("verify provided values are adjusted if needed", func(t *testing.T) {
		subscriber := &subscriber.BaseSubscriber{QueueSize: 0, OfflineBufferSize: 10}
		handler := newHandler(logger, subscriber)
		assert.Equal(t, defaultQueueSize, handler.BaseSubscriber.QueueSize)
		assert.Equal(t, defaultQueueSize+defaultUnackEventsBufferSize, handler.BaseSubscriber.OfflineBufferSize)
		assert.NotNil(t, handler.unacknowledgedEvents)
		assert.NotNil(t, handler.offlineEvents)
		assert.Equal(t, defaultQueueSize, cap(handler.queue))
		assert.NotNil(t, handler.log)
		assert.NotNil(t, handler.BaseSubscriber)
		assert.NotNil(t, handler.ctx)
		assert.NotNil(t, handler.cancel)
	})

}
