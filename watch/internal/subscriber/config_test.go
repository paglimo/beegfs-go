package subscriber

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

var jsonConfig string = `
[
    {
        "type": "grpc",
        "id": "1",
        "name": "bee-remote",
        "hostname":"br-1",
		"port":"1234",
		"allow_insecure":true
    },
    {
        "type": "grpc",
        "id": "2",
        "name": "beegfs-mon",
        "hostname":"bm-1",
		"port":"512312",
		"allow_insecure":false
    },
    {
        "type": "invalid",
        "id": "3",
        "name": "smtp",
		"bad":"field"
    }
]
`

// TestNewSubscribersFromJson also implicitly tests newSubscriberFromConfig and newBaseSubscriber.
// It only tests fields that are unmarshalled from the JSON configuration.
// See the inline notes before adding tests for new/existing subscriber types.
func TestNewSubscribersFromJson(t *testing.T) {

	log, _ := zap.NewProductionConfig().Build()
	configuredSubscribers, err := newSubscribersFromJson(jsonConfig, log)
	assert.NoError(t, err)

	// Expected subscribers should only include subscribers we expect to successfully parse from the JSON.
	// The order matters because we will use that to determine which expected subscriber is compared to the
	// parsed output from the above jsonConfig.
	// In other words the Nth valid entry in jsonConfig should line up with the Nth entry below.
	expectedSubscribers := []Subscriber{
		&GRPCSubscriber{
			BaseSubscriber: BaseSubscriber{
				id:    "1",
				name:  "bee-remote",
				state: DISCONNECTED,
			},
			Hostname:      "br-1",
			Port:          "1234",
			AllowInsecure: true,
		},
		&GRPCSubscriber{
			BaseSubscriber: BaseSubscriber{
				id:    "2",
				name:  "beegfs-mon",
				state: DISCONNECTED,
			},
			Hostname:      "bm-1",
			Port:          "512312",
			AllowInsecure: false,
		},
	}

	assert.Len(t, configuredSubscribers, len(expectedSubscribers))

	for i, subscriber := range configuredSubscribers {
		// If support is added for new subscriber types they will need to be added to the type switch.
		switch subscriber.Subscriber.(type) {
		case *GRPCSubscriber:
			s := subscriber.Subscriber.(*GRPCSubscriber)
			expectedSubscriber, ok := expectedSubscribers[i].(*GRPCSubscriber)
			assert.True(t, ok) // If s is the wrong subscriber type, then the expected subscriber type won't match.
			// We use the newComparableXSubscriber() functions provided alongside each subscriber implementation
			// to get a comparable view of the expected and configured subscriber.
			assert.Equal(t, newComparableGRPCSubscriber(*expectedSubscriber), newComparableGRPCSubscriber(*s))
		default:
			assert.Fail(t, "unable to determine subscriber type")
		}
	}
}
