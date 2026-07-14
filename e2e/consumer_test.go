package e2e

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestCoordinatorIDFromAtomicValue(t *testing.T) {
	t.Run("nil value (no coordinator observed yet) returns ok=false instead of panicking", func(t *testing.T) {
		id, ok := coordinatorIDFromAtomicValue(nil)
		assert.False(t, ok)
		assert.Empty(t, id)
	})

	t.Run("valid BrokerMetadata returns its NodeID as a string", func(t *testing.T) {
		id, ok := coordinatorIDFromAtomicValue(kgo.BrokerMetadata{NodeID: 7})
		assert.True(t, ok)
		assert.Equal(t, "7", id)
	})
}
