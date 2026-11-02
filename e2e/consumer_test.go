package e2e

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestProcessMessage_InvalidJSONIsLoggedAtDebugLevel(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	s := &Service{logger: zap.New(core)}

	s.processMessage(&kgo.Record{Value: []byte("not json")})

	require.Equal(t, 1, logs.Len())
	entry := logs.All()[0]
	assert.Equal(t, zapcore.DebugLevel, entry.Level)
	assert.Equal(t, "failed to unmarshal message value", entry.Message)
}

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
