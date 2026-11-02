package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEndToEndProducerConfig_Validate_ErrorMessageIsWellFormed(t *testing.T) {
	cfg := EndToEndProducerConfig{RequiredAcks: "bogus", AckSla: time.Second}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Equal(t, "producer.requiredAcks must be 'all' or 'leader'", err.Error())
}
