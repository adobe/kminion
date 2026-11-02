package kafka

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestNewKgoConfig_WarnsOnInsecureSkipTLSVerify(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	cfg := Config{
		TLS: TLSConfig{Enabled: true, InsecureSkipTLSVerify: true},
	}

	_, err := NewKgoConfig(cfg, logger)
	require.NoError(t, err)

	found := false
	for _, entry := range logs.All() {
		if strings.Contains(entry.Message, "TLS certificate verification is disabled") {
			found = true
		}
	}
	assert.True(t, found, "expected a warning log when InsecureSkipTLSVerify is enabled")
}
