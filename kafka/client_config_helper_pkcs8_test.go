package kafka

import (
	"encoding/pem"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestDecryptPrivateKey_PKCS8Encrypted_FailsWithClearError(t *testing.T) {
	// A syntactically-valid "ENCRYPTED PRIVATE KEY" PEM block. The contents don't need to be a real
	// PKCS#8 structure since we reject this block type before attempting to parse its contents.
	pemBlock := pem.EncodeToMemory(&pem.Block{Type: "ENCRYPTED PRIVATE KEY", Bytes: []byte("not-real-pkcs8-bytes")})

	_, err := decryptPrivateKey(pemBlock, "somepass", zap.NewNop())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
}
