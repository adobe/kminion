package e2e

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestTopicStatusFromMetadata(t *testing.T) {
	t.Run("no error means topic exists", func(t *testing.T) {
		exists, err := topicStatusFromMetadata(0) // 0 == NONE, no error
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("unknown topic or partition means topic does not exist, no error", func(t *testing.T) {
		exists, err := topicStatusFromMetadata(kerr.UnknownTopicOrPartition.Code)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("other kafka errors are returned as the real typed error, not swallowed as nil", func(t *testing.T) {
		exists, err := topicStatusFromMetadata(kerr.GroupAuthorizationFailed.Code)
		require.Error(t, err)
		assert.False(t, exists)
		assert.ErrorIs(t, err, kerr.GroupAuthorizationFailed)
	})
}
