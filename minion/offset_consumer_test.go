package minion

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPartitionLag(t *testing.T) {
	t.Run("never-consumed single-record partition is NOT considered caught up", func(t *testing.T) {
		// LEO=1 -> highWaterMark = partition.Offset-1 = 0
		lag := partitionLag(0, 0, false)
		assert.Greater(t, lag, int64(0), "a partition that has never been consumed must not report zero lag")
	})

	t.Run("consumed exactly up to the high water mark has zero lag", func(t *testing.T) {
		assert.Equal(t, int64(0), partitionLag(10, 10, true))
	})

	t.Run("lag never goes negative even if consumed offset raced ahead", func(t *testing.T) {
		assert.Equal(t, int64(0), partitionLag(10, 15, true))
	})

	t.Run("partial lag is reported correctly", func(t *testing.T) {
		assert.Equal(t, int64(6), partitionLag(10, 4, true))
	})
}
