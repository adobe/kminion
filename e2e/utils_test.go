package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateHistogramBuckets_SubTenMillisecondSLA_DoesNotPanic(t *testing.T) {
	require.NotPanics(t, func() {
		buckets := createHistogramBuckets(5 * time.Millisecond)
		assert.NotEmpty(t, buckets)
	})
}

func TestCreateHistogramBuckets_NormalSLA(t *testing.T) {
	buckets := createHistogramBuckets(5 * time.Second)
	assert.NotEmpty(t, buckets)
	assert.Equal(t, 0.005, buckets[0])
}
