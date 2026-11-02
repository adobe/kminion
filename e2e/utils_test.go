package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateHistogramBuckets(t *testing.T) {
	tests := []struct {
		name string
		sla  time.Duration
		want []float64
	}{
		{name: "1ms", sla: time.Millisecond, want: []float64{0.005, 0.01, 0.02}},
		{name: "9ms", sla: 9 * time.Millisecond, want: []float64{0.005, 0.01, 0.02}},
		{name: "10ms", sla: 10 * time.Millisecond, want: []float64{0.005, 0.01, 0.02}},
		{name: "5s", sla: 5 * time.Second, want: []float64{0.005, 0.01, 0.02, 0.04, 0.08, 0.16, 0.32, 0.64, 1.28, 2.56, 5.12}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buckets := createHistogramBuckets(tt.sla)
			assert.InDeltaSlice(t, tt.want, buckets, 1e-9)
			assert.GreaterOrEqual(t, buckets[len(buckets)-1], tt.sla.Seconds())
		})
	}
}

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
