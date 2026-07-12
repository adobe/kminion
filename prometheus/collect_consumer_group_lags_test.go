package prometheus

import (
	"testing"

	"github.com/cloudhut/kminion/v2/minion"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newTestExporterForLagTests() *Exporter {
	return &Exporter{
		logger:                         zap.NewNop(),
		consumerGroupTopicPartitionLag: prometheus.NewDesc("test_partition_lag", "help", []string{"group_id", "topic_name", "partition_id"}, nil),
		consumerGroupTopicLag:          prometheus.NewDesc("test_topic_lag", "help", []string{"group_id", "topic_name"}, nil),
		consumerGroupTopicOffsetSum:    prometheus.NewDesc("test_topic_offset_sum", "help", []string{"group_id", "topic_name"}, nil),
	}
}

func TestExporter_EmitTopicLag(t *testing.T) {
	e := newTestExporterForLagTests()

	marks := map[int32]waterMark{
		0: {HighWaterMark: 100},
		1: {HighWaterMark: 50},
	}
	offsets := []partitionOffsetLag{
		{PartitionID: 0, GroupOffset: 90, CommitCount: 3},
		{PartitionID: 1, GroupOffset: 60, CommitCount: 2}, // group offset ahead of HWM -> lag must clamp to 0
	}

	ch := make(chan prometheus.Metric, 10)
	commitCount := e.emitTopicLag(ch, "my-group", "my-topic", offsets, marks, minion.ConsumerGroupGranularityPartition)
	close(ch)

	assert.Equal(t, 5, commitCount)

	var metrics []prometheus.Metric
	for m := range ch {
		metrics = append(metrics, m)
	}
	require.Len(t, metrics, 4) // 2 partition-lag + topic-lag + topic-offset-sum

	var pb dto.Metric
	require.NoError(t, metrics[0].Write(&pb))
	assert.Equal(t, float64(10), pb.GetGauge().GetValue()) // partition 0: 100-90

	require.NoError(t, metrics[1].Write(&pb))
	assert.Equal(t, float64(0), pb.GetGauge().GetValue()) // partition 1: clamped to 0

	require.NoError(t, metrics[2].Write(&pb))
	assert.Equal(t, float64(10), pb.GetGauge().GetValue()) // topic lag = 10 + 0

	require.NoError(t, metrics[3].Write(&pb))
	assert.Equal(t, float64(150), pb.GetGauge().GetValue()) // topic offset sum = 90 + 60
}

func TestExporter_EmitTopicLag_SkipsUnknownPartitions(t *testing.T) {
	e := newTestExporterForLagTests()

	marks := map[int32]waterMark{0: {HighWaterMark: 100}}
	// partition 1 has no watermark -> must be skipped, not counted or emitted
	offsets := []partitionOffsetLag{
		{PartitionID: 0, GroupOffset: 90, CommitCount: 1},
		{PartitionID: 1, GroupOffset: 10, CommitCount: 1},
	}

	ch := make(chan prometheus.Metric, 10)
	commitCount := e.emitTopicLag(ch, "g", "t", offsets, marks, minion.ConsumerGroupGranularityPartition)
	close(ch)

	assert.Equal(t, 1, commitCount, "commit count from the partition with no watermark must not be included")

	var metrics []prometheus.Metric
	for m := range ch {
		metrics = append(metrics, m)
	}
	require.Len(t, metrics, 3) // 1 partition-lag (partition 0 only) + topic-lag + topic-offset-sum
}
