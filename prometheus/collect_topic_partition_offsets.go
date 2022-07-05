package prometheus

import (
	"context"
	"github.com/cloudhut/kminion/v2/minion"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"go.uber.org/zap"
	"strconv"
)

func (e *Exporter) collectTopicPartitionOffsets(ctx context.Context, ch chan<- prometheus.Metric) bool {
	isOk := true

	// Low Watermarks
	lowWaterMarks, err := e.minionSvc.ListOffsetsCached(ctx, -2)
	if err != nil {
		e.logger.Error("failed to fetch low water marks", zap.Error(err))
		return false
	}
	// High Watermarks
	highWaterMarks, err := e.minionSvc.ListOffsetsCached(ctx, -1)
	if err != nil {
		e.logger.Error("failed to fetch low water marks", zap.Error(err))
		return false
	}

	// Highest Timestamp Offsets
	// NB: this requires Kafka Brokers 3.0+ (see https://issues.apache.org/jira/browse/KAFKA-12541)
	// In older versions this is returning the timestamp of the low watermarks (earliest offset)
	maxTimestampOffsets, err := e.minionSvc.ListOffsetsCached(ctx, -3)
	if err != nil {
		e.logger.Error("failed to fetch offsets for max timestamp", zap.Error(err))
		return false
	}

	// Process Low Watermarks
	for _, topic := range lowWaterMarks.Topics {
		if !e.minionSvc.IsTopicAllowed(topic.Topic) {
			continue
		}

		waterMarkSum := int64(0)
		hasErrors := false
		for _, partition := range topic.Partitions {
			err := kerr.ErrorForCode(partition.ErrorCode)
			if err != nil {
				hasErrors = true
				isOk = false
				continue
			}
			waterMarkSum += partition.Offset
			// Let's end here if partition metrics shall not be exposed
			if e.minionSvc.Cfg.Topics.Granularity == minion.TopicGranularityTopic {
				continue
			}
			ch <- prometheus.MustNewConstMetric(
				e.partitionLowWaterMark,
				prometheus.GaugeValue,
				float64(partition.Offset),
				topic.Topic,
				strconv.Itoa(int(partition.Partition)),
			)
		}
		// We only want to report the sum of all partition marks if we receive watermarks from all partition
		if !hasErrors {
			ch <- prometheus.MustNewConstMetric(
				e.topicLowWaterMarkSum,
				prometheus.GaugeValue,
				float64(waterMarkSum),
				topic.Topic,
			)
		}
	}

	for _, topic := range highWaterMarks.Topics {
		if !e.minionSvc.IsTopicAllowed(topic.Topic) {
			continue
		}
		waterMarkSum := int64(0)
		hasErrors := false
		for _, partition := range topic.Partitions {
			err := kerr.ErrorForCode(partition.ErrorCode)
			if err != nil {
				hasErrors = true
				isOk = false
				continue
			}
			waterMarkSum += partition.Offset
			// Let's end here if partition metrics shall not be exposed
			if e.minionSvc.Cfg.Topics.Granularity == minion.TopicGranularityTopic {
				continue
			}
			ch <- prometheus.MustNewConstMetric(
				e.partitionHighWaterMark,
				prometheus.GaugeValue,
				float64(partition.Offset),
				topic.Topic,
				strconv.Itoa(int(partition.Partition)),
			)
		}
		// We only want to report the sum of all partition marks if we receive watermarks from all partitions
		if !hasErrors {
			ch <- prometheus.MustNewConstMetric(
				e.topicHighWaterMarkSum,
				prometheus.GaugeValue,
				float64(waterMarkSum),
				topic.Topic,
			)
		}
	}

	// Process Max Timestamps
	for _, topic := range maxTimestampOffsets.Topics {
		if !e.minionSvc.IsTopicAllowed(topic.Topic) {
			continue
		}
		topicMaxTimestamp := int64(0)
		hasErrors := false
		for _, partition := range topic.Partitions {
			err := kerr.ErrorForCode(partition.ErrorCode)
			if err != nil {
				hasErrors = true
				isOk = false
				continue
			}
			if topicMaxTimestamp < partition.Timestamp {
				topicMaxTimestamp = partition.Timestamp
			}
			// Let's end here if partition metrics shall not be exposed
			if e.minionSvc.Cfg.Topics.Granularity == minion.TopicGranularityTopic {
				continue
			}
			if partition.Timestamp > 0 {
				ch <- prometheus.MustNewConstMetric(
					e.partitionMaxTimestamp,
					prometheus.GaugeValue,
					float64(partition.Timestamp),
					topic.Topic,
					strconv.Itoa(int(partition.Partition)),
				)
			}
		}
		// We only want to report the max of all partition max timestamps if we receive results from all partitions
		// and the topic is not empty
		if !hasErrors && topicMaxTimestamp > 0 {
			ch <- prometheus.MustNewConstMetric(
				e.topicMaxTimestamp,
				prometheus.GaugeValue,
				float64(topicMaxTimestamp),
				topic.Topic,
			)
		}
	}
	return isOk
}
