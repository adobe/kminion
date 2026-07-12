package prometheus

import (
	"context"
	"math"
	"strconv"

	"github.com/cloudhut/kminion/v2/minion"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"go.uber.org/zap"
)

type waterMark struct {
	TopicName     string
	PartitionID   int32
	LowWaterMark  int64
	HighWaterMark int64
}

func (e *Exporter) collectConsumerGroupLags(ctx context.Context, ch chan<- prometheus.Metric) bool {
	if !e.minionSvc.Cfg.ConsumerGroups.Enabled {
		return true
	}

	// Low Watermarks (at the moment they are not needed at all, they could be used to calculate the lag on partitions
	// that don't have any active offsets)
	lowWaterMarks, err := e.minionSvc.ListStartOffsetsCached(ctx)
	if err != nil {
		e.logger.Error("failed to fetch low water marks", zap.Error(err))
		return false
	}
	// High Watermarks
	highWaterMarks, err := e.minionSvc.ListEndOffsetsCached(ctx)
	if err != nil {
		e.logger.Error("failed to fetch low water marks", zap.Error(err))
		return false
	}
	waterMarksByTopic := e.waterMarksByTopic(lowWaterMarks, highWaterMarks)

	// We have two different options to get consumer group offsets - either via the AdminAPI or by consuming the
	// __consumer_offsets topic.
	if e.minionSvc.Cfg.ConsumerGroups.ScrapeMode == minion.ConsumerGroupScrapeModeAdminAPI {
		return e.collectConsumerGroupLagsAdminAPI(ctx, ch, waterMarksByTopic)
	} else {
		return e.collectConsumerGroupLagsOffsetTopic(ctx, ch, waterMarksByTopic)
	}
}

// partitionOffsetLag is a normalized view of one partition's committed offset, used to share lag
// aggregation logic between the AdminAPI and offsets-topic scrape modes.
type partitionOffsetLag struct {
	PartitionID int32
	GroupOffset int64
	CommitCount int // only meaningful for the offsets-topic path; zero for AdminAPI
}

// emitTopicLag computes and emits the per-partition and topic-aggregate lag metrics for one group's
// topic. Callers must only call this when topicMarks is non-nil (known watermarks) -- when a topic
// has no watermarks at all, the caller should skip calling this and omit the aggregate series
// entirely rather than reporting a misleading lag of 0.
func (e *Exporter) emitTopicLag(ch chan<- prometheus.Metric, groupName, topicName string, offsets []partitionOffsetLag, topicMarks map[int32]waterMark, granularity string) (totalCommitCount int) {
	topicLag := float64(0)
	topicOffsetSum := float64(0)
	for _, po := range offsets {
		partitionMark, exists := topicMarks[po.PartitionID]
		if !exists {
			e.logger.Warn("consumer group has committed offsets on a partition we don't have watermarks for",
				zap.String("consumer_group", groupName),
				zap.String("topic_name", topicName),
				zap.Int32("partition_id", po.PartitionID),
				zap.Int64("group_offset", po.GroupOffset))
			continue
		}
		lag := math.Max(0, float64(partitionMark.HighWaterMark-po.GroupOffset))
		topicLag += lag
		topicOffsetSum += float64(po.GroupOffset)
		totalCommitCount += po.CommitCount

		if granularity == minion.ConsumerGroupGranularityTopic {
			continue
		}
		ch <- prometheus.MustNewConstMetric(
			e.consumerGroupTopicPartitionLag, prometheus.GaugeValue, lag, groupName, topicName, strconv.Itoa(int(po.PartitionID)),
		)
	}

	ch <- prometheus.MustNewConstMetric(e.consumerGroupTopicLag, prometheus.GaugeValue, topicLag, groupName, topicName)
	ch <- prometheus.MustNewConstMetric(e.consumerGroupTopicOffsetSum, prometheus.GaugeValue, topicOffsetSum, groupName, topicName)
	return totalCommitCount
}

func (e *Exporter) collectConsumerGroupLagsOffsetTopic(_ context.Context, ch chan<- prometheus.Metric, marks map[string]map[int32]waterMark) bool {
	offsets := e.minionSvc.ListAllConsumerGroupOffsetsInternal()
	for groupName, group := range offsets {
		offsetCommits := 0
		for topicName, topic := range group {
			topicMark, exists := marks[topicName]
			if !exists {
				e.logger.Warn("consumer group has committed offsets on a topic we don't have watermarks for",
					zap.String("consumer_group", groupName), zap.String("topic_name", topicName))
				continue
			}
			offsetsForTopic := make([]partitionOffsetLag, 0, len(topic))
			for partitionID, partition := range topic {
				offsetsForTopic = append(offsetsForTopic, partitionOffsetLag{
					PartitionID: partitionID,
					GroupOffset: partition.Value.Offset,
					CommitCount: partition.CommitCount,
				})
			}
			offsetCommits += e.emitTopicLag(ch, groupName, topicName, offsetsForTopic, topicMark, e.minionSvc.Cfg.ConsumerGroups.Granularity)
		}

		ch <- prometheus.MustNewConstMetric(
			e.offsetCommits,
			prometheus.CounterValue,
			float64(offsetCommits),
			groupName,
		)
	}
	return true
}

func (e *Exporter) collectConsumerGroupLagsAdminAPI(ctx context.Context, ch chan<- prometheus.Metric, marks map[string]map[int32]waterMark) bool {
	isOk := true

	groupOffsets, _ := e.minionSvc.ListAllConsumerGroupOffsetsAdminAPI(ctx)
	for groupName, offsetRes := range groupOffsets {
		err := kerr.ErrorForCode(offsetRes.ErrorCode)
		if err != nil {
			e.logger.Warn("failed to get offsets from consumer group, inner kafka error",
				zap.String("consumer_group", groupName),
				zap.Error(err))
			isOk = false
			continue
		}
		for _, topic := range offsetRes.Topics {
			topicMark, exists := marks[topic.Topic]
			if !exists {
				e.logger.Warn("consumer group has committed offsets on a topic we don't have watermarks for",
					zap.String("consumer_group", groupName), zap.String("topic_name", topic.Topic))
				isOk = false
				continue
			}
			offsetsForTopic := make([]partitionOffsetLag, 0, len(topic.Partitions))
			for _, partition := range topic.Partitions {
				if perr := kerr.ErrorForCode(partition.ErrorCode); perr != nil {
					e.logger.Warn("failed to get consumer group offsets for a partition, inner kafka error",
						zap.String("consumer_group", groupName),
						zap.Error(perr))
					isOk = false
					continue
				}
				offsetsForTopic = append(offsetsForTopic, partitionOffsetLag{
					PartitionID: partition.Partition,
					GroupOffset: partition.Offset,
				})
			}
			e.emitTopicLag(ch, groupName, topic.Topic, offsetsForTopic, topicMark, e.minionSvc.Cfg.ConsumerGroups.Granularity)
		}
	}
	return isOk
}

func (e *Exporter) waterMarksByTopic(lowMarks kadm.ListedOffsets, highMarks kadm.ListedOffsets) map[string]map[int32]waterMark {
	type partitionID = int32
	type topicName = string
	waterMarks := make(map[topicName]map[partitionID]waterMark)

	for topic, lowMarksByPartitionID := range lowMarks {
		_, exists := waterMarks[topic]
		if !exists {
			waterMarks[topic] = make(map[partitionID]waterMark)
		}

		for _, lowOffset := range lowMarksByPartitionID {
			if lowOffset.Err != nil {
				e.logger.Debug("failed to get partition low water mark, inner kafka error",
					zap.String("topic_name", lowOffset.Topic),
					zap.Int32("partition_id", lowOffset.Partition),
					zap.Error(lowOffset.Err))
				continue
			}

			higOffset, exists := highMarks.Lookup(lowOffset.Topic, lowOffset.Partition)
			if !exists {
				e.logger.Error("got low water marks for a topic's partition but no high watermarks",
					zap.String("topic_name", lowOffset.Topic),
					zap.Int32("partition_id", lowOffset.Partition),
					zap.Int64("offset", lowOffset.Offset))
				delete(waterMarks, lowOffset.Topic)
				break // Topic watermarks are invalid -> delete & skip this topic
			}
			if higOffset.Err != nil {
				e.logger.Debug("failed to get partition low water mark, inner kafka error",
					zap.String("topic_name", lowOffset.Topic),
					zap.Int32("partition_id", lowOffset.Partition),
					zap.Error(lowOffset.Err))
				continue
			}

			waterMarks[lowOffset.Topic][lowOffset.Partition] = waterMark{
				TopicName:     lowOffset.Topic,
				PartitionID:   lowOffset.Partition,
				LowWaterMark:  lowOffset.Offset,
				HighWaterMark: higOffset.Offset,
			}
		}
	}

	return waterMarks
}
