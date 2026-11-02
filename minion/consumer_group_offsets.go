package minion

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// ListAllConsumerGroupOffsetsInternal returns a map from the in memory storage. The map value is the offset commit
// value and is grouped by group id, topic, partition id as keys of the nested maps.
func (s *Service) ListAllConsumerGroupOffsetsInternal() map[string]map[string]map[int32]OffsetCommit {
	return s.storage.getGroupOffsets(s.IsGroupAllowed)
}

// ListAllConsumerGroupOffsetsAdminAPI return all consumer group offsets using Kafka's Admin API.
func (s *Service) ListAllConsumerGroupOffsetsAdminAPI(ctx context.Context) (map[string]*kmsg.OffsetFetchResponse, error) {
	groupsRes, err := s.listConsumerGroupsCached(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list groupsRes: %w", err)
	}
	groupIDs := make([]string, len(groupsRes.AllowedGroups.Groups))

	for i, group := range groupsRes.AllowedGroups.Groups {
		groupIDs[i] = group.Group
	}

	return s.listConsumerGroupOffsetsBulk(ctx, groupIDs)
}

// listConsumerGroupOffsetsBulk fetches committed offsets for all given consumer groups using
// batched multi-group OffsetFetch requests (KIP-709, OffsetFetch v8+, Kafka 3.0+).
//
// Groups are chunked at ConsumerGroups.MaxGroupsPerOffsetFetch (the `maxGroupsPerOffsetFetch` config
// option, default 250) and each chunk is issued as one multi-group request. kgo.Client.Request shards
// each request by group coordinator and merges the per-coordinator responses, so thousands of consumer
// groups collapse into a small number of batched requests instead of one request per group. On brokers
// older than v8, franz-go transparently splits each request back into individual per-group requests, so
// this remains compatible with older Kafka/Redpanda clusters.
func (s *Service) listConsumerGroupOffsetsBulk(ctx context.Context, groups []string) (map[string]*kmsg.OffsetFetchResponse, error) {
	res := make(map[string]*kmsg.OffsetFetchResponse, len(groups))

	for _, chunk := range chunkGroups(groups, s.Cfg.ConsumerGroups.MaxGroupsPerOffsetFetch) {
		req := kmsg.NewPtrOffsetFetchRequest()
		for _, group := range chunk {
			reqGroup := kmsg.NewOffsetFetchRequestGroup()
			reqGroup.Group = group
			req.Groups = append(req.Groups, reqGroup)
		}

		kresp, err := s.client.Request(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("failed to request consumer group offsets: %w", err)
		}
		resp, ok := kresp.(*kmsg.OffsetFetchResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type %T for OffsetFetch request", kresp)
		}

		// Per-group errors are carried on each response group's ErrorCode; the prometheus collector
		// already inspects OffsetFetchResponse.ErrorCode via kerr.ErrorForCode and skips errored groups.
		for _, group := range resp.Groups {
			res[group.Group] = offsetFetchResponseGroupToLegacy(group)
		}
	}

	return res, nil
}

// chunkGroups splits groups into consecutive chunks of at most size elements, so that each batched
// OffsetFetch request carries a bounded number of groups regardless of how groups are distributed
// across coordinators. A non-positive size returns a single chunk containing all groups.
func chunkGroups(groups []string, size int) [][]string {
	if size <= 0 {
		return [][]string{groups}
	}
	chunks := make([][]string, 0, (len(groups)+size-1)/size)
	for start := 0; start < len(groups); start += size {
		end := start + size
		if end > len(groups) {
			end = len(groups)
		}
		chunks = append(chunks, groups[start:end])
	}
	return chunks
}

// offsetFetchResponseGroupToLegacy converts a v8+ multi-group OffsetFetch response group into the
// legacy single-group *kmsg.OffsetFetchResponse shape that the rest of the codebase (and the
// prometheus collector) consumes. This keeps the batched fetch an internal implementation detail.
func offsetFetchResponseGroupToLegacy(group kmsg.OffsetFetchResponseGroup) *kmsg.OffsetFetchResponse {
	resp := kmsg.NewPtrOffsetFetchResponse()
	resp.ErrorCode = group.ErrorCode
	resp.Topics = make([]kmsg.OffsetFetchResponseTopic, 0, len(group.Topics))
	for _, topic := range group.Topics {
		legacyTopic := kmsg.NewOffsetFetchResponseTopic()
		legacyTopic.Topic = topic.Topic
		legacyTopic.Partitions = make([]kmsg.OffsetFetchResponseTopicPartition, 0, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			legacyPartition := kmsg.NewOffsetFetchResponseTopicPartition()
			legacyPartition.Partition = partition.Partition
			legacyPartition.Offset = partition.Offset
			legacyPartition.LeaderEpoch = partition.LeaderEpoch
			legacyPartition.Metadata = partition.Metadata
			legacyPartition.ErrorCode = partition.ErrorCode
			legacyTopic.Partitions = append(legacyTopic.Partitions, legacyPartition)
		}
		resp.Topics = append(resp.Topics, legacyTopic)
	}
	return resp
}
