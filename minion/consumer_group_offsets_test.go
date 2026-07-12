package minion

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestOffsetFetchResponseGroupToLegacy(t *testing.T) {
	t.Run("multiple topics and partitions map correctly", func(t *testing.T) {
		partition0 := kmsg.NewOffsetFetchResponseGroupTopicPartition()
		partition0.Partition = 0
		partition0.Offset = 123
		partition0.LeaderEpoch = 5
		metadata := "some-metadata"
		partition0.Metadata = &metadata
		partition0.ErrorCode = 0

		partition1 := kmsg.NewOffsetFetchResponseGroupTopicPartition()
		partition1.Partition = 1
		partition1.Offset = 456
		partition1.LeaderEpoch = 7
		partition1.ErrorCode = 3 // e.g. UNKNOWN_TOPIC_OR_PARTITION

		topic := kmsg.NewOffsetFetchResponseGroupTopic()
		topic.Topic = "my-topic"
		topic.Partitions = []kmsg.OffsetFetchResponseGroupTopicPartition{partition0, partition1}

		group := kmsg.NewOffsetFetchResponseGroup()
		group.Group = "my-group"
		group.Topics = []kmsg.OffsetFetchResponseGroupTopic{topic}
		group.ErrorCode = 0

		legacy := offsetFetchResponseGroupToLegacy(group)

		require.NotNil(t, legacy)
		require.Len(t, legacy.Topics, 1)
		assert.Equal(t, "my-topic", legacy.Topics[0].Topic)
		require.Len(t, legacy.Topics[0].Partitions, 2)

		p0 := legacy.Topics[0].Partitions[0]
		assert.Equal(t, int32(0), p0.Partition)
		assert.Equal(t, int64(123), p0.Offset)
		assert.Equal(t, int32(5), p0.LeaderEpoch)
		require.NotNil(t, p0.Metadata)
		assert.Equal(t, "some-metadata", *p0.Metadata)
		assert.Equal(t, int16(0), p0.ErrorCode)

		p1 := legacy.Topics[0].Partitions[1]
		assert.Equal(t, int32(1), p1.Partition)
		assert.Equal(t, int64(456), p1.Offset)
		assert.Equal(t, int32(7), p1.LeaderEpoch)
		assert.Nil(t, p1.Metadata)
		assert.Equal(t, int16(3), p1.ErrorCode)
	})

	t.Run("group-level error code is preserved", func(t *testing.T) {
		group := kmsg.NewOffsetFetchResponseGroup()
		group.Group = "errored-group"
		group.ErrorCode = 16 // e.g. NOT_COORDINATOR

		legacy := offsetFetchResponseGroupToLegacy(group)

		require.NotNil(t, legacy)
		assert.Equal(t, int16(16), legacy.ErrorCode)
	})

	t.Run("empty group yields empty non-nil topics slice", func(t *testing.T) {
		group := kmsg.NewOffsetFetchResponseGroup()
		group.Group = "empty-group"

		legacy := offsetFetchResponseGroupToLegacy(group)

		require.NotNil(t, legacy)
		require.NotNil(t, legacy.Topics)
		assert.Empty(t, legacy.Topics)
	})
}

func TestChunkGroups(t *testing.T) {
	mkGroups := func(n int) []string {
		groups := make([]string, n)
		for i := range groups {
			groups[i] = "g" + strconv.Itoa(i)
		}
		return groups
	}

	// flatten reconstructs the original slice from chunks so we can assert no group is lost or duplicated.
	flatten := func(chunks [][]string) []string {
		var out []string
		for _, c := range chunks {
			out = append(out, c...)
		}
		return out
	}

	t.Run("exact multiple of size", func(t *testing.T) {
		groups := mkGroups(500)
		chunks := chunkGroups(groups, 250)
		require.Len(t, chunks, 2)
		assert.Len(t, chunks[0], 250)
		assert.Len(t, chunks[1], 250)
		assert.Equal(t, groups, flatten(chunks))
	})

	t.Run("remainder chunk", func(t *testing.T) {
		groups := mkGroups(2001) // representative of a large fleet
		chunks := chunkGroups(groups, 250)
		require.Len(t, chunks, 9) // 8 full chunks of 250 + a remainder of 1
		for _, c := range chunks[:8] {
			assert.Len(t, c, 250)
		}
		assert.Len(t, chunks[8], 1)
		assert.Equal(t, groups, flatten(chunks))
	})

	t.Run("fewer groups than size is a single chunk", func(t *testing.T) {
		groups := mkGroups(10)
		chunks := chunkGroups(groups, 250)
		require.Len(t, chunks, 1)
		assert.Len(t, chunks[0], 10)
	})

	t.Run("empty input yields no chunks", func(t *testing.T) {
		chunks := chunkGroups(nil, 250)
		assert.Empty(t, chunks)
	})

	t.Run("non-positive size returns a single chunk", func(t *testing.T) {
		groups := mkGroups(5)
		chunks := chunkGroups(groups, 0)
		require.Len(t, chunks, 1)
		assert.Equal(t, groups, chunks[0])
	})
}

func TestConsumerGroupConfigMaxGroupsPerOffsetFetchDefault(t *testing.T) {
	var cfg ConsumerGroupConfig
	cfg.SetDefaults()
	assert.Equal(t, 250, cfg.MaxGroupsPerOffsetFetch,
		"maxGroupsPerOffsetFetch should default to 250 when not set in config")
}
