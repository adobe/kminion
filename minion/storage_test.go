package minion

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

func TestStorage_IsReady_DefaultsFalse(t *testing.T) {
	s, err := newStorage(zap.NewNop())
	require.NoError(t, err)
	assert.False(t, s.isReady())
}

func TestStorage_SetReadyState(t *testing.T) {
	s, err := newStorage(zap.NewNop())
	require.NoError(t, err)

	s.setReadyState(true)
	assert.True(t, s.isReady())

	s.setReadyState(false)
	assert.False(t, s.isReady())
}

func TestStorage_AddAndDeleteOffsetCommit(t *testing.T) {
	s, err := newStorage(zap.NewNop())
	require.NoError(t, err)
	s.setReadyState(true)

	key := kmsg.OffsetCommitKey{Group: "g1", Topic: "t1", Partition: 0}
	value := kmsg.OffsetCommitValue{Offset: 42, CommitTimestamp: 1000}

	s.addOffsetCommit(key, value)

	offsets := s.getGroupOffsets(func(groupName, groupState string) bool { return true })
	require.Contains(t, offsets, "g1")
	require.Contains(t, offsets["g1"], "t1")
	commit := offsets["g1"]["t1"][0]
	assert.Equal(t, int64(42), commit.Value.Offset)
	assert.Equal(t, 1, commit.CommitCount)

	// Committing again to the same group/topic/partition increments CommitCount.
	s.addOffsetCommit(key, value)
	offsets = s.getGroupOffsets(func(groupName, groupState string) bool { return true })
	assert.Equal(t, 2, offsets["g1"]["t1"][0].CommitCount)

	s.deleteOffsetCommit(key)
	offsets = s.getGroupOffsets(func(groupName, groupState string) bool { return true })
	assert.NotContains(t, offsets, "g1")
}

func TestStorage_GetGroupOffsets_NotReady_ReturnsEmpty(t *testing.T) {
	s, err := newStorage(zap.NewNop())
	require.NoError(t, err)
	// storage starts not-ready; offsets must not be exposed yet even if some exist.
	s.addOffsetCommit(kmsg.OffsetCommitKey{Group: "g1", Topic: "t1", Partition: 0}, kmsg.OffsetCommitValue{Offset: 1})

	offsets := s.getGroupOffsets(func(groupName, groupState string) bool { return true })
	assert.Empty(t, offsets)
}

func TestStorage_MarkRecordConsumed_TracksProgress(t *testing.T) {
	s, err := newStorage(zap.NewNop())
	require.NoError(t, err)

	s.markRecordConsumed(&kgo.Record{Partition: 3, Offset: 99})

	consumed := s.getConsumedOffsets()
	require.Contains(t, consumed, int32(3))
	assert.Equal(t, int64(99), consumed[3])
	assert.Equal(t, float64(1), s.getNumberOfConsumedRecords())
}
