package e2e

import (
	"testing"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func newTestServiceForMessageTracker(roundtripSla time.Duration) *Service {
	return &Service{
		config:           Config{Consumer: EndToEndConsumerConfig{RoundtripSla: roundtripSla}},
		logger:           zap.NewNop(),
		messagesReceived: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_messages_received"}, []string{"partition_id"}),
		roundtripLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "test_roundtrip_latency"}, []string{"partition_id"}),
		lostMessages:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_lost_messages"}, []string{"partition_id"}),
	}
}

func TestMessageTracker_OnMessageArrived_WithinSLA_RemovesFromCache(t *testing.T) {
	svc := newTestServiceForMessageTracker(100 * time.Millisecond)
	tracker := newMessageTracker(svc)

	msg := &EndToEndMessage{MessageID: "m1", Timestamp: time.Now().UnixNano(), partition: 0}
	tracker.addToTracker(msg)

	tracker.onMessageArrived(&EndToEndMessage{MessageID: "m1", Timestamp: msg.Timestamp, partition: 0})

	assert.Nil(t, tracker.cache.Get("m1"), "an on-time arrival should be removed from the cache")
}

func TestMessageTracker_OnMessageArrived_TooLate_LeftInCache(t *testing.T) {
	svc := newTestServiceForMessageTracker(50 * time.Millisecond)
	tracker := newMessageTracker(svc)

	// A message "created" well before the SLA window relative to now.
	staleTimestamp := time.Now().Add(-200 * time.Millisecond).UnixNano()
	msg := &EndToEndMessage{MessageID: "m2", Timestamp: staleTimestamp, partition: 0}
	tracker.addToTracker(msg)

	tracker.onMessageArrived(&EndToEndMessage{MessageID: "m2", Timestamp: staleTimestamp, partition: 0})

	// A late arrival must be left in the cache for the cache's own TTL eviction to count as lost --
	// regression test for the arrivedInTime/isExpired naming trap (Task 27): if that boolean's
	// polarity were ever flipped again, this message would be incorrectly removed here.
	assert.NotNil(t, tracker.cache.Get("m2"), "a late-arriving message must not be treated as on-time")
}

func TestMessageTracker_OnMessageExpired_DeletedReason_NoOp(t *testing.T) {
	svc := newTestServiceForMessageTracker(100 * time.Millisecond)
	tracker := newMessageTracker(svc)

	tracker.onMessageExpired("m3", ttlcache.EvictionReasonDeleted, &EndToEndMessage{MessageID: "m3", Timestamp: time.Now().UnixNano(), partition: 0})

	assert.Equal(t, float64(0), testutil.ToFloat64(svc.lostMessages.WithLabelValues("0")))
}

func TestMessageTracker_OnMessageExpired_TimeoutReason_IncrementsLostMessages(t *testing.T) {
	svc := newTestServiceForMessageTracker(100 * time.Millisecond)
	tracker := newMessageTracker(svc)

	tracker.onMessageExpired("m4", ttlcache.EvictionReasonExpired, &EndToEndMessage{MessageID: "m4", Timestamp: time.Now().UnixNano(), partition: 2})

	assert.Equal(t, float64(1), testutil.ToFloat64(svc.lostMessages.WithLabelValues("2")))
}
