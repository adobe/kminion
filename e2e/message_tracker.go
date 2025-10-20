package e2e

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jellydator/ttlcache/v3"

	"go.uber.org/zap"
)

// messageTracker keeps track of the messages' lifetime
//
// When we successfully send a mesasge, it will be added to this tracker.
// Later, when we receive the message back in the consumer, the message is marked as completed and removed from the tracker.
// If the message does not arrive within the configured `consumer.roundtripSla`, it is counted as lost. Messages that
// failed to be produced will not be
// considered as lost message.
//
// We use a dedicated counter to track messages that couldn't be  produced to Kafka.
type messageTracker struct {
	svc    *Service
	logger *zap.Logger
	cache  *ttlcache.Cache[string, *EndToEndMessage]
}

func newMessageTracker(svc *Service) *messageTracker {
	defaultExpirationDuration := svc.config.Consumer.RoundtripSla
	cache := ttlcache.New[string, *EndToEndMessage](
		ttlcache.WithTTL[string, *EndToEndMessage](defaultExpirationDuration),
	)

	t := &messageTracker{
		svc:    svc,
		logger: svc.logger.Named("message_tracker"),
		cache:  cache,
	}

	cache.OnEviction(func(ctx context.Context, reason ttlcache.EvictionReason, item *ttlcache.Item[string, *EndToEndMessage]) {
		t.onMessageExpired(item.Key(), reason, item.Value())
	})

	// Start the cache's automatic cleanup
	go cache.Start()

	return t
}

func (t *messageTracker) addToTracker(msg *EndToEndMessage) {
	t.cache.Set(msg.MessageID, msg, ttlcache.DefaultTTL)
}

// updateItemIfExists only updates a message if it still exists in the cache. The remaining time to live will not
// be refreshed.
// If it doesn't exist an error will be returned.
//
//nolint:unused
func (t *messageTracker) updateItemIfExists(msg *EndToEndMessage) error {
	item := t.cache.Get(msg.MessageID)
	if item == nil {
		return fmt.Errorf("item not found")
	}

	// Calculate the remaining TTL to preserve the original expiration time
	remainingTTL := time.Until(item.ExpiresAt())
	if remainingTTL < 0 {
		// This entry should have been deleted already. Race condition.
		return fmt.Errorf("item expired")
	}

	// Set the updated message with the remaining TTL
	t.cache.Set(msg.MessageID, msg, remainingTTL)

	return nil
}

// removeFromTracker removes an entry from the cache. If the key does not exist it will return an error.
func (t *messageTracker) removeFromTracker(messageID string) error {
	// Check if the item exists before trying to delete it
	if !t.cache.Has(messageID) {
		return fmt.Errorf("item not found")
	}
	t.cache.Delete(messageID)
	return nil
}

func (t *messageTracker) onMessageArrived(arrivedMessage *EndToEndMessage) {
	item := t.cache.Get(arrivedMessage.MessageID)
	if item == nil {
		// message expired and was removed from the cache
		// it arrived too late, nothing to do here...
		return
	}

	msg := item.Value()

	expireTime := msg.creationTime().Add(t.svc.config.Consumer.RoundtripSla)
	isExpired := time.Now().Before(expireTime)
	latency := time.Since(msg.creationTime())

	if !isExpired {
		// Message arrived late, but was still in cache. We don't increment the lost counter here because eventually
		// it will be evicted from the cache. This case should only pop up if the sla time is exceeded, but if the
		// item has not been evicted from the cache yet.
		t.logger.Info("message arrived late, will be marked as a lost message",
			zap.Int64("delay_ms", latency.Milliseconds()),
			zap.String("id", msg.MessageID))
		return
	}

	// message arrived early enough
	pID := strconv.Itoa(msg.partition)
	t.svc.messagesReceived.WithLabelValues(pID).Inc()
	t.svc.roundtripLatency.WithLabelValues(pID).Observe(latency.Seconds())

	// Remove message from cache, so that we don't track it any longer and won't mark it as lost when the entry expires.
	t.cache.Delete(msg.MessageID)
}

func (t *messageTracker) onMessageExpired(key string, reason ttlcache.EvictionReason, msg *EndToEndMessage) {
	if reason == ttlcache.EvictionReasonDeleted {
		// We are not interested in messages that have been removed by us!
		return
	}

	created := msg.creationTime()
	age := time.Since(created)
	t.svc.lostMessages.WithLabelValues(strconv.Itoa(msg.partition)).Inc()

	t.logger.Debug("message expired/lost",
		zap.Int64("age_ms", age.Milliseconds()),
		zap.Int("partition", msg.partition),
		zap.String("message_id", msg.MessageID),
		zap.Bool("successfully_produced", msg.state == EndToEndMessageStateProducedSuccessfully),
		zap.Float64("produce_latency_seconds", msg.produceLatency),
	)
}
