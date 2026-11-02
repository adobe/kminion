package e2e

import (
	"sync"
	"time"
)

const (
	_ = iota
	EndToEndMessageStateCreated
	EndToEndMessageStateProducedSuccessfully
)

type EndToEndMessage struct {
	MinionID  string `json:"minionID"`     // unique for each running kminion instance
	MessageID string `json:"messageID"`    // unique for each message
	Timestamp int64  `json:"createdUtcNs"` // when the message was created, unix nanoseconds

	// The following properties are only used within the message tracker
	partition int

	// mu guards state and produceLatency: they are written by the produce-ack callback goroutine
	// and read by the message tracker's onMessageExpired eviction goroutine.
	mu             sync.RWMutex
	state          int
	produceLatency float64
}

func (m *EndToEndMessage) creationTime() time.Time {
	return time.Unix(0, m.Timestamp)
}

// markProducedSuccessfully records that the message was successfully produced along with its ack latency.
func (m *EndToEndMessage) markProducedSuccessfully(latencySeconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = EndToEndMessageStateProducedSuccessfully
	m.produceLatency = latencySeconds
}

func (m *EndToEndMessage) getState() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

func (m *EndToEndMessage) getProduceLatency() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.produceLatency
}
