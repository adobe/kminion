package e2e

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEndToEndMessage_MarkProducedSuccessfully(t *testing.T) {
	msg := &EndToEndMessage{state: EndToEndMessageStateCreated}

	msg.markProducedSuccessfully(1.5)

	assert.Equal(t, EndToEndMessageStateProducedSuccessfully, msg.getState())
	assert.Equal(t, 1.5, msg.getProduceLatency())
}

func TestEndToEndMessage_ConcurrentAccessNoRace(t *testing.T) {
	msg := &EndToEndMessage{state: EndToEndMessageStateCreated}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		msg.markProducedSuccessfully(0.25)
	}()

	go func() {
		defer wg.Done()
		_ = msg.getState()
		_ = msg.getProduceLatency()
	}()

	wg.Wait()
}
