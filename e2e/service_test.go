package e2e

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestService_PartitionCount_ConcurrentAccessNoRace(t *testing.T) {
	svc := &Service{}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		svc.partitionCount.Store(6)
	}()

	go func() {
		defer wg.Done()
		_ = svc.partitionCount.Load()
	}()

	wg.Wait()
	assert.Equal(t, int32(6), svc.partitionCount.Load())
}
