package minion

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestService_LogDirsEnabled_ConcurrentAccessNoRace(t *testing.T) {
	s := &Service{logDirsSupported: atomic.NewBool(true)}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		s.logDirsSupported.Store(false)
	}()
	go func() {
		defer wg.Done()
		_ = s.IsLogDirsEnabled()
	}()
	wg.Wait()

	assert.False(t, s.IsLogDirsEnabled())
}
