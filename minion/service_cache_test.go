package minion

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestService_SetCachedItem_ResetPreventsEarlyEviction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &Service{
			cache:       make(map[string]interface{}),
			cacheTimers: make(map[string]*time.Timer),
		}

		s.setCachedItem("k", "first", 30*time.Millisecond)
		time.Sleep(15 * time.Millisecond)
		s.setCachedItem("k", "second", 30*time.Millisecond) // resets the deadline

		time.Sleep(20 * time.Millisecond) // 35ms since first set -- old code would have evicted here
		synctest.Wait()
		val, exists := s.getCachedItem("k")
		require.True(t, exists, "resetting the key should have cancelled the earlier deleter")
		assert.Equal(t, "second", val)

		time.Sleep(20 * time.Millisecond) // 40ms since the second set -- should now be expired
		synctest.Wait()
		_, exists = s.getCachedItem("k")
		assert.False(t, exists, "the key should eventually expire")
	})
}
