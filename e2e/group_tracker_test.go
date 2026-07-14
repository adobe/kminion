package e2e

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldAttemptGroupDeletion(t *testing.T) {
	assert.True(t, shouldAttemptGroupDeletion(false, []string{"g1"}))
	assert.False(t, shouldAttemptGroupDeletion(true, []string{"g1"}),
		"must not retry deletion once disabled by a prior authorization failure")
	assert.False(t, shouldAttemptGroupDeletion(false, nil), "nothing to delete")
}
