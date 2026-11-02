package minion

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestFilterAllowedGroups_TotalCountIsUnfiltered(t *testing.T) {
	s := &Service{
		AllowedGroupIDsExpr: []*regexp.Regexp{regexp.MustCompile("^allowed-.*")},
	}

	groups := []kmsg.ListGroupsResponseGroup{
		{Group: "allowed-1"},
		{Group: "allowed-2"},
		{Group: "denied-1"},
		{Group: "denied-2"},
		{Group: "denied-3"},
	}

	total, allowed := s.filterAllowedGroups(groups)

	assert.Equal(t, 5, total, "total must count ALL groups seen by the cluster, not just allowed ones")
	assert.Len(t, allowed, 2, "only groups matching the allow-list should be returned")
}
