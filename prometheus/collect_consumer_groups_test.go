package prometheus

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestDecodeMemberAssignments_UnknownProtocolType(t *testing.T) {
	a, err := decodeMemberAssignments("connect", kmsg.DescribeGroupsResponseGroupMember{})
	require.NoError(t, err)
	assert.Nil(t, a)
}

func TestDecodeMemberAssignments_ConsumerProtocolType_InvalidBytes(t *testing.T) {
	member := kmsg.DescribeGroupsResponseGroupMember{MemberAssignment: []byte{0xFF, 0xFF}}
	_, err := decodeMemberAssignments("consumer", member)
	assert.Error(t, err)
}
