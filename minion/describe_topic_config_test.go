package minion

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestNewTopicConfigsRequest(t *testing.T) {
	topics := []kmsg.MetadataResponseTopic{
		{Topic: kmsg.StringPtr("orders")},
		{Topic: kmsg.StringPtr("orders-internal")},
		{Topic: kmsg.StringPtr("payments")},
	}

	tests := []struct {
		name    string
		allowed []string
		ignored []string
		want    []string
	}{
		{
			name:    "only allowed topics",
			allowed: []string{"orders", "payments"},
			want:    []string{"orders", "payments"},
		},
		{
			name:    "ignored topics take precedence",
			allowed: []string{"/^orders/"},
			ignored: []string{"orders-internal"},
			want:    []string{"orders"},
		},
		{
			name:    "default allows all topics",
			allowed: []string{"/.*/"},
			want:    []string{"orders", "orders-internal", "payments"},
		},
		{
			name:    "no matching topics",
			allowed: []string{"missing"},
		},
		{
			name: "empty allow list",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{}
			var err error
			s.AllowedTopicsExpr, err = compileRegexes(tt.allowed)
			require.NoError(t, err)
			s.IgnoredTopicsExpr, err = compileRegexes(tt.ignored)
			require.NoError(t, err)

			req := s.newTopicConfigsRequest(topics)

			var names []string
			for _, resource := range req.Resources {
				assert.Equal(t, kmsg.ConfigResourceTypeTopic, resource.ResourceType)
				names = append(names, resource.ResourceName)
			}
			assert.Equal(t, tt.want, names)
		})
	}
}

func TestGetTopicConfigs_NoAllowedTopicsSkipsRequest(t *testing.T) {
	for _, topics := range [][]kmsg.MetadataResponseTopic{
		nil,
		{{Topic: kmsg.StringPtr("denied")}},
	} {
		// A nil client ensures that the empty request path never contacts Kafka.
		s := &Service{
			cache: map[string]interface{}{
				"metadata-default": &kmsg.MetadataResponse{Topics: topics},
			},
		}
		var err error
		s.AllowedTopicsExpr, err = compileRegexes([]string{"allowed"})
		require.NoError(t, err)

		res, err := s.GetTopicConfigs(context.Background())

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Empty(t, res.Resources)
	}
}
