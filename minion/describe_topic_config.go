package minion

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func (s *Service) GetTopicConfigs(ctx context.Context) (*kmsg.DescribeConfigsResponse, error) {
	metadata, err := s.GetMetadataCached(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}

	req := s.newTopicConfigsRequest(metadata.Topics)
	if len(req.Resources) == 0 {
		return kmsg.NewPtrDescribeConfigsResponse(), nil
	}

	res, err := req.RequestWith(ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("failed to request topic configs: %w", err)
	}

	return res, nil
}

func (s *Service) newTopicConfigsRequest(topics []kmsg.MetadataResponseTopic) kmsg.DescribeConfigsRequest {
	req := kmsg.NewDescribeConfigsRequest()
	for _, topic := range topics {
		if !s.IsTopicAllowed(*topic.Topic) {
			continue
		}
		resourceReq := kmsg.NewDescribeConfigsRequestResource()
		resourceReq.ResourceType = kmsg.ConfigResourceTypeTopic
		resourceReq.ResourceName = *topic.Topic
		req.Resources = append(req.Resources, resourceReq)
	}

	return req
}
