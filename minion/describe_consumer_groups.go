package minion

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

type DescribeConsumerGroupsResponse struct {
	BrokerMetadata kgo.BrokerMetadata
	Groups         *kmsg.DescribeGroupsResponse
}

type GroupsInfo struct {
	AllowedGroups  *kmsg.ListGroupsResponse
	AllGroupsCount int
}

func (s *Service) listConsumerGroupsCached(ctx context.Context) (*GroupsInfo, error) {
	keyAllowedGroups := "list-consumer-groups"

	if cachedRes, exists := s.getCachedItem(keyAllowedGroups); exists {
		return cachedRes.(*GroupsInfo), nil
	}
	groups, err, _ := s.requestGroup.Do(keyAllowedGroups, func() (interface{}, error) {
		res, err := s.listConsumerGroups(ctx)
		if err != nil {
			return nil, err
		}
		allowedGroups := make([]kmsg.ListGroupsResponseGroup, 0)
		for i := range res.Groups {
			if s.IsGroupAllowed(res.Groups[i].Group) {
				allowedGroups = append(allowedGroups, res.Groups[i])
			}
		}
		res.Groups = allowedGroups
		groups := &GroupsInfo{
			AllGroupsCount: len(res.Groups),
			AllowedGroups:  res,
		}
		s.setCachedItem(keyAllowedGroups, groups, 120*time.Second)

		return groups, nil
	})
	if err != nil {
		return nil, err
	}

	return groups.(*GroupsInfo), nil
}

func (s *Service) listConsumerGroups(ctx context.Context) (*kmsg.ListGroupsResponse, error) {
	listReq := kmsg.NewListGroupsRequest()
	res, err := listReq.RequestWith(ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}
	err = kerr.ErrorForCode(res.ErrorCode)
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups. inner kafka error: %w", err)
	}

	return res, nil
}

func (s *Service) DescribeConsumerGroups(ctx context.Context) ([]DescribeConsumerGroupsResponse, int, error) {
	listRes, err := s.listConsumerGroupsCached(ctx)
	if err != nil {
		return nil, -1, err
	}

	groupIDs := make([]string, len(listRes.AllowedGroups.Groups))
	for i, group := range listRes.AllowedGroups.Groups {
		groupIDs[i] = group.Group
	}

	describeReq := kmsg.NewDescribeGroupsRequest()
	describeReq.Groups = groupIDs
	describeReq.IncludeAuthorizedOperations = false
	shardedResp := s.client.RequestSharded(ctx, &describeReq)

	describedGroups := make([]DescribeConsumerGroupsResponse, 0)
	for _, kresp := range shardedResp {
		if kresp.Err != nil {
			s.logger.Warn("broker failed to respond to the described groups request",
				zap.Int32("broker_id", kresp.Meta.NodeID),
				zap.Error(kresp.Err))
			continue
		}
		res := kresp.Resp.(*kmsg.DescribeGroupsResponse)

		describedGroups = append(describedGroups, DescribeConsumerGroupsResponse{
			BrokerMetadata: kresp.Meta,
			Groups:         res,
		})
	}
	return describedGroups, listRes.AllGroupsCount, nil
}
