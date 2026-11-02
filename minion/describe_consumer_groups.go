package minion

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Group types as reported by the ListGroups response (Kafka 3.8+). Groups using the consumer rebalance
// protocol introduced by KIP-848 cannot be described with the classic DescribeGroups API, which answers
// GROUP_ID_NOT_FOUND for them, so they have to be described with ConsumerGroupDescribe instead.
const (
	groupTypeClassic  = "classic"
	groupTypeConsumer = "consumer"
)

// DescribeConsumerGroupsResponse is the describe response of one group coordinator. Groups is nil if the
// coordinator failed to respond, so that the caller can report the failure.
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
		total, allowedGroups := s.filterAllowedGroups(res.Groups)
		res.Groups = allowedGroups
		groups := &GroupsInfo{
			AllGroupsCount: total,
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

// filterAllowedGroups splits groups into the total count (before filtering) and the subset allowed
// by IsGroupAllowed. Keeping the total separate from the filtered slice prevents the count from
// silently becoming "count of allowed groups" if the filtering logic changes later.
func (s *Service) filterAllowedGroups(groups []kmsg.ListGroupsResponseGroup) (total int, allowed []kmsg.ListGroupsResponseGroup) {
	total = len(groups)
	allowed = make([]kmsg.ListGroupsResponseGroup, 0, total)
	for i := range groups {
		if s.IsGroupAllowed(groups[i].Group, groups[i].GroupState) {
			allowed = append(allowed, groups[i])
		}
	}
	return total, allowed
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

	classicGroupIDs, consumerGroupIDs, skippedGroups := partitionGroupIDsByType(listRes.AllowedGroups)
	for _, group := range skippedGroups {
		s.logger.Debug("skipping group with a group type that cannot be described",
			zap.String("group", group.Group),
			zap.String("group_type", group.GroupType))
	}

	// Both batches share the scrape deadline, so they run concurrently. Otherwise a slow coordinator in one
	// batch could use up the deadline before the other batch sends any request.
	var classicGroups, consumerProtocolGroups []DescribeConsumerGroupsResponse
	eg, _ := errgroup.WithContext(ctx)
	eg.Go(func() error {
		classicGroups = s.describeClassicGroups(ctx, classicGroupIDs)
		return nil
	})
	eg.Go(func() error {
		consumerProtocolGroups = s.describeConsumerProtocolGroups(ctx, consumerGroupIDs)
		return nil
	})
	if err := eg.Wait(); err != nil {
		return nil, -1, err
	}

	return append(classicGroups, consumerProtocolGroups...), listRes.AllGroupsCount, nil
}

// partitionGroupIDsByType splits the listed groups into the ones using the classic rebalance protocol and
// the ones using the consumer rebalance protocol. The group type is only reported by ListGroups v5+ (Kafka
// 3.8+), so groups without a reported type are treated as classic ones. Kafka 3.7 early-access KIP-848 groups
// are not detected and are reported as classic groups. Groups of any other type (e.g. share or streams
// groups) cannot be described with either API and are returned as skipped.
func partitionGroupIDsByType(listRes *kmsg.ListGroupsResponse) (classicGroupIDs []string, consumerGroupIDs []string, skippedGroups []kmsg.ListGroupsResponseGroup) {
	classicGroupIDs = make([]string, 0, len(listRes.Groups))
	consumerGroupIDs = make([]string, 0)
	skippedGroups = make([]kmsg.ListGroupsResponseGroup, 0)
	for _, group := range listRes.Groups {
		switch {
		case group.GroupType == "" || strings.EqualFold(group.GroupType, groupTypeClassic):
			classicGroupIDs = append(classicGroupIDs, group.Group)
		case strings.EqualFold(group.GroupType, groupTypeConsumer):
			consumerGroupIDs = append(consumerGroupIDs, group.Group)
		default:
			skippedGroups = append(skippedGroups, group)
		}
	}

	return classicGroupIDs, consumerGroupIDs, skippedGroups
}

// describeClassicGroups describes groups using the classic rebalance protocol via the DescribeGroups API.
func (s *Service) describeClassicGroups(ctx context.Context, groupIDs []string) []DescribeConsumerGroupsResponse {
	if len(groupIDs) == 0 {
		return make([]DescribeConsumerGroupsResponse, 0)
	}

	describeReq := kmsg.NewDescribeGroupsRequest()
	describeReq.Groups = groupIDs
	describeReq.IncludeAuthorizedOperations = false
	shardedResp := s.client.RequestSharded(ctx, &describeReq)

	describedGroups := make([]DescribeConsumerGroupsResponse, 0, len(shardedResp))
	for _, kresp := range shardedResp {
		if kresp.Err != nil {
			s.logger.Warn("broker failed to respond to the described groups request",
				zap.Int32("broker_id", kresp.Meta.NodeID),
				zap.Error(kresp.Err))
			describedGroups = append(describedGroups, DescribeConsumerGroupsResponse{
				BrokerMetadata: kresp.Meta,
				Groups:         nil,
			})
			continue
		}
		res := kresp.Resp.(*kmsg.DescribeGroupsResponse)

		describedGroups = append(describedGroups, DescribeConsumerGroupsResponse{
			BrokerMetadata: kresp.Meta,
			Groups:         res,
		})
	}

	return describedGroups
}

// describeConsumerProtocolGroups describes groups using the consumer rebalance protocol (KIP-848) via the
// ConsumerGroupDescribe API. The responses are converted into DescribeGroups responses so that both group
// types can be exported through the same code path.
func (s *Service) describeConsumerProtocolGroups(ctx context.Context, groupIDs []string) []DescribeConsumerGroupsResponse {
	if len(groupIDs) == 0 {
		return make([]DescribeConsumerGroupsResponse, 0)
	}

	describeReq := kmsg.NewConsumerGroupDescribeRequest()
	describeReq.Groups = groupIDs
	describeReq.IncludeAuthorizedOperations = false
	shardedResp := s.client.RequestSharded(ctx, &describeReq)

	describedGroups := make([]DescribeConsumerGroupsResponse, 0, len(shardedResp))
	for _, kresp := range shardedResp {
		if kresp.Err != nil {
			s.logger.Warn("broker failed to respond to the consumer group describe request",
				zap.Int32("broker_id", kresp.Meta.NodeID),
				zap.Error(kresp.Err))
			describedGroups = append(describedGroups, DescribeConsumerGroupsResponse{
				BrokerMetadata: kresp.Meta,
				Groups:         nil,
			})
			continue
		}
		res := kresp.Resp.(*kmsg.ConsumerGroupDescribeResponse)

		describedGroups = append(describedGroups, DescribeConsumerGroupsResponse{
			BrokerMetadata: kresp.Meta,
			Groups:         consumerGroupDescribeToDescribeGroups(res),
		})
	}

	return describedGroups
}

// consumerGroupDescribeToDescribeGroups converts a ConsumerGroupDescribe response into the equivalent
// DescribeGroups response. Member assignments are re-encoded into the classic wire format so that consumers
// of the response can decode them the same way regardless of the rebalance protocol the group uses.
func consumerGroupDescribeToDescribeGroups(res *kmsg.ConsumerGroupDescribeResponse) *kmsg.DescribeGroupsResponse {
	converted := kmsg.NewPtrDescribeGroupsResponse()
	converted.Version = res.Version
	converted.ThrottleMillis = res.ThrottleMillis
	converted.Groups = make([]kmsg.DescribeGroupsResponseGroup, 0, len(res.Groups))

	for _, group := range res.Groups {
		convertedGroup := kmsg.NewDescribeGroupsResponseGroup()
		convertedGroup.ErrorCode = group.ErrorCode
		convertedGroup.Group = group.Group
		convertedGroup.State = group.State
		// The consumer rebalance protocol has no notion of a protocol type, but every group using it is a
		// consumer group. Reporting it as such keeps the exported labels stable across both protocols, and
		// decodeMemberAssignments requires this value to decode the member assignments.
		convertedGroup.ProtocolType = groupTypeConsumer
		convertedGroup.Protocol = group.AssignorName
		convertedGroup.AuthorizedOperations = group.AuthorizedOperations
		convertedGroup.Members = make([]kmsg.DescribeGroupsResponseGroupMember, 0, len(group.Members))

		for _, member := range group.Members {
			convertedMember := kmsg.NewDescribeGroupsResponseGroupMember()
			convertedMember.MemberID = member.MemberID
			convertedMember.InstanceID = member.InstanceID
			convertedMember.ClientID = member.ClientID
			convertedMember.ClientHost = member.ClientHost
			convertedMember.MemberAssignment = encodeMemberAssignment(member.Assignment)

			convertedGroup.Members = append(convertedGroup.Members, convertedMember)
		}

		converted.Groups = append(converted.Groups, convertedGroup)
	}

	return converted
}

// encodeMemberAssignment encodes a consumer rebalance protocol assignment into the classic member assignment
// wire format. Topics without a name (e.g. a topic ID the broker could not resolve) are skipped, so that no
// series with an empty topic name gets exported. An assignment without any remaining topic partitions is
// encoded as no assignment at all, which is how the classic protocol reports members that have not been
// assigned anything yet.
func encodeMemberAssignment(assignment kmsg.Assignment) []byte {
	kassignment := kmsg.NewConsumerMemberAssignment()
	kassignment.Topics = make([]kmsg.ConsumerMemberAssignmentTopic, 0, len(assignment.TopicPartitions))
	for _, topicPartition := range assignment.TopicPartitions {
		if topicPartition.Topic == "" {
			continue
		}
		topic := kmsg.NewConsumerMemberAssignmentTopic()
		topic.Topic = topicPartition.Topic
		topic.Partitions = topicPartition.Partitions

		kassignment.Topics = append(kassignment.Topics, topic)
	}
	if len(kassignment.Topics) == 0 {
		return nil
	}

	return kassignment.AppendTo(nil)
}
