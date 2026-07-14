#!/usr/bin/env bash
set -euo pipefail

# KMinion E2E Integration Test Script
# This script validates KMinion's end-to-end monitoring and built-in metrics
# Can be run locally or in CI

METRICS_URL="${METRICS_URL:-http://localhost:8080/metrics}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-60}"

# Must match e2e/bin/seed-consumer-groups.sh
SEED_GROUP_PREFIX="${SEED_GROUP_PREFIX:-e2e-batch-test-group}"
SEED_GROUP_COUNT="${SEED_GROUP_COUNT:-5}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

# Fetch metrics from KMinion
fetch_metrics() {
    local url="$1"
    if ! curl -sf "$url" 2>/dev/null; then
        log_error "Failed to fetch metrics from $url"
        return 1
    fi
}

# Validate E2E metrics
validate_e2e_metrics() {
    log_info "Validating E2E metrics..."

    local metrics
    metrics=$(fetch_metrics "$METRICS_URL") || return 1

    # Required E2E metrics
    local required_metrics=(
        "kminion_end_to_end_messages_produced_total"
        "kminion_end_to_end_messages_received_total"
        "kminion_end_to_end_produce_latency_seconds"
        "kminion_end_to_end_roundtrip_latency_seconds"
        "kminion_end_to_end_offset_commit_latency_seconds"
    )

    local missing_metrics=()
    for metric in "${required_metrics[@]}"; do
        if ! echo "$metrics" | grep -q "^${metric}"; then
            missing_metrics+=("$metric")
        fi
    done

    if [ ${#missing_metrics[@]} -ne 0 ]; then
        log_error "Missing required E2E metrics:"
        printf '  - %s\n' "${missing_metrics[@]}"
        echo ""
        log_info "Available E2E metrics:"
        echo "$metrics" | grep -E "kminion_end_to_end_" || echo "  (none found)"
        return 1
    fi

    # Verify messages were produced and received
    local produced received
    produced=$(echo "$metrics" | grep "^kminion_end_to_end_messages_produced_total" | awk '{print $2}')
    received=$(echo "$metrics" | grep "^kminion_end_to_end_messages_received_total" | awk '{print $2}')

    if [ -z "$produced" ] || [ "$produced" = "0" ]; then
        log_error "No messages were produced (expected > 0)"
        return 1
    fi

    if [ -z "$received" ] || [ "$received" = "0" ]; then
        log_error "No messages were received (expected > 0)"
        return 1
    fi

    log_info "✅ E2E metrics validation passed"
    log_info "   Messages produced: $produced"
    log_info "   Messages received: $received"
    return 0
}

# Validate built-in KMinion metrics
validate_builtin_metrics() {
    log_info "Validating built-in KMinion metrics..."

    local metrics
    metrics=$(fetch_metrics "$METRICS_URL") || return 1

    # Core exporter metrics
    local core_metrics=(
        "kminion_exporter_up"
        "kminion_exporter_offset_consumer_records_consumed_total"
        "kminion_kafka_received_bytes"
        "kminion_kafka_requests_received_total"
        "kminion_kafka_requests_sent_total"
        "kminion_kafka_sent_bytes"
        "kminion_log_messages_total"
    )

    # Kafka cluster/broker metrics
    local kafka_metrics=(
        "kminion_kafka_cluster_info"
        "kminion_kafka_broker_info"
    )

    # Topic metrics
    local topic_metrics=(
        "kminion_kafka_topic_info"
        "kminion_kafka_topic_info_min_insync_replicas"
        "kminion_kafka_topic_info_partitions_count"
        "kminion_kafka_topic_info_replication_factor"
        "kminion_kafka_topic_info_retention_ms"
        "kminion_kafka_topic_low_water_mark_sum"
        "kminion_kafka_topic_partition_low_water_mark"
        "kminion_kafka_topic_high_water_mark_sum"
        "kminion_kafka_topic_partition_high_water_mark"
        "kminion_kafka_topic_max_timestamp"
        "kminion_kafka_topic_partition_max_timestamp"
    )

    # Consumer group metrics
    local consumer_group_metrics=(
        "kminion_kafka_consumer_group_info"
        "kminion_kafka_consumer_group_info_count"
        "kminion_kafka_consumer_group_info_empty_groups"
        "kminion_kafka_consumer_group_members"
        "kminion_kafka_consumer_group_topic_assigned_partitions"
        "kminion_kafka_consumer_group_topic_lag"
        "kminion_kafka_consumer_group_topic_members"
        "kminion_kafka_consumer_group_topic_offset_sum"
        "kminion_kafka_consumer_group_topic_partition_lag"
    )

    # Log dir metrics
    local logdir_metrics=(
        "kminion_kafka_broker_log_dir_size_total_bytes"
        "kminion_kafka_topic_log_dir_size_total_bytes"
    )

    local missing_metrics=()

    # Check all metric categories
    for metric in "${core_metrics[@]}" "${kafka_metrics[@]}" "${topic_metrics[@]}" "${consumer_group_metrics[@]}" "${logdir_metrics[@]}"; do
        if ! echo "$metrics" | grep -q "^${metric}"; then
            missing_metrics+=("$metric")
        fi
    done

    if [ ${#missing_metrics[@]} -ne 0 ]; then
        log_error "Missing required built-in metrics:"
        printf '  - %s\n' "${missing_metrics[@]}"
        echo ""
        log_info "Available metrics:"
        echo "$metrics" | grep "^kminion_"
        return 1
    fi

    # Validate specific metric values
    local exporter_up
    exporter_up=$(echo "$metrics" | grep "^kminion_exporter_up" | awk '{print $2}')
    if [ "$exporter_up" != "1" ]; then
        log_error "kminion_exporter_up should be 1, got: $exporter_up"
        return 1
    fi

    # Check that cluster info has broker_count label
    if ! echo "$metrics" | grep "kminion_kafka_cluster_info" | grep -q "broker_count"; then
        log_error "kminion_kafka_cluster_info missing broker_count label"
        return 1
    fi

    # Check that we have broker info for at least one broker
    local broker_count
    # `|| true` keeps `set -e` happy when grep matches nothing (the check below handles a 0 count).
    broker_count=$(echo "$metrics" | grep -c "^kminion_kafka_broker_info" || true)
    if [ "$broker_count" -lt 1 ]; then
        log_error "No broker info metrics found"
        return 1
    fi

    log_info "✅ Built-in metrics validation passed"
    log_info "   Exporter up: $exporter_up"
    log_info "   Brokers detected: $broker_count"
    return 0
}

# Validate the batched multi-group OffsetFetch path.
#
# The seed script (seed-consumer-groups.sh) creates SEED_GROUP_COUNT consumer groups with committed
# offsets before KMinion starts. In adminApi scrape mode, KMinion fetches every group's offsets via
# a single batched multi-group OffsetFetch request (KIP-709). The offset_sum series is emitted
# exclusively from that offset-fetch path, so seeing it for each seeded group proves the batched
# request returned that group and the v8->legacy response conversion worked end-to-end.
validate_batched_group_metrics() {
    log_info "Validating batched multi-group OffsetFetch (${SEED_GROUP_COUNT} seeded groups)..."

    local metrics
    metrics=$(fetch_metrics "$METRICS_URL") || return 1

    local offset_sum_series
    offset_sum_series=$(echo "$metrics" | grep "^kminion_kafka_consumer_group_topic_offset_sum" || true)

    local missing_groups=()
    for i in $(seq 1 "$SEED_GROUP_COUNT"); do
        local group="${SEED_GROUP_PREFIX}-${i}"
        if ! echo "$offset_sum_series" | grep -q "group_id=\"${group}\""; then
            missing_groups+=("$group")
        fi
    done

    if [ ${#missing_groups[@]} -ne 0 ]; then
        log_error "Batched OffsetFetch did not report offsets for these seeded groups:"
        printf '  - %s\n' "${missing_groups[@]}"
        echo ""
        log_info "consumer_group_topic_offset_sum series present:"
        echo "$offset_sum_series" || echo "  (none found)"
        return 1
    fi

    log_info "✅ Batched OffsetFetch validation passed"
    log_info "   All ${SEED_GROUP_COUNT} seeded consumer groups reported offsets via the batched path"
    return 0
}

# Wait for KMinion to be ready
wait_for_kminion() {
    log_info "Waiting for KMinion to be ready (timeout: ${WAIT_TIMEOUT}s)..."

    local attempt=0
    while [ $attempt -lt "$WAIT_TIMEOUT" ]; do
        if curl -sf "$METRICS_URL" > /dev/null 2>&1; then
            log_info "KMinion is ready!"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done

    log_error "KMinion did not become ready within ${WAIT_TIMEOUT}s"
    return 1
}

# Main test execution
main() {
    log_info "Starting KMinion integration tests"
    log_info "Metrics URL: $METRICS_URL"

    # Wait for KMinion to be ready
    if ! wait_for_kminion; then
        exit 1
    fi

    # Give E2E tests time to produce metrics
    log_info "Waiting ${WAIT_TIMEOUT}s for E2E tests to produce metrics..."
    sleep "$WAIT_TIMEOUT"

    local failed=0

    # Run E2E metrics validation
    if ! validate_e2e_metrics; then
        failed=1
    fi

    # Run built-in metrics validation
    if ! validate_builtin_metrics; then
        failed=1
    fi

    # Run batched multi-group OffsetFetch validation
    if ! validate_batched_group_metrics; then
        failed=1
    fi

    if [ $failed -eq 0 ]; then
        log_info "🎉 All integration tests passed!"
        return 0
    else
        log_error "❌ Some integration tests failed"
        return 1
    fi
}

# Run main if executed directly
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    main "$@"
fi

