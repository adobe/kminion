#!/usr/bin/env bash
set -euo pipefail

# Seed multiple consumer groups with committed offsets.
#
# This gives KMinion's AdminAPI scrape mode more than one consumer group to fetch offsets for, so
# the E2E test actually exercises the *batched multi-group* OffsetFetch path (KIP-709 / OffsetFetch
# v8+) rather than just KMinion's own single end-to-end group. Each seeded group's offsets can only
# surface in KMinion's metrics via the batched fetch + the v8->legacy response conversion, so the
# integration test asserting these groups are reported is a real end-to-end check of that code.
#
# Groups are created with committed offsets without a live consumer by using
# `kafka-consumer-groups.sh --reset-offsets --execute`, which persists offsets for an inactive group.
#
# Must run AFTER Kafka is up but BEFORE KMinion starts, because KMinion caches the consumer-group
# list for 120s on its first scrape.

CONTAINER_NAME="${CONTAINER_NAME:-broker}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
BOOTSTRAP="localhost:${KAFKA_PORT}"
SEED_TOPIC="${SEED_TOPIC:-kminion-batch-test}"
SEED_GROUP_PREFIX="${SEED_GROUP_PREFIX:-e2e-batch-test-group}"
SEED_GROUP_COUNT="${SEED_GROUP_COUNT:-5}"
SEED_RECORD_COUNT="${SEED_RECORD_COUNT:-30}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

kbin() { docker exec "$CONTAINER_NAME" "$@"; }

create_topic() {
    log_info "Creating seed topic '$SEED_TOPIC' (3 partitions)..."
    kbin /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
        --create --if-not-exists --topic "$SEED_TOPIC" \
        --partitions 3 --replication-factor 1
}

produce_records() {
    log_info "Producing $SEED_RECORD_COUNT records to '$SEED_TOPIC' (so committed offsets are non-trivial)..."
    seq 1 "$SEED_RECORD_COUNT" | docker exec -i "$CONTAINER_NAME" \
        /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic "$SEED_TOPIC"
}

seed_groups() {
    log_info "Seeding $SEED_GROUP_COUNT consumer groups with committed offsets..."
    for i in $(seq 1 "$SEED_GROUP_COUNT"); do
        local group="${SEED_GROUP_PREFIX}-${i}"
        # --reset-offsets --execute commits offsets for an inactive group without a live consumer,
        # creating it in the Empty state with committed offsets on all partitions of the topic.
        kbin /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" \
            --group "$group" --topic "$SEED_TOPIC" \
            --reset-offsets --to-earliest --execute > /dev/null
        log_info "  seeded group: $group"
    done
}

verify_seed() {
    log_info "Verifying committed offsets for '${SEED_GROUP_PREFIX}-1'..."
    kbin /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" \
        --group "${SEED_GROUP_PREFIX}-1" --describe || true
}

main() {
    log_info "Seeding consumer groups for batched OffsetFetch E2E test"
    create_topic
    produce_records
    seed_groups
    verify_seed
    log_info "✅ Seeded $SEED_GROUP_COUNT consumer groups on topic '$SEED_TOPIC'"
}

if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    main "$@"
fi
