#!/usr/bin/env bash
set -euo pipefail

# Verify KMinion shuts down gracefully on SIGTERM.
#
# Kubernetes (and most orchestrators) send SIGTERM on pod termination. KMinion registers SIGTERM
# alongside SIGINT and, on receiving it, cancels its root context, shuts the HTTP server down
# cleanly, and logs "kminion stopped" as the last line of main().
#
# This is a regression test: if SIGTERM were not handled, Go's default signal disposition would
# terminate the process *without* running that graceful path, so the "kminion stopped" line would be
# absent. We therefore require both a timely exit AND that final log line.
#
# Must run while KMinion is still running (i.e. after the metrics integration test, before cleanup).

PID_FILE="${PID_FILE:-kminion.pid}"
LOG_FILE="${LOG_FILE:-kminion.log}"
SHUTDOWN_TIMEOUT="${SHUTDOWN_TIMEOUT:-15}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

main() {
    log_info "Testing graceful shutdown on SIGTERM"

    if [ ! -f "$PID_FILE" ]; then
        log_error "PID file '$PID_FILE' not found; is KMinion running?"
        exit 1
    fi

    local pid
    pid=$(cat "$PID_FILE")

    if ! kill -0 "$pid" 2>/dev/null; then
        log_error "KMinion (PID $pid) is not running"
        exit 1
    fi

    log_info "Sending SIGTERM to KMinion (PID $pid)..."
    kill -TERM "$pid"

    # Wait for the process to exit.
    local waited=0
    while kill -0 "$pid" 2>/dev/null; do
        if [ "$waited" -ge "$SHUTDOWN_TIMEOUT" ]; then
            log_error "KMinion did not exit within ${SHUTDOWN_TIMEOUT}s of SIGTERM (SIGTERM likely not handled)"
            log_error "Last 30 lines of $LOG_FILE:"
            tail -30 "$LOG_FILE" 2>&1 || true
            exit 1
        fi
        waited=$((waited + 1))
        sleep 1
    done
    log_info "KMinion exited ${waited}s after SIGTERM"

    # Give the log a moment to flush, then confirm the graceful path actually ran.
    sleep 1
    if grep -q "kminion stopped" "$LOG_FILE"; then
        log_info "✅ Graceful shutdown confirmed: 'kminion stopped' was logged"
        rm -f "$PID_FILE"
        exit 0
    fi

    log_error "SIGTERM did not trigger a graceful shutdown ('kminion stopped' not logged)"
    log_error "Last 30 lines of $LOG_FILE:"
    tail -30 "$LOG_FILE" 2>&1 || true
    exit 1
}

if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    main "$@"
fi
