#!/bin/bash
# Auto-retest script — run at NY Open (15:30 CEST)
# Usage: ./scripts/retest.sh [SYMBOL]

set -euo pipefail

SYMBOL="${1:-BTCUSDT}"
LOGDIR="/tmp/oml-retest-logs"
mkdir -p "$LOGDIR"

TIMESTAMP=$(date +'%Y%m%d_%H%M')
LOGFILE="$LOGDIR/retest_${TIMESTAMP}_${SYMBOL}.log"

echo "=== Auto-retest oml-cryexc-mcp ===" | tee "$LOGFILE"
echo "Symbol: $SYMBOL" | tee -a "$LOGFILE"
echo "Time: $(date '+%Y-%m-%d %H:%M:%S %Z')" | tee -a "$LOGFILE"
echo "" | tee -a "$LOGFILE"

cd "$(dirname "$0")/.."

echo "Building test runner..." | tee -a "$LOGFILE"
go build -o /tmp/test-runner ./cmd/test/main.go 2>&1 | tee -a "$LOGFILE"

echo "" | tee -a "$LOGFILE"
echo "Running 7 exchange test..." | tee -a "$LOGFILE"

# Run test with colored output and save to log
if command -v tee >/dev/null; then
    /tmp/test-runner "$SYMBOL" 2>&1 | tee -a "$LOGFILE"
else
    /tmp/test-runner "$SYMBOL" 2>&1 | tee -a "$LOGFILE"
fi

EXIT_CODE=$?

echo "" | tee -a "$LOGFILE"
echo "=== Done. Exit code: $EXIT_CODE ===" | tee -a "$LOGFILE"
echo "Log saved to: $LOGFILE"

exit $EXIT_CODE
