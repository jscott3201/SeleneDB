#!/usr/bin/env bash
# scripts/memory-profile.sh — Measure peak RSS for Selene at various graph scales.
#
# Usage:
#   ./scripts/memory-profile.sh                    # default: 10K, 100K, 250K
#   ./scripts/memory-profile.sh 50000              # single target
#   ./scripts/memory-profile.sh 10000 100000       # multiple targets

set -euo pipefail

TARGETS="${*:-10000 100000 250000}"

# Build the profiling binary in release mode
echo "Building memory_profile (release)..."
cargo build -p selene-testing --bin memory_profile --release --quiet

BINARY="target/release/memory_profile"

# Detect platform for time command
if [[ "$(uname)" == "Darwin" ]]; then
    TIME_CMD="/usr/bin/time -l"
    RSS_FIELD="maximum resident set size"
else
    TIME_CMD="/usr/bin/time -v"
    RSS_FIELD="Maximum resident set size"
fi

echo ""
printf "%-12s %-10s %-10s %-15s %-12s\n" "Target" "Nodes" "Edges" "Peak RSS (MB)" "Bytes/Node"
printf "%-12s %-10s %-10s %-15s %-12s\n" "------" "-----" "-----" "-------------" "----------"

for target in $TARGETS; do
    # Run with time, capture stderr (time output + profile stderr) and stdout
    TMPSTDERR=$(mktemp)
    TMPSTDOUT=$(mktemp)

    # /usr/bin/time writes to stderr; our binary writes profile line to stdout, status to stderr
    $TIME_CMD "$BINARY" "$target" >"$TMPSTDOUT" 2>"$TMPSTDERR" || true

    # Parse RSS from time output (stderr)
    if [[ "$(uname)" == "Darwin" ]]; then
        # macOS: "maximum resident set size" is in bytes
        RSS_BYTES=$(grep "$RSS_FIELD" "$TMPSTDERR" | awk '{print $1}' || echo "0")
    else
        # Linux: "Maximum resident set size" is in KB
        RSS_KB=$(grep "$RSS_FIELD" "$TMPSTDERR" | awk '{print $NF}' || echo "0")
        RSS_BYTES=$((RSS_KB * 1024))
    fi

    RSS_MB=$((RSS_BYTES / 1048576))

    # Parse node/edge count from stdout
    PROFILE_LINE=$(cat "$TMPSTDOUT" | grep "MEMORY_PROFILE" || echo "")
    NODES=$(echo "$PROFILE_LINE" | grep -o 'nodes=[0-9]*' | head -1 | cut -d= -f2)
    EDGES=$(echo "$PROFILE_LINE" | grep -o 'edges=[0-9]*' | head -1 | cut -d= -f2)

    NODES=${NODES:-0}
    EDGES=${EDGES:-0}

    if [ "$NODES" -gt 0 ] && [ "$RSS_BYTES" -gt 0 ]; then
        BPN=$((RSS_BYTES / NODES))
    else
        BPN=0
    fi

    printf "%-12s %-10s %-10s %-15s %-12s\n" "${target}" "${NODES}" "${EDGES}" "${RSS_MB}" "${BPN}"

    rm -f "$TMPSTDERR" "$TMPSTDOUT"
done
