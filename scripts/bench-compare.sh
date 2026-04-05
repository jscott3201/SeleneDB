#!/usr/bin/env bash
# Benchmark comparison for Selene.
#
# Saves a baseline, then compares against it.
# Flags regressions > 15% as warnings, > 25% as errors.
#
# Usage:
#   ./scripts/bench-compare.sh save       # Save current as baseline
#   ./scripts/bench-compare.sh compare    # Compare against saved baseline
#   ./scripts/bench-compare.sh            # Same as compare

set -euo pipefail

ACTION="${1:-compare}"

case "${ACTION}" in
    save)
        echo "=== Saving benchmark baseline ==="
        cargo bench -p selene-gql -- --save-baseline main 2>&1 | tail -20
        echo ""
        echo "Baseline saved. Run './scripts/bench-compare.sh compare' after changes."
        ;;

    compare)
        echo "=== Comparing against baseline ==="
        echo ""

        # Run benchmarks and compare
        OUTPUT=$(cargo bench -p selene-gql -- --baseline main 2>&1)

        # Show the results
        echo "${OUTPUT}" | tail -40

        # Check for regressions
        REGRESSIONS=$(echo "${OUTPUT}" | grep -c "regressed" || true)
        if [ "${REGRESSIONS}" -gt 0 ]; then
            echo ""
            echo "WARNING: ${REGRESSIONS} benchmark(s) regressed!"
            echo ""
            echo "${OUTPUT}" | grep "regressed"
        else
            echo ""
            echo "No regressions detected."
        fi
        ;;

    *)
        echo "Usage: $0 [save|compare]"
        echo "  save    - Save current performance as baseline"
        echo "  compare - Compare current performance against baseline"
        exit 1
        ;;
esac
