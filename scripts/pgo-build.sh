#!/usr/bin/env bash
# Profile-Guided Optimization build for Selene.
#
# Two-pass build:
# 1. Build with instrumentation, run benchmarks to generate profile data
# 2. Rebuild with profile data for optimized binary
#
# Expected improvement: +10-15% on top of LTO (already enabled).
# Usage: ./scripts/pgo-build.sh

set -euo pipefail

PROFILE_DIR="/tmp/selene-pgo-data"
MERGED_PROFILE="${PROFILE_DIR}/merged.profdata"

echo "=== Selene PGO Build ==="
echo ""

# Step 1: Clean previous profile data
rm -rf "${PROFILE_DIR}"
mkdir -p "${PROFILE_DIR}"

# Step 2: Build with instrumentation
echo "Step 1/4: Building with instrumentation..."
RUSTFLAGS="-Cprofile-generate=${PROFILE_DIR}" \
    cargo build --release -p selene-server 2>&1 | tail -3

# Step 3: Run profiling workload
echo "Step 2/4: Running profiling workload..."

# Run GQL benchmarks (primary workload)
RUSTFLAGS="-Cprofile-generate=${PROFILE_DIR}" \
    cargo bench -p selene-gql -- --warm-up-time 1 --measurement-time 2 2>&1 | tail -5

echo "  Benchmark profiling complete."

# Step 4: Merge profile data
echo "Step 3/4: Merging profile data..."
llvm-profdata merge -o "${MERGED_PROFILE}" "${PROFILE_DIR}" 2>/dev/null || {
    # Try with rustup's llvm-profdata
    LLVM_PROFDATA=$(rustup which llvm-profdata 2>/dev/null || echo "")
    if [ -n "${LLVM_PROFDATA}" ]; then
        "${LLVM_PROFDATA}" merge -o "${MERGED_PROFILE}" "${PROFILE_DIR}"
    else
        echo "ERROR: llvm-profdata not found. Install llvm-tools: rustup component add llvm-tools"
        exit 1
    fi
}

echo "  Profile data merged: ${MERGED_PROFILE}"

# Step 5: Rebuild with profile data
echo "Step 4/4: Rebuilding with profile-guided optimization..."
RUSTFLAGS="-Cprofile-use=${MERGED_PROFILE}" \
    cargo build --release -p selene-server 2>&1 | tail -3

echo ""
echo "=== PGO build complete ==="
echo "Binary: target/release/selene-server"
echo ""
echo "Compare performance with: ./scripts/bench-compare.sh"
