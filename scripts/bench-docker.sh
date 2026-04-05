#!/usr/bin/env bash
# ── Docker Throughput Benchmark ─────────────────────────────────────────
# Measures HTTP throughput against a running SeleneDB container.
# Requires: oha (brew install oha), curl, jq
#
# Usage:
#   docker run -d --rm --name selene-bench -p 8080:8080 selenedb:latest --dev --seed /data
#   ./scripts/bench-docker.sh
#
# Configurable via environment:
#   SELENE_URL=http://localhost:8080   Target URL
#   DURATION=10                         Seconds per test
#   CONCURRENCY=10                      Concurrent connections

set -euo pipefail

URL="${SELENE_URL:-http://localhost:8080}"
DURATION="${DURATION:-10}"
CONCURRENCY="${CONCURRENCY:-10}"

# ── Verify target is up ─────────────────────────────────────────────────

echo "=== SeleneDB Docker Throughput Benchmark ==="
echo "Target:      $URL"
echo "Duration:    ${DURATION}s per test"
echo "Concurrency: $CONCURRENCY"
echo ""

HEALTH=$(curl -sf "$URL/health" 2>/dev/null || true)
if [ -z "$HEALTH" ]; then
    echo "ERROR: Cannot reach $URL/health — is the container running?"
    echo "  docker run -d --rm --name selene-bench -p 8080:8080 selenedb:latest --dev --seed /data"
    exit 1
fi
NODE_COUNT=$(echo "$HEALTH" | python3 -c "import sys,json; print(json.load(sys.stdin)['node_count'])" 2>/dev/null || echo "?")
echo "Status:      healthy (${NODE_COUNT} nodes)"
echo ""

# ── Helper ──────────────────────────────────────────────────────────────

run_bench() {
    local name="$1"
    local body="$2"

    echo "--- $name ---"
    oha -z "${DURATION}s" -c "$CONCURRENCY" \
        -m POST \
        -H "Content-Type: application/json" \
        -d "$body" \
        "$URL/gql" 2>&1 | grep -E "(Requests/sec|Slowest|Fastest|Average|Success rate|Status code)"
    echo ""
}

# ── Benchmarks ──────────────────────────────────────────────────────────

echo "════════════════════════════════════════════"
echo " 1. Health Check (GET, baseline)"
echo "════════════════════════════════════════════"
oha -z "${DURATION}s" -c "$CONCURRENCY" "$URL/health" 2>&1 | grep -E "(Requests/sec|Slowest|Fastest|Average|Success rate|Status code)"
echo ""

echo "════════════════════════════════════════════"
echo " 2. count(*) — Bitmap Short-Circuit"
echo "════════════════════════════════════════════"
run_bench "count(*) sensors" \
    '{"query":"MATCH (s:sensor) RETURN count(*) AS n"}'

echo "════════════════════════════════════════════"
echo " 3. Label Scan — Return All Nodes"
echo "════════════════════════════════════════════"
run_bench "MATCH all nodes" \
    '{"query":"MATCH (n) RETURN n.name"}'

echo "════════════════════════════════════════════"
echo " 4. Pattern Match — Two-Hop Traversal"
echo "════════════════════════════════════════════"
run_bench "two-hop expand" \
    '{"query":"MATCH (b:building)-[:contains]->(f:floor)-[:contains]->(s) RETURN b.name, s.name"}'

echo "════════════════════════════════════════════"
echo " 5. Filter + Sort + Limit"
echo "════════════════════════════════════════════"
run_bench "filter+sort+limit" \
    '{"query":"MATCH (n) RETURN n.name ORDER BY n.name LIMIT 5"}'

echo "════════════════════════════════════════════"
echo " 6. Aggregation — GROUP BY"
echo "════════════════════════════════════════════"
run_bench "GROUP BY labels" \
    '{"query":"MATCH (n) RETURN labels(n) AS type, count(*) AS cnt GROUP BY labels(n)"}'

echo "════════════════════════════════════════════"
echo " 7. INSERT + DELETE (mutation throughput)"
echo "════════════════════════════════════════════"
# Insert and immediately delete to avoid accumulating nodes
run_bench "INSERT node" \
    '{"query":"INSERT (:bench_tmp {ts: 1})"}'

# Clean up bench nodes
curl -sf -X POST "$URL/gql" \
    -H "Content-Type: application/json" \
    -d '{"query":"MATCH (n:bench_tmp) DETACH DELETE n"}' > /dev/null 2>&1 || true

echo "════════════════════════════════════════════"
echo " 8. Plan Cache Hit (repeated identical query)"
echo "════════════════════════════════════════════"
run_bench "plan cache hit" \
    '{"query":"MATCH (s:sensor) RETURN count(*) AS n"}'

echo "════════════════════════════════════════════"
echo " Done!"
echo "════════════════════════════════════════════"
