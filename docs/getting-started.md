# Getting Started

SeleneDB is the AI-native graph database. A single binary that combines a full ISO GQL query engine, built-in vector search, GraphRAG, agent memory, time-series storage, and a 64-tool MCP server — all in pure Rust with zero C/C++ dependencies.

This guide gets you from zero to running in under five minutes.

## Quick Start with Docker

The fastest path. No toolchain required:

```bash
docker compose up -d
curl -s http://localhost:8080/health | jq
```

This starts SeleneDB with:

- **HTTP** on port 8080 — GQL queries, REST API, MCP server
- **QUIC** on port 4510 — high-performance binary transport
- **Dev mode** — self-signed TLS, no auth (for local development)

The distroless image is ~14 MB compressed. No shell, no package manager, minimal attack surface.

### With demo data

Add `--seed` to populate a reference building graph (sensors, equipment, spaces, and relationships):

```bash
docker run -p 4510:4510/udp -p 8080:8080 ghcr.io/jscott3201/selenedb --dev --seed
```

## Build from Source

Requires Rust 1.94+ (install via [rustup](https://rustup.rs/)). No C compiler or system libraries needed.

```bash
git clone https://github.com/jscott3201/SeleneDB.git
cd SeleneDB
cargo run -p selene-server --features dev-tls -- --dev
```

All product features (HTTP, MCP, vector search, federation, RDF, time-series) are always compiled. The only compile-time flags are hardware variants:

| Feature | Purpose |
|---------|---------|
| `dev-tls` | Self-signed TLS for local development |
| `cuda` | NVIDIA GPU acceleration for embedding inference |
| `metal` | Apple GPU acceleration for embedding inference |

Build the CLI separately:

```bash
cargo build --release -p selene-cli
```

## Verify It's Running

```bash
curl -s http://localhost:8080/health | jq
```

```json
{
  "status": "ok",
  "uptime_secs": 5
}
```

## Your First Queries

SeleneDB uses [GQL (ISO 39075)](guides/gql/overview.md) as its sole query language. Every transport — HTTP, QUIC, and MCP — routes through GQL.

### Create and query data

```bash
# Insert a building with a sensor
curl -s -X POST http://localhost:8080/gql \
  -H 'Content-Type: application/json' \
  -d '{"query": "INSERT (:building {name: '\''HQ'\''})-[:contains]->(:sensor {name: '\''T1'\'', unit: '\''°F'\'', temp: 72.5})"}'

# Query it back
curl -s -X POST http://localhost:8080/gql \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (b:building)-[:contains]->(s:sensor) RETURN b.name, s.name, s.temp"}' | jq
```

```json
{
  "status_code": "00000",
  "data": [{"b.name": "HQ", "s.name": "T1", "s.temp": 72.5}]
}
```

### Pattern matching

```sql
-- Variable-length path traversal
MATCH (b:building)-[:contains]->{1,3}(s:sensor)
FILTER s.temp > 80.0
RETURN b.name, s.name, s.temp
ORDER BY s.temp DESC LIMIT 10

-- Aggregation
MATCH (b:building)-[:contains]->(s:sensor)
RETURN b.name, count(*) AS sensors, avg(s.temp) AS avg_temp
GROUP BY b.name
```

### Semantic search

Find nodes by meaning, not just structure:

```sql
CALL search.semantic('supply air temperature anomaly', 10)
  YIELD nodeId, score, name
```

### GraphRAG retrieval

Combine vector similarity, graph traversal, and community context in a single query:

```sql
CALL search.graphrag('which zones are overheating?', 'local', 10, 2)
  YIELD nodeId, score, context
```

### Agent memory

Persistent memory that survives across sessions — agents remember what they learned:

```sql
-- Store a fact
CALL memory.remember('my-agent', 'The auth module uses Cedar policies for RBAC')

-- Recall by semantic similarity
CALL memory.recall('my-agent', 'how does auth work?', 5)
  YIELD content, score

-- Forget when no longer relevant
CALL memory.forget('my-agent', 'Cedar policies')
```

### Time-series

Ingest and query sensor data with built-in aggregation:

```sql
-- Query raw samples
CALL ts.range(42, 'temp', '2026-03-20T00:00:00Z', '2026-03-21T00:00:00Z')
  YIELD value, timestamp

-- Aggregated buckets
CALL ts.range_agg(42, 'temp', '2026-03-20T00:00:00Z', '2026-03-21T00:00:00Z', '1h', 'avg')
  YIELD bucket, value
```

### Graph algorithms

```sql
CALL graph.pagerank(0.85, 20) YIELD nodeId, score
CALL graph.louvain(10) YIELD nodeId, communityId
```

### Via the CLI

The CLI connects over QUIC. In dev mode, use `--insecure` to skip TLS verification:

```bash
selene --insecure gql "MATCH (n) RETURN count(*) AS cnt"
```

## Connect AI Agents via MCP

SeleneDB ships a full [Model Context Protocol](https://modelcontextprotocol.io/) server with 64 tools. Point any MCP-compatible client at:

```
http://localhost:8080/mcp
```

For Claude Code / Copilot CLI, add to your MCP config:

```json
{
  "mcpServers": {
    "selenedb": {
      "url": "http://localhost:8080/mcp"
    }
  }
}
```

Agents can immediately use tools like `gql_query`, `semantic_search`, `remember`, `recall`, `graphrag_search`, `batch_ingest`, and `resolve` — no additional setup required.

See the [MCP Guide](guides/mcp.md) for the full tool reference.

## Deployment Profiles

SeleneDB scales from a Raspberry Pi to a GPU-accelerated cloud VM:

```bash
# Edge device (RPi 5, gateways) — minimal memory footprint
docker run ghcr.io/jscott3201/selenedb --profile edge

# Cloud VM — full services, higher memory budgets
docker run ghcr.io/jscott3201/selenedb --profile cloud

# Read replica — live changelog streaming from a primary
docker run ghcr.io/jscott3201/selenedb --replica-of primary:4510
```

### GPU-accelerated inference

For on-device embedding with EmbeddingGemma, build from source with GPU support:

```bash
# Apple Silicon (Metal)
cargo build --release -p selene-server --features metal,dev-tls

# NVIDIA CUDA
cargo build --release -p selene-server --features cuda,dev-tls
```

This enables native embedding inference with no external API calls or network dependency. See the [Deployment guide](operations/deployment.md) for full macOS Metal and CUDA setup instructions.

## Server Flags

| Flag | Description |
|------|-------------|
| `--dev` | Dev mode (self-signed TLS, no auth) |
| `--seed` | Seed demo data on startup (if graph is empty) |
| `--config <path>` | Path to TOML config file |
| `--profile <type>` | Runtime profile: `edge`, `cloud`, or `standalone` |
| `--quic-listen <addr>` | QUIC listen address (default: `0.0.0.0:4510`) |
| `--http-listen <addr>` | HTTP listen address (default: `0.0.0.0:8080`) |
| `--data-dir <path>` | Data directory for WAL and snapshots |
| `--show-config` | Print effective config and exit |
| `--replica-of <addr>` | Start as a read-only replica of the given primary |

## Next Steps

| | |
|---|---|
| [GQL Guide](guides/gql/overview.md) | Learn the query language — pattern matching, mutations, functions, procedures |
| [MCP Tools](guides/mcp.md) | 64 tools purpose-built for AI agents |
| [Vector Search](guides/vector-search.md) | Embeddings, HNSW indexing, semantic search, GraphRAG |
| [Time-Series](guides/time-series.md) | Multi-tier sensor data storage with built-in aggregation |
| [Agent Workflows](agent-workflows.md) | End-to-end patterns for AI agent integration |
| [Configuration](operations/configuration.md) | TOML config, runtime profiles, environment variables |
| [Deployment](operations/deployment.md) | Docker, GPU, edge, cloud, and federation deployment |
| [HTTP API](guides/http-api.md) | REST endpoint reference |
