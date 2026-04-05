# SeleneDB

An in-memory property graph database built for the edge. Pure Rust, single binary, 14 MB Docker image.

[![CI](https://github.com/jscott3201/SeleneDB/actions/workflows/ci.yml/badge.svg)](https://github.com/jscott3201/SeleneDB/actions/workflows/ci.yml)
![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)
[![Rust 1.94+](https://img.shields.io/badge/rust-1.94%2B-orange.svg)](https://www.rust-lang.org/)

## Why Selene?

Most graph databases assume a data center. Selene assumes a Raspberry Pi.

It's a full property graph runtime - ISO GQL query engine, time-series storage, vector search, federation - designed to run on a building controller, a factory gateway, or anywhere you need a real graph on constrained hardware. It also runs just fine on a cloud VM.

Any Selene node can federate with any other. Topology is configuration, not architecture.

**Status:** v0.2.0 release candidate. 13 crates, ~2,600 tests, zero clippy warnings.

**What Selene is not:** a distributed database with multi-writer consensus. It's a single-node runtime that federates and syncs. Edge nodes sync bidirectionally with a hub using LWW conflict resolution, but there's no Raft, no Paxos, no quorum. If you need horizontal partitioning, look elsewhere.

### How Selene compares

| | Selene | Neo4j | Memgraph | FalkorDB | Kuzu |
|---|---|---|---|---|---|
| Query language | ISO GQL | Cypher | Cypher | Cypher | Cypher |
| Memory model | In-memory, single-node | Disk-backed, clustered | In-memory, replicated | In-memory, clustered | Disk-backed, embedded |
| Binary size | ~14 MB (Docker) | ~500 MB+ | ~100 MB+ | ~50 MB+ | ~30 MB (library) |
| Edge deployment | Built for RPi/gateways | Server-class hardware | Server-class hardware | Server-class hardware | Embedded use cases |
| Time-series | Built-in multi-tier | External integration | External integration | External integration | None |
| Vector search | Built-in BERT embedding | Plugin (GDS) | None | Built-in | None |
| Federation | Native QUIC peer-to-peer | Fabric (Enterprise) | None | None | None |
| Offline sync | LWW bidirectional | None | None | None | None |
| C/C++ deps | Zero | JVM | Yes (Memgraph core) | Yes (Redis core) | Yes (Arrow, DuckDB) |

## Quick Start

```bash
docker compose up -d
curl http://localhost:8080/health
```

With demo data (building hierarchy, sensors, time-series):

```bash
docker run -p 4510:4510/udp -p 8080:8080 ghcr.io/jscott3201/selenedb --dev --seed
```

From source (Rust 1.94+, no C dependencies):

```bash
cargo run -p selene-server -- --dev --seed
# QUIC on :4510, HTTP on :8080
```

## Try a Query

Create some data and query it back:

```bash
# Insert a building with a sensor
curl -s -X POST http://localhost:8080/gql \
  -H 'Content-Type: application/json' \
  -d '{"query": "INSERT (:building {name: '\''HQ'\''})-[:contains]->(:sensor {name: '\''T1'\'', unit: '\''°F'\'', temp: 72.5})"}'

# Find it
curl -s -X POST http://localhost:8080/gql \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (b:building)-[:contains]->(s:sensor) RETURN b.name, s.name, s.temp"}'
```

GQL is the sole query interface. HTTP, QUIC, and MCP all route through it:

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

-- Time-series
CALL ts.range(42, 'temp', '2026-03-20T00:00:00Z', '2026-03-21T00:00:00Z')
  YIELD value, timestamp

-- Graph algorithms
CALL graph.pagerank(0.85, 20) YIELD nodeId, score
```

See the [GQL guide](docs/guides/gql/overview.md) for the full language reference.

## What's Inside

- **ISO GQL** (ISO 39075) - pattern matching, mutations, transactions, 101 scalar functions, 56 procedures
- **In-memory graph** - lock-free reads (~1ns via ArcSwap), RoaringBitmap label indexes, typed property indexes
- **Multi-tier time-series** - multi-encoding hot tier (Gorilla, RLE, Dictionary), warm aggregates, Parquet cold tier, cloud offload
- **Materialized views** - `CREATE MATERIALIZED VIEW` with incremental maintenance via changelog subscriber
- **Vector search** - HNSW index with brute-force fallback, cosine/euclidean similarity, auto-embedding via candle BERT, community-enhanced RAG
- **Full-text search** - tantivy BM25, hybrid BM25+cosine via reciprocal rank fusion
- **Graph algorithms** - PageRank, betweenness, Dijkstra, SSSP, APSP, WCC, SCC, Louvain, label propagation, triangle count, topological sort, articulation points, bridges
- **Worst-case optimal joins** - sorted merge intersection for cyclic patterns (triangles), AGM-bounded O(m^1.5)
- **RDF interop** - Turtle/N-Triples import/export, SPARQL queries, BRICK/223P ontology support
- **Federation** - any node queries any other via `USE <graph>` over QUIC with Arrow IPC
- **CDC replicas** - `--replica-of` for read scaling with live changelog streaming
- **Bidirectional sync** - offline-first edge nodes push/pull changes with LWW conflict resolution
- **Persistence** - WAL v2 (postcard + zstd + XXH3 + origin tracking) + binary snapshots, sub-second recovery
- **Schema system** - type DDL, constraints, inheritance, composite indexes, dictionary encoding
- **Temporal** - property version chains, point-in-time queries, `AT TIME` syntax
- **Triggers** - ECA model with WHEN conditions and OLD_VALUE access
- **MCP server** - 36 tools, 5 resources, 3 prompts for AI agent integration, OAuth 2.1 (PKCE + client credentials) + API key auth
- **Pure Rust** - zero C/C++ dependencies, trivial cross-compilation

## Performance

Benchmarked on Apple M5 (10-core, 16 GB) with a 10K-node reference building:

| Operation | Time | Notes |
|-----------|-----:|-------|
| Plan cache hit | 19 ns | Parsed AST by query hash |
| count(*) | 8.7 us | O(1) bitmap cardinality |
| FILTER prop = val | 38 us | TypedIndex lookup |
| Two-hop expand | 180 us | |
| INSERT node | 55 us | With WAL + changelog |
| Snapshot recovery | 1.8 ms | Sub-second cold start |
| Vector top-10 (384-dim) | 1.5 ms | Brute-force scan |

Linear scaling confirmed to 250K entities. Full results including stress tests and algorithm benchmarks in [Benchmarks.md](Benchmarks.md).

## Architecture

13 crates, one binary. Business logic lives in an ops layer; transports (QUIC, HTTP, MCP) are thin adapters. Every operation works identically over every transport.

```
selene-core         Types, schemas, codec traits
selene-graph        In-memory property graph, indexes, mutations
selene-gql          ISO GQL engine (parser, planner, optimizer, executor)
selene-ts           Multi-tier time-series (hot, warm, cold, cloud)
selene-persist      WAL + snapshots, crash recovery
selene-wire         Wire protocol, framing, serialization
selene-server       QUIC + HTTP + MCP, auth, federation, ops layer
selene-client       Async QUIC client
selene-cli          Command-line tool
selene-algorithms   Graph algorithms (15 algos across 4 modules)
selene-rdf          RDF import/export, SPARQL adapter
selene-packs        Schema packs (compact TOML)
selene-testing      Test factories, synthetic topologies
```

See [Architecture](docs/internals/architecture.md) for design decisions and crate boundaries.

## Deployment

```bash
docker run ghcr.io/jscott3201/selenedb --profile edge        # RPi 5, gateways
docker run ghcr.io/jscott3201/selenedb --profile cloud       # VMs, full services
docker run ghcr.io/jscott3201/selenedb --replica-of primary:4510  # read replica
```

Bidirectional sync (offline-first edge nodes):

```toml
# selene.toml on the edge node
[sync]
upstream = "hub.example.com:4510"
peer_name = "building-42"
```

The image is distroless (`gcr.io/distroless/static:nonroot`) with a statically-linked musl binary. No shell, no package manager.

Runtime profiles control memory budgets and service activation. Feature flags (`vector`, `search`, `cloud-storage`, `rdf`, `rdf-sparql`, `federation`, `dev-tls`) are compile-time; services toggle at runtime via config or environment variables. See [Deployment](docs/operations/deployment.md) and [Configuration](docs/operations/configuration.md).

## Documentation

| | |
|---|---|
| [Getting Started](docs/getting-started.md) | Installation and first queries |
| [GQL Guide](docs/guides/gql/overview.md) | Query language, functions, procedures |
| [HTTP API](docs/guides/http-api.md) | REST endpoints |
| [Time-Series](docs/guides/time-series.md) | Sensor data ingestion and queries |
| [Vector Search](docs/guides/vector-search.md) | Embeddings and semantic search |
| [RDF / SPARQL](docs/guides/rdf-sparql.md) | Ontology support, SPARQL queries |
| [MCP Tools](docs/guides/mcp.md) | AI agent integration |
| [Configuration](docs/operations/configuration.md) | TOML config, profiles, env vars |
| [Security](docs/operations/security.md) | TLS, Cedar auth, vault |
| [Architecture](docs/internals/architecture.md) | Crate map, design philosophy |

## Building and Testing

```bash
cargo test --workspace --all-features          # ~2,600 tests
cargo test -p selene-gql                       # GQL engine only
cargo test -p selene-server --all-features     # server + sync + federation
cargo bench -p selene-gql                      # benchmarks (run sequentially)
```

All feature flags are opt-in: `federation`, `vector`, `search`, `cloud-storage`, `rdf`, `rdf-sparql`, `dev-tls`.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for development
setup, coding standards, and the pull request process.

Please review our [Code of Conduct](CODE_OF_CONDUCT.md) before participating.

For security vulnerabilities, see [SECURITY.md](SECURITY.md).

## License

Licensed under either of

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT License](LICENSE-MIT)

at your option.
