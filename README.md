# SeleneDB

A property graph database with GQL, vector search, time-series, and RDF/SPARQL. Pure Rust, single binary, runs on a Raspberry Pi or a cloud VM.

[![CI](https://github.com/jscott3201/SeleneDB/actions/workflows/ci.yml/badge.svg)](https://github.com/jscott3201/SeleneDB/actions/workflows/ci.yml)
![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)
[![Rust 1.94+](https://img.shields.io/badge/rust-1.94%2B-orange.svg)](https://www.rust-lang.org/)

## What is SeleneDB?

SeleneDB is an in-memory property graph runtime built around ISO GQL. Alongside the graph engine it ships a mutable HNSW vector index, a multi-tier time-series store, BM25 full-text search, Louvain-based community detection, RDF/SPARQL interop, and a Model Context Protocol server — all in one ~14 MB binary with zero C/C++ dependencies. SeleneDB is BYO-vector: applications embed text with their own model and pass pre-computed vectors as query parameters.

The design target is domains that need a living graph of connected entities with real-time state: IoT, smart buildings, factory floors, agent knowledge graphs. Anywhere you want to walk a graph, search it by meaning, and query the sensor history attached to its nodes from one endpoint.

## Why SeleneDB?

### One database, many retrieval shapes

Most graph stores make you bolt on a separate vector database, a separate time-series store, and a separate RAG pipeline. SeleneDB treats those as peer capabilities of one engine:

- **Graph** — labels, properties, variable-length paths, worst-case optimal joins, 15 graph algorithms
- **Vector** — mutable HNSW index, cosine/euclidean, PolarQuant (3/4/8-bit) quantization, BYO-vector API
- **Time-series** — hot (Gorilla/RLE/dictionary), warm aggregates, Parquet cold tier, cloud offload
- **Full-text** — BM25 via tantivy, with hybrid BM25+cosine reciprocal rank fusion
- **Spatial** — `GEOMETRY` property type and 18 OGC-aligned `ST_*` functions for point-in-polygon, distance, and envelope queries ([guide](docs/guides/spatial.md))
- **RAG** — GraphRAG combines caller-supplied vectors, BFS traversal, and Louvain community summaries in one call
- **RDF** — Turtle/N-Triples import/export and SPARQL queries over the same graph

GQL is the only write path. HTTP, QUIC, and MCP are thin adapters — the same query runs unchanged across all three.

### Edge-first, cloud-ready

Most databases assume a data center. SeleneDB assumes you might be running on a building controller, a factory gateway, or a Raspberry Pi, and that it should work just as well on a cloud VM with a GPU.

- **~14 MB CPU image** — distroless, statically linked, no shell or package manager
- **Sub-second cold start** — binary snapshot recovery in ~1.8 ms on a 10K-node graph
- **Runtime profiles** — `--profile edge` for constrained devices, `--profile cloud` for full services
- **Offline-first sync** — edge nodes operate independently and reconcile bidirectionally with LWW
- **Federation** — any SeleneDB instance queries any other via `USE <graph>` over QUIC with Arrow IPC

### BYO-vector semantic search

Applications supply pre-computed embeddings; SeleneDB stores, indexes, and searches them:

- **HNSW index** — mutable, with cosine or euclidean distance, and optional PolarQuant (3/4/8-bit) rescoring
- **`graph.semanticSearch($queryVec, k, label?)`** — top-k cosine with containment-path enrichment
- **`graph.similarNodes(nodeId, property, k)`** — reference-node similarity over stored vectors
- **`graph.hybridSearch(label, queryText, queryVec, k)`** — BM25 lexical + vector cosine via reciprocal rank fusion
- **`graphrag.search($queryVec, k, maxHops, mode)`** — vector + BFS + Louvain community context

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

## Try It

Create data and query it back:

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
-- Pattern matching with variable-length paths
MATCH (b:building)-[:contains]->{1,3}(s:sensor)
FILTER s.temp > 80.0
RETURN b.name, s.name, s.temp
ORDER BY s.temp DESC LIMIT 10

-- Aggregation
MATCH (b:building)-[:contains]->(s:sensor)
RETURN b.name, count(*) AS sensors, avg(s.temp) AS avg_temp
GROUP BY b.name

-- Semantic search — find nodes by meaning (client supplies the query vector)
CALL graph.semanticSearch($queryVec, 10)
  YIELD node_id, score, path

-- Graph-enhanced RAG retrieval (BYO-vector)
CALL graphrag.search($queryVec, 10, 2, 'local')
  YIELD node_id, score, source, context, depth

-- Time-series
CALL ts.range(42, 'temp', '2026-03-20T00:00:00Z', '2026-03-21T00:00:00Z')
  YIELD value, timestamp

-- Graph algorithms
CALL graph.pagerank(0.85, 20) YIELD nodeId, score
```

See the [GQL guide](docs/guides/gql/overview.md) for the full language reference.

## Feature Overview

### Query Engine
- **ISO GQL** (ISO 39075): pattern matching, mutations, transactions, variable-length paths, worst-case optimal joins
- **Built-in scalar function and procedure library**: list via `CALL graph.procedures() YIELD *`
- **13-rule query optimizer**: predicate pushdown, join reordering, cardinality estimation
- **Plan cache**: 19 ns cache hits via query hash
- **Materialized views**: `CREATE MATERIALIZED VIEW` with incremental changelog maintenance

### Graph Engine
- **Lock-free reads**: ~1 ns via ArcSwap snapshot isolation
- **RoaringBitmap label indexes**: O(1) cardinality, sub-microsecond label scans
- **Typed property indexes**: equality, range, and composite lookups
- **Schema system**: type DDL, constraints, inheritance, dictionary encoding
- **Temporal queries**: property version chains, point-in-time access via `AT TIME`
- **Triggers**: ECA model with WHEN conditions and OLD_VALUE access
- **15 graph algorithms**: PageRank, betweenness, Dijkstra, SSSP, APSP, WCC, SCC, Louvain, label propagation, triangle count, topological sort, articulation points, bridges

### Vector and Search
- **Vector search**: mutable HNSW index, cosine/euclidean, BYO-vector (clients embed)
- **Quantized vectors**: PolarQuant 3/4/8-bit with optional f32 re-ranking
- **GraphRAG**: local, global, and hybrid search modes combining vectors, BFS expansion, and community context
- **Full-text search**: tantivy BM25, hybrid BM25+cosine via reciprocal rank fusion
- **Community detection**: Louvain clustering with enriched summaries for RAG context

### Time-Series
- **Multi-tier storage**: hot (Gorilla/RLE/Dictionary encoding), warm aggregates, Parquet cold tier, cloud offload
- **Built-in aggregation**: auto-bucketing (5m, 15m, 1h, 1d) with min/max/avg/sum/count

### Networking and Deployment
- **QUIC + HTTP + MCP**: three transports, one ops layer, identical behavior
- **MCP tools**: Model Context Protocol server with read/write/destructive annotations
- **Federation**: cross-instance queries via `USE <graph>` over QUIC with Arrow IPC
- **CDC replicas**: `--replica-of` for read scaling with live changelog streaming
- **Bidirectional sync**: offline-first edge nodes with LWW conflict resolution
- **OAuth 2.1**: PKCE + client credentials, Cedar policy authorization, encrypted vault
- **RDF interop**: Turtle/N-Triples import/export, SPARQL queries, BRICK/223P ontology support

### Persistence
- **WAL v2**: postcard + zstd + XXH3 + HLC origin tracking
- **Binary snapshots**: portable, sub-second recovery
- **Pure Rust**: zero C/C++ dependencies across all 13 crates

## Using SeleneDB with AI agents

SeleneDB's MCP server exposes graph, vector, time-series, and schema operations to agent orchestrators (Claude Desktop, Cursor, Copilot, custom). All writes route through parameterized GQL, and tool descriptions carry read/write/destructive annotations so agents know what they're calling.

Agent-specific semantics — memory tiers, session namespaces, confidence decay, embedding strategy — live in application layers above SeleneDB (e.g. [ai-agent-skills](https://github.com/jscott3201/ai-agent-skills) or [Aether](https://github.com/CambrianTech/Aether)). SeleneDB provides the primitives they compose against.

## Performance

Benchmarked on Apple M5 (10-core, 16 GB) with a 10K-node reference building:

| Operation | Time | Notes |
|-----------|-----:|-------|
| Plan cache hit | 19 ns | Parsed AST by query hash |
| count(*) | 8.7 µs | O(1) bitmap cardinality |
| FILTER prop = val | 38 µs | TypedIndex lookup |
| Two-hop expand | 180 µs | |
| INSERT node | 55 µs | With WAL + changelog |
| Snapshot recovery | 1.8 ms | Sub-second cold start |
| Vector top-10 (384-dim) | 1.5 ms | HNSW scan |

Linear scaling confirmed to 250K entities. Full results including stress tests and algorithm benchmarks in [Benchmarks.md](Benchmarks.md).

## Architecture

13 crates, one binary. Business logic lives in an ops layer; transports (QUIC, HTTP, MCP) are thin adapters over it.

```
selene-core         Types, schemas, codec traits
selene-graph        In-memory property graph, indexes, vector index
selene-gql          ISO GQL engine (parser, planner, optimizer, executor)
selene-ts           Multi-tier time-series (hot, warm, cold, cloud)
selene-persist      WAL + snapshots, crash recovery
selene-wire         Wire protocol, framing, serialization
selene-server       QUIC + HTTP + MCP, auth, federation, ops layer
selene-client       Async QUIC client
selene-cli          Command-line tool
selene-algorithms   Graph algorithms (15 algos)
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

Bidirectional sync for offline-first edge nodes:

```toml
# selene.toml on the edge node
[sync]
upstream = "hub.example.com:4510"
peer_name = "building-42"
```

The Docker image is distroless (`gcr.io/distroless/static:nonroot`) at ~14 MB compressed, with no shell, no package manager, and minimal attack surface. Runtime profiles control memory budgets and service activation. See [Deployment](docs/operations/deployment.md) and [Configuration](docs/operations/configuration.md).

## Documentation

| | |
|---|---|
| [Getting Started](docs/getting-started.md) | Installation and first queries |
| [GQL Guide](docs/guides/gql/overview.md) | Query language, functions, procedures |
| [HTTP API](docs/guides/http-api.md) | REST endpoints |
| [Time-Series](docs/guides/time-series.md) | Sensor data ingestion and queries |
| [Vector Search](docs/guides/vector-search.md) | Embeddings and semantic search |
| [RDF / SPARQL](docs/guides/rdf-sparql.md) | Ontology support, SPARQL queries |
| [MCP Tools](docs/guides/mcp.md) | MCP surface over the GQL engine |
| [Configuration](docs/operations/configuration.md) | TOML config, profiles, env vars |
| [Security](docs/operations/security.md) | TLS, Cedar auth, vault |
| [Architecture](docs/internals/architecture.md) | Crate map, design philosophy |

## Building and Testing

```bash
cargo fmt --all                                # format
cargo clippy --workspace --all-features -- -D warnings  # lint (zero warnings enforced)
cargo test --workspace --all-features          # ~2,800 tests across 13 crates
cargo test -p selene-gql                       # GQL engine only
cargo test -p selene-server --all-features     # server + sync + federation
cargo bench -p selene-gql                      # benchmarks (run sequentially)
cargo doc --workspace --all-features --no-deps # docs (zero warnings required)
```

CI runs on every push with `clippy --all-targets -- -D warnings` to catch lint in all build targets including integration tests.

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
