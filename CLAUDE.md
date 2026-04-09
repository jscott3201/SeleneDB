# Selene

Lightweight, in-memory property graph runtime for IoT, smart buildings, and domains requiring a living graph of connected entities with real-time state. Written in pure Rust (zero C/C++ dependencies).

v0.2.0 release candidate. 13 crates, ~141K LOC, ~2,828 unit tests + 14 integration tests. AI MVP (GraphRAG, agent memory, Text2GQL, EmbeddingGemma, GGUF quantization). Eight rounds of agent usability testing (90+ issues fixed). 56 MCP tools with read/write/destructive annotations.

## Build and test

```bash
# Format
cargo fmt --all

# Lint
cargo clippy --workspace --all-features --all-targets -- -D warnings

# Test
cargo test --workspace --all-features

# Docs (zero warnings required)
cargo doc --workspace --all-features --no-deps

# Run (dev mode: QUIC :4510 + HTTP :8080)
cargo run -p selene-server --features selene-server/dev-tls -- --dev
```

Use `--all-features` locally on macOS (excludes `cuda`). CI (Linux) uses explicit features excluding `metal` (requires Apple frameworks) and `cuda` (requires NVIDIA toolkit): `--features selene-server/dev-tls,selene-testing/bench`. GPU builds use `--features selene-server/cuda` on NVIDIA hosts.

## Architecture

Cargo workspace with 13 crates:

| Crate | Purpose |
|-------|---------|
| `selene-core` | Types: Node, Edge, Value, IStr, PropertyMap, LabelSet, Vector, schema types, Codec trait, Origin |
| `selene-graph` | In-memory property graph (dense Vec), SharedGraph (ArcSwap), transactions, RoaringBitmap label indexes, TypedIndex, hybrid HNSW vector index |
| `selene-gql` | ISO GQL engine: pest parser, AST, planner, 13-rule optimizer, WCO joins, factorized representations, pattern executor, pipeline, mutations, plan cache, materialized views, GraphRAG + memory procedures, pluggable embedding layer |
| `selene-ts` | Multi-tier time-series: hot (Gorilla/RLE/Dictionary), warm aggregates, Parquet cold, cloud offload |
| `selene-persist` | WAL v2 (postcard+zstd+XXH3+HLC) + binary snapshots, recovery |
| `selene-wire` | SWP framing, codec, postcard/JSON/Arrow serialization, federation and sync DTOs |
| `selene-server` | QUIC + HTTP + MCP server, ops layer, federation, Cedar auth, OAuth 2.1, vault, sync, custom tool extensibility, SSE changelog subscriptions |
| `selene-client` | Async QUIC client |
| `selene-cli` | CLI tool |
| `selene-algorithms` | Graph algorithms: WCC, SCC, PageRank, betweenness, Dijkstra, SSSP, APSP, Louvain, label propagation, triangle count, topological sort, articulation points, bridges |
| `selene-rdf` | RDF import/export (oxrdf+oxttl), SPARQL adapter (spareval) |
| `selene-packs` | Schema pack loader (compact TOML) |
| `selene-testing` | Test factories: nodes, edges, reference buildings, synthetic topologies |

## Rust conventions

- Edition: 2024 (resolver v3)
- MSRV: 1.94
- `-D warnings` on clippy, zero warnings required
- `--all-targets` on all CI clippy steps (catches lint in integration test files that incremental builds miss)
- CI uses `--features selene-server/dev-tls,selene-testing/bench` (not `--all-features`) to exclude `metal` on Linux and `cuda` (requires NVIDIA toolkit)
- All product features (AI, vector, search, federation, RDF, cloud-storage) are always compiled. Enable/disable at runtime via `ServicesConfig` profiles (Edge/Cloud/Standalone) or environment variables.
- Remaining compile-time feature flags (build variants only): `dev-tls` (rcgen), `insecure` (client TLS bypass), `bench` (criterion)
- Arrow `Array` trait must be in scope for `is_null()`/`value()`
- `bool as u8` must use `u8::from()`
- Functions that never return `Err` must not wrap in `Result`
- `#![forbid(unsafe_code)]` on 10 crates; `#![deny(unsafe_code)]` on selene-server and selene-gql with targeted allows

## Conventions

- **Commit format:** conventional commits. `feat(scope):`, `fix(scope):`, `refactor(scope):`. Scope matches the crate or component.
- **GQL is the sole query and mutation interface.** All transports (HTTP, QUIC, MCP) route through GQL. No SQL or Cypher paths.
- **Ops layer pattern:** business logic lives in `selene-server/src/ops/`. Transports are thin adapters. All write operations route through the mutation batcher.
- **QueryBuilder for reads:** `QueryBuilder::new(query, &graph).with_scope(&s).execute()`
- **MutationBuilder for writes:** `MutationBuilder::new(query).with_scope(&s).with_parameters(&params).execute(&shared)` for auto-commit, `.execute_in_transaction(&mut txn)` for explicit transactions. Supports `$param` placeholders same as QueryBuilder.
- **MCP tool queries:** All MCP tools must use parameterized GQL queries (`$param` placeholders + `HashMap<String, Value>`). No string interpolation of user input.
- **Branch model:** `main` is releases only. Active development targets `dev`. Feature branches are created from `dev` and merged back to `dev` via PR. The release flow is: merge `dev` into `main`, then tag.

## Test patterns

Graph fixtures: `let mut m = graph.mutate(); m.create_node(LabelSet::from_strs(&["Label"]), PropertyMap::from_pairs(vec![...])).unwrap(); m.commit(0).unwrap();`

SharedGraph for mutation tests: `let shared = SharedGraph::new(SeleneGraph::new());` then use `MutationBuilder::new(query).with_parameters(&params).execute(&shared)`.

GQL edge creation between existing nodes: `MATCH (a) WHERE id(a) = 1 MATCH (b) WHERE id(b) = 2 INSERT (a)-[:edge]->(b)`. The INSERT reuses bound variables from MATCH instead of creating new nodes.

GQL read tests: `QueryBuilder::new(query, &graph).execute().unwrap()`.

Optimizer rule tests: construct `ExecutionPlan` manually, call `rule.rewrite(plan, &OptimizeContext::empty())`. Use `OptimizeContext::new(&graph)` for rules needing cardinality stats.

**Node IDs start at 1** in tests, not 0. The `id()` function returns `GqlValue::Int` (not UInt).

## Key dependencies

| Purpose | Crate | Confined to |
|---------|-------|-------------|
| GQL parser | pest 2 | selene-gql |
| Label indexes | roaring 0.11 | selene-graph |
| Columnar | arrow 58, parquet 58 | selene-ts, selene-gql |
| Serialization | postcard 1 | selene-wire, selene-persist |
| Compression | zstd | selene-wire, selene-persist, selene-ts |
| QUIC | quinn 0.11 | selene-server, selene-client |
| HTTP | axum 0.8 | selene-server |
| MCP | rmcp 1.3 | selene-server |
| Auth | cedar-policy 4 | selene-server |
| Vault | chacha20poly1305 0.10 | selene-server |
| Async | tokio 1 | throughout |
| Concurrency | parking_lot 0.12, arc-swap 1 | selene-graph |
| String interning | lasso 0.7 | selene-core |
| ML inference | candle 0.10 | selene-gql |
| RDF | oxrdf 0.3, oxttl 0.2 | selene-rdf |
| SPARQL | spareval 0.2 | selene-rdf |

## Benchmarks

7 crates, ~100 benchmarks. Run sequentially:

```bash
cargo bench -p selene-gql
cargo bench -p selene-graph
cargo bench -p selene-algorithms
cargo bench -p selene-persist
cargo bench -p selene-ts
cargo bench -p selene-wire
cargo bench -p selene-rdf --all-features
```

Stress profile: `SELENE_BENCH_PROFILE=stress SELENE_MAX_BINDINGS=500000 cargo bench -p selene-gql`

## Docker

```bash
docker build -t selene .                       # distroless, ~14 MB compressed
docker compose up -d                           # hardened: read_only, cap_drop ALL
curl -o backup.snap http://localhost:8080/snapshot  # portable graph backup
```

## Workflow graph (SeleneDB MCP)

**All project knowledge lives in the running SeleneDB graph instance.** Query via MCP tools (`gql_query`, `graph_stats`, `related`, `semantic_search`) before reading files. The graph contains 639+ nodes across 4 projects (SeleneDB, Helios, rusty-bacnet, rusty-modbus) with 718+ edges. Snapshots saved to `~/Development/SeleneSnapshots/`.

Key entity types: `project`, `crate`, `module`, `dependency`, `convention`, `preference`, `design_decision`, `topic`, `document`, `work_item`, `milestone`, `deferred_work`, `upstream_proposal`, `best_practice`, `code_pitfall`, `research`, `session_summary`, `skill`, `agent`.

Key edge types: `depends_on`, `relates_to`, `belongs_to_project`, `contains`, `used_by`, `flows_to`, `produced`, `touches`, `informs`, `preloads`, `applies_to`.

**Start every session** by querying `graph_stats` for current state, then the current sprint, open work, and recent sessions. Update the graph throughout the session: session summaries, work item status, notes, findings.

**Session start queries:**
```gql
-- Current sprint backlog
MATCH (u)-[r:relates_to]->(m:milestone) WHERE m.status = 'next' RETURN m.name, u.title, r.sprint_order ORDER BY r.sprint_order

-- What changed since last session
CALL graph.diff($sinceNanos) YIELD entity_type, change_type, label, total

-- Structural health check
CALL graph.validate() YIELD check, status, total, details
```

**Common queries:**
```gql
MATCH (c:convention) WHERE c.scope = 'selene-gql' AND c.severity = 'critical' RETURN c.name, c.description
MATCH (c:crate)-[:depends_on]->(:crate {name: 'selene-core'}) RETURN c.name
MATCH (d:design_decision) WHERE d.project = 'SeleneDB' RETURN d.name, d.rationale
MATCH (w:work_item) WHERE w.status = 'open' OR w.status = 'gated' RETURN w.title, w.status
MATCH (a:module)-[:flows_to]->(b:module) RETURN a.name, b.name
```
