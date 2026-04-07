# Selene

Lightweight, in-memory property graph runtime for IoT, smart buildings, and domains requiring a living graph of connected entities with real-time state. Written in pure Rust (zero C/C++ dependencies).

v0.2.0 release candidate. 13 crates, ~147K LOC, ~1,100 unit tests (0 failures) + 14 integration tests. AI MVP (GraphRAG, agent memory, Text2GQL, EmbeddingGemma) with integration tests on dev. Release gate cleared. Seven rounds of agent usability testing completed (75 issues fixed).

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
cargo run -p selene-server -- --dev
```

Use `--all-features` for consistency with CI. Only build-variant features (`dev-tls`, `bench`) remain, but the flag is harmless and ensures full coverage.

## Architecture

Cargo workspace with 13 crates:

| Crate | Purpose |
|-------|---------|
| `selene-core` | Types: Node, Edge, Value, IStr, PropertyMap, LabelSet, Vector, schema types, Codec trait, Origin |
| `selene-graph` | In-memory property graph (dense Vec), SharedGraph (ArcSwap), transactions, RoaringBitmap label indexes, TypedIndex, ViewRegistry, hybrid HNSW vector index (ArcSwap reads + RwLock mutable inserts) |
| `selene-gql` | ISO GQL engine: pest parser, AST, planner, 13-rule optimizer, WCO joins, factorized representations, pattern executor, pipeline, mutations, plan cache, materialized views (DDL + MATCH VIEW), GraphRAG + memory procedures, pluggable embedding layer (EmbeddingProvider trait, GemmaProvider), procedure introspection (`graph.procedures()`) |
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
- `--all-features` and `--all-targets` on all CI clippy steps (catches lint in integration test files that incremental builds miss)
- `--all-features` on all CI test/doc steps
- All product features (AI, vector, search, federation, RDF, cloud-storage) are always compiled. Enable/disable at runtime via `ServicesConfig` profiles (Edge/Cloud/Standalone) or environment variables.
- Remaining compile-time feature flags (build variants only): `dev-tls` (rcgen), `insecure` (client TLS bypass), `bench` (criterion)
- Arrow `Array` trait must be in scope for `is_null()`/`value()`
- `bool as u8` must use `u8::from()`
- Functions that never return `Err` must not wrap in `Result`
- `#![forbid(unsafe_code)]` on 10 crates; `#![deny(unsafe_code)]` on selene-server and selene-gql with targeted allows

## Conventions

- **Commit format:** conventional commits. `feat(scope):`, `fix(scope):`, `refactor(scope):`. Scope matches the crate or component.
- **GQL is the sole query and mutation interface.** All transports (HTTP, QUIC, MCP) route through GQL. No SQL or Cypher paths.
- **Ops layer pattern:** business logic lives in `selene-server/src/ops/`. Transports are thin adapters. All write operations route through the mutation batcher. GQL execution is in `ops/gql/{mod,ddl,routing,format}.rs`; HTTP routes in `http/routes/{mod,system,nodes,edges,gql,schemas,data}.rs`.
- **QueryBuilder for reads:** `QueryBuilder::new(query, &graph).with_scope(&s).execute()`
- **MutationBuilder for writes:** `MutationBuilder::new(query).with_scope(&s).with_parameters(&params).execute(&shared)` for auto-commit, `.execute_in_transaction(&mut txn)` for explicit transactions. Target (graph/txn) passed to execute, not constructor. Supports `$param` placeholders same as QueryBuilder.
- **API encapsulation:** ServerState fields `pub(crate)` with accessor methods. selene-gql internal modules `pub(crate)` with explicit re-exports.
- **MCP tool dispatch:** Manual `call_tool` (not `#[tool_handler]` macro) wraps dispatch in `tokio::select!` for cancellation. Static ToolRouter handles built-in tools; `CustomToolRegistry` provides dynamic fallback for embedder tools. Tool implementations split into `http/mcp/tools/{mod,memory,ai,schemas}.rs`. The `#[tool_router]` macro requires all `#[tool]` methods in a single impl block (mod.rs); long tools use thin dispatchers that delegate to domain submodules.
- **MCP tool queries:** All MCP tools must use parameterized GQL queries (`$param` placeholders + `HashMap<String, Value>`). No string interpolation of user input. Enforced by convention after the AI MVP review.
- **MCP auth:** Production uses `tokio::task_local!` to scope `AuthContext` per-request between the axum middleware and rmcp factory. No shared mutable state.
- **HTTP graceful shutdown:** `serve_router` accepts `Option<CancellationToken>`. In-flight requests drain before exit (bounded by 60s `TimeoutLayer`).
- **Readiness vs liveness:** `/health` is the liveness probe (always returns). `/ready` returns 503 until `ServerState::is_ready()` (set after all background tasks spawn).
- **persist_or_die:** WAL retry (3 attempts) then abort. SQLite philosophy.
- **Branch model:** `main` is releases only. Active development targets `dev`. Feature branches are created from `dev` and merged back to `dev` via PR. The release flow is: merge `dev` into `main`, then tag.

## Important notes

- **Node IDs start at 1** in tests, not 0. The `id()` function returns `GqlValue::Int` (not UInt) for consistent comparison with parameters.
- **Value cross-variant equality:** `Value::String(s)` and `Value::InternedStr(i)` compare equal when content matches. Required for dictionary encoding correctness.
- **Dictionary encoding:** `DICTIONARY` DDL keyword auto-promotes string values to `Value::InternedStr(IStr)` on write across all paths (GQL, HTTP CRUD, MCP CRUD, CSV import). All write paths must handle this.
- **Optimizer rules must handle correlated subquery context.** OptimizeContext carries a graph reference for rules needing runtime data.
- **Benchmarks run sequentially**, one crate at a time. Never `cargo bench --workspace`. Stress profile needs `SELENE_MAX_BINDINGS=500000`.
- **Node limit:** RoaringBitmap label indexes store node IDs as `u32`. Graphs exceeding ~4 billion nodes would silently truncate. Practical ceiling for in-memory use is well below this.
- **Transaction keywords (START TRANSACTION, COMMIT, ROLLBACK) return errors over the wire.** Multi-statement transactions only work through the Rust API (`SharedGraph::begin_transaction`). Each GQL mutation auto-commits.
- **MERGE uses label bitmap intersection** for O(label_count) existence checks, not full graph scan. Checks the live graph inside the write lock (not the pre-lock snapshot).
- **Materialized views:** `CREATE MATERIALIZED VIEW name AS MATCH ... RETURN agg(...)`. Maintained incrementally via changelog subscriber. Definitions persist in snapshots (tag 0x04). State rebuilds on startup. Query via `MATCH VIEW name YIELD col1, col2`.
- **MCP server:** 54 tools, 6 resources, 4 prompts, logging, completions, resource subscriptions, cancellation. Custom tools via `CustomMcpTool` trait + `CustomToolRegistry` (registered as a service). OAuth 2.1 (PKCE + client credentials) + API key auth. 11 integration tests.
- **AI tools:** GraphRAG hybrid retriever (`graphrag_search`: local/global/hybrid modes), agent memory (`remember`/`recall`/`forget`/`configure_memory` with configurable eviction), community detection (`build_communities`/`enrich_communities`), Text2GQL toolkit (`schema_dump`, `gql_parse_check` with fuzzy repair, `text2gql` prompt, `gql-examples` resource).
- **Agent tools:** Entity resolver (`resolve`: ID/name/semantic layered lookup), composite neighborhood (`related`), training data (`log_trace`/`export_traces`), action proposals (`propose_action`/`list_proposals`/`approve_proposal`/`reject_proposal`/`execute_proposal`).
- **System node labels:** Double-underscore prefix (`__Memory`, `__Entity`, `__Episode`, `__MemoryConfig`, `__CommunitySummary`, `__Trace`, `__Proposal`) are reserved for internal/AI features. Excluded from `graph.schemaDump()` by default.
- **Agent memory eviction:** Three policies configurable per-namespace via `__MemoryConfig`: "clock" (default, 2-bit counters 0-3), "oldest" (evict oldest created_at), "lowest_confidence" (evict least confident, tiebreak oldest). Evict-on-write in the `remember` tool. Clock counters are ephemeral (in-memory on ServerState, not persisted). Cold start falls back to oldest-first.
- **HNSW index:** Hybrid architecture. `ArcSwap<HnswGraph>` for lock-free reads (~1ns). `RwLock<HnswGraph>` for O(log n) incremental inserts. Periodic `snapshot()` publishes mutable graph to the read path. `rebuild()` retained for bulk operations. MN-RU eager neighbor reconnection on deletion (reconnects mutual neighbors before tombstoning). `snapshot()` cleans up tombstones physically. Rebuilds from node vectors on startup (not persisted in snapshots). Dimension mismatch detection logs warning and skips mismatched inserts.
- **Embedding layer:** Pluggable `EmbeddingProvider` trait in `selene-gql/src/runtime/embed/`. Default (and only built-in): `GemmaProvider` (EmbeddingGemma-300M, 768d with MRL truncation to 512/256/128). Candle-native inference, zero C/C++ deps. Task-specific prompts (Retrieval, Document, Clustering, etc.) routed through all embed call sites. Config via `VectorConfig.dimensions`. Model path resolves from config, `SELENE_MODEL_PATH` env var, or `data/models/embeddinggemma-300m` default. Download via `scripts/fetch-embeddinggemma.sh` (requires HF token with Gemma license). `graph.reindex()` procedure for re-embedding validation after model switch (dry run; validates vectors but does not write them). Namespace parameter on trait methods prepared for future multi-namespace support.
- **Embedding provider error caching:** The embedding provider is cached in a `OnceLock`. If the first load fails (missing model file, corrupt weights), the error is cached permanently. A server restart is required to retry. This is intentional MVP behavior; no hot-reload of ML models.
- **Embedding health:** `/health` endpoint includes `embedding` object with `loaded`, `model_id`, `dimensions`, `model_path`, and `error` fields. Added to HTTP JSON only (not wire DTO) to avoid breaking postcard serialization.
- **Projection catalog:** `SharedCatalog` persists on `ServerState` across HTTP requests. `graph.project()` stores projections with their config; `ensure_fresh()` lazily rebuilds stale projections (generation mismatch) from the stored config, preserving user label/edge filters. Algorithms call `get_projection_or_build()` which invokes `ensure_fresh` before execution. `graph.listProjections()` and `graph.drop()` operate on the same persistent catalog.
- **Multi-MATCH mutations:** `MATCH (a) WHERE ... MATCH (b) WHERE ... INSERT (a)-[:e]->(b)` supported. Grammar allows `(match_stmt | filter_stmt)*` before mutation ops. Planner adds a Join between consecutive MATCH pattern ops. INSERT executor checks bindings for already-bound variables before creating nodes.
- **NestedMatch pipeline op:** `WITH d MATCH (d)-[:e]->(x)` works via `PipelineOp::NestedMatch`. The planner detects MATCH-after-WITH and emits a NestedMatch instead of appending to the initial pattern_ops. The executor runs correlated pattern execution seeded by each input binding.
- **SSE subscriptions:** `GET /subscribe` streams graph change events as Server-Sent Events. Filter by node labels, change types, property keys. Hooks into the existing changelog broadcast channel.
- **Graph traversal:** `graph_slice` supports 4 modes: `full`, `labels`, `containment`, `traverse`. The `traverse` mode does BFS from a root node following specified edge labels and direction, with `_depth` annotation on result nodes.

## Test patterns

Graph fixtures: `let mut m = graph.mutate(); m.create_node(LabelSet::from_strs(&["Label"]), PropertyMap::from_pairs(vec![...])).unwrap(); m.commit(0).unwrap();`

SharedGraph for mutation tests: `let shared = SharedGraph::new(SeleneGraph::new());` then use `MutationBuilder::new(query).with_parameters(&params).execute(&shared)`.

GQL edge creation between existing nodes: `MATCH (a) WHERE id(a) = 1 MATCH (b) WHERE id(b) = 2 INSERT (a)-[:edge]->(b)`. The INSERT reuses bound variables from MATCH instead of creating new nodes.

Multi-label INSERT: `INSERT (n:Label1&Label2 {name: 'test'}) RETURN id(n)`. Uses the full `label_expr` grammar.

MERGE with RETURN: `MERGE (n:Label {key: 'val'}) RETURN id(n) AS id`. The MERGE variable is bound to the output scope.

GQL read tests: `QueryBuilder::new(query, &graph).execute().unwrap()`.

Optimizer rule tests: construct `ExecutionPlan` manually, call `rule.rewrite(plan, &OptimizeContext::empty())`. Use `OptimizeContext::new(&graph)` for rules needing cardinality stats.

Procedure tests: implement `Procedure` trait, call `proc.execute(args, &graph, ts)` directly.

`FunctionCall` struct has `count_star: bool`, not `distinct`.

**Math function domain guards:** `log()`, `log10()`, `ln()`, `sqrt()` all return `Null` for zero/negative domain inputs. This is enforced consistently. Do not use raw Rust `.ln()`/`.sqrt()` which return `-inf`/`NaN`.

**Procedure conventions:** Yield column names use snake_case (`node_id`, `node_count`, `edge_count`, `community_id`, `component_id`). Procedure lookup is case-insensitive. YIELD column matching is underscore-insensitive (both `node_id` and `nodeId` work). Invalid YIELD columns produce a helpful error listing available columns. `CALL proc() YIELD col` works without RETURN. `YIELD *` returns all columns. `CALL graph.procedures() YIELD name, params, yields` introspects all registered procedures.

**Subquery WHERE:** `EXISTS { MATCH (n)-[:e]->(m) WHERE m.prop > val }` and `COUNT { MATCH ... WHERE ... }` correctly apply the inner WHERE clause. The `plan_subquery_cached` function returns pattern ops only; `filter_subquery_results` applies the WHERE as a post-filter.

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

Results file: `Benchmarks.md`

## Docker

```bash
docker build -t selene .                       # distroless, ~14 MB compressed
docker compose up -d                           # hardened: read_only, cap_drop ALL
```

## Deferred work

Tracked in `_agentskills/DEFERRED.md` (4 actionable, 8 done) and `_agentskills/FUTURE_ROADMAP.md` (14 gated/v2+).
