# Changelog

All notable changes to Selene are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.2.0] - 2026-04-19

v1.2.0 is a single-theme breaking release. The agent-memory abstraction —
`remember`/`recall`/`forget`/`configure_memory`, the `memory.recall` procedure,
`__Memory` / `__MemoryConfig` node labels, clock-based eviction, TTL tiers —
leaves SeleneDB. Memory is an application concern; the DB provides graph,
vector, time-series, and text-search primitives. With memory gone, the
embedding backend goes with it: SeleneDB is now **BYO-vector**. Applications
embed text in their own process and pass pre-computed vectors as query
parameters. This removes candle, EmbeddingGemma, and ~4,000 lines of glue
from the server.

### Removed (breaking)

#### Agent memory surface
- **MCP tools**: `remember`, `recall`, `forget`, `configure_memory`. The only
  in-repo consumer was our own training scenarios, which now call Aether's
  `aether-memory` crate with SeleneDB as the storage primitive.
- **GQL procedure**: `CALL memory.recall(...)`.
- **Node labels**: `__Memory`, `__MemoryConfig`. Pre-1.2 snapshots still load
  — these become ordinary nodes with no special handling. Operators who want
  them gone can run `MATCH (m:__Memory) DETACH DELETE m` (and the same for
  `__MemoryConfig`).
- **Eviction plumbing**: `ServerState::clock_counters` and the three
  eviction-candidate helpers.

#### Embedding backend
- **GQL scalar**: `embed('text') -> Vector`. Callers supply vectors directly.
- **`selene-gql/src/runtime/embed/`** (2,196 LOC): EmbeddingGemma loader,
  tokenizer integration, quantized encoder, HTTP provider fallback,
  embedding_status / embedding_dimensions helpers.
- **Dependencies**: `candle-core`, `candle-nn`, `candle-transformers`,
  `tokenizers` removed from `selene-gql`.
- **Feature flags**: `embed`, `cuda`, `metal` (on both `selene-gql` and
  `selene-server`).
- **Server auto-embed**: the `auto_embed_loop` background task and
  `AutoEmbedRule` config. The HNSW rebuild task keeps running; it now indexes
  whatever vectors the application has written.
- **GQL procedure**: `graph.reindex` / `graph.reindexStatus`. Re-embedding is
  an application workflow now; the DB rebuilds HNSW from the vectors already
  on disk.
- **Config surface**: `VectorConfig::{model, model_path, dimensions, endpoint,
  auto_embed, lazy_load}`. HNSW tuning fields are unchanged.

#### Cascading MCP tool removals
- **`semantic_search`** — redundant with `gql_query` +
  `CALL graph.semanticSearch($queryVec, $k, $label?)`.
- **`enrich_communities`** — composed text from community profiles and called
  `embed($text)` server-side; applications now do this themselves.
- **`resolve`** drops its semantic-search fallback strategy. ID match and
  exact name match remain.

### Changed (breaking)

GQL procedure signatures are updated to take a `Vector` where they previously
took text. Client applications must embed and pass `$queryVec`:

- `graph.semanticSearch(queryText, k, label?)` → `(queryVector, k, label?)`
- `graph.scopedSemanticSearch(rootId, maxHops, queryText, k)` →
  `(rootId, maxHops, queryVector, k)`
- `graph.communitySearch(queryText, k, communityProp?)` →
  `(queryVector, k, communityProp?)`
- `graph.hybridSearch(label, query, k)` →
  `(label, queryText, queryVector, k)` — BM25 lexical and vector similarity
  are now both first-class inputs, so the client doesn't have to embed its
  own query twice.
- `graphrag.search(queryText, k, maxHops, mode)` →
  `(queryVector, k, maxHops, mode)`. The `graphrag_search` MCP tool takes
  `query_vector: number[]` instead of `query: string`.

### Migration notes

No backwards-compatibility shims are provided. For applications that
previously relied on server-side embedding:

1. Add an embedding model to your application layer (Aether, aether-memory,
   or any OSS embedder). EmbeddingGemma remains a reasonable default.
2. Replace `embed($text)` in GQL with `$vec` parameters; compute embeddings
   before sending the query.
3. Replace `remember`/`recall`/`forget`/`configure_memory` with equivalents
   in your memory layer (see Aether's `aether-memory` for one implementation).
4. Replace `semantic_search` tool calls with `gql_query` +
   `CALL graph.semanticSearch($queryVec, $k)`.
5. Replace `enrich_communities` with a client loop that writes
   `SET c.embedding = $vec` for each `__CommunitySummary` row.

### Internals

- HNSW rebuild on startup no longer warns on dimension mismatch (it had no
  way to know the "expected" dimension without an embedding provider).
- Schema-dump system-label filtering (`__` prefix) is unchanged. The
  convention still applies to `__CommunitySummary` — the filtered label in
  tests just changed.

## [1.1.0] - 2026-04-17

Spatial becomes a first-class retrieval shape in v1.1.0. A new `GEOMETRY`
property type and 18 OGC-aligned `ST_*` scalar functions cover point-in-polygon,
distance, and envelope queries — all running inside the database process with
zero C/C++ dependencies. Alongside that, the server narrows its scope back to
graph-database primitives: the multi-agent coordination tools and workflow
scaffolding are removed. The README is rewritten capability-first.

### Added

#### Spatial

- **First-class `GEOMETRY` property type** (`Value::Geometry`, `selene_core::geometry::GeometryValue`). Wraps `geo_types::Geometry<f64>` with an optional CRS hint; supports Point, LineString, Polygon (with holes), MultiPoint, MultiLineString, MultiPolygon, and GeometryCollection. Round-trips through GeoJSON (RFC 7946) and postcard. Includes a hand-rolled WKT serializer with no additional crate dependencies.
- **18 `ST_*` scalar functions** in GQL (`crates/selene-gql/src/runtime/functions/spatial.rs`): constructors (`ST_Point`, `ST_GeomFromGeoJSON`, `ST_MakePolygon`), accessors (`ST_X`, `ST_Y`, `ST_GeometryType`, `ST_IsValid`, `ST_AsGeoJSON`), predicates (`ST_Contains`, `ST_Within`, `ST_Intersects`, `ST_Equals`, `ST_DWithin`), measurements (`ST_Distance`, `ST_DistanceSphere`, `ST_Area`, `ST_Length`), and `ST_Envelope`. `ST_Distance` dispatches to haversine for two WGS84 Points and euclidean otherwise.
- **Spatial query guide** (`docs/guides/spatial.md`) covering geometry types, ingest paths, query patterns, the full function reference, CRS semantics, the FILTER-vs-WHERE scoping gotcha, performance notes, and a zone-based sensor monitoring example.
- **GeoSPARQL interop** in `selene-rdf`: Point values export as `geo:wktLiteral` (with the OGC CRS84 IRI for WGS84 points, giving the broadest engine support across Jena, RDF4J, Stardog, and GraphDB); other geometries export as `geo:geoJSONLiteral` so Selene → RDF → Selene round-trips stay lossless until the WKT parser grows beyond the Point shape. The importer accepts both `wktLiteral` and `geoJSONLiteral`, and normalizes CRS84 and EPSG:4326 IRIs back to Selene's short `EPSG:4326` tag.
- **Spatial benchmark suite** — five workloads (distance sort, radius filter, point-in-polygon, polygon intersection, envelope) wired into `cargo bench -p selene-gql` with per-bench throughput reflecting actual work shape.

#### GQL Engine

- **List iteration family**: list comprehensions, pattern comprehensions, and `ANY`/`ALL`/`NONE`/`SINGLE` quantifiers over lists.
- **`EXISTS { ... }` subqueries** with early-termination semi-join.
- **Coercion rules** for mixed numeric, temporal, and string pipeline values.
- **`validation_mode` on DDL** for gradual schema migration.

#### Server

- **OAuth token revocation** MCP tools and signing-key rotation with a retired-key ring to support zero-downtime key changes.
- **Managed API keys in the encrypted vault**, with issuance, rotation, and revocation tools.
- **SSE/WS broadcast backpressure**: subscribers are notified on lag rather than silently dropped; WS message size is configurable.
- **HTTP robustness pass**: Accept header parsing, a snapshot janitor for periodic cleanup, an anonymous request tier with rate limits, and a structured error flag on responses.
- **MCP DX hardening**: structured error types, input validation across all tools, and a standardized `structured_result` / `structured_text_result` return convention.
- **MCP tool hot-path benchmarks** (`crates/selene-server/benches/mcp_tool_bench.rs`). Five criterion groups cover the ops functions that MCP tools delegate to: `graph_stats`, `health`, `list_nodes`, parameterized GQL reads, and GQL INSERTs. Runs under the standard `SELENE_BENCH_PROFILE` scales; integrated into the per-crate bench run commands.
- **Local macOS Metal deployment target** for building GPU-accelerated binaries outside CI.

### Changed

- **README rewritten capability-first** — "One database, many retrieval shapes" replaces the previous AI-agent-centric positioning. Graph, vector, time-series, full-text, spatial, RAG, and RDF now each get a one-line capability bullet. The tagline is "A property graph database with GQL, vector search, time-series, and on-device embeddings."
- **`execute_plan_inner` refactor** (`crates/selene-gql/src/runtime/execute/mod.rs`). Extracted three named helpers — `try_count_only_shortcut`, `partition_pipeline`, and `apply_factorized_streaming_op` — to reduce the core execution function from 383 to 280 lines. No behavioral change; all `selene-gql` tests pass unchanged.
- **Embedding model feature-gated** with an HTTP endpoint fallback for environments that don't want the bundled Candle pipeline compiled in.
- **Migrated to `ureq` 3.x** and bumped the shared Rust dependency group to current minor versions.

### Removed

- **Multi-agent coordination bridge** removed from the server. The 19 MCP tools (`register_agent`, `heartbeat`, `deregister_agent`, `list_agents`, `share_context`, `get_shared_context`, `claim_intent`, `release_intent`, `check_conflicts`, `start_investigation`, `close_investigation`, `list_investigations`, `find_capable_agent`, `agent_stats`, `propose_task`, `accept_task`, `reject_task`, `complete_task`, `list_tasks`), the `selene://agents` and `selene://agents/{project}` MCP resources, and the background agent-session reaper have all been removed. SeleneDB is refocusing on graph-database primitives; coordination patterns belong in consumers that use Selene as a substrate. Existing `__AgentSession`, `__SharedContext`, `__Investigation`, `__Intent`, and `__Task` nodes in persisted graphs are still queryable via GQL but are no longer maintained by the server.
- **Proposals and trace MCP tools** removed as part of the same scope cleanup. These were higher-level workflow primitives that now belong in consumer packs on top of SeleneDB.
- **`agent-workflows.md` and related docs** removed; `docs/guides/` replaces them with feature-specific guides.

### Fixed

- **Auth**: preserve the originally-granted role on `refresh_standalone` instead of falling back to the default role.
- **Rate limiting**: `Authorization: BEARER …` and other mixed-case scheme forms are now recognized per RFC 9110 §11.1 instead of being misclassified as anonymous traffic.
- **WebSocket close codes** use axum's named `close_code::POLICY` / `close_code::ERROR` constants rather than magic numbers.
- **GQL HTTP embedding provider** hardened with timeouts, input length limits, source tracking, and URL validation.
- **README GPU claim** corrected — CUDA and Metal both require building from source with the right feature.
- **Config**: TOML example aligned with the actual `ConfigFile` schema.
- **MCP**: `rmcp` upgraded to 1.4.0 and `prompt_handler` visibility fixed.
- **CI**: rustfmt drift, `cargo audit` handling, and Copilot README suggestions addressed.
- **Release workflow**: removed the orphaned `build-cuda` job; `Dockerfile.gpu` had been deleted with the other GCP infrastructure files but the release pipeline still referenced it, blocking tag-triggered releases. CUDA is now a build-from-source feature (`cargo build --features cuda`) per the README.

## [1.0.0] - 2026-04-12

### Added

#### Graph
- **PolarQuant vector quantization** for HNSW indexes — 4–10× memory compression with >99% recall. Supports 3-bit (10.7×), 4-bit (8×), and 8-bit (4×) quantization. Asymmetric search uses f32 queries against quantized codes for maximum accuracy. Optional rescore re-ranks results with full-precision vectors.
- `QuantizedStorage` with Haar-random rotation, Lloyd-Max scalar quantization, and bit-packed codes. Zero external dependencies — pure Rust implementation.
- Quantized search path: upper HNSW layers use f32 cosine for navigation accuracy; layer-0 beam search uses asymmetric dot product with quantized codes.

#### GQL Engine
- `vector.quantizationStats()` procedure — reports compression ratio, memory savings, bit width, vector count, and configuration.

#### Server
- TOML configuration for quantization: `hnsw_quantize`, `hnsw_quantize_bits`, `hnsw_quantize_rescore` in `[vector]` section.
- `quantization_stats` MCP tool for AI agent access to quantization metrics.
- JSON structured log output for production environments.
- Tracing spans at all service boundaries and spawn sites.
- Test infrastructure consolidation into shared support module.
- CI adoption of cargo-nextest for per-process test isolation.
- 25 MCP tool integration tests for graph CRUD and operations.
- Context bridge enhancements from upstream agent proposals.
- Bridge security hardening and integration tests.
- Heartbeat improvements and agent performance tracking.
- Batch semantic_search lookups and aggregate trust query optimization.

#### DevOps
- Multi-arch Docker release pipeline (amd64 + arm64) with CUDA GPU variant.
- Native binary releases for Linux (musl) and macOS (Apple Silicon with Metal, Intel).
- GitHub Actions release automation with semver tagging.

## [0.2.0] - 2026-04-04

### Added

#### GQL Engine
- Incremental view maintenance: `CREATE MATERIALIZED VIEW name AS MATCH ... RETURN agg(...)` with changelog subscriber for incremental updates. Query via `MATCH VIEW name YIELD col1, col2`. Definitions persist in snapshots (tag 0x04).
- Worst-case optimal (WCO) joins for cyclic graph patterns (triangle queries). Detects LabelScan + Expand + Expand + CycleJoin sequences and rewrites to a single WcoJoin above a cost threshold.
- Community-enhanced RAG: Louvain community detection combined with vector search for context-aware retrieval.
- Factorized representations for multi-hop graph patterns, reducing intermediate materialization.
- Vectorized execution pipeline: columnar DataChunk types, batch expression evaluation, native filter dispatch, and end-to-end vectorized executor.
- TopK pushdown: evaluate property filters inline during ordered index scan.
- Schema audit procedures for migration progress tracking.
- Schema default injection: fall back to schema defaults on property access.
- `ENCODING` keyword in DDL for time-series value encoding selection.

#### Graph
- HNSW vector index with greedy descent, beam search, heuristic neighbor selection, staging buffer, tombstones, and rebuild. Serialized in snapshot extra sections. Used by `vectorSearch` with brute-force fallback.
- TypedIndex statistics for selectivity estimation and range pruning.
- `HnswIndex` integration into `SeleneGraph` with background rebuild task.
- ViewRegistry for materialized view lifecycle management.

#### Server
- OAuth 2.1 with authorization code + PKCE, client credentials, refresh tokens, and deny-list pruning. Wired into MCP sessions with JWT-based AuthContext.
- Partial sync subscriptions: TOML-based subscription config, property predicate evaluation, subscription-based push/pull filtering, and hub-side filtered snapshot builder.
- Bidirectional sync state machine with push/pull orchestration, LWW merge via MergeTracker, WAL v2 format with origin byte for sync tracking, and SyncCursor for resumable push/pull.
- HNSW configuration parameters in VectorConfig.
- MCP resources (health, stats, schemas, info) and 3 prompt templates (explore-graph, query-helper, import-guide).
- MCP tools: `export_rdf`, `sparql_query`, `update_schema`.
- Production MCP authentication via API key.

#### Persistence
- WAL v2 format: postcard + zstd + XXH3 + HLC timestamps + origin byte (Local/Replicated).
- HNSW index serialization in snapshot extra sections.
- SyncCursor for resumable push/pull tracking.

#### Time-Series
- Multi-encoding hot tier: RLE and dictionary value encoding alongside Gorilla (default).
- TsBlock serialization with version envelope and legacy compatibility.
- Encoding hint support in HotTier, propagated from schema on write.

#### Testing
- 564 new tests across all phases of coverage hardening (1,553 total, up from ~989).
  - Phase 1: mutation execution (INSERT, SET, DELETE, MERGE, dictionary encoding), 180 built-in function tests (string, math, temporal, core), 38 WAL corruption and snapshot hardening tests.
  - Phase 2: 25 selene-client tests, 68 ops layer tests (GQL bridge, CSV import, schema CRUD, auth scope), 97 algorithm procedure tests.
  - Phase 3: optimizer rule isolation (7 previously untested rules), WCO planner, subquery execution (EXISTS, COUNT, correlated), 47 server infrastructure tests (sync validation, merge tracker LWW, mutation batcher, vault audit), 62 algorithm edge cases, 11 SharedGraph concurrency tests.

#### Documentation
- Industry applications guide.
- MCP OAuth configuration and usage guide.
- TS hot tier encoding design spec and implementation plan.
- Offline-first sync design spec and implementation plan.

### Fixed
- `ZonedTimeConstructorFunction` now handles negative UTC offsets (e.g., `-05:00`) instead of silently returning offset 0.
- `sqrt()` returns Null for negative inputs instead of NaN, consistent with `log()`/`log10()` domain guards.
- `ln()` returns Null for zero and errors for negative inputs, aligned with `log()`/`log10()`.
- MERGE uses label bitmap intersection for O(label_count) existence checks, not full graph scan. Checks the live graph inside the write lock.
- Transaction keywords (START TRANSACTION, COMMIT, ROLLBACK) now return explicit errors over the wire instead of silently failing.
- SPARQL adapter missing lifetime parameter on `CsrAdjacency`.
- 19 rustdoc warnings across 6 crates.
- Nested mutex guard, batched cursor saves, atomic truncation floor in sync handler.
- Evict deleted entities from MergeTracker to bound memory.
- WAL truncation guard preserves un-pushed sync entries.
- Compact changelog `by_node` index to prevent unbounded growth.
- TOCTOU race in ProjectionCatalog invalidation.
- Bound SPARQL parse cache to prevent unbounded memory growth.
- Route QUIC writes through mutation batcher.
- Restore `updated_at` from WAL entry timestamps during recovery.
- Guard `apply_changes` against duplicate node/edge insertion.

### Changed
- Upgraded `rand` 0.10, `jsonwebtoken` 10, `sha2` 0.11, `getrandom` 0.4.
- Audit remediation tiers 1 and 2 across 10 crates.
- CI Docker builds now use native ARM runners instead of QEMU emulation.
- Workspace dependency consolidation and tightened module visibility.
- Dead code removal: 14 CRUD DTOs, 2 MsgType variants, NFA module, ServiceManager, and other unused items.
- Refactored MCP into module directory with `mutate()`/`submit_mut()` helpers.
- Moved `node_edges` pagination and `server_info` from transports to ops layer.
- Pre-allocate serialize buffer and use integer bit-width math for performance.

### Security
- Ignored RUSTSEC-2023-0071 (rsa timing side-channel, not exploitable in Selene's use).
- MCP OAuth endpoints hardened: CSRF protection, rate limiting, input validation, refresh auth.
- JWT AuthContext threaded into MCP sessions instead of hardcoded admin.
- Pre-built JWT Validation, cached principal lookups.
- Bounded deny-list and live graph role lookup on JWT validation.
- Enforced server-side batch limits on SyncPush requests.
- Gated SyncSubscribe behind auth and Cedar scope intersection.
- Validated SyncSubscribe complexity to prevent DoS.

## [0.1.0] - 2026-03-30

Initial release candidate.

### Core
- In-memory property graph with dense `Vec<Option<Node/Edge>>` storage
- Lock-free reads via ArcSwap (~1 ns), RwLock for writes
- RoaringBitmap label indexes, TypedIndex and CompositeTypedIndex
- PropertyMap (sorted SmallVec), LabelSet, IStr (interned strings)
- 14 Value types including Vector, ZonedDateTime, Duration
- TrackedMutation changelog for CDC replication
- SharedGraph with ArcSwap-based snapshot isolation

### GQL Engine
- ISO GQL (ISO/IEC 39075) parser (pest PEG grammar)
- 12 optimizer rules: ConstantFolding, FilterPushdown, RangeIndexScan,
  PredicateReorder (selectivity-aware), TopK, CompositeIndexLookup, and more
- Pattern executor with LabelScan, Expand, VarExpand (TRAIL), Join
- SIP bitmap propagation via PatternContext
- Plan cache with generation-based invalidation (~19 ns cache hit)
- 98 scalar functions, 53 CALL procedures
- INSERT/SET/DELETE mutations with auto-commit and explicit transactions
- Parameterized queries

### Time-Series
- Multi-encoding hot tier: Gorilla (default), RLE, Dictionary
- Warm tier with minute and hourly aggregates
- Cold tier: Parquet on disk (zstd, bloom filters, row-group pushdown)
- Cloud offload via ObjectStoreExporter (S3/GCS/Azure/MinIO)
- Gap-filling: `ts.valueAt` (LOCF/linear), `ts.gaps`, gap-filled `ts.range`
- Time-weighted average (`twa`), anomaly detection (`ts.anomalies`, `ts.peerAnomalies`)
- Schema-driven encoding hints: `ENCODING` and `FILL`/`INTERVAL` DDL

### Server
- QUIC + HTTP + MCP in a single binary
- 36 MCP tools, 5 resources, 3 prompt templates (rmcp 1.3)
- Ops layer pattern: business logic in ops/, transports are thin adapters
- Runtime profiles: edge, cloud, standalone
- Cedar authorization engine with argon2id credentials
- Encrypted vault graph (XChaCha20-Poly1305)
- Background tasks: snapshots, TS flush, retention, compaction, metrics
- Mutation batcher for serialized write ordering

### Persistence
- WAL v1: postcard + zstd + XXH3 checksums + HLC timestamps
- Binary snapshots with sub-second recovery
- persist_or_die retry policy (3 attempts then abort)

### Federation and Replication
- CDC replicas via `--replica-of` with subscribe-before-snapshot protocol
- GQL-native federation: `USE <graph>` routes to vault, local, or remote peers
- Bloom filter label routing for peer discovery
- QUIC transport with Arrow IPC results

### RDF
- Turtle/N-Triples/N-Quads import/export (oxrdf + oxttl)
- SPARQL query via zero-copy QueryableDataset adapter
- OntologyStore for Brick/223P TBox (persisted in snapshots)
- SOSA observation materialization

### Schema System
- Node and edge schemas with inheritance (max depth 32)
- Schema versioning (semver) with compatibility checking
- Property constraints: required, unique, immutable, min/max, pattern, allowed_values
- Dictionary encoding (83% memory savings for enum-like properties)
- Composite indexes, typed indexes
- Schema packs (compact TOML import)

### Graph Algorithms
- WCC, SCC, Dijkstra, PageRank, Louvain community detection
- Triangle count, betweenness centrality, label propagation
- 18 CALL procedures exposed via GQL

### Vector Search
- Cosine and euclidean similarity (brute-force top-k)
- Auto-embedding via candle all-MiniLM-L6-v2
- Hybrid search: RRF-fused BM25 + vector
- Scoped vector search with graph traversal

### DevOps
- Distroless Docker image (13.9 MB compressed, nonroot)
- Multi-arch builds (amd64 + arm64)
- GitHub Actions CI (lint, test, feature-gated test, doc audit)
- Release pipeline to GHCR with semver tagging

[Unreleased]: https://github.com/jscott3201/SeleneDB/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/jscott3201/SeleneDB/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/jscott3201/SeleneDB/compare/v0.2.0...v1.0.0
[0.2.0]: https://github.com/jscott3201/SeleneDB/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/jscott3201/SeleneDB/releases/tag/v0.1.0
