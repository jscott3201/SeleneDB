# Changelog

All notable changes to Selene are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/jscott3201/SeleneDB/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/jscott3201/SeleneDB/compare/v0.2.0...v1.0.0
[0.2.0]: https://github.com/jscott3201/SeleneDB/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/jscott3201/SeleneDB/releases/tag/v0.1.0
