# Architecture Overview

This document describes Selene's internal architecture for contributors who want to understand how the codebase is structured. It covers design philosophy, crate organization, core data structures, concurrency model, server lifecycle, and the runtime extension points.

## Design Philosophy

Selene follows five architectural principles:

1. **Single binary.** QUIC, HTTP, and MCP transports run in one process. Feature flags control which subsystems compile in; runtime config toggles control which activate.

2. **GQL-only interface.** ISO GQL (ISO/IEC 39075) is the sole query and mutation language. All three transports route through the same GQL execution engine. There is no SQL or Cypher path.

3. **In-memory property graph.** Nodes and edges live in memory, indexed by dense ID slots and secondary bitmap/BTree indexes. Persistence is append-only WAL + periodic snapshots -- the in-memory store is the source of truth at runtime.

4. **Edge-first deployment.** Selene targets Raspberry Pi 5 and equivalent constrained hardware. The binary compiles to a ~14 MB distroless Docker image. Zero C/C++ dependencies -- the entire stack is pure Rust.

5. **Zero C/C++ dependencies.** Every dependency compiles with `cargo build` alone. No system libraries, no pkg-config, no cmake. This simplifies cross-compilation for ARM64 edge devices.

## Crate Dependency Map

Selene is organized as a Cargo workspace with 13 crates. The dependency graph flows top-to-bottom:

```
                        selene-server
                       /   |   |   \  \
                      /    |   |    \   \
              selene-gql   |   |    selene-persist   selene-packs
               /  |  \     |   |       |
              /   |   \    |   |       |
 selene-algorithms |  selene-ts |      |
              \   |      |     |       |
               \  |      |     |       |
              selene-graph     |    selene-wire
                    \         |       /
                     \        |      /
                      selene-core

                  selene-client ── selene-wire
                       |
                  selene-cli

                  selene-rdf ── selene-core, selene-graph

                  selene-testing ── selene-core, selene-graph,
                                    selene-algorithms, selene-gql
```

Arrows point from dependent to dependency. `selene-core` sits at the bottom -- every crate depends on it. `selene-server` sits at the top, pulling in most of the workspace.

## Per-Crate Purpose

| Crate | Purpose |
|-------|---------|
| `selene-core` | Foundational types: `Node`, `Edge`, `Value` (14 variants), `IStr` (interned strings), `PropertyMap`, `LabelSet`, schema types, the `Change` changeset enum, and the `Codec` trait. |
| `selene-graph` | In-memory property graph storage: `SeleneGraph` (column-oriented stores), `SharedGraph` (ArcSwap concurrency), `TrackedMutation` (eager writes with rollback), RoaringBitmap label indexes, `TypedIndex` and `CompositeTypedIndex` for property lookups. |
| `selene-gql` | ISO GQL engine: Pest PEG parser, typed AST, planner with 13 optimizer rules, pattern executor (LabelScan, Expand, VarExpand, Join), pipeline executor (LET, FILTER, ORDER BY, RETURN with GROUP BY/DISTINCT/aggregation), mutation executor, 101 scalar functions, plan cache. |
| `selene-ts` | Multi-tier time series: multi-encoding hot tier (Gorilla/RLE/Dictionary, 30-min blocks), warm aggregates (minute + hourly), Parquet cold tier (zstd, bloom filters, row-group pushdown), export pipeline, and cloud offload (S3/GCS/Azure/MinIO via `object_store`). |
| `selene-persist` | Persistence: WAL v2 (postcard + zstd + XXH3 + HLC), binary snapshots, recovery. Implements `persist_or_die` -- WAL retry (3 attempts) then abort. |
| `selene-wire` | SWP framing protocol, postcard/JSON/Arrow IPC serialization, zstd compression, federation DTOs. Shared between server and client. |
| `selene-server` | Server runtime: QUIC + HTTP + MCP transports, ops layer (business logic), Cedar auth engine, vault (XChaCha20-Poly1305), federation mesh, background tasks, bootstrap, config, service registry. |
| `selene-client` | Async QUIC client with SWP framing. Used by the CLI and by replica nodes for replication. |
| `selene-cli` | Command-line tool for connecting to a Selene server over QUIC. |
| `selene-algorithms` | Graph algorithms: WCC, SCC, Dijkstra, PageRank, Louvain community detection. Exposed as 18 `CALL` procedures in GQL. |
| `selene-rdf` | RDF interop: PG-to-RDF mapping, Turtle/N-Triples/N-Quads import/export (oxrdf + oxttl), SPARQL query via `QueryableDataset` adapter (spareval), SOSA observation materialization, ontology store. Feature-gated. |
| `selene-packs` | Schema pack loader: reads compact TOML schema definitions and registers them as node/edge types. Optional convenience for bootstrapping domain models. |
| `selene-testing` | Test factories: reference building graph (6 overlays, all 14 Value types, schemas), synthetic topologies (star, chain, complete, random). Used by benchmarks and integration tests across crates. |

## Core Data Structures

### Node and Edge Storage

`SeleneGraph` stores nodes and edges in column-oriented `ChunkedVec` stores (`NodeStore`, `EdgeStore`) indexed by numeric ID. Slot 0 is always dead -- IDs start at 1.

```
crates/selene-graph/src/graph.rs  (SeleneGraph struct)
```

This design provides:

- **O(1) lookup by ID.** Array index, no hash lookup.
- **Cache-friendly iteration.** Sequential memory access when scanning.
- **Near-instant clone.** `ChunkedVec` uses 256-element chunks behind `Arc`. Cloning copies chunk pointers (O(N/256) Arc increments), not data. This enables the snapshot publish pattern described below.

Secondary indexes use `imbl::HashMap` (persistent/immutable maps with structural sharing), so cloning the graph also clones indexes in O(log N) time.

### PropertyMap

`PropertyMap` is an enum with two variants. The `Standard` variant is a sorted `SmallVec<[(IStr, Value); 6]>` for schema-less nodes -- sorted order enables binary search, and the 6-entry inline capacity avoids heap allocation for nodes with few properties (the common case in IoT). The `Compact` variant uses a shared `Arc<[IStr]>` key array with a parallel `SmallVec<[Option<Value>; 6]>` for schema-conformant nodes, saving 8 bytes per property per node by deduplicating key storage.

```
crates/selene-core/src/property_map.rs
```

### LabelSet

`LabelSet` is a sorted `SmallVec<IStr>`. Same reasoning as `PropertyMap` -- most nodes carry 1-3 labels.

```
crates/selene-core/src/label_set.rs
```

### IStr (Interned Strings)

`IStr` wraps a key into a global `ThreadedRodeo` (from the `lasso` crate). Equal strings resolve to the same key, giving O(1) comparison by integer equality rather than byte-by-byte string comparison. All identifiers, labels, and property keys are interned at parse time.

```
crates/selene-core/src/interner.rs
```

### Value Enum

`Value` has 14 variants covering all GQL and domain types:

```rust
Null, Bool, Int, UInt, Float, String, InternedStr,
Timestamp, Date, LocalDateTime, Duration,
Bytes, List, Vector
```

Notable design choices:

- **SmolStr for strings.** Strings up to 22 bytes are stored inline (no heap allocation). IoT property values ("degF", "active", "zone_1a") overwhelmingly fit inline.
- **Cross-variant string equality.** `String(s)` and `InternedStr(i)` compare equal when their content matches. This is required because dictionary encoding promotes String to InternedStr on write, and both variants can coexist for the same logical value.
- **Arc-wrapped large values.** `Bytes`, `List`, and `Vector` use `Arc<[T]>` so that cloning is a reference count increment, not a deep copy.

```
crates/selene-core/src/value.rs
```

### RoaringBitmap Label Indexes

Each label maintains a `RoaringBitmap` mapping to the set of node (or edge) IDs that carry it. Label scans resolve to bitmap intersections, unions, or differences depending on the label expression (`AND`, `OR`, `NOT`).

```
crates/selene-graph/src/graph.rs  (idx_label, idx_edge_label fields)
```

### TypedIndex and CompositeTypedIndex

`TypedIndex` uses a `BTreeMap` per value type for single-property indexed lookups. `CompositeTypedIndex` supports multi-property key lookups. Both are populated automatically for schema properties marked with `indexed: true` or schemas with multiple key properties.

```
crates/selene-graph/src/typed_index.rs
```

## SharedGraph and ArcSwap

`SharedGraph` implements a hybrid concurrency model inspired by Fjall's "SuperVersion" pattern:

- **Reads** are lock-free. `ArcSwap` stores an `Arc<SeleneGraph>` snapshot. Readers call `snapshot.load()` (~1 ns) and work with a consistent point-in-time view. No blocking, no contention between readers.
- **Writes** acquire a `RwLock` for exclusive access to the mutable graph. After a successful write, the graph is cloned and published to the `ArcSwap` for future readers.

```
crates/selene-graph/src/shared.rs  (SharedGraph struct)
```

Key methods:

- `read(f)` -- loads the ArcSwap snapshot and passes it to the closure. Short-lived.
- `load_snapshot()` -- returns an owned `Arc<SeleneGraph>` that can be held across async boundaries (used by GQL queries and transactions).
- `write(f)` -- acquires the write lock, creates a `TrackedMutation`, passes it to the closure, commits on success, and publishes a new snapshot.
- `begin_transaction()` -- acquires the write lock and returns a `TransactionHandle` for multi-statement transactions. The lock is held for the entire transaction. A CoW snapshot taken at transaction start enables rollback on drop.

## TrackedMutation

`TrackedMutation` is Selene's mutation primitive. It eagerly applies changes to the graph in-place so that subsequent operations within the same mutation see the effects. Each change records two things:

1. A forward `Change` event (for WAL persistence and changelog subscribers).
2. A reverse `RollbackEntry` (for undo on failure).

On `commit()`: schema validation runs, the generation counter increments, and changes are returned to the caller. On `drop()` without commit: rollback entries replay in reverse, restoring the graph.

```
crates/selene-graph/src/mutation.rs  (TrackedMutation struct)
```

## Ops Layer Pattern

Business logic lives in `crates/selene-server/src/ops/`. The three transports (HTTP, QUIC, MCP) are thin adapters that deserialize requests, call ops functions, and serialize responses.

This means:

- Adding a new transport does not require duplicating business logic.
- Ops functions are testable without starting a server.
- Graph routing (`USE <graph>`) is handled once in `ops/graph_resolver.rs`.

## ServiceRegistry

Optional subsystems (vector store, search index, temporal versioning, federation, vault) register as services at bootstrap instead of being feature-gated `Option` fields on `ServerState`.

`ServiceRegistry` is a `HashMap<TypeId, Arc<dyn Any + Send + Sync>>`. Services implement the `Service` trait (name + health check). Ops code retrieves services with `state.services.get::<T>()`, which returns `None` when a service is not active.

```
crates/selene-server/src/service_registry.rs
```

## Server Startup Sequence

The `bootstrap()` function in `crates/selene-server/src/bootstrap.rs` initializes all server components:

1. **Create data directory.** Ensures `config.data_dir` exists on disk.
2. **Recover graph state.** Loads the latest snapshot and replays WAL entries. If no snapshot exists, starts with an empty graph.
3. **Restore schemas.** Imports recovered node/edge schemas and rebuilds property indexes and composite indexes.
4. **Restore triggers.** Loads persisted trigger definitions into the trigger registry.
5. **Wrap in SharedGraph.** Creates the `SharedGraph` concurrency wrapper around the recovered `SeleneGraph`.
6. **Create hot tier.** Initializes the time-series hot tier from config.
7. **Open WAL.** Opens (or creates) `wal.bin` for append with the configured sync policy.
8. **Initialize changelog.** Creates the changelog buffer and broadcast channel for delta sync subscribers.
9. **Initialize auth.** Creates the Cedar auth engine with the configured policy set.
10. **Register services.** Activates optional subsystems (vector, search, temporal, vault, federation) based on config and feature flags.
11. **Build ServerState.** Assembles all components into the final `ServerState` struct.

After bootstrap, the caller spawns background tasks and starts transport listeners.

## Background Tasks

Background tasks are spawned by `spawn_background_tasks()` in `crates/selene-server/src/tasks.rs`. All tasks share `Arc<ServerState>` and respect a `CancellationToken` for graceful shutdown.

| Task | Interval | Purpose |
|------|----------|---------|
| Snapshot | Configurable (default 5 min) | Snapshot graph + truncate WAL. Also triggers when WAL entry count exceeds threshold. Skipped on replicas. |
| TS flush | Configurable (default 30 min) | Flush hot tier blocks to warm/cold storage. |
| TS retention | 1 hour | Delete time-series data older than retention window. Ships data to export pipeline before deletion. |
| TS compaction | Configurable (hours) | Compact Parquet files in cold storage. Only runs when `compact_after_hours > 0`. |
| Metrics update | 10 seconds | Refresh internal metrics counters (node count, edge count, memory usage). |
| Auto-embed | Changelog-driven | Re-embed nodes matching configured auto-embed rules when properties change. Requires `vector` feature. |
| Search index | Changelog-driven | Update tantivy full-text index from changelog. Requires `search` feature. |
| Stats collector | Changelog-driven | Maintain runtime statistics from changelog events. |
| Vector store | Changelog-driven | Sync vector index with changelog changes. |
| Version prune | Configurable (hours) | Prune old property versions from the temporal version store. Requires `temporal` service. |

## Feature Flag Matrix

Selene uses Cargo features for compile-time gating and TOML config for runtime toggles.

### Compile-Time Features (Cargo)

| Feature | Crate | Effect |
|---------|-------|--------|
| `federation` | selene-server | GQL-native mesh: registry, manager, handler, bloom filter routing |
| `vector` | selene-gql | Candle ML inference for `embed()` function (all-MiniLM-L6-v2) |
| `search` | selene-server | Tantivy full-text search index + search procedures |
| `temporal` | selene-server | Property version store for point-in-time queries |
| `cloud-storage` | selene-ts | S3/GCS/Azure/MinIO export via `object_store` crate |
| `rdf` | selene-server | RDF import/export (oxrdf + oxttl) |
| `rdf-sparql` | selene-server | SPARQL query support (spareval), implies `rdf` |
| `http` | selene-server | HTTP transport (axum) -- always compiled, runtime toggle |
| `mcp` | selene-server | MCP transport (rmcp) -- always compiled, runtime toggle |

### Runtime Toggles (TOML config)

HTTP and MCP are always compiled but activated at runtime via config. Services register based on config values at bootstrap -- for example, a `[vector]` config section activates the vector store service even though the `vector` Cargo feature gates the ML inference code.

Runtime profiles (`--profile edge|cloud|standalone`) set sensible defaults for each deployment target.

## Key External Dependencies

| Purpose | Crate | Version | Confined to |
|---------|-------|---------|-------------|
| GQL parser | pest | 2 | selene-gql |
| Label indexes | roaring | 0.11 | selene-graph |
| Columnar | arrow, parquet | 58 | selene-ts, selene-gql |
| Serialization | postcard | 1 | selene-wire, selene-persist |
| Compression | zstd | 0.13 | selene-wire, selene-persist, selene-ts |
| QUIC | quinn | 0.11 | selene-server, selene-client |
| TLS | rustls | 0.23 | selene-server, selene-client |
| HTTP | axum | 0.8 | selene-server |
| MCP | rmcp | 1 | selene-server |
| Auth | cedar-policy | 4 | selene-server |
| Hashing | xxhash-rust (XXH3) | 0.8 | selene-persist |
| Vault | chacha20poly1305 | 0.10 | selene-server |
| Async | tokio | 1 | throughout |
| Concurrency | parking_lot, arc-swap | 0.12, 1 | selene-graph |
| String interning | lasso | 0.7 | selene-core |
| ML inference | candle | 0.10 | selene-gql (vector feature) |
| Full-text search | tantivy | 0.26 | selene-server (search feature) |
| HLC timestamps | uhlc | 0.9 | selene-server |
| Cloud storage | object_store | 0.13 | selene-ts (cloud-storage feature) |
| RDF data model | oxrdf | 0.3 | selene-rdf |
| RDF serialization | oxttl | 0.2 | selene-rdf |
| SPARQL evaluation | spareval, spargebra | 0.2, 0.4 | selene-rdf (rdf-sparql feature) |
| Persistent maps | imbl | latest | selene-graph |
| Hash maps | rustc-hash (FxHashMap) | latest | selene-graph |
