# Selene v0.2.0 Benchmarks

> Captured 2026-04-03 on Apple M5 (10-core, 16 GB RAM), macOS 26.4.
> Rust 1.94, `criterion` 0.8, stress profile (scales: 200 / 1,000 / 10,000 / 100,000 / 250,000 nodes).
> `SELENE_BENCH_PROFILE=stress SELENE_MAX_BINDINGS=500000`
> **Sequential run** -- all suites executed one at a time, no parallel contention.
> Test data: reference building model (6 overlays, 17 labels, 9 edge types, all 14 Value types).

**7 crates, ~100 benchmarks across 25+ criterion groups.**

---

## Quick Reference (10K-node graph)

| Operation | Time |
|-----------|------|
| Parse simple query | 7.5 us |
| Plan cache hit | 18 ns |
| count(*) short-circuit | 3.0 ms |
| Single-hop expand | 3.2 ms |
| Two-hop expand | 3.1 ms |
| FILTER prop = val | 3.1 ms |
| ORDER BY + LIMIT | 3.4 ms |
| INSERT node | 53 us |
| avg() aggregation | 3.3 ms |
| Vector cosine (1K, 384-dim) | 58 us |
| TypedIndex lookup | 2.1 ns |
| CompositeIndex lookup | 83 ns |
| Label index lookup | 6.1 us |
| RDF export Turtle (10 bldgs) | 1.97 ms |
| SPARQL SELECT sensors (10 bldgs) | 63 us |
| SPARQL two-hop (10 bldgs) | 144 us |
| CSR build (outgoing-only, 10 bldgs) | 56 us |

## Quick Reference (100K-node graph)

| Operation | Time |
|-----------|------|
| Single-hop expand | 34.3 ms |
| Two-hop expand | 34.6 ms |
| FILTER prop = val | 33.1 ms |
| ORDER BY + LIMIT | 36.6 ms |
| count(*) short-circuit | 32.3 ms |
| avg() aggregation | 36.2 ms |
| Variable-length depth 1..3 | 46.9 ms |
| Trail depth 1..5 | 72.9 ms |
| WCC (structural) | 2.4 ms |
| PageRank (20 iter) | 3.3 ms |
| Dijkstra shortest path | 1.4 ms |
| Louvain communities | 76.1 ms |
| WAL append (single) | 106 ms |
| Snapshot write | 16.9 ms |
| Snapshot recovery | 14.8 ms |
| Full recovery | 55.3 ms |

---

## GQL Engine (selene-gql)

### Parsing (PEG grammar)

| Query | Median |
|-------|--------|
| simple (`MATCH (n) RETURN n`) | 7.5 us |
| labeled (`MATCH (s:sensor) RETURN s`) | 10.6 us |
| quantifier_shortcuts | 13.2 us |
| var_length | 20.4 us |
| filter | 23.2 us |
| unwind | 25.9 us |
| like_between | 28.7 us |
| different_edges | 30.0 us |
| complex (multi-hop + filter) | 42.9 us |
| type_ddl (`CREATE NODE TYPE`) | 2.0 us |

### Plan Cache

| Operation | Median |
|-----------|--------|
| cache_miss_parse | 10.7 us |
| **cache_hit** | **18 ns** |

### Label Scan + count(*)

| Benchmark | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| all_sensors | 65.9 us | 315 us | 3.02 ms | 32.5 ms | 114 ms |
| all_nodes | 65.6 us | 319 us | 3.06 ms | 32.7 ms | 114 ms |

### Edge Expansion

| Benchmark | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| single_hop | 82.6 us | 339 us | 3.16 ms | 34.3 ms | 121 ms |
| two_hop | 87.4 us | 341 us | 3.13 ms | 34.6 ms | 122 ms |

### Variable-Length Paths

| Benchmark | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| depth_1_3 | 84.9 us | 383 us | 3.73 ms | 46.9 ms | 151 ms |
| trail_depth_1_5 | 104 us | 491 us | 5.32 ms | 72.9 ms | 211 ms |

### Filter

| Benchmark | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| property_eq | 75.4 us | 324 us | 3.05 ms | 33.1 ms | 114 ms |
| property_gt | 75.3 us | 324 us | 3.05 ms | 33.1 ms | 114 ms |

### Sort

| Benchmark | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| order_by_property | 91.6 us | 386 us | 3.65 ms | 40.1 ms | 136 ms |
| order_by_limit (TopK) | 87.4 us | 361 us | 3.39 ms | 36.6 ms | 126 ms |

### Aggregation

| Benchmark | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| count_star | 65.7 us | 313 us | 3.02 ms | 32.3 ms | 114 ms |
| avg | 77.3 us | 346 us | 3.32 ms | 36.2 ms | 127 ms |
| group_by | 101 us | 397 us | 3.68 ms | 43.2 ms | 140 ms |

### DISTINCT and OFFSET

These benchmarks were not included in the stress profile run. Values retained from the previous (2026-03-28) full profile.

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| distinct_unit | 18.8 us | 46.3 us | 431 us |
| offset_skip_50 | 28.5 us | 75.4 us | 749 us |

### Predicates

| Benchmark | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| like_prefix | 82.6 us | 351 us | 3.31 ms | 35.5 ms | 122 ms |
| like_contains | 83.6 us | 359 us | 3.40 ms | 36.4 ms | 125 ms |
| not_like | 83.4 us | 361 us | 3.41 ms | 37.0 ms | 126 ms |
| between_float | 80.5 us | 338 us | 3.15 ms | 34.0 ms | 117 ms |

### Advanced Pipeline

| Benchmark | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| with_clause | 100 us | 354 us | 3.14 ms | 34.3 ms | 120 ms |
| having | 97.3 us | 351 us | 3.14 ms | 34.5 ms | 120 ms |
| unwind | 138 us | 486 us | 4.75 ms | 57.0 ms | 175 ms |
| nulls_first | 88.5 us | 361 us | 3.37 ms | 36.5 ms | 125 ms |

### End-to-End Workloads

| Benchmark | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| simple_return | 77.2 us | 352 us | 3.37 ms | 36.7 ms | 127 ms |
| filter_sort_limit | 87.0 us | 340 us | 3.08 ms | 33.0 ms | 114 ms |
| two_hop_with_filter | 104 us | 361 us | 3.15 ms | 34.4 ms | 120 ms |
| inline_properties | 84.1 us | 353 us | 3.28 ms | 36.0 ms | 124 ms |

### Subqueries

| Benchmark | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| count_subquery | 87.0 us | 350 us | 3.26 ms | 35.2 ms | 122 ms |
| different_edges | 114 us | 422 us | 3.81 ms | 46.3 ms | 156 ms |

### Phase 5 (Cycle Joins, RPQ)

| Benchmark | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| cyclejoin_hvac | 109 us | 426 us | 3.13 ms | 33.1 ms | 115 ms |
| rpq_containment | 87.6 us | 403 us | 3.95 ms | 54.5 ms | 176 ms |
| rpq_alternation | 99.9 us | 465 us | 4.98 ms | 68.9 ms | 213 ms |
| collect_zones | 91.6 us | 354 us | 3.26 ms | 36.2 ms | 125 ms |

### Mutations

| Operation | Median |
|-----------|--------|
| insert_node | 53.1 us |
| insert_and_read | 53.1 us |
| set_property | 403 us |
| detach_delete | 79.8 us |

### Dictionary Encoding

These benchmarks were not included in the stress profile run. Values retained from the previous (2026-03-28) full profile.

| Operation | Median |
|-----------|--------|
| insert_dict_property | 47.9 us |
| insert_regular_property | 47.5 us |
| set_dict_property | 28.9 us |
| set_regular_property | 29.2 us |

### Transactions

| Operation | Median |
|-----------|--------|
| multi_statement_txn | 149 us |
| txn_rollback | 9.5 us |

### GQL Options

| Mode | Median |
|------|--------|
| strict_coercion | 348 us |
| default_coercion | 347 us |

### Vector Search (384-dim, brute-force)

Only partial results from this run (cosine_similarity/1K). Similar_nodes benchmarks were not included.

| Benchmark | 1K | 10K |
|-----------|-----|------|
| cosine_similarity | 58.3 us | 230 us (*) |
| similar_nodes top-10 | 159 us (*) | 1.46 ms (*) |
| similar_nodes top-50 | 171 us (*) | 1.52 ms (*) |

(*) Value retained from previous (2026-03-28) full profile run.

| Math (per-pair) | Median |
|-----------------|--------|
| dot_product_384 | 114 ns |
| cosine_similarity_384 | 335 ns |

### Time-Series Procedures (via GQL CALL)

These benchmarks were not included in the stress profile run. Values retained from the previous (2026-03-28) full profile.

| Procedure | 200 | 1K | 10K |
|-----------|-----|-----|------|
| ts_latest | 10.3 us | 10.2 us | 10.2 us |
| ts_range | 24.4 us | 24.2 us | 24.4 us |
| ts_aggregate | 15.0 us | 15.0 us | 15.1 us |
| ts_scoped_aggregate | 29.2 us | 29.1 us | 31.3 us |
| ts_anomalies | 16.8 us | 16.9 us | 16.9 us |
| ts_peer_anomalies | 15.9 us | 15.9 us | 16.0 us |

---

## Graph Storage (selene-graph)

### Node CRUD

| Benchmark | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| node_create | 105 us | 547 us | 5.49 ms | 70.7 ms | 193 ms |
| node_get_by_id | 1.2 us | 6.0 us | 63.8 us | 794 us | 2.91 ms |
| node_update_property | 158 us | 567 us | 5.18 ms | 53.8 ms | 154 ms |

### Edge Creation + Node Removal

| Benchmark | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| edge_create | 70.8 us | 394 us | 394 us | 395 us | 392 us |
| node_remove (100 nodes) | 51.1 us | 134 us | 999 us | 14.9 ms | 33.5 ms |

### Label Index

| Scale | Median |
|-------|--------|
| 200 | 153 ns |
| 1K | 618 ns |
| 10K | 6.1 us |
| 100K | 48.6 us |
| 250K | 121 us |

### BFS Traversal

| Depth | 200 | 1K | 10K | 100K | 250K |
|-------|-----|-----|------|------|------|
| d=1 | 275 ns | 983 ns | 8.2 us | 100 us | 228 us |
| d=3 | 4.7 us | 22.5 us | 292 us | 3.57 ms | 12.8 ms |
| d=10 | 13.7 us | 84.0 us | 893 us | 12.4 ms | 50.0 ms |
| d=50 | 13.7 us | 84.9 us | 894 us | 12.4 ms | 50.2 ms |

### Concurrent Reads (10 threads via ArcSwap)

| Scale | Median |
|-------|--------|
| 200 | 75.5 us |
| 1K | 75.3 us |
| 10K | 75.5 us |
| 100K | 75.4 us |
| 250K | 75.3 us |

### Mutations (graph-level)

| Batch size | Commit | Rollback |
|------------|--------|----------|
| 10 | 6.9 us | 1.5 us |
| 100 | 638 us | 13.8 us |
| 1000 | 4.25 ms | 129 us |

### TypedIndex

| Benchmark | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| build | 935 ns | 3.98 us | 42.4 us | 483 us | 1.56 ms |
| **lookup** | **2.1 ns** | **2.1 ns** | **2.1 ns** | **2.1 ns** | **2.1 ns** |
| iter_asc | 1.3 ns | 1.4 ns | 1.4 ns | 1.4 ns | 1.4 ns |

### CompositeTypedIndex

| Benchmark | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| build | 7.4 us | 35.5 us | 361 us | 3.82 ms | 10.0 ms |
| **lookup** | **81 ns** | **82 ns** | **83 ns** | **82 ns** | **82 ns** |

### HNSW Index

| Benchmark | 1K |
|-----------|-----|
| build | 1.32 s |

> 10K+ HNSW build was skipped due to runtime (>25s per iteration). Only the 1K scale was measured.

---

## Graph Algorithms (selene-algorithms)

### Projection Build

| Type | 200 | 1K | 10K | 100K | 250K |
|------|-----|-----|------|------|------|
| full | 28.2 us | 174 us | 1.74 ms | 37.5 ms | 134 ms |
| containment_only | 28.4 us | 174 us | 1.75 ms | 37.5 ms | 134 ms |
| weighted | 28.7 us | 176 us | 1.80 ms | 41.5 ms | 143 ms |

### Structural

| Algorithm | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| wcc | 6.3 us | 30.5 us | 244 us | 2.42 ms | 6.05 ms |
| wcc_count | 2.4 us | 11.9 us | 136 us | 1.36 ms | 3.39 ms |
| scc | 12.1 us | 71.6 us | 709 us | 7.24 ms | 21.9 ms |
| scc_count | 11.3 us | 65.1 us | 622 us | 6.17 ms | 18.7 ms |
| topo_sort (acyclic) | 2.4 us | 10.5 us | 109 us | 1.07 ms | 2.71 ms |
| topo_sort (cyclic) | 2.1 us | 9.4 us | 98.3 us | 974 us | 2.45 ms |
| articulation_points | 33.4 us | 174 us | 1.81 ms | 18.4 ms | 49.8 ms |
| bridges | 33.3 us | 174 us | 1.82 ms | 18.3 ms | 49.7 ms |
| validate | 2.8 us | 13.7 us | 153 us | 1.57 ms | 3.89 ms |
| containment_index | 37.9 us | 199 us | 2.00 ms | 24.1 ms | 88.4 ms |

### Pathfinding

| Algorithm | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| dijkstra | 457 ns | 1.67 us | 38.3 us | 1.39 ms | 605 us |
| sssp | 3.7 us | 17.2 us | 260 us | 3.76 ms | 10.5 ms |
| apsp | 94.7 us | 1.73 ms | (skipped) | -- | -- |

### Centrality

| Algorithm | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| pagerank (20 iter) | 12.6 us | 57.4 us | 524 us | 3.28 ms | 6.89 ms |
| betweenness | 75.7 us | 807 us | (skipped) | -- | -- |

### Community

| Algorithm | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| louvain | 44.1 us | 217 us | 2.90 ms | 76.1 ms | 316 ms |
| label_propagation | 16.6 us | 89.1 us | 1.41 ms | 56.7 ms | 245 ms |
| triangle_count | 9.6 us | 48.3 us | 562 us | 7.91 ms | 28.3 ms |

---

## Time-Series (selene-ts)

Run panicked at 100K `hot_drain_before` due to duplicate benchmark ID. Data collected up to that point.

### Hot Tier

| Benchmark | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| append | 6.7 us | 25.2 us | 216 us | 2.16 ms | 5.36 ms |
| append_batch | 7.5 us | 27.1 us | 237 us | 3.63 ms | 9.98 ms |
| range_query | 73 ns | 159 ns | 632 ns | 8.59 us | 21.0 us |
| evict_idle | 2.4 us | 14.4 us | 192 us | 3.13 ms | 16.1 ms |
| drain_before | 1.04 us | 8.69 us | 48.3 us | -- | -- |

### Eviction (stale heap)

| Benchmark | Median |
|-----------|--------|
| evict_with_stale_entries (900 stale) | see bench output |

### Flush + Parquet

Values retained from previous (2026-03-28) full profile.

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| flush_to_parquet | 346 us | 397 us | 975 us |
| parquet_write_size | 217 us | 251 us | -- |

### Warm Tier

Values retained from previous (2026-03-28) full profile.

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| warm_record | 9.5 us | 47.9 us | 457 us |
| warm_range_query | 108 ns | 125 ns | 552 ns |

### Gorilla Compression

| Pattern | Encode | Decode |
|---------|--------|--------|
| regular | see bench output | see bench output |
| drifting | see bench output | see bench output |
| binary | see bench output | see bench output |

---

## Persistence (selene-persist)

### WAL (Write-Ahead Log v2)

| Benchmark | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| append (single) | 1.19 ms | 2.18 ms | 10.6 ms | 106 ms | 268 ms |
| append_batch | 391 us | 684 us | 1.86 ms | 10.9 ms | 27.2 ms |
| replay | 578 us | 1.29 ms | 8.93 ms | 89.9 ms | 223 ms |

### WAL Size (write + measure)

| Scale | Median |
|-------|--------|
| 200 | 4.84 ms |
| 1K | 6.20 ms |
| 10K | 14.9 ms |
| 100K | 112 ms |
| 250K | 273 ms |

### Snapshots

| Benchmark | 200 | 1K | 10K | 100K | 250K |
|-----------|-----|-----|------|------|------|
| write | 7.75 ms | 7.92 ms | 9.68 ms | 16.9 ms | 29.9 ms |
| write_with_edges | 8.16 ms | 8.01 ms | 10.4 ms | 21.3 ms | 41.8 ms |
| read (recovery) | 376 us | 758 us | 1.96 ms | 14.8 ms | 36.9 ms |

### Full Recovery (snapshot + WAL replay)

| Scale | Median |
|-------|--------|
| 200 | 684 us |
| 1K | 1.36 ms |
| 10K | 5.66 ms |
| 100K | 55.3 ms |
| 250K | 140 ms |

---

## Wire Protocol (selene-wire)

### Frame Encode

| Payload | Median |
|---------|--------|
| 64 B | 12 ns |
| 256 B | 19 ns |
| 1 KB | 23 ns |
| 4 KB | 54 ns |
| 16 KB | 243 ns |
| 64 KB | 819 ns |
| 256 KB | 3.4 us |

### Frame Decode Header

| Payload | Median |
|---------|--------|
| 64 B | 1.1 ns |
| 256 B | 1.1 ns |
| 1 KB | 1.1 ns |
| 4 KB | 1.2 ns |
| 16 KB | 1.2 ns |
| 64 KB | 1.2 ns |
| 256 KB | 1.2 ns |

### Serialization (NodeDto)

| Format | Serialize | Deserialize |
|--------|-----------|-------------|
| Postcard | 148 ns | 123 ns |
| JSON | 166 ns | 253 ns |

### Zstd Compression

| Payload | Compress | Decompress |
|---------|----------|------------|
| 64 B | 1.60 us | 96 ns |
| 256 B | 590 ns | 133 ns |
| 1 KB | 684 ns | 177 ns |
| 4 KB | 1.10 us | 340 ns |
| 16 KB | 1.78 us | 991 ns |
| 64 KB | 2.95 us | 3.65 us |
| 256 KB | 9.66 us | 14.7 us |

### Datagram Codec

| Operation | Median |
|-----------|--------|
| encode_single | 12.6 ns |
| decode_single | 14.0 ns |
| encode_batch_64 | 778 ns |
| roundtrip_single | 26.9 ns |
| serialize_node_dto | 36.4 ns |

---

## RDF Interop (selene-rdf)

### Export (building scale: 1 / 5 / 10 buildings)

| Format | 1 bldg | 5 bldgs | 10 bldgs |
|--------|--------|---------|----------|
| Turtle | 204 us | 982 us | 1.97 ms |
| N-Triples | 141 us | 682 us | 1.37 ms |

### Import (Turtle roundtrip)

| Scale | Median |
|-------|--------|
| 1 bldg | 660 us |
| 5 bldgs | 3.13 ms |
| 10 bldgs | 6.27 ms |

### SPARQL Query (via QueryableDataset adapter)

All queries go through spargebra parse (cached) + spareval evaluate + SeleneDataset adapter.
CSR adjacency built per query with lazy incoming direction (outgoing-only for most patterns).

| Query | 1 bldg | 5 bldgs | 10 bldgs |
|-------|--------|---------|----------|
| SELECT all sensors (type pattern) | 12.3 us | 34.6 us | 62.9 us |
| SELECT sensors + unit (type + prop) | 19.7 us | 63.6 us | 118 us |
| Two-hop containment (building->floor->zone) | 25.4 us | 78.4 us | 144 us |
| Edge traversal (server monitors equip) | 9.1 us | 14.4 us | 20.9 us |
| COUNT sensors (aggregation) | 9.2 us | 14.4 us | 20.6 us |
| **CSR build (outgoing-only)** | **5.5 us** | **24.0 us** | **56.1 us** |

### SPARQL vs GQL Comparison (10 buildings, ~486 nodes)

GQL equivalents below use the full-profile (non-stress) 10K values for comparable graph size.

| Pattern | SPARQL | GQL equivalent | Ratio |
|---------|--------|---------------|-------|
| Type scan (all sensors) | 62.9 us | ~8.6 us (label scan) | 7.3x |
| Type + property | 118 us | ~18 us (filter eq) | 6.6x |
| Two-hop traversal | 144 us | ~28 us (two_hop) | 5.1x |
| COUNT | 20.6 us | ~8.7 us (count_star) | 2.4x |
| Edge traversal | 20.9 us | ~24 us (single_hop) | 0.9x |
| CSR build overhead | 56.1 us | 0 (no CSR needed) | cacheable |

SPARQL overhead is 2-7x vs GQL, dominated by: (1) CSR build per query (cacheable, see DEFERRED.md), (2) spareval join execution, (3) term internalization/externalization. Parse overhead is eliminated on repeated queries via cache. Incoming CSR built lazily only when needed.

---

## How to Run

```bash
# Full profile (200 / 1K / 10K nodes) -- recommended
cargo bench -p selene-gql
cargo bench -p selene-graph
cargo bench -p selene-algorithms
cargo bench -p selene-ts
cargo bench -p selene-persist
cargo bench -p selene-wire
cargo bench -p selene-rdf
cargo bench -p selene-rdf --features sparql

# Stress profile (up to 250K nodes)
SELENE_BENCH_PROFILE=stress cargo bench -p selene-gql
SELENE_BENCH_PROFILE=stress cargo bench -p selene-graph
SELENE_BENCH_PROFILE=stress cargo bench -p selene-algorithms
SELENE_BENCH_PROFILE=stress cargo bench -p selene-persist
SELENE_BENCH_PROFILE=stress cargo bench -p selene-ts

# Quick profile (200 / 1K only)
SELENE_BENCH_PROFILE=quick cargo bench -p selene-gql
```

**Important:** Run benchmarks sequentially (one crate at a time) to avoid contention.
