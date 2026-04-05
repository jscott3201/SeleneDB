# Selene v0.2.0 Benchmarks

> Captured 2026-04-05 on Apple M5 (10-core, 16 GB RAM), macOS 26.4.
> Rust 1.94, `criterion` 0.8, full profile (scales: 200 / 1,000 / 10,000 nodes).
> **Sequential run** -- all suites executed one at a time, no parallel contention.
> Test data: reference building model (6 overlays, 17 labels, 9 edge types, all 14 Value types).

**8 bench targets across 7 crates, ~412 benchmarks across 30+ criterion groups.**
**Total wall time: ~19 minutes (full profile, including compilation).**

---

## Quick Reference (10K-node graph)

| Operation | Time |
|-----------|------|
| Parse simple query | 8.0 us |
| Plan cache hit | 20 ns |
| count(*) short-circuit | 3.1 ms |
| Single-hop expand | 3.2 ms |
| Two-hop expand | 3.2 ms |
| FILTER prop = val | 3.1 ms |
| ORDER BY + LIMIT | 3.5 ms |
| INSERT node | 55 us |
| avg() aggregation | 3.4 ms |
| Vector cosine (1K, 384-dim) | 62 us |
| TypedIndex lookup | 2.1 ns |
| CompositeIndex lookup | 85 ns |
| Label index lookup | 6.1 us |
| RDF export Turtle (10 bldgs) | 1.56 ms |
| SPARQL SELECT sensors (10 bldgs) | 64 us |
| SPARQL two-hop (10 bldgs) | 80 us |
| CSR build (outgoing-only, 10 bldgs) | 61 us |

---

## GQL Engine (selene-gql)

### Parsing (PEG grammar)

| Query | Median |
|-------|--------|
| simple (`MATCH (n) RETURN n`) | 8.0 us |
| labeled (`MATCH (s:sensor) RETURN s`) | 11.3 us |
| quantifier_shortcuts | 13.0 us |
| var_length | 20.8 us |
| filter | 24.1 us |
| unwind | 25.6 us |
| like_between | 29.2 us |
| different_edges | 30.9 us |
| complex (multi-hop + filter) | 43.8 us |
| type_ddl (`CREATE NODE TYPE`) | 2.0 us |

### Plan Cache

| Operation | Median |
|-----------|--------|
| cache_miss_parse | 11.2 us |
| **cache_hit** | **20 ns** |

### Label Scan + count(*)

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| all_sensors | 67.8 us | 322 us | 3.12 ms |
| all_nodes | 68.2 us | 328 us | 3.16 ms |

### Edge Expansion

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| single_hop | 85.0 us | 349 us | 3.25 ms |
| two_hop | 89.5 us | 351 us | 3.22 ms |

### Variable-Length Paths

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| depth_1_3 | 86.9 us | 394 us | 3.86 ms |
| trail_depth_1_5 | 108 us | 504 us | 5.45 ms |

### Filter

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| property_eq | 77.5 us | 332 us | 3.15 ms |
| property_gt | 77.7 us | 333 us | 3.14 ms |

### Sort

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| order_by_property | 93.4 us | 393 us | 3.73 ms |
| order_by_limit (TopK) | 89.1 us | 369 us | 3.47 ms |

### Aggregation

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| count_star | 67.9 us | 321 us | 3.11 ms |
| avg | 79.4 us | 353 us | 3.41 ms |
| group_by | 103 us | 405 us | 3.78 ms |

### DISTINCT and OFFSET

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| distinct_unit | 80.2 us | 364 us | 3.52 ms |
| offset_skip_50 | 91.5 us | 421 us | 4.29 ms |

### Predicates

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| like_prefix | 84.9 us | 359 us | 3.41 ms |
| like_contains | 85.7 us | 368 us | 3.54 ms |
| not_like | 85.1 us | 368 us | 3.50 ms |
| between_float | 82.1 us | 345 us | 3.28 ms |

### Advanced Pipeline

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| with_clause | 103 us | 364 us | 3.24 ms |
| having | 99.4 us | 359 us | 3.23 ms |
| unwind | 139 us | 492 us | 4.85 ms |
| nulls_first | 90.6 us | 368 us | 3.46 ms |

### End-to-End Workloads

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| simple_return | 78.9 us | 360 us | 3.48 ms |
| filter_sort_limit | 89.1 us | 346 us | 3.18 ms |
| two_hop_with_filter | 107 us | 368 us | 3.24 ms |
| inline_properties | 86.6 us | 361 us | 3.38 ms |

### Subqueries

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| count_subquery | 89.8 us | 360 us | 3.36 ms |
| different_edges | 118 us | 433 us | 3.93 ms |

### Phase 5 (Cycle Joins, RPQ)

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| cyclejoin_hvac | 112 us | 434 us | 3.21 ms |
| rpq_containment | 89.9 us | 411 us | 4.07 ms |
| rpq_alternation | 103 us | 473 us | 5.09 ms |
| collect_zones | 94.0 us | 370 us | 3.33 ms |

### Mutations

| Operation | Median |
|-----------|--------|
| insert_node | 54.8 us |
| insert_and_read | 54.7 us |
| set_property | 412 us |
| detach_delete | 80.5 us |

### Dictionary Encoding

| Operation | Median |
|-----------|--------|
| insert_dict_property | 54.9 us |
| insert_regular_property | 55.0 us |
| set_dict_property | 30.6 us |
| set_regular_property | 30.7 us |

### Transactions

| Operation | Median |
|-----------|--------|
| multi_statement_txn | 222 us |
| txn_rollback | 9.5 us |

### GQL Options

| Mode | Median |
|------|--------|
| strict_coercion | 355 us |
| default_coercion | 356 us |

### Optimizer Validation

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| range_indexed_gt | 77.5 us | 334 us | 3.15 ms |
| range_no_index_gt | 77.6 us | 333 us | 3.13 ms |
| inlist_indexed_5 | 92.2 us | 345 us | 3.13 ms |
| exists_semijoin | 102 us | 666 us | 33.2 ms |
| two_hop_interleaved_filter | 92.6 us | 350 us | 3.17 ms |
| multi_predicate_skewed | 87.0 us | 348 us | 3.20 ms |
| expand_target_filter | 92.1 us | 349 us | 3.17 ms |

### Factorized Execution (1K graph)

| Pattern | Flat | Factorized |
|---------|------|------------|
| two_hop | 349 us | 349 us |
| three_hop | 349 us | 348 us |
| three_hop_filter | 357 us | 355 us |

### Vector Search (384-dim, brute-force)

| Benchmark | 1K | 10K |
|-----------|-----|------|
| cosine_similarity | 62.0 us | 331 us |
| similar_nodes top-10 | 161 us | 1.46 ms |
| similar_nodes top-50 | 171 us | 1.54 ms |

| Math (per-pair) | Median |
|-----------------|--------|
| dot_product_384 | 119 ns |
| cosine_similarity_384 | 345 ns |

### Time-Series Procedures (via GQL CALL)

| Procedure | 200 | 1K | 10K |
|-----------|-----|-----|------|
| ts_latest | 68.4 us | 323 us | 3.11 ms |
| ts_range | 83.4 us | 337 us | 3.12 ms |
| ts_aggregate | 73.0 us | 328 us | 3.11 ms |
| ts_scoped_aggregate | 89.2 us | 345 us | 3.14 ms |
| ts_anomalies | 75.7 us | 331 us | 3.12 ms |
| ts_peer_anomalies | 74.2 us | 329 us | 3.12 ms |

---

## Graph Storage (selene-graph)

### Node CRUD

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| node_create | 102 us | 518 us | 5.20 ms |
| node_get_by_id | 1.2 us | 5.8 us | 62.0 us |
| node_update_property | 159 us | 576 us | 5.26 ms |

### Edge Creation + Node Removal

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| edge_create | 71.7 us | 402 us | 402 us |
| node_remove (100 nodes) | 51.8 us | 134 us | 956 us |

### Label Index

| Scale | Median |
|-------|--------|
| 200 | 152 ns |
| 1K | 617 ns |
| 10K | 6.1 us |

### BFS Traversal

| Depth | 200 | 1K | 10K |
|-------|-----|-----|------|
| d=1 | 269 ns | 965 ns | 8.2 us |
| d=3 | 4.8 us | 22.4 us | 291 us |
| d=10 | 13.7 us | 84.3 us | 891 us |
| d=50 | 13.7 us | 84.3 us | 891 us |

### Concurrent Reads (10 threads via ArcSwap)

| Scale | Median |
|-------|--------|
| 200 | 75.2 us |
| 1K | 75.1 us |
| 10K | 75.0 us |

### Mutations (graph-level)

| Batch size | Commit | Rollback |
|------------|--------|----------|
| 10 | 6.9 us | 1.6 us |
| 100 | 571 us | 14.3 us |
| 1000 | 3.07 ms | 129 us |

### TypedIndex

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| build | 981 ns | 4.22 us | 43.8 us |
| **lookup** | **2.1 ns** | **2.1 ns** | **2.1 ns** |
| iter_asc | 929 ps | 929 ps | 929 ps |

### CompositeTypedIndex

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| build | 7.1 us | 33.3 us | 344 us |
| **lookup** | **85 ns** | **85 ns** | **85 ns** |

### HNSW Index

| Benchmark | 1K |
|-----------|-----|
| build | 1.31 s |
| search top-10 | 140 us |

> 10K HNSW build is currently skipped in this benchmark run (~25s per iteration), rather than being profile-dependent. Search at 10K: 241 us.

---

## Graph Algorithms (selene-algorithms)

### Projection Build

| Type | 200 | 1K | 10K |
|------|-----|-----|------|
| full | 27.9 us | 175 us | 1.73 ms |
| containment_only | 28.1 us | 176 us | 1.79 ms |
| weighted | 28.7 us | 181 us | 1.83 ms |

### Structural

| Algorithm | 200 | 1K | 10K |
|-----------|-----|-----|------|
| wcc | 5.3 us | 25.4 us | 210 us |
| wcc_count | 3.1 us | 15.2 us | 118 us |
| scc | 11.9 us | 69.7 us | 710 us |
| scc_count | 11.4 us | 64.8 us | 624 us |
| topo_sort (acyclic) | 2.8 us | 11.9 us | 127 us |
| topo_sort (cyclic) | 2.5 us | 10.8 us | 117 us |
| articulation_points | 33.2 us | 173 us | 1.81 ms |
| bridges | 33.2 us | 173 us | 1.81 ms |
| validate | 3.7 us | 17.6 us | 197 us |
| containment_index | 40.1 us | 211 us | 2.12 ms |

### Pathfinding

| Algorithm | 200 | 1K | 10K |
|-----------|-----|-----|------|
| dijkstra | 459 ns | 3.9 us | 127 us |
| sssp | 3.9 us | 18.3 us | 2.43 ms |

> APSP skipped at 10K (O(n^3) runtime).

### Centrality

| Algorithm | 200 | 1K | 10K |
|-----------|-----|-----|------|
| pagerank (20 iter) | 13.1 us | 57.8 us | 573 us |

> Betweenness skipped at 10K (O(n^2) runtime).

### Community

| Algorithm | 200 | 1K | 10K |
|-----------|-----|-----|------|
| louvain | 44.2 us | 220 us | 2.91 ms |
| label_propagation | 16.8 us | 90.7 us | 1.43 ms |
| triangle_count | 9.5 us | 48.4 us | 573 us |

---

## Time-Series (selene-ts)

### Hot Tier

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| append | 6.4 us | 24.3 us | 215 us |
| append_batch | 6.9 us | 26.1 us | 240 us |
| range_query | 72 ns | 156 ns | 614 ns |
| evict_idle | 2.3 us | 14.4 us | 193 us |
| drain_before | 1.1 us | 8.6 us | 48.0 us |

### Flush + Parquet

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| flush_to_parquet | 359 us | 392 us | 936 us |
| parquet_write_size | 188 us | 227 us | 664 us |

### Gorilla Compression

Gorilla encoding benchmarks use a fixed 1,800-sample block (30 min at 1 Hz).

| Pattern | Encode | Decode |
|---------|--------|--------|
| regular | 7.0 us | 7.5 us |
| drifting | 7.9 us | 8.9 us |
| binary | 5.6 us | 7.0 us |

### RLE + Dictionary Encoding

| Operation | Constant | Regular |
|-----------|----------|---------|
| rle_encode | 3.8 us | 3.8 us |
| rle_decode | 3.6 us | 3.6 us |
| dict_encode | 6.6 us | -- |
| dict_decode | 3.8 us | -- |

### Warm Tier

| Benchmark | Median |
|-----------|--------|
| warm_record | see per-scale results |
| warm_range_query | ~180 ns |

---

## Persistence (selene-persist)

### WAL (Write-Ahead Log v2)

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| append (single) | 1.28 ms | 2.46 ms | 11.4 ms |
| append_batch | 713 us | 898 us | 1.99 ms |
| replay | 626 us | 1.37 ms | 9.20 ms |

### WAL Size (write + measure)

| Scale | Median |
|-------|--------|
| 200 | 5.35 ms |
| 1K | 6.73 ms |
| 10K | 15.4 ms |

### Snapshots

| Benchmark | 200 | 1K | 10K |
|-----------|-----|-----|------|
| write | 8.36 ms | 8.30 ms | 9.93 ms |
| write_with_edges | 8.34 ms | 8.49 ms | 10.6 ms |
| read (recovery) | 517 us | 934 us | 1.93 ms |

### Full Recovery (snapshot + WAL replay)

| Scale | Median |
|-------|--------|
| 200 | 804 us |
| 1K | 1.32 ms |
| 10K | 5.87 ms |

---

## Wire Protocol (selene-wire)

### Frame Encode

| Payload | Median |
|---------|--------|
| 64 B | 12 ns |
| 256 B | 20 ns |
| 1 KB | 26 ns |
| 4 KB | 57 ns |
| 16 KB | 270 ns |
| 64 KB | 888 ns |
| 256 KB | 3.5 us |

### Frame Decode Header

| Payload | Median |
|---------|--------|
| 64 B | 1.0 ns |
| 256 B | 1.1 ns |
| 1 KB | 1.1 ns |
| 4 KB | 1.1 ns |
| 16 KB | 1.1 ns |
| 64 KB | 1.1 ns |
| 256 KB | 1.1 ns |

### Serialization (NodeDto)

| Format | Serialize | Deserialize |
|--------|-----------|-------------|
| Postcard | 161 ns | 123 ns |
| JSON | 186 ns | 251 ns |

### Zstd Compression

| Payload | Compress | Decompress |
|---------|----------|------------|
| 64 B | 1.63 us | 98 ns |
| 256 B | 598 ns | 132 ns |
| 1 KB | 699 ns | 178 ns |
| 4 KB | 1.11 us | 343 ns |
| 16 KB | 1.80 us | 1.00 us |
| 64 KB | 3.00 us | 3.71 us |
| 256 KB | 9.78 us | 14.9 us |

### Datagram Codec

| Operation | Median |
|-----------|--------|
| encode_single | 12.4 ns |
| decode_single | 13.8 ns |
| encode_batch_64 | 770 ns |
| roundtrip_single | 26.6 ns |
| serialize_node_dto | 36.3 ns |

---

## RDF Interop (selene-rdf)

### Export (building scale: 1 / 5 / 10 buildings)

| Format | 1 bldg | 5 bldgs | 10 bldgs |
|--------|--------|---------|----------|
| Turtle | 165 us | 785 us | 1.56 ms |
| N-Triples | 121 us | 574 us | 1.14 ms |

### Import (Turtle roundtrip)

| Scale | Median |
|-------|--------|
| 1 bldg | 455 us |
| 5 bldgs | 2.14 ms |
| 10 bldgs | 4.26 ms |

### SPARQL Query (via QueryableDataset adapter)

All queries go through spargebra parse (cached) + spareval evaluate + SeleneDataset adapter.
CSR adjacency built per query with lazy incoming direction (outgoing-only for most patterns).

| Query | 1 bldg | 5 bldgs | 10 bldgs |
|-------|--------|---------|----------|
| SELECT all sensors (type pattern) | 12.2 us | 34.7 us | 63.9 us |
| SELECT sensors + unit (type + prop) | 19.9 us | 63.9 us | 119 us |
| Two-hop containment (building->floor->zone) | 25.6 us | 79.9 us | 80.0 us |
| Edge traversal (server monitors equip) | 9.1 us | 14.5 us | 20.8 us |
| COUNT sensors (aggregation) | 9.2 us | 14.6 us | 20.8 us |
| **CSR build (outgoing-only)** | **6.1 us** | **28.9 us** | **61.5 us** |

---

## Per-Crate Timing (full profile)

| Crate | Wall Time | Benchmark IDs |
|-------|-----------|---------------|
| selene-wire | 1m10s | 35 |
| selene-persist | 1m26s | 24 |
| selene-rdf | 1m20s | 27 |
| selene-ts | 1m35s | ~40 |
| selene-algorithms | 2m25s | ~55 |
| selene-graph | 3m41s | ~55 |
| selene-gql | 6m55s | 176 |
| **Total** | **~19m** | **~412** |

Includes compilation time. Measurement-only time is ~60% of wall time.

---

## How to Run

```bash
# Full profile (200 / 1K / 10K nodes, ~5 min per crate) -- recommended
cargo bench -p selene-wire --all-features
cargo bench -p selene-persist --all-features
cargo bench -p selene-rdf --all-features
cargo bench -p selene-ts --all-features
cargo bench -p selene-graph --all-features
cargo bench -p selene-algorithms --all-features
cargo bench -p selene-gql --all-features

# Quick profile (200 / 1K only, ~30s per crate)
SELENE_BENCH_PROFILE=quick cargo bench -p selene-gql --all-features

# Stress profile (up to 250K nodes, dedicated runs only)
SELENE_BENCH_PROFILE=stress SELENE_MAX_BINDINGS=500000 cargo bench -p selene-gql --all-features
SELENE_BENCH_PROFILE=stress cargo bench -p selene-graph --all-features
SELENE_BENCH_PROFILE=stress cargo bench -p selene-algorithms --all-features
SELENE_BENCH_PROFILE=stress cargo bench -p selene-persist --all-features
SELENE_BENCH_PROFILE=stress cargo bench -p selene-ts --all-features
```

**Important:** Run benchmarks sequentially (one crate at a time) to avoid contention.
