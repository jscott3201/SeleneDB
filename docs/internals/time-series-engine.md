# Time-Series Engine Internals

This document describes the internals of the `selene-ts` crate, which implements a multi-tier time-series storage engine designed for IoT workloads running at the building edge.

The engine is organized as a four-stage pipeline: hot (in-memory compressed), warm (downsampled aggregates), cold (Parquet on disk), and cloud (object store offload). Each tier trades resolution for capacity, and data flows through the tiers automatically via background tasks.

**Key source files:**

- `crates/selene-ts/src/encoding.rs` -- Block encoding and decoding (Gorilla, RLE, Dictionary)
- `crates/selene-ts/src/hot.rs` -- Hot tier: sharded in-memory buffers with eviction
- `crates/selene-ts/src/warm.rs` -- Warm tier: minute and hourly aggregates with DDSketch
- `crates/selene-ts/src/parquet_writer.rs` -- Cold tier Parquet I/O with predicate pushdown
- `crates/selene-ts/src/flush.rs` -- Flush task: hot to cold transition
- `crates/selene-ts/src/compact.rs` -- Daily Parquet compaction
- `crates/selene-ts/src/retention.rs` -- Retention and export-before-delete
- `crates/selene-ts/src/cloud_store_exporter.rs` -- S3/GCS/Azure offload
- `crates/selene-ts/src/config.rs` -- All configuration knobs

## Architecture: Four-Tier Pipeline

```
Samples arrive
      |
      v
  [Hot Tier]  -- compressed in memory (Gorilla/RLE/Dictionary, default 24h)
      |
      |---> [Warm Tier]  -- minute/hourly aggregates (optional, in memory)
      |
      v  (flush task, every 15 min)
  [Cold Tier] -- Parquet files on disk (default 7 days)
      |
      v  (retention task)
  [Cloud]     -- S3/GCS/Azure via object_store (export-before-delete)
```

Each sample enters through `HotTier::append()`, which simultaneously feeds the warm tier (if configured). The flush task periodically drains expired hot data to Parquet. The retention task deletes old Parquet directories, optionally exporting through an `ExportPipeline` first. Cloud export uploads raw Parquet bytes with Hive-style partitioning.

## Value Encoding

The `encoding.rs` module implements multiple value encoding strategies for hot-tier blocks. The default is Gorilla (Pelkonen et al., 2015), a streaming compression scheme designed for regular-interval sensor data. RLE and Dictionary encodings are also available via the `ENCODING` DDL keyword on schema property definitions. Two separate columnar bit-streams per block handle timestamps and values independently.

### Bit-Level I/O

`BitWriter` and `BitReader` operate on `Vec<u64>` word buffers. `BitWriter` packs variable-width fields (1--64 bits) into 64-bit words, flushing each word to the vector when full. `BitReader` mirrors the process, tracking the current word index and remaining bits.

The write path handles the common case (field fits in the current word) and the split case (field spans two words) with bit-shift arithmetic. No heap allocation occurs per field -- only when the word buffer grows.

### Delta-of-Delta Timestamp Encoding

Timestamps are encoded as delta-of-deltas with variable-length prefix codes:

1. **First timestamp** -- stored as raw 64 bits (no prior reference).
2. **Second timestamp** -- stored as a delta-of-delta where the implicit previous delta is 0, so the DoD equals the raw delta itself.
3. **Subsequent timestamps** -- compute `dod = (current - previous) - previous_delta`, then encode:

| Prefix | Bits after prefix | Range | Total bits |
|--------|-------------------|-------|------------|
| `0` | 0 | dod == 0 (same interval) | 1 |
| `10` | 7 | [-63, 64] | 9 |
| `110` | 9 | [-255, 256] | 12 |
| `1110` | 12 | [-2047, 2048] | 16 |
| `1111` | 64 | full i64 range | 68 |

For regular 1 Hz sensors, every timestamp after the second one encodes as a single `0` bit (1 bit/timestamp). This is the most common case in IoT deployments.

### XOR Value Encoding

Values are encoded as IEEE 754 f64 bit patterns using XOR with leading/trailing zero optimization:

1. **First value** -- stored as raw 64 bits.
2. **Subsequent values** -- XOR with the previous value's bit pattern, then:

| Prefix | Meaning | Typical bits |
|--------|---------|-------------|
| `0` | Identical to previous (XOR == 0) | 1 |
| `10` | Reuse previous leading/trailing zero pattern | 2 + meaningful bits |
| `11` | New pattern: 5-bit leading zeros + 6-bit meaningful length + data | 13 + meaningful bits |

Constant-value sensors (binary status, unchanging setpoints) compress to 1 bit per sample. Slowly drifting analog sensors typically reuse the previous XOR pattern (prefix `10`), needing only a few bits for the changed portion. The implementation caps leading zeros at 5 bits (max 31), which handles the full-width XOR case where both MSB and LSB differ.

### Compression Results

For a 30-minute block at 1 Hz (1,800 samples, 28,800 raw bytes):

- **Regular interval, constant value**: ~1 bit/timestamp + ~1 bit/value. Compression ratio > 20x.
- **Regular interval, sine wave**: ~1 bit/timestamp + ~20 bits/value. Compression ratio ~2--8x.
- **Irregular interval, random values**: Worst case. Still lossless, but minimal compression.

## Hot Tier

The hot tier (`hot.rs`) stores recent time-series data in memory using compressed blocks plus an active collection window. It is the entry point for all sample ingestion.

### TsBuffer

Each `(NodeId, property_name)` pair maps to a `TsBuffer`:

```
TsBuffer
  sealed_blocks: VecDeque<TsBlock>        -- completed, compressed blocks (oldest first)
  active: ActiveWindow                     -- current window collecting raw samples
    window_start_nanos: i64
    samples: Vec<TimeSample>
  last_sample: Option<TimeSample>          -- most recent sample (for latest() lookups)
  last_stored_value: Option<f64>           -- for value deduplication
  compressed_bytes: usize                  -- memory tracking
  last_write_nanos: i64                    -- for eviction ordering
```

Samples accumulate in the active window as a raw `Vec<TimeSample>`. When a new sample falls into a different time window (determined by `gorilla_window_minutes`, default 30 minutes), the active window is sealed into a compressed `TsBlock` and pushed to the back of `sealed_blocks`.

### Sharded Architecture

The hot tier uses 16 shards (`node_id.0 & 0xF`) to minimize lock contention. Each shard holds a `ShardInner` containing:

- `buffers: FxHashMap<TsKey, TsBuffer>` -- the actual data
- `eviction_heap: BinaryHeap<Reverse<(i64, TsKey)>>` -- min-heap for idle eviction

Concurrent writes to different entities contend on separate locks. The shard count is a power of two for fast masking.

### Value Deduplication

Before storing a sample, the hot tier compares the new value's bit pattern (`value.to_bits()`) against the last stored value. When they match, the sample is skipped from storage but the warm tier still receives it (for accurate count/sum/avg aggregates). This optimization saves 50%+ storage for status/binary sensors that repeatedly report unchanged values.

### Out-of-Order Handling

Samples arriving out of order are handled within a configurable tolerance window (`out_of_order_tolerance_nanos`, default 5 seconds):

- **Within tolerance**: The sample is sorted-inserted into the active window using `partition_point` binary search.
- **Beyond tolerance**: The sample is dropped and the `out_of_order_dropped` counter increments.

### Range Queries

Range queries (`HotTier::range()`) combine results from sealed blocks and the active window:

1. **Sealed blocks**: Skip blocks entirely when `block.end_nanos < start` or `block.start_nanos > end`. For blocks that overlap the range, call `TsBlock::decode_range_partial()`, which advances the XOR state for all values before the range (decoding their bit patterns without materializing f64 results), then fully decodes and returns values within the range. The XOR state must be walked sequentially because each value depends on the previous one.
2. **Active window**: Use `partition_point` binary search to find the start index, then iterate until past the end.

### Memory Budget and Eviction

Memory usage is tracked via an `AtomicUsize` counter updated on every append. When usage exceeds `pressure_budget` (default 80% of `hot_memory_budget_mb`, default 256 MB), the eviction loop activates and continues until usage drops below `memory_budget` (the full budget) or all buffers are at `min_samples_per_buffer`:

1. Scan all shards (read locks) to find the buffer with the most samples.
2. Write-lock that shard and remove oldest sealed blocks.
3. If no sealed blocks remain and usage still exceeds the budget, trim the front of the active window.

The per-shard eviction heap uses `BinaryHeap<Reverse<(last_write_nanos, TsKey)>>` as a min-heap. O(1) when nothing is idle (peek and break). Lazy deletion handles stale entries -- when a key is checked for eviction, its current `last_write_nanos` is compared against the heap entry.

### Drain and Flush Integration

`HotTier::drain_before(cutoff_nanos)` drains all samples older than the cutoff for writing to Parquet. To bound per-shard lock hold time, draining is chunked: each shard's write lock is released and reacquired every `DRAIN_CHUNK_SIZE` (8) sealed blocks, letting concurrent writers make progress between chunks. Partial blocks (spanning the cutoff boundary) are decoded, split, and the remaining portion re-encoded.

On flush failure, drained samples are re-inserted via `append_batch()` to prevent data loss.

## Warm Tier

The warm tier (`warm.rs`) stores downsampled aggregates for trend and dashboard queries. It is optional (disabled by default) and runs in memory alongside the hot tier.

### Aggregate Windows

Each `(NodeId, property)` pair has a `WarmBuffer` containing:

- `aggregates: VecDeque<WarmAggregate>` -- completed window aggregates (oldest first)
- `current: WindowAccumulator` -- the in-progress window

A `WarmAggregate` stores per-window statistics: `min`, `max`, `sum`, `count`, `stddev` (population, via Welford's online algorithm), and `quantiles` (p50/p90/p95/p99 via DDSketch). The default window interval is 60 seconds.

When a sample's timestamp falls in a new window, the current accumulator is finalized and pushed to the aggregates deque. At 1-minute windows, 24 hours produces 1,440 aggregates vs. 86,400 raw samples -- a 60x reduction.

### DDSketch Quantiles

Each `WindowAccumulator` optionally holds a `DDSketch` (from the `sketches-ddsketch` crate) for streaming quantile estimation with bounded relative error. DDSketch allocation can be disabled via `ddsketch_enabled: false` in config, saving approximately 2 KB per accumulator on constrained edge devices. Quantile values are finalized alongside each window aggregate and require at least 2 samples.

Standard deviation is always available via Welford's algorithm, independent of the DDSketch setting.

### Hierarchical Warm Tiers

The warm tier supports an optional hourly level. When `hourly.enabled` is true, finalized minute-level aggregates are fed into the hourly tier using merge semantics: min-of-mins, max-of-maxes, sum-of-sums, sum-of-counts. DDSketch data is not propagated to merged tiers, so hourly quantiles are `None`.

The hourly tier uses the same 16-shard architecture and supports independent retention (default 30 days).

### Retention

`WarmTier::drain_expired(cutoff_nanos)` removes aggregates older than the retention window. Empty buffers are cleaned up in the same pass.

## Cold Tier: Parquet on Disk

The cold tier stores time-series data as Parquet files organized in date-partitioned directories.

### Parquet Schema and Writer

The Parquet schema is fixed: `(entity_id: UInt64, property: Utf8, timestamp: Int64, value: Float64)`. Files are sorted by `(entity_id, property, timestamp)` before writing.

Writer properties (`parquet_writer.rs`):

- **Compression**: zstd level 3
- **Statistics**: page-level (enables row-group-level predicate pushdown)
- **Bloom filters**: enabled on all columns (1% false positive rate, 1,000 expected distinct values)
- **Encoding**: dictionary encoding for `entity_id` and `property` (low cardinality in IoT); `DELTA_BINARY_PACKED` for timestamps (regular nanosecond intervals compress dramatically)
- **Sorting columns**: declared in file metadata for external query engine awareness

Writes use atomic rename: data goes to a `.parquet.tmp` file first, then `fs::rename` to the final path. This ensures readers never see partial files.

### Predicate Pushdown

The Parquet reader (`read_samples_from_parquet`) prunes row groups using:

1. **Column statistics**: min/max values from row-group metadata skip groups that cannot contain matching rows.
2. **Bloom filters**: for `entity_id` and `property` columns, the bloom filter definitively excludes row groups that do not contain the queried value.
3. **Arrow compute kernels**: within each batch, vectorized boolean masks (SIMD-accelerated on ARM NEON / x86 AVX2) replace row-by-row scalar filtering.

### Flush Task

The `FlushTask` (`flush.rs`) runs periodically (default every 15 minutes). Each cycle:

1. Compute the cutoff: `now - hot_retention_hours`.
2. Call `HotTier::drain_before(cutoff)` to extract expired samples.
3. Write to a date-partitioned Parquet file (`YYYY-MM-DD/HHMMSS.parquet`).
4. On write failure, re-insert all drained samples via `append_batch()`.

### Compaction

`compact_old_directories` (`compact.rs`) merges multiple flush files within a single date directory into one sorted Parquet file. Each 15-minute flush produces a small file; over 7 days this accumulates approximately 672 files. Compaction reduces filesystem overhead and improves query performance by producing larger, sorted row groups.

The process:

1. Skip directories younger than `compact_after_hours` (default 24) -- they may still receive flush writes.
2. Skip directories with only one Parquet file.
3. Read all files as Arrow `RecordBatch`es, concatenate, lexsort by `(entity_id, property, timestamp)`.
4. Write a compacted file with multi-row-group layout (10,000 rows per group).
5. Atomic swap: write to `compacted.parquet.tmp`, rename to `compacted.parquet`, delete originals.

All intermediate work uses Arrow columnar operations (concat, lexsort, take) to avoid scalar intermediate representations.

### Retention

The retention task (`retention.rs`) scans the time-series directory for date-named subdirectories. Directories older than `medium_retention_days` (default 7) are deleted. When an `ExportPipeline` is configured, each Parquet file in an expired directory is exported through the pipeline before deletion. If any export fails, the directory is kept for retry on the next cycle (export-before-delete guarantee).

## Cloud Offload

The `ObjectStoreExporter` (`cloud_store_exporter.rs`) uploads cold-tier Parquet files to S3, GCS, Azure Blob Storage, or MinIO via the `object_store` crate. It implements the `ExportAdapter` trait and plugs into the `ExportPipeline`.

### Hive Partitioning

Files are stored with Hive-style partitioning for compatibility with external query engines (Spark, DuckDB, Trino):

```
{prefix}/node={node_id}/date=2026-03-15/data.parquet
```

The `node_id` partition key is configurable (`cloud.node_id` in config, defaults to system hostname), enabling multi-node deployments to share a single bucket with node-level isolation.

### Export Pipeline

The `ExportPipeline` runs adapters in sequence. Each adapter receives Arrow `RecordBatch`es read from the source Parquet file (read once, shared across adapters). If all adapters succeed, the source file is eligible for deletion. If any adapter fails, the error is logged and the file is retained for retry.

The built-in `ArrowIpcExporter` writes Arrow IPC files to a local directory, serving as a reference implementation and enabling a Hub aggregator pattern (aggregation nodes read IPC files from a shared directory or network mount).

## Configuration Reference

All configuration lives in `TsConfig` (TOML section `[ts]`):

| Parameter | Default | Description |
|-----------|---------|-------------|
| `hot_retention_hours` | 24 | Hours of data kept in the hot tier |
| `medium_retention_days` | 7 | Days of Parquet data kept on disk |
| `flush_interval_minutes` | 15 | Minutes between flush cycles |
| `max_samples_per_buffer` | 86,400 | Hard cap per buffer (0 = unlimited) |
| `idle_eviction_hours` | 48 | Hours before idle buffer eviction (0 = disabled) |
| `hot_memory_budget_mb` | 256 | Total hot tier memory budget |
| `min_samples_per_buffer` | 60 | Minimum samples kept under memory pressure |
| `flush_pressure_threshold` | 0.8 | Memory fraction triggering early eviction |
| `out_of_order_tolerance_nanos` | 5,000,000,000 | Out-of-order tolerance (5 seconds) |
| `compact_after_hours` | 24 | Hours before a directory is eligible for compaction |
| `gorilla_window_minutes` | 30 | Block window size (applies to all encodings) |
| `warm_tier` | None | Warm tier config (see below) |
| `cloud.url` | None | Cloud storage URL (s3://, gs://, az://) |
| `cloud.node_id` | hostname | Node identifier for Hive partitioning |

Warm tier config (`[ts.warm_tier]`):

| Parameter | Default | Description |
|-----------|---------|-------------|
| `downsample_interval_secs` | 60 | Aggregate window interval |
| `retention_hours` | 24 | How long to keep warm data |
| `ddsketch_enabled` | true | Allocate DDSketch for quantiles |
| `hourly.enabled` | false | Enable hourly aggregation tier |
| `hourly.retention_days` | 30 | Hourly tier retention |
