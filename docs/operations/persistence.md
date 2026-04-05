# Persistence

Selene uses a write-ahead log (WAL) combined with periodic binary snapshots to provide durable storage of the in-memory graph. The design follows SQLite's philosophy: prioritize data integrity over performance, and never silently lose data.

## Overview

Every mutation to the graph first appends to the WAL before becoming visible. Periodically, the server writes a full snapshot of the graph state and truncates the WAL. On startup, the recovery process finds the latest snapshot and replays any WAL entries written after it.

The data directory layout:

```
data_dir/
  wal.bin              # Write-ahead log
  snapshots/
    snap-000000000042.snap   # Binary snapshot (sequence 42)
    snap-000000000035.snap   # Previous snapshot
    snap-000000000028.snap   # Oldest retained snapshot
  ts/                  # Time-series Parquet files
```

## Write-Ahead Log

The WAL is an append-only binary file that records every graph mutation. Each entry contains the serialized changes from a single transaction.

### WAL Format (v1)

```
[Header: 16 bytes]
  magic: "SWAL" (4 bytes)
  version: u16 LE (1)
  padding: 2 bytes
  snapshot_seq: u64 LE

[Entry]*
  len: u32 LE (payload length, after compression)
  xxh3_lo: u32 LE (lower 32 bits of XXH3-64 checksum)
  sequence: u64 LE
  timestamp: u64 LE (HLC NTP64 packed timestamp)
  payload: [u8; len] (zstd-compressed postcard Vec<Change>)
```

Each entry stores a sequence number, a Hybrid Logical Clock timestamp, and the serialized list of graph changes (node/edge creates, property sets, label changes, deletes). Changes are serialized with postcard (a compact no-std binary format) and compressed with zstd level 1 when the payload exceeds 128 bytes.

The XXH3-64 checksum (lower 32 bits) protects against corruption. On open, the WAL scans all entries and verifies checksums. Entries with invalid checksums or partial writes at the end of the file are truncated during recovery.

Maximum allowed entry payload size is 256 MiB. Entries claiming larger sizes are rejected as corrupt.

### Sync Policies

The sync policy controls when the WAL flushes (fsyncs) writes to disk. Choose based on durability requirements and storage characteristics:

| Policy | TOML Value | Behavior | Durability | Latency |
|--------|-----------|----------|------------|---------|
| Every entry | `"every_entry"` | `fsync` after each WAL append | Strongest -- no data loss on crash | Highest -- one fsync per mutation |
| Periodic | `{ periodic = { interval_ms = 100 } }` | `fsync` when the interval elapses | Small loss window (up to the interval duration) | Moderate |
| On snapshot | `"on_snapshot"` | No `fsync` until the next snapshot | Largest loss window (up to `snapshot_interval_secs`) | Lowest |

The default policy is `periodic` with a 100ms interval.

Example configuration:

```toml
[persist]
# Strongest durability
sync_policy = "every_entry"

# Balanced (default) -- flush every 100ms
sync_policy = { periodic = { interval_ms = 100 } }

# Maximum throughput -- rely on snapshots
sync_policy = "on_snapshot"
```

### WAL Coalescer

When `wal_commit_delay_ms` is set to a value greater than 0 in the `[performance]` section, the WAL coalescer batches concurrent writes. Multiple mutations arriving within the delay window are combined into a single `append()` + `fsync` call, reducing the number of disk syncs under bursty workloads.

Each caller blocks until its changes are confirmed durable, preserving the `persist_or_die` guarantee. The coalescer uses a bounded channel (capacity 1024) and a background flush task.

Recommended settings:

| Profile | `wal_commit_delay_ms` | Rationale |
|---------|-----------------------|-----------|
| Edge | `0` (immediate) | Reliability over throughput |
| Gateway | `2` | Moderate batching |
| Cloud | `5` | Higher throughput |

## Snapshots

Snapshots capture the full graph state (nodes, edges, schemas, triggers, and extra sections) in a single binary file. They serve two purposes: faster startup (no need to replay the entire WAL) and WAL truncation (keeping the WAL file small).

### Snapshot Format (v1)

```
[Header: 32 bytes]
  magic: "SSNP" (4 bytes)
  version: u16 LE (1)
  flags: u16 LE (bit 0 = compressed)
  section_count: u32 LE
  reserved: u32 LE
  xxh3_128: u128 LE (hash of all sections + TOC)

[Section 0: Metadata]      postcard(SnapshotMetadata)
[Section 1: Nodes]         postcard(Vec<SnapshotNode>)
[Section 2: Edges]         postcard(Vec<SnapshotEdge>)
[Section 3: Schemas]       postcard(SnapshotSchemas)
[Section 4: Triggers]      postcard(Vec<TriggerDef>)
[Section 5+: Extra]        Tagged bytes (0x01 = version store, 0x02 = RDF ontology)

[TOC: section_count x 16 bytes]
  offset: u64 LE
  length: u64 LE
```

Sections larger than 256 bytes are individually compressed with zstd level 1. An XXH3-128 checksum covers all sections and the table of contents, detecting any corruption.

### Atomic Writes

Snapshots use a write-then-rename pattern to prevent corruption:

1. Serialize all sections to a buffer.
2. Write the buffer to a temporary file (`path.tmp`).
3. `fsync` the temporary file.
4. Rename `path.tmp` to `path.snap`.
5. Optionally `fsync` the parent directory (controlled by `fsync_parent_dir`).

If a crash occurs during write, only the temporary file is affected. The previous snapshot remains intact.

### Snapshot Triggers

Snapshots are taken under two conditions:

- **Periodic** -- every `snapshot_interval_secs` (default: 300 seconds / 5 minutes).
- **WAL threshold** -- when the WAL entry count reaches `snapshot_max_wal_entries` (default: 10,000). A background task checks the WAL size every 10 seconds.

After a snapshot, the WAL is truncated back to just the header (with the new snapshot sequence), and old snapshot files beyond `max_snapshots` (default: 3) are deleted.

### Shutdown Snapshot

On SIGINT or SIGTERM, the server takes a final snapshot before exiting if the WAL contains any entries. This minimizes recovery time on the next startup. Replicas skip this step since they have no local WAL.

## Recovery

On startup, the recovery process reconstructs the graph state:

1. **Find latest snapshot** -- scan the `snapshots/` directory for the file with the highest sequence number (filename format: `snap-{sequence:012}.snap`).
2. **Deserialize snapshot** -- load nodes, edges, schemas, triggers, and extra sections from the snapshot file. Verify the XXH3-128 checksum.
3. **Read WAL entries** -- find WAL entries with sequence numbers greater than the snapshot's sequence.
4. **Replay changes** -- apply each WAL entry's changes using upsert semantics. If a node or edge already exists (from the snapshot), the change is applied on top of it rather than creating a duplicate.
5. **Build indexes** -- reconstruct label bitmap indexes, property indexes, and composite indexes from the loaded data.

If no snapshot exists, recovery starts with an empty graph and replays the entire WAL. If neither exists, the server starts fresh.

### Crash Window Handling

A crash can occur between writing a snapshot and truncating the WAL. In this case, the WAL contains entries that are already captured in the snapshot. The recovery process handles this by skipping WAL entries with sequence numbers at or below the snapshot's sequence. Upsert semantics ensure that replaying an already-applied `NodeCreated` does not create a duplicate -- the existing node from the snapshot is preserved.

## persist_or_die

Every mutation in Selene goes through the `persist_or_die` function, which enforces a strict durability guarantee:

1. Submit changes to the WAL coalescer.
2. The coalescer attempts a WAL append.
3. On failure, attempt up to **3 total tries** with linear backoff (10ms, 20ms between attempts).
4. If all 3 attempts fail, **abort the process** (`std::process::abort()`).

The server will never acknowledge a mutation that failed to persist. This is a deliberate design choice: a crash with data loss is preferable to silent data loss followed by continued operation on a divergent state.

After the WAL append succeeds, the changes are also written to the in-memory changelog buffer and broadcast to subscribers (search indexer, vector store, stats collector, WebSocket clients).

When the temporal feature is active, `persist_or_die` also archives old property values into the version store for point-in-time queries.

## Backup Strategy

Selene's data directory is self-contained. To back up a running instance:

1. **Copy the data directory** -- the snapshot files are atomic (write-then-rename), so a file-system-level copy of the `snapshots/` directory yields a consistent point-in-time.
2. **Use snapshot files directly** -- each snapshot is a complete graph state. Copy the latest `.snap` file for the most recent consistent state.

The WAL file is append-only but may be partially written at the tail. For backup purposes, snapshots alone are sufficient since they capture the full state at the time they were written.

## Configuration Reference

### [persist] Section

| Field | Default | Description |
|-------|---------|-------------|
| `sync_policy` | `{ periodic = { interval_ms = 100 } }` | WAL sync strategy: `"every_entry"`, `{ periodic_ms = N }`, or `"on_snapshot"` |
| `snapshot_interval_secs` | `300` | Seconds between periodic snapshots |
| `snapshot_max_wal_entries` | `10000` | WAL entry count threshold for forcing a snapshot |
| `max_snapshots` | `3` | Maximum snapshot files to retain (oldest deleted first) |
| `fsync_parent_dir` | `true` | Fsync parent directory after snapshot rename. Adds 50-200ms on slow storage (SD cards). Safe to disable on ext4/APFS. |

### [performance] Section (Persistence-Related)

| Field | Default | Description |
|-------|---------|-------------|
| `wal_commit_delay_ms` | `0` | Group commit delay. 0 = immediate flush. Values > 0 enable WAL coalescing. |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `SELENE_DATA_DIR` | Override the data directory path |

### Example Configuration

```toml
data_dir = "/var/lib/selene"

[persist]
sync_policy = { periodic_ms = 50 }
snapshot_interval_secs = 180
snapshot_max_wal_entries = 5000
max_snapshots = 5
fsync_parent_dir = true

[performance]
wal_commit_delay_ms = 2
```
