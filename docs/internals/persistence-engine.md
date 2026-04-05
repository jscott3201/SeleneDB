# Persistence Engine Internals

This document describes the internals of the `selene-persist` crate and the server-layer persistence coordination. The design follows the SQLite philosophy: a write-ahead log (WAL) for durability plus periodic snapshots for fast recovery.

**Key source files:**

- `crates/selene-persist/src/wal.rs` -- WAL format, append, read, truncate
- `crates/selene-persist/src/snapshot.rs` -- Snapshot format, write, read, TOC
- `crates/selene-persist/src/recovery.rs` -- Snapshot + WAL replay algorithm
- `crates/selene-persist/src/config.rs` -- SyncPolicy and PersistConfig
- `crates/selene-server/src/ops/mod.rs` -- `persist_or_die` function
- `crates/selene-server/src/wal_coalescer.rs` -- Group commit batching

## Design Overview

The persistence system has two components:

1. **WAL** -- An append-only log of graph mutations. Every write goes here first. If the process crashes, uncommitted mutations are lost but the graph can be reconstructed from the last snapshot plus WAL replay.
2. **Snapshots** -- Periodic full serializations of the in-memory graph. They establish a recovery checkpoint. After a successful snapshot, the WAL is truncated.

The write path is: mutation -> WAL append -> fsync (per policy) -> return to caller. The snapshot path is: serialize graph -> write to temp file -> atomic rename -> truncate WAL. Recovery is: load latest snapshot -> replay WAL entries after the snapshot sequence.

## WAL Format

The WAL is a single binary file (`wal.bin`) with a fixed header and variable-length entries.

### Header (16 bytes)

```
Offset  Size    Field
------  ------  ---------
0       4       Magic: "SWAL"
4       2       Version: 1 (u16 LE)
6       2       Padding (zero)
8       8       snapshot_seq: u64 LE (sequence at last truncation)
```

The `snapshot_seq` field records the WAL sequence number at which the last snapshot was taken. On recovery, entries with `seq <= snapshot_seq` are skipped.

### Entry Format (24 + N bytes)

```
Offset  Size    Field
------  ------  ---------
0       4       len: u32 LE (payload length after compression)
4       4       xxh3_lo: u32 LE (lower 32 bits of XXH3-64 hash of payload)
8       8       sequence: u64 LE (monotonically increasing)
16      8       timestamp: u64 LE (HLC NTP64 packed timestamp)
24      N       payload: zstd-compressed postcard Vec<Change>
```

Each entry is self-contained. The `sequence` field increases monotonically across appends. The `timestamp` is a Hybrid Logical Clock value in NTP64 format, providing causal ordering across federated nodes.

### Checksumming

Every entry's payload is checksummed with the lower 32 bits of XXH3-64 (`xxhash_rust::xxh3::xxh3_64`). On read, the checksum is recomputed and compared. A mismatch during `read_entries_after()` returns `PersistError::CrcMismatch`. A mismatch during `open_existing()` triggers truncation at the corrupt entry boundary -- the WAL is trimmed to the last valid entry, and the file is fsynced.

### Compression

Payloads 128 bytes or larger are compressed with zstd level 1. Below this threshold, compression overhead exceeds savings and the raw postcard bytes are stored directly. The `compress_entry` function returns `Cow::Borrowed` when compression is skipped, avoiding an unnecessary copy.

Decompression uses magic-byte detection: zstd frames start with `0x28 0xB5 0x2F 0xFD`. If the payload does not start with this magic, it is treated as raw postcard bytes (zero-copy via `Cow::Borrowed`). A 256 MiB decompression bomb guard prevents malicious payloads from exhausting memory.

### Maximum Entry Size

Entries claiming more than 256 MiB (`MAX_WAL_ENTRY`) are rejected on both read and open. This guard fires before any allocation, preventing a corrupt length field from causing an out-of-memory crash.

### Partial Write Handling

On `open_existing()`, if the last entry is truncated (header present but payload extends past EOF), the WAL is truncated to the start of the incomplete entry. This handles the case where a crash occurred mid-write. The single `write_all` call for each entry minimizes the partial-write window, but the truncation logic handles it regardless.

## Sync Policies

The `SyncPolicy` enum controls when `fsync` is called on the WAL file:

| Policy | Behavior | Durability | Latency |
|--------|----------|------------|---------|
| `EveryEntry` | `sync_data()` after every append | Strongest -- no data loss window | Highest -- one fsync per write |
| `Periodic { interval }` | `sync_data()` if `elapsed >= interval` | Small loss window (default 100ms) | Low -- amortized fsync |
| `OnSnapshot` | No fsync until snapshot | Loses all WAL since last snapshot on crash | Lowest -- no per-write fsync |

The default is `Periodic { interval: 100ms }`. This provides sub-second durability with low latency overhead. `EveryEntry` is appropriate for deployments where every mutation must survive a power loss. `OnSnapshot` is appropriate for ephemeral or reconstructible data.

## WAL Coalescer: Group Commit

The WAL coalescer (`wal_coalescer.rs`) sits between the ops layer and the raw WAL. It provides the `persist_or_die` guarantee while optionally batching concurrent writes.

### Synchronous Mode (default)

When `commit_delay_ms == 0`, `submit()` directly calls `wal_append_or_abort()`, which attempts the WAL append with 3 retries and linear backoff (10ms, 20ms). If all attempts fail, the process aborts. This is the `persist_or_die` guarantee: the caller's changes are either durable or the process is dead.

### Group Commit Mode

When `commit_delay_ms > 0`, `submit()` sends changes to a bounded channel. A background flush task drains the channel at the configured interval, collecting all changes into a single flat `Vec<Change>` and writing them as one WAL entry via a single `append()` call. Each individual caller's changeset also gets its own changelog entry for CDC subscribers.

The caller blocks on a `oneshot` channel until the batch is flushed, preserving the synchronous durability guarantee. This mode reduces fsync frequency when many concurrent writers are active.

### Timestamp Generation

The coalescer generates timestamps via Hybrid Logical Clock (HLC) when available, or falls back to wall-clock nanoseconds. The HLC timestamp is packed as a u64 in NTP64 format. All entries in a batch share a single timestamp.

## Snapshot Format

Snapshots are binary files (`snap-{sequence:012}.snap`) containing the complete graph state. The format uses a section-based layout with a Table of Contents (TOC) at the end.

### Header (32 bytes)

```
Offset  Size    Field
------  ------  ---------
0       4       Magic: "SSNP"
4       2       Version: 1 (u16 LE)
6       2       Flags: u16 LE (bit 0 = compressed)
8       4       section_count: u32 LE
12      4       Reserved (zero)
16      16      xxh3_128: u128 LE (hash of all sections + TOC)
```

### Sections

| Index | Content | Serialization |
|-------|---------|---------------|
| 0 | Metadata (next IDs, counts, changelog sequence) | postcard |
| 1 | Nodes (id, labels, properties, timestamps, version) | postcard |
| 2 | Edges (id, source, target, label, properties, timestamp) | postcard |
| 3 | Schemas (node schemas, edge schemas) | postcard |
| 4 | Triggers (name, event, label, condition, action) | postcard |
| 5+ | Extra sections (version store, ontology store, etc.) | pre-serialized bytes |

Each section is independently compressed with zstd level 1 when the section is 256 bytes or larger and compression reduces size. Sections below this threshold are stored raw.

### Table of Contents

The TOC is appended at the end of the file. Each entry is 16 bytes:

```
Offset  Size    Field
------  ------  ---------
0       8       section offset: u64 LE (from start of file)
8       8       section length: u64 LE
```

The TOC enables random access to individual sections without parsing the entire file. On read, the TOC is located by computing `file_length - (section_count * 16)`.

### Integrity

The XXH3-128 checksum in the header covers all section data plus the TOC bytes. On read, the hash is recomputed and compared before any deserialization occurs. A mismatch returns `PersistError::SnapshotRead` with the stored and computed hashes.

A maximum snapshot size guard (4 GiB) prevents reading corrupt files that claim absurd sizes.

### Atomic Write

Snapshots use atomic write for crash safety:

1. Serialize all sections and build the TOC in memory.
2. Write everything to a temp file (`snap-{seq}.tmp`).
3. `sync_data()` the temp file.
4. `fs::rename()` to the final path.
5. Optionally `sync_data()` the parent directory (configurable via `fsync_parent_dir`, default true).

If a crash occurs during step 2 or 3, the temp file is incomplete but the previous snapshot is untouched. Step 5 (parent dir fsync) ensures the directory entry is durable, which matters on power-loss-prone systems without battery-backed write caches. On ext4/APFS, rename is already atomic, so this step can be disabled to avoid the 50--200ms overhead on SD cards.

### Extra Sections

Extra sections carry feature-specific data using a tag-byte prefix for identification. The snapshot writer accepts `extra_sections: Vec<Vec<u8>>` as pre-serialized bytes. The reader returns them as raw decompressed byte vectors for the caller to interpret by tag. Currently defined tags: `0x01` = version store (temporal feature), `0x02` = ontology store (RDF feature). Their positional indices within the extra section list are not fixed.

## Recovery Algorithm

Recovery (`recovery.rs`) reconstructs the graph state from the data directory:

1. **Find latest snapshot**: Scan the `snapshots/` directory for files matching `snap-{sequence}.snap`. Select the highest sequence number.
2. **Deserialize snapshot**: Read the snapshot file, verify the XXH3-128 checksum, decompress sections, deserialize nodes/edges/schemas/triggers into a `RecoveryState` (HashMap-based for fast upsert).
3. **Replay WAL**: Open the WAL and read entries with `seq > snapshot_seq`. For each entry, apply the changes to the recovery state using upsert semantics.
4. **Build result**: Convert the recovery state into `RecoveredNode`/`RecoveredEdge` structures for the caller to load into a `SeleneGraph`.

### Upsert Semantics

WAL replay uses upsert (insert-or-update) semantics to handle the crash window between snapshot write and WAL truncation. If the process crashes after writing a snapshot but before truncating the WAL, recovery will see both the snapshot state and the WAL entries that were already captured in the snapshot. Upsert semantics ensure:

- `NodeCreated` for an existing node is a no-op (the `entry().or_insert_with()` pattern).
- `PropertySet` on an existing node updates the value in place.
- `NodeDeleted` removes the node and cascades to its edges.
- Version numbers are preserved from the snapshot, not inflated by replayed entries.

### Recovery Without Snapshot

If no snapshot exists, recovery starts from an empty state. All WAL entries are replayed from the beginning. This is the normal path for first-time startup and for deployments that have not yet triggered a snapshot.

## persist_or_die

The `persist_or_die` function in `ops/mod.rs` is the central write durability function. Every graph mutation in the ops layer flows through it:

```rust
pub(crate) fn persist_or_die(state: &ServerState, changes: &[Change]) {
    if state.is_replica { return; }
    state.wal_coalescer.submit(changes);
    // ... version store archival if temporal feature enabled
}
```

The function:

1. **Skips replicas** -- read-only replicas receive mutations via CDC replication, not local WAL writes.
2. **Submits to the WAL coalescer** -- which handles retry (3 attempts), batching, and `abort()` on failure.
3. **Archives old values** -- when the temporal feature's version store is available, old property values from `PropertySet` changes are archived for point-in-time queries.

The retry logic uses exponential backoff: 10ms after the first failure, 20ms after the second. If all three attempts fail, the process calls `std::process::abort()`. The rationale follows SQLite's philosophy: a database that cannot persist is worse than a database that is down, because it silently loses data.

## Snapshot Lifecycle

The snapshot lifecycle is driven by the background tasks in `selene-server`:

1. **Trigger**: A snapshot is triggered when either `snapshot_interval_secs` (default 300) has elapsed since the last snapshot, or `snapshot_max_wal_entries` (default 10,000) entries have accumulated in the WAL.
2. **Write**: The current graph state is serialized and written to `snapshots/snap-{sequence:012}.snap` using atomic rename.
3. **Truncate WAL**: After a successful snapshot write, the WAL is truncated (header rewritten with the new `snapshot_seq`, file truncated to header size).
4. **Prune old snapshots**: Only the most recent `max_snapshots` (default 3) snapshot files are kept. Older ones are deleted.

## Configuration Reference

All configuration lives in `PersistConfig` (TOML section `[persist]`):

| Parameter | Default | Description |
|-----------|---------|-------------|
| `sync_policy` | `periodic` (100ms) | WAL fsync strategy |
| `snapshot_interval_secs` | 300 | Seconds between snapshot writes |
| `snapshot_max_wal_entries` | 10,000 | WAL entry count triggering a snapshot |
| `max_snapshots` | 3 | Snapshot files retained on disk |
| `fsync_parent_dir` | true | Fsync parent directory after snapshot rename |

Sync policy TOML examples:

```toml
sync_policy = "every_entry"
sync_policy = "on_snapshot"
sync_policy = { periodic = { interval_ms = 200 } }
```
