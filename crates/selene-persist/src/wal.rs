//! Write-ahead log for graph mutations.
//!
//! The WAL stores serialized `Vec<Change>` entries -- the same changes
//! produced by `TrackedMutation::commit()`.  On recovery, entries are
//! replayed to reconstruct the graph state after the last snapshot.
//!
//! Format (v2 -- postcard + zstd + XXH3 + HLC timestamps + origin):
//! ```text
//! [Header: 16 bytes]
//!   magic: "SWAL" (4 bytes)
//!   version: u16 LE (2 = current, 1 = legacy)
//!   padding: 2 bytes
//!   snapshot_seq: u64 LE
//!
//! [Entry]*
//!   len: u32 LE (payload length, after compression)
//!   xxh3_lo: u32 LE (lower 32 bits of XXH3-64)
//!   sequence: u64 LE
//!   timestamp: u64 LE (HLC NTP64 packed timestamp)
//!   origin: u8 (0x00 = Local, 0x01 = Replicated) [v2 only]
//!   payload: [u8; len] (zstd-compressed postcard Vec<Change>)
//! ```

use std::borrow::Cow;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use selene_core::Origin;
use selene_core::changeset::Change;

use crate::config::SyncPolicy;
use crate::error::PersistError;

pub(crate) const WAL_MAGIC: &[u8; 4] = b"SWAL";
const WAL_VERSION: u16 = 2;
pub(crate) const HEADER_SIZE: u64 = 16;
/// Entry header for v2: len(4) + xxh3_lo(4) + seq(8) + timestamp(8) + origin(1).
pub(crate) const ENTRY_HEADER_SIZE: usize = 25;
/// Entry header for v1 (legacy): len(4) + xxh3_lo(4) + seq(8) + timestamp(8).
pub(crate) const V1_ENTRY_HEADER_SIZE: usize = 24;

/// Maximum allowed WAL entry payload size (256 MiB).
/// Rejects corrupt entries that claim absurdly large lengths before allocation.
pub(crate) const MAX_WAL_ENTRY: u64 = 256 * 1024 * 1024;

/// Minimum payload size before zstd compression is applied.
/// Below this threshold, compression overhead exceeds savings.
const COMPRESS_THRESHOLD: usize = 128;

/// Append-only write-ahead log.
pub struct Wal {
    file: File,
    path: PathBuf,
    next_sequence: u64,
    sync_policy: SyncPolicy,
    last_sync: Instant,
    entry_count: u64,
    /// Reusable buffer for entry serialization (avoids per-append allocation).
    write_buf: Vec<u8>,
}

impl Wal {
    /// Open an existing WAL or create a new one.
    pub fn open(path: &Path, sync_policy: SyncPolicy) -> Result<Self, PersistError> {
        let exists = path.exists() && path.metadata()?.len() > 0;

        if exists {
            Self::open_existing(path, sync_policy)
        } else {
            Self::create_new(path, sync_policy)
        }
    }

    fn create_new(path: &Path, sync_policy: SyncPolicy) -> Result<Self, PersistError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(path)?;

        file.write_all(WAL_MAGIC)?;
        file.write_all(&WAL_VERSION.to_le_bytes())?;
        file.write_all(&[0u8; 2])?; // padding
        file.write_all(&0u64.to_le_bytes())?; // snapshot_seq = 0
        file.sync_all()?;

        Ok(Self {
            file,
            path: path.to_path_buf(),
            next_sequence: 1,
            sync_policy,

            last_sync: Instant::now(),
            entry_count: 0,
            write_buf: Vec::new(),
        })
    }

    fn open_existing(path: &Path, sync_policy: SyncPolicy) -> Result<Self, PersistError> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;

        let mut header = [0u8; 16];
        file.read_exact(&mut header)?;

        if &header[0..4] != WAL_MAGIC {
            return Err(PersistError::InvalidWalMagic);
        }
        let version = u16::from_le_bytes([header[4], header[5]]);
        if version != 1 && version != 2 {
            return Err(PersistError::UnsupportedWalVersion(version));
        }
        let entry_hdr_size = if version >= 2 {
            ENTRY_HEADER_SIZE
        } else {
            V1_ENTRY_HEADER_SIZE
        };
        let snapshot_seq = u64::from_le_bytes(header[8..16].try_into().unwrap());

        // Scan entries to find highest sequence number
        let mut max_seq = snapshot_seq;
        let mut count = 0u64;
        let mut pos = HEADER_SIZE;
        let file_len = file.metadata()?.len();

        while pos + entry_hdr_size as u64 <= file_len {
            let entry_start_offset = pos;
            file.seek(SeekFrom::Start(pos))?;
            let mut entry_header = [0u8; ENTRY_HEADER_SIZE];
            if file
                .read_exact(&mut entry_header[..entry_hdr_size])
                .is_err()
            {
                break;
            }

            let len = u64::from(u32::from_le_bytes(entry_header[0..4].try_into().unwrap()));
            let stored_xxh3 = u32::from_le_bytes(entry_header[4..8].try_into().unwrap());
            let seq = u64::from_le_bytes(entry_header[8..16].try_into().unwrap());

            if len > MAX_WAL_ENTRY {
                return Err(PersistError::WalCorrupted(format!(
                    "WAL entry at offset {pos} claims {len} bytes (max {MAX_WAL_ENTRY})"
                )));
            }

            if pos + entry_hdr_size as u64 + len > file_len {
                // Truncated entry at end of file -- clean up the partial write
                tracing::warn!(pos = pos, "truncating partial WAL entry at end of file");
                file.set_len(pos)?;
                break;
            }

            // Read payload and verify checksum.
            // v2 includes the origin byte in the hash; v1 hashes payload only.
            let origin_byte = if version >= 2 { entry_header[24] } else { 0 };
            let mut payload = vec![0u8; len as usize];
            file.read_exact(&mut payload)?;

            let computed = if version >= 2 {
                xxh3_lo32_with_prefix(origin_byte, &payload)
            } else {
                xxh3_lo32(&payload)
            };
            if computed != stored_xxh3 {
                tracing::warn!(
                    "WAL entry at offset {} has invalid checksum (stored={:#x}, computed={:#x}), truncating",
                    entry_start_offset,
                    stored_xxh3,
                    computed
                );
                file.set_len(entry_start_offset)?;
                file.sync_data()?;
                break;
            }

            if seq > max_seq {
                max_seq = seq;
            }
            count += 1;
            pos += entry_hdr_size as u64 + len;
        }

        // Position at end for new appends
        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            file,
            path: path.to_path_buf(),
            next_sequence: max_seq + 1,
            sync_policy,

            last_sync: Instant::now(),
            entry_count: count,
            write_buf: Vec::new(),
        })
    }

    /// Append a set of changes to the WAL. Returns (sequence, timestamp).
    ///
    /// The caller provides the timestamp (typically HLC NTP64 from ServerState)
    /// and the [`Origin`] indicating whether this change is local or replicated.
    pub fn append(
        &mut self,
        changes: &[Change],
        timestamp: u64,
        origin: Origin,
    ) -> Result<(u64, u64), PersistError> {
        let raw = postcard::to_allocvec(changes)
            .map_err(|e| PersistError::Serialization(e.to_string()))?;

        let payload = compress_entry(&raw)?;
        let payload_len = u32::try_from(payload.len()).map_err(|_| {
            PersistError::Corruption("WAL entry payload exceeds u32::MAX bytes".into())
        })?;
        let seq = self.next_sequence;
        let origin_byte = origin.to_byte();
        let xxh3_lo = xxh3_lo32_with_prefix(origin_byte, &payload);

        // Single write_all to minimize partial-write risk on crash
        self.write_buf.clear();
        self.write_buf.reserve(ENTRY_HEADER_SIZE + payload.len());
        self.write_buf.extend_from_slice(&payload_len.to_le_bytes());
        self.write_buf.extend_from_slice(&xxh3_lo.to_le_bytes());
        self.write_buf.extend_from_slice(&seq.to_le_bytes());
        self.write_buf.extend_from_slice(&timestamp.to_le_bytes());
        self.write_buf.push(origin_byte);
        self.write_buf.extend_from_slice(&payload);
        self.file.write_all(&self.write_buf)?;

        self.next_sequence += 1;
        self.entry_count += 1;

        self.maybe_sync()?;

        Ok((seq, timestamp))
    }

    /// Append multiple change-sets in a single write syscall.
    ///
    /// Each change-set gets its own WAL entry (sequence number + hash),
    /// but all entries are written as one contiguous buffer.
    /// Return (last_sequence, timestamp); all entries share one timestamp.
    pub fn append_batch(
        &mut self,
        batch: &[Vec<Change>],
        timestamp: u64,
        origin: Origin,
    ) -> Result<(u64, u64), PersistError> {
        if batch.is_empty() {
            return Ok((self.next_sequence.saturating_sub(1), timestamp));
        }

        let mut combined = Vec::new();
        let mut last_seq = self.next_sequence;
        let origin_byte = origin.to_byte();

        for changes in batch {
            let raw = postcard::to_allocvec(changes)
                .map_err(|e| PersistError::Serialization(e.to_string()))?;

            let payload = compress_entry(&raw)?;
            let payload_len = u32::try_from(payload.len()).map_err(|_| {
                PersistError::Corruption("WAL entry payload exceeds u32::MAX bytes".into())
            })?;
            let seq = self.next_sequence;
            let xxh3_lo = xxh3_lo32_with_prefix(origin_byte, &payload);

            combined.extend_from_slice(&payload_len.to_le_bytes());
            combined.extend_from_slice(&xxh3_lo.to_le_bytes());
            combined.extend_from_slice(&seq.to_le_bytes());
            combined.extend_from_slice(&timestamp.to_le_bytes());
            combined.push(origin_byte);
            combined.extend_from_slice(&payload);

            self.next_sequence += 1;
            self.entry_count += 1;
            last_seq = seq;
        }

        self.file.write_all(&combined)?;
        self.maybe_sync()?;

        Ok((last_seq, timestamp))
    }

    /// Read all WAL entries after the given sequence.
    ///
    /// Delegates to [`crate::wal_reader::read_entries_after`].
    #[allow(clippy::type_complexity)]
    pub fn read_entries_after(
        path: &Path,
        after_seq: u64,
    ) -> Result<Vec<(u64, u64, Vec<Change>, Origin)>, PersistError> {
        crate::wal_reader::read_entries_after(path, after_seq)
    }

    /// Read only Local-origin entries after the given sequence.
    ///
    /// Delegates to [`crate::wal_reader::read_local_entries_after`].
    #[allow(clippy::type_complexity)]
    pub fn read_local_entries_after(
        path: &Path,
        after_seq: u64,
    ) -> Result<(Vec<(u64, u64, Vec<Change>)>, u64), PersistError> {
        crate::wal_reader::read_local_entries_after(path, after_seq)
    }

    /// Truncate the WAL, keeping only the header with an updated snapshot_seq.
    pub fn truncate(&mut self, snapshot_seq: u64) -> Result<(), PersistError> {
        // Write new header first (crash-safe: old entries are orphaned but harmless)
        self.file.seek(SeekFrom::Start(0))?;
        let mut header_buf = Vec::with_capacity(HEADER_SIZE as usize);
        header_buf.extend_from_slice(WAL_MAGIC);
        header_buf.extend_from_slice(&WAL_VERSION.to_le_bytes());
        header_buf.extend_from_slice(&[0u8; 2]); // padding
        header_buf.extend_from_slice(&snapshot_seq.to_le_bytes());
        self.file.write_all(&header_buf)?;
        // Now truncate entries
        self.file.set_len(HEADER_SIZE)?;
        self.file.sync_all()?;

        self.next_sequence = snapshot_seq + 1;
        self.entry_count = 0;

        self.last_sync = Instant::now();

        Ok(())
    }

    /// Force a sync to disk.
    pub fn sync(&mut self) -> Result<(), PersistError> {
        self.file.sync_data()?;

        self.last_sync = Instant::now();
        Ok(())
    }

    /// Number of entries written since last truncate.
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Current next sequence number.
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Path to the WAL file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn maybe_sync(&mut self) -> Result<(), PersistError> {
        match &self.sync_policy {
            SyncPolicy::EveryEntry => {
                self.file.sync_data()?;

                self.last_sync = Instant::now();
            }
            SyncPolicy::Periodic { interval } => {
                if self.last_sync.elapsed() >= *interval {
                    self.file.sync_data()?;

                    self.last_sync = Instant::now();
                }
            }
            SyncPolicy::OnSnapshot => {}
        }
        Ok(())
    }
}

/// Compute the lower 32 bits of XXH3-64 for checksum verification.
pub(crate) fn xxh3_lo32(data: &[u8]) -> u32 {
    xxhash_rust::xxh3::xxh3_64(data) as u32
}

/// Compute the lower 32 bits of XXH3-64 with a prefix byte mixed in.
///
/// Used by WAL v2 to include the origin byte in the checksum so that
/// origin tampering is detected during recovery.
pub(crate) fn xxh3_lo32_with_prefix(prefix: u8, data: &[u8]) -> u32 {
    let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
    hasher.update(&[prefix]);
    hasher.update(data);
    hasher.digest() as u32
}

/// Compress an entry payload with zstd level 1 if above threshold.
///
/// Returns `Cow::Borrowed` when compression is skipped (below threshold or
/// compressed output is not smaller), avoiding a needless copy.
fn compress_entry(raw: &[u8]) -> Result<Cow<'_, [u8]>, PersistError> {
    if raw.len() >= COMPRESS_THRESHOLD {
        let compressed = zstd::encode_all(raw, 1)
            .map_err(|e| PersistError::Serialization(format!("zstd compress: {e}")))?;
        if compressed.len() < raw.len() {
            return Ok(Cow::Owned(compressed));
        }
    }
    Ok(Cow::Borrowed(raw))
}

/// Decompress a WAL entry payload with decompression bomb protection (256 MiB).
///
/// Returns `Cow::Borrowed` for uncompressed payloads (zero-copy).
pub(crate) fn decompress_entry(payload: &[u8]) -> Result<std::borrow::Cow<'_, [u8]>, PersistError> {
    const MAX_DECOMPRESSED: usize = 256 * 1024 * 1024; // 256 MiB
    crate::compress::decompress_if_zstd(payload, MAX_DECOMPRESSED)
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{IStr, NodeId, Value};
    use smol_str::SmolStr;
    fn sample_changes() -> Vec<Change> {
        vec![
            Change::NodeCreated { node_id: NodeId(1) },
            Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("temp"),
                value: Value::Float(72.5),
                old_value: None,
            },
        ]
    }

    /// Test timestamp (arbitrary HLC-like value).
    const TEST_TS: u64 = 0x0001_ABCD_EF01_0003;

    #[test]
    fn create_new_wal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        let wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
        assert_eq!(wal.next_sequence(), 1);
        assert_eq!(wal.entry_count(), 0);
        assert!(path.exists());
        assert_eq!(path.metadata().unwrap().len(), HEADER_SIZE);
    }

    #[test]
    fn append_and_read_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        {
            let mut wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
            let (seq1, _ts1) = wal
                .append(&sample_changes(), TEST_TS, Origin::Local)
                .unwrap();
            let (seq2, _ts2) = wal
                .append(
                    &[Change::NodeDeleted {
                        node_id: NodeId(1),
                        labels: vec![IStr::new("sensor")],
                    }],
                    TEST_TS + 1,
                    Origin::Local,
                )
                .unwrap();
            assert_eq!(seq1, 1);
            assert_eq!(seq2, 2);
            assert_eq!(wal.entry_count(), 2);
        }

        // Read back
        let entries = Wal::read_entries_after(&path, 0).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, 1);
        assert_eq!(entries[0].1, TEST_TS);
        assert_eq!(entries[0].2.len(), 2); // NodeCreated + PropertySet
        assert_eq!(entries[1].0, 2);
        assert_eq!(entries[1].1, TEST_TS + 1);
        assert_eq!(entries[1].2.len(), 1); // NodeDeleted
    }

    #[test]
    fn reopen_existing_wal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        {
            let mut wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
            wal.append(&sample_changes(), TEST_TS, Origin::Local)
                .unwrap();
            wal.append(&sample_changes(), TEST_TS, Origin::Local)
                .unwrap();
        }

        // Reopen
        let mut wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
        assert_eq!(wal.next_sequence(), 3); // continues from 3
        assert_eq!(wal.entry_count(), 2);

        // Can append more
        let (seq, ts) = wal
            .append(&sample_changes(), TEST_TS + 99, Origin::Local)
            .unwrap();
        assert_eq!(seq, 3);
        assert_eq!(ts, TEST_TS + 99);
    }

    #[test]
    fn truncate_wal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        {
            let mut wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
            wal.append(&sample_changes(), TEST_TS, Origin::Local)
                .unwrap(); // seq 1
            wal.append(&sample_changes(), TEST_TS, Origin::Local)
                .unwrap(); // seq 2
            wal.truncate(2).unwrap();
        }

        // After truncate, WAL should be header-only
        assert_eq!(path.metadata().unwrap().len(), HEADER_SIZE);

        // Read back -- should be empty
        let entries = Wal::read_entries_after(&path, 0).unwrap();
        assert!(entries.is_empty());

        // Reopen -- sequence should continue from snapshot_seq + 1
        let wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
        assert_eq!(wal.next_sequence(), 3);
    }

    #[test]
    fn xxh3_verification() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        {
            let mut wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
            wal.append(&sample_changes(), TEST_TS, Origin::Local)
                .unwrap();
        }

        // Corrupt the payload
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(HEADER_SIZE + ENTRY_HEADER_SIZE as u64 + 1))
            .unwrap();
        file.write_all(&[0xFF]).unwrap();

        let result = Wal::read_entries_after(&path, 0);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            PersistError::CrcMismatch { .. }
        ));
    }

    #[test]
    fn invalid_magic_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        // Write invalid header
        let mut file = File::create(&path).unwrap();
        file.write_all(b"XXXX").unwrap();
        file.write_all(&[0u8; 12]).unwrap();

        let result = Wal::open(&path, SyncPolicy::EveryEntry);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(matches!(err, PersistError::InvalidWalMagic));
    }

    #[test]
    fn empty_changes_appendable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        let mut wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
        let (seq, ts) = wal.append(&[], TEST_TS, Origin::Local).unwrap();
        assert_eq!(seq, 1);
        assert_eq!(ts, TEST_TS);

        let entries = Wal::read_entries_after(&path, 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].2.is_empty());
    }

    #[test]
    fn periodic_sync_policy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        let mut wal = Wal::open(
            &path,
            SyncPolicy::Periodic {
                interval: std::time::Duration::from_secs(60),
            },
        )
        .unwrap();

        // Should not panic -- sync deferred
        wal.append(&sample_changes(), TEST_TS, Origin::Local)
            .unwrap();
        wal.append(&sample_changes(), TEST_TS, Origin::Local)
            .unwrap();
        assert_eq!(wal.entry_count(), 2);
    }

    #[test]
    fn many_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        {
            let mut wal = Wal::open(&path, SyncPolicy::OnSnapshot).unwrap();
            for i in 0..1000 {
                wal.append(&sample_changes(), TEST_TS + i, Origin::Local)
                    .unwrap();
            }
            wal.sync().unwrap();
        }

        let entries = Wal::read_entries_after(&path, 0).unwrap();
        assert_eq!(entries.len(), 1000);
        assert_eq!(entries[999].0, 1000);
    }

    #[test]
    fn truncate_then_append() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        let mut wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
        wal.append(&sample_changes(), TEST_TS, Origin::Local)
            .unwrap(); // seq 1
        wal.append(&sample_changes(), TEST_TS, Origin::Local)
            .unwrap(); // seq 2
        wal.truncate(2).unwrap();

        // Append after truncate
        let (seq, _ts) = wal
            .append(&sample_changes(), TEST_TS, Origin::Local)
            .unwrap();
        assert_eq!(seq, 3);

        let entries = Wal::read_entries_after(&path, 2).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 3);
    }

    #[test]
    fn append_batch_writes_multiple_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        {
            let mut wal = Wal::open(&path, SyncPolicy::OnSnapshot).unwrap();
            let batches = vec![
                sample_changes(),
                vec![Change::NodeCreated { node_id: NodeId(2) }],
                vec![Change::NodeDeleted {
                    node_id: NodeId(1),
                    labels: vec![IStr::new("sensor")],
                }],
            ];
            let (last_seq, ts) = wal.append_batch(&batches, TEST_TS, Origin::Local).unwrap();
            assert_eq!(last_seq, 3);
            assert_eq!(ts, TEST_TS);
            assert_eq!(wal.entry_count(), 3);
            assert_eq!(wal.next_sequence(), 4);
        }

        let entries = Wal::read_entries_after(&path, 0).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0, 1);
        assert_eq!(entries[0].2.len(), 2); // NodeCreated + PropertySet
        assert_eq!(entries[1].0, 2);
        assert_eq!(entries[2].0, 3);
    }

    #[test]
    fn append_batch_empty_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        let mut wal = Wal::open(&path, SyncPolicy::OnSnapshot).unwrap();
        let (seq, ts) = wal.append_batch(&[], TEST_TS, Origin::Local).unwrap();
        assert_eq!(seq, 0); // saturating_sub(1) on initial next_sequence=1
        assert_eq!(ts, TEST_TS);
        assert_eq!(wal.entry_count(), 0);
    }

    #[test]
    fn large_entry_gets_compressed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        // Create a large change set that will benefit from compression
        let mut changes = Vec::new();
        for i in 0..100 {
            changes.push(Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new(&format!("property_{i}")),
                value: Value::String(SmolStr::new(format!("value_{i}_with_some_padding_text"))),
                old_value: None,
            });
        }

        let mut wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
        wal.append(&changes, TEST_TS, Origin::Local).unwrap();

        // Read back and verify
        let entries = Wal::read_entries_after(&path, 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].2.len(), 100);
    }

    #[test]
    fn oversized_wal_entry_rejected_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        // Create a valid WAL with proper header
        {
            let _wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
        }

        // Manually craft a WAL entry header with length exceeding the limit.
        // The size check fires before the file-boundary check, so no actual
        // payload bytes are needed on disk.
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(HEADER_SIZE)).unwrap();

        // Use a length just over the limit (256 MiB + 1)
        let over_limit: u32 = (MAX_WAL_ENTRY + 1) as u32;
        let fake_xxh3: u32 = 0;
        let fake_seq: u64 = 1;
        let fake_ts: u64 = 0;
        let fake_origin: u8 = 0;

        file.write_all(&over_limit.to_le_bytes()).unwrap();
        file.write_all(&fake_xxh3.to_le_bytes()).unwrap();
        file.write_all(&fake_seq.to_le_bytes()).unwrap();
        file.write_all(&fake_ts.to_le_bytes()).unwrap();
        file.write_all(&[fake_origin]).unwrap();

        // open should reject the oversized entry during scan
        let result = Wal::open(&path, SyncPolicy::EveryEntry);
        match result {
            Err(PersistError::WalCorrupted(ref msg)) => {
                assert!(msg.contains("claims"), "unexpected message: {msg}");
            }
            Err(e) => panic!("expected WalCorrupted error, got: {e}"),
            Ok(_) => panic!("expected error, but open succeeded"),
        }
    }

    #[test]
    fn hlc_timestamp_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        let hlc_ts: u64 = 0x0001_ABCD_EF01_0003;

        {
            let mut wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
            let (seq, returned_ts) = wal
                .append(&sample_changes(), hlc_ts, Origin::Local)
                .unwrap();
            assert_eq!(seq, 1);
            assert_eq!(returned_ts, hlc_ts);
        }

        // Read back and verify HLC timestamp preserved
        let entries = Wal::read_entries_after(&path, 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, hlc_ts);
    }

    #[test]
    fn wal_v2_origin_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut wal = Wal::open(&path, SyncPolicy::OnSnapshot).unwrap();

        let (seq1, _) = wal.append(&sample_changes(), 100, Origin::Local).unwrap();
        let (seq2, _) = wal
            .append(&sample_changes(), 200, Origin::Replicated)
            .unwrap();
        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        drop(wal);

        let entries = Wal::read_entries_after(&path, 0).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].3, Origin::Local);
        assert_eq!(entries[1].3, Origin::Replicated);
    }

    // ── Corruption and recovery hardening tests ─────────────────────────

    #[test]
    fn bad_magic_bytes_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        // Write a header with wrong magic, correct length
        let mut header = Vec::with_capacity(16);
        header.extend_from_slice(b"BAAD");
        header.extend_from_slice(&WAL_VERSION.to_le_bytes());
        header.extend_from_slice(&[0u8; 2]); // padding
        header.extend_from_slice(&0u64.to_le_bytes()); // snapshot_seq
        std::fs::write(&path, &header).unwrap();

        let result = Wal::open(&path, SyncPolicy::EveryEntry);
        assert!(
            matches!(result, Err(PersistError::InvalidWalMagic)),
            "expected InvalidWalMagic"
        );
    }

    #[test]
    fn unsupported_version_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut header = Vec::with_capacity(16);
        header.extend_from_slice(WAL_MAGIC);
        header.extend_from_slice(&99u16.to_le_bytes()); // unsupported version
        header.extend_from_slice(&[0u8; 2]);
        header.extend_from_slice(&0u64.to_le_bytes());
        std::fs::write(&path, &header).unwrap();

        let result = Wal::open(&path, SyncPolicy::EveryEntry);
        assert!(
            matches!(result, Err(PersistError::UnsupportedWalVersion(99))),
            "expected UnsupportedWalVersion(99)"
        );
    }

    #[test]
    fn truncated_entry_at_eof_recovered_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        // Write one valid entry, then a full entry header claiming a payload
        // that extends past EOF.
        {
            let mut wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
            wal.append(&sample_changes(), TEST_TS, Origin::Local)
                .unwrap();
        }

        // Write a 25-byte entry header claiming 1000 bytes of payload,
        // but do not write the payload. This simulates a crash mid-write.
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&1000u32.to_le_bytes()).unwrap(); // len = 1000
        file.write_all(&0u32.to_le_bytes()).unwrap(); // xxh3
        file.write_all(&2u64.to_le_bytes()).unwrap(); // seq
        file.write_all(&(TEST_TS + 1).to_le_bytes()).unwrap(); // timestamp
        file.write_all(&[0u8]).unwrap(); // origin
        drop(file);

        let file_len_before = path.metadata().unwrap().len();

        // Reopen: should truncate the partial entry and recover
        let wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
        assert_eq!(
            wal.next_sequence(),
            2,
            "should continue after the one good entry"
        );
        assert_eq!(wal.entry_count(), 1);

        let file_len_after = path.metadata().unwrap().len();
        assert!(
            file_len_after < file_len_before,
            "file should shrink after truncating partial entry"
        );

        // Valid entry should still be readable
        let entries = Wal::read_entries_after(&path, 0).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn checksum_mismatch_truncates_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        // Write 3 valid entries
        {
            let mut wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
            wal.append(&sample_changes(), TEST_TS, Origin::Local)
                .unwrap();
            wal.append(&sample_changes(), TEST_TS + 1, Origin::Local)
                .unwrap();
            wal.append(&sample_changes(), TEST_TS + 2, Origin::Local)
                .unwrap();
        }

        // Corrupt the payload of entry 2 (flip a byte in the payload area).
        // Entry 1 starts at HEADER_SIZE (16). Its payload length is in bytes 0-3.
        let data = std::fs::read(&path).unwrap();
        let entry1_len = u32::from_le_bytes(data[16..20].try_into().unwrap()) as usize;
        let entry2_start = HEADER_SIZE as usize + ENTRY_HEADER_SIZE + entry1_len;
        let entry2_payload_offset = entry2_start + ENTRY_HEADER_SIZE;

        let mut corrupted = data.clone();
        if entry2_payload_offset < corrupted.len() {
            corrupted[entry2_payload_offset] ^= 0xFF;
        }
        std::fs::write(&path, &corrupted).unwrap();

        // Reopen: should truncate at entry 2, keeping only entry 1
        let wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
        assert_eq!(wal.entry_count(), 1, "only entry 1 should survive");
        assert_eq!(wal.next_sequence(), 2, "next sequence after entry 1");

        let entries = Wal::read_entries_after(&path, 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 1);
    }

    #[test]
    fn entry_after_bad_entry_discarded() {
        // Write 3 entries, corrupt entry 2. Only entry 1 should survive.
        // (Same corruption scenario but verifying entry 3 is also gone.)
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        {
            let mut wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
            for i in 0..3 {
                wal.append(
                    &[Change::NodeCreated {
                        node_id: NodeId(i + 1),
                    }],
                    TEST_TS + i,
                    Origin::Local,
                )
                .unwrap();
            }
        }

        // Corrupt entry 2's checksum field directly
        let data = std::fs::read(&path).unwrap();
        let entry1_len = u32::from_le_bytes(data[16..20].try_into().unwrap()) as usize;
        let entry2_start = HEADER_SIZE as usize + ENTRY_HEADER_SIZE + entry1_len;
        // Flip a byte in entry 2's xxh3 field (bytes 4-7 of entry header)
        let mut corrupted = data;
        corrupted[entry2_start + 4] ^= 0xFF;
        std::fs::write(&path, &corrupted).unwrap();

        let wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
        assert_eq!(wal.entry_count(), 1, "entries 2 and 3 should be truncated");

        // Can still append after the truncation
        let mut wal = wal;
        let (seq, _) = wal
            .append(&sample_changes(), TEST_TS + 100, Origin::Local)
            .unwrap();
        assert_eq!(seq, 2, "new entry gets sequence 2");
    }

    #[test]
    fn empty_wal_header_only_opens_successfully() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        // Create, then close immediately (header only, no entries)
        {
            let _wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
        }
        assert_eq!(path.metadata().unwrap().len(), HEADER_SIZE);

        // Reopen
        let wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
        assert_eq!(wal.next_sequence(), 1);
        assert_eq!(wal.entry_count(), 0);

        // read_entries_after also returns empty
        let entries = Wal::read_entries_after(&path, 0).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn origin_byte_preserved_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        {
            let mut wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
            wal.append(
                &[Change::NodeCreated { node_id: NodeId(1) }],
                100,
                Origin::Local,
            )
            .unwrap();
            wal.append(
                &[Change::NodeCreated { node_id: NodeId(2) }],
                200,
                Origin::Replicated,
            )
            .unwrap();
            wal.append(
                &[Change::NodeCreated { node_id: NodeId(3) }],
                300,
                Origin::Local,
            )
            .unwrap();
        }

        // Reopen and verify origin preserved
        let wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
        assert_eq!(wal.next_sequence(), 4);

        let entries = Wal::read_entries_after(&path, 0).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].3, Origin::Local);
        assert_eq!(entries[1].3, Origin::Replicated);
        assert_eq!(entries[2].3, Origin::Local);
    }

    #[test]
    fn small_payload_stored_uncompressed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        // A single NodeCreated change serializes to well under 128 bytes
        let changes = vec![Change::NodeCreated { node_id: NodeId(1) }];
        let raw = postcard::to_allocvec(&changes).unwrap();
        assert!(
            raw.len() < COMPRESS_THRESHOLD,
            "test prerequisite: raw payload ({} bytes) should be below threshold ({COMPRESS_THRESHOLD})",
            raw.len()
        );

        let mut wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
        wal.append(&changes, TEST_TS, Origin::Local).unwrap();

        // Read back the raw payload bytes from disk and verify no zstd magic
        let data = std::fs::read(&path).unwrap();
        let payload_start = HEADER_SIZE as usize + ENTRY_HEADER_SIZE;
        let payload_len = u32::from_le_bytes(data[16..20].try_into().unwrap()) as usize;
        let payload = &data[payload_start..payload_start + payload_len];

        // Zstd compressed data starts with magic bytes 0x28 0xB5 0x2F 0xFD
        let is_zstd = payload.len() >= 4
            && payload[0] == 0x28
            && payload[1] == 0xB5
            && payload[2] == 0x2F
            && payload[3] == 0xFD;
        assert!(!is_zstd, "small payload should not be zstd-compressed");

        // Round-trip still works
        let entries = Wal::read_entries_after(&path, 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].2.len(), 1);
    }

    #[test]
    fn truncated_payload_at_eof_recovered() {
        // Write a valid entry header but only part of the payload
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        {
            let mut wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
            wal.append(&sample_changes(), TEST_TS, Origin::Local)
                .unwrap();
        }

        // Append a valid-looking entry header claiming 100 bytes of payload,
        // but only write 10 bytes of payload.
        let origin_byte = Origin::Local.to_byte();
        let fake_payload = [0xAA; 10];
        let xxh3 = xxh3_lo32_with_prefix(origin_byte, &fake_payload);

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&100u32.to_le_bytes()).unwrap(); // claims 100 bytes
        file.write_all(&xxh3.to_le_bytes()).unwrap();
        file.write_all(&2u64.to_le_bytes()).unwrap(); // seq 2
        file.write_all(&(TEST_TS + 1).to_le_bytes()).unwrap();
        file.write_all(&[origin_byte]).unwrap();
        file.write_all(&fake_payload).unwrap(); // only 10 of 100 bytes
        drop(file);

        // open_existing should truncate the incomplete entry
        let wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
        assert_eq!(wal.entry_count(), 1);
        assert_eq!(wal.next_sequence(), 2);
    }

    #[test]
    fn all_value_variants_round_trip_through_wal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        let changes = vec![
            Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("int_val"),
                value: Value::Int(42),
                old_value: None,
            },
            Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("float_val"),
                value: Value::Float(3.14),
                old_value: None,
            },
            Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("string_val"),
                value: Value::String(SmolStr::new("hello")),
                old_value: None,
            },
            Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("bool_val"),
                value: Value::Bool(true),
                old_value: None,
            },
            Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("null_val"),
                value: Value::Null,
                old_value: None,
            },
            Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("list_val"),
                value: Value::List(std::sync::Arc::from(vec![
                    Value::Int(1),
                    Value::String(SmolStr::new("two")),
                ])),
                old_value: None,
            },
            Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("uint_val"),
                value: Value::UInt(u64::MAX),
                old_value: None,
            },
            Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("bytes_val"),
                value: Value::Bytes(std::sync::Arc::from(vec![0xDE, 0xAD, 0xBE, 0xEF])),
                old_value: None,
            },
            Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("timestamp_val"),
                value: Value::Timestamp(1_000_000_000),
                old_value: None,
            },
            Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("date_val"),
                value: Value::Date(19000), // ~2022
                old_value: None,
            },
            Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("duration_val"),
                value: Value::Duration(3_600_000_000_000), // 1 hour in nanos
                old_value: None,
            },
        ];

        let mut wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
        wal.append(&changes, TEST_TS, Origin::Local).unwrap();
        drop(wal);

        let entries = Wal::read_entries_after(&path, 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].2.len(), changes.len());

        // Verify each value variant round-tripped correctly
        for (original, recovered) in changes.iter().zip(entries[0].2.iter()) {
            if let (
                Change::PropertySet {
                    value: ov, key: ok, ..
                },
                Change::PropertySet {
                    value: rv, key: rk, ..
                },
            ) = (original, recovered)
            {
                assert_eq!(ok.as_str(), rk.as_str(), "key mismatch");
                assert_eq!(ov, rv, "value mismatch for key '{}'", ok.as_str());
            } else {
                panic!("expected PropertySet variants");
            }
        }
    }

    #[test]
    fn reopen_after_checksum_truncation_allows_new_appends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        // Write 2 entries, corrupt entry 2
        {
            let mut wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
            wal.append(&sample_changes(), TEST_TS, Origin::Local)
                .unwrap();
            wal.append(&sample_changes(), TEST_TS + 1, Origin::Local)
                .unwrap();
        }

        // Corrupt last byte of file (in entry 2's payload)
        let mut data = std::fs::read(&path).unwrap();
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();

        // Reopen truncates entry 2
        let mut wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
        assert_eq!(wal.entry_count(), 1);

        // Append a new entry after truncation
        let (seq, _) = wal
            .append(&sample_changes(), TEST_TS + 50, Origin::Local)
            .unwrap();
        assert_eq!(seq, 2, "new entry takes the sequence of the truncated one");

        // Read back: should have 2 valid entries
        let entries = Wal::read_entries_after(&path, 0).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].1, TEST_TS);
        assert_eq!(entries[1].1, TEST_TS + 50);
    }

    #[test]
    fn batch_append_with_mixed_origins() {
        // append_batch writes all entries with the same origin. Verify
        // that a second batch with a different origin coexists correctly.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        let mut wal = Wal::open(&path, SyncPolicy::OnSnapshot).unwrap();

        let local_batch = vec![
            vec![Change::NodeCreated { node_id: NodeId(1) }],
            vec![Change::NodeCreated { node_id: NodeId(2) }],
        ];
        wal.append_batch(&local_batch, 100, Origin::Local).unwrap();

        let replicated_batch = vec![vec![Change::NodeCreated { node_id: NodeId(3) }]];
        wal.append_batch(&replicated_batch, 200, Origin::Replicated)
            .unwrap();
        drop(wal);

        let entries = Wal::read_entries_after(&path, 0).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].3, Origin::Local);
        assert_eq!(entries[1].3, Origin::Local);
        assert_eq!(entries[2].3, Origin::Replicated);
    }

    #[test]
    fn zero_length_payload_entry() {
        // Verify that an empty changes vec produces a valid entry
        // with a very small (but nonzero) serialized payload.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        let mut wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
        wal.append(&[], TEST_TS, Origin::Local).unwrap();
        wal.append(&[], TEST_TS + 1, Origin::Replicated).unwrap();
        drop(wal);

        let entries = Wal::read_entries_after(&path, 0).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries[0].2.is_empty());
        assert!(entries[1].2.is_empty());
        assert_eq!(entries[0].3, Origin::Local);
        assert_eq!(entries[1].3, Origin::Replicated);
    }
}
