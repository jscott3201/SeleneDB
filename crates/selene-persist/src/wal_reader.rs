//! Static read methods for WAL replay and sync.
//!
//! Separated from [`crate::wal::Wal`] because these are standalone
//! file-reading functions with no `&self` receiver. Keeping them here
//! reduces the size of the main WAL module and clarifies the
//! read-path vs. write-path boundary.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use selene_core::Origin;
use selene_core::changeset::Change;

use crate::error::PersistError;
use crate::wal::{ENTRY_HEADER_SIZE, HEADER_SIZE, MAX_WAL_ENTRY, V1_ENTRY_HEADER_SIZE, WAL_MAGIC};

/// Read all WAL entries after the given sequence.
///
/// Return `(sequence, timestamp, changes, origin)` for each entry.
/// The timestamp is a u64 HLC NTP64 value.
/// Partial entries at the end of the file are skipped with a warning
/// but not truncated; `open_existing` handles truncation.
///
/// Backward compatible: v1 files default to [`Origin::Local`].
#[allow(clippy::type_complexity)]
pub fn read_entries_after(
    path: &Path,
    after_seq: u64,
) -> Result<Vec<(u64, u64, Vec<Change>, Origin)>, PersistError> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();

    if file_len < HEADER_SIZE {
        return Ok(vec![]);
    }

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

    let mut entries = Vec::new();
    let mut pos = HEADER_SIZE;

    while pos + entry_hdr_size as u64 <= file_len {
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
        let timestamp = u64::from_le_bytes(entry_header[16..24].try_into().unwrap());
        let origin = if version >= 2 {
            Origin::from_byte(entry_header[24])
        } else {
            Origin::Local
        };

        if len > MAX_WAL_ENTRY {
            return Err(PersistError::WalCorrupted(format!(
                "WAL entry at offset {pos} claims {len} bytes (max {MAX_WAL_ENTRY})"
            )));
        }

        if pos + entry_hdr_size as u64 + len > file_len {
            tracing::warn!(
                offset = pos,
                "WAL entry truncated at end of file, stopping replay"
            );
            break;
        }

        // Read the full payload even for skipped entries (seq <= after_seq).
        // This is required for two reasons: (1) length-prefix framing needs
        // the read to advance the file cursor past this entry, and (2) we
        // validate the XXH3 checksum on every entry to detect corruption
        // early rather than silently skipping corrupted data.
        let mut payload = vec![0u8; len as usize];
        file.read_exact(&mut payload)?;

        let computed_xxh3 = if version >= 2 {
            crate::wal::xxh3_lo32_with_prefix(origin.to_byte(), &payload)
        } else {
            crate::wal::xxh3_lo32(&payload)
        };
        if computed_xxh3 != stored_xxh3 {
            // Treat CRC mismatch like a truncated tail entry: stop replay
            // with a warning instead of failing. Crash-induced corruption
            // nearly always affects only the last entry (partial write).
            tracing::warn!(
                offset = pos,
                expected = stored_xxh3,
                actual = computed_xxh3,
                "WAL entry CRC mismatch, stopping replay (likely crash artifact)"
            );
            break;
        }

        if seq > after_seq {
            let raw = crate::wal::decompress_entry(&payload)?;
            let changes: Vec<Change> = postcard::from_bytes(&raw)
                .map_err(|e| PersistError::Serialization(e.to_string()))?;
            entries.push((seq, timestamp, changes, origin));
        }

        pos += entry_hdr_size as u64 + len;
    }

    Ok(entries)
}

/// Read only Local-origin entries after the given sequence.
///
/// Skips decompression and deserialization for Replicated entries,
/// only reading their headers to advance past them. This is
/// significantly cheaper when the WAL contains many replicated entries.
///
/// Returns `(local_entries, max_scanned_seq)` where `max_scanned_seq`
/// is the highest sequence number seen across all entries (including
/// Replicated ones that were skipped). This allows the caller to
/// advance a sync cursor past replicated segments.
///
/// For v1 entries (no origin byte), all entries are treated as Local.
#[allow(clippy::type_complexity)]
pub fn read_local_entries_after(
    path: &Path,
    after_seq: u64,
) -> Result<(Vec<(u64, u64, Vec<Change>)>, u64), PersistError> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();

    if file_len < HEADER_SIZE {
        return Ok((vec![], after_seq));
    }

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

    let mut entries = Vec::new();
    let mut pos = HEADER_SIZE;
    let mut max_seq = after_seq;

    while pos + entry_hdr_size as u64 <= file_len {
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
        let timestamp = u64::from_le_bytes(entry_header[16..24].try_into().unwrap());
        let origin = if version >= 2 {
            Origin::from_byte(entry_header[24])
        } else {
            Origin::Local
        };

        if len > MAX_WAL_ENTRY {
            return Err(PersistError::WalCorrupted(format!(
                "WAL entry at offset {pos} claims {len} bytes (max {MAX_WAL_ENTRY})"
            )));
        }

        if pos + entry_hdr_size as u64 + len > file_len {
            tracing::warn!(
                offset = pos,
                "WAL entry truncated at end of file, stopping replay"
            );
            break;
        }

        if seq > max_seq {
            max_seq = seq;
        }

        // For Replicated entries, skip the payload entirely (no I/O,
        // decompression, or deserialization). This is the core
        // optimization over read_entries_after.
        if origin == Origin::Replicated {
            pos += entry_hdr_size as u64 + len;
            continue;
        }

        // Local entry: read, verify, decompress, deserialize.
        let mut payload = vec![0u8; len as usize];
        file.read_exact(&mut payload)?;

        let computed_xxh3 = if version >= 2 {
            crate::wal::xxh3_lo32_with_prefix(origin.to_byte(), &payload)
        } else {
            crate::wal::xxh3_lo32(&payload)
        };
        if computed_xxh3 != stored_xxh3 {
            tracing::warn!(
                offset = pos,
                expected = stored_xxh3,
                actual = computed_xxh3,
                "WAL entry CRC mismatch, stopping replay (likely crash artifact)"
            );
            break;
        }

        if seq > after_seq {
            let raw = crate::wal::decompress_entry(&payload)?;
            let changes: Vec<Change> = postcard::from_bytes(&raw)
                .map_err(|e| PersistError::Serialization(e.to_string()))?;
            entries.push((seq, timestamp, changes));
        }

        pos += entry_hdr_size as u64 + len;
    }

    Ok((entries, max_seq))
}

#[cfg(test)]
mod tests {
    use selene_core::changeset::Change;
    use selene_core::{IStr, NodeId, Origin};

    use crate::config::SyncPolicy;
    use crate::wal::Wal;

    use super::*;

    fn sample_changes() -> Vec<Change> {
        vec![
            Change::NodeCreated { node_id: NodeId(1) },
            Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("temp"),
                value: selene_core::Value::Float(72.5),
                old_value: None,
            },
        ]
    }

    const TEST_TS: u64 = 0x0001_ABCD_EF01_0003;

    #[test]
    fn read_entries_after_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        {
            let mut wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
            wal.append(&sample_changes(), TEST_TS, Origin::Local)
                .unwrap(); // seq 1
            wal.append(&sample_changes(), TEST_TS, Origin::Local)
                .unwrap(); // seq 2
            wal.append(&sample_changes(), TEST_TS, Origin::Local)
                .unwrap(); // seq 3
        }

        let entries = read_entries_after(&path, 2).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 3);
    }

    #[test]
    fn unsupported_version_rejected_on_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut header = Vec::with_capacity(16);
        header.extend_from_slice(WAL_MAGIC);
        header.extend_from_slice(&42u16.to_le_bytes());
        header.extend_from_slice(&[0u8; 2]);
        header.extend_from_slice(&0u64.to_le_bytes());
        std::fs::write(&path, &header).unwrap();

        let result = read_entries_after(&path, 0);
        assert!(
            matches!(result, Err(PersistError::UnsupportedWalVersion(42))),
            "expected UnsupportedWalVersion(42), got: {result:?}"
        );
    }

    #[test]
    fn oversized_wal_entry_rejected_on_read() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.bin");

        // Create a valid WAL with proper header
        {
            let _wal = Wal::open(&path, SyncPolicy::EveryEntry).unwrap();
        }

        // Manually craft a WAL entry header with an absurdly large length
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(HEADER_SIZE)).unwrap();

        let huge_len: u32 = 0xFFFF_FFFF; // ~4 GiB, well above 256 MiB limit
        let fake_xxh3: u32 = 0;
        let fake_seq: u64 = 1;
        let fake_ts: u64 = 0;
        let fake_origin: u8 = 0;

        file.write_all(&huge_len.to_le_bytes()).unwrap();
        file.write_all(&fake_xxh3.to_le_bytes()).unwrap();
        file.write_all(&fake_seq.to_le_bytes()).unwrap();
        file.write_all(&fake_ts.to_le_bytes()).unwrap();
        file.write_all(&[fake_origin]).unwrap();

        // read_entries_after should reject the oversized entry
        let result = read_entries_after(&path, 0);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, PersistError::WalCorrupted(ref msg) if msg.contains("claims")),
            "expected WalCorrupted error, got: {err}"
        );
    }

    #[test]
    fn read_local_entries_after_skips_replicated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut wal = Wal::open(&path, SyncPolicy::OnSnapshot).unwrap();

        // Write mixed entries: Local, Replicated, Replicated, Local.
        wal.append(
            &[Change::NodeCreated { node_id: NodeId(1) }],
            100,
            Origin::Local,
        )
        .unwrap(); // seq 1
        wal.append(
            &[Change::NodeCreated { node_id: NodeId(2) }],
            200,
            Origin::Replicated,
        )
        .unwrap(); // seq 2
        wal.append(
            &[Change::NodeCreated { node_id: NodeId(3) }],
            300,
            Origin::Replicated,
        )
        .unwrap(); // seq 3
        wal.append(
            &[Change::NodeCreated { node_id: NodeId(4) }],
            400,
            Origin::Local,
        )
        .unwrap(); // seq 4
        drop(wal);

        let (entries, max_seq) = read_local_entries_after(&path, 0).unwrap();
        assert_eq!(entries.len(), 2, "only Local entries returned");
        assert_eq!(entries[0].0, 1, "first local entry has seq 1");
        assert_eq!(entries[1].0, 4, "second local entry has seq 4");
        assert_eq!(max_seq, 4, "highest sequence seen across all entries");
    }

    #[test]
    fn read_local_entries_after_tracks_max_seq_from_replicated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut wal = Wal::open(&path, SyncPolicy::OnSnapshot).unwrap();

        // Local at seq 1, then two Replicated entries.
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
            Origin::Replicated,
        )
        .unwrap();
        drop(wal);

        // Read after seq 1: no Local entries remain, but max_seq = 3.
        let (entries, max_seq) = read_local_entries_after(&path, 1).unwrap();
        assert!(entries.is_empty(), "no local entries after seq 1");
        assert_eq!(max_seq, 3, "max_seq includes skipped Replicated entries");
    }
}
