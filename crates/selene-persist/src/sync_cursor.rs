//! Persistent sync cursor for resumable push/pull progress tracking.
//!
//! Each edge node maintains a [`SyncCursor`] that records how far it has
//! pushed local changes and pulled remote changes relative to an upstream
//! peer. The cursor is serialized with postcard and written atomically
//! (write-to-temp then rename) so a crash never leaves a half-written file.

use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::PersistError;

const CURSOR_FILENAME: &str = "sync_cursor.postcard";

/// Tracks push/pull progress for a single upstream peer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncCursor {
    /// Human-readable name of the peer.
    pub peer_name: String,
    /// Sequence number of the last change pushed to the upstream.
    pub last_pushed_seq: u64,
    /// Sequence number of the last change pulled from the upstream.
    pub last_pulled_seq: u64,
}

impl SyncCursor {
    /// Creates a new cursor with zero progress.
    pub fn new(peer_name: String) -> Self {
        Self {
            peer_name,
            last_pushed_seq: 0,
            last_pulled_seq: 0,
        }
    }

    /// Loads a cursor from `{data_dir}/sync_cursor.postcard`.
    ///
    /// Returns `Ok(None)` when the file does not exist.
    pub fn load(data_dir: &Path) -> Result<Option<Self>, PersistError> {
        let path = data_dir.join(CURSOR_FILENAME);
        match fs::read(&path) {
            Ok(bytes) => {
                let cursor: Self = postcard::from_bytes(&bytes)
                    .map_err(|e| PersistError::Serialization(e.to_string()))?;
                Ok(Some(cursor))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(PersistError::Io(e)),
        }
    }

    /// Saves the cursor atomically to `{data_dir}/sync_cursor.postcard`.
    ///
    /// Writes to a temporary file first, then renames into place so that a
    /// crash mid-write never corrupts the persisted cursor.
    pub fn save(&self, data_dir: &Path) -> Result<(), PersistError> {
        let path = data_dir.join(CURSOR_FILENAME);
        let tmp_path = data_dir.join("sync_cursor.postcard.tmp");

        let bytes =
            postcard::to_allocvec(self).map_err(|e| PersistError::Serialization(e.to_string()))?;

        fs::write(&tmp_path, &bytes)?;
        fs::rename(&tmp_path, &path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let cursor = SyncCursor {
            peer_name: "hub-north".into(),
            last_pushed_seq: 42,
            last_pulled_seq: 99,
        };

        cursor.save(dir.path()).unwrap();
        let loaded = SyncCursor::load(dir.path())
            .unwrap()
            .expect("cursor should exist");
        assert_eq!(cursor, loaded);
    }

    #[test]
    fn cursor_missing_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let result = SyncCursor::load(dir.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn cursor_atomic_overwrite() {
        let dir = tempfile::tempdir().unwrap();

        let mut cursor = SyncCursor::new("hub-south".into());
        cursor.last_pushed_seq = 10;
        cursor.last_pulled_seq = 20;
        cursor.save(dir.path()).unwrap();

        // Update and save again.
        cursor.last_pushed_seq = 50;
        cursor.last_pulled_seq = 75;
        cursor.save(dir.path()).unwrap();

        let loaded = SyncCursor::load(dir.path())
            .unwrap()
            .expect("cursor should exist");
        assert_eq!(loaded.last_pushed_seq, 50);
        assert_eq!(loaded.last_pulled_seq, 75);
        assert_eq!(loaded.peer_name, "hub-south");
    }
}
