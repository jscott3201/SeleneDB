//! Persistence configuration.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

/// WAL sync-to-disk strategy.
///
/// TOML: `"every_entry"`, `"on_snapshot"`, or `{ periodic_ms = 100 }`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncPolicy {
    /// Fsync after every WAL entry. Strongest durability, highest latency.
    EveryEntry,
    /// Fsync periodically. Small loss window on crash.
    #[serde(rename = "periodic")]
    Periodic {
        #[serde(
            default = "default_sync_interval_ms",
            rename = "interval_ms",
            deserialize_with = "deserialize_duration_ms"
        )]
        interval: Duration,
    },
    /// Fsync only on snapshot. Fastest, but loses all WAL since last snapshot on crash.
    OnSnapshot,
}

fn default_sync_interval_ms() -> Duration {
    Duration::from_millis(100)
}

fn deserialize_duration_ms<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
    let ms = u64::deserialize(d)?;
    Ok(Duration::from_millis(ms))
}

impl Default for SyncPolicy {
    fn default() -> Self {
        Self::Periodic {
            interval: Duration::from_millis(100),
        }
    }
}

/// Configuration for the persistence layer.
///
/// When deserialized from TOML, `data_dir` defaults to an empty path and must
/// be set via [`PersistConfig::fixup_data_dir`] after loading.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PersistConfig {
    /// Directory for WAL, snapshots, and TS Parquet files.
    /// Set via [`PersistConfig::fixup_data_dir`] after deserialization.
    #[serde(skip)]
    pub data_dir: PathBuf,
    /// WAL sync policy.
    pub sync_policy: SyncPolicy,
    /// Seconds between snapshot writes (default: 300 = 5 minutes).
    pub snapshot_interval_secs: u64,
    /// Max WAL entries before forcing a snapshot (default: 10,000).
    pub snapshot_max_wal_entries: u64,
    /// Maximum number of snapshot files to keep (default: 3).
    pub max_snapshots: usize,
    /// Fsync parent directory after snapshot rename (default: true).
    ///
    /// Ensures the directory entry is persisted after rename. Adds 50-200 ms
    /// on slow storage (SD cards). Safe to disable on ext4/APFS where rename
    /// is already atomic, but keep enabled on power-loss-prone systems
    /// without a battery-backed write cache.
    pub fsync_parent_dir: bool,
}

impl Default for PersistConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::new(),
            sync_policy: SyncPolicy::default(),
            snapshot_interval_secs: 300,
            snapshot_max_wal_entries: 10_000,
            max_snapshots: 3,
            fsync_parent_dir: true,
        }
    }
}

impl PersistConfig {
    /// Set the data directory after deserialization.
    pub fn fixup_data_dir(&mut self, data_dir: &Path) {
        self.data_dir = data_dir.to_path_buf();
    }
}

impl PersistConfig {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            sync_policy: SyncPolicy::default(),
            snapshot_interval_secs: 300,
            snapshot_max_wal_entries: 10_000,
            max_snapshots: 3,
            fsync_parent_dir: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_sync_policy() {
        let policy = SyncPolicy::default();
        assert!(
            matches!(policy, SyncPolicy::Periodic { interval } if interval == Duration::from_millis(100))
        );
    }

    #[test]
    fn config_construction() {
        let cfg = PersistConfig::new("/tmp/selene");
        assert_eq!(cfg.snapshot_interval_secs, 300);
        assert_eq!(cfg.snapshot_max_wal_entries, 10_000);
    }
}
