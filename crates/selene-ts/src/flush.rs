//! Flush task: drain expired hot tier entries to Parquet files.
//!
//! Runs periodically (default every 15 minutes). Drains all samples older
//! than `hot_retention_hours` from the hot tier and writes them to Parquet.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use selene_core::now_nanos;

use crate::error::TsError;
use crate::hot::HotTier;
use crate::parquet_writer::write_samples_to_parquet;

/// Flush task that moves expired hot data to Parquet.
pub struct FlushTask {
    hot: Arc<HotTier>,
    ts_dir: PathBuf,
}

impl FlushTask {
    pub fn new(hot: Arc<HotTier>, ts_dir: impl Into<PathBuf>) -> Self {
        Self {
            hot,
            ts_dir: ts_dir.into(),
        }
    }

    /// Execute one flush cycle.
    ///
    /// Drain samples older than `hot_retention_hours` and write them
    /// to a timestamped Parquet file. Return the number of samples flushed.
    pub fn flush_once(&self) -> Result<usize, TsError> {
        let cutoff = now_nanos() - self.hot.retention_nanos();
        self.flush_with_cutoff(cutoff)
    }

    /// Flush with a specific cutoff timestamp (for testing).
    pub fn flush_before(&self, cutoff_nanos: i64) -> Result<usize, TsError> {
        self.flush_with_cutoff(cutoff_nanos)
    }

    /// Shared flush logic: drain samples before `cutoff`, write to Parquet,
    /// and re-insert on failure to avoid data loss.
    fn flush_with_cutoff(&self, cutoff: i64) -> Result<usize, TsError> {
        let drained = self.hot.drain_before(cutoff);

        if drained.is_empty() {
            return Ok(0);
        }

        // Generate date-partitioned filename
        let now = chrono_like_now();
        let day_dir = self.ts_dir.join(&now.date);
        let filename = format!("{}.parquet", now.time);
        let path = day_dir.join(filename);

        // Write to Parquet; re-insert on failure to avoid data loss
        match write_samples_to_parquet(&path, &drained, None) {
            Ok(count) => {
                tracing::info!(
                    samples = count,
                    path = %path.display(),
                    "flushed hot tier to parquet"
                );
                Ok(count)
            }
            Err(e) => {
                tracing::error!(
                    "parquet write failed, re-inserting {} batches: {e}",
                    drained.len()
                );
                // Re-insert via batch API to minimize lock contention
                let batch: Vec<_> = drained
                    .iter()
                    .flat_map(|(key, samples)| {
                        samples
                            .iter()
                            .map(move |s| (key.node_id, key.property.as_str(), *s))
                    })
                    .collect();
                self.hot.append_batch(&batch);
                Err(e)
            }
        }
    }

    /// Path to the time-series data directory.
    pub fn ts_dir(&self) -> &Path {
        &self.ts_dir
    }
}

struct SimpleDateTime {
    date: String, // YYYY-MM-DD
    time: String, // HHMMSS
}

fn chrono_like_now() -> SimpleDateTime {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let days = secs / 86400;
    let time_of_day = secs % 86400;

    // Civil-days calendar conversion
    let (year, month, day) = crate::calendar::days_to_ymd(days);
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    SimpleDateTime {
        date: format!("{year:04}-{month:02}-{day:02}"),
        time: format!("{hours:02}{minutes:02}{seconds:02}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TsConfig;
    use crate::hot::TimeSample;
    use selene_core::NodeId;

    fn sample(ts: i64, val: f64) -> TimeSample {
        TimeSample {
            timestamp_nanos: ts,
            value: val,
        }
    }

    #[test]
    fn flush_moves_expired_to_parquet() {
        let dir = tempfile::tempdir().unwrap();
        let hot = Arc::new(HotTier::new(TsConfig::default()));

        // Add samples with old timestamps (will be drained)
        hot.append(NodeId(1), "temp", sample(100, 72.0));
        hot.append(NodeId(1), "temp", sample(200, 73.0));
        // Add a sample far in the future (won't be drained)
        hot.append(NodeId(1), "temp", sample(i64::MAX / 2, 74.0));

        let flush = FlushTask::new(Arc::clone(&hot), dir.path().join("ts"));
        let count = flush.flush_before(1000).unwrap();

        assert_eq!(count, 2);
        assert_eq!(hot.sample_count(), 1); // only the future one remains
    }

    #[test]
    fn flush_nothing_to_drain() {
        let dir = tempfile::tempdir().unwrap();
        let hot = Arc::new(HotTier::new(TsConfig::default()));

        // Add only recent samples
        hot.append(NodeId(1), "temp", sample(now_nanos(), 72.0));

        let flush = FlushTask::new(Arc::clone(&hot), dir.path().join("ts"));
        let count = flush.flush_before(100).unwrap(); // cutoff before any samples
        assert_eq!(count, 0);
    }

    #[test]
    fn flush_creates_parquet_file() {
        let dir = tempfile::tempdir().unwrap();
        let hot = Arc::new(HotTier::new(TsConfig::default()));

        hot.append(NodeId(1), "temp", sample(100, 72.0));

        let ts_dir = dir.path().join("ts");
        let flush = FlushTask::new(Arc::clone(&hot), &ts_dir);
        flush.flush_before(now_nanos()).unwrap();

        // Should have created a date directory with a parquet file
        let entries: Vec<_> = std::fs::read_dir(&ts_dir)
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert_eq!(entries.len(), 1); // one date directory

        let date_dir = &entries[0].path();
        let parquet_files: Vec<_> = std::fs::read_dir(date_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .collect();
        assert_eq!(parquet_files.len(), 1);
    }
}
