//! Server-side TsHistoryProvider — reads Parquet cold tier files.
//!
//! Scans `data_dir/ts/YYYY-MM-DD/` directories for Parquet files
//! and filters by entity_id, property, and time range.

use std::path::PathBuf;

use selene_core::NodeId;
use selene_gql::runtime::procedures::ts_history_provider::TsHistoryProvider;
use selene_ts::parquet_writer::read_samples_from_parquet;

/// Parquet-backed cold tier history provider.
pub struct ParquetTsHistoryProvider {
    ts_dir: PathBuf,
}

impl ParquetTsHistoryProvider {
    pub fn new(ts_dir: PathBuf) -> Self {
        Self { ts_dir }
    }

    /// Convert nanosecond timestamp to YYYY-MM-DD string for directory matching.
    fn nanos_to_date(nanos: i64) -> String {
        let secs = nanos / 1_000_000_000;
        let days = secs / 86400;
        let (y, m, d) = days_to_ymd(days);
        format!("{y:04}-{m:02}-{d:02}")
    }
}

impl TsHistoryProvider for ParquetTsHistoryProvider {
    fn query(
        &self,
        entity_id: u64,
        property: &str,
        start_nanos: i64,
        end_nanos: i64,
    ) -> Vec<(i64, f64)> {
        if !self.ts_dir.exists() {
            return vec![];
        }

        let start_date = Self::nanos_to_date(start_nanos);
        let end_date = Self::nanos_to_date(end_nanos);

        let mut results = Vec::new();

        // Scan date directories
        let Ok(entries) = std::fs::read_dir(&self.ts_dir) else {
            return vec![];
        };

        for entry in entries.flatten() {
            let name = entry.file_name();
            let date_str = name.to_string_lossy();

            // Skip directories outside the requested date range
            if date_str.as_ref() < start_date.as_str() || date_str.as_ref() > end_date.as_str() {
                continue;
            }

            let day_dir = entry.path();
            if !day_dir.is_dir() {
                continue;
            }

            // Read all Parquet files in this date directory
            let Ok(parquet_files) = std::fs::read_dir(&day_dir) else {
                continue;
            };

            for file_entry in parquet_files.flatten() {
                let file_path = file_entry.path();
                if file_path.extension().is_none_or(|ext| ext != "parquet") {
                    continue;
                }

                match read_samples_from_parquet(
                    &file_path,
                    Some(NodeId(entity_id)),
                    Some(property),
                    Some(start_nanos),
                    Some(end_nanos),
                ) {
                    Ok(samples) => {
                        for (_, _, sample) in samples {
                            results.push((sample.timestamp_nanos, sample.value));
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            path = %file_path.display(),
                            error = %e,
                            "failed to read Parquet file for ts.history"
                        );
                    }
                }
            }
        }

        // Sort by timestamp (files may be read in any order)
        results.sort_by_key(|&(ts, _)| ts);
        results
    }
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_ymd(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::IStr;
    use selene_ts::parquet_writer::write_samples_to_parquet;
    use selene_ts::{TimeSample, TsKey};

    #[test]
    fn query_parquet_cold_tier() {
        let dir = tempfile::tempdir().unwrap();
        let ts_dir = dir.path().join("ts");

        // Create a date directory with a Parquet file
        let day_dir = ts_dir.join("2026-03-25");
        std::fs::create_dir_all(&day_dir).unwrap();

        let data = vec![
            (
                TsKey {
                    node_id: NodeId(1),
                    property: IStr::new("temp"),
                },
                vec![
                    TimeSample {
                        timestamp_nanos: 1_000_000,
                        value: 72.0,
                    },
                    TimeSample {
                        timestamp_nanos: 2_000_000,
                        value: 73.0,
                    },
                ],
            ),
            (
                TsKey {
                    node_id: NodeId(1),
                    property: IStr::new("humidity"),
                },
                vec![TimeSample {
                    timestamp_nanos: 1_500_000,
                    value: 45.0,
                }],
            ),
            (
                TsKey {
                    node_id: NodeId(2),
                    property: IStr::new("temp"),
                },
                vec![TimeSample {
                    timestamp_nanos: 1_000_000,
                    value: 68.0,
                }],
            ),
        ];
        write_samples_to_parquet(&day_dir.join("data.parquet"), &data, None).unwrap();

        let provider = ParquetTsHistoryProvider::new(ts_dir);

        // Query entity 1, temp only
        let results = provider.query(1, "temp", 0, i64::MAX);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1, 72.0);
        assert_eq!(results[1].1, 73.0);

        // Query entity 1, humidity only
        let results = provider.query(1, "humidity", 0, i64::MAX);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, 45.0);

        // Query entity 2
        let results = provider.query(2, "temp", 0, i64::MAX);
        assert_eq!(results.len(), 1);

        // Query nonexistent entity
        let results = provider.query(99, "temp", 0, i64::MAX);
        assert!(results.is_empty());
    }

    #[test]
    fn empty_ts_dir() {
        let provider = ParquetTsHistoryProvider::new(PathBuf::from("/nonexistent"));
        let results = provider.query(1, "temp", 0, i64::MAX);
        assert!(results.is_empty());
    }

    #[test]
    fn nanos_to_date_epoch() {
        assert_eq!(ParquetTsHistoryProvider::nanos_to_date(0), "1970-01-01");
    }

    #[test]
    fn nanos_to_date_recent() {
        // 2026-03-25 = day 20537 from epoch
        let nanos = 20537i64 * 86400 * 1_000_000_000;
        assert_eq!(ParquetTsHistoryProvider::nanos_to_date(nanos), "2026-03-25");
    }
}
