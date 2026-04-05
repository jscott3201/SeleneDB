//! Retention task: export then delete expired Parquet files.
//!
//! Scan the time-series data directory for date-named subdirectories
//! and delete any older than the configured retention window. Export
//! via pipeline (if configured) before deletion.

use std::path::Path;

use crate::error::TsError;
use crate::export::{ExportMetadata, ExportPipeline};

/// Delete date directories older than `retention_days` from `ts_dir`.
///
/// When a pipeline is provided, each Parquet file in an expired directory
/// is exported before deletion. If any export fails, the directory is
/// kept for retry on the next cycle. Return the number of directories deleted.
pub async fn cleanup_expired_with_export(
    ts_dir: &Path,
    retention_days: u32,
    pipeline: Option<&ExportPipeline>,
) -> Result<usize, TsError> {
    if !ts_dir.exists() {
        return Ok(0);
    }

    let cutoff_days = current_day_number().saturating_sub(u64::from(retention_days) + 1);
    let mut deleted = 0;

    let mut expired_dirs: Vec<(String, std::path::PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(ts_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy().to_string();

        if let Some(day_num) = parse_date_to_day_number(&name_str)
            && day_num < cutoff_days
        {
            expired_dirs.push((name_str, entry.path()));
        }
    }

    for (date, dir_path) in expired_dirs {
        // Export files before deletion
        if let Some(pipeline) = pipeline
            && !pipeline.is_empty()
        {
            let mut export_ok = true;
            if let Ok(files) = std::fs::read_dir(&dir_path) {
                for file_entry in files.flatten() {
                    let file_path = file_entry.path();
                    if file_path.extension().is_some_and(|ext| ext == "parquet") {
                        let file_size = std::fs::metadata(&file_path).map(|m| m.len()).unwrap_or(0);
                        let metadata = ExportMetadata {
                            date: date.clone(),
                            row_count: 0, // unknown without reading
                            file_size,
                            source_path: file_path.clone(),
                        };
                        if let Err(e) = pipeline.export_file(&file_path, &metadata).await {
                            tracing::error!(
                                path = %file_path.display(),
                                error = %e,
                                "export failed, keeping directory for retry"
                            );
                            export_ok = false;
                            break;
                        }
                    }
                }
            }
            if !export_ok {
                continue; // skip deletion, retry next cycle
            }
        }

        tracing::info!(dir = %date, "deleting expired TS directory");
        std::fs::remove_dir_all(&dir_path)?;
        deleted += 1;
    }

    Ok(deleted)
}

/// Synchronous version without export (test-only).
#[cfg(test)]
pub fn cleanup_expired(ts_dir: &Path, retention_days: u32) -> Result<usize, TsError> {
    if !ts_dir.exists() {
        return Ok(0);
    }

    let cutoff_days = current_day_number().saturating_sub(u64::from(retention_days) + 1);
    let mut deleted = 0;

    for entry in std::fs::read_dir(ts_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        if let Some(day_num) = parse_date_to_day_number(&name_str)
            && day_num < cutoff_days
        {
            tracing::info!(dir = %name_str, "deleting expired TS directory");
            std::fs::remove_dir_all(entry.path())?;
            deleted += 1;
        }
    }

    Ok(deleted)
}

/// Parse `YYYY-MM-DD` to days since Unix epoch.
fn parse_date_to_day_number(date: &str) -> Option<u64> {
    let parts: Vec<&str> = date.split('-').collect();
    if parts.len() != 3 {
        return None;
    }

    let year: u64 = parts[0].parse().ok()?;
    let month: u64 = parts[1].parse().ok()?;
    let day: u64 = parts[2].parse().ok()?;

    if year < 1970 || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    Some(crate::calendar::ymd_to_days(year, month, day))
}

/// Current day number (days since Unix epoch).
fn current_day_number() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        / 86400
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_date() {
        let days = parse_date_to_day_number("2026-03-15").unwrap();
        assert_eq!(days, 20527);
    }

    #[test]
    fn parse_invalid_dates() {
        assert!(parse_date_to_day_number("not-a-date").is_none());
        assert!(parse_date_to_day_number("2026-13-01").is_none());
        assert!(parse_date_to_day_number("2026-00-01").is_none());
        assert!(parse_date_to_day_number("2026").is_none());
    }

    #[test]
    fn cleanup_deletes_old_directories() {
        let dir = tempfile::tempdir().unwrap();
        let ts_dir = dir.path().join("ts");

        // Create some date directories
        std::fs::create_dir_all(ts_dir.join("1970-01-01")).unwrap(); // very old
        std::fs::create_dir_all(ts_dir.join("1970-01-02")).unwrap(); // very old
        std::fs::create_dir_all(ts_dir.join("2099-12-31")).unwrap(); // future

        let deleted = cleanup_expired(&ts_dir, 7).unwrap();
        assert_eq!(deleted, 2); // both old dirs deleted
        assert!(ts_dir.join("2099-12-31").exists()); // future kept
    }

    #[test]
    fn cleanup_nonexistent_dir() {
        let deleted = cleanup_expired(Path::new("/nonexistent"), 7).unwrap();
        assert_eq!(deleted, 0);
    }

    #[test]
    fn cleanup_ignores_non_date_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let ts_dir = dir.path().join("ts");

        std::fs::create_dir_all(ts_dir.join("not-a-date")).unwrap();
        std::fs::create_dir_all(ts_dir.join("README")).unwrap();

        let deleted = cleanup_expired(&ts_dir, 7).unwrap();
        assert_eq!(deleted, 0); // nothing deleted
    }

    #[tokio::test]
    async fn cleanup_with_empty_pipeline() {
        let dir = tempfile::tempdir().unwrap();
        let ts_dir = dir.path().join("ts");
        std::fs::create_dir_all(ts_dir.join("1970-01-01")).unwrap();

        let pipeline = ExportPipeline::new();
        let deleted = cleanup_expired_with_export(&ts_dir, 7, Some(&pipeline))
            .await
            .unwrap();
        assert_eq!(deleted, 1);
    }

    #[tokio::test]
    async fn cleanup_with_export_pipeline() {
        use crate::export::ArrowIpcExporter;
        use crate::hot::TimeSample;
        use crate::hot::TsKey;
        use crate::parquet_writer::write_samples_to_parquet;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let ts_dir = dir.path().join("ts");
        let export_dir = dir.path().join("export");

        // Create an old date directory with a Parquet file
        let old_dir = ts_dir.join("1970-01-01");
        std::fs::create_dir_all(&old_dir).unwrap();
        let data = vec![(
            TsKey {
                node_id: selene_core::NodeId(1),
                property: selene_core::IStr::new("temp"),
            },
            vec![TimeSample {
                timestamp_nanos: 100,
                value: 72.0,
            }],
        )];
        write_samples_to_parquet(&old_dir.join("data.parquet"), &data, None).unwrap();

        let mut pipeline = ExportPipeline::new();
        pipeline.add_adapter(Arc::new(ArrowIpcExporter::new(&export_dir)));

        let deleted = cleanup_expired_with_export(&ts_dir, 7, Some(&pipeline))
            .await
            .unwrap();
        assert_eq!(deleted, 1);

        // Verify export happened
        assert!(export_dir.join("1970-01-01").exists());
        assert!(!old_dir.exists()); // original deleted
    }
}
