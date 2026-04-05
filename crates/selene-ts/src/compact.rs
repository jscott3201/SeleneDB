//! Daily Parquet compaction: merge multiple flush files into one per day.
//!
//! Each 15-minute flush produces a small Parquet file (672 files over 7 days).
//! Compaction merges a day's files into a single sorted file, reducing
//! filesystem overhead and improving cold tier query performance.

use std::path::Path;

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::compute::{SortColumn, concat_batches, lexsort_to_indices, take_record_batch};

use crate::error::TsError;
use crate::parquet_writer::write_samples_to_parquet;

/// Compact date directories older than `min_age_hours` with multiple Parquet files.
///
/// For each qualifying directory: read all Parquet files, write a single
/// compacted file with multi-row-group layout, then atomic swap (`.tmp` +
/// rename + delete originals). Return the number of directories compacted.
pub fn compact_old_directories(ts_dir: &Path, min_age_hours: u32) -> Result<usize, TsError> {
    if !ts_dir.exists() {
        return Ok(0);
    }

    let cutoff_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .saturating_sub(u64::from(min_age_hours) * 3600);

    let mut compacted = 0;

    let entries: Vec<_> = std::fs::read_dir(ts_dir)?.filter_map(|e| e.ok()).collect();

    for entry in entries {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        // Skip directories younger than min_age_hours
        let Ok(meta) = std::fs::metadata(&path) else {
            continue;
        };
        let dir_modified = meta
            .modified()
            .unwrap_or(std::time::UNIX_EPOCH)
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if dir_modified > cutoff_secs {
            continue;
        }

        let parquet_files: Vec<_> = std::fs::read_dir(&path)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "parquet"))
            .collect();

        if parquet_files.len() <= 1 {
            continue;
        }

        match compact_directory(&path, &parquet_files) {
            Ok(rows) => {
                tracing::info!(
                    dir = %path.display(),
                    files = parquet_files.len(),
                    rows,
                    "compacted TS directory"
                );
                compacted += 1;
            }
            Err(e) => {
                tracing::error!(
                    dir = %path.display(),
                    error = %e,
                    "TS compaction failed"
                );
            }
        }
    }

    Ok(compacted)
}

/// Compact all Parquet files in a directory into a single file.
///
/// Uses Arrow operations (concat + lexsort + take) to merge and sort batches
/// in columnar form, avoiding a scalar intermediate representation.
fn compact_directory(dir: &Path, files: &[std::path::PathBuf]) -> Result<usize, TsError> {
    let schema = Arc::new(crate::parquet_writer::ts_schema());

    // Read all files as RecordBatches (full projection for re-write)
    let mut all_batches = Vec::new();
    for file in files {
        let batches = crate::parquet_writer::read_parquet_batches(file, None, None, None, false)?;
        all_batches.extend(batches);
    }

    if all_batches.is_empty() {
        for file in files {
            let _ = std::fs::remove_file(file);
        }
        return Ok(0);
    }

    // Concatenate all batches into one
    let merged = concat_batches(&schema, &all_batches)?;
    let row_count = merged.num_rows();

    if row_count == 0 {
        for file in files {
            let _ = std::fs::remove_file(file);
        }
        return Ok(0);
    }

    // Sort by (entity_id, property, timestamp) using Arrow lexsort
    let sort_columns = vec![
        SortColumn {
            values: Arc::clone(merged.column(0)),
            options: None,
        },
        SortColumn {
            values: Arc::clone(merged.column(1)),
            options: None,
        },
        SortColumn {
            values: Arc::clone(merged.column(2)),
            options: None,
        },
    ];
    let indices = lexsort_to_indices(&sort_columns, None)?;
    let sorted = take_record_batch(&merged, &indices)?;

    // Extract sorted data back to (TsKey, Vec<TimeSample>) for write_samples_to_parquet
    // (which handles row-group chunking and Parquet writer properties)
    let ids = sorted
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let props = sorted
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let timestamps = sorted
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let values = sorted
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    use crate::hot::{TimeSample, TsKey};
    use std::collections::HashMap;

    // Use a HashMap<(u64, &str), usize> to index into a Vec, avoiding a String
    // allocation per row. The &str references borrow from the Arrow StringArray
    // which lives for the duration of this scope.
    let mut key_index: HashMap<(u64, &str), usize> = HashMap::new();
    let mut grouped: Vec<(TsKey, Vec<TimeSample>)> = Vec::new();
    for i in 0..sorted.num_rows() {
        let id = ids.value(i);
        let prop = props.value(i);
        let sample = TimeSample {
            timestamp_nanos: timestamps.value(i),
            value: values.value(i),
        };
        let idx = *key_index.entry((id, prop)).or_insert_with(|| {
            let pos = grouped.len();
            grouped.push((
                TsKey {
                    node_id: selene_core::NodeId(id),
                    property: selene_core::IStr::new(prop),
                },
                Vec::new(),
            ));
            pos
        });
        grouped[idx].1.push(sample);
    }
    // Sort by key for deterministic output order (BTreeMap was sorted).
    grouped.sort_by(|a, b| {
        a.0.node_id
            .0
            .cmp(&b.0.node_id.0)
            .then_with(|| a.0.property.cmp(&b.0.property))
    });

    let data = grouped;

    // Write compacted file (multi-row-group layout, 10K rows per group)
    let tmp_path = dir.join("compacted.parquet.tmp");
    write_samples_to_parquet(&tmp_path, &data, Some(10_000))?;

    // Atomic swap and cleanup
    let final_path = dir.join("compacted.parquet");
    std::fs::rename(&tmp_path, &final_path)?;
    for file in files {
        if file != &final_path {
            let _ = std::fs::remove_file(file);
        }
    }

    Ok(row_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hot::{TimeSample, TsKey};
    use crate::parquet_writer::{read_samples_from_parquet, write_samples_to_parquet};
    use selene_core::{IStr, NodeId};

    fn sample(ts: i64, val: f64) -> TimeSample {
        TimeSample {
            timestamp_nanos: ts,
            value: val,
        }
    }

    #[test]
    fn compact_merges_files() {
        let dir = tempfile::tempdir().unwrap();
        let day_dir = dir.path().join("ts").join("2020-01-01");
        std::fs::create_dir_all(&day_dir).unwrap();

        // Write 3 separate files
        for i in 0..3 {
            let data = vec![(
                TsKey {
                    node_id: NodeId(1),
                    property: IStr::new("temp"),
                },
                vec![sample(i * 1000 + 100, 70.0 + i as f64)],
            )];
            let path = day_dir.join(format!("flush_{i}.parquet"));
            write_samples_to_parquet(&path, &data, None).unwrap();
        }

        // Verify 3 files exist
        let files_before: Vec<_> = std::fs::read_dir(&day_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .collect();
        assert_eq!(files_before.len(), 3);

        // Compact (use 0 age so all directories qualify)
        let compacted = compact_old_directories(&dir.path().join("ts"), 0).unwrap();
        assert_eq!(compacted, 1);

        // Verify single compacted file
        let files_after: Vec<_> = std::fs::read_dir(&day_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .collect();
        assert_eq!(files_after.len(), 1);
        assert_eq!(
            files_after[0].file_name().to_str().unwrap(),
            "compacted.parquet"
        );

        // Verify all data is present
        let results =
            read_samples_from_parquet(&files_after[0].path(), None, None, None, None).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn compact_skips_single_file_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let day_dir = dir.path().join("ts").join("2020-01-01");
        std::fs::create_dir_all(&day_dir).unwrap();

        let data = vec![(
            TsKey {
                node_id: NodeId(1),
                property: IStr::new("temp"),
            },
            vec![sample(100, 70.0)],
        )];
        write_samples_to_parquet(&day_dir.join("single.parquet"), &data, None).unwrap();

        let compacted = compact_old_directories(&dir.path().join("ts"), 0).unwrap();
        assert_eq!(compacted, 0);
    }

    #[test]
    fn compact_nonexistent_dir() {
        let result = compact_old_directories(Path::new("/nonexistent"), 0).unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn compacted_file_has_multiple_row_groups() {
        use parquet::file::reader::FileReader;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi_rg.parquet");

        // Write enough data for 3 row groups (10K each)
        let data: Vec<(TsKey, Vec<TimeSample>)> = (1..=5)
            .map(|eid| {
                let samples: Vec<TimeSample> = (0..5000)
                    .map(|i| sample(i * 1000, 70.0 + i as f64 * 0.01))
                    .collect();
                (
                    TsKey {
                        node_id: NodeId(eid),
                        property: IStr::new("temp"),
                    },
                    samples,
                )
            })
            .collect();

        write_samples_to_parquet(&path, &data, Some(10_000)).unwrap();

        // Verify multiple row groups
        let file = std::fs::File::open(&path).unwrap();
        let reader = parquet::file::reader::SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();
        assert!(
            metadata.num_row_groups() >= 2,
            "expected >=2 row groups, got {}",
            metadata.num_row_groups()
        );
    }
}
