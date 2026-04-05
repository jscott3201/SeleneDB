//! Parquet writer for time-series data.
//!
//! Writes samples to Parquet files with schema:
//! `(entity_id: UInt64, property: Utf8, timestamp: Int64, value: Float64)`
//! Sorted by `(entity_id, property, timestamp)`.  Zstd compression with
//! page-level statistics and bloom filters for efficient predicate pushdown.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::compute::kernels::aggregate::{max as arrow_max, min as arrow_min, sum as arrow_sum};
use arrow::compute::kernels::cmp::{eq as arrow_eq, gt_eq as arrow_gte, lt_eq as arrow_lte};
use arrow::compute::{and as arrow_and, filter_record_batch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::bloom_filter::Sbbf;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::reader::ChunkReader;
use parquet::schema::types::ColumnPath;

use selene_core::NodeId;

use crate::error::TsError;
use crate::hot::{TimeSample, TsKey};

/// The Arrow schema for time-series Parquet files.
pub fn ts_schema() -> Schema {
    Schema::new(vec![
        Field::new("entity_id", DataType::UInt64, false),
        Field::new("property", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ])
}

/// Write time-series samples to a Parquet file.
///
/// Samples are sorted by `(entity_id, property, timestamp)` before writing.
/// Uses zstd compression, page-level statistics, and bloom filters for
/// efficient predicate pushdown during queries.
///
/// When `max_row_group_size` is `Some(n)`, rows are chunked into batches
/// of at most `n` rows, producing multiple row groups in a single file
/// for better row-group-level pushdown during reads. When `None`, all
/// rows are written in a single row group.
///
/// Returns the number of rows written.
pub fn write_samples_to_parquet(
    path: &Path,
    data: &[(TsKey, Vec<TimeSample>)],
    max_row_group_size: Option<usize>,
) -> Result<usize, TsError> {
    // Flatten and sort by (entity_id, property, timestamp)
    let mut rows: Vec<(u64, &str, i64, f64)> = Vec::new();
    for (key, samples) in data {
        for sample in samples {
            rows.push((
                key.node_id.0,
                key.property.as_str(),
                sample.timestamp_nanos,
                sample.value,
            ));
        }
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(b.1)).then(a.2.cmp(&b.2)));

    if rows.is_empty() {
        return Ok(0);
    }

    let row_count = rows.len();
    let schema = Arc::new(ts_schema());

    // Ensure parent directory
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Zstd level 3, page-level statistics, bloom filters, sorting columns
    let mut builder = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_bloom_filter_enabled(true)
        .set_bloom_filter_fpp(0.01) // 1% false positive rate
        .set_bloom_filter_ndv(1000) // expected distinct values
        .set_sorting_columns(Some(vec![
            parquet::file::metadata::SortingColumn { column_idx: 0, descending: false, nulls_first: false },
            parquet::file::metadata::SortingColumn { column_idx: 1, descending: false, nulls_first: false },
            parquet::file::metadata::SortingColumn { column_idx: 2, descending: false, nulls_first: false },
        ]))
        // Per-column encoding: entity_id and property have low cardinality in IoT
        // data, so dictionary encoding is ideal (also the Parquet default, but
        // explicit here for clarity). Timestamps are regular-interval nanoseconds
        // that compress dramatically with delta encoding; dictionary is disabled
        // for that column so DELTA_BINARY_PACKED is used directly.
        .set_column_dictionary_enabled(
            ColumnPath::new(vec!["entity_id".into()]),
            true,
        )
        .set_column_dictionary_enabled(
            ColumnPath::new(vec!["property".into()]),
            true,
        )
        .set_column_dictionary_enabled(
            ColumnPath::new(vec!["timestamp".into()]),
            false,
        )
        .set_column_encoding(
            ColumnPath::new(vec!["timestamp".into()]),
            Encoding::DELTA_BINARY_PACKED,
        );

    builder = builder.set_max_row_group_row_count(max_row_group_size);

    let props = builder.build();

    let tmp_path = path.with_extension("parquet.tmp");
    let file = std::fs::File::create(&tmp_path)?;
    let mut writer =
        ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).map_err(TsError::Parquet)?;

    // Chunk rows into multiple row groups when configured
    let chunks: Vec<&[(u64, &str, i64, f64)]> = match max_row_group_size {
        Some(rg_size) => rows.chunks(rg_size).collect(),
        None => vec![&rows],
    };

    for chunk in chunks {
        let entity_ids: UInt64Array = chunk.iter().map(|r| r.0).collect();
        let properties: StringArray = chunk.iter().map(|r| Some(r.1)).collect();
        let timestamps: Int64Array = chunk.iter().map(|r| r.2).collect();
        let values: Float64Array = chunk.iter().map(|r| r.3).collect();

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(entity_ids),
                Arc::new(properties),
                Arc::new(timestamps),
                Arc::new(values),
            ],
        )
        .map_err(TsError::Arrow)?;

        writer.write(&batch).map_err(TsError::Parquet)?;
    }

    writer.close().map_err(TsError::Parquet)?;
    // Rename after close so the destination file is always a complete, valid Parquet file.
    std::fs::rename(&tmp_path, path)?;

    Ok(row_count)
}

/// Read time-series samples from a Parquet file.
///
/// Use row-group statistics and bloom filters for predicate pushdown when
/// filtering by entity_id, property, and timestamp range, skipping
/// irrelevant row groups. Within each batch, Arrow compute kernels build a
/// vectorized boolean mask (SIMD-accelerated on ARM NEON / x86 AVX2)
/// instead of row-by-row scalar checks.
pub fn read_samples_from_parquet(
    path: &Path,
    entity_id: Option<NodeId>,
    property: Option<&str>,
    start: Option<i64>,
    end: Option<i64>,
) -> Result<Vec<(u64, String, TimeSample)>, TsError> {
    let file = std::fs::File::open(path)?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(TsError::Parquet)?;

    // Open a second file handle for bloom filter reads (the builder consumed the first).
    let bf_file = std::fs::File::open(path)?;
    let row_groups = prune_row_groups(
        builder.metadata(),
        &bf_file,
        entity_id,
        property,
        start,
        end,
    );
    let reader = builder
        .with_row_groups(row_groups)
        .build()
        .map_err(TsError::Parquet)?;

    let mut results = Vec::new();

    for batch_result in reader {
        let batch = batch_result.map_err(TsError::Arrow)?;
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let props = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let timestamps = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Build vectorized boolean mask using Arrow compute kernels
        let mut mask = BooleanArray::from(vec![true; num_rows]);

        if let Some(filter_id) = entity_id {
            let target = UInt64Array::from(vec![filter_id.0; num_rows]);
            let eq_mask = arrow_eq(ids, &target).map_err(TsError::Arrow)?;
            mask = arrow_and(&mask, &eq_mask).map_err(TsError::Arrow)?;
        }

        if let Some(filter_prop) = property {
            let target = StringArray::from(vec![filter_prop; num_rows]);
            let eq_mask = arrow_eq(props, &target).map_err(TsError::Arrow)?;
            mask = arrow_and(&mask, &eq_mask).map_err(TsError::Arrow)?;
        }

        if let Some(s) = start {
            let target = Int64Array::from(vec![s; num_rows]);
            let gte_mask = arrow_gte(timestamps, &target).map_err(TsError::Arrow)?;
            mask = arrow_and(&mask, &gte_mask).map_err(TsError::Arrow)?;
        }

        if let Some(e) = end {
            let target = Int64Array::from(vec![e; num_rows]);
            let lte_mask = arrow_lte(timestamps, &target).map_err(TsError::Arrow)?;
            mask = arrow_and(&mask, &lte_mask).map_err(TsError::Arrow)?;
        }

        let filtered = filter_record_batch(&batch, &mask).map_err(TsError::Arrow)?;

        let f_ids = filtered
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let f_props = filtered
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let f_ts = filtered
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let f_vals = filtered
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for i in 0..filtered.num_rows() {
            results.push((
                f_ids.value(i),
                f_props.value(i).to_string(),
                TimeSample {
                    timestamp_nanos: f_ts.value(i),
                    value: f_vals.value(i),
                },
            ));
        }
    }

    Ok(results)
}

/// Read Parquet file into RecordBatches with row-group pruning and optional column projection.
///
/// When `project_value_only` is true, reads only timestamp (col 2) and value (col 3) columns,
/// skipping entity_id and property decoding. Use this when entity_id and property filtering
/// is handled by row-group pruning alone (single-entity files or post-prune).
pub fn read_parquet_batches(
    path: &Path,
    entity_id: Option<NodeId>,
    start: Option<i64>,
    end: Option<i64>,
    project_value_only: bool,
) -> Result<Vec<RecordBatch>, TsError> {
    let file = std::fs::File::open(path)?;
    let mut builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(TsError::Parquet)?;

    // Row-group pruning via statistics + bloom filters
    let bf_file = std::fs::File::open(path)?;
    let row_groups = prune_row_groups(builder.metadata(), &bf_file, entity_id, None, start, end);

    if project_value_only {
        let schema = builder.parquet_schema();
        let mask = ProjectionMask::leaves(schema, [2, 3]); // timestamp + value only
        builder = builder.with_projection(mask);
    }

    let reader = builder
        .with_row_groups(row_groups)
        .build()
        .map_err(TsError::Parquet)?;

    let mut batches = Vec::new();
    for batch_result in reader {
        batches.push(batch_result.map_err(TsError::Arrow)?);
    }
    Ok(batches)
}

/// Aggregate a value column from Parquet RecordBatches using SIMD-accelerated
/// Arrow compute kernels. Returns (sum, min, max, count).
///
/// `value_col_idx` is the column index of the Float64 value column in the
/// RecordBatch. For full-projection reads this is 3 (the 4th column). For
/// value-only projected reads (timestamp + value) this is 1.
pub fn aggregate_value_column(
    batches: &[RecordBatch],
    value_col_idx: usize,
) -> (f64, f64, f64, u64) {
    let mut total_sum = 0.0f64;
    let mut total_min = f64::INFINITY;
    let mut total_max = f64::NEG_INFINITY;
    let mut total_count = 0u64;

    for batch in batches {
        let col = batch.column(value_col_idx);
        let values = col.as_any().downcast_ref::<Float64Array>().unwrap();

        if let Some(s) = arrow_sum(values) {
            total_sum += s;
        }
        if let Some(mn) = arrow_min(values) {
            total_min = total_min.min(mn);
        }
        if let Some(mx) = arrow_max(values) {
            total_max = total_max.max(mx);
        }
        total_count += values.len() as u64 - values.null_count() as u64;
    }

    (total_sum, total_min, total_max, total_count)
}

/// Select row groups whose statistics and bloom filters overlap the given filters.
/// Return indexes of row groups to read (no filters = all groups).
///
/// Pruning order per row group:
/// 1. Min/max statistics on entity_id and timestamp (cheap metadata check).
/// 2. Bloom filters on entity_id (column 0) and property (column 1) when
///    the corresponding filter value is provided. Bloom filter reads require
///    seeking into the Parquet file, so they run only for groups that survive
///    statistics pruning.
fn prune_row_groups<R: ChunkReader>(
    metadata: &Arc<parquet::file::metadata::ParquetMetaData>,
    reader: &R,
    entity_id: Option<NodeId>,
    property: Option<&str>,
    start: Option<i64>,
    end: Option<i64>,
) -> Vec<usize> {
    use parquet::file::statistics::Statistics;

    let num_groups = metadata.num_row_groups();
    let mut selected = Vec::with_capacity(num_groups);

    for i in 0..num_groups {
        let rg = metadata.row_group(i);
        let mut dominated = false;

        // --- Phase 1: min/max statistics (free, already in metadata footer) ---

        // Column 0: entity_id (UInt64 stored as Int64 in Parquet statistics)
        if let Some(filter_id) = entity_id
            && let Some(stats) = rg.column(0).statistics()
            && let Statistics::Int64(s) = stats
        {
            // Entity IDs are always positive (< i64::MAX), so direct comparison works.
            let min = *s.min_opt().unwrap_or(&0) as u64;
            let max = *s.max_opt().unwrap_or(&(i64::MAX)) as u64;
            if filter_id.0 < min || filter_id.0 > max {
                dominated = true;
            }
        }

        // Column 2: timestamp (Int64)
        if !dominated
            && let Some(stats) = rg.column(2).statistics()
            && let Statistics::Int64(s) = stats
        {
            let rg_min = *s.min_opt().unwrap_or(&i64::MIN);
            let rg_max = *s.max_opt().unwrap_or(&i64::MAX);
            if let Some(filter_start) = start
                && rg_max < filter_start
            {
                dominated = true;
            }
            if let Some(filter_end) = end
                && rg_min > filter_end
            {
                dominated = true;
            }
        }

        // --- Phase 2: bloom filters (requires file I/O, run after stats pruning) ---

        // Column 0: entity_id bloom filter.
        // Arrow UInt64 maps to Parquet INT64 physical type, so the bloom filter
        // stores the i64 byte representation. Cast through i64 for the check.
        if !dominated
            && let Some(filter_id) = entity_id
            && let Ok(Some(bf)) = Sbbf::read_from_column_chunk(rg.column(0), reader)
        {
            let physical_val = filter_id.0 as i64;
            if !bf.check(&physical_val) {
                dominated = true;
            }
        }

        // Column 1: property bloom filter (Utf8 / BYTE_ARRAY physical type).
        if !dominated
            && let Some(filter_prop) = property
            && let Ok(Some(bf)) = Sbbf::read_from_column_chunk(rg.column(1), reader)
            && !bf.check(&filter_prop)
        {
            dominated = true;
        }

        if !dominated {
            selected.push(i);
        }
    }

    selected
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(ts: i64, val: f64) -> TimeSample {
        TimeSample {
            timestamp_nanos: ts,
            value: val,
        }
    }

    #[test]
    fn write_and_read_parquet() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        let data = vec![
            (
                TsKey {
                    node_id: NodeId(1),
                    property: selene_core::IStr::new("temp"),
                },
                vec![sample(100, 72.0), sample(200, 73.0)],
            ),
            (
                TsKey {
                    node_id: NodeId(2),
                    property: selene_core::IStr::new("humidity"),
                },
                vec![sample(150, 45.0)],
            ),
        ];

        let count = write_samples_to_parquet(&path, &data, None).unwrap();
        assert_eq!(count, 3);
        assert!(path.exists());

        let results = read_samples_from_parquet(&path, None, None, None, None).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn read_with_entity_filter() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        let data = vec![
            (
                TsKey {
                    node_id: NodeId(1),
                    property: selene_core::IStr::new("temp"),
                },
                vec![sample(100, 72.0)],
            ),
            (
                TsKey {
                    node_id: NodeId(2),
                    property: selene_core::IStr::new("temp"),
                },
                vec![sample(100, 68.0)],
            ),
        ];

        write_samples_to_parquet(&path, &data, None).unwrap();

        let results = read_samples_from_parquet(&path, Some(NodeId(1)), None, None, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
    }

    #[test]
    fn read_with_property_filter() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        let data = vec![
            (
                TsKey {
                    node_id: NodeId(1),
                    property: selene_core::IStr::new("temp"),
                },
                vec![sample(100, 72.0)],
            ),
            (
                TsKey {
                    node_id: NodeId(1),
                    property: selene_core::IStr::new("humidity"),
                },
                vec![sample(100, 45.0)],
            ),
        ];

        write_samples_to_parquet(&path, &data, None).unwrap();

        let results =
            read_samples_from_parquet(&path, Some(NodeId(1)), Some("temp"), None, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, "temp");

        // All properties
        let results = read_samples_from_parquet(&path, Some(NodeId(1)), None, None, None).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn read_with_time_range() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        let data = vec![(
            TsKey {
                node_id: NodeId(1),
                property: selene_core::IStr::new("temp"),
            },
            vec![sample(100, 70.0), sample(200, 71.0), sample(300, 72.0)],
        )];

        write_samples_to_parquet(&path, &data, None).unwrap();

        let results = read_samples_from_parquet(&path, None, None, Some(150), Some(250)).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].2.timestamp_nanos, 200);
    }

    #[test]
    fn write_empty_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        let count = write_samples_to_parquet(&path, &[], None).unwrap();
        assert_eq!(count, 0);
        assert!(!path.exists()); // no file written for empty data
    }

    #[test]
    fn data_is_sorted_in_parquet() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        // Write out of order
        let data = vec![
            (
                TsKey {
                    node_id: NodeId(2),
                    property: selene_core::IStr::new("temp"),
                },
                vec![sample(300, 72.0)],
            ),
            (
                TsKey {
                    node_id: NodeId(1),
                    property: selene_core::IStr::new("temp"),
                },
                vec![sample(100, 70.0)],
            ),
        ];

        write_samples_to_parquet(&path, &data, None).unwrap();

        let results = read_samples_from_parquet(&path, None, None, None, None).unwrap();
        // Should be sorted by entity_id
        assert_eq!(results[0].0, 1);
        assert_eq!(results[1].0, 2);
    }

    #[test]
    fn schema_is_correct() {
        let schema = ts_schema();
        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "entity_id");
        assert_eq!(schema.field(1).name(), "property");
        assert_eq!(schema.field(2).name(), "timestamp");
        assert_eq!(schema.field(3).name(), "value");
    }

    #[test]
    fn parquet_has_statistics() {
        use parquet::file::reader::FileReader;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        let data = vec![(
            TsKey {
                node_id: NodeId(1),
                property: selene_core::IStr::new("temp"),
            },
            vec![sample(100, 70.0), sample(200, 71.0), sample(300, 72.0)],
        )];

        write_samples_to_parquet(&path, &data, None).unwrap();

        // Verify the file has statistics by reading the metadata
        let file = std::fs::File::open(&path).unwrap();
        let reader = parquet::file::reader::SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();

        assert!(metadata.num_row_groups() > 0);
        let rg = metadata.row_group(0);
        // entity_id column should have statistics
        let col = rg.column(0);
        assert!(col.statistics().is_some());
    }

    #[test]
    fn read_parquet_batches_with_projection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        let data = vec![(
            TsKey {
                node_id: NodeId(1),
                property: selene_core::IStr::new("temp"),
            },
            vec![sample(100, 72.0), sample(200, 73.0)],
        )];

        write_samples_to_parquet(&path, &data, None).unwrap();

        // Full projection: 4 columns
        let batches = read_parquet_batches(&path, None, None, None, false).unwrap();
        assert_eq!(batches[0].num_columns(), 4);
        assert_eq!(batches[0].num_rows(), 2);

        // Value-only projection: 2 columns (timestamp + value)
        let batches = read_parquet_batches(&path, None, None, None, true).unwrap();
        assert_eq!(batches[0].num_columns(), 2);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn aggregate_value_column_simd() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        let data = vec![(
            TsKey {
                node_id: NodeId(1),
                property: selene_core::IStr::new("temp"),
            },
            vec![sample(100, 70.0), sample(200, 72.0), sample(300, 74.0)],
        )];

        write_samples_to_parquet(&path, &data, None).unwrap();

        // Full projection: value is column 3
        let batches = read_parquet_batches(&path, Some(NodeId(1)), None, None, false).unwrap();
        let (sum, min, max, count) = aggregate_value_column(&batches, 3);
        assert_eq!(count, 3);
        assert!((sum - 216.0).abs() < 0.001);
        assert!((min - 70.0).abs() < 0.001);
        assert!((max - 74.0).abs() < 0.001);

        // Value-only projection: value is column 1
        let batches = read_parquet_batches(&path, Some(NodeId(1)), None, None, true).unwrap();
        let (sum2, min2, max2, count2) = aggregate_value_column(&batches, 1);
        assert_eq!(count2, 3);
        assert!((sum2 - 216.0).abs() < 0.001);
        assert!((min2 - 70.0).abs() < 0.001);
        assert!((max2 - 74.0).abs() < 0.001);
    }

    #[test]
    fn parquet_size_report() {
        for count in [100, 1_000, 5_000] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("size_test.parquet");

            let data: Vec<(TsKey, Vec<TimeSample>)> = (1..=10)
                .map(|eid| {
                    let samples: Vec<TimeSample> = (0..count / 10)
                        .map(|i| {
                            sample(1_000_000 + i64::from(i) * 1_000, 72.0 + f64::from(i) * 0.01)
                        })
                        .collect();
                    (
                        TsKey {
                            node_id: NodeId(eid),
                            property: selene_core::IStr::new("temp"),
                        },
                        samples,
                    )
                })
                .collect();

            let rows = write_samples_to_parquet(&path, &data, None).unwrap();
            let file_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            let bytes_per_row = if rows > 0 { file_size / rows as u64 } else { 0 };
            eprintln!(
                "parquet_size: {count} samples = {file_size} bytes ({:.1} KB), {bytes_per_row} bytes/row",
                file_size as f64 / 1024.0
            );
        }
    }

    /// Verify that bloom filter pruning skips row groups that do not contain
    /// the target entity_id. We write entities into separate row groups (one
    /// entity per group) so that each group's bloom filter covers exactly one
    /// entity, then confirm that reading for a specific entity returns only
    /// that entity's data and that irrelevant row groups are pruned away.
    #[test]
    fn bloom_filter_prunes_entity_id() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bloom_entity.parquet");

        // Three entities, each with 10 samples. Use max_row_group_size = 10
        // so each entity lands in its own row group.
        let data = vec![
            (
                TsKey {
                    node_id: NodeId(100),
                    property: selene_core::IStr::new("temp"),
                },
                (0..10).map(|i| sample(i * 1000, 70.0 + i as f64)).collect(),
            ),
            (
                TsKey {
                    node_id: NodeId(200),
                    property: selene_core::IStr::new("temp"),
                },
                (0..10).map(|i| sample(i * 1000, 80.0 + i as f64)).collect(),
            ),
            (
                TsKey {
                    node_id: NodeId(300),
                    property: selene_core::IStr::new("temp"),
                },
                (0..10).map(|i| sample(i * 1000, 90.0 + i as f64)).collect(),
            ),
        ];

        write_samples_to_parquet(&path, &data, Some(10)).unwrap();

        // Verify 3 row groups were created
        let file = std::fs::File::open(&path).unwrap();
        let reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 3);

        // Read for entity 200 only
        let results =
            read_samples_from_parquet(&path, Some(NodeId(200)), None, None, None).unwrap();
        assert_eq!(results.len(), 10);
        for (eid, _prop, _sample) in &results {
            assert_eq!(*eid, 200);
        }

        // Verify bloom filter pruning reduced the row groups selected.
        // Open a fresh reader and run prune_row_groups directly.
        let file2 = std::fs::File::open(&path).unwrap();
        let builder2 =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file2).unwrap();
        let bf_file = std::fs::File::open(&path).unwrap();
        let pruned = prune_row_groups(
            builder2.metadata(),
            &bf_file,
            Some(NodeId(200)),
            None,
            None,
            None,
        );
        // Bloom filters should eliminate 2 of the 3 row groups (entity 100 and 300).
        assert_eq!(
            pruned.len(),
            1,
            "bloom filter should prune to 1 row group, got {pruned:?}"
        );
    }

    /// Verify bloom filter pruning on the property column. Separate row groups
    /// by property and confirm that querying for one property skips the others.
    #[test]
    fn bloom_filter_prunes_property() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bloom_prop.parquet");

        // Same entity, different properties, each in its own row group.
        let data = vec![
            (
                TsKey {
                    node_id: NodeId(1),
                    property: selene_core::IStr::new("temp"),
                },
                (0..10).map(|i| sample(i * 1000, 70.0)).collect(),
            ),
            (
                TsKey {
                    node_id: NodeId(1),
                    property: selene_core::IStr::new("humidity"),
                },
                (0..10).map(|i| sample(i * 1000, 45.0)).collect(),
            ),
            (
                TsKey {
                    node_id: NodeId(1),
                    property: selene_core::IStr::new("pressure"),
                },
                (0..10).map(|i| sample(i * 1000, 1013.0)).collect(),
            ),
        ];

        write_samples_to_parquet(&path, &data, Some(10)).unwrap();

        // Verify 3 row groups
        let file = std::fs::File::open(&path).unwrap();
        let reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 3);

        // Read for "humidity" only
        let results = read_samples_from_parquet(&path, None, Some("humidity"), None, None).unwrap();
        assert_eq!(results.len(), 10);
        for (_eid, prop, _sample) in &results {
            assert_eq!(prop, "humidity");
        }

        // Verify bloom filter pruning on property column.
        let file2 = std::fs::File::open(&path).unwrap();
        let builder2 =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file2).unwrap();
        let bf_file = std::fs::File::open(&path).unwrap();
        let pruned = prune_row_groups(
            builder2.metadata(),
            &bf_file,
            None,
            Some("humidity"),
            None,
            None,
        );
        assert_eq!(
            pruned.len(),
            1,
            "bloom filter should prune to 1 row group for property, got {pruned:?}"
        );
    }

    /// Verify that bloom filter and statistics pruning work together: a query
    /// for an entity that exists in the file but not in the requested time
    /// range should return nothing.
    #[test]
    fn bloom_filter_combined_with_stats() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bloom_combined.parquet");

        let data = vec![
            (
                TsKey {
                    node_id: NodeId(10),
                    property: selene_core::IStr::new("temp"),
                },
                vec![sample(1000, 70.0), sample(2000, 71.0)],
            ),
            (
                TsKey {
                    node_id: NodeId(20),
                    property: selene_core::IStr::new("temp"),
                },
                vec![sample(5000, 80.0), sample(6000, 81.0)],
            ),
        ];

        // Two row groups of 2 rows each
        write_samples_to_parquet(&path, &data, Some(2)).unwrap();

        // Query entity 10 in a time range that only overlaps its row group
        let results =
            read_samples_from_parquet(&path, Some(NodeId(10)), None, Some(500), Some(2500))
                .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 10);
        assert_eq!(results[1].0, 10);

        // Query entity that does not exist at all
        let results =
            read_samples_from_parquet(&path, Some(NodeId(999)), None, None, None).unwrap();
        assert!(results.is_empty());
    }
}
