//! Export adapter framework for shipping time-series data to external systems.
//!
//! The retention task calls registered export adapters before deleting expired
//! Parquet files. This gives adapters a chance to archive, replicate, or
//! aggregate the data before it's lost.
//!
//! # Built-in adapters
//!
//! - [`ArrowIpcExporter`] — writes Arrow IPC files to a target directory.
//!   Serves as the reference implementation and the Hub aggregator pattern
//!   (aggregation nodes read IPC files from a shared directory or network mount).
//!
//! # Custom adapters
//!
//! Implement the [`ExportAdapter`] trait and register via
//! `ExportPipeline::add_adapter()`.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

/// Metadata about a Parquet file being exported.
#[derive(Debug, Clone)]
pub struct ExportMetadata {
    /// Date string from the directory name (e.g., "2026-03-15").
    pub date: String,
    /// Number of rows in the Parquet file.
    pub row_count: usize,
    /// File size in bytes.
    pub file_size: u64,
    /// Path to the original Parquet file on disk.
    pub source_path: PathBuf,
}

/// Trait for exporting time-series data to external systems.
///
/// Adapters receive Arrow RecordBatches already read from Parquet.
pub trait ExportAdapter: Send + Sync {
    /// Human-readable name for logging.
    fn name(&self) -> &'static str;

    /// Export a batch of time-series records.
    ///
    /// Called once per Parquet file with its contents as Arrow RecordBatches.
    /// Must be idempotent: the same data may be re-exported on retry.
    fn export<'a>(
        &'a self,
        batches: &'a [RecordBatch],
        metadata: &'a ExportMetadata,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExportError>> + Send + 'a>>;
}

/// Error from an export adapter.
#[derive(Debug, thiserror::Error)]
pub enum ExportError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("export failed: {0}")]
    Failed(String),
}

/// A pipeline of export adapters. Files pass through all adapters in order.
///
/// If any adapter fails, the error is logged but the pipeline continues
/// (best-effort delivery). The file is deleted only if all adapters succeed.
pub struct ExportPipeline {
    adapters: Vec<Arc<dyn ExportAdapter>>,
}

impl ExportPipeline {
    /// Create an empty pipeline (no adapters — files are deleted without export).
    pub fn new() -> Self {
        Self {
            adapters: Vec::new(),
        }
    }

    /// Add an adapter to the pipeline.
    pub fn add_adapter(&mut self, adapter: Arc<dyn ExportAdapter>) {
        self.adapters.push(adapter);
    }

    /// Returns true if the pipeline has no adapters.
    pub fn is_empty(&self) -> bool {
        self.adapters.is_empty()
    }

    /// Export a Parquet file through all adapters.
    ///
    /// Read the file into Arrow RecordBatches once, then pass them to
    /// each adapter. Return Ok only if all adapters succeed.
    pub async fn export_file(
        &self,
        path: &Path,
        metadata: &ExportMetadata,
    ) -> Result<(), ExportError> {
        if self.adapters.is_empty() {
            return Ok(());
        }

        // Read file once, share batches across adapters
        let batches = read_parquet_batches(path)?;

        let mut all_ok = true;
        for adapter in &self.adapters {
            if let Err(e) = adapter.export(&batches, metadata).await {
                tracing::error!(
                    adapter = adapter.name(),
                    error = %e,
                    path = %path.display(),
                    "export adapter failed"
                );
                all_ok = false;
            }
        }

        if all_ok {
            Ok(())
        } else {
            Err(ExportError::Failed(
                "one or more export adapters failed".into(),
            ))
        }
    }
}

impl Default for ExportPipeline {
    fn default() -> Self {
        Self::new()
    }
}

/// Read a Parquet file into Arrow RecordBatches for export.
fn read_parquet_batches(path: &Path) -> Result<Vec<RecordBatch>, ExportError> {
    let file = std::fs::File::open(path)?;
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| ExportError::Failed(format!("parquet reader: {e}")))?
        .build()
        .map_err(|e| ExportError::Failed(format!("parquet build: {e}")))?;

    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

/// Export time-series data as Arrow IPC files to a target directory.
///
/// Writes IPC files to a shared directory for any Arrow-aware consumer.
/// IPC is zero-copy-friendly and preserves full Arrow type information.
///
/// Directory structure:
/// ```text
/// export_dir/
///   2026-03-15/
///     120000.ipc
///     130000.ipc
///   2026-03-16/
///     ...
/// ```
pub struct ArrowIpcExporter {
    export_dir: PathBuf,
}

impl ArrowIpcExporter {
    /// Create a new exporter that writes to the given directory.
    pub fn new(export_dir: impl Into<PathBuf>) -> Self {
        Self {
            export_dir: export_dir.into(),
        }
    }
}

impl ExportAdapter for ArrowIpcExporter {
    fn name(&self) -> &'static str {
        "arrow_ipc"
    }

    fn export<'a>(
        &'a self,
        batches: &'a [RecordBatch],
        metadata: &'a ExportMetadata,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExportError>> + Send + 'a>> {
        Box::pin(async move {
            if batches.is_empty() {
                return Ok(());
            }

            let day_dir = self.export_dir.join(&metadata.date);
            std::fs::create_dir_all(&day_dir)?;

            // Generate time-based filename
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            let time_part = (now.as_secs() % 86400) as u32;
            let hours = time_part / 3600;
            let minutes = (time_part % 3600) / 60;
            let seconds = time_part % 60;
            let filename = format!("{hours:02}{minutes:02}{seconds:02}.ipc");
            let path = day_dir.join(filename);

            let file = std::fs::File::create(&path)?;
            let mut writer = arrow::ipc::writer::FileWriter::try_new(file, &batches[0].schema())?;
            for batch in batches {
                writer.write(batch)?;
            }
            writer.finish()?;

            tracing::debug!(
                path = %path.display(),
                rows = metadata.row_count,
                "exported Arrow IPC"
            );

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hot::{TimeSample, TsKey};
    use crate::parquet_writer::write_samples_to_parquet;
    use selene_core::NodeId;

    fn write_test_parquet(dir: &Path) -> PathBuf {
        let path = dir.join("test.parquet");
        let data = vec![(
            TsKey {
                node_id: NodeId(1),
                property: selene_core::IStr::new("temp"),
            },
            vec![
                TimeSample {
                    timestamp_nanos: 100,
                    value: 72.0,
                },
                TimeSample {
                    timestamp_nanos: 200,
                    value: 73.0,
                },
                TimeSample {
                    timestamp_nanos: 300,
                    value: 74.0,
                },
            ],
        )];
        write_samples_to_parquet(&path, &data, None).unwrap();
        path
    }

    #[test]
    fn read_parquet_batches_works() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_parquet(dir.path());
        let batches = read_parquet_batches(&path).unwrap();
        assert!(!batches.is_empty());
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn arrow_ipc_exporter_writes_file() {
        let src_dir = tempfile::tempdir().unwrap();
        let export_dir = tempfile::tempdir().unwrap();
        let parquet_path = write_test_parquet(src_dir.path());

        let exporter = ArrowIpcExporter::new(export_dir.path());
        let metadata = ExportMetadata {
            date: "2026-03-15".into(),
            row_count: 3,
            file_size: std::fs::metadata(&parquet_path).unwrap().len(),
            source_path: parquet_path.clone(),
        };
        let batches = read_parquet_batches(&parquet_path).unwrap();
        exporter.export(&batches, &metadata).await.unwrap();

        // Verify IPC file was created
        let day_dir = export_dir.path().join("2026-03-15");
        assert!(day_dir.exists());
        let files: Vec<_> = std::fs::read_dir(&day_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "ipc"))
            .collect();
        assert_eq!(files.len(), 1);
    }

    #[tokio::test]
    async fn pipeline_runs_all_adapters() {
        let src_dir = tempfile::tempdir().unwrap();
        let export_dir1 = tempfile::tempdir().unwrap();
        let export_dir2 = tempfile::tempdir().unwrap();
        let parquet_path = write_test_parquet(src_dir.path());

        let mut pipeline = ExportPipeline::new();
        pipeline.add_adapter(Arc::new(ArrowIpcExporter::new(export_dir1.path())));
        pipeline.add_adapter(Arc::new(ArrowIpcExporter::new(export_dir2.path())));

        let metadata = ExportMetadata {
            date: "2026-03-15".into(),
            row_count: 3,
            file_size: std::fs::metadata(&parquet_path).unwrap().len(),
            source_path: parquet_path.clone(),
        };
        pipeline
            .export_file(&parquet_path, &metadata)
            .await
            .unwrap();

        // Both directories should have IPC files
        assert!(export_dir1.path().join("2026-03-15").exists());
        assert!(export_dir2.path().join("2026-03-15").exists());
    }

    #[tokio::test]
    async fn empty_pipeline_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_parquet(dir.path());
        let pipeline = ExportPipeline::new();
        let metadata = ExportMetadata {
            date: "2026-03-15".into(),
            row_count: 3,
            file_size: 0,
            source_path: path.clone(),
        };
        pipeline.export_file(&path, &metadata).await.unwrap();
    }
}
