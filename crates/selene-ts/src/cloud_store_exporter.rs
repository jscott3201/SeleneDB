//! Cloud object store exporter for cold-tier Parquet files.
//!
//! Uploads Parquet files as raw bytes to S3, GCS, Azure Blob Storage,
//! or MinIO via the `object_store` crate. Files are stored with
//! Hive-style partitioning (`node=<id>/date=<date>/`) for
//! compatibility with external query engines (Spark, DuckDB, Trino).
//!
//! Feature-gated behind `cloud-storage`.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::export::{ExportAdapter, ExportError, ExportMetadata};

/// Exports cold-tier Parquet files to a cloud object store.
///
/// The exporter uploads the original Parquet bytes (no re-encoding)
/// to a Hive-partitioned path under the configured prefix:
///
/// ```text
/// {prefix}/node={node_id}/date=2026-03-15/data.parquet
/// ```
///
/// Construct via [`ObjectStoreExporter::new`] with a cloud URL
/// (e.g., `s3://bucket/prefix`) or via [`ObjectStoreExporter::with_store`]
/// in tests.
pub struct ObjectStoreExporter {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    node_id: String,
}

impl ObjectStoreExporter {
    /// Create a new exporter from a cloud URL.
    ///
    /// Supported URL schemes: `s3://`, `gs://`, `az://`, `azblob://`,
    /// and any scheme supported by `object_store::parse_url`.
    ///
    /// The URL path is used as the base prefix for all uploads.
    pub fn new(cloud_url: &str, node_id: String) -> Result<Self, ExportError> {
        let parsed =
            url::Url::parse(cloud_url).map_err(|e| ExportError::Failed(format!("bad URL: {e}")))?;
        let (store, prefix) = object_store::parse_url(&parsed)
            .map_err(|e| ExportError::Failed(format!("object store init: {e}")))?;
        Ok(Self {
            store: Arc::new(store),
            prefix,
            node_id,
        })
    }

    /// Constructor that accepts a pre-built store (useful for testing with InMemory backends).
    pub fn with_store(store: Arc<dyn ObjectStore>, prefix: ObjectPath, node_id: String) -> Self {
        Self {
            store,
            prefix,
            node_id,
        }
    }

    /// Build the destination object path with Hive-style partitioning.
    fn dest_path(&self, metadata: &ExportMetadata) -> ObjectPath {
        let filename = Path::new(&metadata.source_path)
            .file_name()
            .and_then(|f| f.to_str())
            .unwrap_or("data.parquet");

        let node_part = format!("node={}", self.node_id);
        let date_part = format!("date={}", metadata.date);

        let mut path = self.prefix.clone();
        path = path.join(node_part);
        path = path.join(date_part);
        path = path.join(filename);
        path
    }
}

impl ExportAdapter for ObjectStoreExporter {
    fn name(&self) -> &'static str {
        "cloud"
    }

    fn export<'a>(
        &'a self,
        _batches: &'a [RecordBatch],
        metadata: &'a ExportMetadata,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExportError>> + Send + 'a>> {
        Box::pin(async move {
            let bytes = tokio::fs::read(&metadata.source_path)
                .await
                .map_err(|e| ExportError::Failed(format!("read source file: {e}")))?;

            let dest = self.dest_path(metadata);
            let size = bytes.len();
            self.store
                .put(&dest, PutPayload::from(bytes))
                .await
                .map_err(|e| ExportError::Failed(format!("cloud upload: {e}")))?;

            tracing::debug!(
                dest = %dest,
                size,
                date = %metadata.date,
                "uploaded Parquet to cloud"
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
    use object_store::memory::InMemory;
    use selene_core::NodeId;
    use std::path::PathBuf;

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

    fn make_exporter(store: Arc<dyn ObjectStore>, node_id: &str) -> ObjectStoreExporter {
        ObjectStoreExporter::with_store(store, ObjectPath::from("ts-export"), node_id.to_string())
    }

    #[tokio::test]
    async fn upload_parquet_to_cloud() {
        let dir = tempfile::tempdir().unwrap();
        let parquet_path = write_test_parquet(dir.path());
        let expected_bytes = std::fs::read(&parquet_path).unwrap();

        let store = Arc::new(InMemory::new());
        let exporter = make_exporter(store.clone(), "node-1");

        let metadata = ExportMetadata {
            date: "2026-03-15".into(),
            row_count: 3,
            file_size: std::fs::metadata(&parquet_path).unwrap().len(),
            source_path: parquet_path,
        };

        exporter.export(&[], &metadata).await.unwrap();

        // Verify the uploaded object matches the source file
        let dest = ObjectPath::from("ts-export/node=node-1/date=2026-03-15/test.parquet");
        let result = store.get(&dest).await.unwrap();
        let uploaded_bytes = result.bytes().await.unwrap();
        assert_eq!(uploaded_bytes.as_ref(), expected_bytes.as_slice());
    }

    #[tokio::test]
    async fn hive_path_construction() {
        let store = Arc::new(InMemory::new());
        let exporter = ObjectStoreExporter::with_store(
            store,
            ObjectPath::from("warehouse/cold"),
            "edge-42".to_string(),
        );

        let metadata = ExportMetadata {
            date: "2026-01-01".into(),
            row_count: 0,
            file_size: 0,
            source_path: PathBuf::from("/data/cold/2026-01-01/sensors.parquet"),
        };

        let path = exporter.dest_path(&metadata);
        assert_eq!(
            path.as_ref(),
            "warehouse/cold/node=edge-42/date=2026-01-01/sensors.parquet"
        );
    }

    #[tokio::test]
    async fn idempotent_upload() {
        let dir = tempfile::tempdir().unwrap();
        let parquet_path = write_test_parquet(dir.path());
        let expected_bytes = std::fs::read(&parquet_path).unwrap();

        let store = Arc::new(InMemory::new());
        let exporter = make_exporter(store.clone(), "node-1");

        let metadata = ExportMetadata {
            date: "2026-03-15".into(),
            row_count: 3,
            file_size: std::fs::metadata(&parquet_path).unwrap().len(),
            source_path: parquet_path,
        };

        // Upload twice -- second call must not fail
        exporter.export(&[], &metadata).await.unwrap();
        exporter.export(&[], &metadata).await.unwrap();

        let dest = ObjectPath::from("ts-export/node=node-1/date=2026-03-15/test.parquet");
        let result = store.get(&dest).await.unwrap();
        let uploaded_bytes = result.bytes().await.unwrap();
        assert_eq!(uploaded_bytes.as_ref(), expected_bytes.as_slice());
    }

    #[tokio::test]
    async fn missing_source_file_returns_error() {
        let store = Arc::new(InMemory::new());
        let exporter = make_exporter(store, "node-1");

        let metadata = ExportMetadata {
            date: "2026-03-15".into(),
            row_count: 0,
            file_size: 0,
            source_path: PathBuf::from("/nonexistent/path/data.parquet"),
        };

        let result = exporter.export(&[], &metadata).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("read source file"),
            "expected read error, got: {err}"
        );
    }
}
