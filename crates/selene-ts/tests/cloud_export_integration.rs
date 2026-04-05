//! Integration test: full export pipeline round-trip with cloud adapter.

#![cfg(feature = "cloud-storage")]

use std::path::Path;
use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};

use selene_core::{IStr, NodeId};
use selene_ts::ObjectStoreExporter;
use selene_ts::export::ExportPipeline;
use selene_ts::hot::{TimeSample, TsKey};
use selene_ts::parquet_writer::write_samples_to_parquet;
use selene_ts::retention::cleanup_expired_with_export;

fn write_parquet(dir: &Path) -> std::path::PathBuf {
    let path = dir.join("data.parquet");
    let data = vec![(
        TsKey {
            node_id: NodeId(1),
            property: IStr::new("temp"),
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
        ],
    )];
    write_samples_to_parquet(&path, &data, None).unwrap();
    path
}

#[tokio::test]
async fn cloud_export_pipeline_round_trip() {
    // Set up: create an expired date directory with a Parquet file
    let dir = tempfile::tempdir().unwrap();
    let ts_dir = dir.path().join("ts");
    let old_dir = ts_dir.join("1970-01-01");
    std::fs::create_dir_all(&old_dir).unwrap();
    write_parquet(&old_dir);

    // Create pipeline with cloud adapter (InMemory backend)
    let store = Arc::new(InMemory::new());
    let exporter = ObjectStoreExporter::with_store(
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        ObjectPath::from("backup"),
        "test-node".to_string(),
    );

    let mut pipeline = ExportPipeline::new();
    pipeline.add_adapter(Arc::new(exporter));

    // Run retention -- should export then delete
    let deleted = cleanup_expired_with_export(&ts_dir, 7, Some(&pipeline))
        .await
        .unwrap();
    assert_eq!(deleted, 1);

    // Verify: local file deleted
    assert!(!old_dir.exists(), "expired directory should be deleted");

    // Verify: cloud object exists at expected Hive path
    let cloud_path = ObjectPath::from("backup/node=test-node/date=1970-01-01/data.parquet");
    let result = store.get(&cloud_path).await;
    assert!(result.is_ok(), "cloud object should exist at {cloud_path}");

    // Verify: uploaded bytes are valid Parquet (non-empty)
    let bytes = result.unwrap().bytes().await.unwrap();
    assert!(bytes.len() > 4, "uploaded file should be non-trivial");
    assert_eq!(
        &bytes[..4],
        b"PAR1",
        "should start with Parquet magic bytes"
    );
}
