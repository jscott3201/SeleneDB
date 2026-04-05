#![forbid(unsafe_code)]
//! Multi-tier time-series: in-memory hot + warm aggregates + Parquet cold.
//!
//! - **Hot**: recent data in Gorilla-compressed 30-min blocks (16 shards, memory-budgeted)
//! - **Warm**: downsampled aggregates (min/max/sum/count per window)
//! - **Cold**: expired data flushed to Parquet on disk (default 7d retention)
//! - **Export**: adapters ship data to external systems before retention cleanup
//! - **Compaction**: daily merge of small flush files into single sorted files

pub(crate) mod calendar;
#[cfg(feature = "cloud-storage")]
mod cloud_store_exporter;
pub mod compact;
pub mod config;
pub mod encoding;
pub mod error;
pub mod export;
#[cfg(feature = "cloud-storage")]
pub use cloud_store_exporter::ObjectStoreExporter;
pub mod flush;
pub mod hot;
pub mod hot_drain;
pub mod parquet_writer;
pub mod retention;
pub mod warm;

pub use config::{TsConfig, WarmTierConfig};
pub use error::TsError;
pub use flush::FlushTask;
pub use hot::{HotTier, TimeSample, TsKey};
pub use warm::WarmAggregate;
