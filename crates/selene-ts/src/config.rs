//! Time-series configuration.

use serde::Deserialize;

/// Configuration for the multi-tier time-series system.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TsConfig {
    /// Hours of hot data to keep in memory (default: 24).
    pub hot_retention_hours: u32,
    /// Days of medium data to keep on disk as Parquet (default: 7).
    pub medium_retention_days: u32,
    /// Minutes between flush cycles (default: 15).
    pub flush_interval_minutes: u32,
    /// Hard cap on samples per buffer (default: 86,400 = 1/sec for 24h).
    /// Safety valve alongside the memory budget. Set to 0 to disable.
    pub max_samples_per_buffer: usize,
    /// Hours of inactivity before evicting a buffer's key (default: 48).
    /// Set to 0 to disable idle eviction.
    pub idle_eviction_hours: u32,
    /// Total memory budget for the hot tier in megabytes (default: 256).
    /// Oldest samples from the largest buffers are evicted when exceeded.
    pub hot_memory_budget_mb: usize,
    /// Minimum samples to keep per buffer even under memory pressure (default: 60).
    pub min_samples_per_buffer: usize,
    /// Memory pressure threshold (0.0–1.0) that triggers early eviction (default: 0.8).
    pub flush_pressure_threshold: f32,
    /// Tolerance for out-of-order samples in nanoseconds (default: 5 seconds).
    /// Samples within this window of the latest timestamp are sorted-inserted;
    /// samples beyond the window are dropped.
    pub out_of_order_tolerance_nanos: i64,
    /// Hours before a date directory is eligible for compaction (default: 24).
    /// Directories younger than this are still being written to by flush.
    pub compact_after_hours: u32,
    /// Gorilla compression window in minutes (default: 30).
    /// Samples within the same window are collected in a raw Vec; when a new
    /// sample falls into a different window the active window is sealed into
    /// a compressed TsBlock.
    pub gorilla_window_minutes: u32,
    /// Warm tier configuration. `None` = disabled (default).
    /// Stores downsampled aggregates (min/max/sum/count) for trend queries.
    pub warm_tier: Option<WarmTierConfig>,
    /// Cloud export configuration for offloading cold-tier Parquet to object storage.
    /// Requires the `cloud-storage` feature flag.
    pub cloud: CloudExportConfig,
}

/// Configuration for the warm tier downsampled aggregates.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WarmTierConfig {
    /// Downsample window interval in seconds (default: 60).
    pub downsample_interval_secs: u32,
    /// How long to keep warm tier data in hours (default: 24).
    pub retention_hours: u32,
    /// Whether to allocate DDSketch accumulators for streaming quantile
    /// estimation (p50/p90/p95/p99). Disabling saves memory on constrained
    /// edge devices where quantiles are not needed (default: true).
    pub ddsketch_enabled: bool,
    /// Optional hourly warm tier for month-scale dashboard queries.
    pub hourly: Option<HourlyWarmConfig>,
}

/// Configuration for the hourly warm tier level.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct HourlyWarmConfig {
    /// Whether the hourly warm tier is enabled (default: false).
    pub enabled: bool,
    /// How long to keep hourly aggregates in days (default: 30).
    pub retention_days: u32,
}

/// Configuration for cloud export of cold-tier Parquet files.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct CloudExportConfig {
    /// Cloud storage destination URL (e.g., "s3://bucket/prefix/").
    /// Supports s3://, gs://, az:// schemes via the object_store crate.
    /// None = cloud export disabled.
    pub url: Option<String>,
    /// Node identifier for Hive-style partitioning (node={node_id}/date=.../file.parquet).
    /// Defaults to the system hostname if not set.
    pub node_id: Option<String>,
}

impl Default for TsConfig {
    fn default() -> Self {
        Self {
            hot_retention_hours: 24,
            medium_retention_days: 7,
            flush_interval_minutes: 15,
            max_samples_per_buffer: 86_400,
            idle_eviction_hours: 48,
            hot_memory_budget_mb: 256,
            min_samples_per_buffer: 60,
            flush_pressure_threshold: 0.8,
            out_of_order_tolerance_nanos: 5_000_000_000, // 5 seconds
            compact_after_hours: 24,
            gorilla_window_minutes: 30,
            warm_tier: None,
            cloud: CloudExportConfig::default(),
        }
    }
}

impl Default for WarmTierConfig {
    fn default() -> Self {
        Self {
            downsample_interval_secs: 60,
            retention_hours: 24,
            ddsketch_enabled: true,
            hourly: None,
        }
    }
}

impl Default for HourlyWarmConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            retention_days: 30,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = TsConfig::default();
        assert_eq!(config.hot_retention_hours, 24);
        assert_eq!(config.medium_retention_days, 7);
        assert_eq!(config.flush_interval_minutes, 15);
    }

    #[test]
    fn cloud_export_config_defaults() {
        let config = CloudExportConfig::default();
        assert!(config.url.is_none());
        assert!(config.node_id.is_none());
    }

    #[test]
    fn ts_config_with_cloud_section() {
        let toml_str = r#"
            medium_retention_days = 14
            [cloud]
            url = "s3://my-bucket/data/"
            node_id = "edge-01"
        "#;
        let config: TsConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.medium_retention_days, 14);
        assert_eq!(config.cloud.url.as_deref(), Some("s3://my-bucket/data/"));
        assert_eq!(config.cloud.node_id.as_deref(), Some("edge-01"));
    }
}
