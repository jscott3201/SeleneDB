//! Warm tier: downsampled time-series aggregates for dashboard queries.
//!
//! Stores min/max/sum/count per tumbling window (default: 1 minute) for
//! each entity/property pair. Much smaller than full-resolution hot data --
//! 1,440 aggregates for 24h at 1-minute windows vs 86,400 raw samples.
//!
//! Integrated with the hot tier: each `append()` to the hot tier also
//! feeds `WarmTier::record()` to update the current window accumulator.
//!
//! DDSketch accumulators provide streaming quantile estimates (p50/p90/p95/p99)
//! with bounded relative error, finalized alongside each window aggregate.

use std::collections::VecDeque;
use std::fmt;

use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use selene_core::{IStr, NodeId};
use sketches_ddsketch::{Config as DDSketchConfig, DDSketch};

use crate::config::WarmTierConfig;
use crate::hot::{TimeSample, TsKey};

const SHARD_COUNT: usize = 16;
const SHARD_MASK: usize = SHARD_COUNT - 1;

/// A single downsampled aggregate for one tumbling window.
#[derive(Debug, Clone, Copy)]
pub struct WarmAggregate {
    pub window_start_nanos: i64,
    pub min: f64,
    pub max: f64,
    pub sum: f64,
    pub count: u32,
    /// Population standard deviation for this window. None if count < 2.
    pub stddev: Option<f64>,
    /// Pre-computed quantile values: (p50, p90, p95, p99). None if the sketch
    /// was not fed directly (e.g. hourly tier merge) or count < 2.
    pub quantiles: Option<(f64, f64, f64, f64)>,
}

impl WarmAggregate {
    /// Compute the average value in this window.
    pub fn avg(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / f64::from(self.count)
        }
    }
}

/// Accumulator for the current (incomplete) window.
///
/// DDSketch does not implement Debug, so we provide a manual impl.
struct WindowAccumulator {
    window_start_nanos: i64,
    min: f64,
    max: f64,
    sum: f64,
    count: u32,
    /// Welford's online algorithm state for incremental stddev.
    welford_mean: f64,
    welford_m2: f64,
    /// True when `record()` was called (individual samples fed to Welford).
    /// False when only `record_aggregate()` was used (merged data, Welford not fed).
    welford_fed: bool,
    /// DDSketch for streaming quantile estimation. None when ddsketch_enabled
    /// is false, saving ~2 KB per accumulator on constrained devices.
    sketch: Option<DDSketch>,
}

impl fmt::Debug for WindowAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WindowAccumulator")
            .field("window_start_nanos", &self.window_start_nanos)
            .field("min", &self.min)
            .field("max", &self.max)
            .field("sum", &self.sum)
            .field("count", &self.count)
            .field("welford_mean", &self.welford_mean)
            .field("welford_m2", &self.welford_m2)
            .field("welford_fed", &self.welford_fed)
            .field("sketch_count", &self.sketch.as_ref().map(|s| s.count()))
            .finish()
    }
}

impl WindowAccumulator {
    fn new(window_start: i64, ddsketch_enabled: bool) -> Self {
        Self {
            window_start_nanos: window_start,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            sum: 0.0,
            count: 0,
            welford_mean: 0.0,
            welford_m2: 0.0,
            welford_fed: false,
            sketch: if ddsketch_enabled {
                Some(DDSketch::new(DDSketchConfig::defaults()))
            } else {
                None
            },
        }
    }

    fn record(&mut self, value: f64) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.sum += value;
        self.count += 1;
        self.welford_fed = true;

        // Welford's online algorithm for incremental mean and variance
        let delta = value - self.welford_mean;
        self.welford_mean += delta / f64::from(self.count);
        let delta2 = value - self.welford_mean;
        self.welford_m2 += delta * delta2;

        // DDSketch for streaming quantiles (when enabled)
        if let Some(sketch) = &mut self.sketch {
            sketch.add(value);
        }
    }

    fn finalize(&self) -> WarmAggregate {
        let stddev = if self.welford_fed && self.count >= 2 {
            Some((self.welford_m2 / f64::from(self.count)).sqrt())
        } else {
            None
        };
        // Use sketch's own count to avoid producing bogus quantiles from
        // hourly-tier accumulators where count was manually set but sketch
        // was never fed.
        let quantiles = self.sketch.as_ref().and_then(|sketch| {
            if sketch.count() >= 2 {
                let q = |p: f64| sketch.quantile(p).unwrap_or(None).unwrap_or(0.0);
                Some((q(0.50), q(0.90), q(0.95), q(0.99)))
            } else {
                None
            }
        });
        WarmAggregate {
            window_start_nanos: self.window_start_nanos,
            min: if self.count > 0 { self.min } else { 0.0 },
            max: if self.count > 0 { self.max } else { 0.0 },
            sum: self.sum,
            count: self.count,
            stddev,
            quantiles,
        }
    }
}

/// Per-key warm buffer: completed aggregates + current window accumulator.
struct WarmBuffer {
    aggregates: VecDeque<WarmAggregate>,
    current: WindowAccumulator,
}

/// Warm tier -- stores downsampled aggregates for trend queries.
///
/// Thread-safe via sharded `RwLock`s (same sharding as the hot tier).
pub struct WarmTier {
    shards: Vec<RwLock<FxHashMap<TsKey, WarmBuffer>>>,
    config: WarmTierConfig,
    /// Window interval in nanoseconds.
    interval_nanos: i64,
    /// Retention in nanoseconds.
    retention_nanos: i64,
    /// Whether DDSketch accumulators are allocated for streaming quantiles.
    ddsketch_enabled: bool,
    /// Optional hourly tier fed from finalized minute-level windows.
    hourly: Option<Box<WarmTier>>,
}

impl WarmTier {
    pub fn new(config: WarmTierConfig) -> Self {
        let interval_nanos = i64::from(config.downsample_interval_secs.max(1)) * 1_000_000_000;
        let retention_nanos = i64::from(config.retention_hours) * 3_600 * 1_000_000_000;
        let ddsketch_enabled = config.ddsketch_enabled;
        let shards = (0..SHARD_COUNT)
            .map(|_| RwLock::new(FxHashMap::default()))
            .collect();

        // Hourly tier (optional hierarchical level)
        let hourly = config.hourly.as_ref().and_then(|hc| {
            if hc.enabled {
                Some(Box::new(WarmTier {
                    shards: (0..SHARD_COUNT)
                        .map(|_| RwLock::new(FxHashMap::default()))
                        .collect(),
                    config: WarmTierConfig {
                        downsample_interval_secs: 3600,
                        retention_hours: hc.retention_days * 24,
                        ddsketch_enabled,
                        hourly: None, // no recursion
                    },
                    interval_nanos: 3_600 * 1_000_000_000,
                    retention_nanos: i64::from(hc.retention_days) * 86_400 * 1_000_000_000,
                    ddsketch_enabled,
                    hourly: None,
                }))
            } else {
                None
            }
        });

        Self {
            shards,
            config,
            interval_nanos,
            retention_nanos,
            ddsketch_enabled,
            hourly,
        }
    }

    /// Align a timestamp to the start of its window.
    fn window_start(&self, timestamp_nanos: i64) -> i64 {
        timestamp_nanos.div_euclid(self.interval_nanos) * self.interval_nanos
    }

    fn shard_for(&self, node_id: NodeId) -> &RwLock<FxHashMap<TsKey, WarmBuffer>> {
        &self.shards[node_id.0 as usize & SHARD_MASK]
    }

    /// Record a sample into the warm tier.
    ///
    /// Samples in the current window update the accumulator. Samples in a
    /// new window finalize the current aggregate and start a new one.
    pub fn record(&self, node_id: NodeId, property: IStr, sample: TimeSample) {
        let key = TsKey { node_id, property };
        let ws = self.window_start(sample.timestamp_nanos);
        let mut shard = self.shard_for(node_id).write();

        let dds = self.ddsketch_enabled;
        let buffer = shard.entry(key).or_insert_with(|| WarmBuffer {
            aggregates: VecDeque::new(),
            current: WindowAccumulator::new(ws, dds),
        });

        if ws < buffer.current.window_start_nanos {
            return; // out of order: too late for its window
        }

        if ws != buffer.current.window_start_nanos {
            // Finalize the current window and start a new one
            if buffer.current.count > 0 {
                let finalized = buffer.current.finalize();
                buffer.aggregates.push_back(finalized);

                // Feed hierarchical hourly tier with finalized aggregate
                if let Some(hourly) = &self.hourly {
                    hourly.record_aggregate(node_id, property, finalized);
                }
            }
            buffer.current = WindowAccumulator::new(ws, dds);
        }

        buffer.current.record(sample.value);
    }

    /// Record a pre-computed aggregate into this tier (hierarchical feed).
    /// Merges using min-of-mins, max-of-maxes, sum-of-sums, sum-of-counts.
    /// Sketch data is not propagated (quantiles are None on merged tiers).
    fn record_aggregate(&self, node_id: NodeId, property: IStr, agg: WarmAggregate) {
        let key = TsKey { node_id, property };
        let ws = self.window_start(agg.window_start_nanos);
        let dds = self.ddsketch_enabled;
        let mut shard = self.shard_for(node_id).write();

        let buffer = shard.entry(key).or_insert_with(|| WarmBuffer {
            aggregates: VecDeque::new(),
            current: WindowAccumulator::new(ws, dds),
        });

        if ws < buffer.current.window_start_nanos {
            return; // out of order
        }

        if ws != buffer.current.window_start_nanos {
            if buffer.current.count > 0 {
                buffer.aggregates.push_back(buffer.current.finalize());
            }
            buffer.current = WindowAccumulator::new(ws, dds);
        }

        // Merge: min-of-mins, max-of-maxes, sum-of-sums, sum-of-counts
        // Note: sketch is NOT fed here, so finalize() will produce quantiles: None
        buffer.current.min = buffer.current.min.min(agg.min);
        buffer.current.max = buffer.current.max.max(agg.max);
        buffer.current.sum += agg.sum;
        buffer.current.count += agg.count;
    }

    /// Query aggregates for a specific entity/property within a time range.
    pub fn range(
        &self,
        node_id: NodeId,
        property: &str,
        start_nanos: i64,
        end_nanos: i64,
    ) -> Vec<WarmAggregate> {
        let key = TsKey {
            node_id,
            property: IStr::new(property),
        };
        let shard = self.shard_for(node_id).read();
        let Some(buffer) = shard.get(&key) else {
            return vec![];
        };

        let mut results: Vec<WarmAggregate> = buffer
            .aggregates
            .iter()
            .filter(|a| a.window_start_nanos >= start_nanos && a.window_start_nanos <= end_nanos)
            .copied()
            .collect();

        // Include current (incomplete) window if it overlaps the range
        if buffer.current.count > 0
            && buffer.current.window_start_nanos >= start_nanos
            && buffer.current.window_start_nanos <= end_nanos
        {
            results.push(buffer.current.finalize());
        }

        results
    }

    /// Get all aggregates for a specific entity/property.
    pub fn all_aggregates(&self, node_id: NodeId, property: &str) -> Vec<WarmAggregate> {
        let key = TsKey {
            node_id,
            property: IStr::new(property),
        };
        let shard = self.shard_for(node_id).read();
        let Some(buffer) = shard.get(&key) else {
            return vec![];
        };

        let mut results: Vec<WarmAggregate> = buffer.aggregates.iter().copied().collect();
        if buffer.current.count > 0 {
            results.push(buffer.current.finalize());
        }
        results
    }

    /// List all active keys across all shards.
    pub fn all_keys(&self) -> Vec<TsKey> {
        let mut keys = Vec::new();
        for shard in &self.shards {
            keys.extend(shard.read().keys().cloned());
        }
        keys
    }

    /// Drain aggregates older than the retention cutoff.
    pub fn drain_expired(&self, cutoff_nanos: i64) -> usize {
        let mut total_drained = 0;
        for shard in &self.shards {
            let mut guard = shard.write();
            for buffer in guard.values_mut() {
                while let Some(front) = buffer.aggregates.front() {
                    if front.window_start_nanos < cutoff_nanos {
                        buffer.aggregates.pop_front();
                        total_drained += 1;
                    } else {
                        break;
                    }
                }
            }
            // Clean up empty buffers
            guard.retain(|_, b| !b.aggregates.is_empty() || b.current.count > 0);
        }
        total_drained
    }

    /// Total number of completed aggregates across all buffers.
    pub fn aggregate_count(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.read().values().map(|b| b.aggregates.len()).sum::<usize>())
            .sum()
    }

    /// Number of active buffers.
    pub fn buffer_count(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    /// The downsample interval in seconds.
    pub fn interval_secs(&self) -> u32 {
        self.config.downsample_interval_secs
    }

    /// Retention in nanoseconds.
    pub fn retention_nanos(&self) -> i64 {
        self.retention_nanos
    }

    /// Access the hourly tier (if configured).
    pub fn hourly_tier(&self) -> Option<&WarmTier> {
        self.hourly.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn warm() -> WarmTier {
        WarmTier::new(WarmTierConfig {
            downsample_interval_secs: 60, // 1-minute windows
            retention_hours: 24,
            ddsketch_enabled: true,
            hourly: None,
        })
    }

    fn sample(ts: i64, val: f64) -> TimeSample {
        TimeSample {
            timestamp_nanos: ts,
            value: val,
        }
    }

    // 1 minute in nanos
    const MIN_NS: i64 = 60_000_000_000;

    #[test]
    fn single_sample_in_current_window() {
        let w = warm();
        w.record(
            NodeId(1),
            IStr::new("temp"),
            sample(1000 * MIN_NS + 5, 72.0),
        );

        let aggs = w.all_aggregates(NodeId(1), "temp");
        assert_eq!(aggs.len(), 1); // current window finalized
        assert_eq!(aggs[0].count, 1);
        assert_eq!(aggs[0].min, 72.0);
        assert_eq!(aggs[0].max, 72.0);
    }

    #[test]
    fn multiple_samples_same_window() {
        let w = warm();
        let base = 1000 * MIN_NS;
        w.record(NodeId(1), IStr::new("temp"), sample(base + 1, 70.0));
        w.record(NodeId(1), IStr::new("temp"), sample(base + 2, 75.0));
        w.record(NodeId(1), IStr::new("temp"), sample(base + 3, 72.0));

        let aggs = w.all_aggregates(NodeId(1), "temp");
        assert_eq!(aggs.len(), 1);
        assert_eq!(aggs[0].count, 3);
        assert_eq!(aggs[0].min, 70.0);
        assert_eq!(aggs[0].max, 75.0);
        assert!((aggs[0].avg() - 72.333).abs() < 0.01);
    }

    #[test]
    fn window_boundary_finalizes() {
        let w = warm();
        let base = 1000 * MIN_NS;

        // Window 1: minute 1000
        w.record(NodeId(1), IStr::new("temp"), sample(base + 1, 70.0));
        w.record(NodeId(1), IStr::new("temp"), sample(base + 2, 72.0));

        // Window 2: minute 1001 -- should finalize window 1
        w.record(
            NodeId(1),
            IStr::new("temp"),
            sample(base + MIN_NS + 1, 80.0),
        );

        let aggs = w.all_aggregates(NodeId(1), "temp");
        assert_eq!(aggs.len(), 2); // window 1 finalized + window 2 current
        assert_eq!(aggs[0].count, 2);
        assert_eq!(aggs[0].min, 70.0);
        assert_eq!(aggs[1].count, 1);
        assert_eq!(aggs[1].min, 80.0);
    }

    #[test]
    fn range_query() {
        let w = warm();
        let base = 1000 * MIN_NS;

        // Write 5 windows
        for i in 0..5 {
            w.record(
                NodeId(1),
                IStr::new("temp"),
                sample(base + i * MIN_NS + 1, 70.0 + i as f64),
            );
        }

        // Query windows 1-3
        let start = base + MIN_NS;
        let end = base + 3 * MIN_NS;
        let aggs = w.range(NodeId(1), "temp", start, end);
        assert_eq!(aggs.len(), 3);
    }

    #[test]
    fn empty_range() {
        let w = warm();
        let aggs = w.range(NodeId(99), "nonexistent", 0, i64::MAX);
        assert!(aggs.is_empty());
    }

    #[test]
    fn drain_expired() {
        let w = warm();
        let base = 1000 * MIN_NS;

        // Write 10 windows
        for i in 0..10 {
            w.record(
                NodeId(1),
                IStr::new("temp"),
                sample(base + i * MIN_NS + 1, 70.0),
            );
        }

        // Drain windows before minute 1005
        let cutoff = base + 5 * MIN_NS;
        let drained = w.drain_expired(cutoff);
        assert!(drained > 0);

        let aggs = w.all_aggregates(NodeId(1), "temp");
        for a in &aggs {
            assert!(a.window_start_nanos >= cutoff);
        }
    }

    #[test]
    fn multiple_entities() {
        let w = warm();
        let base = 1000 * MIN_NS;

        w.record(NodeId(1), IStr::new("temp"), sample(base + 1, 70.0));
        w.record(NodeId(2), IStr::new("humidity"), sample(base + 1, 45.0));

        assert_eq!(w.buffer_count(), 2);
        assert_eq!(w.all_aggregates(NodeId(1), "temp").len(), 1);
        assert_eq!(w.all_aggregates(NodeId(2), "humidity").len(), 1);
    }

    #[test]
    fn warm_tier_disabled_by_default() {
        let config = crate::config::TsConfig::default();
        assert!(config.warm_tier.is_none());
    }

    #[test]
    fn aggregate_avg() {
        let agg = WarmAggregate {
            window_start_nanos: 0,
            min: 10.0,
            max: 30.0,
            sum: 60.0,
            count: 3,
            stddev: None,
            quantiles: None,
        };
        assert!((agg.avg() - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn aggregate_avg_empty() {
        let agg = WarmAggregate {
            window_start_nanos: 0,
            min: 0.0,
            max: 0.0,
            sum: 0.0,
            count: 0,
            stddev: None,
            quantiles: None,
        };
        assert_eq!(agg.avg(), 0.0);
    }

    #[test]
    fn stddev_accumulator() {
        let mut acc = WindowAccumulator::new(0, true);
        // Values: 2, 4, 4, 4, 5, 5, 7, 9
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            acc.record(v);
        }
        let agg = acc.finalize();
        assert_eq!(agg.count, 8);
        // Population stddev = 2.0
        let stddev = agg.stddev.unwrap();
        assert!((stddev - 2.0).abs() < 0.001, "expected ~2.0, got {stddev}");
    }

    #[test]
    fn stddev_none_for_single_sample() {
        let mut acc = WindowAccumulator::new(0, true);
        acc.record(42.0);
        let agg = acc.finalize();
        assert!(agg.stddev.is_none());
    }

    #[test]
    fn ddsketch_quantiles() {
        let w = warm();
        let base = 1000 * MIN_NS;
        // Record 100 values in the same window
        for i in 1..=100 {
            w.record(NodeId(1), IStr::new("temp"), sample(base + i, i as f64));
        }

        let aggs = w.all_aggregates(NodeId(1), "temp");
        assert_eq!(aggs.len(), 1);
        let q = aggs[0].quantiles.unwrap();
        // p50 ~ 50, p90 ~ 90, p95 ~ 95, p99 ~ 99
        // DDSketch has bounded relative error, allow ~5% tolerance
        assert!((q.0 - 50.0).abs() < 5.0, "p50 expected ~50, got {}", q.0);
        assert!((q.1 - 90.0).abs() < 5.0, "p90 expected ~90, got {}", q.1);
        assert!((q.2 - 95.0).abs() < 5.0, "p95 expected ~95, got {}", q.2);
        assert!((q.3 - 99.0).abs() < 5.0, "p99 expected ~99, got {}", q.3);
    }

    #[test]
    fn quantiles_none_for_single_sample() {
        let w = warm();
        let base = 1000 * MIN_NS;
        w.record(NodeId(1), IStr::new("temp"), sample(base + 1, 42.0));
        let aggs = w.all_aggregates(NodeId(1), "temp");
        assert!(aggs[0].quantiles.is_none());
    }

    #[test]
    fn ddsketch_disabled_produces_no_quantiles_but_stddev_works() {
        let w = WarmTier::new(WarmTierConfig {
            downsample_interval_secs: 60,
            retention_hours: 24,
            ddsketch_enabled: false,
            hourly: None,
        });
        let base = 1000 * MIN_NS;
        // Record enough values for both stddev and quantiles to be computable
        for i in 1..=100 {
            w.record(NodeId(1), IStr::new("temp"), sample(base + i, i as f64));
        }

        let aggs = w.all_aggregates(NodeId(1), "temp");
        assert_eq!(aggs.len(), 1);
        // Quantiles must be None (DDSketch disabled)
        assert!(
            aggs[0].quantiles.is_none(),
            "quantiles should be None when ddsketch_enabled=false"
        );
        // Stddev must still work (Welford is independent of DDSketch)
        let stddev = aggs[0].stddev.unwrap();
        assert!(stddev > 0.0, "stddev should be positive, got {stddev}");
    }

    #[test]
    fn merged_aggregate_stddev_is_none() {
        let w = warm();
        let base = 1000 * MIN_NS;
        let prop = IStr::new("temperature");

        // Feed via record_aggregate (simulating hourly merge from minute tier).
        // Welford state is not fed during merge, so stddev must be None.
        let agg = WarmAggregate {
            window_start_nanos: base,
            min: 10.0,
            max: 30.0,
            sum: 200.0,
            count: 10,
            stddev: Some(5.0),
            quantiles: None,
        };
        w.record_aggregate(NodeId(1), prop, agg);

        let results = w.all_aggregates(NodeId(1), "temperature");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].count, 10);
        assert_eq!(results[0].min, 10.0);
        assert_eq!(results[0].max, 30.0);
        assert!(
            results[0].stddev.is_none(),
            "merged aggregate stddev should be None, got {:?}",
            results[0].stddev
        );
    }

    #[test]
    fn individual_samples_still_produce_stddev() {
        // Ensure the fix did not break the normal (non-merged) path
        let mut acc = WindowAccumulator::new(0, false);
        for v in [10.0, 20.0, 30.0] {
            acc.record(v);
        }
        let agg = acc.finalize();
        assert!(
            agg.stddev.is_some(),
            "individual samples should produce stddev"
        );
    }
}
