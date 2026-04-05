//! Hot tier: compressed in-memory storage for recent time-series data.
//!
//! Each `(NodeId, property_name)` pair has a `TsBuffer` holding the last
//! `hot_retention_hours` of data. Samples accumulate in an active window
//! (`Vec<TimeSample>`); when a sample falls into a new time window the
//! active window is sealed into a compressed `TsBlock`. Range queries
//! skip sealed blocks by timestamp bounds and scan active samples raw.
//!
//! Storage is **sharded by entity_id** (16 shards) so concurrent writers to
//! different entities don't contend on a single lock.
//!
//! Memory is tracked via an `AtomicUsize` counter. When usage exceeds the
//! configured budget, oldest sealed blocks from the largest buffers are
//! evicted (respecting `min_samples_per_buffer`).

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use selene_core::{IStr, NodeId};

use crate::config::TsConfig;
use crate::encoding::TsBlock;
use crate::warm::WarmTier;
use selene_core::ValueEncoding;

/// Number of shards for the hot tier buffer map.
const SHARD_COUNT: usize = 16;
const SHARD_MASK: usize = SHARD_COUNT - 1;

/// Maximum sealed blocks drained per shard lock acquisition in `drain_before`.
/// Bounding the hold time lets concurrent writers make progress between chunks.
pub(crate) const DRAIN_CHUNK_SIZE: usize = 8;

/// Bytes per `TimeSample` (timestamp_nanos: i64 + value: f64).
pub(crate) const SAMPLE_BYTES: usize = 16;
/// Estimated overhead per buffer (TsBuffer metadata + HashMap entry + TsKey).
pub(crate) const BUFFER_OVERHEAD: usize = 128;

/// A single time-series data point.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TimeSample {
    pub timestamp_nanos: i64,
    pub value: f64,
}

/// Key for a time-series buffer -- identifies which sensor property.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TsKey {
    pub node_id: NodeId,
    pub property: IStr,
}

/// Active (uncompressed) window that collects samples until sealed.
pub(crate) struct ActiveWindow {
    /// Start of this window (aligned to `window_nanos` boundaries).
    pub(crate) window_start_nanos: i64,
    /// Raw samples in this window, sorted by timestamp.
    pub(crate) samples: Vec<TimeSample>,
}

/// Per-key buffer: sealed blocks plus an active collection window.
pub(crate) struct TsBuffer {
    /// Completed, compressed blocks (oldest first).
    pub(crate) sealed_blocks: VecDeque<TsBlock>,
    /// Currently accumulating window.
    pub(crate) active: ActiveWindow,
    /// The most recently appended sample (for `latest()` lookups).
    /// Not updated by dedup -- only set when a sample is actually stored.
    pub(crate) last_sample: Option<TimeSample>,
    /// Last stored value for deduplication. When a new sample has the
    /// same value as the previous, skip storing it (saves 50%+ for
    /// status/binary sensors that report unchanged values).
    pub(crate) last_stored_value: Option<f64>,
    /// Total bytes used by sealed block compressed data.
    pub(crate) compressed_bytes: usize,
    /// Timestamp (nanos) of last write to this buffer (including dedup touches).
    pub(crate) last_write_nanos: i64,
    /// Value encoding strategy for this buffer's sealed blocks.
    pub(crate) encoding: ValueEncoding,
}

impl TsBuffer {
    fn new(encoding: ValueEncoding) -> Self {
        Self {
            sealed_blocks: VecDeque::new(),
            active: ActiveWindow {
                window_start_nanos: 0,
                samples: Vec::new(),
            },
            last_sample: None,
            last_stored_value: None,
            compressed_bytes: 0,
            last_write_nanos: 0,
            encoding,
        }
    }

    /// Total number of samples (sealed + active).
    pub(crate) fn sample_count(&self) -> usize {
        let sealed: usize = self
            .sealed_blocks
            .iter()
            .map(|b| b.sample_count as usize)
            .sum();
        sealed + self.active.samples.len()
    }

    /// Total memory footprint of this buffer (excluding BUFFER_OVERHEAD).
    pub(crate) fn data_bytes(&self) -> usize {
        self.compressed_bytes + self.active.samples.len() * SAMPLE_BYTES
    }

    /// Decompress all samples from sealed blocks and active window, in order.
    fn all_samples(&self) -> Vec<TimeSample> {
        let mut result = Vec::with_capacity(self.sample_count());
        for block in &self.sealed_blocks {
            result.extend(block.decode_all());
        }
        result.extend_from_slice(&self.active.samples);
        result
    }
}

/// Per-shard data: buffers + eviction min-heap.
pub(crate) struct ShardInner {
    pub(crate) buffers: FxHashMap<TsKey, TsBuffer>,
    pub(crate) eviction_heap: BinaryHeap<Reverse<(i64, TsKey)>>,
}

impl ShardInner {
    fn new() -> Self {
        Self {
            buffers: FxHashMap::default(),
            eviction_heap: BinaryHeap::new(),
        }
    }
}

/// In-memory hot tier for recent time-series data.
///
/// Thread-safe via sharded `RwLock`s -- 16 shards keyed by `node_id.0 & 0xF`.
/// Concurrent writes to different entities contend on separate locks.
///
/// Memory usage is tracked approximately via an atomic counter. When usage
/// exceeds the configured budget, oldest sealed blocks from the largest
/// buffers are evicted.
pub struct HotTier {
    pub(crate) shards: Vec<RwLock<ShardInner>>,
    pub(crate) config: TsConfig,
    /// Total memory budget in bytes.
    pub(crate) memory_budget: usize,
    /// Pressure threshold in bytes (budget * flush_pressure_threshold).
    /// Eviction triggers when memory_used exceeds this.
    pub(crate) pressure_budget: usize,
    /// Current estimated memory usage in bytes (atomic for lock-free reads).
    pub(crate) memory_used: AtomicUsize,
    /// Optional warm tier for downsampled aggregates.
    pub(crate) warm: Option<WarmTier>,
    /// Count of samples dropped due to exceeding out-of-order tolerance.
    out_of_order_dropped: AtomicU64,
    /// Gorilla window size in nanoseconds.
    window_nanos: i64,
    /// Per-(node, property) encoding hints from schema definitions.
    /// Buffers created for keys with a hint use that encoding; others default to Gorilla.
    encoding_hints: RwLock<FxHashMap<TsKey, ValueEncoding>>,
}

impl HotTier {
    /// Create a new hot tier with the given configuration.
    pub fn new(config: TsConfig) -> Self {
        let shards = (0..SHARD_COUNT)
            .map(|_| RwLock::new(ShardInner::new()))
            .collect();
        let memory_budget = config.hot_memory_budget_mb * 1024 * 1024;
        let pressure_budget =
            (memory_budget as f64 * f64::from(config.flush_pressure_threshold)) as usize;
        let warm = config
            .warm_tier
            .as_ref()
            .map(|wc| WarmTier::new(wc.clone()));
        let window_nanos = i64::from(config.gorilla_window_minutes) * 60 * 1_000_000_000;
        Self {
            shards,
            config,
            memory_budget,
            pressure_budget,
            memory_used: AtomicUsize::new(0),
            warm,
            out_of_order_dropped: AtomicU64::new(0),
            window_nanos,
            encoding_hints: RwLock::new(FxHashMap::default()),
        }
    }

    /// Get the shard for a given node ID.
    fn shard_for(&self, node_id: NodeId) -> &RwLock<ShardInner> {
        &self.shards[node_id.0 as usize & SHARD_MASK]
    }

    /// Set the encoding hint for a (node, property) pair.
    /// Called by the server layer when schema definitions are loaded or changed.
    pub fn set_encoding_hint(&self, node_id: NodeId, property: &str, encoding: ValueEncoding) {
        let key = TsKey {
            node_id,
            property: IStr::new(property),
        };
        self.encoding_hints.write().insert(key, encoding);
    }

    /// Look up the encoding for a key, defaulting to Gorilla.
    fn encoding_for(&self, key: &TsKey) -> ValueEncoding {
        self.encoding_hints
            .read()
            .get(key)
            .copied()
            .unwrap_or(ValueEncoding::Gorilla)
    }

    /// Subtract from memory_used with saturation to prevent underflow.
    pub(crate) fn saturating_sub_memory(&self, amount: usize) {
        let mut current = self.memory_used.load(Ordering::Relaxed);
        loop {
            let new = current.saturating_sub(amount);
            match self.memory_used.compare_exchange_weak(
                current,
                new,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Seal the active window of a buffer into a TsBlock if it has samples.
    /// Returns the change in memory: (freed_raw_bytes, added_compressed_bytes).
    fn seal_active(buf: &mut TsBuffer) -> (usize, usize) {
        if buf.active.samples.is_empty() {
            return (0, 0);
        }
        let raw_bytes = buf.active.samples.len() * SAMPLE_BYTES;
        let block = TsBlock::encode(&buf.active.samples, buf.encoding);
        let compressed = block.compressed_size();
        buf.compressed_bytes += compressed;
        buf.sealed_blocks.push_back(block);
        buf.active.samples.clear();
        (raw_bytes, compressed)
    }

    /// Append a sample to the buffer, enforcing capacity limit and ordering.
    ///
    /// Returns `(is_new_buffer, memory_delta, was_dropped, was_deduped)` for
    /// the caller to update memory counter and out-of-order metrics.
    /// `memory_delta` is a signed value as an isize.
    fn append_to_buffer(
        shard: &mut ShardInner,
        key: TsKey,
        sample: TimeSample,
        max_samples: usize,
        tolerance_nanos: i64,
        window_nanos: i64,
        encoding: ValueEncoding,
    ) -> (bool, isize, bool, bool) {
        let is_new = !shard.buffers.contains_key(&key);
        let buf = shard
            .buffers
            .entry(key.clone())
            .or_insert_with(|| TsBuffer::new(encoding));

        // Value deduplication: skip storing if value is bitwise-identical to the last stored.
        // Skip the heap push for dedup hits -- the buffer's existing heap entry
        // (from the original write) is sufficient since lazy deletion handles
        // staleness, and last_write_nanos is updated for eviction freshness.
        if let Some(last_val) = buf.last_stored_value
            && sample.value.to_bits() == last_val.to_bits()
        {
            buf.last_write_nanos = buf.last_write_nanos.max(sample.timestamp_nanos);
            buf.last_sample = Some(sample);
            return (is_new, 0, false, true);
        }

        // Check ordering against the buffer's latest stored timestamp
        let last_ts = buf
            .active
            .samples
            .last()
            .map(|s| s.timestamp_nanos)
            .or_else(|| buf.sealed_blocks.back().map(|b| b.end_nanos))
            .unwrap_or(i64::MIN);

        if sample.timestamp_nanos < last_ts {
            // Out of order: check tolerance
            let gap = last_ts - sample.timestamp_nanos;
            if gap > tolerance_nanos {
                // Beyond tolerance: drop
                return (is_new, 0, true, false);
            }
            // Within tolerance: sorted insert into active window
            let pos = buf
                .active
                .samples
                .partition_point(|s| s.timestamp_nanos <= sample.timestamp_nanos);
            buf.active.samples.insert(pos, sample);
            buf.last_stored_value = Some(sample.value);
            buf.last_sample = Some(*buf.active.samples.last().unwrap_or(&sample));
            buf.last_write_nanos = buf.last_write_nanos.max(sample.timestamp_nanos);
            shard
                .eviction_heap
                .push(Reverse((buf.last_write_nanos, key)));
            return (is_new, SAMPLE_BYTES as isize, false, false);
        }

        // In-order: check if we need to seal the active window
        let mut mem_delta: isize = 0;

        if window_nanos > 0 && !buf.active.samples.is_empty() {
            let sample_window = if sample.timestamp_nanos >= 0 {
                sample.timestamp_nanos / window_nanos
            } else {
                (sample.timestamp_nanos - window_nanos + 1) / window_nanos
            };
            let active_window = if buf.active.window_start_nanos >= 0 {
                buf.active.window_start_nanos / window_nanos
            } else {
                (buf.active.window_start_nanos - window_nanos + 1) / window_nanos
            };

            if sample_window != active_window {
                // Seal the active window
                let (freed_raw, added_compressed) = Self::seal_active(buf);
                mem_delta -= freed_raw as isize;
                mem_delta += added_compressed as isize;
            }
        }

        // Update active window start if empty
        if buf.active.samples.is_empty() {
            buf.active.window_start_nanos = if window_nanos > 0 {
                if sample.timestamp_nanos >= 0 {
                    (sample.timestamp_nanos / window_nanos) * window_nanos
                } else {
                    ((sample.timestamp_nanos - window_nanos + 1) / window_nanos) * window_nanos
                }
            } else {
                0
            };
        }

        buf.active.samples.push(sample);
        buf.last_stored_value = Some(sample.value);
        buf.last_sample = Some(sample);
        buf.last_write_nanos = buf.last_write_nanos.max(sample.timestamp_nanos);
        mem_delta += SAMPLE_BYTES as isize;

        // Enforce max_samples by evicting oldest sealed blocks
        if max_samples > 0 && buf.sample_count() > max_samples {
            let excess = buf.sample_count() - max_samples;
            let mut removed = 0;
            while removed < excess && !buf.sealed_blocks.is_empty() {
                let block = buf.sealed_blocks.pop_front().unwrap();
                let block_samples = block.sample_count as usize;
                let block_bytes = block.compressed_size();
                buf.compressed_bytes -= block_bytes;
                mem_delta -= block_bytes as isize;
                removed += block_samples;
            }
            // If still over, trim active window from the front
            if buf.sample_count() > max_samples && buf.active.samples.len() > max_samples {
                let trim = buf.active.samples.len() - max_samples;
                buf.active.samples.drain(..trim);
                mem_delta -= (trim * SAMPLE_BYTES) as isize;
            }
        }

        shard
            .eviction_heap
            .push(Reverse((buf.last_write_nanos, key)));
        (is_new, mem_delta, false, false)
    }

    /// Append a sample for a given node and property.
    pub fn append(&self, node_id: NodeId, property: &str, sample: TimeSample) {
        let key = TsKey {
            node_id,
            property: IStr::new(property),
        };
        let encoding = self.encoding_for(&key);
        let (is_new, mem_delta, dropped, deduped) = {
            let mut shard = self.shard_for(node_id).write();
            Self::append_to_buffer(
                &mut shard,
                key.clone(),
                sample,
                self.config.max_samples_per_buffer,
                self.config.out_of_order_tolerance_nanos,
                self.window_nanos,
                encoding,
            )
        };

        if dropped {
            self.out_of_order_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }

        if deduped {
            // Warm tier needs every sample for accurate count/sum/avg aggregates
            if let Some(warm) = &self.warm {
                warm.record(node_id, key.property, sample);
            }
            return;
        }

        // Update memory tracking
        let overhead = if is_new { BUFFER_OVERHEAD as isize } else { 0 };
        let total_delta = overhead + mem_delta;
        if total_delta > 0 {
            self.memory_used
                .fetch_add(total_delta as usize, Ordering::Relaxed);
        } else if total_delta < 0 {
            self.saturating_sub_memory((-total_delta) as usize);
        }

        // Evict if over pressure threshold
        if self.memory_used.load(Ordering::Relaxed) > self.pressure_budget {
            self.evict_under_pressure();
        }

        // Feed warm tier if configured
        if let Some(warm) = &self.warm {
            warm.record(node_id, key.property, sample);
        }
    }

    /// Append multiple samples in one pass, grouping by shard to lock each once.
    pub fn append_batch(&self, samples: &[(NodeId, &str, TimeSample)]) {
        let mut total_added: isize = 0;
        let mut new_buffers: usize = 0;
        let mut total_dropped: u64 = 0;
        // Track out-of-order dropped indexes (skip warm tier entirely)
        let mut dropped = vec![false; samples.len()];
        // Track deduped indexes (still feed warm tier for accurate aggregates)
        let mut deduped_flags = vec![false; samples.len()];

        // Pre-compute IStr for each sample to avoid duplicate interning in warm tier
        let interned: Vec<IStr> = samples.iter().map(|(_, p, _)| IStr::new(p)).collect();

        // Group by shard, preserving original index for dropped-set filtering
        let mut sample_indices: Vec<Vec<(usize, TsKey, TimeSample)>> =
            (0..SHARD_COUNT).map(|_| Vec::new()).collect();
        for (idx, &(node_id, _property, sample)) in samples.iter().enumerate() {
            let key = TsKey {
                node_id,
                property: interned[idx],
            };
            let shard_idx = node_id.0 as usize & SHARD_MASK;
            sample_indices[shard_idx].push((idx, key, sample));
        }

        for (shard_idx, entries) in sample_indices.into_iter().enumerate() {
            if entries.is_empty() {
                continue;
            }
            // Look up encodings before acquiring shard lock to avoid holding
            // shard.write() while acquiring encoding_hints.read().
            let encodings: Vec<ValueEncoding> = entries
                .iter()
                .map(|(_, key, _)| self.encoding_for(key))
                .collect();
            let mut shard = self.shards[shard_idx].write();
            for ((idx, key, sample), encoding) in entries.into_iter().zip(encodings) {
                let (is_new, mem_delta, was_dropped, deduped) = Self::append_to_buffer(
                    &mut shard,
                    key,
                    sample,
                    self.config.max_samples_per_buffer,
                    self.config.out_of_order_tolerance_nanos,
                    self.window_nanos,
                    encoding,
                );
                if was_dropped {
                    total_dropped += 1;
                    dropped[idx] = true;
                    continue;
                }
                if deduped {
                    deduped_flags[idx] = true;
                    continue;
                }
                if is_new {
                    new_buffers += 1;
                }
                total_added += mem_delta;
            }
        }

        if total_dropped > 0 {
            self.out_of_order_dropped
                .fetch_add(total_dropped, Ordering::Relaxed);
        }

        // Bulk memory tracking update
        let overhead = (new_buffers * BUFFER_OVERHEAD) as isize;
        let total_delta = overhead + total_added;
        if total_delta > 0 {
            self.memory_used
                .fetch_add(total_delta as usize, Ordering::Relaxed);
        } else if total_delta < 0 {
            self.saturating_sub_memory((-total_delta) as usize);
        }

        // Evict if over pressure threshold
        if self.memory_used.load(Ordering::Relaxed) > self.pressure_budget {
            self.evict_under_pressure();
        }

        // Feed warm tier (skip only out-of-order dropped; deduped still feed for accurate aggregates)
        if let Some(warm) = &self.warm {
            for (idx, &(node_id, _property, sample)) in samples.iter().enumerate() {
                if dropped[idx] {
                    continue;
                }
                warm.record(node_id, interned[idx], sample);
            }
        }
    }

    /// Query samples for a specific node+property within a time range.
    pub fn range(&self, node_id: NodeId, property: &str, start: i64, end: i64) -> Vec<TimeSample> {
        let key = TsKey {
            node_id,
            property: IStr::new(property),
        };
        let shard = self.shard_for(node_id).read();
        let Some(buf) = shard.buffers.get(&key) else {
            return vec![];
        };

        let mut result = Vec::new();

        // Scan sealed blocks (skip by start/end bounds)
        for block in &buf.sealed_blocks {
            if block.end_nanos < start || block.start_nanos > end {
                continue;
            }
            result.extend(block.decode_range_partial(start, end));
        }

        // Scan active window
        let start_idx = buf
            .active
            .samples
            .partition_point(|s| s.timestamp_nanos < start);
        result.extend(
            buf.active
                .samples
                .iter()
                .skip(start_idx)
                .take_while(|s| s.timestamp_nanos <= end)
                .copied(),
        );

        result
    }

    /// Get the latest sample for a node+property.
    pub fn latest(&self, node_id: NodeId, property: &str) -> Option<TimeSample> {
        let key = TsKey {
            node_id,
            property: IStr::new(property),
        };
        let shard = self.shard_for(node_id).read();
        shard.buffers.get(&key).and_then(|buf| buf.last_sample)
    }

    /// Find the sample at or immediately before the given timestamp.
    /// Used by `ts.valueAt` for LOCF and linear interpolation.
    pub fn sample_at_or_before(
        &self,
        node_id: NodeId,
        property: &str,
        timestamp_nanos: i64,
    ) -> Option<TimeSample> {
        let key = TsKey {
            node_id,
            property: IStr::new(property),
        };
        let shard = self.shard_for(node_id).read();
        let buf = shard.buffers.get(&key)?;

        // Search active window (binary search for the rightmost sample <= timestamp)
        let active_result = {
            let idx = buf
                .active
                .samples
                .partition_point(|s| s.timestamp_nanos <= timestamp_nanos);
            if idx > 0 {
                Some(buf.active.samples[idx - 1])
            } else {
                None
            }
        };

        // Search sealed blocks in reverse (newest first)
        let sealed_result = buf.sealed_blocks.iter().rev().find_map(|block| {
            if block.start_nanos > timestamp_nanos {
                return None;
            }
            let samples = block.decode_range_partial(block.start_nanos, timestamp_nanos);
            samples.last().copied()
        });

        // Return the more recent of the two
        match (active_result, sealed_result) {
            (Some(a), Some(s)) => {
                if a.timestamp_nanos >= s.timestamp_nanos {
                    Some(a)
                } else {
                    Some(s)
                }
            }
            (Some(a), None) => Some(a),
            (None, Some(s)) => Some(s),
            (None, None) => None,
        }
    }

    /// Find the first sample strictly after the given timestamp.
    /// Used by `ts.valueAt` for linear interpolation (finding the right bracket).
    pub fn sample_after(
        &self,
        node_id: NodeId,
        property: &str,
        timestamp_nanos: i64,
    ) -> Option<TimeSample> {
        let key = TsKey {
            node_id,
            property: IStr::new(property),
        };
        let shard = self.shard_for(node_id).read();
        let buf = shard.buffers.get(&key)?;

        // Search sealed blocks (oldest first)
        let sealed_result = buf.sealed_blocks.iter().find_map(|block| {
            if block.end_nanos <= timestamp_nanos {
                return None;
            }
            let samples = block.decode_all();
            samples
                .into_iter()
                .find(|s| s.timestamp_nanos > timestamp_nanos)
        });

        // Search active window
        let active_result = {
            let idx = buf
                .active
                .samples
                .partition_point(|s| s.timestamp_nanos <= timestamp_nanos);
            buf.active.samples.get(idx).copied()
        };

        // Return the earlier of the two
        match (sealed_result, active_result) {
            (Some(s), Some(a)) => {
                if s.timestamp_nanos <= a.timestamp_nanos {
                    Some(s)
                } else {
                    Some(a)
                }
            }
            (Some(s), None) => Some(s),
            (None, Some(a)) => Some(a),
            (None, None) => None,
        }
    }

    /// List all active keys (node+property pairs with data).
    pub fn all_keys(&self) -> Vec<TsKey> {
        let mut keys = Vec::new();
        for shard in &self.shards {
            keys.extend(shard.read().buffers.keys().cloned());
        }
        keys
    }

    /// Snapshot only buffers matching a specific entity ID and property.
    pub fn snapshot_key(
        &self,
        entity_id: NodeId,
        property: &str,
    ) -> Option<(TsKey, Vec<TimeSample>)> {
        let key = TsKey {
            node_id: entity_id,
            property: IStr::new(property),
        };
        let shard = self.shard_for(entity_id).read();
        shard.buffers.get(&key).map(|buf| (key, buf.all_samples()))
    }

    /// Number of active buffers (unique node+property pairs).
    pub fn buffer_count(&self) -> usize {
        self.shards.iter().map(|s| s.read().buffers.len()).sum()
    }

    /// Total number of samples across all buffers.
    pub fn sample_count(&self) -> usize {
        self.shards
            .iter()
            .map(|s| {
                s.read()
                    .buffers
                    .values()
                    .map(|b| b.sample_count())
                    .sum::<usize>()
            })
            .sum()
    }

    /// Hot retention in nanoseconds (computed from config).
    pub fn retention_nanos(&self) -> i64 {
        i64::from(self.config.hot_retention_hours) * 3_600 * 1_000_000_000
    }

    /// Access the config.
    pub fn config(&self) -> &TsConfig {
        &self.config
    }

    /// Current estimated memory usage in bytes.
    pub fn memory_used(&self) -> usize {
        self.memory_used.load(Ordering::Relaxed)
    }

    /// Total memory budget in bytes.
    pub fn memory_budget(&self) -> usize {
        self.memory_budget
    }

    /// Number of samples dropped due to exceeding out-of-order tolerance.
    pub fn out_of_order_dropped(&self) -> u64 {
        self.out_of_order_dropped.load(Ordering::Relaxed)
    }

    /// Return the per-buffer sample capacity if memory were evenly distributed
    /// across all active buffers.
    pub fn effective_cap(&self) -> usize {
        let active = self.buffer_count();
        if active == 0 {
            return self.memory_budget / SAMPLE_BYTES;
        }
        self.memory_budget / active / SAMPLE_BYTES
    }

    /// Current memory pressure as a fraction of the budget (0.0 to 1.0+).
    pub fn memory_pressure(&self) -> f32 {
        self.memory_used() as f32 / self.memory_budget as f32
    }

    /// Access the warm tier (if configured).
    pub fn warm_tier(&self) -> Option<&WarmTier> {
        self.warm.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn sample(ts: i64, val: f64) -> TimeSample {
        TimeSample {
            timestamp_nanos: ts,
            value: val,
        }
    }

    fn hot() -> HotTier {
        HotTier::new(TsConfig::default())
    }

    /// Create a HotTier with a specific memory budget in bytes and min_samples=0.
    fn hot_with_budget(budget_bytes: usize) -> HotTier {
        HotTier::new(TsConfig {
            hot_memory_budget_mb: 0,
            min_samples_per_buffer: 0,
            ..TsConfig::default()
        })
        .with_budget_bytes(budget_bytes)
    }

    impl HotTier {
        /// Test helper: override the memory budget in bytes (bypasses MB conversion).
        pub(crate) fn with_budget_bytes(mut self, bytes: usize) -> Self {
            self.memory_budget = bytes;
            self.pressure_budget = bytes;
            self
        }
    }

    #[test]
    fn append_and_latest() {
        let h = hot();
        h.append(NodeId(1), "temp", sample(100, 72.5));
        h.append(NodeId(1), "temp", sample(200, 73.0));

        let latest = h.latest(NodeId(1), "temp").unwrap();
        assert_eq!(latest.timestamp_nanos, 200);
        assert_eq!(latest.value, 73.0);
    }

    #[test]
    fn latest_missing_key() {
        let h = hot();
        assert!(h.latest(NodeId(999), "nonexistent").is_none());
    }

    #[test]
    fn range_query() {
        let h = hot();
        for i in 0..100 {
            h.append(NodeId(1), "temp", sample(i * 1000, i as f64));
        }

        let results = h.range(NodeId(1), "temp", 10_000, 20_000);
        assert_eq!(results.len(), 11);
        assert_eq!(results[0].timestamp_nanos, 10_000);
        assert_eq!(results[10].timestamp_nanos, 20_000);
    }

    #[test]
    fn range_query_empty() {
        let h = hot();
        let results = h.range(NodeId(1), "temp", 0, 1000);
        assert!(results.is_empty());
    }

    #[test]
    fn range_query_no_match() {
        let h = hot();
        h.append(NodeId(1), "temp", sample(100, 72.0));
        let results = h.range(NodeId(1), "temp", 200, 300);
        assert!(results.is_empty());
    }

    #[test]
    fn append_batch() {
        let h = hot();
        h.append_batch(&[
            (NodeId(1), "temp", sample(100, 72.0)),
            (NodeId(1), "humidity", sample(100, 45.0)),
            (NodeId(2), "temp", sample(100, 68.0)),
        ]);

        assert_eq!(h.buffer_count(), 3);
        assert_eq!(h.sample_count(), 3);
    }

    #[test]
    fn multiple_properties_per_node() {
        let h = hot();
        h.append(NodeId(1), "temp", sample(100, 72.0));
        h.append(NodeId(1), "humidity", sample(100, 45.0));
        h.append(NodeId(1), "pressure", sample(100, 1013.0));

        assert_eq!(h.buffer_count(), 3);
        assert_eq!(h.latest(NodeId(1), "humidity").unwrap().value, 45.0);
    }

    #[test]
    fn multiple_nodes() {
        let h = hot();
        for i in 1..=100 {
            h.append(NodeId(i), "temp", sample(100, 70.0 + i as f64 * 0.1));
        }

        assert_eq!(h.buffer_count(), 100);
        assert_eq!(h.sample_count(), 100);
    }

    #[test]
    fn all_keys() {
        let h = hot();
        h.append(NodeId(1), "temp", sample(100, 72.0));
        h.append(NodeId(2), "humidity", sample(100, 45.0));

        let keys = h.all_keys();
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn retention_nanos() {
        let h = hot();
        assert_eq!(h.retention_nanos(), 86_400_000_000_000);
    }

    #[test]
    fn concurrent_read_write() {
        let h = Arc::new(hot());

        for i in 0..1000 {
            h.append(NodeId(1), "temp", sample(i, i as f64));
        }

        let h2 = Arc::clone(&h);
        let results = h2.range(NodeId(1), "temp", 0, 999);
        assert_eq!(results.len(), 1000);
    }

    #[test]
    fn sharding_distributes_across_shards() {
        let h = hot();
        // Write to 16 different entities — should hit all 16 shards
        for i in 0..16 {
            h.append(NodeId(i), "temp", sample(100, 70.0));
        }
        assert_eq!(h.buffer_count(), 16);
        // Each shard should have exactly 1 entry
        for shard in &h.shards {
            assert_eq!(shard.read().buffers.len(), 1);
        }
    }

    // Memory tracking tests

    #[test]
    fn memory_tracking_basic() {
        let h = hot();
        assert_eq!(h.memory_used(), 0);

        // New buffer: BUFFER_OVERHEAD + SAMPLE_BYTES
        h.append(NodeId(1), "temp", sample(100, 72.0));
        assert_eq!(h.memory_used(), BUFFER_OVERHEAD + SAMPLE_BYTES);

        // Same buffer: +SAMPLE_BYTES
        h.append(NodeId(1), "temp", sample(200, 73.0));
        assert_eq!(h.memory_used(), BUFFER_OVERHEAD + 2 * SAMPLE_BYTES);

        // New buffer: +BUFFER_OVERHEAD + SAMPLE_BYTES
        h.append(NodeId(1), "humidity", sample(100, 45.0));
        assert_eq!(h.memory_used(), 2 * BUFFER_OVERHEAD + 3 * SAMPLE_BYTES);
    }

    #[test]
    fn effective_cap_adapts() {
        let h = hot();
        let budget = h.memory_budget();

        // No buffers: full budget available
        let cap0 = h.effective_cap();
        assert_eq!(cap0, budget / SAMPLE_BYTES);

        // Add one buffer
        h.append(NodeId(1), "temp", sample(100, 72.0));
        let cap1 = h.effective_cap();
        assert_eq!(cap1, budget / SAMPLE_BYTES);

        // Two buffers: cap halves
        h.append(NodeId(2), "temp", sample(100, 68.0));
        let cap2 = h.effective_cap();
        assert_eq!(cap2, budget / 2 / SAMPLE_BYTES);
        assert!(cap2 < cap1);
    }

    #[test]
    fn memory_pressure_calculation() {
        // Use a known budget: 1024 bytes
        let h = hot_with_budget(1024);

        assert_eq!(h.memory_pressure(), 0.0);

        // One buffer + sample = BUFFER_OVERHEAD + SAMPLE_BYTES = 144 bytes
        h.append(NodeId(1), "temp", sample(100, 72.0));
        let expected_pressure = (BUFFER_OVERHEAD + SAMPLE_BYTES) as f32 / 1024.0;
        assert!(
            (h.memory_pressure() - expected_pressure).abs() < 0.001,
            "pressure {} != expected {}",
            h.memory_pressure(),
            expected_pressure
        );
    }

    #[test]
    fn batch_append_tracks_memory() {
        let h = hot();

        h.append_batch(&[
            (NodeId(1), "temp", sample(100, 72.0)),
            (NodeId(1), "humidity", sample(100, 45.0)),
            (NodeId(2), "temp", sample(100, 68.0)),
        ]);

        // 3 buffers * (BUFFER_OVERHEAD + SAMPLE_BYTES)
        assert_eq!(h.memory_used(), 3 * BUFFER_OVERHEAD + 3 * SAMPLE_BYTES);
    }

    // Out-of-order handling

    #[test]
    fn out_of_order_within_tolerance_inserts_sorted() {
        let hot = HotTier::new(TsConfig::default());
        let nid = NodeId(1);
        hot.append(
            nid,
            "temp",
            TimeSample {
                timestamp_nanos: 1000,
                value: 70.0,
            },
        );
        hot.append(
            nid,
            "temp",
            TimeSample {
                timestamp_nanos: 3000,
                value: 72.0,
            },
        );
        // Out-of-order but within 5s tolerance
        hot.append(
            nid,
            "temp",
            TimeSample {
                timestamp_nanos: 2000,
                value: 71.0,
            },
        );

        let samples = hot.range(nid, "temp", 0, 5000);
        assert_eq!(samples.len(), 3);
        assert_eq!(samples[0].timestamp_nanos, 1000);
        assert_eq!(samples[1].timestamp_nanos, 2000);
        assert_eq!(samples[2].timestamp_nanos, 3000);
        assert_eq!(hot.out_of_order_dropped(), 0);
    }

    #[test]
    fn out_of_order_beyond_tolerance_dropped() {
        let config = TsConfig {
            out_of_order_tolerance_nanos: 1_000, // 1 microsecond
            ..TsConfig::default()
        };
        let hot = HotTier::new(config);
        let nid = NodeId(1);
        hot.append(
            nid,
            "temp",
            TimeSample {
                timestamp_nanos: 10_000,
                value: 70.0,
            },
        );
        // Way beyond tolerance
        hot.append(
            nid,
            "temp",
            TimeSample {
                timestamp_nanos: 1,
                value: 60.0,
            },
        );

        let samples = hot.range(nid, "temp", 0, 20_000);
        assert_eq!(samples.len(), 1);
        assert_eq!(hot.out_of_order_dropped(), 1);
    }

    #[test]
    fn out_of_order_batch_within_tolerance() {
        let hot = HotTier::new(TsConfig::default());
        let nid = NodeId(1);
        let samples = vec![
            (
                nid,
                "temp",
                TimeSample {
                    timestamp_nanos: 1000,
                    value: 70.0,
                },
            ),
            (
                nid,
                "temp",
                TimeSample {
                    timestamp_nanos: 3000,
                    value: 72.0,
                },
            ),
            (
                nid,
                "temp",
                TimeSample {
                    timestamp_nanos: 2000,
                    value: 71.0,
                },
            ),
        ];
        hot.append_batch(&samples);
        let result = hot.range(nid, "temp", 0, 5000);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].timestamp_nanos, 1000);
        assert_eq!(result[1].timestamp_nanos, 2000);
        assert_eq!(result[2].timestamp_nanos, 3000);
        assert_eq!(hot.out_of_order_dropped(), 0);
    }

    #[test]
    fn out_of_order_batch_beyond_tolerance_dropped() {
        let config = TsConfig {
            out_of_order_tolerance_nanos: 500,
            ..TsConfig::default()
        };
        let hot = HotTier::new(config);
        let nid = NodeId(1);
        let samples = vec![
            (
                nid,
                "temp",
                TimeSample {
                    timestamp_nanos: 10_000,
                    value: 70.0,
                },
            ),
            (
                nid,
                "temp",
                TimeSample {
                    timestamp_nanos: 1,
                    value: 60.0,
                },
            ), // dropped
        ];
        hot.append_batch(&samples);
        let result = hot.range(nid, "temp", 0, 20_000);
        assert_eq!(result.len(), 1);
        assert_eq!(hot.out_of_order_dropped(), 1);
    }

    #[test]
    fn in_order_append_unaffected() {
        let hot = HotTier::new(TsConfig::default());
        let nid = NodeId(1);
        for i in 0..100 {
            hot.append(
                nid,
                "temp",
                TimeSample {
                    timestamp_nanos: i * 1000,
                    value: 70.0 + i as f64,
                },
            );
        }
        let samples = hot.range(nid, "temp", 0, 100_000);
        assert_eq!(samples.len(), 100);
        // Sorted invariant
        for w in samples.windows(2) {
            assert!(w[0].timestamp_nanos <= w[1].timestamp_nanos);
        }
        assert_eq!(hot.out_of_order_dropped(), 0);
    }

    #[test]
    fn dedup_updates_last_sample_timestamp() {
        let config = TsConfig {
            max_samples_per_buffer: 100,
            ..Default::default()
        };
        let hot = HotTier::new(config);
        let nid = NodeId(1);
        hot.append(
            nid,
            "temp",
            TimeSample {
                timestamp_nanos: 100,
                value: 72.0,
            },
        );
        hot.append(
            nid,
            "temp",
            TimeSample {
                timestamp_nanos: 200,
                value: 72.0,
            },
        );
        hot.append(
            nid,
            "temp",
            TimeSample {
                timestamp_nanos: 300,
                value: 72.0,
            },
        );

        let latest = hot.latest(nid, "temp").unwrap();
        assert_eq!(
            latest.timestamp_nanos, 300,
            "latest() must return newest timestamp for deduped values"
        );
    }

    #[test]
    fn dedup_preserves_zero_sign_bit() {
        let config = TsConfig {
            max_samples_per_buffer: 100,
            ..Default::default()
        };
        let hot = HotTier::new(config);
        let nid = NodeId(1);
        hot.append(
            nid,
            "val",
            TimeSample {
                timestamp_nanos: 100,
                value: 0.0,
            },
        );
        hot.append(
            nid,
            "val",
            TimeSample {
                timestamp_nanos: 200,
                value: -0.0,
            },
        );

        let samples = hot.range(nid, "val", 0, i64::MAX);
        assert_eq!(
            samples.len(),
            2,
            "0.0 and -0.0 are bitwise different, both must be stored"
        );
    }

    #[test]
    fn value_dedup_skips_identical() {
        let hot = HotTier::new(TsConfig::default());
        let node = NodeId(1);

        // Write same value 5 times
        for i in 0..5 {
            hot.append(
                node,
                "status",
                TimeSample {
                    timestamp_nanos: i * 1000,
                    value: 1.0,
                },
            );
        }

        // Only the first sample should be stored (rest are deduplicated)
        let range = hot.range(node, "status", 0, i64::MAX);
        assert_eq!(range.len(), 1);
        assert_eq!(range[0].value, 1.0);

        // Write a different value
        hot.append(
            node,
            "status",
            TimeSample {
                timestamp_nanos: 5000,
                value: 2.0,
            },
        );
        let range = hot.range(node, "status", 0, i64::MAX);
        assert_eq!(range.len(), 2);
    }
}
