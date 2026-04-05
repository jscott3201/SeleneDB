//! Eviction and drain methods for [`HotTier`].
//!
//! Extracted from `hot.rs` to isolate the memory-pressure eviction and
//! time-based drain logic from the core write/query paths.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::Ordering;

use crate::encoding::TsBlock;
use crate::hot::{BUFFER_OVERHEAD, DRAIN_CHUNK_SIZE, HotTier, SAMPLE_BYTES, TimeSample, TsKey};

impl HotTier {
    /// Evict oldest sealed blocks from the largest buffers until under budget.
    ///
    /// Scan all shards (read locks) to find the largest buffer, then write-lock
    /// that shard and remove oldest sealed blocks until under budget or at
    /// `min_samples_per_buffer`.
    pub(crate) fn evict_under_pressure(&self) {
        let min_samples = self.config.min_samples_per_buffer;

        loop {
            let used = self.memory_used.load(Ordering::Relaxed);
            if used <= self.memory_budget {
                break;
            }

            // Find shard+key with the largest buffer
            let mut best: Option<(usize, TsKey, usize)> = None; // (shard_idx, key, count)
            for (shard_idx, shard) in self.shards.iter().enumerate() {
                let guard = shard.read();
                for (key, buf) in &guard.buffers {
                    let count = buf.sample_count();
                    if count > min_samples
                        && best
                            .as_ref()
                            .is_none_or(|(_, _, best_count)| count > *best_count)
                    {
                        best = Some((shard_idx, key.clone(), count));
                    }
                }
            }

            let Some((shard_idx, key, _)) = best else {
                break; // all buffers at or below min_samples
            };

            // Write-lock target shard and evict sealed blocks
            let mut guard = self.shards[shard_idx].write();
            if let Some(buf) = guard.buffers.get_mut(&key) {
                let mut freed = 0usize;
                // Evict oldest sealed blocks first
                while buf.sample_count() > min_samples && !buf.sealed_blocks.is_empty() {
                    if self
                        .memory_used
                        .load(Ordering::Relaxed)
                        .saturating_sub(freed)
                        <= self.memory_budget
                    {
                        break;
                    }
                    let block = buf.sealed_blocks.pop_front().unwrap();
                    let block_bytes = block.compressed_size();
                    buf.compressed_bytes -= block_bytes;
                    freed += block_bytes;
                }
                // If no sealed blocks remain and still over budget, trim active
                if buf.sealed_blocks.is_empty() {
                    let available = buf.active.samples.len().saturating_sub(min_samples);
                    if available > 0 {
                        let over_budget = (self
                            .memory_used
                            .load(Ordering::Relaxed)
                            .saturating_sub(freed))
                        .saturating_sub(self.memory_budget);
                        let needed = over_budget.div_ceil(SAMPLE_BYTES);
                        let trim_count = needed.min(available);
                        if trim_count > 0 {
                            buf.active.samples.drain(..trim_count);
                            freed += trim_count * SAMPLE_BYTES;
                        }
                    }
                }
                if freed > 0 {
                    self.saturating_sub_memory(freed);
                }
            }
        }
    }

    /// Drain all samples with timestamps before `cutoff_nanos`.
    ///
    /// Each shard's write lock is released and reacquired every
    /// `DRAIN_CHUNK_SIZE` sealed blocks to bound per-shard hold time and
    /// let concurrent writers make progress.
    pub fn drain_before(&self, cutoff_nanos: i64) -> Vec<(TsKey, Vec<TimeSample>)> {
        let mut drained: Vec<(TsKey, Vec<TimeSample>)> = Vec::new();
        // Index from TsKey to position in `drained` for O(1) merge lookups
        // instead of linear scan.
        let mut drained_index: HashMap<TsKey, usize> = HashMap::new();
        let mut freed_bytes: usize = 0;

        for shard in &self.shards {
            // Chunked loop: each iteration holds the lock for at most
            // DRAIN_CHUNK_SIZE sealed-block removals across all keys.
            loop {
                let mut blocks_this_chunk = 0usize;
                let mut chunk_done = true; // assume we finish in this pass

                {
                    let mut guard = shard.write();
                    for (key, buf) in &mut guard.buffers {
                        let mut samples = Vec::new();

                        // Drain sealed blocks whose samples are all before cutoff
                        while !buf.sealed_blocks.is_empty() && blocks_this_chunk < DRAIN_CHUNK_SIZE
                        {
                            let block = &buf.sealed_blocks[0];
                            if block.end_nanos < cutoff_nanos {
                                // Entire block is before cutoff
                                let block = buf.sealed_blocks.pop_front().unwrap();
                                let block_bytes = block.compressed_size();
                                buf.compressed_bytes -= block_bytes;
                                freed_bytes += block_bytes;
                                samples.extend(block.decode_all());
                                blocks_this_chunk += 1;
                            } else if block.start_nanos < cutoff_nanos {
                                // Partial block: decode, split, re-encode remaining
                                let block = buf.sealed_blocks.pop_front().unwrap();
                                let block_bytes = block.compressed_size();
                                buf.compressed_bytes -= block_bytes;
                                freed_bytes += block_bytes;
                                let block_encoding = block.encoding;
                                let all = block.decode_all();
                                let split_pos =
                                    all.partition_point(|s| s.timestamp_nanos < cutoff_nanos);
                                samples.extend_from_slice(&all[..split_pos]);
                                let remaining = &all[split_pos..];
                                if !remaining.is_empty() {
                                    let new_block = TsBlock::encode(remaining, block_encoding);
                                    let new_bytes = new_block.compressed_size();
                                    buf.compressed_bytes += new_bytes;
                                    freed_bytes = freed_bytes.saturating_sub(new_bytes);
                                    buf.sealed_blocks.push_front(new_block);
                                }
                                blocks_this_chunk += 1;
                                break;
                            } else {
                                break;
                            }
                        }

                        // Check if this key still has drainable sealed blocks
                        if !buf.sealed_blocks.is_empty()
                            && buf.sealed_blocks[0].start_nanos < cutoff_nanos
                            && blocks_this_chunk >= DRAIN_CHUNK_SIZE
                        {
                            chunk_done = false;
                        }

                        // Drain active window samples before cutoff (only on final pass
                        // for this key, i.e. when sealed blocks are done)
                        if chunk_done || blocks_this_chunk < DRAIN_CHUNK_SIZE {
                            let active_drain_count = buf
                                .active
                                .samples
                                .partition_point(|s| s.timestamp_nanos < cutoff_nanos);
                            if active_drain_count > 0 {
                                let drained_active: Vec<TimeSample> =
                                    buf.active.samples.drain(..active_drain_count).collect();
                                freed_bytes += drained_active.len() * SAMPLE_BYTES;
                                samples.extend(drained_active);
                            }
                        }

                        if !samples.is_empty() {
                            // Merge into existing drained entry for this key, or create new.
                            if let Some(&idx) = drained_index.get(key) {
                                drained[idx].1.extend(samples);
                            } else {
                                let idx = drained.len();
                                drained_index.insert(key.clone(), idx);
                                drained.push((key.clone(), samples));
                            }
                        }

                        if blocks_this_chunk >= DRAIN_CHUNK_SIZE {
                            chunk_done = false;
                            break; // release lock, will reacquire
                        }
                    }

                    // Remove empty buffers at the end of each chunk pass
                    if chunk_done {
                        let before = guard.buffers.len();
                        guard.buffers.retain(|_, buf| buf.sample_count() > 0);
                        freed_bytes += (before - guard.buffers.len()) * BUFFER_OVERHEAD;
                    }
                } // guard dropped here

                if chunk_done {
                    break;
                }
            }
        }

        if freed_bytes > 0 {
            self.saturating_sub_memory(freed_bytes);
        }

        // Also drain expired warm tier aggregates
        if let Some(warm) = &self.warm {
            let warm_cutoff = selene_core::now_nanos() - warm.retention_nanos();
            warm.drain_expired(warm_cutoff);
        }

        drained
    }

    /// Evict idle buffers that haven't received writes since `idle_cutoff_nanos`.
    ///
    /// Uses the per-shard eviction min-heap for O(k log n) performance, where k
    /// is the number of evicted buffers. When nothing is idle (the common case),
    /// each shard peek is O(1) and the method returns immediately.
    ///
    /// Stale heap entries (where the buffer was written to after the heap entry
    /// was pushed) are detected via lazy deletion: if the buffer's
    /// `last_write_nanos` is >= `idle_cutoff_nanos`, the heap entry is skipped.
    /// If the heap grows to more than 4x the live buffer count, it is rebuilt
    /// from scratch to prevent unbounded growth.
    pub fn evict_idle(&self, idle_cutoff_nanos: i64) -> usize {
        let mut total_evicted = 0;
        let mut freed_bytes: usize = 0;

        for shard in &self.shards {
            let mut guard = shard.write();

            while let Some(&Reverse((ts, _))) = guard.eviction_heap.peek() {
                if ts >= idle_cutoff_nanos {
                    break;
                }
                let Reverse((_, key)) = guard.eviction_heap.pop().unwrap();

                // Lazy deletion: verify this entry is still current
                if let Some(buf) = guard.buffers.get(&key)
                    && buf.last_write_nanos < idle_cutoff_nanos
                {
                    freed_bytes += BUFFER_OVERHEAD + buf.data_bytes();
                    guard.buffers.remove(&key);
                    total_evicted += 1;
                }
                // else: buffer was written to since -- stale heap entry, skip
                // else: buffer already removed -- stale heap entry, skip
            }

            // Compact heap if stale entries exceed 4x buffer count
            if guard.eviction_heap.len() > guard.buffers.len().saturating_mul(4).max(64) {
                let new_heap: BinaryHeap<Reverse<(i64, TsKey)>> = guard
                    .buffers
                    .iter()
                    .map(|(k, b)| Reverse((b.last_write_nanos, k.clone())))
                    .collect();
                guard.eviction_heap = new_heap;
            }
        }

        if freed_bytes > 0 {
            self.saturating_sub_memory(freed_bytes);
        }

        total_evicted
    }
}

#[cfg(test)]
mod tests {
    use selene_core::NodeId;

    use crate::config::TsConfig;
    use crate::hot::{BUFFER_OVERHEAD, HotTier, SAMPLE_BYTES, TimeSample};

    fn sample(ts: i64, val: f64) -> TimeSample {
        TimeSample {
            timestamp_nanos: ts,
            value: val,
        }
    }

    fn hot() -> HotTier {
        HotTier::new(TsConfig::default())
    }

    fn hot_with_budget(budget_bytes: usize) -> HotTier {
        HotTier::new(TsConfig {
            hot_memory_budget_mb: 0,
            min_samples_per_buffer: 0,
            ..TsConfig::default()
        })
        .with_budget_bytes(budget_bytes)
    }

    #[test]
    fn drain_before() {
        let h = hot();
        h.append(NodeId(1), "temp", sample(100, 72.0));
        h.append(NodeId(1), "temp", sample(200, 73.0));
        h.append(NodeId(1), "temp", sample(300, 74.0));

        let drained = h.drain_before(250);
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].1.len(), 2);
        assert_eq!(h.sample_count(), 1);
    }

    #[test]
    fn drain_before_cleans_empty_buffers() {
        let h = hot();
        h.append(NodeId(1), "temp", sample(100, 72.0));

        let drained = h.drain_before(200);
        assert_eq!(drained.len(), 1);
        assert_eq!(h.buffer_count(), 0);
    }

    #[test]
    fn drain_before_nothing_to_drain() {
        let h = hot();
        h.append(NodeId(1), "temp", sample(1000, 72.0));

        let drained = h.drain_before(500);
        assert!(drained.is_empty());
        assert_eq!(h.sample_count(), 1);
    }

    #[test]
    fn evict_idle_buffers() {
        let h = hot();
        h.append(NodeId(1), "temp", sample(100, 72.0));
        h.append(NodeId(2), "temp", sample(200, 68.0));

        assert_eq!(h.buffer_count(), 2);

        let evicted = h.evict_idle(150);
        assert_eq!(evicted, 1);
        assert_eq!(h.buffer_count(), 1);

        assert!(h.latest(NodeId(1), "temp").is_none());
        assert!(h.latest(NodeId(2), "temp").is_some());
    }

    #[test]
    fn evict_idle_none_evicted() {
        let h = hot();
        h.append(NodeId(1), "temp", sample(1000, 72.0));
        let evicted = h.evict_idle(500);
        assert_eq!(evicted, 0);
        assert_eq!(h.buffer_count(), 1);
    }

    #[test]
    fn large_drain() {
        let h = hot();
        for i in 0..3600 {
            h.append(
                NodeId(1),
                "temp",
                sample(i * 1_000_000_000, 72.0 + (i % 10) as f64 * 0.1),
            );
        }

        assert_eq!(h.sample_count(), 3600);

        let cutoff = 1800 * 1_000_000_000;
        let drained = h.drain_before(cutoff);
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].1.len(), 1800);
        assert_eq!(h.sample_count(), 1800);
    }

    #[test]
    fn memory_budget_enforced() {
        let budget = BUFFER_OVERHEAD + 20 * SAMPLE_BYTES;
        let h = HotTier::new(TsConfig {
            hot_memory_budget_mb: 0,
            min_samples_per_buffer: 5,
            ..TsConfig::default()
        })
        .with_budget_bytes(budget);

        for i in 0..50 {
            h.append(NodeId(1), "temp", sample(i * 1000, i as f64));
        }

        assert!(
            h.memory_used() <= budget,
            "memory_used {} should be <= budget {}",
            h.memory_used(),
            budget
        );
        assert!(h.sample_count() > 0);
    }

    #[test]
    fn eviction_under_pressure() {
        let budget = BUFFER_OVERHEAD + 10 * SAMPLE_BYTES;
        let h = hot_with_budget(budget);

        for i in 0..15 {
            h.append(NodeId(1), "temp", sample(i * 1000, i as f64));
        }

        assert!(h.memory_used() <= budget);
        let latest = h.latest(NodeId(1), "temp").unwrap();
        assert_eq!(latest.timestamp_nanos, 14_000);
    }

    #[test]
    fn min_samples_respected() {
        let budget = BUFFER_OVERHEAD + 2 * SAMPLE_BYTES;
        let h = HotTier::new(TsConfig {
            hot_memory_budget_mb: 0,
            min_samples_per_buffer: 10,
            ..TsConfig::default()
        })
        .with_budget_bytes(budget);

        for i in 0..15 {
            h.append(NodeId(1), "temp", sample(i * 1000, i as f64));
        }

        assert!(
            h.sample_count() >= 10,
            "sample_count {} should be >= min_samples 10",
            h.sample_count()
        );
    }

    #[test]
    fn drain_updates_memory() {
        let h = hot();
        h.append(NodeId(1), "temp", sample(100, 72.0));
        h.append(NodeId(1), "temp", sample(200, 73.0));
        h.append(NodeId(1), "temp", sample(300, 74.0));

        let before = h.memory_used();
        assert_eq!(before, BUFFER_OVERHEAD + 3 * SAMPLE_BYTES);

        let drained = h.drain_before(250);
        assert_eq!(drained[0].1.len(), 2);

        assert_eq!(h.memory_used(), BUFFER_OVERHEAD + SAMPLE_BYTES);
    }

    #[test]
    fn evict_idle_stale_entry_skipped() {
        let h = hot();
        h.append(NodeId(1), "temp", sample(100, 72.0));
        h.append(NodeId(1), "temp", sample(500, 73.0));

        let evicted = h.evict_idle(300);
        assert_eq!(evicted, 0);
        assert_eq!(h.buffer_count(), 1);
    }

    #[test]
    fn evict_idle_heap_compaction() {
        let h = hot();
        for i in 0..100 {
            h.append(NodeId(1), "temp", sample(i * 10, 72.0 + i as f64 * 0.01));
        }
        let evicted = h.evict_idle(500);
        assert_eq!(evicted, 0);
        assert_eq!(h.buffer_count(), 1);
    }

    #[test]
    fn drain_removes_buffer_updates_memory() {
        let h = hot();
        h.append(NodeId(1), "temp", sample(100, 72.0));
        assert_eq!(h.memory_used(), BUFFER_OVERHEAD + SAMPLE_BYTES);

        h.drain_before(200);

        assert_eq!(h.memory_used(), 0);
        assert_eq!(h.buffer_count(), 0);
    }

    #[test]
    fn evict_idle_updates_memory() {
        let h = hot();
        h.append(NodeId(1), "temp", sample(100, 72.0));
        h.append(NodeId(1), "temp", sample(200, 73.0));
        h.append(NodeId(2), "temp", sample(300, 68.0));

        let before = h.memory_used();
        assert_eq!(before, 2 * BUFFER_OVERHEAD + 3 * SAMPLE_BYTES);

        let evicted = h.evict_idle(250);
        assert_eq!(evicted, 1);

        assert_eq!(h.memory_used(), BUFFER_OVERHEAD + SAMPLE_BYTES);
    }
}
