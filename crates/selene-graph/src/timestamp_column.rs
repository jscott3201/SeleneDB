//! Delta-encoded timestamp column -- 4 bytes per entity instead of 8.
//!
//! Stores millisecond deltas from a graph epoch (creation time).
//! i32 in milliseconds covers ±24.8 days. Entities outside this window
//! use an overflow HashMap.

use rustc_hash::FxHashMap;

use crate::chunked_vec::ChunkedVec;

const OVERFLOW_SENTINEL: i32 = i32::MIN;
const MS_PER_NANO: i64 = 1_000_000;

/// Default epoch for TimestampColumn: 2024-01-01T00:00:00Z in nanoseconds.
///
/// Using a recent epoch ensures that entities created after 2024 use the
/// compact 4-byte delta path instead of overflowing to the HashMap fallback.
/// With epoch=0, ALL modern timestamps overflow because the delta exceeds
/// the ±24.8 day i32 ms range.
pub const DEFAULT_EPOCH_NANOS: i64 = 1_704_067_200_000_000_000;

/// Delta-encoded timestamp column.
///
/// Stores timestamps as i32 millisecond deltas from an epoch, reducing
/// per-entity memory from 8 bytes (i64 nanos) to 4 bytes (i32 ms delta).
/// Entities outside the ±24.8 day window use an overflow map for lossless
/// storage.
#[derive(Clone, Debug)]
pub struct TimestampColumn {
    epoch_nanos: i64,
    deltas: ChunkedVec<i32>,
    overflow: FxHashMap<u32, i64>,
}

impl TimestampColumn {
    /// Create with the given epoch (typically graph creation time, or 0).
    pub fn new(epoch_nanos: i64) -> Self {
        Self {
            epoch_nanos,
            deltas: ChunkedVec::new(),
            overflow: FxHashMap::default(),
        }
    }

    /// Get the full nanosecond timestamp for a slot.
    pub fn get(&self, slot: u32) -> i64 {
        let delta = self.deltas.get(slot as usize).copied().unwrap_or(0);
        if delta == OVERFLOW_SENTINEL {
            self.overflow
                .get(&slot)
                .copied()
                .unwrap_or(self.epoch_nanos)
        } else {
            self.epoch_nanos + i64::from(delta) * MS_PER_NANO
        }
    }

    /// Set the timestamp for a slot (must be within allocated range).
    pub fn set(&mut self, slot: u32, nanos: i64) {
        let delta_ms = (nanos - self.epoch_nanos) / MS_PER_NANO;
        if delta_ms > i64::from(i32::MIN) && delta_ms <= i64::from(i32::MAX) {
            self.deltas.set(slot as usize, delta_ms as i32);
            self.overflow.remove(&slot);
        } else {
            self.deltas.set(slot as usize, OVERFLOW_SENTINEL);
            self.overflow.insert(slot, nanos);
        }
    }

    /// Grow column to `new_len`, filling new slots with `default_nanos`.
    /// Matches ChunkedVec::resize() API used by NodeStore/EdgeStore.
    pub fn resize(&mut self, new_len: usize, default_nanos: i64) {
        let delta_ms = (default_nanos - self.epoch_nanos) / MS_PER_NANO;
        let default_delta = if delta_ms > i64::from(i32::MIN) && delta_ms <= i64::from(i32::MAX) {
            delta_ms as i32
        } else {
            OVERFLOW_SENTINEL
        };
        let old_len = self.deltas.len();
        self.deltas.resize(new_len, default_delta);
        // If overflow sentinel, store full timestamp for new slots
        if default_delta == OVERFLOW_SENTINEL && default_nanos != 0 {
            for slot in old_len..new_len {
                self.overflow.insert(slot as u32, default_nanos);
            }
        }
    }

    /// Number of entries.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.deltas.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_roundtrip() {
        let epoch = 1_700_000_000_000_000_000i64;
        let mut col = TimestampColumn::new(epoch);
        col.resize(1, 0);
        let ts = epoch + 5_000_000_000; // 5 seconds later
        col.set(0, ts);
        let recovered = col.get(0);
        // Delta encoding loses sub-ms precision
        assert!((recovered - ts).abs() < MS_PER_NANO);
    }

    #[test]
    fn overflow_roundtrip() {
        let epoch = 1_700_000_000_000_000_000i64;
        let mut col = TimestampColumn::new(epoch);
        col.resize(1, 0);
        // 30 days later — exceeds ±24.8 day i32 ms range
        let ts = epoch + 30 * 24 * 3600 * 1_000_000_000i64;
        col.set(0, ts);
        assert_eq!(col.get(0), ts); // overflow path is lossless
    }

    #[test]
    fn resize_and_access() {
        let epoch = 1_700_000_000_000_000_000i64;
        let mut col = TimestampColumn::new(epoch);
        col.resize(100, 0);
        assert_eq!(col.len(), 100);
        for i in 0..100u32 {
            let ts = epoch + i64::from(i) * 1_000_000_000;
            col.set(i, ts);
        }
        for i in 0..100u32 {
            let expected = epoch + i64::from(i) * 1_000_000_000;
            assert!((col.get(i) - expected).abs() < MS_PER_NANO);
        }
    }

    #[test]
    fn zero_epoch() {
        // Common case: epoch = 0 (default graph)
        let mut col = TimestampColumn::new(0);
        col.resize(2, 0);
        col.set(0, 1_700_000_000_000_000_000);
        col.set(1, 1_700_000_005_000_000_000);
        // Both overflow because delta from 0 exceeds i32 ms range
        assert_eq!(col.get(0), 1_700_000_000_000_000_000);
        assert_eq!(col.get(1), 1_700_000_005_000_000_000);
    }
}
