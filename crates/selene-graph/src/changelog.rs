//! In-memory changelog buffer for delta sync to consumers.
//!
//! Fixed-capacity ring buffer of [`ChangelogEntry`] records. Consumers
//! subscribe and receive deltas. If a consumer falls behind beyond
//! buffer capacity, it must perform a full re-sync.

use std::collections::VecDeque;

use rustc_hash::FxHashMap;

use selene_core::changeset::Change;

/// A single changelog batch -- one atomic commit.
#[derive(Debug, Clone)]
pub struct ChangelogEntry {
    /// Monotonically increasing sequence number.
    pub sequence: u64,
    /// Wall-clock timestamp of the commit (nanoseconds since epoch).
    pub timestamp_nanos: i64,
    /// HLC timestamp for causal ordering. 0 for pre-HLC entries.
    pub hlc_timestamp: u64,
    /// Changes from this commit.
    pub changes: Vec<Change>,
}

/// Fixed-capacity ring buffer of committed change batches.
///
/// Each call to [`append`][ChangelogBuffer::append] assigns a strictly
/// monotonic sequence number and stores the batch. When the buffer is
/// full, the oldest entry is evicted. Callers that ask for a sequence
/// that has been evicted receive [`None`] and must perform a full
/// re-sync.
#[derive(Clone)]
pub struct ChangelogBuffer {
    entries: VecDeque<ChangelogEntry>,
    next_sequence: u64,
    capacity: usize,
    /// Per-entity index: node_id → sequence numbers containing changes for this node.
    by_node: FxHashMap<u64, Vec<u64>>,
    /// Sequences below this watermark have been evicted from the ring buffer.
    /// Used for lazy filtering of stale `by_node` entries instead of eager `retain()`.
    min_valid_sequence: u64,
    /// Counts evictions since the last full by_node compaction. A full sweep runs
    /// every `capacity` evictions, ensuring stale entries from inactive nodes are
    /// removed after at most one full buffer turnover.
    evictions_since_compaction: usize,
}

impl ChangelogBuffer {
    /// Create a new buffer with the given maximum entry count.
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(capacity),
            next_sequence: 1,
            capacity,
            by_node: FxHashMap::default(),
            min_valid_sequence: 0,
            evictions_since_compaction: 0,
        }
    }

    /// Append a batch of changes with a commit timestamp.
    ///
    /// Returns the sequence number assigned to this batch. If the buffer
    /// is already at capacity the oldest entry is evicted first.
    pub fn append(
        &mut self,
        changes: Vec<Change>,
        timestamp_nanos: i64,
        hlc_timestamp: u64,
    ) -> u64 {
        // Evict oldest if at capacity
        if self.entries.len() >= self.capacity {
            self.entries.pop_front();
            self.min_valid_sequence = self.entries.front().map_or(u64::MAX, |e| e.sequence);
            self.evictions_since_compaction += 1;

            // Full sweep every `capacity` evictions (one complete buffer turnover).
            // Removes HashMap entries for nodes no longer referenced by any live entry.
            if self.evictions_since_compaction >= self.capacity {
                self.by_node.retain(|_, seqs| {
                    seqs.retain(|s| *s >= self.min_valid_sequence);
                    !seqs.is_empty()
                });
                self.evictions_since_compaction = 0;
            }
        }

        let seq = self.next_sequence;
        self.next_sequence += 1;

        // Update per-entity index (edge changes index under both source and target).
        // Lazy compaction: when touching a node, discard stale sequences first.
        for change in &changes {
            for nid in change.affected_node_ids() {
                let seqs = self.by_node.entry(nid).or_default();
                if seqs.first().is_some_and(|&s| s < self.min_valid_sequence) {
                    seqs.retain(|&s| s >= self.min_valid_sequence);
                }
                seqs.push(seq);
            }
        }

        self.entries.push_back(ChangelogEntry {
            sequence: seq,
            timestamp_nanos,
            hlc_timestamp,
            changes,
        });
        seq
    }

    /// Return owned clones of all entries with sequence number greater than
    /// `after_seq`.
    ///
    /// Returns `None` if `after_seq` is non-zero and falls before the oldest
    /// retained entry -- the caller must perform a full re-sync in that case.
    /// Returns an empty `Vec` if `after_seq` is at or beyond the current
    /// sequence.
    pub fn since(&self, after_seq: u64) -> Option<Vec<ChangelogEntry>> {
        // A sequence of 0 means "give me everything".
        if after_seq == 0 {
            return Some(self.entries.iter().cloned().collect());
        }

        // If nothing has been appended yet, return empty.
        if self.entries.is_empty() {
            return Some(vec![]);
        }

        let oldest_seq = self.entries.front().map_or(1, |e| e.sequence);

        // The requested sequence was evicted.
        if after_seq < oldest_seq {
            return None;
        }

        // Binary search: sequences are monotonic, use partition_point for O(log n)
        let start = self.entries.partition_point(|e| e.sequence <= after_seq);
        Some(self.entries.iter().skip(start).cloned().collect())
    }

    /// Query changes for a specific entity (by node ID).
    ///
    /// Returns changelog entries that contain changes for the given node,
    /// filtered by optional time range. Uses the per-entity index for O(1)
    /// lookup of relevant sequences.
    pub fn entity_history(
        &self,
        node_id: u64,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Vec<&ChangelogEntry> {
        let Some(seqs) = self.by_node.get(&node_id) else {
            return vec![];
        };

        let mut result = Vec::new();
        for &seq in seqs {
            // Skip stale index entries that reference evicted sequences
            if seq < self.min_valid_sequence {
                continue;
            }
            // Find entry by sequence (binary search on sorted VecDeque)
            let idx = self.entries.partition_point(|e| e.sequence < seq);
            if let Some(entry) = self.entries.get(idx)
                && entry.sequence == seq
            {
                // Apply time filter
                if let Some(start) = start_time
                    && entry.timestamp_nanos < start
                {
                    continue;
                }
                if let Some(end) = end_time
                    && entry.timestamp_nanos > end
                {
                    continue;
                }
                result.push(entry);
            }
        }
        result
    }

    /// The sequence number of the most recently appended batch, or 0 if
    /// the buffer is empty.
    pub fn current_sequence(&self) -> u64 {
        self.entries.back().map_or(0, |e| e.sequence)
    }

    /// Borrow the changes from the most recently appended entry.
    ///
    /// Used by `TrackedMutation::commit()` to clone from the canonical
    /// changelog copy instead of cloning before the append.
    pub fn last_changes(&self) -> Option<&Vec<Change>> {
        self.entries.back().map(|e| &e.changes)
    }

    /// Number of entries currently held in the buffer.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if the buffer contains no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use selene_core::{
        IStr,
        changeset::Change,
        entity::{EdgeId, NodeId},
        value::Value,
    };

    use super::*;

    // ── helpers ──────────────────────────────────────────────────────────────

    fn node_created(id: u64) -> Change {
        Change::NodeCreated {
            node_id: NodeId(id),
        }
    }

    /// Default timestamp for tests that don't care about time.
    const TS: i64 = 1_000_000_000;

    // 1 ─────────────────────────────────────────────────────────────────────
    #[test]
    fn new_buffer_is_empty() {
        let buf = ChangelogBuffer::new(10);
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.current_sequence(), 0);
    }

    // 2 ─────────────────────────────────────────────────────────────────────
    #[test]
    fn append_returns_monotonic_sequences() {
        let mut buf = ChangelogBuffer::new(10);
        let s1 = buf.append(vec![node_created(1)], TS, 0);
        let s2 = buf.append(vec![node_created(2)], TS, 0);
        let s3 = buf.append(vec![node_created(3)], TS, 0);
        assert_eq!(s1, 1);
        assert_eq!(s2, 2);
        assert_eq!(s3, 3);
        assert_eq!(buf.len(), 3);
    }

    // 3 ─────────────────────────────────────────────────────────────────────
    #[test]
    fn since_returns_entries_after_sequence() {
        let mut buf = ChangelogBuffer::new(10);
        buf.append(vec![node_created(1)], TS, 0);
        buf.append(vec![node_created(2)], TS, 0);
        buf.append(vec![node_created(3)], TS, 0);

        let result = buf.since(1).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].sequence, 2);
        assert_eq!(result[1].sequence, 3);
    }

    // 4 ─────────────────────────────────────────────────────────────────────
    #[test]
    fn since_zero_returns_all() {
        let mut buf = ChangelogBuffer::new(10);
        buf.append(vec![node_created(1)], TS, 0);
        buf.append(vec![node_created(2)], TS, 0);

        let result = buf.since(0).unwrap();
        assert_eq!(result.len(), 2);
    }

    // 5 ─────────────────────────────────────────────────────────────────────
    #[test]
    fn since_current_returns_empty() {
        let mut buf = ChangelogBuffer::new(10);
        buf.append(vec![node_created(1)], TS, 0);
        let cur = buf.current_sequence();

        let result = buf.since(cur).unwrap();
        assert!(result.is_empty());
    }

    // 6 ─────────────────────────────────────────────────────────────────────
    #[test]
    fn since_future_returns_empty() {
        let mut buf = ChangelogBuffer::new(10);
        buf.append(vec![node_created(1)], TS, 0);

        let result = buf.since(999).unwrap();
        assert!(result.is_empty());
    }

    // 7 ─────────────────────────────────────────────────────────────────────
    #[test]
    fn eviction_on_capacity() {
        let mut buf = ChangelogBuffer::new(3);
        buf.append(vec![node_created(1)], TS, 0); // seq 1
        buf.append(vec![node_created(2)], TS, 0); // seq 2
        buf.append(vec![node_created(3)], TS, 0); // seq 3
        buf.append(vec![node_created(4)], TS, 0); // seq 4 — evicts seq 1

        assert_eq!(buf.len(), 3);
        // oldest retained is now seq 2
        let result = buf.since(0).unwrap();
        assert_eq!(result[0].sequence, 2);
    }

    // 8 ─────────────────────────────────────────────────────────────────────
    #[test]
    fn since_evicted_returns_none() {
        let mut buf = ChangelogBuffer::new(2);
        buf.append(vec![node_created(1)], TS, 0); // seq 1
        buf.append(vec![node_created(2)], TS, 0); // seq 2
        buf.append(vec![node_created(3)], TS, 0); // seq 3 — evicts seq 1

        // Asking for changes after seq 0 would be fine (0 means "all"),
        // but asking after the evicted seq 1 should return None because
        // seq 1 itself is gone and we cannot guarantee completeness.
        assert!(buf.since(1).is_none());
    }

    // 9 ─────────────────────────────────────────────────────────────────────
    #[test]
    fn append_records_change_details() {
        let mut buf = ChangelogBuffer::new(10);

        let changes = vec![
            Change::NodeCreated { node_id: NodeId(7) },
            Change::LabelAdded {
                node_id: NodeId(7),
                label: IStr::new("Sensor"),
            },
            Change::PropertySet {
                node_id: NodeId(7),
                key: IStr::new("temp"),
                value: Value::Float(22.5),
                old_value: None,
            },
            Change::EdgeCreated {
                edge_id: EdgeId(1),
                source: NodeId(7),
                target: NodeId(8),
                label: IStr::new("feeds"),
            },
            Change::EdgePropertySet {
                edge_id: EdgeId(1),
                source: NodeId(7),
                target: NodeId(8),
                key: IStr::new("weight"),
                value: Value::Float(1.0),
                old_value: None,
            },
        ];

        let seq = buf.append(changes, TS, 0);
        let result = buf.since(0).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].sequence, seq);
        assert_eq!(result[0].changes.len(), 5);
        assert_eq!(result[0].timestamp_nanos, TS);

        // Spot-check one variant.
        assert!(matches!(
            result[0].changes[0],
            Change::NodeCreated { node_id: NodeId(7) }
        ));
    }

    // 10 ────────────────────────────────────────────────────────────────────
    #[test]
    fn entity_index_tracks_changes() {
        let mut buf = ChangelogBuffer::new(10);
        buf.append(vec![node_created(1)], 100, 0); // seq 1
        buf.append(vec![node_created(2)], 200, 0); // seq 2
        buf.append(
            vec![Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("temp"),
                value: Value::Float(22.0),
                old_value: None,
            }],
            300,
            0,
        ); // seq 3 -- node 1 again

        let history = buf.entity_history(1, None, None);
        assert_eq!(history.len(), 2); // seq 1 (created) + seq 3 (property set)
        assert_eq!(history[0].sequence, 1);
        assert_eq!(history[1].sequence, 3);

        // Node 2 only appears once
        let history2 = buf.entity_history(2, None, None);
        assert_eq!(history2.len(), 1);
    }

    // 11 ────────────────────────────────────────────────────────────────────
    #[test]
    fn entity_index_time_filter() {
        let mut buf = ChangelogBuffer::new(10);
        buf.append(vec![node_created(1)], 100, 0);
        buf.append(
            vec![Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("temp"),
                value: Value::Float(22.0),
                old_value: None,
            }],
            300,
            0,
        );

        // Only changes after timestamp 200
        let history = buf.entity_history(1, Some(200), None);
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].timestamp_nanos, 300);

        // Only changes before timestamp 200
        let history = buf.entity_history(1, None, Some(200));
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].timestamp_nanos, 100);
    }

    // 12 ────────────────────────────────────────────────────────────────────
    #[test]
    fn entity_index_cleaned_on_eviction() {
        let mut buf = ChangelogBuffer::new(2);
        buf.append(vec![node_created(1)], TS, 0); // seq 1
        buf.append(vec![node_created(2)], TS, 0); // seq 2
        buf.append(vec![node_created(3)], TS, 0); // seq 3 — evicts seq 1

        // Node 1's only entry (seq 1) was evicted
        let history = buf.entity_history(1, None, None);
        assert!(history.is_empty());

        // Node 2 still present
        let history = buf.entity_history(2, None, None);
        assert_eq!(history.len(), 1);
    }
}
