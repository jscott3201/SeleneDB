//! Per-property HLC tracker for last-write-wins conflict resolution.
//!
//! Used by the hub node to decide whether an incoming change from an
//! edge should overwrite the hub's current value. Each property,
//! label, and deletion is tracked independently by its most recent
//! HLC timestamp. Ties go to the hub (Skip).

use std::collections::HashMap;

use selene_core::IStr;
use selene_core::changeset::Change;
use selene_graph::changelog::ChangelogEntry;

/// Outcome of a last-write-wins comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeDecision {
    /// Incoming HLC is strictly newer; apply the change.
    Apply,
    /// Local HLC is newer or equal (tie goes to hub).
    Skip,
}

/// Tracks the most recent HLC timestamp for every (entity, property),
/// (entity, label), and entity deletion so the hub can perform
/// per-field LWW resolution against incoming edge changes.
#[derive(Debug, Clone, Default)]
pub struct MergeTracker {
    /// (entity_id, property_key) -> last write HLC.
    properties: HashMap<(u64, IStr), u64>,
    /// (entity_id, label) -> last write HLC.
    labels: HashMap<(u64, IStr), u64>,
    /// entity_id -> deletion HLC.
    deletions: HashMap<u64, u64>,
}

impl MergeTracker {
    /// Create an empty tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a property write for the given entity.
    pub fn record_property(&mut self, entity_id: u64, key: IStr, hlc: u64) {
        self.properties
            .entry((entity_id, key))
            .and_modify(|existing| {
                if hlc > *existing {
                    *existing = hlc;
                }
            })
            .or_insert(hlc);
    }

    /// Record a label change for the given entity.
    pub fn record_label(&mut self, entity_id: u64, label: IStr, hlc: u64) {
        self.labels
            .entry((entity_id, label))
            .and_modify(|existing| {
                if hlc > *existing {
                    *existing = hlc;
                }
            })
            .or_insert(hlc);
    }

    /// Record an entity deletion.
    pub fn record_deletion(&mut self, entity_id: u64, hlc: u64) {
        self.deletions
            .entry(entity_id)
            .and_modify(|existing| {
                if hlc > *existing {
                    *existing = hlc;
                }
            })
            .or_insert(hlc);
    }

    /// Decide whether an incoming change should be applied based on LWW.
    ///
    /// The rule is simple: if `incoming_hlc` is strictly greater than the
    /// recorded HLC for the same field, the change wins. Otherwise the
    /// hub's value stands (tie goes to hub / Skip).
    pub fn should_apply(&self, change: &Change, incoming_hlc: u64) -> MergeDecision {
        match change {
            // Property changes on nodes
            Change::PropertySet { node_id, key, .. }
            | Change::PropertyRemoved { node_id, key, .. } => {
                self.decide_property(node_id.0, *key, incoming_hlc)
            }

            // Property changes on edges
            Change::EdgePropertySet { edge_id, key, .. }
            | Change::EdgePropertyRemoved { edge_id, key, .. } => {
                self.decide_property(edge_id.0, *key, incoming_hlc)
            }

            // Label changes
            Change::LabelAdded { node_id, label } | Change::LabelRemoved { node_id, label } => {
                self.decide_label(node_id.0, *label, incoming_hlc)
            }

            // Creations are idempotent; always apply.
            Change::NodeCreated { .. } | Change::EdgeCreated { .. } => MergeDecision::Apply,

            // Deletions
            Change::NodeDeleted { node_id, .. } => self.decide_deletion(node_id.0, incoming_hlc),
            Change::EdgeDeleted { edge_id, .. } => self.decide_deletion(edge_id.0, incoming_hlc),
        }
    }

    /// Record all changes in a batch under a single HLC.
    pub fn record_batch(&mut self, changes: &[Change], hlc: u64) {
        for change in changes {
            match change {
                Change::PropertySet { node_id, key, .. }
                | Change::PropertyRemoved { node_id, key, .. } => {
                    self.record_property(node_id.0, *key, hlc);
                }

                Change::EdgePropertySet { edge_id, key, .. }
                | Change::EdgePropertyRemoved { edge_id, key, .. } => {
                    self.record_property(edge_id.0, *key, hlc);
                }

                Change::LabelAdded { node_id, label } | Change::LabelRemoved { node_id, label } => {
                    self.record_label(node_id.0, *label, hlc);
                }

                Change::NodeDeleted { node_id, .. } => {
                    self.record_deletion(node_id.0, hlc);
                    // Evict property and label entries for deleted entity
                    self.properties.retain(|(eid, _), _| *eid != node_id.0);
                    self.labels.retain(|(eid, _), _| *eid != node_id.0);
                }
                Change::EdgeDeleted { edge_id, .. } => {
                    self.record_deletion(edge_id.0, hlc);
                    // Evict property entries for deleted edge
                    self.properties.retain(|(eid, _), _| *eid != edge_id.0);
                }

                // Creations don't need tracking (always idempotent).
                Change::NodeCreated { .. } | Change::EdgeCreated { .. } => {}
            }
        }
    }

    /// Initialize the tracker from existing changelog entries.
    pub fn backfill_from_changelog(&mut self, entries: &[ChangelogEntry]) {
        for entry in entries {
            if entry.hlc_timestamp > 0 {
                self.record_batch(&entry.changes, entry.hlc_timestamp);
            }
        }
    }

    /// Returns `true` if no entries have been recorded.
    pub fn is_empty(&self) -> bool {
        self.properties.is_empty() && self.labels.is_empty() && self.deletions.is_empty()
    }

    /// Look up the recorded HLC for a specific property on an entity.
    ///
    /// Useful for conflict reporting.
    pub fn property_hlc_for(&self, entity_id: u64, key: IStr) -> Option<u64> {
        self.properties.get(&(entity_id, key)).copied()
    }

    /// Returns `true` if a deletion has been recorded for the given entity.
    #[allow(dead_code)]
    pub fn has_deletion(&self, entity_id: u64) -> bool {
        self.deletions.contains_key(&entity_id)
    }

    // ── private helpers ─────────────────────────────────────────────────

    fn decide_property(&self, entity_id: u64, key: IStr, incoming_hlc: u64) -> MergeDecision {
        match self.properties.get(&(entity_id, key)) {
            Some(&current) if incoming_hlc > current => MergeDecision::Apply,
            Some(_) => MergeDecision::Skip,
            None => MergeDecision::Apply,
        }
    }

    fn decide_label(&self, entity_id: u64, label: IStr, incoming_hlc: u64) -> MergeDecision {
        match self.labels.get(&(entity_id, label)) {
            Some(&current) if incoming_hlc > current => MergeDecision::Apply,
            Some(_) => MergeDecision::Skip,
            None => MergeDecision::Apply,
        }
    }

    fn decide_deletion(&self, entity_id: u64, incoming_hlc: u64) -> MergeDecision {
        match self.deletions.get(&entity_id) {
            Some(&current) if incoming_hlc > current => MergeDecision::Apply,
            Some(_) => MergeDecision::Skip,
            None => MergeDecision::Apply,
        }
    }
}

#[cfg(test)]
mod tests {
    use selene_core::IStr;
    use selene_core::changeset::Change;
    use selene_core::entity::{EdgeId, NodeId};
    use selene_core::value::Value;

    use super::*;

    // 1 ─────────────────────────────────────────────────────────────────
    #[test]
    fn property_lww_newer_wins() {
        let mut tracker = MergeTracker::new();
        let key = IStr::new("temperature");

        // Hub records a property write at HLC 100.
        tracker.record_property(1, key, 100);

        let change = Change::PropertySet {
            node_id: NodeId(1),
            key,
            value: Value::Float(22.5),
            old_value: None,
        };

        // Edge sends HLC 150 (newer) -> Apply.
        assert_eq!(tracker.should_apply(&change, 150), MergeDecision::Apply);

        // Edge sends HLC 50 (older) -> Skip.
        assert_eq!(tracker.should_apply(&change, 50), MergeDecision::Skip);

        // Edge sends HLC 100 (tie) -> Skip (tie goes to hub).
        assert_eq!(tracker.should_apply(&change, 100), MergeDecision::Skip);
    }

    // 2 ─────────────────────────────────────────────────────────────────
    #[test]
    fn delete_wins() {
        let tracker = MergeTracker::new();

        let change = Change::NodeDeleted {
            node_id: NodeId(5),
            labels: vec![IStr::new("Sensor")],
        };

        // No existing deletion recorded -> Apply.
        assert_eq!(tracker.should_apply(&change, 200), MergeDecision::Apply);
    }

    // 3 ─────────────────────────────────────────────────────────────────
    #[test]
    fn node_created_always_applies() {
        let mut tracker = MergeTracker::new();

        // Even with a property recorded for the same node, creation is idempotent.
        tracker.record_property(10, IStr::new("x"), 500);

        let change = Change::NodeCreated {
            node_id: NodeId(10),
        };

        assert_eq!(tracker.should_apply(&change, 1), MergeDecision::Apply);
        assert_eq!(tracker.should_apply(&change, 999), MergeDecision::Apply);
    }

    // 4 ─────────────────────────────────────────────────────────────────
    #[test]
    fn record_batch_tracks_all_change_types() {
        let mut tracker = MergeTracker::new();

        let prop_key = IStr::new("temp");
        let label = IStr::new("Sensor");
        let edge_key = IStr::new("weight");

        let changes = vec![
            Change::PropertySet {
                node_id: NodeId(1),
                key: prop_key,
                value: Value::Float(21.0),
                old_value: None,
            },
            Change::LabelAdded {
                node_id: NodeId(1),
                label,
            },
            Change::EdgePropertySet {
                edge_id: EdgeId(100),
                source: NodeId(1),
                target: NodeId(2),
                key: edge_key,
                value: Value::Float(1.0),
                old_value: None,
            },
        ];

        tracker.record_batch(&changes, 300);

        // All three should be tracked at HLC 300.
        assert_eq!(tracker.property_hlc_for(1, prop_key), Some(300));
        assert_eq!(tracker.property_hlc_for(100, edge_key), Some(300));

        // Verify label tracking via should_apply: HLC 200 (older) -> Skip.
        let label_change = Change::LabelAdded {
            node_id: NodeId(1),
            label,
        };
        assert_eq!(
            tracker.should_apply(&label_change, 200),
            MergeDecision::Skip
        );

        // HLC 400 (newer) -> Apply.
        assert_eq!(
            tracker.should_apply(&label_change, 400),
            MergeDecision::Apply
        );

        assert!(!tracker.is_empty());
    }

    // 5 ─────────────────────────────────────────────────────────────────
    #[test]
    fn deletion_evicts_property_and_label_entries() {
        let mut tracker = MergeTracker::new();
        let key = IStr::new("temp");
        let label = IStr::new("Sensor");

        // Record some property and label writes for node 1.
        tracker.record_property(1, key, 100);
        tracker.record_label(1, label, 100);
        assert!(tracker.property_hlc_for(1, key).is_some());

        // Delete node 1.
        let changes = vec![Change::NodeDeleted {
            node_id: NodeId(1),
            labels: vec![label],
        }];
        tracker.record_batch(&changes, 200);

        // Property and label entries should be evicted.
        assert!(tracker.property_hlc_for(1, key).is_none());
        // Deletion entry should exist.
        assert!(tracker.has_deletion(1));
    }

    // 6 ─────────────────────────────────────────────────────────────────
    #[test]
    fn edge_deletion_evicts_property_entries() {
        let mut tracker = MergeTracker::new();
        let key = IStr::new("weight");

        // Record a property write for edge 50.
        tracker.record_property(50, key, 100);
        assert!(tracker.property_hlc_for(50, key).is_some());

        // Delete edge 50.
        let changes = vec![Change::EdgeDeleted {
            edge_id: EdgeId(50),
            source: NodeId(1),
            target: NodeId(2),
            label: IStr::new("CONNECTS"),
        }];
        tracker.record_batch(&changes, 200);

        // Property entry should be evicted.
        assert!(tracker.property_hlc_for(50, key).is_none());
        // Deletion entry should exist.
        assert!(tracker.has_deletion(50));
    }

    // 7 ─────────────────────────────────────────────────────────────────
    /// Fresh tracker is empty.
    #[test]
    fn new_tracker_is_empty() {
        let tracker = MergeTracker::new();
        assert!(tracker.is_empty());
    }

    // 8 ─────────────────────────────────────────────────────────────────
    /// has_deletion returns false for an entity with no recorded deletion.
    #[test]
    fn has_deletion_false_for_unknown_entity() {
        let tracker = MergeTracker::new();
        assert!(!tracker.has_deletion(999));
    }

    // 9 ─────────────────────────────────────────────────────────────────
    /// property_hlc_for returns None for an untracked entity/key pair.
    #[test]
    fn property_hlc_for_unknown_returns_none() {
        let tracker = MergeTracker::new();
        assert!(tracker.property_hlc_for(1, IStr::new("missing")).is_none());
    }

    // 10 ────────────────────────────────────────────────────────────────
    /// record_property: older HLC does not overwrite a newer one.
    #[test]
    fn record_property_ignores_older_hlc() {
        let mut tracker = MergeTracker::new();
        let key = IStr::new("temp");

        tracker.record_property(1, key, 200);
        tracker.record_property(1, key, 100);

        assert_eq!(tracker.property_hlc_for(1, key), Some(200));
    }

    // 11 ────────────────────────────────────────────────────────────────
    /// record_label: older HLC does not overwrite a newer one.
    #[test]
    fn record_label_ignores_older_hlc() {
        let mut tracker = MergeTracker::new();
        let label = IStr::new("Sensor");

        tracker.record_label(1, label, 500);
        tracker.record_label(1, label, 300);

        // Verify via should_apply: HLC 400 is older than 500, should be skipped.
        let change = Change::LabelAdded {
            node_id: NodeId(1),
            label,
        };
        assert_eq!(tracker.should_apply(&change, 400), MergeDecision::Skip);
        assert_eq!(tracker.should_apply(&change, 600), MergeDecision::Apply);
    }

    // 12 ────────────────────────────────────────────────────────────────
    /// record_deletion: older HLC does not overwrite a newer one.
    #[test]
    fn record_deletion_ignores_older_hlc() {
        let mut tracker = MergeTracker::new();

        tracker.record_deletion(1, 500);
        tracker.record_deletion(1, 300);

        // A deletion at HLC 400 should be skipped (500 is newer).
        let change = Change::NodeDeleted {
            node_id: NodeId(1),
            labels: vec![],
        };
        assert_eq!(tracker.should_apply(&change, 400), MergeDecision::Skip);
    }

    // 13 ────────────────────────────────────────────────────────────────
    /// PropertyRemoved follows the same LWW logic as PropertySet.
    #[test]
    fn property_removed_follows_lww() {
        let mut tracker = MergeTracker::new();
        let key = IStr::new("temp");

        tracker.record_property(1, key, 100);

        let removal = Change::PropertyRemoved {
            node_id: NodeId(1),
            key,
            old_value: Some(Value::Float(22.0)),
        };

        assert_eq!(tracker.should_apply(&removal, 50), MergeDecision::Skip);
        assert_eq!(tracker.should_apply(&removal, 200), MergeDecision::Apply);
    }

    // 14 ────────────────────────────────────────────────────────────────
    /// EdgePropertyRemoved follows the same LWW logic.
    #[test]
    fn edge_property_removed_follows_lww() {
        let mut tracker = MergeTracker::new();
        let key = IStr::new("weight");

        tracker.record_property(42, key, 300);

        let removal = Change::EdgePropertyRemoved {
            edge_id: EdgeId(42),
            source: NodeId(1),
            target: NodeId(2),
            key,
            old_value: None,
        };

        assert_eq!(tracker.should_apply(&removal, 200), MergeDecision::Skip);
        assert_eq!(tracker.should_apply(&removal, 400), MergeDecision::Apply);
    }

    // 15 ────────────────────────────────────────────────────────────────
    /// LabelRemoved follows the same LWW logic as LabelAdded.
    #[test]
    fn label_removed_follows_lww() {
        let mut tracker = MergeTracker::new();
        let label = IStr::new("Active");

        tracker.record_label(1, label, 100);

        let removal = Change::LabelRemoved {
            node_id: NodeId(1),
            label,
        };

        assert_eq!(tracker.should_apply(&removal, 50), MergeDecision::Skip);
        assert_eq!(tracker.should_apply(&removal, 200), MergeDecision::Apply);
    }

    // 16 ────────────────────────────────────────────────────────────────
    /// EdgeCreated always applies, same as NodeCreated.
    #[test]
    fn edge_created_always_applies() {
        let tracker = MergeTracker::new();
        let change = Change::EdgeCreated {
            edge_id: EdgeId(10),
            source: NodeId(1),
            target: NodeId(2),
            label: IStr::new("feeds"),
        };

        assert_eq!(tracker.should_apply(&change, 1), MergeDecision::Apply);
        assert_eq!(
            tracker.should_apply(&change, u64::MAX),
            MergeDecision::Apply
        );
    }

    // 17 ────────────────────────────────────────────────────────────────
    /// EdgeDeleted follows LWW against recorded deletions.
    #[test]
    fn edge_deleted_follows_lww() {
        let mut tracker = MergeTracker::new();
        tracker.record_deletion(10, 200);

        let change = Change::EdgeDeleted {
            edge_id: EdgeId(10),
            source: NodeId(1),
            target: NodeId(2),
            label: IStr::new("feeds"),
        };

        assert_eq!(tracker.should_apply(&change, 100), MergeDecision::Skip);
        assert_eq!(tracker.should_apply(&change, 300), MergeDecision::Apply);
    }

    // 18 ────────────────────────────────────────────────────────────────
    /// backfill_from_changelog with empty entries leaves tracker empty.
    #[test]
    fn backfill_from_empty_changelog() {
        let mut tracker = MergeTracker::new();
        tracker.backfill_from_changelog(&[]);
        assert!(tracker.is_empty());
    }

    // 19 ────────────────────────────────────────────────────────────────
    /// backfill_from_changelog skips entries with hlc_timestamp == 0.
    #[test]
    fn backfill_skips_zero_hlc_entries() {
        use selene_graph::changelog::ChangelogEntry;

        let mut tracker = MergeTracker::new();

        let entries = vec![ChangelogEntry {
            sequence: 1,
            timestamp_nanos: 0,
            hlc_timestamp: 0,
            changes: vec![Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("temp"),
                value: Value::Float(22.0),
                old_value: None,
            }],
        }];

        tracker.backfill_from_changelog(&entries);
        assert!(
            tracker.is_empty(),
            "entries with hlc_timestamp == 0 must be skipped"
        );
    }

    // 20 ────────────────────────────────────────────────────────────────
    /// Different properties on the same entity are tracked independently.
    #[test]
    fn independent_property_tracking() {
        let mut tracker = MergeTracker::new();
        let key_a = IStr::new("temp");
        let key_b = IStr::new("humidity");

        tracker.record_property(1, key_a, 100);
        tracker.record_property(1, key_b, 200);

        assert_eq!(tracker.property_hlc_for(1, key_a), Some(100));
        assert_eq!(tracker.property_hlc_for(1, key_b), Some(200));

        // A newer write on key_a should not affect key_b.
        let change_a = Change::PropertySet {
            node_id: NodeId(1),
            key: key_a,
            value: Value::Float(1.0),
            old_value: None,
        };
        let change_b = Change::PropertySet {
            node_id: NodeId(1),
            key: key_b,
            value: Value::Float(1.0),
            old_value: None,
        };

        assert_eq!(tracker.should_apply(&change_a, 150), MergeDecision::Apply);
        assert_eq!(tracker.should_apply(&change_b, 150), MergeDecision::Skip);
    }
}
