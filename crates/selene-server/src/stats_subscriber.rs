//! Incremental label statistics maintained via graph change events.
//!
//! Subscribes to graph changes and maintains per-label node/edge counts
//! without full graph scans. Dashboard queries for count-by-label become
//! O(1) lookups.

use std::collections::HashMap;

use parking_lot::RwLock;
use selene_core::IStr;

/// Per-label statistics maintained incrementally.
#[derive(Debug, Clone, Default)]
pub struct LabelStats {
    pub node_count: i64,
    pub edge_count: i64,
}

/// Collects per-label statistics from graph change events.
///
/// Thread-safe via `RwLock`. Updated by the changelog consumer,
/// read by query procedures.
pub struct StatsCollector {
    stats: RwLock<HashMap<IStr, LabelStats>>,
}

impl StatsCollector {
    pub fn new() -> Self {
        Self {
            stats: RwLock::new(HashMap::new()),
        }
    }

    /// Get a snapshot of current label statistics.
    #[cfg(test)]
    pub fn snapshot(&self) -> HashMap<IStr, LabelStats> {
        self.stats.read().clone()
    }

    /// Get stats for a specific label.
    #[cfg(test)]
    pub fn get(&self, label: IStr) -> Option<LabelStats> {
        self.stats.read().get(&label).cloned()
    }

    /// Increment node count for a label.
    pub fn increment_node(&self, label: IStr) {
        self.stats.write().entry(label).or_default().node_count += 1;
    }

    /// Decrement node count for a label.
    pub fn decrement_node(&self, label: IStr) {
        let mut stats = self.stats.write();
        if let Some(entry) = stats.get_mut(&label) {
            entry.node_count = (entry.node_count - 1).max(0);
        }
    }

    /// Increment edge count for a label.
    pub fn increment_edge(&self, label: IStr) {
        self.stats.write().entry(label).or_default().edge_count += 1;
    }

    /// Decrement edge count for a label.
    pub fn decrement_edge(&self, label: IStr) {
        let mut stats = self.stats.write();
        if let Some(entry) = stats.get_mut(&label) {
            entry.edge_count = (entry.edge_count - 1).max(0);
        }
    }

    /// Build initial stats from a graph snapshot.
    /// Call this on startup before processing changes.
    pub fn rebuild_from_graph(
        &self,
        node_label_counts: HashMap<IStr, i64>,
        edge_label_counts: HashMap<IStr, i64>,
    ) {
        let mut stats = self.stats.write();
        stats.clear();
        for (label, count) in node_label_counts {
            stats.entry(label).or_default().node_count = count;
        }
        for (label, count) in edge_label_counts {
            stats.entry(label).or_default().edge_count = count;
        }
    }
}

impl crate::service_registry::Service for StatsCollector {
    fn name(&self) -> &'static str {
        "stats"
    }

    fn health(&self) -> crate::service_registry::ServiceHealth {
        crate::service_registry::ServiceHealth::Healthy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn increment_decrement_nodes() {
        let collector = StatsCollector::new();
        let label = IStr::new("Sensor");

        collector.increment_node(label);
        collector.increment_node(label);
        assert_eq!(collector.get(label).unwrap().node_count, 2);

        collector.decrement_node(label);
        assert_eq!(collector.get(label).unwrap().node_count, 1);
    }

    #[test]
    fn increment_decrement_edges() {
        let collector = StatsCollector::new();
        let label = IStr::new("CONTAINS");

        collector.increment_edge(label);
        collector.increment_edge(label);
        collector.increment_edge(label);
        assert_eq!(collector.get(label).unwrap().edge_count, 3);

        collector.decrement_edge(label);
        assert_eq!(collector.get(label).unwrap().edge_count, 2);
    }

    #[test]
    fn snapshot_returns_all_labels() {
        let collector = StatsCollector::new();
        collector.increment_node(IStr::new("Sensor"));
        collector.increment_node(IStr::new("Sensor"));
        collector.increment_node(IStr::new("Room"));
        collector.increment_edge(IStr::new("CONTAINS"));

        let snap = collector.snapshot();
        assert_eq!(snap.get(&IStr::new("Sensor")).unwrap().node_count, 2);
        assert_eq!(snap.get(&IStr::new("Room")).unwrap().node_count, 1);
        assert_eq!(snap.get(&IStr::new("CONTAINS")).unwrap().edge_count, 1);
    }

    #[test]
    fn decrement_below_zero_clamps() {
        let collector = StatsCollector::new();
        let label = IStr::new("Ghost");
        collector.decrement_node(label);
        // Never incremented, so entry was never created.
        assert!(collector.get(label).is_none());
    }

    #[test]
    fn rebuild_from_graph() {
        let collector = StatsCollector::new();
        collector.increment_node(IStr::new("stale"));

        let mut nodes = HashMap::new();
        nodes.insert(IStr::new("Sensor"), 100);
        nodes.insert(IStr::new("Room"), 50);
        let mut edges = HashMap::new();
        edges.insert(IStr::new("CONTAINS"), 200);

        collector.rebuild_from_graph(nodes, edges);

        let snap = collector.snapshot();
        assert!(!snap.contains_key(&IStr::new("stale"))); // cleared
        assert_eq!(snap.get(&IStr::new("Sensor")).unwrap().node_count, 100);
        assert_eq!(snap.get(&IStr::new("Room")).unwrap().node_count, 50);
        assert_eq!(snap.get(&IStr::new("Room")).unwrap().edge_count, 0);
        assert_eq!(snap.get(&IStr::new("CONTAINS")).unwrap().edge_count, 200);
    }

    #[test]
    fn mixed_node_and_edge_counts_on_same_label() {
        let collector = StatsCollector::new();
        let label = IStr::new("Connection");

        collector.increment_node(label);
        collector.increment_edge(label);
        collector.increment_edge(label);

        let stats = collector.get(label).unwrap();
        assert_eq!(stats.node_count, 1);
        assert_eq!(stats.edge_count, 2);
    }

    #[test]
    fn get_unknown_label_returns_none() {
        let collector = StatsCollector::new();
        assert!(collector.get(IStr::new("NonExistent")).is_none());
    }
}
