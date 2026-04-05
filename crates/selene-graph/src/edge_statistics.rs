//! Lightweight degree statistics per (source_label, edge_type, target_label) triple.
//!
//! Built by scanning all edges in the graph and grouping by the label triple.
//! Used by the GQL planner for cardinality estimation and scan direction choice.
//! Invalidated when the graph's generation counter changes.

use std::collections::HashMap;

use selene_core::IStr;

use crate::SeleneGraph;

/// Degree statistics for a single (source_label, edge_type, target_label) triple.
#[derive(Debug, Clone)]
pub struct DegreeStats {
    /// Total number of edges with this triple.
    pub edge_count: u64,
    /// Number of distinct source nodes.
    pub source_count: u64,
    /// Average outgoing degree: edge_count / source_count.
    pub avg_out_degree: f64,
    /// Maximum outgoing degree observed for any single source node.
    pub max_out_degree: u64,
}

/// Cached edge statistics keyed by (source_label, edge_type, target_label).
///
/// Build cost is O(E) where E is the number of edges. At 30K edges on a 10K-node
/// building graph, this takes <1ms. Statistics are cached and invalidated when
/// the graph's generation counter changes.
#[derive(Debug)]
pub struct EdgeStatistics {
    stats: HashMap<(IStr, IStr, IStr), DegreeStats>,
    generation: u64,
}

impl EdgeStatistics {
    /// Build statistics by scanning all edges in the graph.
    ///
    /// For each edge, all combinations of source and target labels are used
    /// as grouping keys along with the edge label. This ensures multi-labeled
    /// nodes contribute statistics under every label. Nodes with no labels are
    /// skipped (they don't participate in label-based planning).
    pub fn build(graph: &SeleneGraph) -> Self {
        // Intermediate accumulator: triple -> (per-source counts)
        let mut accum: HashMap<(IStr, IStr, IStr), HashMap<u64, u64>> = HashMap::new();

        let edge_bitmap = graph.all_edge_bitmap();
        for eid_u32 in &edge_bitmap {
            let eid = selene_core::EdgeId(u64::from(eid_u32));
            let Some(edge) = graph.get_edge(eid) else {
                continue;
            };

            let src_labels: Vec<IStr> = graph
                .get_node(edge.source)
                .map(|n| n.labels.iter().collect())
                .unwrap_or_default();
            let tgt_labels: Vec<IStr> = graph
                .get_node(edge.target)
                .map(|n| n.labels.iter().collect())
                .unwrap_or_default();

            if src_labels.is_empty() || tgt_labels.is_empty() {
                continue;
            }

            for &sl in &src_labels {
                for &tl in &tgt_labels {
                    let triple = (sl, edge.label, tl);
                    accum
                        .entry(triple)
                        .or_default()
                        .entry(edge.source.0)
                        .and_modify(|c| *c += 1)
                        .or_insert(1);
                }
            }
        }

        let mut stats = HashMap::with_capacity(accum.len());
        for (triple, per_source) in accum {
            let edge_count: u64 = per_source.values().sum();
            let source_count = per_source.len() as u64;
            let max_out_degree = per_source.values().copied().max().unwrap_or(0);
            let avg_out_degree = if source_count > 0 {
                edge_count as f64 / source_count as f64
            } else {
                0.0
            };
            stats.insert(
                triple,
                DegreeStats {
                    edge_count,
                    source_count,
                    avg_out_degree,
                    max_out_degree,
                },
            );
        }

        Self {
            stats,
            generation: graph.generation(),
        }
    }

    /// Look up degree stats for a (source_label, edge_type, target_label) triple.
    pub fn get(&self, src: IStr, edge_type: IStr, tgt: IStr) -> Option<&DegreeStats> {
        self.stats.get(&(src, edge_type, tgt))
    }

    /// Check if statistics are stale (graph generation has changed).
    pub fn is_stale(&self, current_generation: u64) -> bool {
        self.generation != current_generation
    }

    /// Number of distinct triples tracked.
    pub fn len(&self) -> usize {
        self.stats.len()
    }

    /// Whether no statistics have been collected.
    pub fn is_empty(&self) -> bool {
        self.stats.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{LabelSet, PropertyMap};

    fn build_test_graph() -> SeleneGraph {
        let mut graph = SeleneGraph::new();
        let mut mutation = crate::TrackedMutation::new(&mut graph);

        // 3 buildings, 6 floors (2 per building), 12 sensors (2 per floor)
        let b1 = mutation
            .create_node(LabelSet::from_strs(&["building"]), PropertyMap::new())
            .unwrap();
        let b2 = mutation
            .create_node(LabelSet::from_strs(&["building"]), PropertyMap::new())
            .unwrap();
        let b3 = mutation
            .create_node(LabelSet::from_strs(&["building"]), PropertyMap::new())
            .unwrap();

        let mut floors = Vec::new();
        for _ in 0..6 {
            let f = mutation
                .create_node(LabelSet::from_strs(&["floor"]), PropertyMap::new())
                .unwrap();
            floors.push(f);
        }

        let mut sensors = Vec::new();
        for _ in 0..12 {
            let s = mutation
                .create_node(LabelSet::from_strs(&["sensor"]), PropertyMap::new())
                .unwrap();
            sensors.push(s);
        }

        let contains = IStr::new("contains");
        // b1 -> f0, f1; b2 -> f2, f3; b3 -> f4, f5
        mutation
            .create_edge(b1, contains, floors[0], PropertyMap::new())
            .unwrap();
        mutation
            .create_edge(b1, contains, floors[1], PropertyMap::new())
            .unwrap();
        mutation
            .create_edge(b2, contains, floors[2], PropertyMap::new())
            .unwrap();
        mutation
            .create_edge(b2, contains, floors[3], PropertyMap::new())
            .unwrap();
        mutation
            .create_edge(b3, contains, floors[4], PropertyMap::new())
            .unwrap();
        mutation
            .create_edge(b3, contains, floors[5], PropertyMap::new())
            .unwrap();

        // Each floor -> 2 sensors
        for i in 0..6 {
            mutation
                .create_edge(floors[i], contains, sensors[i * 2], PropertyMap::new())
                .unwrap();
            mutation
                .create_edge(floors[i], contains, sensors[i * 2 + 1], PropertyMap::new())
                .unwrap();
        }

        let _ = mutation.commit(0);
        graph
    }

    #[test]
    fn edge_stats_building_to_floor() {
        let graph = build_test_graph();
        let stats = EdgeStatistics::build(&graph);

        let ds = stats
            .get(
                IStr::new("building"),
                IStr::new("contains"),
                IStr::new("floor"),
            )
            .expect("should have building-contains-floor stats");

        assert_eq!(ds.edge_count, 6);
        assert_eq!(ds.source_count, 3);
        assert!((ds.avg_out_degree - 2.0).abs() < f64::EPSILON);
        assert_eq!(ds.max_out_degree, 2);
    }

    #[test]
    fn edge_stats_floor_to_sensor() {
        let graph = build_test_graph();
        let stats = EdgeStatistics::build(&graph);

        let ds = stats
            .get(
                IStr::new("floor"),
                IStr::new("contains"),
                IStr::new("sensor"),
            )
            .expect("should have floor-contains-sensor stats");

        assert_eq!(ds.edge_count, 12);
        assert_eq!(ds.source_count, 6);
        assert!((ds.avg_out_degree - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn edge_stats_nonexistent_triple() {
        let graph = build_test_graph();
        let stats = EdgeStatistics::build(&graph);

        assert!(
            stats
                .get(
                    IStr::new("sensor"),
                    IStr::new("contains"),
                    IStr::new("building"),
                )
                .is_none()
        );
    }

    #[test]
    fn edge_stats_staleness() {
        let graph = build_test_graph();
        let stats = EdgeStatistics::build(&graph);

        assert!(!stats.is_stale(graph.generation()));
        assert!(stats.is_stale(graph.generation() + 1));
    }

    #[test]
    fn edge_stats_empty_graph() {
        let graph = SeleneGraph::new();
        let stats = EdgeStatistics::build(&graph);

        assert!(stats.is_empty());
        assert_eq!(stats.len(), 0);
    }

    #[test]
    fn edge_stats_multi_label_nodes() {
        let mut graph = SeleneGraph::new();
        let mut m = crate::TrackedMutation::new(&mut graph);

        let s1 = m
            .create_node(
                LabelSet::from_strs(&["sensor", "temperature_sensor"]),
                PropertyMap::new(),
            )
            .unwrap();
        let room = m
            .create_node(LabelSet::from_strs(&["room"]), PropertyMap::new())
            .unwrap();
        let monitors = IStr::new("monitors");
        m.create_edge(s1, monitors, room, PropertyMap::new())
            .unwrap();
        let _ = m.commit(0);

        let stats = EdgeStatistics::build(&graph);

        let ds1 = stats
            .get(IStr::new("sensor"), monitors, IStr::new("room"))
            .expect("should have sensor-monitors-room");
        assert_eq!(ds1.edge_count, 1);

        let ds2 = stats
            .get(IStr::new("temperature_sensor"), monitors, IStr::new("room"))
            .expect("should have temperature_sensor-monitors-room");
        assert_eq!(ds2.edge_count, 1);
    }
}
