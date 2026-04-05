//! Graph projections: named subgraph views for algorithm computation.
//!
//! A [`GraphProjection`] is a filtered view of the graph defined by:
//! - A node bitmap (which nodes are included)
//! - An edge label filter (which edge types to include)
//! - An optional weight property key (for weighted algorithms)
//! - Cached CSR adjacency built from the filtered graph
//!
//! Projections are created from `&SeleneGraph` and an optional auth scope bitmap.
//! The scope bitmap is ANDed with the node filter at creation time, ensuring
//! algorithms never see nodes outside the caller's authorization scope.

use std::collections::VecDeque;

use roaring::RoaringBitmap;
use selene_core::{EdgeId, IStr, NodeId};
use selene_graph::SeleneGraph;

/// A neighbor entry in the projection's CSR adjacency.
#[derive(Debug, Clone, Copy)]
pub struct ProjNeighbor {
    pub edge_id: EdgeId,
    pub node_id: NodeId,
    pub weight: f64,
}

/// CSR adjacency for one direction within a projection.
#[derive(Debug)]
struct ProjCsr {
    /// offsets[node_id] = start index in neighbors. Length = max_node + 2.
    offsets: Vec<u32>,
    /// Flat array of neighbor entries.
    neighbors: Vec<ProjNeighbor>,
}

/// Configuration for creating a graph projection.
#[derive(Debug, Clone)]
pub struct ProjectionConfig {
    /// Name for the projection in the catalog.
    pub name: String,
    /// Node labels to include. If empty, include all nodes.
    pub node_labels: Vec<IStr>,
    /// Edge labels to include. If empty, include all edge types.
    pub edge_labels: Vec<IStr>,
    /// Property key to use as edge weight. If None, all weights are 1.0.
    pub weight_property: Option<IStr>,
}

/// A named subgraph view with cached CSR adjacency for fast algorithm traversal.
///
/// The projection is immutable once created. When the underlying graph mutates
/// (generation changes), the projection catalog invalidates and rebuilds.
#[derive(Debug)]
pub struct GraphProjection {
    /// Projection name.
    pub name: String,
    /// Which nodes are part of this projection.
    pub nodes: RoaringBitmap,
    /// Edge labels included (empty = all).
    pub edge_labels: Vec<IStr>,
    /// Property key for edge weights (None = unweighted, all 1.0).
    weight_property: Option<IStr>,
    /// Outgoing CSR adjacency.
    out_csr: ProjCsr,
    /// Incoming CSR adjacency.
    in_csr: ProjCsr,
    /// Graph generation at build time.
    generation: u64,
}

impl GraphProjection {
    /// Create a projection from the graph and configuration.
    ///
    /// If `scope` is provided, AND it with the node filter so algorithms
    /// never see nodes outside the auth scope.
    pub fn build(
        graph: &SeleneGraph,
        config: &ProjectionConfig,
        scope: Option<&RoaringBitmap>,
    ) -> Self {
        let mut nodes = if config.node_labels.is_empty() {
            graph.all_node_bitmap()
        } else {
            let mut bm = RoaringBitmap::new();
            for label in &config.node_labels {
                if let Some(label_bm) = graph.label_bitmap(label.as_str()) {
                    bm |= label_bm;
                }
            }
            bm
        };

        if let Some(scope_bm) = scope {
            nodes &= scope_bm;
        }

        let max_node = if nodes.is_empty() {
            0
        } else {
            u64::from(nodes.max().unwrap_or(0))
        };
        let out_csr = build_proj_csr(
            graph,
            &nodes,
            &config.edge_labels,
            config.weight_property.as_ref(),
            max_node,
            true,
        );
        let in_csr = build_proj_csr(
            graph,
            &nodes,
            &config.edge_labels,
            config.weight_property.as_ref(),
            max_node,
            false,
        );

        Self {
            name: config.name.clone(),
            nodes,
            edge_labels: config.edge_labels.clone(),
            weight_property: config.weight_property,
            out_csr,
            in_csr,
            generation: graph.generation(),
        }
    }

    /// Number of nodes in the projection.
    pub fn node_count(&self) -> u64 {
        self.nodes.len()
    }

    /// Number of edges in the projection (outgoing direction).
    pub fn edge_count(&self) -> usize {
        self.out_csr.neighbors.len()
    }

    /// Get outgoing neighbors for a node.
    pub fn outgoing(&self, node_id: NodeId) -> &[ProjNeighbor] {
        slice_csr(&self.out_csr, node_id)
    }

    /// Get incoming neighbors for a node.
    pub fn incoming(&self, node_id: NodeId) -> &[ProjNeighbor] {
        slice_csr(&self.in_csr, node_id)
    }

    /// Out-degree of a node within this projection.
    pub fn out_degree(&self, node_id: NodeId) -> usize {
        self.outgoing(node_id).len()
    }

    /// In-degree of a node within this projection.
    pub fn in_degree(&self, node_id: NodeId) -> usize {
        self.incoming(node_id).len()
    }

    /// Check if a node is in this projection.
    pub fn contains_node(&self, node_id: NodeId) -> bool {
        self.nodes.contains(node_id.0 as u32)
    }

    /// Iterate over all node IDs in the projection.
    pub fn node_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.nodes.iter().map(|id| NodeId(u64::from(id)))
    }

    /// The graph generation this projection was built from.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Whether this projection has edge weights.
    pub fn is_weighted(&self) -> bool {
        self.weight_property.is_some()
    }

    /// BFS from start node within this projection. Returns nodes in visit order.
    pub fn bfs(&self, start: NodeId, max_depth: u32) -> Vec<NodeId> {
        if !self.contains_node(start) || max_depth == 0 {
            return vec![];
        }
        let mut visited = RoaringBitmap::new();
        visited.insert(start.0 as u32);
        let mut queue = VecDeque::new();
        queue.push_back((start, 0u32));
        let mut result = Vec::new();

        while let Some((current, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }
            for nb in self.outgoing(current) {
                if visited.insert(nb.node_id.0 as u32) {
                    result.push(nb.node_id);
                    queue.push_back((nb.node_id, depth + 1));
                }
            }
        }
        result
    }
}

/// Return the CSR neighbor slice for a given node.
fn slice_csr(csr: &ProjCsr, node_id: NodeId) -> &[ProjNeighbor] {
    let idx = node_id.0 as usize;
    if idx >= csr.offsets.len().saturating_sub(1) {
        return &[];
    }
    let start = csr.offsets[idx] as usize;
    let end = csr.offsets[idx + 1] as usize;
    &csr.neighbors[start..end]
}

/// Build CSR for one direction from the graph, filtered by projection config.
///
/// Two-pass construction: pass 1 counts qualifying neighbors per node into a
/// `Vec<u32>` (4 bytes/slot), then prefix-sums into offsets. Pass 2 fills the
/// flat neighbor array. This uses 6x less memory per slot than the previous
/// Vec-of-Vec approach (4 bytes vs 24 bytes), which matters for sparse
/// projections on graphs with high max node IDs.
fn build_proj_csr(
    graph: &SeleneGraph,
    nodes: &RoaringBitmap,
    edge_labels: &[IStr],
    weight_prop: Option<&IStr>,
    max_node: u64,
    outgoing: bool,
) -> ProjCsr {
    let size = (max_node + 2) as usize;
    let mut offsets = vec![0u32; size];

    // Pass 1: count qualifying neighbors per node.
    for nid_u32 in nodes {
        let nid = NodeId(u64::from(nid_u32));
        let edges = if outgoing {
            graph.outgoing(nid)
        } else {
            graph.incoming(nid)
        };
        for &eid in edges {
            if let Some(edge) = graph.get_edge(eid) {
                let other = if outgoing { edge.target } else { edge.source };
                if !nodes.contains(other.0 as u32) {
                    continue;
                }
                if !edge_labels.is_empty() && !edge_labels.contains(&edge.label) {
                    continue;
                }
                offsets[nid_u32 as usize] += 1;
            }
        }
    }

    // Convert counts to cumulative offsets (prefix sum).
    let mut cumulative = 0u32;
    for offset in &mut offsets {
        let count = *offset;
        *offset = cumulative;
        cumulative += count;
    }

    // Pass 2: fill the flat neighbor array.
    let mut neighbors = vec![
        ProjNeighbor {
            edge_id: EdgeId(0),
            node_id: NodeId(0),
            weight: 0.0,
        };
        cumulative as usize
    ];
    let mut write_pos = offsets.clone();

    for nid_u32 in nodes {
        let nid = NodeId(u64::from(nid_u32));
        let edges = if outgoing {
            graph.outgoing(nid)
        } else {
            graph.incoming(nid)
        };
        for &eid in edges {
            if let Some(edge) = graph.get_edge(eid) {
                let other = if outgoing { edge.target } else { edge.source };
                if !nodes.contains(other.0 as u32) {
                    continue;
                }
                if !edge_labels.is_empty() && !edge_labels.contains(&edge.label) {
                    continue;
                }
                let weight = weight_prop
                    .and_then(|key| edge.properties.get(*key))
                    .and_then(|v| match v {
                        selene_core::Value::Float(f) => Some(*f),
                        selene_core::Value::Int(i) => Some(*i as f64),
                        _ => None,
                    })
                    .unwrap_or(1.0);
                let pos = write_pos[nid_u32 as usize] as usize;
                neighbors[pos] = ProjNeighbor {
                    edge_id: eid,
                    node_id: other,
                    weight,
                };
                write_pos[nid_u32 as usize] += 1;
            }
        }
    }

    // Sentinel: offsets[max_node+1] = total neighbor count.
    // Already correct from prefix sum since cumulative holds the total,
    // and offsets has size = max_node + 2, so the last slot is the sentinel.

    ProjCsr { offsets, neighbors }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{LabelSet, PropertyMap, Value};

    fn test_graph() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();

        // 3 nodes: A(1), B(2), C(3) with labels
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs([(IStr::new("name"), Value::str("s1"))]),
        )
        .unwrap();
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs([(IStr::new("name"), Value::str("s2"))]),
        )
        .unwrap();
        m.create_node(
            LabelSet::from_strs(&["ahu", "equipment"]),
            PropertyMap::from_pairs([(IStr::new("name"), Value::str("ahu1"))]),
        )
        .unwrap();

        // Edges: 1->2 (feeds, weight=10), 1->3 (monitors), 2->3 (feeds, weight=5)
        m.create_edge(
            NodeId(1),
            IStr::new("feeds"),
            NodeId(2),
            PropertyMap::from_pairs([(IStr::new("weight"), Value::Float(10.0))]),
        )
        .unwrap();
        m.create_edge(
            NodeId(1),
            IStr::new("monitors"),
            NodeId(3),
            PropertyMap::new(),
        )
        .unwrap();
        m.create_edge(
            NodeId(2),
            IStr::new("feeds"),
            NodeId(3),
            PropertyMap::from_pairs([(IStr::new("weight"), Value::Float(5.0))]),
        )
        .unwrap();

        m.commit(0).unwrap();
        g
    }

    #[test]
    fn projection_all_nodes() {
        let g = test_graph();
        let config = ProjectionConfig {
            name: "all".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        let proj = GraphProjection::build(&g, &config, None);
        assert_eq!(proj.node_count(), 3);
        assert_eq!(proj.edge_count(), 3);
    }

    #[test]
    fn projection_label_filter() {
        let g = test_graph();
        let config = ProjectionConfig {
            name: "sensors".into(),
            node_labels: vec![IStr::new("sensor")],
            edge_labels: vec![IStr::new("feeds")],
            weight_property: None,
        };
        let proj = GraphProjection::build(&g, &config, None);
        assert_eq!(proj.node_count(), 2); // only sensor nodes
        assert_eq!(proj.edge_count(), 1); // only feeds between sensors: 1->2
    }

    #[test]
    fn projection_weighted() {
        let g = test_graph();
        let config = ProjectionConfig {
            name: "weighted".into(),
            node_labels: vec![],
            edge_labels: vec![IStr::new("feeds")],
            weight_property: Some(IStr::new("weight")),
        };
        let proj = GraphProjection::build(&g, &config, None);
        assert!(proj.is_weighted());
        // Node 1 outgoing via feeds: should have weight 10.0 to node 2
        let out = proj.outgoing(NodeId(1));
        assert_eq!(out.len(), 1); // only feeds edge to node 2 (monitors excluded)
        assert!((out[0].weight - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn projection_scope_filter() {
        let g = test_graph();
        let mut scope = RoaringBitmap::new();
        scope.insert(1);
        scope.insert(2);
        // Scope excludes node 3
        let config = ProjectionConfig {
            name: "scoped".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        let proj = GraphProjection::build(&g, &config, Some(&scope));
        assert_eq!(proj.node_count(), 2);
        // Only edge 1->2 should remain (1->3 and 2->3 excluded because node 3 not in scope)
        assert_eq!(proj.edge_count(), 1);
    }

    #[test]
    fn projection_bfs() {
        let g = test_graph();
        let config = ProjectionConfig {
            name: "all".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        let proj = GraphProjection::build(&g, &config, None);
        let visited = proj.bfs(NodeId(1), 10);
        assert_eq!(visited.len(), 2); // visits 2 and 3
    }

    #[test]
    fn projection_empty_graph() {
        let g = SeleneGraph::new();
        let config = ProjectionConfig {
            name: "empty".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        let proj = GraphProjection::build(&g, &config, None);
        assert_eq!(proj.node_count(), 0);
        assert_eq!(proj.edge_count(), 0);
        assert!(!proj.contains_node(NodeId(1)));
    }

    #[test]
    fn projection_scope_empty_bitmap_excludes_all() {
        let g = test_graph();
        let empty_scope = RoaringBitmap::new();
        let config = ProjectionConfig {
            name: "scoped".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        let proj = GraphProjection::build(&g, &config, Some(&empty_scope));
        assert_eq!(proj.node_count(), 0);
        assert_eq!(proj.edge_count(), 0);
    }

    #[test]
    fn projection_nonexistent_label_filter() {
        let g = test_graph();
        let config = ProjectionConfig {
            name: "none".into(),
            node_labels: vec![IStr::new("nonexistent_label")],
            edge_labels: vec![],
            weight_property: None,
        };
        let proj = GraphProjection::build(&g, &config, None);
        assert_eq!(proj.node_count(), 0);
    }

    #[test]
    fn projection_bfs_nonexistent_start() {
        let g = test_graph();
        let config = ProjectionConfig {
            name: "all".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        let proj = GraphProjection::build(&g, &config, None);
        let visited = proj.bfs(NodeId(999), 10);
        assert!(visited.is_empty());
    }

    #[test]
    fn projection_bfs_depth_zero() {
        let g = test_graph();
        let config = ProjectionConfig {
            name: "all".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        let proj = GraphProjection::build(&g, &config, None);
        let visited = proj.bfs(NodeId(1), 0);
        assert!(visited.is_empty());
    }

    #[test]
    fn projection_bfs_depth_one() {
        let g = test_graph();
        let config = ProjectionConfig {
            name: "all".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        let proj = GraphProjection::build(&g, &config, None);
        let visited = proj.bfs(NodeId(1), 1);
        // Depth 1: only direct neighbors of node 1
        assert_eq!(visited.len(), 2); // nodes 2 and 3
    }

    #[test]
    fn projection_outgoing_nonexistent_node() {
        let g = test_graph();
        let config = ProjectionConfig {
            name: "all".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        let proj = GraphProjection::build(&g, &config, None);
        assert!(proj.outgoing(NodeId(999)).is_empty());
        assert!(proj.incoming(NodeId(999)).is_empty());
    }

    #[test]
    fn projection_degree() {
        let g = test_graph();
        let config = ProjectionConfig {
            name: "all".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        let proj = GraphProjection::build(&g, &config, None);
        assert_eq!(proj.out_degree(NodeId(1)), 2); // feeds + monitors
        assert_eq!(proj.in_degree(NodeId(3)), 2); // from 1 (monitors) + 2 (feeds)
        assert_eq!(proj.out_degree(NodeId(3)), 0); // no outgoing
    }
}
