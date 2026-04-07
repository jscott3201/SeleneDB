//! Variable-length path expansion: ->{min,max} with TRAIL.
//!
//! BFS-based traversal with depth bounds. Uses a parent-pointer arena to
//! avoid per-hop path cloning -- paths are reconstructed only at emit time.
//! TRAIL mode uses `imbl::HashSet` for O(log n) structural sharing.
//! CSR path iterates neighbor slices directly (no Vec allocation).

use std::collections::VecDeque;

use selene_core::{EdgeId, IStr, NodeId};
use smallvec::SmallVec;

use crate::ast::pattern::EdgeDirection;
use crate::types::binding::{Binding, BoundValue};
use crate::types::chunk::{ChunkSchema, ColumnBuilder, ColumnKind, DataChunk};
use crate::types::error::GqlError;
use crate::types::value::GqlPath;

use super::expand::ExpandContext;
use super::scan::label_matches;

// ── Arena-based BFS infrastructure ───────────────────────────────────────────

/// Arena entry for BFS traversal. Parent-pointer tree -- paths are
/// reconstructed by walking parent links only when emitting results.
/// Each entry is 24 bytes (vs ~200+ bytes for the old SmallVec-based BfsState).
struct BfsEntry {
    node: NodeId,
    edge: EdgeId,
    parent: u32, // index into arena (u32::MAX = root)
    depth: u32,
}

struct BfsArena {
    entries: Vec<BfsEntry>,
}

impl BfsArena {
    fn with_capacity(cap: usize) -> Self {
        Self {
            entries: Vec::with_capacity(cap),
        }
    }

    fn push(&mut self, node: NodeId, edge: EdgeId, parent: u32, depth: u32) -> u32 {
        assert!(
            self.entries.len() < u32::MAX as usize,
            "BFS arena size {} exceeds u32 index range",
            self.entries.len()
        );
        let idx = self.entries.len() as u32;
        self.entries.push(BfsEntry {
            node,
            edge,
            parent,
            depth,
        });
        idx
    }

    #[inline]
    fn node_at(&self, idx: u32) -> NodeId {
        self.entries[idx as usize].node
    }

    #[inline]
    fn depth_at(&self, idx: u32) -> u32 {
        self.entries[idx as usize].depth
    }

    /// Walk parent pointers to check if a node appears in the path.
    /// O(depth) -- used only for ACYCLIC/SIMPLE which are uncommon.
    fn path_contains_node(&self, idx: u32, target: NodeId) -> bool {
        let mut cur = idx;
        while cur != u32::MAX {
            if self.entries[cur as usize].node == target {
                return true;
            }
            cur = self.entries[cur as usize].parent;
        }
        false
    }

    /// Reconstruct full path by walking parent pointers. Called only at emit time.
    fn collect_path(&self, idx: u32) -> (SmallVec<[NodeId; 8]>, SmallVec<[EdgeId; 8]>) {
        let mut nodes = SmallVec::new();
        let mut edges = SmallVec::new();
        let mut cur = idx;
        while cur != u32::MAX {
            let entry = &self.entries[cur as usize];
            nodes.push(entry.node);
            if entry.parent != u32::MAX {
                edges.push(entry.edge);
            }
            cur = entry.parent;
        }
        nodes.reverse();
        edges.reverse();
        (nodes, edges)
    }
}

/// Minimal BFS queue state -- arena index + optional TRAIL visited set.
/// Uses `imbl::HashSet` for O(log n) structural sharing on clone.
struct BfsState {
    entry_idx: u32,
    visited_edges: Option<imbl::HashSet<EdgeId>>,
}

// ── Main expansion function ──────────────────────────────────────────────────

/// Configuration for variable-length BFS expansion.
pub(crate) struct VarExpandConfig {
    pub min_hops: u32,
    pub max_hops: Option<u32>,
    pub trail: bool,
    pub acyclic: bool,
    pub simple: bool,
    pub shortest: Option<crate::ast::pattern::PathSelector>,
    pub path_var: Option<IStr>,
}

/// Columnar variable-length expansion producing a DataChunk.
///
/// Delegates to the row-at-a-time BFS (`execute_var_expand`) and converts
/// the output to columnar form. The BFS arena logic is inherently per-path,
/// so the column output is the only change. Full columnar BFS is deferred
/// to Phase 2 factorized representations (#16).
pub(crate) fn execute_var_expand_chunk(
    input: &DataChunk,
    ctx: &ExpandContext<'_>,
    var_cfg: &VarExpandConfig,
) -> Result<DataChunk, GqlError> {
    let bindings = input.to_bindings();
    let result = execute_var_expand(&bindings, ctx, var_cfg)?;

    Ok(bindings_to_chunk(
        &result,
        ctx.source_var,
        ctx.edge_var,
        ctx.target_var,
        var_cfg.path_var,
    ))
}

/// Convert VarExpand output bindings to a DataChunk.
///
/// Source, edge, target variables use typed columns. Path and Group
/// variables use `Column::Values` for full fidelity.
#[allow(dead_code)]
fn bindings_to_chunk(
    bindings: &[Binding],
    source_var: IStr,
    edge_var: Option<IStr>,
    target_var: IStr,
    path_var: Option<IStr>,
) -> DataChunk {
    if bindings.is_empty() {
        let mut schema = ChunkSchema::new();
        let mut builders: Vec<ColumnBuilder> = Vec::new();
        schema.extend(source_var, ColumnKind::NodeId);
        builders.push(ColumnBuilder::new_node_ids(0));
        if let Some(ev) = edge_var {
            schema.extend(ev, ColumnKind::Values);
            builders.push(ColumnBuilder::new_values(0));
        }
        schema.extend(target_var, ColumnKind::NodeId);
        builders.push(ColumnBuilder::new_node_ids(0));
        if let Some(pv) = path_var {
            schema.extend(pv, ColumnKind::Values);
            builders.push(ColumnBuilder::new_values(0));
        }
        return DataChunk::from_builders(builders, schema, 0);
    }

    let len = bindings.len();
    let mut schema = ChunkSchema::new();
    let mut builders: Vec<ColumnBuilder> = Vec::new();

    // Build columns for all variables present in the first binding.
    // VarExpand output always has: source + (optional edge group) + target + (optional path).
    // Plus any parent variables carried from the input binding.
    let first = &bindings[0];
    for (var, val) in first.iter() {
        let (kind, builder) = if *var == source_var || *var == target_var {
            (ColumnKind::NodeId, ColumnBuilder::new_node_ids(len))
        } else if edge_var == Some(*var) {
            // Group -> Values column
            (ColumnKind::Values, ColumnBuilder::new_values(len))
        } else if path_var == Some(*var) {
            // Path -> Values column
            (ColumnKind::Values, ColumnBuilder::new_values(len))
        } else {
            // Parent variable: infer kind from value
            match val {
                BoundValue::Node(_) => (ColumnKind::NodeId, ColumnBuilder::new_node_ids(len)),
                BoundValue::Edge(_) => (ColumnKind::EdgeId, ColumnBuilder::new_edge_ids(len)),
                _ => (ColumnKind::Values, ColumnBuilder::new_values(len)),
            }
        };
        schema.extend(*var, kind);
        builders.push(builder);
    }

    // Fill columns from all bindings
    for binding in bindings {
        for (slot, (var, _)) in first.iter().enumerate() {
            match binding.get(var) {
                Some(bv) => builders[slot].append_bound_value(bv),
                None => builders[slot].append_null(),
            }
        }
    }

    DataChunk::from_builders(builders, schema, len)
}

/// Execute a variable-length edge expansion.
///
/// For each input binding, performs BFS from the source node with depth bounds,
/// edge label filtering, TRAIL mode, group variable, and path variable support.
pub(crate) fn execute_var_expand(
    input: &[Binding],
    ctx: &ExpandContext<'_>,
    var_cfg: &VarExpandConfig,
) -> Result<Vec<Binding>, GqlError> {
    let ExpandContext {
        graph,
        scope,
        csr,
        source_var,
        edge_var,
        target_var,
        edge_labels,
        target_labels,
        direction,
    } = *ctx;
    let VarExpandConfig {
        min_hops,
        max_hops,
        trail,
        acyclic,
        simple,
        shortest,
        path_var,
    } = *var_cfg;
    let max = max_hops.unwrap_or(64); // Safety cap for unbounded

    // Pre-resolve target label bitmap for O(1) membership checks
    let target_bitmap = target_labels.map(|l| super::scan::resolve_label_expr(l, graph));
    let mut output = Vec::new();

    for binding in input {
        let start_id = binding.get_node_id(&source_var)?;
        let output_start = output.len();
        let mut shortest_depth: Option<u32> = None;

        // Arena for parent-pointer BFS -- paths reconstructed only at emit time
        let mut arena = BfsArena::with_capacity(256);
        let mut queue: VecDeque<BfsState> = VecDeque::new();

        let root_idx = arena.push(start_id, EdgeId(0), u32::MAX, 0);
        queue.push_back(BfsState {
            entry_idx: root_idx,
            visited_edges: if trail {
                Some(imbl::HashSet::new())
            } else {
                None
            },
        });

        while let Some(state) = queue.pop_front() {
            let state_node = arena.node_at(state.entry_idx);
            let state_depth = arena.depth_at(state.entry_idx);

            // SHORTEST: if we've found results and moved past shortest depth, stop
            if let Some(sd) = shortest_depth
                && state_depth > sd
            {
                break;
            }

            // Emit bindings for paths within [min, max] range
            if state_depth >= min_hops && state_depth <= max {
                let target_id = state_node;

                // Check target label + scope filters
                let target_ok = match target_bitmap {
                    Some(ref bitmap) => bitmap.contains(target_id.0 as u32),
                    None => true,
                };
                let scope_ok = match scope {
                    Some(s) => s.contains(target_id.0 as u32),
                    None => true,
                };

                if target_ok && scope_ok {
                    if edge_var.is_some() || path_var.is_some() {
                        // Reconstruct path from arena only when caller needs it
                        let (nodes, edges) = arena.collect_path(state.entry_idx);
                        emit_binding(
                            binding,
                            target_var,
                            target_id,
                            edge_var,
                            &edges,
                            path_var,
                            &nodes,
                            &mut output,
                        );
                    } else {
                        // Fast path: just emit target binding without path data
                        let mut new_binding = binding.clone();
                        new_binding.bind(target_var, BoundValue::Node(target_id));
                        output.push(new_binding);
                    }
                }

                // Track shortest depth for SHORTEST selectors
                if shortest.is_some() && output.len() > output_start && shortest_depth.is_none() {
                    shortest_depth = Some(state_depth);
                }
            }

            // Continue BFS if under max depth
            if state_depth >= max {
                continue;
            }

            // Extract state fields before mutable borrows on arena/queue
            let parent_idx = state.entry_idx;
            let next_depth = state_depth + 1;

            // ── Expand neighbors ─────────────────────────────────────────

            // Helper: process a single neighbor candidate
            macro_rules! process_neighbor {
                ($eid:expr, $next:expr, $label:expr) => {{
                    let eid = $eid;
                    let next = $next;
                    let label = $label;

                    // Edge label filter
                    if let Some(labels) = edge_labels {
                        if !label_matches(label, labels, graph) {
                            continue;
                        }
                    }
                    // TRAIL: skip already-visited edges in this path
                    if let Some(ref visited) = state.visited_edges {
                        if visited.contains(&eid) {
                            continue;
                        }
                    }
                    // ACYCLIC: no repeated nodes in path
                    if acyclic && arena.path_contains_node(parent_idx, next) {
                        continue;
                    }
                    // SIMPLE: no repeated nodes except start=end
                    if simple && arena.path_contains_node(parent_idx, next) && next != start_id {
                        continue;
                    }

                    let new_idx = arena.push(next, eid, parent_idx, next_depth);
                    let new_visited = state.visited_edges.as_ref().map(|v| v.update(eid));
                    queue.push_back(BfsState {
                        entry_idx: new_idx,
                        visited_edges: new_visited,
                    });
                }};
            }

            if let Some(csr) = csr {
                // CSR fast path: iterate &[CsrNeighbor] directly -- no Vec, no get_edge()
                match direction {
                    EdgeDirection::Out => {
                        for n in csr.outgoing(state_node) {
                            process_neighbor!(n.edge_id, n.node_id, n.label);
                        }
                    }
                    EdgeDirection::In => {
                        for n in csr.incoming(state_node) {
                            process_neighbor!(n.edge_id, n.node_id, n.label);
                        }
                    }
                    EdgeDirection::Any => {
                        for n in csr.outgoing(state_node) {
                            process_neighbor!(n.edge_id, n.node_id, n.label);
                        }
                        for n in csr.incoming(state_node) {
                            process_neighbor!(n.edge_id, n.node_id, n.label);
                        }
                    }
                }
            } else {
                // Non-CSR fallback: collect edge IDs, get_edge for metadata
                let edge_ids: SmallVec<[EdgeId; 16]> = match direction {
                    EdgeDirection::Out => graph.outgoing(state_node).iter().copied().collect(),
                    EdgeDirection::In => graph.incoming(state_node).iter().copied().collect(),
                    EdgeDirection::Any => {
                        let mut ids: SmallVec<[EdgeId; 16]> =
                            graph.outgoing(state_node).iter().copied().collect();
                        ids.extend_from_slice(graph.incoming(state_node));
                        ids
                    }
                };

                for eid in edge_ids {
                    let Some(edge) = graph.get_edge(eid) else {
                        continue;
                    };
                    let next = match direction {
                        EdgeDirection::Out => edge.target,
                        EdgeDirection::In => edge.source,
                        EdgeDirection::Any => {
                            if edge.source == state_node {
                                edge.target
                            } else {
                                edge.source
                            }
                        }
                    };
                    process_neighbor!(eid, next, edge.label);
                }
            }
        }

        // ANY SHORTEST: keep only the first result for this binding
        if matches!(
            shortest,
            Some(crate::ast::pattern::PathSelector::AnyShortest)
        ) && output.len() > output_start + 1
        {
            output.truncate(output_start + 1);
        }
        // ALL SHORTEST: all results at shortest depth already ensured by BFS + break
    }

    Ok(output)
}

#[allow(clippy::too_many_arguments)]
fn emit_binding(
    base: &Binding,
    target_var: IStr,
    target_id: NodeId,
    edge_var: Option<IStr>,
    edges: &[EdgeId],
    path_var: Option<IStr>,
    nodes: &[NodeId],
    output: &mut Vec<Binding>,
) {
    let mut new_binding = base.clone();
    new_binding.bind(target_var, BoundValue::Node(target_id));

    // Bind edge group variable (for horizontal aggregation)
    if let Some(ev) = edge_var {
        new_binding.bind(ev, BoundValue::Group(edges.to_vec()));
    }

    // Bind path variable
    if let Some(pv) = path_var {
        let path = GqlPath::from_nodes_and_edges(nodes, edges);
        new_binding.bind(pv, BoundValue::Path(path));
    }

    output.push(new_binding);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::pattern::LabelExpr;
    use selene_core::{LabelSet, PropertyMap};
    use selene_graph::SeleneGraph;

    fn setup_chain() -> SeleneGraph {
        // Linear chain: 1 -contains-> 2 -contains-> 3 -contains-> 4
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        for i in 1..=4 {
            m.create_node(
                LabelSet::from_strs(&[&format!("level{i}")]),
                PropertyMap::new(),
            )
            .unwrap();
        }
        m.create_edge(
            NodeId(1),
            IStr::new("contains"),
            NodeId(2),
            PropertyMap::new(),
        )
        .unwrap();
        m.create_edge(
            NodeId(2),
            IStr::new("contains"),
            NodeId(3),
            PropertyMap::new(),
        )
        .unwrap();
        m.create_edge(
            NodeId(3),
            IStr::new("contains"),
            NodeId(4),
            PropertyMap::new(),
        )
        .unwrap();
        m.commit(0).unwrap();
        g
    }

    fn setup_cycle() -> SeleneGraph {
        // Cycle: 1 -> 2 -> 3 -> 1
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        for _ in 1..=3 {
            m.create_node(LabelSet::from_strs(&["node"]), PropertyMap::new())
                .unwrap();
        }
        m.create_edge(NodeId(1), IStr::new("knows"), NodeId(2), PropertyMap::new())
            .unwrap();
        m.create_edge(NodeId(2), IStr::new("knows"), NodeId(3), PropertyMap::new())
            .unwrap();
        m.create_edge(NodeId(3), IStr::new("knows"), NodeId(1), PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();
        g
    }

    #[test]
    fn var_expand_bounded() {
        let g = setup_chain();
        let input = vec![Binding::single(IStr::new("a"), BoundValue::Node(NodeId(1)))];
        let result = execute_var_expand(
            &input,
            &ExpandContext {
                graph: &g,
                scope: None,
                csr: None,
                source_var: IStr::new("a"),
                edge_var: None,
                target_var: IStr::new("b"),
                edge_labels: Some(&LabelExpr::Name(IStr::new("contains"))),
                target_labels: None,
                direction: EdgeDirection::Out,
            },
            &VarExpandConfig {
                min_hops: 1,
                max_hops: Some(3),
                trail: false,
                acyclic: false,
                simple: false,
                shortest: None,
                path_var: None,
            },
        )
        .unwrap();
        // Depth 1: node 2, depth 2: node 3, depth 3: node 4
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn var_expand_min_bound() {
        let g = setup_chain();
        let input = vec![Binding::single(IStr::new("a"), BoundValue::Node(NodeId(1)))];
        let result = execute_var_expand(
            &input,
            &ExpandContext {
                graph: &g,
                scope: None,
                csr: None,
                source_var: IStr::new("a"),
                edge_var: None,
                target_var: IStr::new("b"),
                edge_labels: Some(&LabelExpr::Name(IStr::new("contains"))),
                target_labels: None,
                direction: EdgeDirection::Out,
            },
            &VarExpandConfig {
                min_hops: 2,
                max_hops: Some(3),
                trail: false,
                acyclic: false,
                simple: false,
                shortest: None,
                path_var: None,
            },
        )
        .unwrap();
        // Depth 2: node 3, depth 3: node 4
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn var_expand_exact() {
        let g = setup_chain();
        let input = vec![Binding::single(IStr::new("a"), BoundValue::Node(NodeId(1)))];
        let result = execute_var_expand(
            &input,
            &ExpandContext {
                graph: &g,
                scope: None,
                csr: None,
                source_var: IStr::new("a"),
                edge_var: None,
                target_var: IStr::new("b"),
                edge_labels: Some(&LabelExpr::Name(IStr::new("contains"))),
                target_labels: None,
                direction: EdgeDirection::Out,
            },
            &VarExpandConfig {
                min_hops: 2,
                max_hops: Some(2),
                trail: false,
                acyclic: false,
                simple: false,
                shortest: None,
                path_var: None,
            },
        )
        .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get_node_id(&IStr::new("b")).unwrap(), NodeId(3));
    }

    #[test]
    fn var_expand_trail_prevents_cycles() {
        let g = setup_cycle();
        let input = vec![Binding::single(IStr::new("a"), BoundValue::Node(NodeId(1)))];
        // Without TRAIL: would loop forever (capped at max_hops=10)
        // With TRAIL: each edge visited at most once per path
        let result = execute_var_expand(
            &input,
            &ExpandContext {
                graph: &g,
                scope: None,
                csr: None,
                source_var: IStr::new("a"),
                edge_var: None,
                target_var: IStr::new("b"),
                edge_labels: Some(&LabelExpr::Name(IStr::new("knows"))),
                target_labels: None,
                direction: EdgeDirection::Out,
            },
            &VarExpandConfig {
                min_hops: 1,
                max_hops: Some(10),
                trail: true,
                acyclic: false,
                simple: false,
                shortest: None,
                path_var: None,
            },
        )
        .unwrap();
        // With 3 edges in a cycle, TRAIL allows at most 3 hops:
        // depth 1: node 2 (edge 1)
        // depth 2: node 3 (edges 1,2)
        // depth 3: node 1 (edges 1,2,3) -- back to start, but all edges used
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn var_expand_without_trail_explores_cycle() {
        let g = setup_cycle();
        let input = vec![Binding::single(IStr::new("a"), BoundValue::Node(NodeId(1)))];
        let result = execute_var_expand(
            &input,
            &ExpandContext {
                graph: &g,
                scope: None,
                csr: None,
                source_var: IStr::new("a"),
                edge_var: None,
                target_var: IStr::new("b"),
                edge_labels: Some(&LabelExpr::Name(IStr::new("knows"))),
                target_labels: None,
                direction: EdgeDirection::Out,
            },
            &VarExpandConfig {
                min_hops: 1,
                max_hops: Some(4),
                trail: false,
                acyclic: false,
                simple: false,
                shortest: None,
                path_var: None,
            },
        )
        .unwrap();
        // depth 1: node 2
        // depth 2: node 3
        // depth 3: node 1 (back to start)
        // depth 4: node 2 (again)
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn var_expand_with_edge_group_variable() {
        let g = setup_chain();
        let input = vec![Binding::single(IStr::new("a"), BoundValue::Node(NodeId(1)))];
        let result = execute_var_expand(
            &input,
            &ExpandContext {
                graph: &g,
                scope: None,
                csr: None,
                source_var: IStr::new("a"),
                edge_var: Some(IStr::new("e")),
                target_var: IStr::new("b"),
                edge_labels: Some(&LabelExpr::Name(IStr::new("contains"))),
                target_labels: None,
                direction: EdgeDirection::Out,
            },
            &VarExpandConfig {
                min_hops: 2,
                max_hops: Some(2),
                trail: false,
                acyclic: false,
                simple: false,
                shortest: None,
                path_var: None,
            },
        )
        .unwrap();
        assert_eq!(result.len(), 1);
        // Edge group should have 2 edges
        match result[0].get(&IStr::new("e")) {
            Some(BoundValue::Group(edges)) => assert_eq!(edges.len(), 2),
            _ => panic!("expected Group"),
        }
    }

    #[test]
    fn var_expand_with_path_variable() {
        let g = setup_chain();
        let input = vec![Binding::single(IStr::new("a"), BoundValue::Node(NodeId(1)))];
        let result = execute_var_expand(
            &input,
            &ExpandContext {
                graph: &g,
                scope: None,
                csr: None,
                source_var: IStr::new("a"),
                edge_var: None,
                target_var: IStr::new("b"),
                edge_labels: Some(&LabelExpr::Name(IStr::new("contains"))),
                target_labels: None,
                direction: EdgeDirection::Out,
            },
            &VarExpandConfig {
                min_hops: 3,
                max_hops: Some(3),
                trail: false,
                acyclic: false,
                simple: false,
                shortest: None,
                path_var: Some(IStr::new("p")),
            },
        )
        .unwrap();
        assert_eq!(result.len(), 1);
        match result[0].get(&IStr::new("p")) {
            Some(BoundValue::Path(path)) => {
                assert_eq!(path.edge_count(), 3);
                assert_eq!(path.node_count(), 4);
            }
            _ => panic!("expected Path"),
        }
    }

    #[test]
    fn var_expand_zero_min_includes_start() {
        let g = setup_chain();
        let input = vec![Binding::single(IStr::new("a"), BoundValue::Node(NodeId(1)))];
        let result = execute_var_expand(
            &input,
            &ExpandContext {
                graph: &g,
                scope: None,
                csr: None,
                source_var: IStr::new("a"),
                edge_var: None,
                target_var: IStr::new("b"),
                edge_labels: Some(&LabelExpr::Name(IStr::new("contains"))),
                target_labels: None,
                direction: EdgeDirection::Out,
            },
            &VarExpandConfig {
                min_hops: 0,
                max_hops: Some(1),
                trail: false,
                acyclic: false,
                simple: false,
                shortest: None,
                path_var: None,
            },
        )
        .unwrap();
        // Depth 0: node 1 (start), depth 1: node 2
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn var_expand_with_target_label_filter() {
        // Chain: 1(level1) -contains-> 2(level2) -contains-> 3(level3) -contains-> 4(level4)
        // Target filter: only level3 and level4
        let g = setup_chain();
        let input = vec![Binding::single(IStr::new("a"), BoundValue::Node(NodeId(1)))];
        // Filter for level3 nodes only
        let target = LabelExpr::Name(IStr::new("level3"));
        let result = execute_var_expand(
            &input,
            &ExpandContext {
                graph: &g,
                scope: None,
                csr: None,
                source_var: IStr::new("a"),
                edge_var: None,
                target_var: IStr::new("b"),
                edge_labels: Some(&LabelExpr::Name(IStr::new("contains"))),
                target_labels: Some(&target),
                direction: EdgeDirection::Out,
            },
            &VarExpandConfig {
                min_hops: 1,
                max_hops: Some(3),
                trail: false,
                acyclic: false,
                simple: false,
                shortest: None,
                path_var: None,
            },
        )
        .unwrap();
        // Only node 3 (level3) should be emitted, even though nodes 2 and 4 are reachable
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get_node_id(&IStr::new("b")).unwrap(), NodeId(3));
    }

    #[test]
    fn var_expand_target_filter_traverses_through_non_matching() {
        // Chain: 1(level1) -contains-> 2(level2) -contains-> 3(level3) -contains-> 4(level4)
        // Target filter: level4 -- must traverse THROUGH level2 and level3 to reach it
        let g = setup_chain();
        let input = vec![Binding::single(IStr::new("a"), BoundValue::Node(NodeId(1)))];
        let target = LabelExpr::Name(IStr::new("level4"));
        let result = execute_var_expand(
            &input,
            &ExpandContext {
                graph: &g,
                scope: None,
                csr: None,
                source_var: IStr::new("a"),
                edge_var: None,
                target_var: IStr::new("b"),
                edge_labels: Some(&LabelExpr::Name(IStr::new("contains"))),
                target_labels: Some(&target),
                direction: EdgeDirection::Out,
            },
            &VarExpandConfig {
                min_hops: 1,
                max_hops: Some(5),
                trail: false,
                acyclic: false,
                simple: false,
                shortest: None,
                path_var: None,
            },
        )
        .unwrap();
        // Node 4 at depth 3 -- reached through non-matching nodes 2 and 3
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get_node_id(&IStr::new("b")).unwrap(), NodeId(4));
    }
}
