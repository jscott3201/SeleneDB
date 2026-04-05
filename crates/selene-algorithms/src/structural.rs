//! Structural algorithms: WCC, SCC, topological sort, articulation points, bridges.

use std::collections::HashMap;

use selene_core::NodeId;

use crate::projection::GraphProjection;

/// Sentinel value indicating an uninitialized or absent entry in flat Vec storage.
const SENTINEL: u32 = u32::MAX;

struct UnionFind {
    parent: Vec<u32>,
    rank: Vec<u32>,
}

impl UnionFind {
    fn with_capacity(size: usize) -> Self {
        Self {
            parent: vec![SENTINEL; size],
            rank: vec![0; size],
        }
    }

    fn make_set(&mut self, x: u32) {
        let i = x as usize;
        if self.parent[i] == SENTINEL {
            self.parent[i] = x;
        }
    }

    fn find(&mut self, x: u32) -> u32 {
        let i = x as usize;
        let p = self.parent[i];
        if p == SENTINEL {
            return x;
        }
        if p == x {
            x
        } else {
            let root = self.find(p);
            self.parent[i] = root;
            root
        }
    }

    fn union(&mut self, x: u32, y: u32) {
        let rx = self.find(x);
        let ry = self.find(y);
        if rx == ry {
            return;
        }

        let rank_x = self.rank[rx as usize];
        let rank_y = self.rank[ry as usize];

        match rank_x.cmp(&rank_y) {
            std::cmp::Ordering::Less => {
                self.parent[rx as usize] = ry;
            }
            std::cmp::Ordering::Greater => {
                self.parent[ry as usize] = rx;
            }
            std::cmp::Ordering::Equal => {
                self.parent[ry as usize] = rx;
                self.rank[rx as usize] = rank_x + 1;
            }
        }
    }
}

/// Build a union-find structure from the projection's edges (undirected).
fn build_wcc_uf(proj: &GraphProjection) -> UnionFind {
    let size = proj.nodes.max().map_or(0, |m| m as usize + 1);
    let mut uf = UnionFind::with_capacity(size);

    for nid in &proj.nodes {
        uf.make_set(nid);
    }

    for nid in &proj.nodes {
        let node_id = NodeId(u64::from(nid));
        for nb in proj.outgoing(node_id) {
            uf.union(nid, nb.node_id.0 as u32);
        }
    }

    uf
}

/// Weakly Connected Components via union-find.
///
/// Treats all edges as undirected. Returns a map of node_id -> component_id.
/// Component IDs are the smallest node ID in each component.
pub fn wcc(proj: &GraphProjection) -> Vec<(NodeId, u64)> {
    let mut uf = build_wcc_uf(proj);

    let mut result: Vec<(NodeId, u64)> = proj
        .nodes
        .iter()
        .map(|nid| {
            let comp = uf.find(nid);
            (NodeId(u64::from(nid)), u64::from(comp))
        })
        .collect();

    let mut comp_min: HashMap<u64, u64> = HashMap::new();
    for &(node_id, comp_id) in &result {
        let entry = comp_min.entry(comp_id).or_insert(node_id.0);
        if node_id.0 < *entry {
            *entry = node_id.0;
        }
    }
    for item in &mut result {
        item.1 = comp_min[&item.1];
    }

    result
}

/// Count the number of weakly connected components.
///
/// Uses union-find directly and counts unique roots, avoiding the full
/// result allocation that `wcc()` performs.
pub fn wcc_count(proj: &GraphProjection) -> usize {
    let mut uf = build_wcc_uf(proj);

    // Count unique roots: a node is a root when find(nid) == nid.
    let mut count = 0;
    for nid in &proj.nodes {
        if uf.find(nid) == nid {
            count += 1;
        }
    }
    count
}

struct TarjanState {
    index: u32,
    stack: Vec<u32>,
    on_stack: Vec<bool>,
    indices: Vec<u32>,
    lowlinks: Vec<u32>,
    components: Vec<Vec<u32>>,
}

impl TarjanState {
    fn with_capacity(size: usize) -> Self {
        Self {
            index: 0,
            stack: Vec::new(),
            on_stack: vec![false; size],
            indices: vec![SENTINEL; size],
            lowlinks: vec![SENTINEL; size],
            components: Vec::new(),
        }
    }
}

/// Run Tarjan's algorithm on the projection, returning the completed state.
fn run_tarjan(proj: &GraphProjection) -> TarjanState {
    let size = proj.nodes.max().map_or(0, |m| m as usize + 1);
    let mut state = TarjanState::with_capacity(size);

    for nid in &proj.nodes {
        if state.indices[nid as usize] == SENTINEL {
            tarjan_strongconnect(&mut state, nid, proj);
        }
    }

    state
}

/// Strongly Connected Components via Tarjan's algorithm.
///
/// Returns a map of node_id -> component_id.
/// Only directed edges are considered. Useful for finding cycles (e.g., HVAC return air).
pub fn scc(proj: &GraphProjection) -> Vec<(NodeId, u64)> {
    let state = run_tarjan(proj);

    let mut result = Vec::new();
    for component in &state.components {
        let min_id = *component.iter().min().unwrap_or(&0);
        for &nid in component {
            result.push((NodeId(u64::from(nid)), u64::from(min_id)));
        }
    }

    result.sort_by_key(|&(nid, _)| nid.0);
    result
}

/// Iterative Tarjan's algorithm using an explicit call stack to avoid
/// stack overflow on deep graphs.
fn tarjan_strongconnect(state: &mut TarjanState, start: u32, proj: &GraphProjection) {
    let mut call_stack: Vec<(u32, usize)> = Vec::new();
    let mut neighbors_cache: HashMap<u32, Vec<u32>> = HashMap::new();
    let si = start as usize;
    state.indices[si] = state.index;
    state.lowlinks[si] = state.index;
    state.index += 1;
    state.stack.push(start);
    state.on_stack[si] = true;
    call_stack.push((start, 0));

    while let Some(&mut (v, ref mut ni)) = call_stack.last_mut() {
        let neighbors = neighbors_cache.entry(v).or_insert_with(|| {
            proj.outgoing(NodeId(u64::from(v)))
                .iter()
                .map(|nb| nb.node_id.0 as u32)
                .collect()
        });

        if *ni < neighbors.len() {
            let w = neighbors[*ni];
            *ni += 1;
            let wi = w as usize;

            if state.indices[wi] == SENTINEL {
                state.indices[wi] = state.index;
                state.lowlinks[wi] = state.index;
                state.index += 1;
                state.stack.push(w);
                state.on_stack[wi] = true;
                call_stack.push((w, 0));
            } else if state.on_stack[wi] {
                let vi = v as usize;
                state.lowlinks[vi] = state.lowlinks[vi].min(state.indices[wi]);
            }
        } else {
            let vi = v as usize;
            if state.lowlinks[vi] == state.indices[vi] {
                let mut component = Vec::new();
                loop {
                    let w = state.stack.pop().unwrap();
                    state.on_stack[w as usize] = false;
                    component.push(w);
                    if w == v {
                        break;
                    }
                }
                state.components.push(component);
            }

            call_stack.pop();
            if let Some(&mut (parent, _)) = call_stack.last_mut() {
                let pi = parent as usize;
                state.lowlinks[pi] = state.lowlinks[pi].min(state.lowlinks[vi]);
            }
        }
    }
}

/// Count strongly connected components.
///
/// Runs Tarjan's algorithm directly and returns the component count,
/// avoiding the full result allocation and HashSet deduplication.
pub fn scc_count(proj: &GraphProjection) -> usize {
    run_tarjan(proj).components.len()
}

/// Topological sort via Kahn's algorithm.
///
/// Returns nodes in topological order with their position (0-indexed).
/// Returns Err if the graph has cycles (not a DAG).
pub fn topological_sort(proj: &GraphProjection) -> Result<Vec<(NodeId, usize)>, TopoSortError> {
    let size = proj.nodes.max().map_or(0, |m| m as usize + 1);
    // SENTINEL marks nodes not in the projection; 0+ are valid in-degree values.
    let mut in_degree: Vec<u32> = vec![SENTINEL; size];

    for nid in &proj.nodes {
        let i = nid as usize;
        if in_degree[i] == SENTINEL {
            in_degree[i] = 0;
        }
        for nb in proj.outgoing(NodeId(u64::from(nid))) {
            let ni = nb.node_id.0 as usize;
            if in_degree[ni] == SENTINEL {
                in_degree[ni] = 0;
            }
            in_degree[ni] += 1;
        }
    }

    let mut queue: std::collections::VecDeque<u32> = proj
        .nodes
        .iter()
        .filter(|&nid| in_degree[nid as usize] == 0)
        .collect();

    let mut queue_sorted: Vec<u32> = queue.drain(..).collect();
    queue_sorted.sort_unstable();
    queue = queue_sorted.into_iter().collect();

    let mut result = Vec::new();
    let mut position = 0;

    while let Some(nid) = queue.pop_front() {
        result.push((NodeId(u64::from(nid)), position));
        position += 1;

        let mut neighbors: Vec<u32> = proj
            .outgoing(NodeId(u64::from(nid)))
            .iter()
            .map(|nb| nb.node_id.0 as u32)
            .collect();
        neighbors.sort_unstable();

        for w in neighbors {
            let wi = w as usize;
            if in_degree[wi] != SENTINEL {
                in_degree[wi] -= 1;
                if in_degree[wi] == 0 {
                    queue.push_back(w);
                }
            }
        }
    }

    if result.len() != proj.node_count() as usize {
        return Err(TopoSortError::CycleDetected);
    }

    Ok(result)
}

/// Topological sort failed because the graph contains a cycle.
#[derive(Debug, thiserror::Error)]
pub enum TopoSortError {
    #[error("graph contains a cycle -- topological sort not possible")]
    CycleDetected,
}

/// Pre-computed containment ancestry for O(1) ancestor checks.
///
/// Built by walking "contains" edges in the graph. Stores the full
/// ancestor chain for each node.
pub struct ContainmentIndex {
    /// Ancestor node IDs per node (transitive via "contains" edges).
    ancestors: HashMap<u32, roaring::RoaringBitmap>,
}

impl ContainmentIndex {
    /// Build the containment index by walking all "contains" edges.
    ///
    /// This takes `&SeleneGraph` directly rather than `&GraphProjection`
    /// because containment is a whole-graph structural property: auth scope
    /// projections and algorithm-specific label filters must not exclude
    /// intermediate ancestors, which would silently break the transitive
    /// closure used for scope resolution and topological ordering.
    pub fn build(graph: &selene_graph::SeleneGraph) -> Self {
        let contains = selene_core::IStr::new("contains");
        let mut children_of: HashMap<u32, Vec<u32>> = HashMap::new();

        for eid in &graph.all_edge_bitmap() {
            if let Some(edge) = graph.get_edge(selene_core::EdgeId(u64::from(eid)))
                && edge.label == contains
            {
                children_of
                    .entry(edge.source.0 as u32)
                    .or_default()
                    .push(edge.target.0 as u32);
            }
        }

        let mut ancestors: HashMap<u32, roaring::RoaringBitmap> = HashMap::new();

        let all_children: roaring::RoaringBitmap = children_of
            .values()
            .flat_map(|v| v.iter().copied())
            .collect();
        let all_parents: roaring::RoaringBitmap = children_of.keys().copied().collect();
        let roots: Vec<u32> = (&all_parents - &all_children).iter().collect();

        for root in roots {
            let mut queue = std::collections::VecDeque::new();
            let mut visited = std::collections::HashSet::new();
            queue.push_back(root);
            visited.insert(root);
            ancestors.entry(root).or_default();

            while let Some(parent) = queue.pop_front() {
                let parent_ancestors = ancestors.get(&parent).cloned().unwrap_or_default();

                if let Some(children) = children_of.get(&parent) {
                    for &child in children {
                        if visited.insert(child) {
                            let child_anc = ancestors.entry(child).or_default();
                            child_anc.insert(parent);
                            *child_anc |= &parent_ancestors;
                            queue.push_back(child);
                        }
                    }
                }
            }
        }

        Self { ancestors }
    }

    /// Check whether `ancestor` is an ancestor of `descendant` via "contains" edges (O(1)).
    pub fn is_ancestor(&self, ancestor: NodeId, descendant: NodeId) -> bool {
        self.ancestors
            .get(&(descendant.0 as u32))
            .is_some_and(|anc| anc.contains(ancestor.0 as u32))
    }

    /// Get all ancestors of a node.
    pub fn ancestors_of(&self, node: NodeId) -> Vec<NodeId> {
        self.ancestors
            .get(&(node.0 as u32))
            .map(|bm| bm.iter().map(|id| NodeId(u64::from(id))).collect())
            .unwrap_or_default()
    }
}

struct BiconnState {
    timer: u32,
    disc: Vec<u32>,
    low: Vec<u32>,
    parent: Vec<u32>,
    ap: std::collections::HashSet<u32>,
    bridges: Vec<(u32, u32)>,
}

impl BiconnState {
    fn with_capacity(size: usize) -> Self {
        Self {
            timer: 0,
            disc: vec![SENTINEL; size],
            low: vec![SENTINEL; size],
            parent: vec![SENTINEL; size],
            ap: std::collections::HashSet::new(),
            bridges: Vec::new(),
        }
    }
}

/// Find articulation points in the projection (treated as undirected).
///
/// An articulation point is a node whose removal disconnects the graph.
pub fn articulation_points(proj: &GraphProjection) -> Vec<NodeId> {
    let size = proj.nodes.max().map_or(0, |m| m as usize + 1);
    let mut state = BiconnState::with_capacity(size);

    for nid in &proj.nodes {
        if state.disc[nid as usize] == SENTINEL {
            biconn_dfs(&mut state, nid, proj);
        }
    }

    let mut result: Vec<NodeId> = state
        .ap
        .into_iter()
        .map(|id| NodeId(u64::from(id)))
        .collect();
    result.sort_by_key(|n| n.0);
    result
}

/// Find bridges in the projection (treated as undirected).
///
/// A bridge is an edge whose removal disconnects the graph.
/// Returns pairs of (source, target) node IDs.
pub fn bridges(proj: &GraphProjection) -> Vec<(NodeId, NodeId)> {
    let size = proj.nodes.max().map_or(0, |m| m as usize + 1);
    let mut state = BiconnState::with_capacity(size);

    for nid in &proj.nodes {
        if state.disc[nid as usize] == SENTINEL {
            biconn_dfs(&mut state, nid, proj);
        }
    }

    let mut result: Vec<(NodeId, NodeId)> = state
        .bridges
        .into_iter()
        .map(|(a, b)| (NodeId(u64::from(a)), NodeId(u64::from(b))))
        .collect();
    result.sort_by_key(|&(a, b)| (a.0, b.0));
    result
}

/// Iterative biconnectivity DFS using an explicit call stack to avoid stack
/// overflow on deep chain graphs.
fn biconn_dfs(state: &mut BiconnState, start: u32, proj: &GraphProjection) {
    let mut call_stack: Vec<(u32, usize, u32)> = Vec::new();
    let mut neighbors_cache: HashMap<u32, Vec<u32>> = HashMap::new();
    let si = start as usize;
    state.disc[si] = state.timer;
    state.low[si] = state.timer;
    state.timer += 1;
    call_stack.push((start, 0, 0));

    while let Some(&mut (u, ref mut ni, ref mut children)) = call_stack.last_mut() {
        let neighbors = neighbors_cache.entry(u).or_insert_with(|| {
            let mut set = std::collections::HashSet::new();
            for nb in proj.outgoing(NodeId(u64::from(u))) {
                set.insert(nb.node_id.0 as u32);
            }
            for nb in proj.incoming(NodeId(u64::from(u))) {
                set.insert(nb.node_id.0 as u32);
            }
            let mut v: Vec<u32> = set.into_iter().collect();
            v.sort_unstable();
            v
        });

        if *ni < neighbors.len() {
            let v = neighbors[*ni];
            *ni += 1;
            let vi = v as usize;
            let ui = u as usize;

            if state.disc[vi] == SENTINEL {
                *children += 1;
                state.parent[vi] = u;
                state.disc[vi] = state.timer;
                state.low[vi] = state.timer;
                state.timer += 1;
                call_stack.push((v, 0, 0));
            } else if state.parent[ui] != v {
                state.low[ui] = state.low[ui].min(state.disc[vi]);
            }
        } else {
            let finished_u = u;
            let finished_children = *children;
            call_stack.pop();
            let fi = finished_u as usize;

            if let Some(&mut (parent, _, _)) = call_stack.last_mut() {
                let pi = parent as usize;
                state.low[pi] = state.low[pi].min(state.low[fi]);

                let is_root = state.parent[pi] == SENTINEL;
                if !is_root && state.low[fi] >= state.disc[pi] {
                    state.ap.insert(parent);
                }

                if state.low[fi] > state.disc[pi] {
                    state
                        .bridges
                        .push((parent.min(finished_u), parent.max(finished_u)));
                }
            }

            if state.parent[fi] == SENTINEL && finished_children > 1 {
                state.ap.insert(finished_u);
            }
        }
    }
}

/// A structural issue found during validation.
#[derive(Debug, Clone)]
pub struct ValidationIssue {
    pub severity: IssueSeverity,
    pub issue_type: String,
    pub node_id: Option<NodeId>,
    pub message: String,
}

/// Issue severity level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IssueSeverity {
    Warning,
    Error,
}

impl std::fmt::Display for IssueSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IssueSeverity::Warning => write!(f, "warning"),
            IssueSeverity::Error => write!(f, "error"),
        }
    }
}

/// Validate graph structure and report issues.
///
/// Checks:
/// - Orphan nodes (nodes with no edges at all)
/// - Disconnected components (more than 1 WCC)
/// - Nodes with only self-loops
pub fn validate(proj: &GraphProjection) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();

    for nid in &proj.nodes {
        let node_id = NodeId(u64::from(nid));
        if proj.out_degree(node_id) == 0 && proj.in_degree(node_id) == 0 {
            issues.push(ValidationIssue {
                severity: IssueSeverity::Warning,
                issue_type: "orphan_node".into(),
                node_id: Some(node_id),
                message: format!("Node {nid} has no edges"),
            });
        }
    }

    let num_components = wcc_count(proj);
    if num_components > 1 {
        issues.push(ValidationIssue {
            severity: IssueSeverity::Warning,
            issue_type: "disconnected_components".into(),
            node_id: None,
            message: format!("Graph has {num_components} disconnected components"),
        });
    }

    issues
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection::ProjectionConfig;
    use selene_core::{IStr, LabelSet, PropertyMap};
    use selene_graph::SeleneGraph;

    fn make_graph(nodes: &[(u64, &[&str])], edges: &[(u64, u64, &str)]) -> SeleneGraph {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        for &(_id, labels) in nodes {
            // We need to create nodes in ID order (IDs are auto-assigned)
            let _ = m
                .create_node(LabelSet::from_strs(labels), PropertyMap::new())
                .unwrap();
        }
        for &(src, tgt, label) in edges {
            m.create_edge(
                NodeId(src),
                IStr::new(label),
                NodeId(tgt),
                PropertyMap::new(),
            )
            .unwrap();
        }
        m.commit(0).unwrap();
        g
    }

    fn project_all(g: &SeleneGraph) -> GraphProjection {
        GraphProjection::build(
            g,
            &ProjectionConfig {
                name: "all".into(),
                node_labels: vec![],
                edge_labels: vec![],
                weight_property: None,
            },
            None,
        )
    }

    // ── WCC Tests ───────────────────────────────────────────────────

    #[test]
    fn wcc_single_component() {
        // Triangle: 1->2->3->1
        let g = make_graph(
            &[(1, &["a"]), (2, &["a"]), (3, &["a"])],
            &[(1, 2, "link"), (2, 3, "link"), (3, 1, "link")],
        );
        let proj = project_all(&g);
        assert_eq!(wcc_count(&proj), 1);
    }

    #[test]
    fn wcc_two_components() {
        // 1->2, 3->4 (disconnected)
        let g = make_graph(
            &[(1, &["a"]), (2, &["a"]), (3, &["b"]), (4, &["b"])],
            &[(1, 2, "link"), (3, 4, "link")],
        );
        let proj = project_all(&g);
        assert_eq!(wcc_count(&proj), 2);
    }

    #[test]
    fn wcc_single_node() {
        let g = make_graph(&[(1, &["a"])], &[]);
        let proj = project_all(&g);
        assert_eq!(wcc_count(&proj), 1);
    }

    // ── SCC Tests ───────────────────────────────────────────────────

    #[test]
    fn scc_cycle() {
        // Cycle: 1->2->3->1
        let g = make_graph(
            &[(1, &["a"]), (2, &["a"]), (3, &["a"])],
            &[(1, 2, "link"), (2, 3, "link"), (3, 1, "link")],
        );
        let proj = project_all(&g);
        let components = scc(&proj);
        // All in same SCC
        let comp_ids: std::collections::HashSet<u64> = components.iter().map(|c| c.1).collect();
        assert_eq!(comp_ids.len(), 1);
    }

    #[test]
    fn scc_dag() {
        // DAG: 1->2->3 (no cycles, each node is its own SCC)
        let g = make_graph(
            &[(1, &["a"]), (2, &["a"]), (3, &["a"])],
            &[(1, 2, "link"), (2, 3, "link")],
        );
        let proj = project_all(&g);
        assert_eq!(scc_count(&proj), 3);
    }

    #[test]
    fn scc_two_cycles() {
        // Two separate cycles: 1->2->1, 3->4->3
        let g = make_graph(
            &[(1, &["a"]), (2, &["a"]), (3, &["b"]), (4, &["b"])],
            &[
                (1, 2, "link"),
                (2, 1, "link"),
                (3, 4, "link"),
                (4, 3, "link"),
            ],
        );
        let proj = project_all(&g);
        assert_eq!(scc_count(&proj), 2);
    }

    // ── Topological Sort Tests ──────────────────────────────────────

    #[test]
    fn topo_sort_dag() {
        // 1->2->3, 1->3
        let g = make_graph(
            &[(1, &["a"]), (2, &["a"]), (3, &["a"])],
            &[(1, 2, "link"), (2, 3, "link"), (1, 3, "link")],
        );
        let proj = project_all(&g);
        let sorted = topological_sort(&proj).unwrap();
        // Node 1 should come first
        assert_eq!(sorted[0].0, NodeId(1));
        // Node 3 should come last
        assert_eq!(sorted[2].0, NodeId(3));
    }

    #[test]
    fn topo_sort_cycle_error() {
        let g = make_graph(
            &[(1, &["a"]), (2, &["a"])],
            &[(1, 2, "link"), (2, 1, "link")],
        );
        let proj = project_all(&g);
        assert!(topological_sort(&proj).is_err());
    }

    // ── Containment Index Tests ─────────────────────────────────────

    #[test]
    fn containment_index_basic() {
        // campus(1) -> building(2) -> floor(3) -> zone(4)
        let g = make_graph(
            &[
                (1, &["campus"]),
                (2, &["building"]),
                (3, &["floor"]),
                (4, &["zone"]),
            ],
            &[(1, 2, "contains"), (2, 3, "contains"), (3, 4, "contains")],
        );
        let idx = ContainmentIndex::build(&g);

        assert!(idx.is_ancestor(NodeId(1), NodeId(4))); // campus is ancestor of zone
        assert!(idx.is_ancestor(NodeId(2), NodeId(4))); // building is ancestor of zone
        assert!(idx.is_ancestor(NodeId(1), NodeId(2))); // campus is ancestor of building
        assert!(!idx.is_ancestor(NodeId(4), NodeId(1))); // zone is NOT ancestor of campus
        assert!(!idx.is_ancestor(NodeId(3), NodeId(2))); // floor is NOT ancestor of building
    }

    #[test]
    fn containment_index_ancestors_of() {
        let g = make_graph(
            &[
                (1, &["campus"]),
                (2, &["building"]),
                (3, &["floor"]),
                (4, &["zone"]),
            ],
            &[(1, 2, "contains"), (2, 3, "contains"), (3, 4, "contains")],
        );
        let idx = ContainmentIndex::build(&g);
        let ancestors = idx.ancestors_of(NodeId(4));
        assert_eq!(ancestors.len(), 3); // campus, building, floor
        assert!(ancestors.contains(&NodeId(1)));
        assert!(ancestors.contains(&NodeId(2)));
        assert!(ancestors.contains(&NodeId(3)));
    }

    // ── Articulation Points Tests ───────────────────────────────────

    #[test]
    fn articulation_points_chain() {
        // Chain: 1-2-3-4 (undirected)
        // Nodes 2 and 3 are articulation points
        let g = make_graph(
            &[(1, &["a"]), (2, &["a"]), (3, &["a"]), (4, &["a"])],
            &[
                (1, 2, "link"),
                (2, 1, "link"),
                (2, 3, "link"),
                (3, 2, "link"),
                (3, 4, "link"),
                (4, 3, "link"),
            ],
        );
        let proj = project_all(&g);
        let aps = articulation_points(&proj);
        assert!(aps.contains(&NodeId(2)));
        assert!(aps.contains(&NodeId(3)));
        assert!(!aps.contains(&NodeId(1)));
        assert!(!aps.contains(&NodeId(4)));
    }

    #[test]
    fn articulation_points_cycle_none() {
        // Triangle: no articulation points
        let g = make_graph(
            &[(1, &["a"]), (2, &["a"]), (3, &["a"])],
            &[
                (1, 2, "l"),
                (2, 1, "l"),
                (2, 3, "l"),
                (3, 2, "l"),
                (1, 3, "l"),
                (3, 1, "l"),
            ],
        );
        let proj = project_all(&g);
        let aps = articulation_points(&proj);
        assert!(aps.is_empty());
    }

    // ── Bridges Tests ───────────────────────────────────────────────

    #[test]
    fn bridges_chain() {
        // Chain: 1-2-3 (undirected), all edges are bridges
        let g = make_graph(
            &[(1, &["a"]), (2, &["a"]), (3, &["a"])],
            &[
                (1, 2, "link"),
                (2, 1, "link"),
                (2, 3, "link"),
                (3, 2, "link"),
            ],
        );
        let proj = project_all(&g);
        let br = bridges(&proj);
        assert_eq!(br.len(), 2);
    }

    #[test]
    fn bridges_cycle_none() {
        // Triangle: no bridges
        let g = make_graph(
            &[(1, &["a"]), (2, &["a"]), (3, &["a"])],
            &[
                (1, 2, "l"),
                (2, 1, "l"),
                (2, 3, "l"),
                (3, 2, "l"),
                (1, 3, "l"),
                (3, 1, "l"),
            ],
        );
        let proj = project_all(&g);
        let br = bridges(&proj);
        assert!(br.is_empty());
    }

    // ── Validation Tests ────────────────────────────────────────────

    #[test]
    fn validate_orphan_node() {
        let g = make_graph(
            &[(1, &["a"]), (2, &["a"]), (3, &["a"])],
            &[(1, 2, "link")], // node 3 is orphan
        );
        let proj = project_all(&g);
        let issues = validate(&proj);
        assert!(issues.iter().any(|i| i.issue_type == "orphan_node"));
    }

    #[test]
    fn validate_disconnected() {
        let g = make_graph(
            &[(1, &["a"]), (2, &["a"]), (3, &["b"]), (4, &["b"])],
            &[(1, 2, "link"), (3, 4, "link")],
        );
        let proj = project_all(&g);
        let issues = validate(&proj);
        assert!(
            issues
                .iter()
                .any(|i| i.issue_type == "disconnected_components")
        );
    }

    // ── WCC edge cases ──────────────────────────────────────────────

    #[test]
    fn wcc_empty_graph() {
        let g = SeleneGraph::new();
        let proj = project_all(&g);
        assert_eq!(wcc_count(&proj), 0);
        assert!(wcc(&proj).is_empty());
    }

    #[test]
    fn wcc_all_isolated_nodes() {
        // Each isolated node is its own component
        let g = make_graph(&[(1, &["a"]), (2, &["b"]), (3, &["c"])], &[]);
        let proj = project_all(&g);
        assert_eq!(wcc_count(&proj), 3);
    }

    #[test]
    fn wcc_directed_edge_connects_undirected() {
        // WCC treats edges as undirected: 1->2 means 1 and 2 share a component
        let g = make_graph(&[(1, &["a"]), (2, &["a"])], &[(1, 2, "link")]);
        let proj = project_all(&g);
        assert_eq!(wcc_count(&proj), 1);
    }

    // ── SCC edge cases ─────────────────────────────────────────────

    #[test]
    fn scc_single_node() {
        let g = make_graph(&[(1, &["a"])], &[]);
        let proj = project_all(&g);
        let components = scc(&proj);
        assert_eq!(components.len(), 1);
        assert_eq!(scc_count(&proj), 1);
    }

    #[test]
    fn scc_self_loop() {
        // A self-loop makes a single-node SCC
        let g = make_graph(&[(1, &["a"])], &[(1, 1, "loop")]);
        let proj = project_all(&g);
        assert_eq!(scc_count(&proj), 1);
    }

    #[test]
    fn scc_empty_graph() {
        let g = SeleneGraph::new();
        let proj = project_all(&g);
        assert_eq!(scc_count(&proj), 0);
        assert!(scc(&proj).is_empty());
    }

    // ── Topological Sort edge cases ────────────────────────────────

    #[test]
    fn topo_sort_single_node() {
        let g = make_graph(&[(1, &["a"])], &[]);
        let proj = project_all(&g);
        let sorted = topological_sort(&proj).unwrap();
        assert_eq!(sorted.len(), 1);
        assert_eq!(sorted[0], (NodeId(1), 0));
    }

    #[test]
    fn topo_sort_disconnected_dag() {
        // Two disconnected chains: 1->2, 3->4
        let g = make_graph(
            &[(1, &["a"]), (2, &["a"]), (3, &["a"]), (4, &["a"])],
            &[(1, 2, "link"), (3, 4, "link")],
        );
        let proj = project_all(&g);
        let sorted = topological_sort(&proj).unwrap();
        assert_eq!(sorted.len(), 4);
        // 1 must come before 2; 3 must come before 4
        let pos: std::collections::HashMap<u64, usize> =
            sorted.iter().map(|(n, p)| (n.0, *p)).collect();
        assert!(pos[&1] < pos[&2]);
        assert!(pos[&3] < pos[&4]);
    }

    #[test]
    fn topo_sort_three_node_cycle() {
        // Triangle cycle: 1->2->3->1
        let g = make_graph(
            &[(1, &["a"]), (2, &["a"]), (3, &["a"])],
            &[(1, 2, "link"), (2, 3, "link"), (3, 1, "link")],
        );
        let proj = project_all(&g);
        assert!(matches!(
            topological_sort(&proj),
            Err(TopoSortError::CycleDetected)
        ));
    }

    #[test]
    fn topo_sort_empty_graph() {
        let g = SeleneGraph::new();
        let proj = project_all(&g);
        let sorted = topological_sort(&proj).unwrap();
        assert!(sorted.is_empty());
    }

    // ── Articulation Points edge cases ─────────────────────────────

    #[test]
    fn articulation_points_single_node() {
        let g = make_graph(&[(1, &["a"])], &[]);
        let proj = project_all(&g);
        let aps = articulation_points(&proj);
        assert!(aps.is_empty());
    }

    #[test]
    fn articulation_points_complete_k4() {
        // K4: every pair connected, no articulation points
        let g = make_graph(
            &[(1, &["a"]), (2, &["a"]), (3, &["a"]), (4, &["a"])],
            &[
                (1, 2, "l"),
                (2, 1, "l"),
                (1, 3, "l"),
                (3, 1, "l"),
                (1, 4, "l"),
                (4, 1, "l"),
                (2, 3, "l"),
                (3, 2, "l"),
                (2, 4, "l"),
                (4, 2, "l"),
                (3, 4, "l"),
                (4, 3, "l"),
            ],
        );
        let proj = project_all(&g);
        let aps = articulation_points(&proj);
        assert!(aps.is_empty(), "K4 has no articulation points");
    }

    #[test]
    fn articulation_points_two_nodes_one_edge() {
        // 1-2: neither is an articulation point (removing one leaves a single node)
        let g = make_graph(&[(1, &["a"]), (2, &["a"])], &[(1, 2, "l"), (2, 1, "l")]);
        let proj = project_all(&g);
        let aps = articulation_points(&proj);
        assert!(aps.is_empty(), "two-node graph has no articulation points");
    }

    #[test]
    fn articulation_points_empty_graph() {
        let g = SeleneGraph::new();
        let proj = project_all(&g);
        let aps = articulation_points(&proj);
        assert!(aps.is_empty());
    }

    // ── Bridges edge cases ─────────────────────────────────────────

    #[test]
    fn bridges_single_edge() {
        // 1-2: the one edge is a bridge
        let g = make_graph(&[(1, &["a"]), (2, &["a"])], &[(1, 2, "l"), (2, 1, "l")]);
        let proj = project_all(&g);
        let br = bridges(&proj);
        assert_eq!(br.len(), 1);
    }

    #[test]
    fn bridges_complete_k4_none() {
        // K4: no bridges because every edge has alternate paths
        let g = make_graph(
            &[(1, &["a"]), (2, &["a"]), (3, &["a"]), (4, &["a"])],
            &[
                (1, 2, "l"),
                (2, 1, "l"),
                (1, 3, "l"),
                (3, 1, "l"),
                (1, 4, "l"),
                (4, 1, "l"),
                (2, 3, "l"),
                (3, 2, "l"),
                (2, 4, "l"),
                (4, 2, "l"),
                (3, 4, "l"),
                (4, 3, "l"),
            ],
        );
        let proj = project_all(&g);
        let br = bridges(&proj);
        assert!(br.is_empty(), "K4 has no bridges");
    }

    #[test]
    fn bridges_single_node_none() {
        let g = make_graph(&[(1, &["a"])], &[]);
        let proj = project_all(&g);
        let br = bridges(&proj);
        assert!(br.is_empty());
    }

    #[test]
    fn bridges_empty_graph() {
        let g = SeleneGraph::new();
        let proj = project_all(&g);
        let br = bridges(&proj);
        assert!(br.is_empty());
    }

    // ── Reference Building Tests ────────────────────────────────────

    #[test]
    fn reference_building_wcc() {
        let g = selene_testing::reference_building::reference_building(1);
        let proj = project_all(&g);
        // Parking garage is disconnected from HVAC when considering ALL edge types
        // But they're connected through containment via campus
        // So WCC should be 1 (all connected through containment)
        let count = wcc_count(&proj);
        assert_eq!(
            count, 1,
            "expected 1 component (all connected via campus), got {count}"
        );
    }

    #[test]
    fn reference_building_scc_finds_hvac_cycles() {
        let g = selene_testing::reference_building::reference_building(1);
        // Project only HVAC edges to find cycles
        let proj = GraphProjection::build(
            &g,
            &ProjectionConfig {
                name: "hvac".into(),
                node_labels: vec![],
                edge_labels: vec![
                    IStr::new("feeds"),
                    IStr::new("serves"),
                    IStr::new("returns_to"),
                ],
                weight_property: None,
            },
            None,
        );
        let count = scc_count(&proj);
        // HVAC has cycles (AHU feeds VAV, VAV serves zone, zone returns_to AHU)
        // So there should be fewer SCCs than nodes (at least one multi-node SCC)
        assert!(
            count < proj.node_count() as usize,
            "expected fewer SCCs than nodes due to HVAC cycles, got {count} SCCs for {} nodes",
            proj.node_count()
        );
    }

    #[test]
    fn reference_building_containment_index() {
        let g = selene_testing::reference_building::reference_building(1);
        let idx = ContainmentIndex::build(&g);
        // Campus (1) should be ancestor of all building nodes
        assert!(
            idx.is_ancestor(NodeId(1), NodeId(2)),
            "campus should be ancestor of building"
        );
    }
}
