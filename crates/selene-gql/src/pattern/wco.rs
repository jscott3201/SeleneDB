//! Worst-case optimal (WCO) multi-way join for cyclic graph patterns.
//!
//! Uses Generic Join with sorted merge intersection on CSR neighbor
//! lists. For a triangle `(a)->(b)->(c)->(a)`, enumerates anchor nodes,
//! collects sorted neighbor sets, and intersects them with a two-pointer
//! merge pass. Total work is bounded by the AGM fractional edge cover:
//! O(m^1.5) for triangles instead of O(m^2) with binary joins.

use roaring::RoaringBitmap;
use selene_core::{EdgeId, IStr, NodeId};
use selene_graph::{CsrAdjacency, CsrNeighbor, SeleneGraph};
use smallvec::SmallVec;

use crate::ast::pattern::{EdgeDirection, LabelExpr};
use crate::planner::plan::WcoRelation;
use crate::types::chunk::{ColumnKind, DataChunk};
use crate::types::error::GqlError;

use super::scan::resolve_label_expr;

// ---------------------------------------------------------------------------
// Sorted intersection primitives
// ---------------------------------------------------------------------------

/// Sort CSR neighbors by node ID, returning (node_id, edge_id) pairs.
///
/// CSR neighbors are sorted by label, not node ID. For merge intersection
/// we need node-ID order. Uses a stack-allocated SmallVec for low-degree
/// nodes (the common case in building/IoT graphs).
fn sort_neighbors_by_node_id(neighbors: &[CsrNeighbor]) -> SmallVec<[(u64, EdgeId); 64]> {
    let mut sorted: SmallVec<[(u64, EdgeId); 64]> =
        neighbors.iter().map(|n| (n.node_id.0, n.edge_id)).collect();
    sorted.sort_unstable_by_key(|&(nid, _)| nid);
    sorted
}

/// Two-pointer merge intersection of two sorted (node_id, edge_id) arrays.
///
/// Returns matching node IDs with the edge ID from each input array.
/// O(|a| + |b|) instead of O(|a| * |b|) for nested loops.
fn two_pointer_intersect(
    a: &[(u64, EdgeId)],
    b: &[(u64, EdgeId)],
) -> SmallVec<[(u64, EdgeId, EdgeId); 16]> {
    let mut result = SmallVec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].0.cmp(&b[j].0) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                // Handle duplicates: same node_id may appear multiple times
                // (multiple edges between same pair). Emit all combinations.
                let node_id = a[i].0;
                let a_start = i;
                let b_start = j;
                while i < a.len() && a[i].0 == node_id {
                    i += 1;
                }
                while j < b.len() && b[j].0 == node_id {
                    j += 1;
                }
                for a_val in &a[a_start..i] {
                    for b_val in &b[b_start..j] {
                        result.push((node_id, a_val.1, b_val.1));
                    }
                }
            }
        }
    }
    result
}

// ---------------------------------------------------------------------------
// WCO triangle executor
// ---------------------------------------------------------------------------

/// Execute a WCO multi-way join for cyclic patterns.
///
/// Currently supports triangles (3 relations). Dispatches to the
/// triangle-specific implementation.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_wco_join(
    scan_var: IStr,
    scan_labels: Option<&LabelExpr>,
    scan_property_filters: &[crate::pattern::scan::PropertyFilter],
    relations: &[WcoRelation],
    scope: Option<&RoaringBitmap>,
    graph: &SeleneGraph,
    csr: Option<&CsrAdjacency>,
    _scan_limit: Option<usize>,
) -> Result<DataChunk, GqlError> {
    if relations.len() == 3 {
        execute_wco_triangle(
            scan_var,
            scan_labels,
            scan_property_filters,
            relations,
            scope,
            graph,
            csr,
        )
    } else {
        Err(GqlError::internal(format!(
            "WCO join with {} relations not yet supported (only triangles)",
            relations.len()
        )))
    }
}

/// Execute a triangle WCO join: (a)-[e1]->(b)-[e2]->(c)-[e3]->(a).
///
/// Algorithm:
/// 1. Label scan for anchor variable `a`
/// 2. For each `a`: collect sorted b-candidates (relation 0) and sorted
///    c-from-a candidates (relation 2, the closing edge, inverted)
/// 3. For each `b`: collect sorted c-candidates (relation 1), intersect
///    with c-from-a using two-pointer merge
/// 4. Emit (a, e1, b, e2, c, e3) tuples
#[allow(clippy::too_many_arguments)]
fn execute_wco_triangle(
    scan_var: IStr,
    scan_labels: Option<&LabelExpr>,
    scan_property_filters: &[crate::pattern::scan::PropertyFilter],
    relations: &[WcoRelation],
    scope: Option<&RoaringBitmap>,
    graph: &SeleneGraph,
    csr: Option<&CsrAdjacency>,
) -> Result<DataChunk, GqlError> {
    let rel0 = &relations[0]; // a -> b
    let rel1 = &relations[1]; // b -> c
    let rel2 = &relations[2]; // c -> a (closing edge)

    // Resolve label bitmaps for target filtering
    let scan_bitmap = scan_labels.map(|l| resolve_label_expr(l, graph));
    let rel0_target_bitmap = rel0
        .target_labels
        .as_ref()
        .map(|l| resolve_label_expr(l, graph));
    let rel1_target_bitmap = rel1
        .target_labels
        .as_ref()
        .map(|l| resolve_label_expr(l, graph));

    // Output column builders
    let estimated = 1024;
    let mut a_ids: Vec<u64> = Vec::with_capacity(estimated);
    let mut b_ids: Vec<u64> = Vec::with_capacity(estimated);
    let mut c_ids: Vec<u64> = Vec::with_capacity(estimated);
    let mut e0_ids: Vec<u64> = Vec::with_capacity(estimated);
    let mut e1_ids: Vec<u64> = Vec::with_capacity(estimated);
    let mut e2_ids: Vec<u64> = Vec::with_capacity(estimated);
    let mut last_check: usize = 0;

    // Scan anchor nodes
    let anchor_nodes = collect_scan_nodes(scan_labels, scan_bitmap.as_ref(), scope, graph);

    for &a_node in &anchor_nodes {
        // Apply scan property filters
        if !scan_property_filters.is_empty()
            && !scan_property_filters
                .iter()
                .all(|f| f.matches(graph, a_node))
        {
            continue;
        }

        // Relation 0: a -> b candidates
        let b_sorted = collect_sorted_neighbors(a_node, rel0.edge_label, rel0.direction, csr);

        // Relation 2 (closing edge): the closing relation connects c back to a.
        // We need the set of nodes that could be c, reachable from a via the
        // closing edge. The direction is inverted: if rel2 says c->a (Out),
        // then from a's perspective we look at incoming edges.
        let inverted_dir = match rel2.direction {
            EdgeDirection::Out => EdgeDirection::In,
            EdgeDirection::In => EdgeDirection::Out,
            EdgeDirection::Any => EdgeDirection::Any,
        };
        let c_from_a_sorted = collect_sorted_neighbors(a_node, rel2.edge_label, inverted_dir, csr);

        if c_from_a_sorted.is_empty() {
            continue; // No closing edge candidates, skip this anchor
        }

        for &(b_node_id, e0_edge_id) in &b_sorted {
            let b_node = NodeId(b_node_id);

            // Apply rel0 target label filter
            if let Some(ref bitmap) = rel0_target_bitmap
                && !bitmap.contains(b_node_id as u32)
            {
                continue;
            }
            // Apply scope to b
            if let Some(s) = scope
                && !s.contains(b_node_id as u32)
            {
                continue;
            }
            // Apply rel0 target property filters
            if !rel0.target_property_filters.is_empty()
                && !rel0
                    .target_property_filters
                    .iter()
                    .all(|f| f.matches(graph, b_node))
            {
                continue;
            }

            // Relation 1: b -> c candidates
            let c_sorted = collect_sorted_neighbors(b_node, rel1.edge_label, rel1.direction, csr);

            // Intersect c_sorted with c_from_a_sorted
            let matches = two_pointer_intersect(&c_sorted, &c_from_a_sorted);

            for &(c_node_id, e1_edge_id, e2_edge_id) in &matches {
                let c_node = NodeId(c_node_id);

                // Apply rel1 target label filter
                if let Some(ref bitmap) = rel1_target_bitmap
                    && !bitmap.contains(c_node_id as u32)
                {
                    continue;
                }
                // Apply scope to c
                if let Some(s) = scope
                    && !s.contains(c_node_id as u32)
                {
                    continue;
                }
                // Apply rel1 target property filters
                if !rel1.target_property_filters.is_empty()
                    && !rel1
                        .target_property_filters
                        .iter()
                        .all(|f| f.matches(graph, c_node))
                {
                    continue;
                }

                // Emit the triangle
                a_ids.push(a_node.0);
                b_ids.push(b_node_id);
                c_ids.push(c_node_id);
                e0_ids.push(e0_edge_id.0);
                e1_ids.push(e1_edge_id.0);
                e2_ids.push(e2_edge_id.0);
            }
        }

        // Periodic memory check: verify every 50K new rows
        if a_ids.len() >= last_check + 50_000 {
            last_check = a_ids.len();
            let temp = build_triangle_chunk(
                scan_var, relations, &a_ids, &b_ids, &c_ids, &e0_ids, &e1_ids, &e2_ids,
            );
            crate::runtime::execute::check_chunk_limit(&temp)?;
        }
    }

    Ok(build_triangle_chunk(
        scan_var, relations, &a_ids, &b_ids, &c_ids, &e0_ids, &e1_ids, &e2_ids,
    ))
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Collect all nodes matching the scan labels and scope.
fn collect_scan_nodes(
    _scan_labels: Option<&LabelExpr>,
    scan_bitmap: Option<&RoaringBitmap>,
    scope: Option<&RoaringBitmap>,
    graph: &SeleneGraph,
) -> Vec<NodeId> {
    let mut nodes = Vec::new();

    if let Some(bitmap) = scan_bitmap {
        // Use label bitmap for efficient scan
        let effective = match scope {
            Some(s) => bitmap & s,
            None => bitmap.clone(),
        };
        for nid in &effective {
            if graph.get_node(NodeId(u64::from(nid))).is_some() {
                nodes.push(NodeId(u64::from(nid)));
            }
        }
    } else {
        // No label filter: scan all nodes
        for nid in 0..=graph.max_node_id() {
            let node_id = NodeId(nid);
            if graph.get_node(node_id).is_some() {
                if let Some(s) = scope
                    && !s.contains(nid as u32)
                {
                    continue;
                }
                nodes.push(node_id);
            }
        }
    }

    nodes
}

/// Collect sorted (node_id, edge_id) neighbors from CSR, handling
/// `EdgeDirection::Any` by combining both outgoing and incoming lists.
///
/// Returns an already-sorted SmallVec suitable for two-pointer intersection.
/// For directional queries this is equivalent to `sort_neighbors_by_node_id`
/// on a single CSR slice. For `Any`, both directions are merged and sorted.
fn collect_sorted_neighbors(
    node: NodeId,
    edge_label: Option<IStr>,
    direction: EdgeDirection,
    csr: Option<&CsrAdjacency>,
) -> SmallVec<[(u64, EdgeId); 64]> {
    let Some(csr) = csr else {
        return SmallVec::new();
    };

    if direction == EdgeDirection::Any {
        let out_slice = match edge_label {
            Some(lbl) => csr.outgoing_typed(node, lbl),
            None => csr.outgoing(node),
        };
        let in_slice = match edge_label {
            Some(lbl) => csr.incoming_typed(node, lbl),
            None => csr.incoming(node),
        };
        let mut sorted: SmallVec<[(u64, EdgeId); 64]> =
            SmallVec::with_capacity(out_slice.len() + in_slice.len());
        sorted.extend(out_slice.iter().map(|n| (n.node_id.0, n.edge_id)));
        sorted.extend(in_slice.iter().map(|n| (n.node_id.0, n.edge_id)));
        sorted.sort_unstable_by_key(|&(nid, _)| nid);
        sorted
    } else {
        let slice = match (direction, edge_label) {
            (EdgeDirection::Out, Some(lbl)) => csr.outgoing_typed(node, lbl),
            (EdgeDirection::Out, None) => csr.outgoing(node),
            (EdgeDirection::In, Some(lbl)) => csr.incoming_typed(node, lbl),
            (EdgeDirection::In, None) => csr.incoming(node),
            // Any is handled above
            _ => unreachable!(),
        };
        sort_neighbors_by_node_id(slice)
    }
}

/// Build the output DataChunk from accumulated triangle results.
#[allow(clippy::too_many_arguments)]
fn build_triangle_chunk(
    scan_var: IStr,
    relations: &[WcoRelation],
    a_ids: &[u64],
    b_ids: &[u64],
    c_ids: &[u64],
    e0_ids: &[u64],
    e1_ids: &[u64],
    e2_ids: &[u64],
) -> DataChunk {
    use std::sync::Arc;

    use arrow::array::UInt64Array;

    let len = a_ids.len();
    let mut columns: SmallVec<[crate::types::chunk::Column; 8]> = SmallVec::new();
    let mut schema = crate::types::chunk::ChunkSchema::new();

    // Scan variable (a)
    columns.push(crate::types::chunk::Column::NodeIds(Arc::new(
        UInt64Array::from(a_ids.to_vec()),
    )));
    schema.extend(scan_var, ColumnKind::NodeId);

    // Relation 0: edge e0, target b
    if let Some(ev) = relations[0].edge_var {
        columns.push(crate::types::chunk::Column::EdgeIds(Arc::new(
            UInt64Array::from(e0_ids.to_vec()),
        )));
        schema.extend(ev, ColumnKind::EdgeId);
    }
    columns.push(crate::types::chunk::Column::NodeIds(Arc::new(
        UInt64Array::from(b_ids.to_vec()),
    )));
    schema.extend(relations[0].target_var, ColumnKind::NodeId);

    // Relation 1: edge e1, target c
    if let Some(ev) = relations[1].edge_var {
        columns.push(crate::types::chunk::Column::EdgeIds(Arc::new(
            UInt64Array::from(e1_ids.to_vec()),
        )));
        schema.extend(ev, ColumnKind::EdgeId);
    }
    columns.push(crate::types::chunk::Column::NodeIds(Arc::new(
        UInt64Array::from(c_ids.to_vec()),
    )));
    schema.extend(relations[1].target_var, ColumnKind::NodeId);

    // Relation 2 (closing edge): edge e2 only (target is scan_var, already in schema)
    if let Some(ev) = relations[2].edge_var {
        columns.push(crate::types::chunk::Column::EdgeIds(Arc::new(
            UInt64Array::from(e2_ids.to_vec()),
        )));
        schema.extend(ev, ColumnKind::EdgeId);
    }

    DataChunk::from_columns(columns, schema, len)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort_neighbors_by_node_id() {
        let neighbors = vec![
            CsrNeighbor {
                edge_id: EdgeId(10),
                node_id: NodeId(5),
                label: IStr::new("x"),
            },
            CsrNeighbor {
                edge_id: EdgeId(11),
                node_id: NodeId(2),
                label: IStr::new("x"),
            },
            CsrNeighbor {
                edge_id: EdgeId(12),
                node_id: NodeId(8),
                label: IStr::new("x"),
            },
        ];
        let sorted = sort_neighbors_by_node_id(&neighbors);
        assert_eq!(sorted.len(), 3);
        assert_eq!(sorted[0].0, 2); // node 2 first
        assert_eq!(sorted[1].0, 5); // then 5
        assert_eq!(sorted[2].0, 8); // then 8
        // Edge IDs preserved
        assert_eq!(sorted[0].1, EdgeId(11));
        assert_eq!(sorted[1].1, EdgeId(10));
        assert_eq!(sorted[2].1, EdgeId(12));
    }

    #[test]
    fn test_sort_empty() {
        let sorted = sort_neighbors_by_node_id(&[]);
        assert!(sorted.is_empty());
    }

    #[test]
    fn test_two_pointer_intersect_basic() {
        let a = vec![(1u64, EdgeId(10)), (3, EdgeId(11)), (5, EdgeId(12))];
        let b = vec![(2u64, EdgeId(20)), (3, EdgeId(21)), (5, EdgeId(22))];
        let result = two_pointer_intersect(&a, &b);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], (3, EdgeId(11), EdgeId(21)));
        assert_eq!(result[1], (5, EdgeId(12), EdgeId(22)));
    }

    #[test]
    fn test_two_pointer_intersect_no_overlap() {
        let a = vec![(1u64, EdgeId(10)), (3, EdgeId(11))];
        let b = vec![(2u64, EdgeId(20)), (4, EdgeId(21))];
        let result = two_pointer_intersect(&a, &b);
        assert!(result.is_empty());
    }

    #[test]
    fn test_two_pointer_intersect_empty() {
        let a: Vec<(u64, EdgeId)> = vec![];
        let b = vec![(1u64, EdgeId(10))];
        assert!(two_pointer_intersect(&a, &b).is_empty());
        assert!(two_pointer_intersect(&b, &a).is_empty());
    }

    #[test]
    fn test_two_pointer_intersect_full_overlap() {
        let a = vec![(1u64, EdgeId(10)), (2, EdgeId(11))];
        let b = vec![(1u64, EdgeId(20)), (2, EdgeId(21))];
        let result = two_pointer_intersect(&a, &b);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], (1, EdgeId(10), EdgeId(20)));
        assert_eq!(result[1], (2, EdgeId(11), EdgeId(21)));
    }

    #[test]
    fn test_two_pointer_intersect_duplicates() {
        // Multiple edges between same node pair
        let a = vec![(3u64, EdgeId(10)), (3, EdgeId(11))];
        let b = vec![(3u64, EdgeId(20))];
        let result = two_pointer_intersect(&a, &b);
        // Should produce 2 results: (3, e10, e20) and (3, e11, e20)
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], (3, EdgeId(10), EdgeId(20)));
        assert_eq!(result[1], (3, EdgeId(11), EdgeId(20)));
    }
}
