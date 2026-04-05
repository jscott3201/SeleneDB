//! Edge expansion -- single-hop traversal along edges.
//!
//! For each input binding, looks up adjacency indexes, filters by edge
//! label and target label, applies property pushdown, and produces
//! extended bindings with edge + target node bound.

use arrow::array::Array;
use roaring::RoaringBitmap;
use selene_core::{EdgeId, IStr, NodeId};
use selene_graph::{CsrAdjacency, CsrNeighbor, SeleneGraph};
use smallvec::SmallVec;

use crate::ast::pattern::{EdgeDirection, LabelExpr};
use crate::types::binding::{Binding, BoundValue};
use crate::types::chunk::{ColumnBuilder, ColumnKind, DataChunk};
use crate::types::error::GqlError;

use super::scan::{label_matches, resolve_label_expr};

/// Shared context for edge expansion operations (single-hop and variable-length).
///
/// Bundles the 9 parameters common across every expand variant:
/// graph references, pattern variables, label filters, and direction.
pub(crate) struct ExpandContext<'a> {
    pub graph: &'a SeleneGraph,
    pub scope: Option<&'a RoaringBitmap>,
    pub csr: Option<&'a CsrAdjacency>,
    pub source_var: IStr,
    pub edge_var: Option<IStr>,
    pub target_var: IStr,
    pub edge_labels: Option<&'a LabelExpr>,
    pub target_labels: Option<&'a LabelExpr>,
    pub direction: EdgeDirection,
}

/// Columnar single-hop edge expansion producing a DataChunk.
///
/// For each active source row in the input chunk:
/// 1. Read source NodeId from the source column
/// 2. Look up neighbors (CSR fast path or adjacency fallback)
/// 3. Filter by edge label, target label, scope, property pushdowns
/// 4. Append matching edge/target IDs to new columns
/// 5. Replicate parent columns via gather on source row indices
///
/// Output DataChunk has: all parent columns (replicated) + optional EdgeIds + target NodeIds.
pub(crate) fn execute_expand_chunk(
    input: &DataChunk,
    ctx: &ExpandContext<'_>,
    target_property_filters: &[crate::pattern::scan::PropertyFilter],
    edge_property_filters: &[crate::pattern::scan::PropertyFilter],
    sip_ctx: &crate::pattern::context::PatternContext,
) -> Result<DataChunk, GqlError> {
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
    let target_bitmap = target_labels.map(|l| resolve_label_expr(l, graph));
    let source_col = input.node_id_column(&source_var)?;

    // Phase 1: collect (source_row_idx, edge_id, target_id) triples.
    // Pre-allocate assuming ~4 neighbors per source (typical for building graphs).
    let estimated = input.active_len() * 4;
    let mut source_indices: Vec<u32> = Vec::with_capacity(estimated);
    let mut edge_ids: Vec<EdgeId> = Vec::with_capacity(estimated);
    let mut target_ids: Vec<NodeId> = Vec::with_capacity(estimated);

    for row_idx in input.active_indices() {
        if source_col.is_null(row_idx) {
            continue;
        }
        let source_id = NodeId(source_col.value(row_idx));

        // SIP check
        if sip_ctx.has_filters() && !sip_ctx.check(source_var, source_id) {
            continue;
        }

        let row_u32 = row_idx as u32;

        if let Some(csr) = csr {
            let simple_label = match edge_labels {
                Some(LabelExpr::Name(name)) => Some(*name),
                _ => None,
            };

            let expand_csr = |dir_out: bool| -> &[CsrNeighbor] {
                if dir_out {
                    if let Some(lbl) = simple_label {
                        csr.outgoing_typed(source_id, lbl)
                    } else {
                        csr.outgoing(source_id)
                    }
                } else if let Some(lbl) = simple_label {
                    csr.incoming_typed(source_id, lbl)
                } else {
                    csr.incoming(source_id)
                }
            };

            let emit_csr = |neighbors: &[CsrNeighbor],
                            source_indices: &mut Vec<u32>,
                            edge_ids: &mut Vec<EdgeId>,
                            target_ids: &mut Vec<NodeId>| {
                for nbr in neighbors {
                    if !label_already_ok(simple_label, edge_labels, nbr.label, graph) {
                        continue;
                    }
                    if !target_scope_ok(nbr.node_id, target_bitmap.as_ref(), scope) {
                        continue;
                    }
                    if !edge_property_filters.is_empty()
                        && !edge_property_filters
                            .iter()
                            .all(|f| f.matches_edge(graph, nbr.edge_id))
                    {
                        continue;
                    }
                    if !target_property_filters.is_empty()
                        && !target_property_filters
                            .iter()
                            .all(|f| f.matches(graph, nbr.node_id))
                    {
                        continue;
                    }
                    source_indices.push(row_u32);
                    edge_ids.push(nbr.edge_id);
                    target_ids.push(nbr.node_id);
                }
            };

            match direction {
                EdgeDirection::Out => {
                    emit_csr(
                        expand_csr(true),
                        &mut source_indices,
                        &mut edge_ids,
                        &mut target_ids,
                    );
                }
                EdgeDirection::In => {
                    emit_csr(
                        expand_csr(false),
                        &mut source_indices,
                        &mut edge_ids,
                        &mut target_ids,
                    );
                }
                EdgeDirection::Any => {
                    emit_csr(
                        expand_csr(true),
                        &mut source_indices,
                        &mut edge_ids,
                        &mut target_ids,
                    );
                    emit_csr(
                        expand_csr(false),
                        &mut source_indices,
                        &mut edge_ids,
                        &mut target_ids,
                    );
                }
            }
        } else {
            // Adjacency list fallback
            let edge_lists: SmallVec<[&[EdgeId]; 2]> = match direction {
                EdgeDirection::Out => smallvec::smallvec![graph.outgoing(source_id)],
                EdgeDirection::In => smallvec::smallvec![graph.incoming(source_id)],
                EdgeDirection::Any => {
                    smallvec::smallvec![graph.outgoing(source_id), graph.incoming(source_id)]
                }
            };

            for edge_id_slice in edge_lists {
                for &eid in edge_id_slice {
                    let Some(edge) = graph.get_edge(eid) else {
                        continue;
                    };
                    if let Some(labels) = edge_labels
                        && !label_matches(edge.label, labels, graph)
                    {
                        continue;
                    }
                    let target_id = match direction {
                        EdgeDirection::Out => edge.target,
                        EdgeDirection::In => edge.source,
                        EdgeDirection::Any => {
                            if edge.source == source_id {
                                edge.target
                            } else {
                                edge.source
                            }
                        }
                    };
                    if !target_scope_ok(target_id, target_bitmap.as_ref(), scope) {
                        continue;
                    }
                    if !edge_property_filters.is_empty()
                        && !edge_property_filters
                            .iter()
                            .all(|f| f.matches_edge(graph, eid))
                    {
                        continue;
                    }
                    if !target_property_filters.is_empty()
                        && !target_property_filters
                            .iter()
                            .all(|f| f.matches(graph, target_id))
                    {
                        continue;
                    }
                    source_indices.push(row_u32);
                    edge_ids.push(eid);
                    target_ids.push(target_id);
                }
            }
        }
    }

    // Phase 2: build output DataChunk.
    let output_len = source_indices.len();

    // Replicate parent columns using gather
    let mut columns: SmallVec<[crate::types::chunk::Column; 8]> = input
        .columns()
        .iter()
        .map(|col| col.gather(&source_indices))
        .collect();
    let mut schema = input.schema().clone();

    // Add edge variable column (if named)
    if let Some(ev) = edge_var {
        let mut builder = ColumnBuilder::new_edge_ids(output_len);
        for &eid in &edge_ids {
            builder.append_edge_id(eid);
        }
        columns.push(builder.finish());
        schema.extend(ev, ColumnKind::EdgeId);
    }

    // Add target variable column
    let mut target_builder = ColumnBuilder::new_node_ids(output_len);
    for &tid in &target_ids {
        target_builder.append_node_id(tid);
    }
    columns.push(target_builder.finish());
    schema.extend(target_var, ColumnKind::NodeId);

    Ok(DataChunk::from_columns(columns, schema, output_len))
}

/// Check if target node passes bitmap and scope filters.
#[inline]
fn target_scope_ok(
    target_id: NodeId,
    target_bitmap: Option<&RoaringBitmap>,
    scope: Option<&RoaringBitmap>,
) -> bool {
    if let Some(bitmap) = target_bitmap
        && !bitmap.contains(target_id.0 as u32)
    {
        return false;
    }
    if let Some(scope_bitmap) = scope
        && !scope_bitmap.contains(target_id.0 as u32)
    {
        return false;
    }
    true
}

/// Check if CSR neighbor label is already filtered or needs further check.
#[inline]
fn label_already_ok(
    simple_label: Option<IStr>,
    edge_labels: Option<&LabelExpr>,
    neighbor_label: IStr,
    graph: &SeleneGraph,
) -> bool {
    if simple_label.is_some() {
        return true; // already filtered by typed CSR lookup
    }
    if let Some(labels) = edge_labels {
        return label_matches(neighbor_label, labels, graph);
    }
    true // no label filter
}

/// Execute a single-hop edge expansion.
///
/// For each input binding:
/// 1. Get the source node ID from the binding
/// 2. Look up outgoing/incoming/both edges from adjacency index
/// 3. Filter by edge label expression
/// 4. Filter by target node label expression
/// 5. Apply auth scope on target
/// 6. Create extended binding with edge var + target var
#[allow(dead_code)]
pub(crate) fn execute_expand(
    input: &[Binding],
    ctx: &ExpandContext<'_>,
    target_property_filters: &[crate::pattern::scan::PropertyFilter],
    edge_property_filters: &[crate::pattern::scan::PropertyFilter],
    sip_ctx: &crate::pattern::context::PatternContext,
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
    // Pre-resolve target label bitmap for O(1) membership checks
    let target_bitmap = target_labels.map(|l| resolve_label_expr(l, graph));

    // Use parallel expand when: rayon enabled, large input, no CSR
    // (expand_single uses adjacency list fallback, not CSR)
    #[cfg(feature = "rayon")]
    if csr.is_none() && input.len() >= crate::parallel::parallel_threshold() {
        use rayon::prelude::*;

        // Pre-filter with SIP bitmaps before parallelizing. PatternContext
        // uses RefCell internally (not Sync), so we filter here rather than
        // passing sip_ctx into expand_single.
        let filtered_input: Vec<&Binding> = if sip_ctx.has_filters() {
            input
                .iter()
                .filter(|b| {
                    b.get_node_id(&source_var)
                        .map(|nid| sip_ctx.check(source_var, nid))
                        .unwrap_or(true)
                })
                .collect()
        } else {
            input.iter().collect()
        };

        let results: Vec<Result<Vec<Binding>, GqlError>> = filtered_input
            .par_iter()
            .with_min_len(crate::parallel::parallel_threshold() / 4)
            .map(|binding| {
                expand_single(
                    binding,
                    ctx,
                    target_bitmap.as_ref(),
                    target_property_filters,
                    edge_property_filters,
                )
            })
            .collect();
        let mut output = Vec::new();
        for r in results {
            output.extend(r?);
        }
        return Ok(output);
    }

    let mut output = Vec::with_capacity(input.len() * 4);

    for binding in input {
        let source_id = binding.get_node_id(&source_var)?;

        // SIP check: skip if this source node was filtered out by earlier ops
        if sip_ctx.has_filters() && !sip_ctx.check(source_var, source_id) {
            continue;
        }

        if let Some(csr) = csr {
            // ── CSR fast path ──
            // When edge_labels is a simple Name, use typed lookup to skip irrelevant edges.
            // This avoids get_edge() entirely -- label is already in CsrNeighbor.
            let simple_label = match edge_labels {
                Some(LabelExpr::Name(name)) => Some(*name),
                _ => None,
            };

            // Collect neighbor slices based on direction + optional type filter
            let expand_csr_direction = |dir_out: bool| -> &[CsrNeighbor] {
                if dir_out {
                    if let Some(lbl) = simple_label {
                        csr.outgoing_typed(source_id, lbl)
                    } else {
                        csr.outgoing(source_id)
                    }
                } else if let Some(lbl) = simple_label {
                    csr.incoming_typed(source_id, lbl)
                } else {
                    csr.incoming(source_id)
                }
            };

            match direction {
                EdgeDirection::Out => {
                    emit_csr_neighbors(
                        expand_csr_direction(true),
                        binding,
                        ctx,
                        simple_label.is_some(),
                        target_bitmap.as_ref(),
                        &mut output,
                        target_property_filters,
                        edge_property_filters,
                    );
                }
                EdgeDirection::In => {
                    emit_csr_neighbors(
                        expand_csr_direction(false),
                        binding,
                        ctx,
                        simple_label.is_some(),
                        target_bitmap.as_ref(),
                        &mut output,
                        target_property_filters,
                        edge_property_filters,
                    );
                }
                EdgeDirection::Any => {
                    emit_csr_neighbors(
                        expand_csr_direction(true),
                        binding,
                        ctx,
                        simple_label.is_some(),
                        target_bitmap.as_ref(),
                        &mut output,
                        target_property_filters,
                        edge_property_filters,
                    );
                    emit_csr_neighbors(
                        expand_csr_direction(false),
                        binding,
                        ctx,
                        simple_label.is_some(),
                        target_bitmap.as_ref(),
                        &mut output,
                        target_property_filters,
                        edge_property_filters,
                    );
                }
            }
        } else {
            // ── Original ImblMap path (unchanged) ──
            let edge_lists: Vec<&[EdgeId]> = match direction {
                EdgeDirection::Out => vec![graph.outgoing(source_id)],
                EdgeDirection::In => vec![graph.incoming(source_id)],
                EdgeDirection::Any => vec![graph.outgoing(source_id), graph.incoming(source_id)],
            };

            for edge_ids in edge_lists {
                for &eid in edge_ids {
                    let Some(edge) = graph.get_edge(eid) else {
                        continue;
                    };
                    if let Some(labels) = edge_labels
                        && !label_matches(edge.label, labels, graph)
                    {
                        continue;
                    }
                    let target_id = match direction {
                        EdgeDirection::Out => edge.target,
                        EdgeDirection::In => edge.source,
                        EdgeDirection::Any => {
                            if edge.source == source_id {
                                edge.target
                            } else {
                                edge.source
                            }
                        }
                    };
                    if let Some(bitmap) = &target_bitmap
                        && !bitmap.contains(target_id.0 as u32)
                    {
                        continue;
                    }
                    if let Some(scope_bitmap) = scope
                        && !scope_bitmap.contains(target_id.0 as u32)
                    {
                        continue;
                    }
                    // Edge property filters: check before clone to avoid allocation
                    if !edge_property_filters.is_empty()
                        && !edge_property_filters
                            .iter()
                            .all(|f| f.matches_edge(graph, eid))
                    {
                        continue;
                    }
                    // Target node property filters: check before clone
                    if !target_property_filters.is_empty()
                        && !target_property_filters
                            .iter()
                            .all(|f| f.matches(graph, target_id))
                    {
                        continue;
                    }
                    let mut new_binding = binding.clone();
                    if let Some(ev) = edge_var {
                        new_binding.bind(ev, BoundValue::Edge(eid));
                    }
                    new_binding.bind(target_var, BoundValue::Node(target_id));
                    output.push(new_binding);
                }
            }
        }
    }

    Ok(output)
}

/// Emit bindings from a CSR neighbor slice. Handles complex label expressions
/// (when typed lookup wasn't used) and target/scope filtering.
#[inline]
#[allow(clippy::too_many_arguments)]
fn emit_csr_neighbors(
    neighbors: &[CsrNeighbor],
    binding: &Binding,
    ctx: &ExpandContext<'_>,
    label_already_filtered: bool,
    target_bitmap: Option<&RoaringBitmap>,
    output: &mut Vec<Binding>,
    target_property_filters: &[crate::pattern::scan::PropertyFilter],
    edge_property_filters: &[crate::pattern::scan::PropertyFilter],
) {
    let edge_var = ctx.edge_var;
    let target_var = ctx.target_var;
    let edge_labels = ctx.edge_labels;
    let scope = ctx.scope;
    let graph = ctx.graph;
    for nbr in neighbors {
        // Complex label filter (OR/AND/NOT expressions) -- only if not already typed-filtered
        if !label_already_filtered
            && let Some(labels) = edge_labels
            && !label_matches(nbr.label, labels, graph)
        {
            continue;
        }

        if let Some(bitmap) = target_bitmap
            && !bitmap.contains(nbr.node_id.0 as u32)
        {
            continue;
        }
        if let Some(scope_bitmap) = scope
            && !scope_bitmap.contains(nbr.node_id.0 as u32)
        {
            continue;
        }
        if !edge_property_filters.is_empty()
            && !edge_property_filters
                .iter()
                .all(|f| f.matches_edge(graph, nbr.edge_id))
        {
            continue;
        }
        if !target_property_filters.is_empty()
            && !target_property_filters
                .iter()
                .all(|f| f.matches(graph, nbr.node_id))
        {
            continue;
        }

        let mut new_binding = binding.clone();
        if let Some(ev) = edge_var {
            new_binding.bind(ev, BoundValue::Edge(nbr.edge_id));
        }
        new_binding.bind(target_var, BoundValue::Node(nbr.node_id));
        output.push(new_binding);
    }
}

/// Expand a single binding -- extracted for parallel use.
fn expand_single(
    binding: &Binding,
    ctx: &ExpandContext<'_>,
    target_bitmap: Option<&RoaringBitmap>,
    target_property_filters: &[crate::pattern::scan::PropertyFilter],
    edge_property_filters: &[crate::pattern::scan::PropertyFilter],
) -> Result<Vec<Binding>, GqlError> {
    let source_var = ctx.source_var;
    let edge_var = ctx.edge_var;
    let target_var = ctx.target_var;
    let edge_labels = ctx.edge_labels;
    let direction = ctx.direction;
    let scope = ctx.scope;
    let graph = ctx.graph;
    let source_id = binding.get_node_id(&source_var)?;
    let mut output = Vec::new();

    let edge_lists: Vec<&[EdgeId]> = match direction {
        EdgeDirection::Out => vec![graph.outgoing(source_id)],
        EdgeDirection::In => vec![graph.incoming(source_id)],
        EdgeDirection::Any => vec![graph.outgoing(source_id), graph.incoming(source_id)],
    };

    for edge_ids in edge_lists {
        for &eid in edge_ids {
            let Some(edge) = graph.get_edge(eid) else {
                continue;
            };
            if let Some(labels) = edge_labels
                && !label_matches(edge.label, labels, graph)
            {
                continue;
            }
            let target_id = match direction {
                EdgeDirection::Out => edge.target,
                EdgeDirection::In => edge.source,
                EdgeDirection::Any => {
                    if edge.source == source_id {
                        edge.target
                    } else {
                        edge.source
                    }
                }
            };
            if let Some(bitmap) = target_bitmap
                && !bitmap.contains(target_id.0 as u32)
            {
                continue;
            }
            if let Some(scope_bitmap) = scope
                && !scope_bitmap.contains(target_id.0 as u32)
            {
                continue;
            }
            if !edge_property_filters.is_empty()
                && !edge_property_filters
                    .iter()
                    .all(|f| f.matches_edge(graph, eid))
            {
                continue;
            }
            if !target_property_filters.is_empty()
                && !target_property_filters
                    .iter()
                    .all(|f| f.matches(graph, target_id))
            {
                continue;
            }
            let mut new_binding = binding.clone();
            if let Some(ev) = edge_var {
                new_binding.bind(ev, BoundValue::Edge(eid));
            }
            new_binding.bind(target_var, BoundValue::Node(target_id));
            output.push(new_binding);
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{LabelSet, NodeId, PropertyMap, Value};
    use smol_str::SmolStr;

    fn setup_graph() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();

        // Node 1: building
        m.create_node(
            LabelSet::from_strs(&["building"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("HQ")))]),
        )
        .unwrap();
        // Node 2: floor
        m.create_node(LabelSet::from_strs(&["floor"]), PropertyMap::new())
            .unwrap();
        // Node 3: sensor
        m.create_node(LabelSet::from_strs(&["sensor"]), PropertyMap::new())
            .unwrap();
        // Node 4: equipment
        m.create_node(LabelSet::from_strs(&["equipment"]), PropertyMap::new())
            .unwrap();

        // Edges: building -contains-> floor -contains-> sensor
        //        sensor -feeds-> equipment
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
        m.create_edge(NodeId(3), IStr::new("feeds"), NodeId(4), PropertyMap::new())
            .unwrap();

        m.commit(0).unwrap();
        g
    }

    #[test]
    fn expand_outgoing() {
        let g = setup_graph();
        let input = vec![Binding::single(IStr::new("a"), BoundValue::Node(NodeId(1)))];
        let result = execute_expand(
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
            &[],
            &[],
            &crate::pattern::context::PatternContext::new(),
        )
        .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get_node_id(&IStr::new("b")).unwrap(), NodeId(2));
    }

    #[test]
    fn expand_incoming() {
        let g = setup_graph();
        let input = vec![Binding::single(IStr::new("a"), BoundValue::Node(NodeId(2)))];
        let result = execute_expand(
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
                direction: EdgeDirection::In,
            },
            &[],
            &[],
            &crate::pattern::context::PatternContext::new(),
        )
        .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get_node_id(&IStr::new("b")).unwrap(), NodeId(1));
    }

    #[test]
    fn expand_undirected() {
        let g = setup_graph();
        // Floor (2) has contains edges in both directions
        let input = vec![Binding::single(IStr::new("a"), BoundValue::Node(NodeId(2)))];
        let result = execute_expand(
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
                direction: EdgeDirection::Any,
            },
            &[],
            &[],
            &crate::pattern::context::PatternContext::new(),
        )
        .unwrap();
        // Outgoing: 2->3, Incoming: 1->2 (target is 1)
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn expand_with_edge_label_filter() {
        let g = setup_graph();
        let input = vec![Binding::single(IStr::new("a"), BoundValue::Node(NodeId(3)))];
        // Sensor 3 has outgoing: feeds->4. Filter for "contains" should return nothing.
        let result = execute_expand(
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
            &[],
            &[],
            &crate::pattern::context::PatternContext::new(),
        )
        .unwrap();
        assert_eq!(result.len(), 0);

        // Filter for "feeds" should return equipment
        let result = execute_expand(
            &input,
            &ExpandContext {
                graph: &g,
                scope: None,
                csr: None,
                source_var: IStr::new("a"),
                edge_var: None,
                target_var: IStr::new("b"),
                edge_labels: Some(&LabelExpr::Name(IStr::new("feeds"))),
                target_labels: None,
                direction: EdgeDirection::Out,
            },
            &[],
            &[],
            &crate::pattern::context::PatternContext::new(),
        )
        .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get_node_id(&IStr::new("b")).unwrap(), NodeId(4));
    }

    #[test]
    fn expand_with_target_label_filter() {
        let g = setup_graph();
        let input = vec![Binding::single(IStr::new("a"), BoundValue::Node(NodeId(1)))];
        // Building 1 -contains-> Floor 2. Filter target for "sensor" should return nothing.
        let result = execute_expand(
            &input,
            &ExpandContext {
                graph: &g,
                scope: None,
                csr: None,
                source_var: IStr::new("a"),
                edge_var: None,
                target_var: IStr::new("b"),
                edge_labels: Some(&LabelExpr::Name(IStr::new("contains"))),
                target_labels: Some(&LabelExpr::Name(IStr::new("sensor"))),
                direction: EdgeDirection::Out,
            },
            &[],
            &[],
            &crate::pattern::context::PatternContext::new(),
        )
        .unwrap();
        assert_eq!(result.len(), 0);

        // Filter target for "floor" should return floor
        let result = execute_expand(
            &input,
            &ExpandContext {
                graph: &g,
                scope: None,
                csr: None,
                source_var: IStr::new("a"),
                edge_var: None,
                target_var: IStr::new("b"),
                edge_labels: Some(&LabelExpr::Name(IStr::new("contains"))),
                target_labels: Some(&LabelExpr::Name(IStr::new("floor"))),
                direction: EdgeDirection::Out,
            },
            &[],
            &[],
            &crate::pattern::context::PatternContext::new(),
        )
        .unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn expand_with_edge_variable() {
        let g = setup_graph();
        let input = vec![Binding::single(IStr::new("a"), BoundValue::Node(NodeId(1)))];
        let result = execute_expand(
            &input,
            &ExpandContext {
                graph: &g,
                scope: None,
                csr: None,
                source_var: IStr::new("a"),
                edge_var: Some(IStr::new("e")),
                target_var: IStr::new("b"),
                edge_labels: None,
                target_labels: None,
                direction: EdgeDirection::Out,
            },
            &[],
            &[],
            &crate::pattern::context::PatternContext::new(),
        )
        .unwrap();
        assert_eq!(result.len(), 1);
        // Edge variable should be bound
        assert!(result[0].get_edge_id(&IStr::new("e")).is_ok());
    }

    #[test]
    fn expand_with_scope() {
        let g = setup_graph();
        let input = vec![Binding::single(IStr::new("a"), BoundValue::Node(NodeId(2)))];
        // Scope: only node 3 (sensor), not node 1 (building)
        let mut scope = RoaringBitmap::new();
        scope.insert(3);
        let result = execute_expand(
            &input,
            &ExpandContext {
                graph: &g,
                scope: Some(&scope),
                csr: None,
                source_var: IStr::new("a"),
                edge_var: None,
                target_var: IStr::new("b"),
                edge_labels: None,
                target_labels: None,
                direction: EdgeDirection::Any,
            },
            &[],
            &[],
            &crate::pattern::context::PatternContext::new(),
        )
        .unwrap();
        // Only outgoing contains->3 should match (incoming from 1 is out of scope)
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get_node_id(&IStr::new("b")).unwrap(), NodeId(3));
    }

    #[test]
    fn expand_multiple_inputs() {
        let g = setup_graph();
        let input = vec![
            Binding::single(IStr::new("a"), BoundValue::Node(NodeId(1))),
            Binding::single(IStr::new("a"), BoundValue::Node(NodeId(2))),
        ];
        let result = execute_expand(
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
            &[],
            &[],
            &crate::pattern::context::PatternContext::new(),
        )
        .unwrap();
        // Building->floor, floor->sensor
        assert_eq!(result.len(), 2);
    }
}
