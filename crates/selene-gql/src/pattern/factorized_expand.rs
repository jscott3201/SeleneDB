//! Factorized edge expansion: single-hop traversal that stores parent
//! linkage instead of replicating parent columns.
//!
//! Produces a new [`FactorLevel`] with only the edge/target columns
//! and an `Arc<[u32]>` of parent indices pointing back into the
//! previous level. This avoids the O(n) column gather that the flat
//! expand performs for every parent column.

use std::sync::Arc;

use roaring::RoaringBitmap;
use selene_core::{EdgeId, IStr, NodeId};
use selene_graph::SeleneGraph;
use smallvec::SmallVec;

use crate::ast::pattern::{EdgeDirection, LabelExpr};
use crate::types::chunk::{ColumnBuilder, ColumnKind};
use crate::types::error::GqlError;
use crate::types::factor::{FactorLevel, FactorizedChunk, LevelSchema};

use super::expand::ExpandContext;
use super::scan::{label_matches, resolve_label_expr};

/// Factorized single-hop edge expansion.
///
/// Shares the same neighbor iteration logic as `execute_expand_chunk`
/// (CSR fast path, adjacency fallback, label/scope/property filtering)
/// but instead of gathering parent columns, stores the source row
/// indices as `parent_indices` in a new [`FactorLevel`].
///
/// The source variable is resolved from the factorized chunk's level
/// stack (may be in any level, not just the deepest).
pub(crate) fn execute_expand_factorized(
    input: &FactorizedChunk,
    ctx: &ExpandContext<'_>,
    target_property_filters: &[crate::pattern::scan::PropertyFilter],
    edge_property_filters: &[crate::pattern::scan::PropertyFilter],
    sip_ctx: &crate::pattern::context::PatternContext,
) -> Result<FactorizedChunk, GqlError> {
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

    // Find source variable's location in the factorized chunk
    let (src_level_idx, src_col_slot) = input.find_var(&source_var).ok_or_else(|| {
        GqlError::internal(format!(
            "source variable '{source_var}' not found in factorized chunk"
        ))
    })?;

    // Phase 1: collect (deepest_active_row_idx, edge_id, target_id) triples.
    let deep = input.deepest();
    let estimated = deep.active_len() * 4;
    let mut parent_indices: Vec<u32> = Vec::with_capacity(estimated);
    let mut edge_ids: Vec<EdgeId> = Vec::with_capacity(estimated);
    let mut target_ids: Vec<NodeId> = Vec::with_capacity(estimated);

    for row_idx in deep.selection.active_indices(deep.len) {
        // Resolve source node ID by walking parent chain to the source's level
        let src_row = input.resolve_row_at_level(row_idx, src_level_idx);
        let source_id = input.levels[src_level_idx].get_node_id(src_col_slot, src_row)?;

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

            let expand_csr = |dir_out: bool| -> &[selene_graph::CsrNeighbor] {
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

            let emit_csr = |neighbors: &[selene_graph::CsrNeighbor],
                            parent_indices: &mut Vec<u32>,
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
                    parent_indices.push(row_u32);
                    edge_ids.push(nbr.edge_id);
                    target_ids.push(nbr.node_id);
                }
            };

            match direction {
                EdgeDirection::Out => {
                    emit_csr(
                        expand_csr(true),
                        &mut parent_indices,
                        &mut edge_ids,
                        &mut target_ids,
                    );
                }
                EdgeDirection::In => {
                    emit_csr(
                        expand_csr(false),
                        &mut parent_indices,
                        &mut edge_ids,
                        &mut target_ids,
                    );
                }
                EdgeDirection::Any => {
                    emit_csr(
                        expand_csr(true),
                        &mut parent_indices,
                        &mut edge_ids,
                        &mut target_ids,
                    );
                    emit_csr(
                        expand_csr(false),
                        &mut parent_indices,
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
                    parent_indices.push(row_u32);
                    edge_ids.push(eid);
                    target_ids.push(target_id);
                }
            }
        }
    }

    // Phase 2: build new FactorLevel (NO gather of parent columns).
    let output_len = parent_indices.len();
    let mut level_columns: SmallVec<[crate::types::chunk::Column; 4]> = SmallVec::new();
    let mut level_schema = LevelSchema::new();

    // Add edge variable column (if named)
    if let Some(ev) = edge_var {
        let mut builder = ColumnBuilder::new_edge_ids(output_len);
        for &eid in &edge_ids {
            builder.append_edge_id(eid);
        }
        level_columns.push(builder.finish());
        level_schema.push(ev, ColumnKind::EdgeId);
    }

    // Add target variable column
    let mut target_builder = ColumnBuilder::new_node_ids(output_len);
    for &tid in &target_ids {
        target_builder.append_node_id(tid);
    }
    level_columns.push(target_builder.finish());
    level_schema.push(target_var, ColumnKind::NodeId);

    // Build factorized output: clone parent levels + new level
    let mut output = input.clone();
    output.push_level(FactorLevel::expansion(
        level_columns,
        level_schema,
        output_len,
        Arc::from(parent_indices),
    ));

    Ok(output)
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
