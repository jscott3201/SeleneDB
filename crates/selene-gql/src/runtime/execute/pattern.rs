//! Pattern operation execution: label scans, expands, joins, cycles.

use roaring::RoaringBitmap;
use selene_core::IStr;
use selene_graph::SeleneGraph;

use crate::pattern::context::PatternContext;
use crate::pattern::expand::ExpandContext;
use crate::pattern::scan::ScanContext;
use crate::pattern::varlength::VarExpandConfig;
use crate::pattern::{expand, factorized_expand, join, scan, varlength};
use crate::planner::plan::*;
use crate::types::binding::{Binding, BoundValue};
use crate::types::chunk::DataChunk;
use crate::types::error::GqlError;
use crate::types::factor::{FactorLevel, FactorizedChunk};
use crate::types::value::GqlValue;

/// Shared execution context for pattern operations, bundling the
/// graph reference, authorization scope, CSR adjacency, SIP
/// bitmaps, and evaluation context (for `$param` resolution in
/// inline property expressions) to keep function signatures concise.
pub(crate) struct PatternExecCtx<'a> {
    pub graph: &'a SeleneGraph,
    pub scope: Option<&'a RoaringBitmap>,
    pub csr: Option<&'a selene_graph::CsrAdjacency>,
    pub sip_ctx: &'a PatternContext,
    pub eval_ctx: &'a crate::runtime::eval::EvalContext<'a>,
}

#[allow(dead_code)]
pub(crate) fn execute_pattern_ops_public(
    ops: &[PatternOp],
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
) -> Result<Vec<Binding>, GqlError> {
    execute_pattern_ops(ops, graph, scope)
}

/// Execute pattern operations seeded with an initial binding (correlated subquery).
/// Variables already bound in `seed` are available to the inner pattern.
#[allow(dead_code)]
pub(crate) fn execute_pattern_ops_correlated(
    ops: &[PatternOp],
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
    seed: &Binding,
) -> Result<Vec<Binding>, GqlError> {
    execute_pattern_ops_with_limit_and_seed(ops, graph, scope, None, Some(seed))
}

/// Execute correlated pattern operations with an explicit EvalContext.
pub(crate) fn execute_pattern_ops_correlated_with_ctx(
    ops: &[PatternOp],
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
    seed: &Binding,
    eval_ctx: &crate::runtime::eval::EvalContext<'_>,
) -> Result<Vec<Binding>, GqlError> {
    execute_pattern_ops_with_limit_seed_and_ctx(ops, graph, scope, None, Some(seed), Some(eval_ctx))
}

pub(super) fn execute_pattern_ops(
    ops: &[PatternOp],
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
) -> Result<Vec<Binding>, GqlError> {
    execute_pattern_ops_with_limit_and_seed(ops, graph, scope, None, None)
}

/// Execute pattern operations with an explicit EvalContext for `$param` resolution.
pub(crate) fn execute_pattern_ops_with_eval_ctx(
    ops: &[PatternOp],
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
    eval_ctx: &crate::runtime::eval::EvalContext<'_>,
) -> Result<Vec<Binding>, GqlError> {
    execute_pattern_ops_with_limit_seed_and_ctx(ops, graph, scope, None, None, Some(eval_ctx))
}

/// Execute pattern operations with an optional scan limit for LIMIT pushdown
/// and an optional seed binding for correlated subqueries.
pub(super) fn execute_pattern_ops_with_limit_and_seed(
    ops: &[PatternOp],
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
    scan_limit: Option<usize>,
    seed: Option<&Binding>,
) -> Result<Vec<Binding>, GqlError> {
    execute_pattern_ops_with_csr(ops, graph, scope, scan_limit, seed, None)
}

/// Execute pattern operations with an optional scan limit, seed, and EvalContext.
fn execute_pattern_ops_with_limit_seed_and_ctx(
    ops: &[PatternOp],
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
    scan_limit: Option<usize>,
    seed: Option<&Binding>,
    eval_ctx: Option<&crate::runtime::eval::EvalContext<'_>>,
) -> Result<Vec<Binding>, GqlError> {
    execute_pattern_ops_core(ops, graph, scope, scan_limit, seed, None, None, eval_ctx)
        .map(|chunk| chunk.to_bindings())
}

/// Execute pattern operations with a pre-built CSR adjacency.
pub(super) fn execute_pattern_ops_with_csr(
    ops: &[PatternOp],
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
    scan_limit: Option<usize>,
    seed: Option<&Binding>,
    csr: Option<&selene_graph::CsrAdjacency>,
) -> Result<Vec<Binding>, GqlError> {
    execute_pattern_ops_with_max(ops, graph, scope, scan_limit, seed, csr, None)
}

/// Execute pattern operations with a pre-built CSR and explicit EvalContext.
pub(super) fn execute_pattern_ops_with_csr_and_ctx(
    ops: &[PatternOp],
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
    scan_limit: Option<usize>,
    seed: Option<&Binding>,
    csr: Option<&selene_graph::CsrAdjacency>,
    eval_ctx: &crate::runtime::eval::EvalContext<'_>,
) -> Result<Vec<Binding>, GqlError> {
    execute_pattern_ops_core(
        ops,
        graph,
        scope,
        scan_limit,
        seed,
        csr,
        None,
        Some(eval_ctx),
    )
    .map(|chunk| chunk.to_bindings())
}

/// Execute pattern operations with early termination after `max_bindings` results.
/// Used by EXISTS (max=1) and COUNT threshold subqueries.
///
/// Internally threads a DataChunk through the operator pipeline, converting
/// to `Vec<Binding>` only at the return boundary.
pub(crate) fn execute_pattern_ops_with_max(
    ops: &[PatternOp],
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
    scan_limit: Option<usize>,
    seed: Option<&Binding>,
    csr: Option<&selene_graph::CsrAdjacency>,
    max_bindings: Option<usize>,
) -> Result<Vec<Binding>, GqlError> {
    execute_pattern_ops_core(ops, graph, scope, scan_limit, seed, csr, max_bindings, None)
        .map(|chunk| chunk.to_bindings())
}

/// Execute pattern operations with early termination and an explicit EvalContext.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_pattern_ops_with_max_and_ctx(
    ops: &[PatternOp],
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
    scan_limit: Option<usize>,
    seed: Option<&Binding>,
    csr: Option<&selene_graph::CsrAdjacency>,
    max_bindings: Option<usize>,
    eval_ctx: &crate::runtime::eval::EvalContext<'_>,
) -> Result<Vec<Binding>, GqlError> {
    execute_pattern_ops_core(
        ops,
        graph,
        scope,
        scan_limit,
        seed,
        csr,
        max_bindings,
        Some(eval_ctx),
    )
    .map(|chunk| chunk.to_bindings())
}

/// Execute pattern operations, returning the DataChunk directly.
///
/// This is the primary entry point for the vectorized pipeline: callers
/// keep the columnar representation instead of converting to row-based
/// bindings at the pattern/pipeline boundary.
#[allow(dead_code)]
pub(super) fn execute_pattern_ops_as_chunk(
    ops: &[PatternOp],
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
    scan_limit: Option<usize>,
    seed: Option<&Binding>,
    csr: Option<&selene_graph::CsrAdjacency>,
) -> Result<DataChunk, GqlError> {
    execute_pattern_ops_core(ops, graph, scope, scan_limit, seed, csr, None, None)
}

/// Execute pattern operations as a DataChunk with an explicit EvalContext.
pub(super) fn execute_pattern_ops_as_chunk_with_ctx(
    ops: &[PatternOp],
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
    scan_limit: Option<usize>,
    seed: Option<&Binding>,
    csr: Option<&selene_graph::CsrAdjacency>,
    eval_ctx: &crate::runtime::eval::EvalContext<'_>,
) -> Result<DataChunk, GqlError> {
    execute_pattern_ops_core(
        ops,
        graph,
        scope,
        scan_limit,
        seed,
        csr,
        None,
        Some(eval_ctx),
    )
}

/// Check if a pattern op sequence is eligible for factorized execution.
///
/// Factorized execution supports chains of LabelScan + Expand +
/// IntermediateFilter + DifferentEdgesFilter. Falls back to flat for
/// VarExpand, Join, Optional, CycleJoin (these require full row context).
fn is_factorizable(ops: &[PatternOp]) -> bool {
    !ops.is_empty()
        && ops.iter().all(|op| {
            matches!(
                op,
                PatternOp::LabelScan { .. }
                    | PatternOp::Expand { .. }
                    | PatternOp::IntermediateFilter { .. }
                    | PatternOp::DifferentEdgesFilter { .. }
            )
        })
}

/// Execute pattern operations as a factorized chunk.
///
/// Only activates for patterns containing LabelScan + Expand chains
/// (no VarExpand, Join, Optional, CycleJoin). Returns `None` if the
/// pattern is not factorizable, signaling the caller to use the flat path.
pub(super) fn execute_pattern_ops_as_factorized_chunk(
    ops: &[PatternOp],
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
    csr: Option<&selene_graph::CsrAdjacency>,
    caller_eval_ctx: Option<&crate::runtime::eval::EvalContext<'_>>,
) -> Option<Result<FactorizedChunk, GqlError>> {
    if !is_factorizable(ops) {
        return None;
    }

    let registry = crate::runtime::functions::FunctionRegistry::builtins();
    let default_ctx = crate::runtime::eval::EvalContext::new(graph, registry).with_scope(scope);
    let eval_ctx = caller_eval_ctx.unwrap_or(&default_ctx);
    Some(execute_factorized_core(ops, graph, scope, csr, eval_ctx))
}

/// Core factorized execution: threads a FactorizedChunk through
/// factorized-compatible pattern operators.
fn execute_factorized_core(
    ops: &[PatternOp],
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
    csr: Option<&selene_graph::CsrAdjacency>,
    eval_ctx: &crate::runtime::eval::EvalContext<'_>,
) -> Result<FactorizedChunk, GqlError> {
    let mut sip_ctx = PatternContext::new();
    let mut chunk: Option<FactorizedChunk> = None;

    for op in ops {
        match op {
            PatternOp::LabelScan { var, .. } => {
                // Reuse existing label scan (produces DataChunk), convert root level
                let flat = execute_single_pattern_op_chunk(
                    op,
                    chunk.as_ref().map_or_else(DataChunk::unit, |c| c.flatten()),
                    ops,
                    None,
                    &PatternExecCtx {
                        graph,
                        scope,
                        csr,
                        sip_ctx: &sip_ctx,
                        eval_ctx,
                    },
                )?;

                // Convert the flat scan result to a root FactorLevel
                let scan_col_slot = flat.schema().slot_of(var).ok_or_else(|| {
                    GqlError::internal(format!("LabelScan var '{var}' not in output"))
                })?;
                let root = FactorLevel::root(*var, flat.column(scan_col_slot).clone());
                chunk = Some(FactorizedChunk::from_root(root));

                sip_ctx.update_from_chunk(&flat, &[*var]);
            }

            PatternOp::Expand {
                source_var,
                edge_var,
                target_var,
                edge_labels,
                target_labels,
                direction,
                target_property_filters,
                edge_property_filters,
            } => {
                let input = chunk.as_ref().ok_or_else(|| {
                    GqlError::internal("Expand without prior LabelScan in factorized path")
                })?;

                let expand_ctx = ExpandContext {
                    graph,
                    scope,
                    csr,
                    source_var: *source_var,
                    edge_var: *edge_var,
                    target_var: *target_var,
                    edge_labels: edge_labels.as_ref(),
                    target_labels: target_labels.as_ref(),
                    direction: *direction,
                };
                let result = factorized_expand::execute_expand_factorized(
                    input,
                    &expand_ctx,
                    target_property_filters,
                    edge_property_filters,
                    &sip_ctx,
                )?;

                // Update SIP from flattened view of new level
                let bound_vars = vars_from_op(op);
                if !bound_vars.is_empty() {
                    let deep_chunk = result.deepest_as_chunk();
                    sip_ctx.update_from_chunk(&deep_chunk, &bound_vars);
                }

                chunk = Some(result);
            }

            PatternOp::IntermediateFilter { predicate } => {
                let fc = chunk.as_mut().ok_or_else(|| {
                    GqlError::internal(
                        "IntermediateFilter without prior pattern in factorized path",
                    )
                })?;

                // Flatten to DataChunk for eval_vec (Phase C will make this native)
                let flat = fc.flatten();
                let functions = crate::runtime::functions::FunctionRegistry::builtins();
                let eval_ctx = crate::runtime::eval::EvalContext::new(graph, functions);
                let gatherer = crate::runtime::vector::gather::GraphPropertyGatherer::new(graph);

                if let Ok(crate::types::chunk::Column::Bool(arr)) =
                    crate::runtime::vector::eval_vec(predicate, &flat, &gatherer, &eval_ctx)
                {
                    // Apply filter to the deepest level's selection vector
                    // The flat chunk's active rows correspond 1:1 with factorized deepest active rows
                    let deep = fc.deepest_mut();
                    let active_indices: Vec<usize> =
                        deep.selection.active_indices(deep.len).collect();
                    let mut new_active = Vec::with_capacity(active_indices.len());
                    for (pos, &phys_idx) in active_indices.iter().enumerate() {
                        use arrow::array::Array;
                        if !arr.is_null(pos) && arr.value(pos) {
                            new_active.push(phys_idx as u32);
                        }
                    }
                    deep.selection = crate::types::chunk::SelectionVector::from_indices(new_active);
                } else {
                    // Per-row fallback on flattened chunk
                    let deep = fc.deepest_mut();
                    let active_indices: Vec<usize> =
                        deep.selection.active_indices(deep.len).collect();
                    let functions = crate::runtime::functions::FunctionRegistry::builtins();
                    let eval_ctx = crate::runtime::eval::EvalContext::new(graph, functions);
                    let mut new_active = Vec::with_capacity(active_indices.len());
                    for (pos, &phys_idx) in active_indices.iter().enumerate() {
                        let row = flat.row_view(pos);
                        let pass =
                            crate::runtime::eval::eval_predicate_row(predicate, &row, &eval_ctx)
                                .is_ok_and(|t| t.is_true());
                        if pass {
                            new_active.push(phys_idx as u32);
                        }
                    }
                    deep.selection = crate::types::chunk::SelectionVector::from_indices(new_active);
                }
            }

            PatternOp::DifferentEdgesFilter { edge_vars } => {
                let fc = chunk.as_mut().ok_or_else(|| {
                    GqlError::internal(
                        "DifferentEdgesFilter without prior pattern in factorized path",
                    )
                })?;

                // Per-row check using FactorizedRowView
                let deep = fc.deepest();
                let active_indices: Vec<usize> = deep.selection.active_indices(deep.len).collect();
                let mut new_active = Vec::with_capacity(active_indices.len());

                for &phys_idx in &active_indices {
                    let view = crate::types::factor::FactorizedRowView::new(fc, phys_idx);
                    let mut seen = smallvec::SmallVec::<[selene_core::entity::EdgeId; 4]>::new();
                    let unique = edge_vars.iter().all(|var| match view.get_edge_id(var) {
                        Ok(eid) => {
                            if seen.contains(&eid) {
                                false
                            } else {
                                seen.push(eid);
                                true
                            }
                        }
                        Err(_) => true,
                    });
                    if unique {
                        new_active.push(phys_idx as u32);
                    }
                }

                let deep = fc.deepest_mut();
                deep.selection = crate::types::chunk::SelectionVector::from_indices(new_active);
            }

            // VarExpand, Join, Optional, CycleJoin should not reach here
            // (is_factorizable guards against it)
            _ => {
                return Err(GqlError::internal(format!(
                    "unexpected pattern op in factorized path: {op:?}"
                )));
            }
        }

        // Check row and memory limits on the factorized chunk without
        // materializing. active_len() returns the deepest level's active
        // count, which equals the flattened row count. The memory estimate
        // uses the total column count across all levels as a proxy for the
        // flattened width (conservative: matches check_chunk_limit's model).
        if let Some(ref fc) = chunk {
            let active = fc.active_len();
            let limit = super::max_bindings();
            if active > limit {
                return Err(GqlError::ResourcesExhausted {
                    message: format!("query produced {active} rows (max {limit})"),
                });
            }
            let total_cols: usize = fc.levels.iter().map(|l| l.columns.len()).sum();
            let estimated_bytes = active * total_cols * 8;
            if estimated_bytes > super::MAX_QUERY_BYTES {
                return Err(GqlError::ResourcesExhausted {
                    message: format!(
                        "query memory estimate {:.1} MB exceeds budget {:.1} MB ({active} rows, {total_cols} cols)",
                        estimated_bytes as f64 / (1024.0 * 1024.0),
                        super::MAX_QUERY_BYTES as f64 / (1024.0 * 1024.0),
                    ),
                });
            }
        }
    }

    chunk.ok_or_else(|| GqlError::internal("empty factorized pattern ops"))
}

/// Core pattern execution: threads a DataChunk through pattern operators
/// with optional early termination.
#[allow(clippy::too_many_arguments)]
fn execute_pattern_ops_core(
    ops: &[PatternOp],
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
    scan_limit: Option<usize>,
    seed: Option<&Binding>,
    csr: Option<&selene_graph::CsrAdjacency>,
    max_rows: Option<usize>,
    caller_eval_ctx: Option<&crate::runtime::eval::EvalContext<'_>>,
) -> Result<DataChunk, GqlError> {
    // Build a default EvalContext when the caller does not supply one.
    let registry = crate::runtime::functions::FunctionRegistry::builtins();
    let default_ctx = crate::runtime::eval::EvalContext::new(graph, registry).with_scope(scope);
    let eval_ctx = caller_eval_ctx.unwrap_or(&default_ctx);

    // Seed chunk: unit (one row, no columns) or from seed binding
    let mut chunk = match seed {
        Some(s) => seed_to_chunk(s),
        None => DataChunk::unit(),
    };

    let mut sip_ctx = PatternContext::new();

    // Build skip set for Join right-side ops
    let mut skip_indices = std::collections::HashSet::new();
    for op in ops {
        if let PatternOp::Join {
            right_start,
            right_end,
            ..
        } = op
        {
            for idx in *right_start..*right_end {
                skip_indices.insert(idx);
            }
        }
    }

    for (i, op) in ops.iter().enumerate() {
        if skip_indices.contains(&i) {
            continue;
        }
        let ctx = PatternExecCtx {
            graph,
            scope,
            csr,
            sip_ctx: &sip_ctx,
            eval_ctx,
        };
        chunk = execute_single_pattern_op_chunk(op, chunk, ops, scan_limit, &ctx)?;

        // Update SIP bitmaps from chunk columns
        let bound_vars = vars_from_op(op);
        if !bound_vars.is_empty() {
            sip_ctx.update_from_chunk(&chunk, &bound_vars);
        }

        // Early termination
        if let Some(max) = max_rows
            && chunk.active_len() >= max
        {
            let phys_len = chunk.len();
            chunk.selection_mut().truncate(max, phys_len);
            return Ok(chunk);
        }

        super::check_chunk_limit(&chunk)?;
    }

    Ok(chunk)
}

/// Convert a seed Binding to a single-row DataChunk preserving all variables.
fn seed_to_chunk(seed: &Binding) -> DataChunk {
    use crate::types::chunk::{ChunkSchema, ColumnBuilder, ColumnKind};

    if seed.is_empty() {
        return DataChunk::unit();
    }

    let mut schema = ChunkSchema::new();
    let mut builders: Vec<ColumnBuilder> = Vec::new();

    for (var, val) in seed.iter() {
        let (kind, mut builder) = match val {
            BoundValue::Node(_) => (ColumnKind::NodeId, ColumnBuilder::new_node_ids(1)),
            BoundValue::Edge(_) => (ColumnKind::EdgeId, ColumnBuilder::new_edge_ids(1)),
            BoundValue::Scalar(gv) => match gv {
                GqlValue::Int(_) => (ColumnKind::Int64, ColumnBuilder::new_int64(1)),
                GqlValue::UInt(_) => (ColumnKind::UInt64, ColumnBuilder::new_uint64(1)),
                GqlValue::Float(_) => (ColumnKind::Float64, ColumnBuilder::new_float64(1)),
                GqlValue::Bool(_) => (ColumnKind::Bool, ColumnBuilder::new_bool(1)),
                GqlValue::String(_) => (ColumnKind::Utf8, ColumnBuilder::new_utf8()),
                _ => (ColumnKind::Values, ColumnBuilder::new_values(1)),
            },
            BoundValue::Path(_) | BoundValue::Group(_) => {
                (ColumnKind::Values, ColumnBuilder::new_values(1))
            }
        };
        builder.append_bound_value(val);
        schema.extend(*var, kind);
        builders.push(builder);
    }

    DataChunk::from_builders(builders, schema, 1)
}

/// Extract variables bound by a single PatternOp.
fn vars_from_op(op: &PatternOp) -> Vec<IStr> {
    match op {
        PatternOp::LabelScan { var, .. } => vec![*var],
        PatternOp::Expand {
            target_var,
            edge_var,
            ..
        } => {
            let mut v = vec![*target_var];
            if let Some(ev) = edge_var {
                v.push(*ev);
            }
            v
        }
        PatternOp::VarExpand {
            target_var,
            edge_var,
            path_var,
            ..
        } => {
            let mut v = vec![*target_var];
            if let Some(ev) = edge_var {
                v.push(*ev);
            }
            if let Some(pv) = path_var {
                v.push(*pv);
            }
            v
        }
        PatternOp::WcoJoin {
            scan_var,
            relations,
            ..
        } => {
            let mut v = vec![*scan_var];
            for rel in relations {
                if let Some(ev) = rel.edge_var {
                    v.push(ev);
                }
                v.push(rel.target_var);
            }
            v
        }
        _ => vec![],
    }
}

/// Public entry point for testing: dispatch a single pattern op on a DataChunk.
#[cfg(test)]
pub(crate) fn execute_single_pattern_op_chunk_public(
    op: &PatternOp,
    current: DataChunk,
    all_ops: &[PatternOp],
    scan_limit: Option<usize>,
    ctx: &PatternExecCtx<'_>,
) -> Result<DataChunk, GqlError> {
    execute_single_pattern_op_chunk(op, current, all_ops, scan_limit, ctx)
}

/// Dispatch a single pattern op on a DataChunk, producing a new DataChunk.
///
/// LabelScan and Expand use native chunk variants. VarExpand and Join use
/// adapter variants (Binding bridge). Optional and CycleJoin bridge through
/// bindings. IntermediateFilter and DifferentEdgesFilter operate directly
/// on the selection vector.
fn execute_single_pattern_op_chunk(
    op: &PatternOp,
    current: DataChunk,
    all_ops: &[PatternOp],
    scan_limit: Option<usize>,
    ctx: &PatternExecCtx<'_>,
) -> Result<DataChunk, GqlError> {
    let graph = ctx.graph;
    let scope = ctx.scope;
    let csr = ctx.csr;
    let sip_ctx = ctx.sip_ctx;

    match op {
        PatternOp::LabelScan {
            var,
            labels,
            inline_props,
            property_filters,
            index_order,
            composite_index_keys,
            range_index_hint,
            in_list_hint,
        } => {
            // Correlated: if scan var already in chunk, return as-is
            if current.schema().slot_of(var).is_some() {
                return Ok(current);
            }

            // Try index-ordered scan
            if let Some(order) = index_order
                && let Some(label_expr) = labels
                && let Some(label_istr) = scan::single_label(label_expr)
                && let Some(chunk) = scan::execute_index_ordered_scan_chunk(
                    *var,
                    label_istr,
                    order.key,
                    order.descending,
                    order.limit,
                    &ScanContext {
                        graph,
                        scope,
                        property_filters,
                        eval_ctx: ctx.eval_ctx,
                    },
                )
            {
                return Ok(chunk);
            }

            // Try composite index scan
            if let Some(hint_keys) = composite_index_keys
                && let Some(label_expr) = labels
                && let Some(label_istr) = scan::single_label(label_expr)
                && let Some(chunk) = scan::execute_composite_index_scan_chunk(
                    *var,
                    label_istr,
                    hint_keys,
                    inline_props,
                    scan_limit,
                    &ScanContext {
                        graph,
                        scope,
                        property_filters,
                        eval_ctx: ctx.eval_ctx,
                    },
                )
            {
                return Ok(chunk);
            }

            // Range and IN-list hint scope narrowing
            let mut hint_scope: Option<roaring::RoaringBitmap> = None;

            if let Some(hint) = range_index_hint
                && let Some(label_expr) = labels
                && let Some(label_istr) = scan::single_label(label_expr)
                && let Some(index) = graph.property_index_entries(label_istr, hint.key)
            {
                if check_range_satisfiable(index, hint) {
                    let lo = hint.lower.as_ref().and_then(|(v, inc)| {
                        selene_core::Value::try_from(v).ok().map(|cv| (cv, *inc))
                    });
                    let hi = hint.upper.as_ref().and_then(|(v, inc)| {
                        selene_core::Value::try_from(v).ok().map(|cv| (cv, *inc))
                    });
                    let lo_ref = lo.as_ref().map(|(v, inc)| (v, *inc));
                    let hi_ref = hi.as_ref().map(|(v, inc)| (v, *inc));
                    let range_bitmap = index.range_to_bitmap(lo_ref, hi_ref);
                    hint_scope = Some(range_bitmap);
                } else {
                    hint_scope = Some(roaring::RoaringBitmap::new());
                }
            }

            if let Some(hint) = in_list_hint
                && let Some(label_expr) = labels
                && let Some(label_istr) = scan::single_label(label_expr)
                && graph.has_property_index(label_istr, hint.key)
            {
                let mut list_bitmap = roaring::RoaringBitmap::new();
                for val in &hint.values {
                    if let Ok(core_val) = selene_core::Value::try_from(val)
                        && let Some(node_ids) =
                            graph.property_index_lookup(label_istr, hint.key, &core_val)
                    {
                        for nid in node_ids {
                            list_bitmap.insert(nid.0 as u32);
                        }
                    }
                }
                hint_scope = Some(match hint_scope {
                    Some(existing) => existing & list_bitmap,
                    None => list_bitmap,
                });
            }

            let merged_scope = match (scope, &hint_scope) {
                (Some(auth), Some(hint)) => Some(auth & hint),
                (None, Some(hint)) => Some(hint.clone()),
                (Some(auth), None) => Some(auth.clone()),
                (None, None) => None,
            };
            let effective_scope = merged_scope.as_ref().or(scope);

            scan::execute_label_scan_chunk(
                *var,
                labels.as_ref(),
                inline_props,
                scan_limit,
                &ScanContext {
                    graph,
                    scope: effective_scope,
                    property_filters,
                    eval_ctx: ctx.eval_ctx,
                },
            )
        }

        PatternOp::Expand {
            source_var,
            edge_var,
            target_var,
            edge_labels,
            target_labels,
            direction,
            target_property_filters,
            edge_property_filters,
        } => {
            let expand_ctx = ExpandContext {
                graph,
                scope,
                csr,
                source_var: *source_var,
                edge_var: *edge_var,
                target_var: *target_var,
                edge_labels: edge_labels.as_ref(),
                target_labels: target_labels.as_ref(),
                direction: *direction,
            };
            expand::execute_expand_chunk(
                &current,
                &expand_ctx,
                target_property_filters,
                edge_property_filters,
                sip_ctx,
            )
        }

        PatternOp::VarExpand {
            source_var,
            edge_var,
            target_var,
            edge_labels,
            target_labels,
            direction,
            min_hops,
            max_hops,
            trail,
            acyclic,
            simple,
            shortest,
            path_var,
        } => {
            let expand_ctx = ExpandContext {
                graph,
                scope,
                csr,
                source_var: *source_var,
                edge_var: *edge_var,
                target_var: *target_var,
                edge_labels: edge_labels.as_ref(),
                target_labels: target_labels.as_ref(),
                direction: *direction,
            };
            let var_cfg = VarExpandConfig {
                min_hops: *min_hops,
                max_hops: *max_hops,
                trail: *trail,
                acyclic: *acyclic,
                simple: *simple,
                shortest: *shortest,
                path_var: *path_var,
            };
            varlength::execute_var_expand_chunk(&current, &expand_ctx, &var_cfg)
        }

        PatternOp::Join {
            right_start,
            right_end,
            join_vars,
        } => {
            let right_ops = &all_ops[*right_start..*right_end];
            let right_bindings = execute_pattern_ops_with_limit_seed_and_ctx(
                right_ops,
                graph,
                scope,
                None,
                None,
                Some(ctx.eval_ctx),
            )?;
            // Bridge: convert right to chunk, then join
            let right_chunk = join::bindings_to_chunk_generic(&right_bindings);
            join::execute_join_chunk(&current, &right_chunk, join_vars)
        }

        PatternOp::Optional {
            inner_ops,
            new_vars,
            join_vars,
        } => {
            // Bridge through bindings for Optional (complex left-outer-join).
            let current_bindings = current.to_bindings();
            let inner_result = execute_pattern_ops_with_limit_seed_and_ctx(
                inner_ops,
                graph,
                scope,
                None,
                None,
                Some(ctx.eval_ctx),
            )?;
            let result =
                execute_optional_bindings(&current_bindings, &inner_result, new_vars, join_vars);
            Ok(join::bindings_to_chunk_generic(&result))
        }

        PatternOp::CycleJoin {
            bound_var,
            source_var,
            edge_labels,
            direction,
        } => {
            // Bridge through bindings for CycleJoin
            let bindings = current.to_bindings();
            let result = execute_cycle_join(
                bindings,
                *bound_var,
                *source_var,
                edge_labels.as_ref(),
                *direction,
                graph,
            )?;
            Ok(join::bindings_to_chunk_generic(&result))
        }

        PatternOp::DifferentEdgesFilter { edge_vars } => {
            // Native chunk filter: check edge uniqueness per active row
            let mut chunk = current;
            let mut mask = Vec::with_capacity(chunk.active_len());

            for row_idx in chunk.active_indices() {
                let row = chunk.row_view(row_idx);
                let mut seen = smallvec::SmallVec::<[selene_core::entity::EdgeId; 4]>::new();
                let unique = edge_vars.iter().all(|var| match row.get_edge_id(var) {
                    Ok(eid) => {
                        if seen.contains(&eid) {
                            false
                        } else {
                            seen.push(eid);
                            true
                        }
                    }
                    Err(_) => true,
                });
                mask.push(unique);
            }

            let phys_len = chunk.len();
            chunk.selection_mut().apply_bool_mask(&mask, phys_len);
            Ok(chunk)
        }

        PatternOp::WcoJoin {
            scan_var,
            scan_labels,
            scan_property_filters,
            relations,
        } => crate::pattern::wco::execute_wco_join(
            *scan_var,
            scan_labels.as_ref(),
            scan_property_filters,
            relations,
            scope,
            graph,
            csr,
            scan_limit,
        ),

        PatternOp::IntermediateFilter { predicate } => {
            // Batch filter via eval_vec, falling back to per-row on Unsupported
            let mut chunk = current;
            let functions = crate::runtime::functions::FunctionRegistry::builtins();
            let eval_ctx = crate::runtime::eval::EvalContext::new(graph, functions);

            let gatherer = crate::runtime::vector::gather::GraphPropertyGatherer::new(graph);

            if let Ok(crate::types::chunk::Column::Bool(arr)) =
                crate::runtime::vector::eval_vec(predicate, &chunk, &gatherer, &eval_ctx)
            {
                let phys_len = chunk.len();
                chunk.selection_mut().apply_bool_column(&arr, phys_len);
            } else {
                // Fallback: per-row evaluation
                let mut mask = Vec::with_capacity(chunk.active_len());
                for row_idx in chunk.active_indices() {
                    let row = chunk.row_view(row_idx);
                    let pass = crate::runtime::eval::eval_predicate_row(predicate, &row, &eval_ctx)
                        .is_ok_and(|t| t.is_true());
                    mask.push(pass);
                }
                let phys_len = chunk.len();
                chunk.selection_mut().apply_bool_mask(&mask, phys_len);
            }

            Ok(chunk)
        }
    }
}

/// Execute Optional left-outer-join on binding vectors.
/// Extracted to keep execute_single_pattern_op_chunk readable.
fn execute_optional_bindings(
    current: &[Binding],
    inner_result: &[Binding],
    new_vars: &[IStr],
    join_vars: &[IStr],
) -> Vec<Binding> {
    let mut output = Vec::new();
    for binding in current {
        let matching: Vec<&Binding> = if join_vars.is_empty() {
            inner_result.iter().collect()
        } else {
            inner_result
                .iter()
                .filter(|inner| {
                    join_vars
                        .iter()
                        .all(|jv| match (binding.get(jv), inner.get(jv)) {
                            (Some(BoundValue::Node(a)), Some(BoundValue::Node(b))) => a == b,
                            (Some(BoundValue::Edge(a)), Some(BoundValue::Edge(b))) => a == b,
                            (Some(BoundValue::Scalar(a)), Some(BoundValue::Scalar(b))) => {
                                a.distinctness_key() == b.distinctness_key()
                            }
                            _ => false,
                        })
                })
                .collect()
        };

        if matching.is_empty() {
            let mut null_binding = binding.clone();
            for var in new_vars {
                if !binding.contains(var) {
                    null_binding.bind(*var, BoundValue::Scalar(GqlValue::Null));
                }
            }
            output.push(null_binding);
        } else {
            for inner_binding in &matching {
                let mut merged = binding.clone();
                for var in new_vars {
                    if !join_vars.contains(var)
                        && let Some(val) = inner_binding.get(var)
                    {
                        merged.bind(*var, val.clone());
                    }
                }
                output.push(merged);
            }
        }
    }
    output
}

/// Execute a cycle closure: filter bindings where source_var has an edge
/// (matching edge_labels) pointing to bound_var's value.
pub(super) fn execute_cycle_join(
    bindings: Vec<Binding>,
    bound_var: IStr,
    source_var: IStr,
    edge_labels: Option<&crate::ast::pattern::LabelExpr>,
    direction: crate::ast::pattern::EdgeDirection,
    graph: &SeleneGraph,
) -> Result<Vec<Binding>, GqlError> {
    use crate::ast::pattern::EdgeDirection;

    let mut result = Vec::new();

    for binding in bindings {
        let source_id = binding.get_node_id(&source_var)?;
        let target_id = binding.get_node_id(&bound_var)?;

        // Check if there's an edge from source to target (or target to source for In)
        let edge_ids: &[selene_core::EdgeId] = match direction {
            EdgeDirection::Out => graph.outgoing(source_id),
            EdgeDirection::In => graph.incoming(source_id),
            EdgeDirection::Any => {
                // Need to check both directions
                let out = graph.outgoing(source_id);
                let has_match_out = out.iter().any(|&eid| {
                    if let Some(edge) = graph.get_edge(eid) {
                        let target_matches = edge.target == target_id;
                        let label_matches = edge_labels.is_none_or(|labels| {
                            crate::pattern::scan::label_matches(edge.label, labels, graph)
                        });
                        target_matches && label_matches
                    } else {
                        false
                    }
                });
                let has_match_in = graph.incoming(source_id).iter().any(|&eid| {
                    if let Some(edge) = graph.get_edge(eid) {
                        let target_matches = edge.source == target_id;
                        let label_matches = edge_labels.is_none_or(|labels| {
                            crate::pattern::scan::label_matches(edge.label, labels, graph)
                        });
                        target_matches && label_matches
                    } else {
                        false
                    }
                });
                if has_match_out || has_match_in {
                    result.push(binding);
                }
                continue;
            }
        };

        let has_edge = edge_ids.iter().any(|&eid| {
            if let Some(edge) = graph.get_edge(eid) {
                let endpoint = match direction {
                    EdgeDirection::Out => edge.target,
                    EdgeDirection::In => edge.source,
                    EdgeDirection::Any => unreachable!(),
                };
                let target_matches = endpoint == target_id;
                let label_matches = edge_labels.is_none_or(|labels| {
                    crate::pattern::scan::label_matches(edge.label, labels, graph)
                });
                target_matches && label_matches
            } else {
                false
            }
        });

        if has_edge {
            result.push(binding);
        }
    }

    Ok(result)
}

/// Check whether a [`RangeIndexHint`] can possibly be satisfied given
/// the index's tracked min/max value range (zone-map check).
///
/// Returns `false` when either bound is provably outside the index's
/// value range, meaning `range_to_bitmap` would return an empty bitmap.
/// Returns `true` when the check is inconclusive (proceed with the scan).
fn check_range_satisfiable(
    index: &selene_graph::typed_index::TypedIndex,
    hint: &crate::planner::plan::RangeIndexHint,
) -> bool {
    use selene_graph::typed_index::RangeOp;

    if let Some((ref val, inclusive)) = hint.lower
        && let Ok(core_val) = selene_core::Value::try_from(val)
    {
        let op = if inclusive { RangeOp::Gte } else { RangeOp::Gt };
        if !index.can_satisfy(op, &core_val) {
            return false;
        }
    }

    if let Some((ref val, inclusive)) = hint.upper
        && let Ok(core_val) = selene_core::Value::try_from(val)
    {
        let op = if inclusive { RangeOp::Lte } else { RangeOp::Lt };
        if !index.can_satisfy(op, &core_val) {
            return false;
        }
    }

    true
}
