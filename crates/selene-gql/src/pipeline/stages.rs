//! Pipeline stage execution: LET, FILTER, ORDER BY, OFFSET, LIMIT, RETURN.
//!
//! Each stage takes `Vec<Binding>` and produces `Vec<Binding>`.
//! Streaming stages (LET, FILTER, OFFSET, LIMIT) process one binding at a time.
//! Pipeline breakers (Sort, Return with GROUP BY) must materialize all input.

use selene_core::IStr;
use smallvec::SmallVec;

use crate::ast::expr::{Expr, FunctionCall};
use crate::ast::statement::OrderTerm;
use crate::pattern::join::bindings_to_chunk_generic;
use crate::planner::plan::{PipelineOp, PlannedProjection};
use crate::runtime::eval::{self, EvalContext, eval_aggregate, eval_expr_ctx, eval_predicate};
use crate::types::binding::{Binding, BoundValue};
use crate::types::chunk::DataChunk;
use crate::types::error::GqlError;
use crate::types::value::GqlValue;

/// Compare two values with NULL handling per GQL spec (NULLS FIRST/LAST).
///
/// Default: NULLS LAST for ASC, NULLS FIRST for DESC.
/// Used by `execute_sort`, `execute_topk`, and `execute_return_topk`.
fn compare_with_nulls(a: &GqlValue, b: &GqlValue, term: &OrderTerm) -> std::cmp::Ordering {
    let a_null = a.is_null();
    let b_null = b.is_null();

    if a_null || b_null {
        if a_null && b_null {
            return std::cmp::Ordering::Equal;
        }
        let nulls_first = term.nulls_first.unwrap_or(term.descending);
        return if a_null {
            if nulls_first {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        } else if nulls_first {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Less
        };
    }

    let ord = a.sort_order(b);
    if term.descending { ord.reverse() } else { ord }
}

/// Execute a single pipeline operation on a set of bindings.
pub(crate) fn execute_pipeline_op(
    op: &PipelineOp,
    bindings: Vec<Binding>,
    ctx: &EvalContext<'_>,
) -> Result<Vec<Binding>, GqlError> {
    match op {
        PipelineOp::Let { bindings: lets } => execute_let(bindings, lets, ctx),
        PipelineOp::Filter { predicate } => execute_filter(bindings, predicate, ctx),
        PipelineOp::Sort { terms } => Ok(execute_sort(bindings, terms, ctx)),
        PipelineOp::TopK { terms, limit } => Ok(execute_topk(bindings, terms, *limit, ctx)),
        PipelineOp::Offset { count } => Ok(execute_offset(bindings, *count)),
        PipelineOp::Limit { count } => Ok(execute_limit(bindings, *count)),
        PipelineOp::Return {
            projections,
            group_by,
            distinct,
            having,
            all,
        } => {
            if *all {
                execute_return_all(bindings, *distinct, ctx)
            } else {
                execute_return(
                    bindings,
                    projections,
                    group_by,
                    *distinct,
                    having.as_ref(),
                    ctx,
                )
            }
        }
        PipelineOp::With {
            projections,
            group_by,
            distinct,
            having,
            where_filter,
        } => {
            // WITH has the same projection/aggregation semantics as RETURN
            let mut result = execute_return(
                bindings,
                projections,
                group_by,
                *distinct,
                having.as_ref(),
                ctx,
            )?;
            // Apply optional WHERE filter after projection (scope-reset filter)
            if let Some(pred) = where_filter {
                result = execute_filter(result, pred, ctx)?;
            }
            Ok(result)
        }
        PipelineOp::Call { .. } => {
            // CALL is handled separately by the executor (needs procedure registry)
            Err(GqlError::internal("CALL must be handled by the executor"))
        }
        PipelineOp::Subquery { .. } => {
            // Subquery execution is handled by the main executor (needs graph access)
            Err(GqlError::internal(
                "CALL { subquery } must be handled by the executor",
            ))
        }
        PipelineOp::For { var, list_expr } => {
            let mut output = Vec::new();
            for binding in &bindings {
                let list_val = eval_expr_ctx(list_expr, binding, ctx)?;
                match list_val {
                    GqlValue::List(list) => {
                        for elem in list.elements.iter() {
                            let mut new_binding = binding.clone();
                            new_binding.bind(*var, BoundValue::Scalar(elem.clone()));
                            output.push(new_binding);
                        }
                    }
                    GqlValue::Null => {
                        // NULL list → 0 rows (dropped)
                    }
                    _ => {
                        return Err(GqlError::type_error(format!(
                            "FOR requires a list, got {:?}",
                            list_val.gql_type()
                        )));
                    }
                }
            }
            Ok(output)
        }
        PipelineOp::ViewScan {
            view_name, yields, ..
        } => {
            let provider = crate::runtime::procedures::view_provider::get_view_provider()?;
            let row = provider.read_view(view_name.as_str())?;
            let mut binding = Binding::empty();
            for (col, alias) in yields {
                let key = alias.unwrap_or(*col);
                let val = row
                    .iter()
                    .find(|(k, _)| *k == *col)
                    .map_or(GqlValue::Null, |(_, v)| v.clone());
                binding.bind(key, BoundValue::Scalar(val));
            }
            Ok(vec![binding])
        }
    }
}

/// Execute a single pipeline operation on a DataChunk.
///
/// Native chunk implementations for Filter, LET, Sort, Offset, Limit.
/// Complex stages (GroupBy, Aggregate, TopK, With, For, DISTINCT) bridge
/// through bindings. eval_vec handles common expression patterns; unsupported
/// expressions fall back to per-row RowView evaluation.
pub(crate) fn execute_pipeline_op_chunk(
    op: &PipelineOp,
    chunk: DataChunk,
    ctx: &EvalContext<'_>,
) -> Result<DataChunk, GqlError> {
    match op {
        PipelineOp::Filter { predicate } => Ok(execute_filter_chunk(chunk, predicate, ctx)),

        PipelineOp::Let { bindings: lets } => execute_let_chunk(chunk, lets, ctx),

        PipelineOp::Sort { terms } => Ok(execute_sort_chunk(chunk, terms, ctx)),

        PipelineOp::Offset { count } => {
            let mut chunk = chunk;
            let phys_len = chunk.len();
            chunk.selection_mut().skip(*count as usize, phys_len);
            Ok(chunk)
        }

        PipelineOp::Limit { count } => {
            let mut chunk = chunk;
            let phys_len = chunk.len();
            chunk.selection_mut().truncate(*count as usize, phys_len);
            Ok(chunk)
        }

        // Stages that bridge through bindings.
        _ => {
            let bindings = chunk.to_bindings();
            let result = execute_pipeline_op(op, bindings, ctx)?;
            Ok(bindings_to_chunk_generic(&result))
        }
    }
}

/// Native chunk filter using batch eval_vec.
///
/// Tries eval_vec first (single column-level dispatch). On Unsupported,
/// falls back to per-row RowView evaluation. Both paths produce a bool
/// mask applied to the selection vector with zero data movement.
fn execute_filter_chunk(
    mut chunk: DataChunk,
    predicate: &Expr,
    ctx: &EvalContext<'_>,
) -> DataChunk {
    use crate::runtime::vector::eval_vec;
    use crate::runtime::vector::gather::GraphPropertyGatherer;
    use crate::types::chunk::Column;

    let gatherer = GraphPropertyGatherer::new(ctx.graph);

    // Try batch evaluation
    match eval_vec(predicate, &chunk, &gatherer, ctx) {
        Ok(Column::Bool(arr)) => {
            let phys_len = chunk.len();
            chunk.selection_mut().apply_bool_column(&arr, phys_len);
            return chunk;
        }
        Ok(_other) => {
            // Non-bool result from predicate: treat non-true as false
            // (matches GQL FILTER semantics: only TRUE passes)
        }
        Err(GqlError::Unsupported { .. }) => {
            // Fall through to per-row path
        }
        Err(_) => {
            // Other eval errors: fall through to per-row for consistent error handling
        }
    }

    // Fallback: per-row evaluation via RowView
    let mut mask = Vec::with_capacity(chunk.active_len());
    for row_idx in chunk.active_indices() {
        let row = chunk.row_view(row_idx);
        let pass = eval::eval_predicate_row(predicate, &row, ctx).is_ok_and(|t| t.is_true());
        mask.push(pass);
    }
    let phys_len = chunk.len();
    chunk.selection_mut().apply_bool_mask(&mask, phys_len);
    chunk
}

/// Native chunk LET using batch eval_vec.
///
/// For each LET binding, tries eval_vec to produce a column directly.
/// Falls back to per-row evaluation on Unsupported.
fn execute_let_chunk(
    mut chunk: DataChunk,
    lets: &[(IStr, Expr)],
    ctx: &EvalContext<'_>,
) -> Result<DataChunk, GqlError> {
    use crate::runtime::vector::eval_vec;
    use crate::runtime::vector::gather::GraphPropertyGatherer;
    use crate::types::chunk::ColumnBuilder;

    let gatherer = GraphPropertyGatherer::new(ctx.graph);

    for (var, expr) in lets {
        // Try batch evaluation
        match eval_vec(expr, &chunk, &gatherer, ctx) {
            Ok(col) => {
                chunk.append_column(*var, col);
            }
            Err(GqlError::Unsupported { .. }) => {
                // Fallback: per-row evaluation
                let mut builder = ColumnBuilder::new_values(chunk.len());
                for row_idx in 0..chunk.len() {
                    let row = chunk.row_view(row_idx);
                    let val = eval::eval_expr_row(expr, &row, ctx).unwrap_or(GqlValue::Null);
                    builder.append_gql_value(&val);
                }
                chunk.append_column(*var, builder.finish());
            }
            Err(e) => return Err(e),
        }
    }
    Ok(chunk)
}

/// Native chunk sort using eval_vec for sort key pre-evaluation.
///
/// Evaluates sort keys as columns, compacts the chunk, sorts an index
/// array by key values, then applies the permutation to all columns.
fn execute_sort_chunk(chunk: DataChunk, terms: &[OrderTerm], ctx: &EvalContext<'_>) -> DataChunk {
    use crate::runtime::vector::eval_vec;
    use crate::runtime::vector::gather::GraphPropertyGatherer;
    use crate::types::chunk::column_to_gql_value_pub;

    if chunk.active_len() <= 1 {
        return chunk;
    }

    // Compact to dense so physical indices match logical row order
    let chunk = chunk.compact();
    let len = chunk.len();
    let gatherer = GraphPropertyGatherer::new(ctx.graph);

    // Pre-evaluate sort keys as columns (or per-row fallback)
    let key_columns: Vec<crate::types::chunk::Column> = terms
        .iter()
        .map(|t| {
            eval_vec(&t.expr, &chunk, &gatherer, ctx).unwrap_or_else(|_| {
                // Fallback: evaluate per-row into a Values column
                let mut vals = Vec::with_capacity(len);
                for row_idx in 0..len {
                    let row = chunk.row_view(row_idx);
                    vals.push(eval::eval_expr_row(&t.expr, &row, ctx).unwrap_or(GqlValue::Null));
                }
                crate::types::chunk::Column::Values(std::sync::Arc::from(vals))
            })
        })
        .collect();

    // Sort index array by comparing key column values
    let mut indices: Vec<u32> = (0..len as u32).collect();
    indices.sort_by(|&ai, &bi| {
        for (ti, term) in terms.iter().enumerate() {
            let a_val = column_to_gql_value_pub(&key_columns[ti], ai as usize);
            let b_val = column_to_gql_value_pub(&key_columns[ti], bi as usize);
            let ord = compare_with_nulls(&a_val, &b_val, term);
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });

    // Apply permutation: gather all columns at sorted indices
    chunk.gather_rows(&indices)
}

// ── LET ────────────────────────────────────────────────────────────

fn execute_let(
    bindings: Vec<Binding>,
    lets: &[(IStr, Expr)],
    ctx: &EvalContext<'_>,
) -> Result<Vec<Binding>, GqlError> {
    bindings
        .into_iter()
        .map(|mut b| {
            for (var, expr) in lets {
                let val = eval_expr_ctx(expr, &b, ctx)?;
                b.bind(*var, BoundValue::Scalar(val));
            }
            Ok(b)
        })
        .collect()
}

// ── FILTER ─────────────────────────────────────────────────────────

fn execute_filter(
    bindings: Vec<Binding>,
    predicate: &Expr,
    ctx: &EvalContext<'_>,
) -> Result<Vec<Binding>, GqlError> {
    let mut result = Vec::with_capacity(bindings.len());
    for b in bindings {
        let tri = eval_predicate(predicate, &b, ctx)?;
        if tri.is_true() {
            result.push(b);
        }
        // FALSE and UNKNOWN are filtered out
    }
    Ok(result)
}

// ── ORDER BY ───────────────────────────────────────────────────────

fn execute_sort(
    bindings: Vec<Binding>,
    terms: &[OrderTerm],
    ctx: &EvalContext<'_>,
) -> Vec<Binding> {
    if bindings.len() <= 1 {
        return bindings;
    }

    // Decorate-sort-undecorate: evaluate sort keys once per binding (O(N)),
    // then sort indices (16 bytes per swap vs ~344 bytes for Binding).
    // Previously called eval_expr_ctx per comparison = O(N log N) evals.

    // 1. Pre-evaluate sort keys for each binding
    let keys: Vec<Vec<GqlValue>> = bindings
        .iter()
        .map(|b| {
            terms
                .iter()
                .map(|t| eval_expr_ctx(&t.expr, b, ctx).unwrap_or(GqlValue::Null))
                .collect()
        })
        .collect();

    // 2. Sort indices instead of moving Binding objects
    let mut indices: Vec<usize> = (0..bindings.len()).collect();
    indices.sort_by(|&ai, &bi| {
        for (ti, term) in terms.iter().enumerate() {
            let ord = compare_with_nulls(&keys[ai][ti], &keys[bi][ti], term);
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });

    // 3. Apply permutation -- take each binding out by replacing with a
    //    zero-allocation empty. Binding::empty() is a trivial SmallVec::new()
    //    so this avoids the clone overhead of the previous approach.
    let mut sorted = Vec::with_capacity(bindings.len());
    let mut bindings = bindings;
    for &idx in &indices {
        sorted.push(std::mem::replace(&mut bindings[idx], Binding::empty()));
    }

    sorted
}

// ── TopK (ORDER BY + LIMIT fused) ────────────────────────────────

fn execute_topk(
    bindings: Vec<Binding>,
    terms: &[OrderTerm],
    limit: u64,
    ctx: &EvalContext<'_>,
) -> Vec<Binding> {
    use std::cmp::Ordering;
    use std::collections::BinaryHeap;

    let k = limit as usize;
    if bindings.is_empty() || k == 0 {
        return vec![];
    }

    // Compare pre-evaluated sort keys in desired output order (ASC/DESC per term).
    fn compare_keys(a: &[GqlValue], b: &[GqlValue], terms: &[OrderTerm]) -> Ordering {
        for (i, term) in terms.iter().enumerate() {
            let ord = compare_with_nulls(&a[i], &b[i], term);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    }

    // HeapEntry with pre-evaluated sort keys. Avoids re-evaluating
    // expressions on every comparison (N * log(K) * 2 -> N evaluations).
    struct HeapEntry<'a> {
        binding: Binding,
        keys: Vec<GqlValue>,
        terms: &'a [OrderTerm],
    }

    impl Eq for HeapEntry<'_> {}
    impl PartialEq for HeapEntry<'_> {
        fn eq(&self, other: &Self) -> bool {
            compare_keys(&self.keys, &other.keys, self.terms) == Ordering::Equal
        }
    }
    impl PartialOrd for HeapEntry<'_> {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }
    impl Ord for HeapEntry<'_> {
        fn cmp(&self, other: &Self) -> Ordering {
            // Eviction heap: keep K entries at the HEAD of desired sort order.
            // BinaryHeap is a max-heap, so pop removes the "greatest" entry.
            // Using desired output order directly makes entries ranked LAST in
            // the output the heap maximum -- exactly what we want to evict.
            compare_keys(&self.keys, &other.keys, self.terms)
        }
    }

    let mut heap: BinaryHeap<HeapEntry<'_>> = BinaryHeap::with_capacity(k + 1);

    for binding in bindings {
        // Pre-evaluate sort keys ONCE per binding
        let keys: Vec<GqlValue> = terms
            .iter()
            .map(|t| eval_expr_ctx(&t.expr, &binding, ctx).unwrap_or(GqlValue::Null))
            .collect();
        // Threshold check: skip if worse than current worst keeper
        if heap.len() >= k
            && let Some(worst) = heap.peek()
            && compare_keys(&keys, &worst.keys, worst.terms) != Ordering::Less
        {
            continue;
        }
        heap.push(HeapEntry {
            binding,
            keys,
            terms,
        });
        if heap.len() > k {
            heap.pop();
        }
    }

    // Extract and sort the K results using cached keys
    let mut result: Vec<(Binding, Vec<GqlValue>)> =
        heap.into_iter().map(|e| (e.binding, e.keys)).collect();
    result.sort_by(|a, b| compare_keys(&a.1, &b.1, terms));
    result.into_iter().map(|(b, _)| b).collect()
}

// ── Fused Return+TopK (lazy projection) ──────────────────────────

/// Fused Return + TopK: evaluate sort keys first, full projection only for K winners.
///
/// For `RETURN a, b ORDER BY c LIMIT K` on N input bindings:
/// - Standard path: project a,b,c for all N → heap keeps K = O(N * projections)
/// - Fused path: eval c for all N, heap keeps K, then project a,b for K winners = O(N * 1 + K * projections)
///
/// This avoids evaluating expensive projections (property lookups) for rows that
/// will be discarded by the TopK heap.
pub(crate) fn execute_return_topk(
    bindings: Vec<Binding>,
    projections: &[PlannedProjection],
    terms: &[OrderTerm],
    limit: u64,
    ctx: &EvalContext<'_>,
) -> Result<Vec<Binding>, GqlError> {
    use std::cmp::Ordering;
    use std::collections::BinaryHeap;

    let k = limit as usize;
    if bindings.is_empty() || k == 0 {
        return Ok(vec![]);
    }

    // Identify which projections are needed for sort keys vs which are "extra"
    // Sort key expressions reference projected aliases after rewrite_sort_after_return,
    // so we need to find which projections the sort terms reference.
    let sort_proj_indices: Vec<usize> = terms
        .iter()
        .filter_map(|t| {
            if let Expr::Var(alias) = &t.expr {
                projections.iter().position(|p| p.alias == *alias)
            } else {
                None
            }
        })
        .collect();

    // If we can't identify sort projections by alias (complex expressions), fall back
    // to evaluating all projections for every row (standard path).
    let can_lazy = !sort_proj_indices.is_empty() && sort_proj_indices.len() == terms.len();

    if !can_lazy {
        // Fallback: standard Return then TopK
        let projected = execute_return_inner(bindings, projections, ctx)?;
        return Ok(execute_topk(projected, terms, limit, ctx));
    }

    // === Lazy projection path ===

    // HeapEntry stores: original binding + pre-evaluated sort keys
    struct HeapEntry<'a> {
        binding_idx: usize,
        keys: Vec<GqlValue>,
        terms: &'a [OrderTerm],
    }

    impl Eq for HeapEntry<'_> {}
    impl PartialEq for HeapEntry<'_> {
        fn eq(&self, other: &Self) -> bool {
            cmp_keys(&self.keys, &other.keys, self.terms) == Ordering::Equal
        }
    }
    impl PartialOrd for HeapEntry<'_> {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }
    impl Ord for HeapEntry<'_> {
        fn cmp(&self, other: &Self) -> Ordering {
            cmp_keys(&self.keys, &other.keys, self.terms)
        }
    }

    fn cmp_keys(a: &[GqlValue], b: &[GqlValue], terms: &[OrderTerm]) -> Ordering {
        for (i, term) in terms.iter().enumerate() {
            let ord = compare_with_nulls(&a[i], &b[i], term);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    }

    let mut heap: BinaryHeap<HeapEntry<'_>> = BinaryHeap::with_capacity(k + 1);

    // Phase 1: Build heap using ONLY sort key evaluations (cheap)
    // Threshold pushdown: once the heap is full, skip any binding whose sort
    // key can't beat the current worst entry (heap max = worst keeper).
    for (idx, binding) in bindings.iter().enumerate() {
        // Evaluate only the sort-key projections from the original binding
        let keys: Vec<GqlValue> = sort_proj_indices
            .iter()
            .map(|&pi| eval_expr_ctx(&projections[pi].expr, binding, ctx))
            .collect::<Result<Vec<_>, _>>()?;

        // Threshold check: if heap is full, compare against worst entry
        if heap.len() >= k
            && let Some(worst) = heap.peek()
        {
            // If this candidate is worse than or equal to the worst keeper, skip it
            if cmp_keys(&keys, &worst.keys, terms) != Ordering::Less {
                continue;
            }
        }

        heap.push(HeapEntry {
            binding_idx: idx,
            keys,
            terms,
        });
        if heap.len() > k {
            heap.pop(); // evict the worst entry
        }
    }

    // Phase 2: Full projection ONLY for the K winners
    let mut winners: Vec<(usize, Vec<GqlValue>)> =
        heap.into_iter().map(|e| (e.binding_idx, e.keys)).collect();
    // Sort winners in desired output order
    winners.sort_by(|a, b| cmp_keys(&a.1, &b.1, terms));

    // Now project all columns for just the K winners
    let mut result = Vec::with_capacity(winners.len());
    for (binding_idx, _keys) in &winners {
        let binding = &bindings[*binding_idx];
        let mut out = Binding::empty();
        for proj in projections {
            let val = eval_expr_ctx(&proj.expr, binding, ctx)?;
            out.bind(proj.alias, BoundValue::Scalar(val));
        }
        result.push(out);
    }

    Ok(result)
}

/// Inner return execution without HAVING/DISTINCT. Used by fused ReturnTopK fallback.
pub(crate) fn execute_return_inner(
    bindings: Vec<Binding>,
    projections: &[PlannedProjection],
    ctx: &EvalContext<'_>,
) -> Result<Vec<Binding>, GqlError> {
    bindings
        .into_iter()
        .map(|b| {
            let mut out = Binding::empty();
            for proj in projections {
                let val = eval_expr_ctx(&proj.expr, &b, ctx)?;
                out.bind(proj.alias, BoundValue::Scalar(val));
            }
            Ok(out)
        })
        .collect()
}

// ── OFFSET ─────────────────────────────────────────────────────────

fn execute_offset(bindings: Vec<Binding>, count: u64) -> Vec<Binding> {
    let mut bindings = bindings;
    let skip = (count as usize).min(bindings.len());
    // split_off moves the tail into a new Vec without shifting elements
    // in-place. More efficient than drain(..skip) which shifts remaining
    // elements to the front of the original allocation.
    bindings.split_off(skip)
}

// ── LIMIT ──────────────────────────────────────────────────────────

fn execute_limit(bindings: Vec<Binding>, count: u64) -> Vec<Binding> {
    let mut bindings = bindings;
    bindings.truncate(count as usize);
    bindings
}

// ── RETURN ─────────────────────────────────────────────────────────

/// RETURN *: project all bound variables from each binding.
fn execute_return_all(
    bindings: Vec<Binding>,
    distinct: bool,
    ctx: &EvalContext<'_>,
) -> Result<Vec<Binding>, GqlError> {
    if bindings.is_empty() {
        return Ok(vec![]);
    }

    // Build projections from the first binding's variable names
    let first = &bindings[0];
    let projections: Vec<PlannedProjection> = first
        .iter()
        .map(|(name, _)| PlannedProjection {
            expr: Expr::Var(*name),
            alias: *name,
        })
        .collect();

    execute_return(bindings, &projections, &[], distinct, None, ctx)
}

fn execute_return(
    bindings: Vec<Binding>,
    projections: &[PlannedProjection],
    group_by: &[IStr],
    distinct: bool,
    having: Option<&Expr>,
    ctx: &EvalContext<'_>,
) -> Result<Vec<Binding>, GqlError> {
    let has_aggregates = projections.iter().any(|p| p.expr.is_aggregate());

    // Detect horizontal aggregation: all Expr::Aggregate inner expressions
    // resolve to List values per-row (from Group variables)
    let horizontal = has_aggregates && group_by.is_empty() && !bindings.is_empty() && {
        projections
            .iter()
            .filter(|p| p.expr.is_aggregate())
            .all(|p| match &p.expr {
                Expr::Aggregate(agg) => {
                    if let Some(inner) = &agg.expr {
                        matches!(
                            eval_expr_ctx(inner, &bindings[0], ctx),
                            Ok(GqlValue::List(_))
                        )
                    } else {
                        false
                    }
                }
                _ => false,
            })
    };

    let mut result = if !group_by.is_empty() {
        execute_grouped_return(bindings, projections, group_by, ctx)?
    } else if has_aggregates && !horizontal {
        execute_aggregate_return(bindings, projections, ctx)?
    } else {
        // Simple projection or horizontal aggregation over Group variables
        execute_simple_return(bindings, projections, ctx)?
    };

    // Apply HAVING (post-aggregation filter on projected/aggregated values)
    if let Some(having_expr) = having {
        result = execute_filter(result, having_expr, ctx)?;
    }

    // Apply DISTINCT
    if distinct {
        Ok(deduplicate(result))
    } else {
        Ok(result)
    }
}

/// Simple projection: evaluate each expression per binding.
fn execute_simple_return(
    bindings: Vec<Binding>,
    projections: &[PlannedProjection],
    ctx: &EvalContext<'_>,
) -> Result<Vec<Binding>, GqlError> {
    bindings
        .into_iter()
        .map(|b| {
            let mut out = Binding::empty();
            for proj in projections {
                let val = eval_expr_ctx(&proj.expr, &b, ctx)?;
                out.bind(proj.alias, BoundValue::Scalar(val));
            }
            Ok(out)
        })
        .collect()
}

/// Whole-table aggregation: single result row.
fn execute_aggregate_return(
    bindings: Vec<Binding>,
    projections: &[PlannedProjection],
    ctx: &EvalContext<'_>,
) -> Result<Vec<Binding>, GqlError> {
    let mut out = Binding::empty();
    for proj in projections {
        let val = eval_projection_aggregate(&proj.expr, &bindings, ctx)?;
        out.bind(proj.alias, BoundValue::Scalar(val));
    }
    Ok(vec![out])
}

/// GROUP BY aggregation: one result row per group.
///
/// Uses hash-based grouping for O(n) amortized instead of O(n*g) linear scan.
/// GroupKey uses distinctness_key() for hashing + is_not_distinct() for equality,
/// giving zero collision risk with NULL-safe grouping (NULL groups with NULL).
fn execute_grouped_return(
    bindings: Vec<Binding>,
    projections: &[PlannedProjection],
    group_by: &[IStr],
    ctx: &EvalContext<'_>,
) -> Result<Vec<Binding>, GqlError> {
    use std::hash::{Hash, Hasher};

    /// Hash-friendly group key: uses distinctness_key() for hash, is_not_distinct() for eq.
    #[derive(Clone)]
    struct GroupKey(Vec<GqlValue>);

    impl PartialEq for GroupKey {
        fn eq(&self, other: &Self) -> bool {
            self.0.len() == other.0.len()
                && self
                    .0
                    .iter()
                    .zip(other.0.iter())
                    .all(|(a, b)| a.is_not_distinct(b))
        }
    }
    impl Eq for GroupKey {}
    impl Hash for GroupKey {
        fn hash<H: Hasher>(&self, state: &mut H) {
            for val in &self.0 {
                val.distinctness_key().hash(state);
            }
        }
    }

    // HashMap for O(1) group lookup + Vec for insertion-order output
    let mut group_map: std::collections::HashMap<GroupKey, usize> =
        std::collections::HashMap::with_capacity(bindings.len().min(1024));
    let mut groups: Vec<Vec<Binding>> = Vec::new();

    for binding in bindings {
        let key_values: Vec<GqlValue> = group_by
            .iter()
            .map(|var| eval::resolve_var_as_value(var, &binding, ctx))
            .collect::<Result<Vec<_>, _>>()?;
        let key = GroupKey(key_values);

        if let Some(&idx) = group_map.get(&key) {
            groups[idx].push(binding);
        } else {
            let idx = groups.len();
            group_map.insert(key, idx);
            groups.push(vec![binding]);
        }
    }

    // Project each group
    groups
        .into_iter()
        .map(|group_bindings| {
            let mut out = Binding::empty();
            for proj in projections {
                let val = if proj.expr.is_aggregate() {
                    eval_projection_aggregate(&proj.expr, &group_bindings, ctx)?
                } else {
                    // Non-aggregate: evaluate against first row in group
                    eval_expr_ctx(&proj.expr, &group_bindings[0], ctx)?
                };
                out.bind(proj.alias, BoundValue::Scalar(val));
            }
            Ok(out)
        })
        .collect()
}

/// Evaluate a projection expression that may contain aggregates.
fn eval_projection_aggregate(
    expr: &Expr,
    bindings: &[Binding],
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    match expr {
        Expr::Aggregate(agg) => eval_aggregate(agg, bindings, ctx),
        Expr::Function(FunctionCall {
            count_star: true, ..
        }) => Ok(GqlValue::Int(bindings.len() as i64)),
        // Non-aggregate expressions in aggregate context: evaluate against first row
        _ => {
            if bindings.is_empty() {
                Ok(GqlValue::Null)
            } else {
                eval_expr_ctx(expr, &bindings[0], ctx)
            }
        }
    }
}

/// Deduplicate bindings using distinctness semantics.
/// NULL is NOT distinct from NULL (groups together).
fn deduplicate(mut bindings: Vec<Binding>) -> Vec<Binding> {
    if bindings.len() <= 1 {
        return bindings;
    }

    let mut seen = std::collections::HashSet::with_capacity(bindings.len());
    bindings.retain(|b| {
        let key: SmallVec<[u64; 4]> = b
            .iter()
            .map(|(_, v)| match v {
                BoundValue::Scalar(val) => val.distinctness_key(),
                BoundValue::Node(id) => id.0,
                BoundValue::Edge(id) => id.0,
                _ => 0,
            })
            .collect();
        seen.insert(key)
    });

    bindings
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::expr::{AggregateExpr, AggregateOp, ArithOp, CompareOp};
    use crate::runtime::functions::FunctionRegistry;
    use selene_core::{LabelSet, NodeId, PropertyMap, Value};
    use selene_graph::SeleneGraph;
    use smol_str::SmolStr;

    fn make_ctx(graph: &SeleneGraph) -> EvalContext<'_> {
        // Leak a registry for test lifetime (tests are short-lived)
        let registry = FunctionRegistry::builtins();
        EvalContext::new(graph, registry)
    }

    fn empty_graph() -> SeleneGraph {
        SeleneGraph::new()
    }

    fn test_graph() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::String(SmolStr::new("S1"))),
                (IStr::new("temp"), Value::Float(72.5)),
                (IStr::new("floor"), Value::String(SmolStr::new("F1"))),
            ]),
        )
        .unwrap();
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::String(SmolStr::new("S2"))),
                (IStr::new("temp"), Value::Float(80.0)),
                (IStr::new("floor"), Value::String(SmolStr::new("F1"))),
            ]),
        )
        .unwrap();
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::String(SmolStr::new("S3"))),
                (IStr::new("temp"), Value::Float(68.0)),
                (IStr::new("floor"), Value::String(SmolStr::new("F2"))),
            ]),
        )
        .unwrap();
        m.commit(0).unwrap();
        g
    }

    fn sensor_bindings() -> Vec<Binding> {
        vec![
            Binding::single(IStr::new("s"), BoundValue::Node(NodeId(1))),
            Binding::single(IStr::new("s"), BoundValue::Node(NodeId(2))),
            Binding::single(IStr::new("s"), BoundValue::Node(NodeId(3))),
        ]
    }

    // ── LET ──

    #[test]
    fn let_adds_variable() {
        let g = test_graph();
        let bindings = sensor_bindings();
        // LET x = s.temp + 10
        let lets = vec![(
            IStr::new("x"),
            Expr::Arithmetic(
                Box::new(Expr::Property(
                    Box::new(Expr::Var(IStr::new("s"))),
                    IStr::new("temp"),
                )),
                ArithOp::Add,
                Box::new(Expr::Literal(GqlValue::Float(10.0))),
            ),
        )];
        let result = execute_let(bindings, &lets, &make_ctx(&g)).unwrap();
        assert_eq!(result.len(), 3);
        // First binding: s.temp=72.5 + 10 = 82.5
        match result[0].get(&IStr::new("x")) {
            Some(BoundValue::Scalar(GqlValue::Float(f))) => assert_eq!(*f, 82.5),
            other => panic!("expected Float(82.5), got {other:?}"),
        }
    }

    // ── FILTER ──

    #[test]
    fn filter_passes_true() {
        let g = test_graph();
        let bindings = sensor_bindings();
        // FILTER s.temp > 72
        let predicate = Expr::Compare(
            Box::new(Expr::Property(
                Box::new(Expr::Var(IStr::new("s"))),
                IStr::new("temp"),
            )),
            CompareOp::Gt,
            Box::new(Expr::Literal(GqlValue::Float(72.0))),
        );
        let result = execute_filter(bindings, &predicate, &make_ctx(&g)).unwrap();
        // S1: 72.5 > 72 ✓, S2: 80.0 > 72 ✓, S3: 68.0 > 72 ✗
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn filter_null_is_filtered_out() {
        let g = test_graph();
        // Binding with a node that has no 'missing_prop' → NULL > 72 → UNKNOWN → filtered
        let bindings = sensor_bindings();
        let predicate = Expr::Compare(
            Box::new(Expr::Property(
                Box::new(Expr::Var(IStr::new("s"))),
                IStr::new("missing_prop"),
            )),
            CompareOp::Gt,
            Box::new(Expr::Literal(GqlValue::Float(72.0))),
        );
        let result = execute_filter(bindings, &predicate, &make_ctx(&g)).unwrap();
        assert_eq!(result.len(), 0); // All filtered (NULL comparison → UNKNOWN)
    }

    // ── ORDER BY ──

    #[test]
    fn sort_ascending() {
        let g = test_graph();
        let bindings = sensor_bindings();
        let terms = vec![OrderTerm {
            expr: Expr::Property(Box::new(Expr::Var(IStr::new("s"))), IStr::new("temp")),
            descending: false,
            nulls_first: None,
        }];
        let result = execute_sort(bindings, &terms, &make_ctx(&g));
        // Order: S3(68.0), S1(72.5), S2(80.0)
        assert_eq!(result[0].get_node_id(&IStr::new("s")).unwrap(), NodeId(3));
        assert_eq!(result[1].get_node_id(&IStr::new("s")).unwrap(), NodeId(1));
        assert_eq!(result[2].get_node_id(&IStr::new("s")).unwrap(), NodeId(2));
    }

    #[test]
    fn sort_descending() {
        let g = test_graph();
        let bindings = sensor_bindings();
        let terms = vec![OrderTerm {
            expr: Expr::Property(Box::new(Expr::Var(IStr::new("s"))), IStr::new("temp")),
            descending: true,
            nulls_first: None,
        }];
        let result = execute_sort(bindings, &terms, &make_ctx(&g));
        // Order: S2(80.0), S1(72.5), S3(68.0)
        assert_eq!(result[0].get_node_id(&IStr::new("s")).unwrap(), NodeId(2));
    }

    #[test]
    fn sort_null_ordering_defaults() {
        let g = empty_graph();
        let bindings: Vec<Binding> = vec![
            {
                let mut b = Binding::empty();
                b.bind(IStr::new("x"), BoundValue::Scalar(GqlValue::Int(5)));
                b
            },
            {
                let mut b = Binding::empty();
                b.bind(IStr::new("x"), BoundValue::Scalar(GqlValue::Null));
                b
            },
            {
                let mut b = Binding::empty();
                b.bind(IStr::new("x"), BoundValue::Scalar(GqlValue::Int(3)));
                b
            },
        ];

        // ASC default: NULLS LAST (SQL/GQL standard)
        let terms = vec![OrderTerm {
            expr: Expr::Var(IStr::new("x")),
            descending: false,
            nulls_first: None,
        }];
        let result = execute_sort(bindings.clone(), &terms, &make_ctx(&g));
        // ASC NULLS LAST: 3, 5, NULL
        match result[0].get(&IStr::new("x")) {
            Some(BoundValue::Scalar(GqlValue::Int(3))) => {}
            other => panic!("expected Int(3) first for ASC NULLS LAST, got {other:?}"),
        }
        match result[2].get(&IStr::new("x")) {
            Some(BoundValue::Scalar(GqlValue::Null)) => {}
            other => panic!("expected Null last for ASC NULLS LAST, got {other:?}"),
        }

        // ASC NULLS FIRST: explicit override
        let terms_nf = vec![OrderTerm {
            expr: Expr::Var(IStr::new("x")),
            descending: false,
            nulls_first: Some(true),
        }];
        let result = execute_sort(bindings.clone(), &terms_nf, &make_ctx(&g));
        // ASC NULLS FIRST: NULL, 3, 5
        match result[0].get(&IStr::new("x")) {
            Some(BoundValue::Scalar(GqlValue::Null)) => {}
            other => panic!("expected Null first for ASC NULLS FIRST, got {other:?}"),
        }

        // DESC default: NULLS FIRST
        let terms_desc = vec![OrderTerm {
            expr: Expr::Var(IStr::new("x")),
            descending: true,
            nulls_first: None,
        }];
        let result = execute_sort(bindings, &terms_desc, &make_ctx(&g));
        // DESC NULLS FIRST: NULL, 5, 3
        match result[0].get(&IStr::new("x")) {
            Some(BoundValue::Scalar(GqlValue::Null)) => {}
            other => panic!("expected Null first for DESC NULLS FIRST, got {other:?}"),
        }
    }

    // ── OFFSET / LIMIT ──

    #[test]
    fn offset_skips() {
        let bindings = sensor_bindings();
        let result = execute_offset(bindings, 2);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn limit_truncates() {
        let bindings = sensor_bindings();
        let result = execute_limit(bindings, 2);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn offset_and_limit() {
        let bindings = sensor_bindings();
        let result = execute_limit(execute_offset(bindings, 1), 1);
        assert_eq!(result.len(), 1);
    }

    // ── RETURN (simple projection) ──

    #[test]
    fn return_simple_projection() {
        let g = test_graph();
        let bindings = sensor_bindings();
        let projections = vec![
            PlannedProjection {
                expr: Expr::Property(Box::new(Expr::Var(IStr::new("s"))), IStr::new("name")),
                alias: IStr::new("sensor_name"),
            },
            PlannedProjection {
                expr: Expr::Property(Box::new(Expr::Var(IStr::new("s"))), IStr::new("temp")),
                alias: IStr::new("temperature"),
            },
        ];
        let result =
            execute_return(bindings, &projections, &[], false, None, &make_ctx(&g)).unwrap();
        assert_eq!(result.len(), 3);
        // Check first row
        match result[0].get(&IStr::new("sensor_name")) {
            Some(BoundValue::Scalar(GqlValue::String(s))) => assert_eq!(&**s, "S1"),
            other => panic!("expected String, got {other:?}"),
        }
    }

    // ── RETURN (whole-table aggregation) ──

    #[test]
    fn return_count_star() {
        let g = test_graph();
        let bindings = sensor_bindings();
        let projections = vec![PlannedProjection {
            expr: Expr::Function(FunctionCall {
                name: IStr::new("count"),
                args: vec![],
                count_star: true,
            }),
            alias: IStr::new("total"),
        }];
        let result =
            execute_return(bindings, &projections, &[], false, None, &make_ctx(&g)).unwrap();
        assert_eq!(result.len(), 1);
        match result[0].get(&IStr::new("total")) {
            Some(BoundValue::Scalar(GqlValue::Int(3))) => {}
            other => panic!("expected Int(3), got {other:?}"),
        }
    }

    #[test]
    fn return_avg_aggregate() {
        let g = test_graph();
        let bindings = sensor_bindings();
        let projections = vec![PlannedProjection {
            expr: Expr::Aggregate(AggregateExpr {
                op: AggregateOp::Avg,
                expr: Some(Box::new(Expr::Property(
                    Box::new(Expr::Var(IStr::new("s"))),
                    IStr::new("temp"),
                ))),
                distinct: false,
            }),
            alias: IStr::new("avg_temp"),
        }];
        let result =
            execute_return(bindings, &projections, &[], false, None, &make_ctx(&g)).unwrap();
        assert_eq!(result.len(), 1);
        match result[0].get(&IStr::new("avg_temp")) {
            Some(BoundValue::Scalar(GqlValue::Float(f))) => {
                let expected = (72.5 + 80.0 + 68.0) / 3.0;
                assert!((f - expected).abs() < 0.01);
            }
            other => panic!("expected Float, got {other:?}"),
        }
    }

    // ── RETURN (GROUP BY) ──

    #[test]
    fn return_group_by() {
        let g = test_graph();
        let bindings = sensor_bindings();
        // GROUP BY floor: F1 has S1+S2, F2 has S3
        // First add floor as LET variable
        let lets = vec![(
            IStr::new("floor_name"),
            Expr::Property(Box::new(Expr::Var(IStr::new("s"))), IStr::new("floor")),
        )];
        let bindings = execute_let(bindings, &lets, &make_ctx(&g)).unwrap();

        let projections = vec![
            PlannedProjection {
                expr: Expr::Var(IStr::new("floor_name")),
                alias: IStr::new("floor"),
            },
            PlannedProjection {
                expr: Expr::Function(FunctionCall {
                    name: IStr::new("count"),
                    args: vec![],
                    count_star: true,
                }),
                alias: IStr::new("sensor_count"),
            },
        ];
        let result = execute_return(
            bindings,
            &projections,
            &[IStr::new("floor_name")],
            false,
            None,
            &make_ctx(&g),
        )
        .unwrap();
        assert_eq!(result.len(), 2); // F1 and F2

        // Find F1 group (should have count 2)
        let f1 = result
            .iter()
            .find(|b| {
                matches!(
                    b.get(&IStr::new("floor")),
                    Some(BoundValue::Scalar(GqlValue::String(s))) if &**s == "F1"
                )
            })
            .unwrap();
        match f1.get(&IStr::new("sensor_count")) {
            Some(BoundValue::Scalar(GqlValue::Int(2))) => {}
            other => panic!("expected Int(2) for F1, got {other:?}"),
        }
    }

    // ── RETURN (DISTINCT) ──

    #[test]
    fn return_distinct() {
        let g = test_graph();
        let bindings = sensor_bindings();
        // Project just the floor -- should dedup F1 (appears twice)
        let projections = vec![PlannedProjection {
            expr: Expr::Property(Box::new(Expr::Var(IStr::new("s"))), IStr::new("floor")),
            alias: IStr::new("floor"),
        }];
        let result =
            execute_return(bindings, &projections, &[], true, None, &make_ctx(&g)).unwrap();
        assert_eq!(result.len(), 2); // F1, F2 (deduplicated)
    }

    // ── Full pipeline integration ──

    #[test]
    fn pipeline_filter_sort_limit() {
        let g = test_graph();
        let bindings = sensor_bindings();

        // FILTER s.temp > 70
        let filtered = execute_filter(
            bindings,
            &Expr::Compare(
                Box::new(Expr::Property(
                    Box::new(Expr::Var(IStr::new("s"))),
                    IStr::new("temp"),
                )),
                CompareOp::Gt,
                Box::new(Expr::Literal(GqlValue::Float(70.0))),
            ),
            &make_ctx(&g),
        )
        .unwrap();
        assert_eq!(filtered.len(), 2); // S1(72.5), S2(80.0)

        // ORDER BY s.temp DESC
        let sorted = execute_sort(
            filtered,
            &[OrderTerm {
                expr: Expr::Property(Box::new(Expr::Var(IStr::new("s"))), IStr::new("temp")),
                descending: true,
                nulls_first: None,
            }],
            &make_ctx(&g),
        );
        assert_eq!(sorted[0].get_node_id(&IStr::new("s")).unwrap(), NodeId(2)); // 80.0 first

        // LIMIT 1
        let limited = execute_limit(sorted, 1);
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].get_node_id(&IStr::new("s")).unwrap(), NodeId(2));
    }
}
