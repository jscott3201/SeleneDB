//! GQL planner -- AST to ExecutionPlan.
//!
//! Converts parsed GQL statements into executable plans. Pattern planning
//! selects scan roots by label cardinality, chains expand operations, and
//! pushes down property filters. Pipeline planning maps AST statements
//! to sequential pipeline operations.

pub mod optimize;
pub mod plan;
pub(crate) mod wco_rule;

use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::Arc;

use selene_core::IStr;
use selene_graph::{EdgeStatistics, SeleneGraph};

// Thread-local cache for EdgeStatistics keyed by graph identity + generation.
// Avoids O(E) rebuild on every plan_query/plan_mutation call. The pointer
// discriminator prevents false hits across distinct graph instances that
// share the same generation (e.g., multiple fresh graphs at generation 0).
type EdgeStatsEntry = ((usize, u64), Arc<EdgeStatistics>);
thread_local! {
    static EDGE_STATS_CACHE: RefCell<Option<EdgeStatsEntry>> = const { RefCell::new(None) };
}

fn cached_edge_statistics(graph: &SeleneGraph) -> Arc<EdgeStatistics> {
    EDGE_STATS_CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        let key = (std::ptr::from_ref(graph) as usize, graph.generation());
        if let Some((cached_key, stats)) = c.as_ref()
            && *cached_key == key
        {
            return Arc::clone(stats);
        }
        let stats = Arc::new(EdgeStatistics::build(graph));
        *c = Some((key, Arc::clone(&stats)));
        stats
    })
}

use crate::ast::expr::Expr;
use crate::ast::pattern::*;
use crate::ast::statement::*;
use crate::types::error::GqlError;

use plan::*;

/// Plan a single pipeline statement (Let, Filter, OrderBy, Offset, Limit, With,
/// Call, Subquery, For) into `pipeline_ops`. Returns `Ok(true)` if handled,
/// `Ok(false)` if the caller should handle it (Match, Return).
fn plan_pipeline_stmt(
    stmt: &PipelineStatement,
    pipeline_ops: &mut Vec<PipelineOp>,
    graph: &SeleneGraph,
) -> Result<bool, GqlError> {
    match stmt {
        PipelineStatement::Let(bindings) => {
            let lets: Vec<(IStr, Expr)> = bindings
                .iter()
                .map(|lb| (lb.var, lb.expr.clone()))
                .collect();
            pipeline_ops.push(PipelineOp::Let { bindings: lets });
        }
        PipelineStatement::Filter(expr) => {
            pipeline_ops.push(PipelineOp::Filter {
                predicate: expr.clone(),
            });
        }
        PipelineStatement::OrderBy(terms) => {
            pipeline_ops.push(PipelineOp::Sort {
                terms: terms.clone(),
            });
        }
        PipelineStatement::Offset(n) => {
            pipeline_ops.push(PipelineOp::Offset { count: *n });
        }
        PipelineStatement::Limit(n) => {
            pipeline_ops.push(PipelineOp::Limit { count: *n });
        }
        PipelineStatement::With(with) => {
            plan_with(with, pipeline_ops);
        }
        PipelineStatement::Call(call) => {
            pipeline_ops.push(PipelineOp::Call {
                procedure: call.clone(),
            });
        }
        PipelineStatement::Subquery(sub_pipeline) => {
            let sub_plan = plan_query(sub_pipeline, graph)?;
            pipeline_ops.push(PipelineOp::Subquery {
                plan: Box::new(sub_plan),
            });
        }
        PipelineStatement::For { var, list_expr } => {
            pipeline_ops.push(PipelineOp::For {
                var: *var,
                list_expr: list_expr.clone(),
            });
        }
        PipelineStatement::MatchView { name, yields } => {
            pipeline_ops.push(PipelineOp::ViewScan {
                view_name: *name,
                yields: yields.iter().map(|y| (y.name, y.alias)).collect(),
            });
        }
        _ => return Ok(false),
    }
    Ok(true)
}

/// Plan a query pipeline into an ExecutionPlan.
pub fn plan_query(
    pipeline: &QueryPipeline,
    graph: &SeleneGraph,
) -> Result<ExecutionPlan, GqlError> {
    let mut pattern_ops = Vec::new();
    let mut pipeline_ops = Vec::new();
    let mut bound_vars: HashSet<IStr> = HashSet::new();
    let edge_stats = cached_edge_statistics(graph);

    for stmt in &pipeline.statements {
        match stmt {
            PipelineStatement::Match(m) => {
                if m.optional {
                    // OPTIONAL MATCH: wrap inner ops in Optional with join vars
                    let mut inner_ops = Vec::new();
                    plan_match(m, &mut inner_ops, graph, &edge_stats)?;
                    let inner_vars = collect_bound_vars(&inner_ops);
                    let new_vars = inner_vars.iter().copied().collect();
                    let join_vars: Vec<IStr> =
                        bound_vars.intersection(&inner_vars).copied().collect();
                    pattern_ops.push(PatternOp::Optional {
                        inner_ops,
                        new_vars,
                        join_vars,
                    });
                    bound_vars.extend(inner_vars);
                } else {
                    let mut new_ops = Vec::new();
                    plan_match(m, &mut new_ops, graph, &edge_stats)?;
                    let new_vars = collect_bound_vars(&new_ops);

                    // If not the first MATCH, use Join (shared vars → equi-join, else → cartesian)
                    let shared: Vec<IStr> = bound_vars.intersection(&new_vars).copied().collect();
                    if pattern_ops.is_empty() {
                        pattern_ops.extend(new_ops);
                    } else {
                        let right_start = pattern_ops.len();
                        pattern_ops.extend(new_ops);
                        let right_end = pattern_ops.len();
                        pattern_ops.push(PatternOp::Join {
                            right_start,
                            right_end,
                            join_vars: shared,
                        });
                    }
                    bound_vars.extend(new_vars);
                }

                // MATCH ... WHERE predicate → add as pipeline filter
                if let Some(ref predicate) = m.where_clause {
                    pipeline_ops.push(PipelineOp::Filter {
                        predicate: predicate.clone(),
                    });
                }
            }
            PipelineStatement::Return(ret) => {
                plan_return(ret, &mut pipeline_ops);
            }
            other => {
                plan_pipeline_stmt(other, &mut pipeline_ops, graph)?;
            }
        }
    }

    // Rewrite Sort/TopK expressions that follow Return to reference projected aliases.
    // After RETURN, original pattern variables are destroyed -- Sort must use alias names.
    rewrite_sort_after_return(&mut pipeline_ops);

    let output_schema = derive_output_schema(&pipeline_ops);
    let count_only = is_count_only_query(&pattern_ops, &pipeline_ops);

    let plan = ExecutionPlan {
        pattern_ops,
        pipeline: pipeline_ops,
        mutations: vec![],
        output_schema,
        count_only,
    };

    // Run optimizer
    let optimizer = optimize::GqlOptimizer::with_default_rules();
    let ctx = optimize::OptimizeContext::new(graph);
    optimizer.optimize(plan, &ctx)
}

/// Plan a mutation pipeline into an ExecutionPlan.
pub fn plan_mutation(
    mp: &crate::ast::mutation::MutationPipeline,
    graph: &SeleneGraph,
) -> Result<ExecutionPlan, GqlError> {
    let mut pattern_ops = Vec::new();
    let mut pipeline_ops = Vec::new();
    let edge_stats = cached_edge_statistics(graph);

    if let Some(query) = &mp.query {
        for stmt in &query.statements {
            match stmt {
                PipelineStatement::Match(m) => {
                    plan_match(m, &mut pattern_ops, graph, &edge_stats)?;
                }
                PipelineStatement::Return(_) => {
                    return Err(GqlError::parse_error(
                        "RETURN inside mutation query block is not supported; \
                         use the post-mutation RETURN clause instead",
                    ));
                }
                other => {
                    plan_pipeline_stmt(other, &mut pipeline_ops, graph)?;
                }
            }
        }
    }

    if let Some(ret) = &mp.returning {
        plan_return(ret, &mut pipeline_ops);
    }

    let output_schema = derive_output_schema(&pipeline_ops);

    let plan = ExecutionPlan {
        pattern_ops,
        pipeline: pipeline_ops,
        mutations: mp.mutations.clone(),
        output_schema,
        count_only: false, // mutations never count-only
    };

    // Optimize the read portion of the mutation plan (filter pushdown,
    // range index hints, etc.). TopK is safe since it only affects the
    // MATCH result set, not mutation semantics.
    if plan.pattern_ops.is_empty() {
        Ok(plan)
    } else {
        let optimizer = optimize::GqlOptimizer::with_default_rules();
        let ctx = optimize::OptimizeContext::new(graph);
        optimizer.optimize(plan, &ctx)
    }
}

/// Public wrapper for EXISTS subquery planning.
pub fn plan_match_public(
    m: &MatchClause,
    ops: &mut Vec<PatternOp>,
    graph: &SeleneGraph,
) -> Result<(), GqlError> {
    let edge_stats = cached_edge_statistics(graph);
    plan_match(m, ops, graph, &edge_stats)
}

fn plan_match(
    m: &MatchClause,
    ops: &mut Vec<PatternOp>,
    graph: &SeleneGraph,
    edge_stats: &EdgeStatistics,
) -> Result<(), GqlError> {
    if m.patterns.len() == 1 {
        plan_single_pattern(
            &m.patterns[0],
            m.path_mode,
            m.selector,
            ops,
            graph,
            edge_stats,
        )?;
    } else {
        let mut all_pattern_ops: Vec<Vec<PatternOp>> = Vec::new();
        for pattern in &m.patterns {
            let mut pattern_ops = Vec::new();
            plan_single_pattern(
                pattern,
                m.path_mode,
                m.selector,
                &mut pattern_ops,
                graph,
                edge_stats,
            )?;
            all_pattern_ops.push(pattern_ops);
        }

        let first_ops = all_pattern_ops.remove(0);
        let mut all_vars = collect_bound_vars(&first_ops);
        ops.extend(first_ops);

        for other_ops in all_pattern_ops {
            let other_vars = collect_bound_vars(&other_ops);
            let join_vars: Vec<IStr> = all_vars.intersection(&other_vars).copied().collect();

            let right_start = ops.len();
            ops.extend(other_ops);
            let right_end = ops.len();

            ops.push(PatternOp::Join {
                right_start,
                right_end,
                join_vars,
            });

            // Accumulate variables for subsequent joins
            all_vars.extend(other_vars);
        }
    }

    // DIFFERENT EDGES match mode: append filter that ensures all edge
    // variables in the MATCH clause bind to distinct edges.
    if m.match_mode == Some(crate::ast::pattern::MatchMode::DifferentEdges) {
        let edge_vars = collect_edge_vars(ops);
        if edge_vars.len() >= 2 {
            ops.push(PatternOp::DifferentEdgesFilter { edge_vars });
        }
    }

    Ok(())
}

fn plan_single_pattern(
    pattern: &GraphPattern,
    path_mode: crate::ast::pattern::PathMode,
    selector: Option<crate::ast::pattern::PathSelector>,
    ops: &mut Vec<PatternOp>,
    graph: &SeleneGraph,
    edge_stats: &EdgeStatistics,
) -> Result<(), GqlError> {
    // ── Cardinality-based root selection ──
    // If the last node in the pattern has lower cardinality than the first,
    // reverse the element order and flip edge directions. This ensures we
    // scan the rarest label and expand outward from it.
    let reordered = maybe_reorder_for_cardinality(&pattern.elements, graph, edge_stats);
    let effective_elements = if reordered.is_empty() {
        &pattern.elements
    } else {
        &reordered
    };

    let mut prev_node_var: Option<IStr> = None;
    let mut bound_vars: std::collections::HashSet<IStr> = std::collections::HashSet::new();
    let start_idx = ops.len();
    // Track if next edge closes a cycle (target node already bound)
    let mut pending_cycle_close: Option<(IStr, IStr, Option<LabelExpr>, EdgeDirection)> = None;

    for element in effective_elements {
        match element {
            PatternElement::Node(node) => {
                let var = node
                    .var
                    .unwrap_or_else(|| IStr::new(&format!("_anon_{}", ops.len())));

                if prev_node_var.is_none() {
                    ops.push(PatternOp::LabelScan {
                        var,
                        labels: node.labels.clone(),
                        inline_props: node.properties.clone(),
                        property_filters: vec![],
                        index_order: None,
                        composite_index_keys: None,
                        range_index_hint: None,
                        in_list_hint: None,
                    });
                    bound_vars.insert(var);
                } else if bound_vars.contains(&var) {
                    // Cycle detected: this node was already bound.
                    // The preceding edge should close the cycle with a CycleJoin.
                    if let Some((bound_var, source_var, edge_labels, direction)) =
                        pending_cycle_close.take()
                    {
                        ops.push(PatternOp::CycleJoin {
                            bound_var,
                            source_var,
                            edge_labels,
                            direction,
                        });
                    }
                } else {
                    bound_vars.insert(var);
                }
                prev_node_var = Some(var);
                pending_cycle_close = None;
            }
            PatternElement::Edge(edge) => {
                let source_var = prev_node_var
                    .ok_or_else(|| GqlError::parse_error("edge without preceding node"))?;

                // Peek ahead: if the next node is already bound, this edge closes a cycle.
                // We'll know when we process the next Node element.
                // For now, emit the Expand as normal -- if next node is bound, we'll replace
                // it with a CycleJoin when processing that node.
                let target_var = IStr::new(&format!("_target_{}", ops.len()));

                // Check if the pattern's next element is a node that's already bound
                // (we set pending_cycle_close and the Expand target won't be used --
                // the CycleJoin filters instead)
                let next_node_var = find_next_node_var(effective_elements, element);
                let is_cycle = next_node_var.is_some_and(|v| bound_vars.contains(&v));

                if is_cycle {
                    // Don't emit Expand -- store info for CycleJoin when we hit the node
                    pending_cycle_close = Some((
                        next_node_var.unwrap(),
                        source_var,
                        edge.labels.clone(),
                        edge.direction,
                    ));
                } else if let Some(ref quantifier) = edge.quantifier {
                    ops.push(PatternOp::VarExpand {
                        source_var,
                        edge_var: edge.var,
                        target_var,
                        edge_labels: edge.labels.clone(),
                        target_labels: None,
                        direction: edge.direction,
                        min_hops: quantifier.min,
                        max_hops: quantifier.max,
                        trail: matches!(path_mode, crate::ast::pattern::PathMode::Trail),
                        acyclic: matches!(path_mode, crate::ast::pattern::PathMode::Acyclic),
                        simple: matches!(path_mode, crate::ast::pattern::PathMode::Simple),
                        shortest: selector,
                        path_var: None,
                    });
                } else {
                    ops.push(PatternOp::Expand {
                        source_var,
                        edge_var: edge.var,
                        target_var,
                        edge_labels: edge.labels.clone(),
                        target_labels: None,
                        direction: edge.direction,
                        target_property_filters: vec![],
                        edge_property_filters: vec![],
                    });
                }
                if !is_cycle {
                    prev_node_var = Some(target_var);
                }
            }
        }
    }

    fixup_target_vars(effective_elements, &mut ops[start_idx..], pattern.path_var);
    Ok(())
}

/// Evaluate whether reordering the pattern element chain produces a better
/// scan root. Compares the estimated total intermediate cardinality when
/// starting from the first node vs the last node.
///
/// Uses EdgeStatistics for actual fan-out data when available, falling
/// back to a default fan-out of 4.0 per hop.
///
/// Only handles linear chains (Node-Edge-Node-Edge-...-Node).
/// Skips patterns with VarExpand (quantifiers) or unlabeled first nodes
/// (likely correlated variables from outer scope).
fn maybe_reorder_for_cardinality(
    elements: &[PatternElement],
    graph: &SeleneGraph,
    edge_stats: &EdgeStatistics,
) -> Vec<PatternElement> {
    // Need at least 3 elements (Node-Edge-Node) to have a choice
    if elements.len() < 3 {
        return vec![];
    }

    // Don't reorder if any edge has a quantifier (VarExpand)
    let has_var_expand = elements
        .iter()
        .any(|e| matches!(e, PatternElement::Edge(edge) if edge.quantifier.is_some()));
    if has_var_expand {
        return vec![];
    }

    // Don't reorder if the first node has no labels -- it's likely a correlated
    // variable from an outer scope (e.g., COUNT { MATCH (b)-[:r]->(x:label) }).
    if matches!(&elements[0], PatternElement::Node(n) if n.labels.is_none()) {
        return vec![];
    }

    let default_fan_out = 4.0_f64;

    // Estimate total intermediate cardinality starting from one end.
    let estimate_forward_cost = |elems: &[PatternElement]| -> f64 {
        let root_card = node_cardinality(&elems[0], graph) as f64;
        let mut cost = root_card;
        let mut current_card = root_card;
        let mut i = 1;
        while i < elems.len() {
            if let PatternElement::Edge(edge) = &elems[i] {
                let src_elem = &elems[i - 1];
                let tgt_elem = elems.get(i + 1);
                let fan_out = edge_fan_out(src_elem, edge, tgt_elem, edge_stats, default_fan_out);
                current_card *= fan_out;
                cost += current_card;
            }
            i += 1;
        }
        cost
    };

    let forward_cost = estimate_forward_cost(elements);

    // Build reversed version and estimate its cost
    let mut reversed: Vec<PatternElement> = elements.iter().rev().cloned().collect();
    flip_edge_directions(&mut reversed);
    let reverse_cost = estimate_forward_cost(&reversed);

    if reverse_cost < forward_cost {
        reversed
    } else {
        vec![]
    }
}

/// Look up the average fan-out for an edge traversal from EdgeStatistics.
/// Falls back to `default` if no statistics are available for the triple.
fn edge_fan_out(
    source_elem: &PatternElement,
    edge: &EdgePattern,
    target_elem: Option<&PatternElement>,
    edge_stats: &EdgeStatistics,
    default: f64,
) -> f64 {
    let src_label = match source_elem {
        PatternElement::Node(n) => match &n.labels {
            Some(LabelExpr::Name(name)) => Some(name),
            _ => None,
        },
        PatternElement::Edge(_) => None,
    };
    let edge_label = match &edge.labels {
        Some(LabelExpr::Name(name)) => Some(*name),
        _ => None,
    };
    let tgt_label = target_elem.and_then(|e| match e {
        PatternElement::Node(n) => match &n.labels {
            Some(LabelExpr::Name(name)) => Some(name),
            _ => None,
        },
        PatternElement::Edge(_) => None,
    });

    if let (Some(sl), Some(el), Some(tl)) = (src_label, edge_label, tgt_label)
        && let Some(ds) = edge_stats.get(*sl, el, *tl)
    {
        return ds.avg_out_degree;
    }

    default
}

/// Flip edge directions in a pattern element list (for reversed traversal).
fn flip_edge_directions(elements: &mut [PatternElement]) {
    for elem in elements.iter_mut() {
        if let PatternElement::Edge(edge) = elem {
            edge.direction = match edge.direction {
                EdgeDirection::Out => EdgeDirection::In,
                EdgeDirection::In => EdgeDirection::Out,
                EdgeDirection::Any => EdgeDirection::Any,
            };
        }
    }
}

/// Get the label cardinality for a pattern node. Returns u64::MAX for
/// unlabeled nodes (they match all nodes, so never prefer them as scan root).
fn node_cardinality(element: &PatternElement, graph: &SeleneGraph) -> u64 {
    match element {
        PatternElement::Node(node) => match &node.labels {
            Some(LabelExpr::Name(name)) => graph.nodes_by_label_count(name.as_str()) as u64,
            Some(label_expr) => {
                // Complex label expression -- resolve bitmap and check cardinality
                crate::pattern::scan::resolve_label_expr(label_expr, graph).len()
            }
            None => u64::MAX, // No label -- matches everything
        },
        PatternElement::Edge(_) => u64::MAX,
    }
}

/// Find the next node variable after the current element in the pattern.
fn find_next_node_var(elements: &[PatternElement], current: &PatternElement) -> Option<IStr> {
    let mut found_current = false;
    for element in elements {
        if std::ptr::eq(element, current) {
            found_current = true;
            continue;
        }
        if found_current && let PatternElement::Node(node) = element {
            return node.var;
        }
    }
    None
}

fn fixup_target_vars(elements: &[PatternElement], ops: &mut [PatternOp], path_var: Option<IStr>) {
    // Count: first node → ops[0] (LabelScan). Each edge → ops[edge_count] (Expand/VarExpand).
    // So the Nth edge's op is at ops[N] (1-indexed: ops[1] for first edge, etc.)
    let mut node_idx = 0;
    let mut edge_count = 0;

    for element in elements {
        match element {
            PatternElement::Node(node) => {
                let var = node
                    .var
                    .unwrap_or_else(|| IStr::new(&format!("_anon_{node_idx}")));

                // After the first node, each node follows an edge.
                // The edge's op is at ops[edge_count] (edge_count is 1-based after increment).
                if node_idx > 0 && edge_count > 0 && edge_count <= ops.len() {
                    match &mut ops[edge_count] {
                        PatternOp::Expand {
                            target_var,
                            target_labels,
                            ..
                        } => {
                            *target_var = var;
                            *target_labels = node.labels.clone();
                        }
                        PatternOp::VarExpand {
                            target_var,
                            target_labels,
                            path_var: pv,
                            ..
                        } => {
                            *target_var = var;
                            *target_labels = node.labels.clone();
                            if let Some(pvar) = path_var {
                                *pv = Some(pvar);
                            }
                        }
                        _ => {}
                    }
                }
                node_idx += 1;
            }
            PatternElement::Edge(_) => {
                edge_count += 1;
            }
        }
    }
}

// Re-use the shared collect_bound_vars from plan module.
use plan::collect_bound_vars;

/// Collect all named edge variables from pattern ops.
fn collect_edge_vars(ops: &[PatternOp]) -> Vec<IStr> {
    let mut vars = Vec::new();
    for op in ops {
        match op {
            PatternOp::Expand { edge_var, .. } | PatternOp::VarExpand { edge_var, .. } => {
                if let Some(ev) = edge_var {
                    vars.push(*ev);
                }
            }
            PatternOp::Optional { inner_ops, .. } => {
                vars.extend(collect_edge_vars(inner_ops));
            }
            _ => {}
        }
    }
    vars
}

fn plan_return(ret: &ReturnClause, ops: &mut Vec<PipelineOp>) {
    let projections = plan_projections(&ret.projections);

    ops.push(PipelineOp::Return {
        projections,
        group_by: ret.group_by.clone(),
        distinct: ret.distinct,
        having: ret.having.clone(),
        all: ret.all,
    });

    if !ret.order_by.is_empty() {
        ops.push(PipelineOp::Sort {
            terms: ret.order_by.clone(),
        });
    }
    if let Some(offset) = ret.offset {
        ops.push(PipelineOp::Offset { count: offset });
    }
    if let Some(limit) = ret.limit {
        ops.push(PipelineOp::Limit { count: limit });
    }
}

fn plan_with(with: &WithClause, ops: &mut Vec<PipelineOp>) {
    let projections = plan_projections(&with.projections);

    ops.push(PipelineOp::With {
        projections,
        group_by: with.group_by.clone(),
        distinct: with.distinct,
        having: with.having.clone(),
        where_filter: with.where_filter.clone(),
    });
}

fn plan_projections(projections: &[Projection]) -> Vec<PlannedProjection> {
    projections
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let alias = p.alias.unwrap_or_else(|| {
                // Infer alias from bare variable references (standard GQL
                // behavior: `RETURN x` produces a column named `x`).
                // All other expressions fall back to positional `col_{i}`.
                match &p.expr {
                    crate::ast::expr::Expr::Var(name) => *name,
                    _ => IStr::new(&format!("col_{i}")),
                }
            });
            PlannedProjection {
                expr: p.expr.clone(),
                alias,
            }
        })
        .collect()
}

/// Rewrite Sort/TopK expressions that follow a Return to reference projected aliases.
///
/// After RETURN, original pattern variables (s, n, etc.) are replaced by
/// projection aliases (col_0, name, temp, etc.). Any Sort/TopK that follows
/// must use the alias names. If an ORDER BY expression wasn't projected, it's
/// added as a hidden projection (prefixed with `_sort_`) that is computed but
/// excluded from the output Arrow schema.
fn rewrite_sort_after_return(pipeline: &mut [PipelineOp]) {
    // Find the Return op and collect its projections
    let Some(return_idx) = pipeline
        .iter()
        .position(|op| matches!(op, PipelineOp::Return { .. }))
    else {
        return;
    };

    let mut all_projections: Vec<PlannedProjection> = match &pipeline[return_idx] {
        PipelineOp::Return { projections, .. } => projections.clone(),
        _ => unreachable!(),
    };

    // Collect all sort terms that need rewriting, along with their pipeline index
    let sort_ops: Vec<(usize, Vec<OrderTerm>)> = pipeline
        .iter()
        .enumerate()
        .skip(return_idx + 1)
        .filter_map(|(i, op)| match op {
            PipelineOp::Sort { terms } => Some((i, terms.clone())),
            PipelineOp::TopK { terms, .. } => Some((i, terms.clone())),
            _ => None,
        })
        .collect();

    if sort_ops.is_empty() {
        return;
    }

    // Rewrite each sort op's terms
    let mut rewrites: Vec<(usize, Vec<OrderTerm>)> = Vec::new();
    for (idx, terms) in sort_ops {
        let mut rewritten = Vec::with_capacity(terms.len());
        for term in &terms {
            // Check if ORDER BY expr matches a projected expression
            let mut matched = false;
            for proj in &all_projections {
                if expr_structurally_equal(&term.expr, &proj.expr) {
                    rewritten.push(OrderTerm {
                        expr: Expr::Var(proj.alias),
                        descending: term.descending,
                        nulls_first: term.nulls_first,
                    });
                    matched = true;
                    break;
                }
            }
            if matched {
                continue;
            }

            // Check if it's already a Var referencing an alias
            if let Expr::Var(name) = &term.expr
                && all_projections.iter().any(|p| p.alias == *name)
            {
                rewritten.push(term.clone());
                continue;
            }

            // Not projected -- add as hidden projection
            let hidden_alias = IStr::new(&format!("_sort_{}", all_projections.len()));
            all_projections.push(PlannedProjection {
                expr: term.expr.clone(),
                alias: hidden_alias,
            });
            rewritten.push(OrderTerm {
                expr: Expr::Var(hidden_alias),
                descending: term.descending,
                nulls_first: term.nulls_first,
            });
        }
        rewrites.push((idx, rewritten));
    }

    // Apply rewrites to pipeline
    for (idx, rewritten) in rewrites {
        match &mut pipeline[idx] {
            PipelineOp::Sort { terms } => *terms = rewritten,
            PipelineOp::TopK { terms, .. } => *terms = rewritten,
            _ => {}
        }
    }

    // Update Return projections with any hidden columns added
    if let PipelineOp::Return {
        projections: ret_projs,
        ..
    } = &mut pipeline[return_idx]
    {
        *ret_projs = all_projections;
    }
}

/// Structural equality check for Expr trees.
/// Uses IStr identity (interned u32 comparison) for variable and property names.
fn expr_structurally_equal(a: &Expr, b: &Expr) -> bool {
    match (a, b) {
        (Expr::Literal(va), Expr::Literal(vb)) => va.gql_eq(vb).is_true(),
        (Expr::Var(na), Expr::Var(nb)) => na == nb,
        (Expr::Property(ea, ka), Expr::Property(eb, kb)) => {
            ka == kb && expr_structurally_equal(ea, eb)
        }
        (Expr::Compare(la, opa, ra), Expr::Compare(lb, opb, rb)) => {
            opa == opb && expr_structurally_equal(la, lb) && expr_structurally_equal(ra, rb)
        }
        (Expr::Arithmetic(la, opa, ra), Expr::Arithmetic(lb, opb, rb)) => {
            opa == opb && expr_structurally_equal(la, lb) && expr_structurally_equal(ra, rb)
        }
        (Expr::Negate(ea), Expr::Negate(eb)) => expr_structurally_equal(ea, eb),
        (Expr::Not(ea), Expr::Not(eb)) => expr_structurally_equal(ea, eb),
        (Expr::Function(fa), Expr::Function(fb)) => {
            fa.name == fb.name
                && fa.args.len() == fb.args.len()
                && fa
                    .args
                    .iter()
                    .zip(fb.args.iter())
                    .all(|(a, b)| expr_structurally_equal(a, b))
        }
        (Expr::Aggregate(aa), Expr::Aggregate(ab)) => {
            aa.op == ab.op
                && match (&aa.expr, &ab.expr) {
                    (Some(ea), Some(eb)) => expr_structurally_equal(ea, eb),
                    (None, None) => true,
                    _ => false,
                }
        }
        _ => false,
    }
}

/// Detect if this is a pure `RETURN count(*)` query that can skip binding
/// materialization:
/// - Single LabelScan pattern (no Expand, VarExpand, Join)
/// - Pipeline has only RETURN (no Sort, TopK, Let, With, Call, etc.)
/// - Pipeline-level FILTERs are OK (we fall back to counting iteration)
/// - RETURN has exactly one projection: count(*)
/// - No GROUP BY, no HAVING, no DISTINCT
fn is_count_only_query(pattern_ops: &[PatternOp], pipeline_ops: &[PipelineOp]) -> bool {
    // Must be single label scan (no joins, expands)
    if pattern_ops.len() != 1 || !matches!(pattern_ops[0], PatternOp::LabelScan { .. }) {
        return false;
    }
    // Find the RETURN op -- must be the only non-Filter pipeline op
    let mut return_op = None;
    for op in pipeline_ops {
        match op {
            PipelineOp::Return { .. } => {
                if return_op.is_some() {
                    return false; // multiple RETURNs
                }
                return_op = Some(op);
            }
            PipelineOp::Filter { .. } => {} // filters are fine
            _ => return false,              // Sort, TopK, Let, With, Call, etc. disqualify
        }
    }
    let Some(PipelineOp::Return {
        projections,
        group_by,
        distinct,
        having,
        all,
    }) = return_op
    else {
        return false;
    };
    if *all || *distinct || !group_by.is_empty() || having.is_some() {
        return false;
    }
    if projections.len() != 1 {
        return false;
    }
    matches!(
        &projections[0].expr,
        Expr::Function(crate::ast::expr::FunctionCall {
            count_star: true,
            ..
        })
    )
}

fn derive_output_schema(ops: &[PipelineOp]) -> Arc<arrow::datatypes::Schema> {
    for op in ops {
        if let PipelineOp::Return { projections, .. } = op {
            let fields: Vec<arrow::datatypes::Field> = projections
                .iter()
                .filter(|p| !p.alias.as_str().starts_with("_sort_"))
                .map(|p| {
                    let dt = gql_type_to_arrow(&p.expr.infer_type());
                    arrow::datatypes::Field::new(p.alias.as_str(), dt, true)
                })
                .collect();
            return Arc::new(arrow::datatypes::Schema::new(fields));
        }
    }
    Arc::new(arrow::datatypes::Schema::empty())
}

/// Convert a GqlType to an Arrow DataType for schema derivation.
fn gql_type_to_arrow(t: &crate::types::value::GqlType) -> arrow::datatypes::DataType {
    use crate::types::value::GqlType;
    match t {
        GqlType::Bool => arrow::datatypes::DataType::Boolean,
        GqlType::Int => arrow::datatypes::DataType::Int64,
        GqlType::UInt => arrow::datatypes::DataType::UInt64,
        GqlType::Float => arrow::datatypes::DataType::Float64,
        _ => arrow::datatypes::DataType::Utf8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_statement;
    use selene_core::{LabelSet, NodeId, PropertyMap, Value};
    use smol_str::SmolStr;

    fn setup_graph() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(
            LabelSet::from_strs(&["building"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("HQ")))]),
        )
        .unwrap();
        m.create_node(LabelSet::from_strs(&["sensor"]), PropertyMap::new())
            .unwrap();
        m.create_node(LabelSet::from_strs(&["sensor"]), PropertyMap::new())
            .unwrap();
        m.create_edge(
            selene_core::NodeId(1),
            IStr::new("contains"),
            selene_core::NodeId(2),
            PropertyMap::new(),
        )
        .unwrap();
        m.create_edge(
            selene_core::NodeId(1),
            IStr::new("contains"),
            selene_core::NodeId(3),
            PropertyMap::new(),
        )
        .unwrap();
        m.commit(0).unwrap();
        g
    }

    #[test]
    fn plan_simple_match_return() {
        let g = setup_graph();
        let stmt = parse_statement("MATCH (n) RETURN n").unwrap();
        match stmt {
            GqlStatement::Query(pipeline) => {
                let plan = plan_query(&pipeline, &g).unwrap();
                assert_eq!(plan.pattern_ops.len(), 1);
                assert!(matches!(plan.pattern_ops[0], PatternOp::LabelScan { .. }));
                assert!(
                    plan.pipeline
                        .iter()
                        .any(|op| matches!(op, PipelineOp::Return { .. }))
                );
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_labeled_scan() {
        let g = setup_graph();
        let stmt = parse_statement("MATCH (s:sensor) RETURN s").unwrap();
        match stmt {
            GqlStatement::Query(pipeline) => {
                let plan = plan_query(&pipeline, &g).unwrap();
                match &plan.pattern_ops[0] {
                    PatternOp::LabelScan { var, labels, .. } => {
                        assert_eq!(var.as_str(), "S");
                        assert!(labels.is_some());
                    }
                    _ => panic!("expected LabelScan"),
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_edge_pattern() {
        let g = setup_graph();
        let stmt =
            parse_statement("MATCH (a:building)-[:contains]->(b:sensor) RETURN a, b").unwrap();
        match stmt {
            GqlStatement::Query(pipeline) => {
                let plan = plan_query(&pipeline, &g).unwrap();
                assert_eq!(plan.pattern_ops.len(), 2);
                assert!(matches!(plan.pattern_ops[0], PatternOp::LabelScan { .. }));
                match &plan.pattern_ops[1] {
                    PatternOp::Expand {
                        target_var,
                        target_labels,
                        direction,
                        ..
                    } => {
                        assert_eq!(target_var.as_str(), "B");
                        assert!(target_labels.is_some());
                        assert_eq!(*direction, EdgeDirection::Out);
                    }
                    _ => panic!("expected Expand"),
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_var_length() {
        let g = setup_graph();
        let stmt = parse_statement("MATCH (a)-[:contains]->{1,5}(b) RETURN a, b").unwrap();
        match stmt {
            GqlStatement::Query(pipeline) => {
                let plan = plan_query(&pipeline, &g).unwrap();
                match &plan.pattern_ops[1] {
                    PatternOp::VarExpand {
                        min_hops, max_hops, ..
                    } => {
                        assert_eq!(*min_hops, 1);
                        assert_eq!(*max_hops, Some(5));
                    }
                    _ => panic!("expected VarExpand"),
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_trail() {
        let g = setup_graph();
        let stmt = parse_statement("MATCH TRAIL (a)-[:contains]->{1,3}(b) RETURN a, b").unwrap();
        match stmt {
            GqlStatement::Query(pipeline) => {
                let plan = plan_query(&pipeline, &g).unwrap();
                match &plan.pattern_ops[1] {
                    PatternOp::VarExpand { trail, .. } => assert!(*trail),
                    _ => panic!("expected VarExpand"),
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_filter() {
        let g = setup_graph();
        let stmt = parse_statement("MATCH (s:sensor) FILTER s.temp > 72 RETURN s").unwrap();
        match stmt {
            GqlStatement::Query(pipeline) => {
                let plan = plan_query(&pipeline, &g).unwrap();
                // Inequality filter is pushed into property_filters on LabelScan by optimizer
                match &plan.pattern_ops[0] {
                    PatternOp::LabelScan {
                        property_filters, ..
                    } => {
                        assert_eq!(property_filters.len(), 1);
                    }
                    _ => panic!("expected LabelScan"),
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_let() {
        let g = setup_graph();
        let stmt = parse_statement("MATCH (n) LET x = n.id RETURN x").unwrap();
        match stmt {
            GqlStatement::Query(pipeline) => {
                let plan = plan_query(&pipeline, &g).unwrap();
                assert!(
                    plan.pipeline
                        .iter()
                        .any(|op| matches!(op, PipelineOp::Let { .. }))
                );
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_order_by_limit() {
        let g = setup_graph();
        let stmt = parse_statement("MATCH (n) RETURN n.id ORDER BY n.id DESC LIMIT 10").unwrap();
        match stmt {
            GqlStatement::Query(pipeline) => {
                let plan = plan_query(&pipeline, &g).unwrap();
                // Sort + Limit fused into TopK by optimizer
                assert!(
                    plan.pipeline
                        .iter()
                        .any(|op| matches!(op, PipelineOp::TopK { .. })),
                    "expected TopK, got: {:?}",
                    plan.pipeline
                );
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_return_alias() {
        let g = setup_graph();
        let stmt = parse_statement("MATCH (n) RETURN n.id AS node_id").unwrap();
        match stmt {
            GqlStatement::Query(pipeline) => {
                let plan = plan_query(&pipeline, &g).unwrap();
                match plan
                    .pipeline
                    .iter()
                    .find(|op| matches!(op, PipelineOp::Return { .. }))
                {
                    Some(PipelineOp::Return { projections, .. }) => {
                        assert_eq!(projections[0].alias.as_str(), "NODE_ID");
                    }
                    _ => panic!("expected Return"),
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_output_schema() {
        let g = setup_graph();
        let stmt = parse_statement("MATCH (n) RETURN n.id AS id, n.name AS name").unwrap();
        match stmt {
            GqlStatement::Query(pipeline) => {
                let plan = plan_query(&pipeline, &g).unwrap();
                assert_eq!(plan.output_schema.fields().len(), 2);
                assert_eq!(plan.output_schema.field(0).name(), "ID");
                assert_eq!(plan.output_schema.field(1).name(), "NAME");
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_call() {
        let g = setup_graph();
        let stmt = parse_statement(
            "MATCH (s:sensor) CALL ts.latest(s.id, 'temp') YIELD value AS v RETURN s, v",
        )
        .unwrap();
        match stmt {
            GqlStatement::Query(pipeline) => {
                let plan = plan_query(&pipeline, &g).unwrap();
                assert!(
                    plan.pipeline
                        .iter()
                        .any(|op| matches!(op, PipelineOp::Call { .. }))
                );
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn plan_mutation_insert() {
        let g = setup_graph();
        let stmt = parse_statement("INSERT (:sensor {name: 'new'})").unwrap();
        match stmt {
            GqlStatement::Mutate(mp) => {
                let plan = plan_mutation(&mp, &g).unwrap();
                assert_eq!(plan.mutations.len(), 1);
            }
            _ => panic!("expected Mutate"),
        }
    }

    #[test]
    fn plan_mutation_with_match() {
        let g = setup_graph();
        let stmt =
            parse_statement("MATCH (s:sensor) FILTER s.name = 'x' SET s.temp = 72.5 RETURN s")
                .unwrap();
        match stmt {
            GqlStatement::Mutate(mp) => {
                let plan = plan_mutation(&mp, &g).unwrap();
                assert!(!plan.pattern_ops.is_empty());
                assert_eq!(plan.mutations.len(), 1);
                assert!(
                    plan.pipeline
                        .iter()
                        .any(|op| matches!(op, PipelineOp::Return { .. }))
                );
            }
            _ => panic!("expected Mutate"),
        }
    }

    #[test]
    fn plan_cycle_pattern_emits_cycle_join() {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        // Triangle: a->b->c->a
        m.create_node(LabelSet::from_strs(&["x"]), PropertyMap::new())
            .unwrap();
        m.create_node(LabelSet::from_strs(&["x"]), PropertyMap::new())
            .unwrap();
        m.create_node(LabelSet::from_strs(&["x"]), PropertyMap::new())
            .unwrap();
        m.create_edge(NodeId(1), IStr::new("link"), NodeId(2), PropertyMap::new())
            .unwrap();
        m.create_edge(NodeId(2), IStr::new("link"), NodeId(3), PropertyMap::new())
            .unwrap();
        m.create_edge(NodeId(3), IStr::new("link"), NodeId(1), PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();

        // Execute a cycle query
        let result = crate::QueryBuilder::new(
            "MATCH (a:x)-[:link]->(b:x)-[:link]->(c:x)-[:link]->(a) RETURN a, b, c",
            &g,
        )
        .execute();
        let result = result.unwrap();
        // Should find exactly 3 triangle rotations: (1,2,3), (2,3,1), (3,1,2)
        assert_eq!(
            result.row_count(),
            3,
            "expected 3 triangle rotations, got {}",
            result.row_count()
        );
    }

    #[test]
    fn plan_no_cycle_works_normally() {
        let g = setup_graph();
        // Linear pattern -- no cycle
        let result =
            crate::QueryBuilder::new("MATCH (b:building)-[:contains]->(s:sensor) RETURN b, s", &g)
                .execute();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().row_count(), 2);
    }
}
