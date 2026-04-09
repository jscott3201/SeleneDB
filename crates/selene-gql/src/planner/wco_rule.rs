//! WCO join optimizer rule: detects triangle patterns and rewrites them
//! to a single `PatternOp::WcoJoin` for worst-case optimal execution.
//!
//! Detects the sequence: LabelScan + Expand + Expand + CycleJoin
//! and replaces it with a single WcoJoin when the estimated binary
//! intermediate size exceeds a threshold.

use selene_core::IStr;

use super::optimize::{GqlOptimizerRule, OptimizeContext, Transformed};
use super::plan::{ExecutionPlan, PatternOp, WcoRelation};
use crate::ast::pattern::EdgeDirection;
use crate::types::error::GqlError;

/// Minimum estimated binary intermediate rows to trigger WCO rewrite.
/// Below this threshold, binary joins are fast enough.
const WCO_REWRITE_THRESHOLD: u64 = 10_000;

#[derive(Debug)]
pub struct WcoJoinRule;

impl GqlOptimizerRule for WcoJoinRule {
    fn name(&self) -> &'static str {
        "WcoJoinRule"
    }

    fn rewrite(
        &self,
        mut plan: ExecutionPlan,
        ctx: &OptimizeContext<'_>,
    ) -> Result<Transformed<ExecutionPlan>, GqlError> {
        // Need graph for statistics-based cost estimation
        let Some(graph) = ctx.graph else {
            return Ok(Transformed::no(plan));
        };

        let ops = &plan.pattern_ops;
        if ops.len() < 4 {
            return Ok(Transformed::no(plan));
        }

        // Scan for triangle pattern: LabelScan + Expand + Expand + CycleJoin
        if let Some((start, triangle)) = detect_triangle(ops) {
            // Cost check: only rewrite if binary intermediate is large enough
            let binary_estimate = estimate_binary_intermediate(&triangle, graph);

            if binary_estimate < WCO_REWRITE_THRESHOLD {
                return Ok(Transformed::no(plan));
            }

            // Build WcoJoin op
            let wco_op = build_wco_op(&triangle);

            // Replace ops[start..start+4] with single WcoJoin.
            // DifferentEdgesFilter and other trailing ops are preserved below.
            let mut new_ops = Vec::with_capacity(plan.pattern_ops.len() - 3);
            new_ops.extend(plan.pattern_ops.drain(..start));
            plan.pattern_ops.drain(..4); // remove the 4 ops
            new_ops.push(wco_op);

            // Adjust Join right_start/right_end for subsequent ops
            let offset: i64 = -3; // 4 ops became 1
            for op in plan.pattern_ops.drain(..) {
                match op {
                    PatternOp::Join {
                        right_start,
                        right_end,
                        join_vars,
                    } => {
                        new_ops.push(PatternOp::Join {
                            right_start: (right_start as i64 + offset).max(0) as usize,
                            right_end: (right_end as i64 + offset).max(0) as usize,
                            join_vars,
                        });
                    }
                    other => new_ops.push(other),
                }
            }

            plan.pattern_ops = new_ops;
            return Ok(Transformed::yes(plan));
        }

        Ok(Transformed::no(plan))
    }
}

/// Detected triangle pattern with all relevant information.
struct TrianglePattern {
    scan_var: IStr,
    scan_labels: Option<crate::ast::pattern::LabelExpr>,
    scan_property_filters: Vec<crate::pattern::scan::PropertyFilter>,

    // Relation 0: scan_var -> mid_var
    mid_var: IStr,
    rel0_edge_var: Option<IStr>,
    rel0_edge_labels: Option<crate::ast::pattern::LabelExpr>,
    rel0_target_labels: Option<crate::ast::pattern::LabelExpr>,
    rel0_direction: EdgeDirection,
    rel0_target_property_filters: Vec<crate::pattern::scan::PropertyFilter>,

    // Relation 1: mid_var -> end_var
    end_var: IStr,
    rel1_edge_var: Option<IStr>,
    rel1_edge_labels: Option<crate::ast::pattern::LabelExpr>,
    rel1_target_labels: Option<crate::ast::pattern::LabelExpr>,
    rel1_direction: EdgeDirection,
    rel1_target_property_filters: Vec<crate::pattern::scan::PropertyFilter>,

    // Relation 2 (closing): end_var -> scan_var
    rel2_edge_labels: Option<crate::ast::pattern::LabelExpr>,
    rel2_direction: EdgeDirection,
}

/// Detect a triangle pattern starting at any position in the op sequence.
fn detect_triangle(ops: &[PatternOp]) -> Option<(usize, TrianglePattern)> {
    for i in 0..ops.len().saturating_sub(3) {
        // ops[i] = LabelScan
        let PatternOp::LabelScan {
            var: scan_var,
            labels: scan_labels,
            property_filters: scan_pf,
            ..
        } = &ops[i]
        else {
            continue;
        };

        // ops[i+1] = Expand from scan_var
        let PatternOp::Expand {
            source_var: e0_source,
            edge_var: e0_edge_var,
            target_var: e0_target,
            edge_labels: e0_edge_labels,
            target_labels: e0_target_labels,
            direction: e0_dir,
            target_property_filters: e0_tpf,
            edge_property_filters: e0_epf,
        } = &ops[i + 1]
        else {
            continue;
        };
        if e0_source != scan_var {
            continue;
        }
        // Skip if edge property filters are present (not yet supported in WCO)
        if !e0_epf.is_empty() {
            continue;
        }

        // ops[i+2] = Expand from mid_var
        let PatternOp::Expand {
            source_var: e1_source,
            edge_var: e1_edge_var,
            target_var: e1_target,
            edge_labels: e1_edge_labels,
            target_labels: e1_target_labels,
            direction: e1_dir,
            target_property_filters: e1_tpf,
            edge_property_filters: e1_epf,
        } = &ops[i + 2]
        else {
            continue;
        };
        if e1_source != e0_target {
            continue;
        }
        if !e1_epf.is_empty() {
            continue;
        }

        // ops[i+3] = CycleJoin closing back to scan_var
        let PatternOp::CycleJoin {
            bound_var,
            source_var: cj_source,
            edge_labels: cj_edge_labels,
            direction: cj_dir,
        } = &ops[i + 3]
        else {
            continue;
        };
        if bound_var != scan_var || cj_source != e1_target {
            continue;
        }

        return Some((
            i,
            TrianglePattern {
                scan_var: *scan_var,
                scan_labels: scan_labels.clone(),
                scan_property_filters: scan_pf.clone(),

                mid_var: *e0_target,
                rel0_edge_var: *e0_edge_var,
                rel0_edge_labels: e0_edge_labels.clone(),
                rel0_target_labels: e0_target_labels.clone(),
                rel0_direction: *e0_dir,
                rel0_target_property_filters: e0_tpf.clone(),

                end_var: *e1_target,
                rel1_edge_var: *e1_edge_var,
                rel1_edge_labels: e1_edge_labels.clone(),
                rel1_target_labels: e1_target_labels.clone(),
                rel1_direction: *e1_dir,
                rel1_target_property_filters: e1_tpf.clone(),

                rel2_edge_labels: cj_edge_labels.clone(),
                rel2_direction: *cj_dir,
            },
        ));
    }
    None
}

/// Extract a simple label name from a LabelExpr for WcoRelation.
fn extract_simple_label(expr: Option<&crate::ast::pattern::LabelExpr>) -> Option<IStr> {
    match expr {
        Some(crate::ast::pattern::LabelExpr::Name(name)) => Some(*name),
        _ => None,
    }
}

/// Estimate the intermediate result size of binary joins for this triangle.
///
/// Binary path: scan(a) * avg_degree(a->b) * avg_degree(b->c)
/// This is the number of (a,b,c) triples before the CycleJoin filter.
///
/// Uses graph node/edge counts as a rough proxy when per-label stats
/// are not available.
fn estimate_binary_intermediate(
    _triangle: &TrianglePattern,
    graph: &selene_graph::SeleneGraph,
) -> u64 {
    // Rough estimation: total_edges / max_node gives average degree.
    // Binary intermediate = nodes * avg_degree^2.
    let nodes = graph.max_node_id().max(1) as f64;
    let edges = graph.all_edge_bitmap().len() as f64;
    let avg_degree = if nodes > 0.0 {
        edges / nodes
    } else {
        return 0;
    };

    (nodes * avg_degree * avg_degree) as u64
}

/// Build the WcoJoin PatternOp from a detected triangle pattern.
fn build_wco_op(t: &TrianglePattern) -> PatternOp {
    PatternOp::WcoJoin {
        scan_var: t.scan_var,
        scan_labels: t.scan_labels.clone(),
        scan_property_filters: t.scan_property_filters.clone(),
        relations: vec![
            WcoRelation {
                source_var: t.scan_var,
                edge_var: t.rel0_edge_var,
                target_var: t.mid_var,
                edge_label: extract_simple_label(t.rel0_edge_labels.as_ref()),
                target_labels: t.rel0_target_labels.clone(),
                direction: t.rel0_direction,
                target_property_filters: t.rel0_target_property_filters.clone(),
            },
            WcoRelation {
                source_var: t.mid_var,
                edge_var: t.rel1_edge_var,
                target_var: t.end_var,
                edge_label: extract_simple_label(t.rel1_edge_labels.as_ref()),
                target_labels: t.rel1_target_labels.clone(),
                direction: t.rel1_direction,
                target_property_filters: t.rel1_target_property_filters.clone(),
            },
            WcoRelation {
                source_var: t.end_var,
                edge_var: None, // CycleJoin doesn't bind an edge variable
                target_var: t.scan_var,
                edge_label: extract_simple_label(t.rel2_edge_labels.as_ref()),
                target_labels: None,
                direction: t.rel2_direction,
                target_property_filters: vec![],
            },
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::pattern::LabelExpr;
    use crate::planner::plan::{ExecutionPlan, PatternOp, PipelineOp, PlannedProjection};
    use selene_core::{IStr, LabelSet, PropertyMap};
    use selene_graph::SeleneGraph;

    /// Build a graph large enough to exceed WCO_REWRITE_THRESHOLD.
    /// Creates a clique of `n` nodes (all connected to each other),
    /// yielding n*(n-1) directed edges.
    fn build_large_triangle_graph(n: usize) -> SeleneGraph {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        let mut ids = Vec::with_capacity(n);
        for _ in 0..n {
            ids.push(
                m.create_node(LabelSet::from_strs(&["N"]), PropertyMap::new())
                    .unwrap(),
            );
        }
        let edge_label = IStr::new("E");
        for i in 0..n {
            for j in 0..n {
                if i != j {
                    m.create_edge(ids[i], edge_label, ids[j], PropertyMap::new())
                        .unwrap();
                }
            }
        }
        m.commit(0).unwrap();
        g
    }

    /// Build a triangle pattern plan: LabelScan(a) -> Expand(a->b) -> Expand(b->c) -> CycleJoin(c->a).
    fn make_triangle_plan() -> ExecutionPlan {
        let a = IStr::new("A");
        let b = IStr::new("B");
        let c = IStr::new("C");
        let label_n = LabelExpr::Name(IStr::new("N"));
        let edge_label = LabelExpr::Name(IStr::new("E"));
        ExecutionPlan {
            pattern_ops: vec![
                PatternOp::LabelScan {
                    var: a,
                    labels: Some(label_n.clone()),
                    inline_props: vec![],
                    property_filters: vec![],
                    index_order: None,
                    composite_index_keys: None,
                    range_index_hint: None,
                    in_list_hint: None,
                },
                PatternOp::Expand {
                    source_var: a,
                    edge_var: Some(IStr::new("E0")),
                    target_var: b,
                    edge_labels: Some(edge_label.clone()),
                    target_labels: Some(label_n.clone()),
                    direction: EdgeDirection::Out,
                    target_property_filters: vec![],
                    edge_property_filters: vec![],
                },
                PatternOp::Expand {
                    source_var: b,
                    edge_var: Some(IStr::new("E1")),
                    target_var: c,
                    edge_labels: Some(edge_label.clone()),
                    target_labels: Some(label_n.clone()),
                    direction: EdgeDirection::Out,
                    target_property_filters: vec![],
                    edge_property_filters: vec![],
                },
                PatternOp::CycleJoin {
                    bound_var: a,
                    source_var: c,
                    edge_labels: Some(edge_label),
                    direction: EdgeDirection::Out,
                },
            ],
            pipeline: vec![PipelineOp::Return {
                projections: vec![PlannedProjection {
                    expr: crate::ast::expr::Expr::Var(a),
                    alias: a,
                    display_name: a,
                }],
                group_by: vec![],
                distinct: false,
                having: None,
                all: false,
            }],
            mutations: vec![],
            output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
            display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
            count_only: false,
        }
    }

    #[test]
    fn wco_fires_on_large_triangle_graph() {
        let g = build_large_triangle_graph(25); // 25*24 = 600 edges, estimate > 10k
        let plan = make_triangle_plan();
        let ctx = OptimizeContext::new(&g);
        let rule = WcoJoinRule;
        let result = rule.rewrite(plan, &ctx).unwrap();
        assert!(
            result.changed,
            "triangle on large graph should trigger WCO rewrite"
        );
        assert!(
            result
                .data
                .pattern_ops
                .iter()
                .any(|op| matches!(op, PatternOp::WcoJoin { .. })),
            "should produce WcoJoin op"
        );
        // The 4 original ops should be collapsed to 1 WcoJoin
        let wco_count = result
            .data
            .pattern_ops
            .iter()
            .filter(|op| matches!(op, PatternOp::WcoJoin { .. }))
            .count();
        assert_eq!(wco_count, 1, "exactly one WcoJoin");
    }

    #[test]
    fn wco_skips_small_graph() {
        // 3 nodes, 6 edges: binary estimate = 3 * (6/3)^2 = 12, above threshold.
        // Use 2 nodes: 2 edges, estimate = 2 * 1 = 2, below threshold.
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        let n1 = m
            .create_node(LabelSet::from_strs(&["N"]), PropertyMap::new())
            .unwrap();
        let n2 = m
            .create_node(LabelSet::from_strs(&["N"]), PropertyMap::new())
            .unwrap();
        m.create_edge(n1, IStr::new("E"), n2, PropertyMap::new())
            .unwrap();
        m.create_edge(n2, IStr::new("E"), n1, PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();

        let plan = make_triangle_plan();
        let ctx = OptimizeContext::new(&g);
        let rule = WcoJoinRule;
        let result = rule.rewrite(plan, &ctx).unwrap();
        assert!(
            !result.changed,
            "small graph should not trigger WCO (below threshold)"
        );
    }

    #[test]
    fn wco_skips_no_graph_context() {
        let plan = make_triangle_plan();
        let ctx = OptimizeContext { graph: None };
        let rule = WcoJoinRule;
        let result = rule.rewrite(plan, &ctx).unwrap();
        assert!(
            !result.changed,
            "no graph context means no statistics for cost estimation"
        );
    }

    #[test]
    fn wco_skips_fewer_than_four_ops() {
        // Plan with only 2 ops: LabelScan + Expand (no cycle)
        let a = IStr::new("A");
        let b = IStr::new("B");
        let plan = ExecutionPlan {
            pattern_ops: vec![
                PatternOp::LabelScan {
                    var: a,
                    labels: Some(LabelExpr::Name(IStr::new("N"))),
                    inline_props: vec![],
                    property_filters: vec![],
                    index_order: None,
                    composite_index_keys: None,
                    range_index_hint: None,
                    in_list_hint: None,
                },
                PatternOp::Expand {
                    source_var: a,
                    edge_var: None,
                    target_var: b,
                    edge_labels: None,
                    target_labels: None,
                    direction: EdgeDirection::Out,
                    target_property_filters: vec![],
                    edge_property_filters: vec![],
                },
            ],
            pipeline: vec![],
            mutations: vec![],
            output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
            display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
            count_only: false,
        };
        let g = build_large_triangle_graph(20);
        let ctx = OptimizeContext::new(&g);
        let rule = WcoJoinRule;
        let result = rule.rewrite(plan, &ctx).unwrap();
        assert!(
            !result.changed,
            "fewer than 4 ops means no triangle pattern"
        );
    }

    #[test]
    fn wco_skips_non_triangle_pattern() {
        // 4 ops but not a triangle: LabelScan + Expand + Expand + Expand (linear, no CycleJoin)
        let a = IStr::new("A");
        let b = IStr::new("B");
        let c = IStr::new("C");
        let d = IStr::new("D");
        let plan = ExecutionPlan {
            pattern_ops: vec![
                PatternOp::LabelScan {
                    var: a,
                    labels: Some(LabelExpr::Name(IStr::new("N"))),
                    inline_props: vec![],
                    property_filters: vec![],
                    index_order: None,
                    composite_index_keys: None,
                    range_index_hint: None,
                    in_list_hint: None,
                },
                PatternOp::Expand {
                    source_var: a,
                    edge_var: None,
                    target_var: b,
                    edge_labels: None,
                    target_labels: None,
                    direction: EdgeDirection::Out,
                    target_property_filters: vec![],
                    edge_property_filters: vec![],
                },
                PatternOp::Expand {
                    source_var: b,
                    edge_var: None,
                    target_var: c,
                    edge_labels: None,
                    target_labels: None,
                    direction: EdgeDirection::Out,
                    target_property_filters: vec![],
                    edge_property_filters: vec![],
                },
                PatternOp::Expand {
                    source_var: c,
                    edge_var: None,
                    target_var: d,
                    edge_labels: None,
                    target_labels: None,
                    direction: EdgeDirection::Out,
                    target_property_filters: vec![],
                    edge_property_filters: vec![],
                },
            ],
            pipeline: vec![],
            mutations: vec![],
            output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
            display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
            count_only: false,
        };
        let g = build_large_triangle_graph(20);
        let ctx = OptimizeContext::new(&g);
        let rule = WcoJoinRule;
        let result = rule.rewrite(plan, &ctx).unwrap();
        assert!(
            !result.changed,
            "linear chain without CycleJoin is not a triangle"
        );
    }

    #[test]
    fn wco_skips_edge_property_filters() {
        // Triangle pattern but first Expand has edge property filters
        let a = IStr::new("A");
        let b = IStr::new("B");
        let c = IStr::new("C");
        let label_n = LabelExpr::Name(IStr::new("N"));
        let edge_label = LabelExpr::Name(IStr::new("E"));
        let plan = ExecutionPlan {
            pattern_ops: vec![
                PatternOp::LabelScan {
                    var: a,
                    labels: Some(label_n.clone()),
                    inline_props: vec![],
                    property_filters: vec![],
                    index_order: None,
                    composite_index_keys: None,
                    range_index_hint: None,
                    in_list_hint: None,
                },
                PatternOp::Expand {
                    source_var: a,
                    edge_var: Some(IStr::new("E0")),
                    target_var: b,
                    edge_labels: Some(edge_label.clone()),
                    target_labels: Some(label_n.clone()),
                    direction: EdgeDirection::Out,
                    target_property_filters: vec![],
                    edge_property_filters: vec![crate::pattern::scan::PropertyFilter {
                        key: IStr::new("weight"),
                        op: crate::ast::expr::CompareOp::Gt,
                        value: crate::types::value::GqlValue::Float(0.5),
                    }],
                },
                PatternOp::Expand {
                    source_var: b,
                    edge_var: Some(IStr::new("E1")),
                    target_var: c,
                    edge_labels: Some(edge_label.clone()),
                    target_labels: Some(label_n),
                    direction: EdgeDirection::Out,
                    target_property_filters: vec![],
                    edge_property_filters: vec![],
                },
                PatternOp::CycleJoin {
                    bound_var: a,
                    source_var: c,
                    edge_labels: Some(edge_label),
                    direction: EdgeDirection::Out,
                },
            ],
            pipeline: vec![],
            mutations: vec![],
            output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
            display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
            count_only: false,
        };
        let g = build_large_triangle_graph(20);
        let ctx = OptimizeContext::new(&g);
        let rule = WcoJoinRule;
        let result = rule.rewrite(plan, &ctx).unwrap();
        assert!(
            !result.changed,
            "edge property filters on Expand should prevent WCO rewrite"
        );
    }

    #[test]
    fn wco_preserves_trailing_ops() {
        // Triangle pattern followed by DifferentEdgesFilter: trailing op should survive.
        let g = build_large_triangle_graph(25);
        let mut plan = make_triangle_plan();
        plan.pattern_ops.push(PatternOp::DifferentEdgesFilter {
            edge_vars: vec![IStr::new("E0"), IStr::new("E1")],
        });
        let ctx = OptimizeContext::new(&g);
        let rule = WcoJoinRule;
        let result = rule.rewrite(plan, &ctx).unwrap();
        assert!(result.changed);
        // Should have WcoJoin + DifferentEdgesFilter
        assert!(
            result
                .data
                .pattern_ops
                .iter()
                .any(|op| matches!(op, PatternOp::DifferentEdgesFilter { .. })),
            "trailing DifferentEdgesFilter should be preserved"
        );
    }
}
