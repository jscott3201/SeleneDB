use super::*;
use crate::ast::expr::{CompareOp, Expr};
use crate::ast::pattern::{EdgeDirection, LabelExpr, PathMode};
use crate::parser::parse_statement;
use crate::pattern::scan::PropertyFilter;
use crate::planner;
use crate::types::value::GqlValue;
use selene_core::{IStr, LabelSet, PropertyMap, Value};
use selene_graph::SeleneGraph;
use smol_str::SmolStr;

fn setup_graph() -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    m.create_node(
        LabelSet::from_strs(&["sensor"]),
        PropertyMap::from_pairs(vec![
            (selene_core::IStr::new("temp"), Value::Float(72.5)),
            (selene_core::IStr::new("unit"), Value::str("°F")),
        ]),
    )
    .unwrap();
    m.create_node(
        LabelSet::from_strs(&["sensor"]),
        PropertyMap::from_pairs(vec![
            (selene_core::IStr::new("temp"), Value::Float(80.0)),
            (selene_core::IStr::new("unit"), Value::str("°C")),
        ]),
    )
    .unwrap();
    m.commit(0).unwrap();
    g
}

fn plan_for(gql: &str, g: &SeleneGraph) -> ExecutionPlan {
    let stmt = parse_statement(gql).unwrap();
    match stmt {
        crate::ast::statement::GqlStatement::Query(pipeline) => {
            planner::plan_query(&pipeline, g).unwrap()
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn empty_optimizer_passthrough() {
    let g = setup_graph();
    let plan = plan_for("MATCH (s:sensor) FILTER s.temp > 72 RETURN s", &g);
    let pipeline_len_before = plan.pipeline.len();
    let opt = GqlOptimizer::empty();
    let plan = opt.optimize(plan, &OptimizeContext::empty()).unwrap();
    assert_eq!(plan.pipeline.len(), pipeline_len_before);
}

#[test]
fn constant_fold_arithmetic() {
    let g = setup_graph();
    let plan = plan_for("MATCH (s:sensor) FILTER s.temp > 70 + 2 RETURN s", &g);
    let opt = GqlOptimizer::with_default_rules();
    let plan = opt.optimize(plan, &OptimizeContext::empty()).unwrap();
    // After folding 70+2→72, the filter should contain a literal
    // (it may also be pushed down into property_filters)
    let has_literal = plan.pattern_ops.iter().any(|op| match op {
        PatternOp::LabelScan {
            property_filters, ..
        } => !property_filters.is_empty(),
        _ => false,
    });
    assert!(
        has_literal
            || !plan
                .pipeline
                .iter()
                .any(|op| matches!(op, PipelineOp::Filter { .. }))
    );
}

#[test]
fn and_splitting_enables_pushdown() {
    let g = setup_graph();
    let plan = plan_for(
        "MATCH (s:sensor) FILTER s.temp > 72 AND s.unit = '°F' RETURN s",
        &g,
    );
    let opt = GqlOptimizer::with_default_rules();
    let plan = opt.optimize(plan, &OptimizeContext::empty()).unwrap();
    // Both filters should be pushed into LabelScan
    match &plan.pattern_ops[0] {
        PatternOp::LabelScan {
            inline_props,
            property_filters,
            ..
        } => {
            let total_pushed = inline_props.len() + property_filters.len();
            assert!(
                total_pushed >= 2,
                "both filters should be pushed: inline={}, range={}",
                inline_props.len(),
                property_filters.len()
            );
        }
        _ => panic!("expected LabelScan"),
    }
    // No filters should remain in pipeline
    assert!(
        !plan
            .pipeline
            .iter()
            .any(|op| matches!(op, PipelineOp::Filter { .. })),
        "all filters should be pushed: {:?}",
        plan.pipeline
    );
}

#[test]
fn filter_pushdown_equality() {
    let g = setup_graph();
    let plan = plan_for("MATCH (s:sensor) FILTER s.temp = 72.5 RETURN s", &g);
    let opt = GqlOptimizer::with_default_rules();
    let plan = opt.optimize(plan, &OptimizeContext::empty()).unwrap();
    assert!(
        !plan
            .pipeline
            .iter()
            .any(|op| matches!(op, PipelineOp::Filter { .. }))
    );
    match &plan.pattern_ops[0] {
        PatternOp::LabelScan {
            property_filters, ..
        } => {
            assert_eq!(property_filters.len(), 1);
            assert_eq!(property_filters[0].op, CompareOp::Eq);
        }
        _ => panic!("expected LabelScan"),
    }
}

#[test]
fn filter_pushdown_inequality_into_range_index_hint() {
    let g = setup_graph();
    let plan = plan_for("MATCH (s:sensor) FILTER s.temp > 72 RETURN s", &g);
    let opt = GqlOptimizer::with_default_rules();
    let plan = opt.optimize(plan, &OptimizeContext::empty()).unwrap();
    assert!(
        !plan
            .pipeline
            .iter()
            .any(|op| matches!(op, PipelineOp::Filter { .. }))
    );
    match &plan.pattern_ops[0] {
        PatternOp::LabelScan {
            property_filters,
            range_index_hint,
            ..
        } => {
            // Range filter should be promoted to range_index_hint
            assert!(
                range_index_hint.is_some(),
                "range filter should produce a range_index_hint"
            );
            let hint = range_index_hint.as_ref().unwrap();
            assert!(hint.lower.is_some(), "should have a lower bound");
            assert!(hint.upper.is_none(), "should not have an upper bound");
            let (_, inclusive) = hint.lower.as_ref().unwrap();
            assert!(!inclusive, "> is exclusive lower bound");
            // The range filter stays in property_filters as fallback
            assert_eq!(property_filters.len(), 1);
            assert_eq!(property_filters[0].op, CompareOp::Gt);
        }
        _ => panic!("expected LabelScan"),
    }
}

#[test]
fn topk_fuses_sort_and_limit() {
    let g = setup_graph();
    let plan = plan_for(
        "MATCH (s:sensor) RETURN s.temp AS temp ORDER BY s.temp DESC LIMIT 10",
        &g,
    );
    let opt = GqlOptimizer::with_default_rules();
    let plan = opt.optimize(plan, &OptimizeContext::empty()).unwrap();
    assert!(
        plan.pipeline
            .iter()
            .any(|op| matches!(op, PipelineOp::TopK { .. })),
        "Sort + Limit should be fused into TopK"
    );
    assert!(
        !plan
            .pipeline
            .iter()
            .any(|op| matches!(op, PipelineOp::Limit { .. })),
        "Limit should be consumed by TopK"
    );
}

#[test]
fn fixed_point_converges() {
    let g = setup_graph();
    // Needs two passes: AND split → filter pushdown
    let plan = plan_for(
        "MATCH (s:sensor) FILTER s.temp > 70 AND s.unit = '°F' RETURN s",
        &g,
    );
    let opt = GqlOptimizer::with_default_rules();
    let plan = opt.optimize(plan, &OptimizeContext::empty()).unwrap();
    assert!(
        !plan
            .pipeline
            .iter()
            .any(|op| matches!(op, PipelineOp::Filter { .. })),
        "fixed-point should push all filters: {:?}",
        plan.pipeline
    );
}

// ── Symmetry Breaking tests ─────────────────────────────────

/// Build a graph with Person nodes connected by undirected FRIEND edges.
fn setup_person_graph() -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    let p1 = m
        .create_node(
            LabelSet::from_strs(&["Person"]),
            PropertyMap::from_pairs(vec![(selene_core::IStr::new("name"), Value::str("Alice"))]),
        )
        .unwrap();
    let p2 = m
        .create_node(
            LabelSet::from_strs(&["Person"]),
            PropertyMap::from_pairs(vec![(selene_core::IStr::new("name"), Value::str("Bob"))]),
        )
        .unwrap();
    // Edge: p1 -[:FRIEND]-> p2 (stored as directed, matched undirected)
    m.create_edge(p1, selene_core::IStr::new("FRIEND"), p2, PropertyMap::new())
        .unwrap();
    // Also create a Room node (different label, for negative test)
    m.create_node(
        LabelSet::from_strs(&["Room"]),
        PropertyMap::from_pairs(vec![(selene_core::IStr::new("name"), Value::str("Lobby"))]),
    )
    .unwrap();
    m.commit(0).unwrap();
    g
}

/// Helper: count Filter ops in the pipeline that contain an id comparison.
fn count_id_filters(plan: &ExecutionPlan) -> usize {
    plan.pipeline
        .iter()
        .filter(|op| {
            if let PipelineOp::Filter { predicate } = op {
                // Check for a.id < b.id pattern
                matches!(predicate, Expr::Compare(left, CompareOp::Lt, right)
                    if matches!(left.as_ref(), Expr::Property(_, key) if key.as_str() == "id")
                    && matches!(right.as_ref(), Expr::Property(_, key) if key.as_str() == "id"))
            } else {
                false
            }
        })
        .count()
}

#[test]
fn symmetry_breaking_injects_filter_for_undirected_same_label() {
    let g = setup_person_graph();
    // Undirected edge between two Person nodes -- should get symmetry-breaking filter.
    let plan = plan_for("MATCH (a:Person)-[:FRIEND]-(b:Person) RETURN a, b", &g);
    assert_eq!(
        count_id_filters(&plan),
        1,
        "should inject exactly one id comparison filter for undirected same-label pattern"
    );
}

#[test]
fn symmetry_breaking_skips_directed_edge() {
    let g = setup_person_graph();
    // Directed edge -- should NOT get symmetry-breaking filter.
    let plan = plan_for("MATCH (a:Person)-[:FRIEND]->(b:Person) RETURN a, b", &g);
    assert_eq!(
        count_id_filters(&plan),
        0,
        "should not inject filter for directed pattern"
    );
}

#[test]
fn symmetry_breaking_skips_different_labels() {
    let g = setup_person_graph();
    // Undirected edge between different labels -- should NOT trigger.
    let plan = plan_for("MATCH (a:Person)-[:FRIEND]-(b:Room) RETURN a, b", &g);
    assert_eq!(
        count_id_filters(&plan),
        0,
        "should not inject filter when endpoint labels differ"
    );
}

// ── IndexOrderRule tests ──────────────────────────────────────

/// Helper: build an ExecutionPlan with a single LabelScan and
/// a TopK in the pipeline (simulates post-TopKRule state).
fn make_index_order_plan(sort_key: &str, descending: bool, limit_n: u64) -> ExecutionPlan {
    let scan_var = IStr::new("S");
    let key = IStr::new(sort_key);
    ExecutionPlan {
        pattern_ops: vec![PatternOp::LabelScan {
            var: scan_var,
            labels: Some(LabelExpr::Name(IStr::new("sensor"))),
            inline_props: vec![],
            property_filters: vec![],
            index_order: None,
            composite_index_keys: None,
            range_index_hint: None,
            in_list_hint: None,
        }],
        pipeline: vec![PipelineOp::TopK {
            terms: vec![crate::ast::statement::OrderTerm {
                expr: Expr::Property(Box::new(Expr::Var(scan_var)), key),
                descending,
                nulls_first: None,
            }],
            limit: crate::ast::statement::LimitValue::Literal(limit_n),
        }],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    }
}

#[test]
fn index_order_fires_on_single_scan_with_topk() {
    let plan = make_index_order_plan("temp", true, 10);
    let rule = IndexOrderRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(
        result.changed,
        "rule should fire for single LabelScan + TopK"
    );
    match &result.data.pattern_ops[0] {
        PatternOp::LabelScan { index_order, .. } => {
            let io = index_order.as_ref().expect("index_order should be set");
            assert_eq!(io.key.as_str(), "temp");
            assert!(io.descending);
            assert_eq!(io.limit, 10);
        }
        _ => panic!("expected LabelScan"),
    }
}

#[test]
fn index_order_ascending() {
    let plan = make_index_order_plan("name", false, 5);
    let rule = IndexOrderRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(result.changed);
    match &result.data.pattern_ops[0] {
        PatternOp::LabelScan { index_order, .. } => {
            let io = index_order.as_ref().unwrap();
            assert!(!io.descending, "should preserve ascending sort");
            assert_eq!(io.limit, 5);
        }
        _ => panic!("expected LabelScan"),
    }
}

#[test]
fn index_order_skips_multiple_scans() {
    // Two LabelScans: rule should not fire
    let scan_var = IStr::new("S");
    let plan = ExecutionPlan {
        pattern_ops: vec![
            PatternOp::LabelScan {
                var: scan_var,
                labels: Some(LabelExpr::Name(IStr::new("sensor"))),
                inline_props: vec![],
                property_filters: vec![],
                index_order: None,
                composite_index_keys: None,
                range_index_hint: None,
                in_list_hint: None,
            },
            PatternOp::LabelScan {
                var: IStr::new("T"),
                labels: Some(LabelExpr::Name(IStr::new("room"))),
                inline_props: vec![],
                property_filters: vec![],
                index_order: None,
                composite_index_keys: None,
                range_index_hint: None,
                in_list_hint: None,
            },
        ],
        pipeline: vec![PipelineOp::TopK {
            terms: vec![crate::ast::statement::OrderTerm {
                expr: Expr::Property(Box::new(Expr::Var(scan_var)), IStr::new("temp")),
                descending: true,
                nulls_first: None,
            }],
            limit: crate::ast::statement::LimitValue::Literal(10),
        }],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = IndexOrderRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(
        !result.changed,
        "rule should not fire with multiple pattern ops"
    );
}

#[test]
fn index_order_skips_no_topk() {
    let scan_var = IStr::new("S");
    let plan = ExecutionPlan {
        pattern_ops: vec![PatternOp::LabelScan {
            var: scan_var,
            labels: Some(LabelExpr::Name(IStr::new("sensor"))),
            inline_props: vec![],
            property_filters: vec![],
            index_order: None,
            composite_index_keys: None,
            range_index_hint: None,
            in_list_hint: None,
        }],
        pipeline: vec![PipelineOp::Filter {
            predicate: Expr::Literal(GqlValue::Bool(true)),
        }],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = IndexOrderRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(!result.changed, "no TopK in pipeline means no rewrite");
}

#[test]
fn index_order_skips_multi_term_topk() {
    // TopK with two sort terms: rule requires exactly one term.
    let scan_var = IStr::new("S");
    let plan = ExecutionPlan {
        pattern_ops: vec![PatternOp::LabelScan {
            var: scan_var,
            labels: Some(LabelExpr::Name(IStr::new("sensor"))),
            inline_props: vec![],
            property_filters: vec![],
            index_order: None,
            composite_index_keys: None,
            range_index_hint: None,
            in_list_hint: None,
        }],
        pipeline: vec![PipelineOp::TopK {
            terms: vec![
                crate::ast::statement::OrderTerm {
                    expr: Expr::Property(Box::new(Expr::Var(scan_var)), IStr::new("temp")),
                    descending: true,
                    nulls_first: None,
                },
                crate::ast::statement::OrderTerm {
                    expr: Expr::Property(Box::new(Expr::Var(scan_var)), IStr::new("name")),
                    descending: false,
                    nulls_first: None,
                },
            ],
            limit: crate::ast::statement::LimitValue::Literal(10),
        }],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = IndexOrderRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(
        !result.changed,
        "multi-term TopK should not trigger index order"
    );
}

#[test]
fn index_order_skips_inline_props() {
    // LabelScan with inline_props: rule should bail out.
    let scan_var = IStr::new("S");
    let plan = ExecutionPlan {
        pattern_ops: vec![PatternOp::LabelScan {
            var: scan_var,
            labels: Some(LabelExpr::Name(IStr::new("sensor"))),
            inline_props: vec![(
                IStr::new("unit"),
                Expr::Literal(GqlValue::String(SmolStr::new("F"))),
            )],
            property_filters: vec![],
            index_order: None,
            composite_index_keys: None,
            range_index_hint: None,
            in_list_hint: None,
        }],
        pipeline: vec![PipelineOp::TopK {
            terms: vec![crate::ast::statement::OrderTerm {
                expr: Expr::Property(Box::new(Expr::Var(scan_var)), IStr::new("temp")),
                descending: true,
                nulls_first: None,
            }],
            limit: crate::ast::statement::LimitValue::Literal(10),
        }],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = IndexOrderRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(!result.changed, "inline_props should prevent index order");
}

#[test]
fn index_order_idempotent() {
    // Apply twice: second application should be a no-op.
    let plan = make_index_order_plan("temp", true, 10);
    let rule = IndexOrderRule;
    let first = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(first.changed);
    let second = rule.rewrite(first.data, &OptimizeContext::empty()).unwrap();
    assert!(!second.changed, "second application should be a no-op");
}

// ── PredicateReorderRule tests ────────────────────────────────

#[test]
fn predicate_reorder_moves_cheap_filter_first() {
    // Construct a plan with two filters: an expensive subquery check
    // followed by a cheap property comparison. The rule should swap them.
    let scan_var = IStr::new("S");
    let plan = ExecutionPlan {
        pattern_ops: vec![PatternOp::LabelScan {
            var: scan_var,
            labels: Some(LabelExpr::Name(IStr::new("sensor"))),
            inline_props: vec![],
            property_filters: vec![],
            index_order: None,
            composite_index_keys: None,
            range_index_hint: None,
            in_list_hint: None,
        }],
        pipeline: vec![
            // Expensive: EXISTS subquery (cost=20)
            PipelineOp::Filter {
                predicate: Expr::Exists {
                    pattern: Box::new(crate::ast::pattern::MatchClause {
                        selector: None,
                        match_mode: None,
                        path_mode: PathMode::Walk,
                        optional: false,
                        patterns: vec![],
                        where_clause: None,
                    }),
                    negated: false,
                },
            },
            // Cheap: property comparison (cost=4)
            PipelineOp::Filter {
                predicate: Expr::Compare(
                    Box::new(Expr::Property(
                        Box::new(Expr::Var(scan_var)),
                        IStr::new("temp"),
                    )),
                    CompareOp::Gt,
                    Box::new(Expr::Literal(GqlValue::Int(70))),
                ),
            },
        ],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = PredicateReorderRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(result.changed, "rule should reorder filters");
    // Cheap comparison should now be first
    match &result.data.pipeline[0] {
        PipelineOp::Filter { predicate } => {
            assert!(
                matches!(predicate, Expr::Compare(..)),
                "cheaper Compare filter should move first"
            );
        }
        _ => panic!("expected Filter"),
    }
}

#[test]
fn predicate_reorder_no_change_when_already_optimal() {
    // Single filter: no reordering needed.
    let scan_var = IStr::new("S");
    let plan = ExecutionPlan {
        pattern_ops: vec![PatternOp::LabelScan {
            var: scan_var,
            labels: Some(LabelExpr::Name(IStr::new("sensor"))),
            inline_props: vec![],
            property_filters: vec![],
            index_order: None,
            composite_index_keys: None,
            range_index_hint: None,
            in_list_hint: None,
        }],
        pipeline: vec![PipelineOp::Filter {
            predicate: Expr::Compare(
                Box::new(Expr::Property(
                    Box::new(Expr::Var(scan_var)),
                    IStr::new("temp"),
                )),
                CompareOp::Gt,
                Box::new(Expr::Literal(GqlValue::Int(70))),
            ),
        }],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = PredicateReorderRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(
        !result.changed,
        "single filter should not trigger reordering"
    );
}

#[test]
fn predicate_reorder_non_contiguous_filters_untouched() {
    // Filters separated by a non-Filter op should not be reordered
    // across that boundary.
    let scan_var = IStr::new("S");
    let plan = ExecutionPlan {
        pattern_ops: vec![PatternOp::LabelScan {
            var: scan_var,
            labels: Some(LabelExpr::Name(IStr::new("sensor"))),
            inline_props: vec![],
            property_filters: vec![],
            index_order: None,
            composite_index_keys: None,
            range_index_hint: None,
            in_list_hint: None,
        }],
        pipeline: vec![
            PipelineOp::Filter {
                predicate: Expr::Exists {
                    pattern: Box::new(crate::ast::pattern::MatchClause {
                        selector: None,
                        match_mode: None,
                        path_mode: PathMode::Walk,
                        optional: false,
                        patterns: vec![],
                        where_clause: None,
                    }),
                    negated: false,
                },
            },
            PipelineOp::Limit {
                value: crate::ast::statement::LimitValue::Literal(5),
            },
            PipelineOp::Filter {
                predicate: Expr::Compare(
                    Box::new(Expr::Property(
                        Box::new(Expr::Var(scan_var)),
                        IStr::new("temp"),
                    )),
                    CompareOp::Gt,
                    Box::new(Expr::Literal(GqlValue::Int(70))),
                ),
            },
        ],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = PredicateReorderRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(
        !result.changed,
        "filters separated by Limit should not be reordered"
    );
}

// ── CompositeIndexLookupRule tests ────────────────────────────

#[test]
fn composite_index_fires_with_two_literal_props() {
    let plan = ExecutionPlan {
        pattern_ops: vec![PatternOp::LabelScan {
            var: IStr::new("S"),
            labels: Some(LabelExpr::Name(IStr::new("sensor"))),
            inline_props: vec![
                (
                    IStr::new("unit"),
                    Expr::Literal(GqlValue::String(SmolStr::new("F"))),
                ),
                (
                    IStr::new("zone"),
                    Expr::Literal(GqlValue::String(SmolStr::new("A"))),
                ),
            ],
            property_filters: vec![],
            index_order: None,
            composite_index_keys: None,
            range_index_hint: None,
            in_list_hint: None,
        }],
        pipeline: vec![],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = CompositeIndexLookupRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(result.changed, "should fire with 2+ literal inline_props");
    match &result.data.pattern_ops[0] {
        PatternOp::LabelScan {
            composite_index_keys,
            ..
        } => {
            let keys = composite_index_keys.as_ref().unwrap();
            assert_eq!(keys.len(), 2);
        }
        _ => panic!("expected LabelScan"),
    }
}

#[test]
fn composite_index_skips_single_prop() {
    let plan = ExecutionPlan {
        pattern_ops: vec![PatternOp::LabelScan {
            var: IStr::new("S"),
            labels: Some(LabelExpr::Name(IStr::new("sensor"))),
            inline_props: vec![(
                IStr::new("unit"),
                Expr::Literal(GqlValue::String(SmolStr::new("F"))),
            )],
            property_filters: vec![],
            index_order: None,
            composite_index_keys: None,
            range_index_hint: None,
            in_list_hint: None,
        }],
        pipeline: vec![],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = CompositeIndexLookupRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(
        !result.changed,
        "single inline_prop should not trigger composite index"
    );
}

#[test]
fn composite_index_skips_non_literal_props() {
    // One literal, one non-literal: only 1 literal key, below threshold of 2.
    let scan_var = IStr::new("S");
    let plan = ExecutionPlan {
        pattern_ops: vec![PatternOp::LabelScan {
            var: scan_var,
            labels: Some(LabelExpr::Name(IStr::new("sensor"))),
            inline_props: vec![
                (
                    IStr::new("unit"),
                    Expr::Literal(GqlValue::String(SmolStr::new("F"))),
                ),
                (
                    IStr::new("zone"),
                    Expr::Property(Box::new(Expr::Var(scan_var)), IStr::new("other")),
                ),
            ],
            property_filters: vec![],
            index_order: None,
            composite_index_keys: None,
            range_index_hint: None,
            in_list_hint: None,
        }],
        pipeline: vec![],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = CompositeIndexLookupRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(
        !result.changed,
        "non-literal inline_prop should not be counted"
    );
}

#[test]
fn composite_index_idempotent() {
    let plan = ExecutionPlan {
        pattern_ops: vec![PatternOp::LabelScan {
            var: IStr::new("S"),
            labels: Some(LabelExpr::Name(IStr::new("sensor"))),
            inline_props: vec![
                (
                    IStr::new("unit"),
                    Expr::Literal(GqlValue::String(SmolStr::new("F"))),
                ),
                (
                    IStr::new("zone"),
                    Expr::Literal(GqlValue::String(SmolStr::new("A"))),
                ),
            ],
            property_filters: vec![],
            index_order: None,
            composite_index_keys: None,
            range_index_hint: None,
            in_list_hint: None,
        }],
        pipeline: vec![],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = CompositeIndexLookupRule;
    let first = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(first.changed);
    let second = rule.rewrite(first.data, &OptimizeContext::empty()).unwrap();
    assert!(!second.changed, "second pass should be a no-op");
}

#[test]
fn composite_index_skips_no_label() {
    // LabelScan without a label: rule should not fire.
    let plan = ExecutionPlan {
        pattern_ops: vec![PatternOp::LabelScan {
            var: IStr::new("S"),
            labels: None,
            inline_props: vec![
                (
                    IStr::new("unit"),
                    Expr::Literal(GqlValue::String(SmolStr::new("F"))),
                ),
                (
                    IStr::new("zone"),
                    Expr::Literal(GqlValue::String(SmolStr::new("A"))),
                ),
            ],
            property_filters: vec![],
            index_order: None,
            composite_index_keys: None,
            range_index_hint: None,
            in_list_hint: None,
        }],
        pipeline: vec![],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = CompositeIndexLookupRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(
        !result.changed,
        "no label should prevent composite index hint"
    );
}

// ── RangeIndexScanRule tests ──────────────────────────────────

fn make_range_plan(filters: Vec<PropertyFilter>) -> ExecutionPlan {
    ExecutionPlan {
        pattern_ops: vec![PatternOp::LabelScan {
            var: IStr::new("S"),
            labels: Some(LabelExpr::Name(IStr::new("sensor"))),
            inline_props: vec![],
            property_filters: filters,
            index_order: None,
            composite_index_keys: None,
            range_index_hint: None,
            in_list_hint: None,
        }],
        pipeline: vec![],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    }
}

#[test]
fn range_index_fires_on_gt_filter() {
    let plan = make_range_plan(vec![PropertyFilter {
        key: IStr::new("temp"),
        op: CompareOp::Gt,
        value: GqlValue::Int(70),
    }]);
    let rule = RangeIndexScanRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(result.changed);
    match &result.data.pattern_ops[0] {
        PatternOp::LabelScan {
            range_index_hint, ..
        } => {
            let hint = range_index_hint.as_ref().unwrap();
            assert_eq!(hint.key.as_str(), "temp");
            assert!(hint.lower.is_some(), "GT should set lower bound");
            let (val, incl) = hint.lower.as_ref().unwrap();
            assert_eq!(*val, GqlValue::Int(70));
            assert!(!incl, "GT is exclusive");
            assert!(hint.upper.is_none());
        }
        _ => panic!("expected LabelScan"),
    }
}

#[test]
fn range_index_fires_on_gte_filter() {
    let plan = make_range_plan(vec![PropertyFilter {
        key: IStr::new("temp"),
        op: CompareOp::Gte,
        value: GqlValue::Int(70),
    }]);
    let rule = RangeIndexScanRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(result.changed);
    match &result.data.pattern_ops[0] {
        PatternOp::LabelScan {
            range_index_hint, ..
        } => {
            let hint = range_index_hint.as_ref().unwrap();
            let (_, incl) = hint.lower.as_ref().unwrap();
            assert!(incl, "GTE is inclusive");
        }
        _ => panic!("expected LabelScan"),
    }
}

#[test]
fn range_index_fires_on_lt_filter() {
    let plan = make_range_plan(vec![PropertyFilter {
        key: IStr::new("temp"),
        op: CompareOp::Lt,
        value: GqlValue::Int(100),
    }]);
    let rule = RangeIndexScanRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(result.changed);
    match &result.data.pattern_ops[0] {
        PatternOp::LabelScan {
            range_index_hint, ..
        } => {
            let hint = range_index_hint.as_ref().unwrap();
            assert!(hint.lower.is_none());
            assert!(hint.upper.is_some(), "LT should set upper bound");
            let (val, incl) = hint.upper.as_ref().unwrap();
            assert_eq!(*val, GqlValue::Int(100));
            assert!(!incl, "LT is exclusive");
        }
        _ => panic!("expected LabelScan"),
    }
}

#[test]
fn range_index_merges_gt_and_lt_same_key() {
    // Two range filters on the same key produce one hint with both bounds.
    let plan = make_range_plan(vec![
        PropertyFilter {
            key: IStr::new("temp"),
            op: CompareOp::Gt,
            value: GqlValue::Int(50),
        },
        PropertyFilter {
            key: IStr::new("temp"),
            op: CompareOp::Lt,
            value: GqlValue::Int(100),
        },
    ]);
    let rule = RangeIndexScanRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(result.changed);
    match &result.data.pattern_ops[0] {
        PatternOp::LabelScan {
            range_index_hint, ..
        } => {
            let hint = range_index_hint.as_ref().unwrap();
            assert!(hint.lower.is_some(), "should have lower bound");
            assert!(hint.upper.is_some(), "should have upper bound");
        }
        _ => panic!("expected LabelScan"),
    }
}

#[test]
fn range_index_skips_equality_filter() {
    let plan = make_range_plan(vec![PropertyFilter {
        key: IStr::new("temp"),
        op: CompareOp::Eq,
        value: GqlValue::Int(70),
    }]);
    let rule = RangeIndexScanRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(
        !result.changed,
        "equality filter should not trigger range index"
    );
}

#[test]
fn range_index_idempotent() {
    let plan = make_range_plan(vec![PropertyFilter {
        key: IStr::new("temp"),
        op: CompareOp::Gt,
        value: GqlValue::Int(70),
    }]);
    let rule = RangeIndexScanRule;
    let first = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(first.changed);
    let second = rule.rewrite(first.data, &OptimizeContext::empty()).unwrap();
    assert!(!second.changed, "second pass should not reapply hint");
}

// ── InListOptimizationRule tests ──────────────────────────────

fn setup_indexed_graph() -> SeleneGraph {
    use selene_core::schema::{NodeSchema, PropertyDef, ValidationMode, ValueType};

    let mut g = SeleneGraph::with_config(
        selene_graph::SchemaValidator::new(ValidationMode::Warn),
        100,
    );
    let schema = NodeSchema {
        label: std::sync::Arc::from("sensor"),
        parent: None,
        properties: vec![PropertyDef {
            name: std::sync::Arc::from("unit"),
            value_type: ValueType::String,
            required: false,
            default: None,
            description: String::new(),
            indexed: true,
            unique: false,
            min: None,
            max: None,
            min_length: None,
            max_length: None,
            allowed_values: vec![],
            pattern: None,
            immutable: false,
            searchable: false,
            dictionary: false,
            fill: None,
            expected_interval_nanos: None,
            encoding: selene_core::ValueEncoding::Gorilla,
        }],
        valid_edge_labels: vec![],
        description: String::new(),
        annotations: std::collections::HashMap::new(),
        version: Default::default(),
        validation_mode: None,
        key_properties: vec![],
    };
    g.schema_mut().register_node_schema(schema).unwrap();

    let mut m = g.mutate();
    m.create_node(
        LabelSet::from_strs(&["sensor"]),
        PropertyMap::from_pairs(vec![(selene_core::IStr::new("unit"), Value::str("F"))]),
    )
    .unwrap();
    m.commit(0).unwrap();
    g.build_property_indexes();
    g
}

#[test]
fn in_list_fires_on_literal_list() {
    let g = setup_indexed_graph();
    let scan_var = IStr::new("S");
    let plan = ExecutionPlan {
        pattern_ops: vec![PatternOp::LabelScan {
            var: scan_var,
            labels: Some(LabelExpr::Name(IStr::new("sensor"))),
            inline_props: vec![],
            property_filters: vec![],
            index_order: None,
            composite_index_keys: None,
            range_index_hint: None,
            in_list_hint: None,
        }],
        pipeline: vec![PipelineOp::Filter {
            predicate: Expr::InList {
                expr: Box::new(Expr::Property(
                    Box::new(Expr::Var(scan_var)),
                    IStr::new("unit"),
                )),
                list: vec![
                    Expr::Literal(GqlValue::String(SmolStr::new("F"))),
                    Expr::Literal(GqlValue::String(SmolStr::new("C"))),
                ],
                negated: false,
            },
        }],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = InListOptimizationRule;
    let result = rule.rewrite(plan, &OptimizeContext::new(&g)).unwrap();
    assert!(result.changed, "should fire on literal IN-list with index");
    // Filter should be removed from pipeline
    assert!(
        !result
            .data
            .pipeline
            .iter()
            .any(|op| matches!(op, PipelineOp::Filter { .. })),
        "filter should be consumed"
    );
    // Hint should be set on LabelScan
    match &result.data.pattern_ops[0] {
        PatternOp::LabelScan { in_list_hint, .. } => {
            let hint = in_list_hint.as_ref().unwrap();
            assert_eq!(hint.key.as_str(), "unit");
            assert_eq!(hint.values.len(), 2);
        }
        _ => panic!("expected LabelScan"),
    }
}

#[test]
fn in_list_keeps_filter_without_index() {
    // No property index on "unit"; the filter must stay in the pipeline.
    let g = setup_graph();
    let scan_var = IStr::new("S");
    let plan = ExecutionPlan {
        pattern_ops: vec![PatternOp::LabelScan {
            var: scan_var,
            labels: Some(LabelExpr::Name(IStr::new("sensor"))),
            inline_props: vec![],
            property_filters: vec![],
            index_order: None,
            composite_index_keys: None,
            range_index_hint: None,
            in_list_hint: None,
        }],
        pipeline: vec![PipelineOp::Filter {
            predicate: Expr::InList {
                expr: Box::new(Expr::Property(
                    Box::new(Expr::Var(scan_var)),
                    IStr::new("unit"),
                )),
                list: vec![
                    Expr::Literal(GqlValue::String(SmolStr::new("F"))),
                    Expr::Literal(GqlValue::String(SmolStr::new("C"))),
                ],
                negated: false,
            },
        }],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = InListOptimizationRule;
    let result = rule.rewrite(plan, &OptimizeContext::new(&g)).unwrap();
    assert!(
        !result.changed,
        "IN-list on non-indexed property should not fire"
    );
    assert!(
        result
            .data
            .pipeline
            .iter()
            .any(|op| matches!(op, PipelineOp::Filter { .. })),
        "filter must remain for runtime eval_in_list fallback"
    );
    match &result.data.pattern_ops[0] {
        PatternOp::LabelScan { in_list_hint, .. } => {
            assert!(in_list_hint.is_none(), "no hint without an index");
        }
        _ => panic!("expected LabelScan"),
    }
}

#[test]
fn in_list_skips_negated() {
    let scan_var = IStr::new("S");
    let plan = ExecutionPlan {
        pattern_ops: vec![PatternOp::LabelScan {
            var: scan_var,
            labels: Some(LabelExpr::Name(IStr::new("sensor"))),
            inline_props: vec![],
            property_filters: vec![],
            index_order: None,
            composite_index_keys: None,
            range_index_hint: None,
            in_list_hint: None,
        }],
        pipeline: vec![PipelineOp::Filter {
            predicate: Expr::InList {
                expr: Box::new(Expr::Property(
                    Box::new(Expr::Var(scan_var)),
                    IStr::new("unit"),
                )),
                list: vec![Expr::Literal(GqlValue::String(SmolStr::new("F")))],
                negated: true,
            },
        }],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = InListOptimizationRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(
        !result.changed,
        "NOT IN should not be optimized into in_list_hint"
    );
}

#[test]
fn in_list_skips_non_literal_elements() {
    let scan_var = IStr::new("S");
    let plan = ExecutionPlan {
        pattern_ops: vec![PatternOp::LabelScan {
            var: scan_var,
            labels: Some(LabelExpr::Name(IStr::new("sensor"))),
            inline_props: vec![],
            property_filters: vec![],
            index_order: None,
            composite_index_keys: None,
            range_index_hint: None,
            in_list_hint: None,
        }],
        pipeline: vec![PipelineOp::Filter {
            predicate: Expr::InList {
                expr: Box::new(Expr::Property(
                    Box::new(Expr::Var(scan_var)),
                    IStr::new("unit"),
                )),
                list: vec![
                    Expr::Literal(GqlValue::String(SmolStr::new("F"))),
                    Expr::Var(IStr::new("DYNAMIC")), // non-literal
                ],
                negated: false,
            },
        }],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = InListOptimizationRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(
        !result.changed,
        "non-literal list elements should prevent optimization"
    );
}

// ── ExpandFilterPushdownRule tests ─────────────────────────────

#[test]
fn expand_filter_pushes_target_property() {
    let scan_var = IStr::new("S");
    let target_var = IStr::new("T");
    let plan = ExecutionPlan {
        pattern_ops: vec![
            PatternOp::LabelScan {
                var: scan_var,
                labels: Some(LabelExpr::Name(IStr::new("sensor"))),
                inline_props: vec![],
                property_filters: vec![],
                index_order: None,
                composite_index_keys: None,
                range_index_hint: None,
                in_list_hint: None,
            },
            PatternOp::Expand {
                source_var: scan_var,
                edge_var: Some(IStr::new("E")),
                target_var,
                edge_labels: Some(LabelExpr::Name(IStr::new("feeds"))),
                target_labels: Some(LabelExpr::Name(IStr::new("room"))),
                direction: EdgeDirection::Out,
                target_property_filters: vec![],
                edge_property_filters: vec![],
            },
        ],
        pipeline: vec![PipelineOp::Filter {
            predicate: Expr::Compare(
                Box::new(Expr::Property(
                    Box::new(Expr::Var(target_var)),
                    IStr::new("floor"),
                )),
                CompareOp::Eq,
                Box::new(Expr::Literal(GqlValue::Int(3))),
            ),
        }],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = ExpandFilterPushdownRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(
        result.changed,
        "should push target property filter into Expand"
    );
    // Pipeline filter should be consumed
    assert!(
        !result
            .data
            .pipeline
            .iter()
            .any(|op| matches!(op, PipelineOp::Filter { .. })),
    );
    // Filter should be in target_property_filters
    match &result.data.pattern_ops[1] {
        PatternOp::Expand {
            target_property_filters,
            ..
        } => {
            assert_eq!(target_property_filters.len(), 1);
            assert_eq!(target_property_filters[0].key.as_str(), "floor");
        }
        _ => panic!("expected Expand"),
    }
}

#[test]
fn expand_filter_pushes_edge_property() {
    let scan_var = IStr::new("S");
    let edge_var = IStr::new("E");
    let plan = ExecutionPlan {
        pattern_ops: vec![
            PatternOp::LabelScan {
                var: scan_var,
                labels: Some(LabelExpr::Name(IStr::new("sensor"))),
                inline_props: vec![],
                property_filters: vec![],
                index_order: None,
                composite_index_keys: None,
                range_index_hint: None,
                in_list_hint: None,
            },
            PatternOp::Expand {
                source_var: scan_var,
                edge_var: Some(edge_var),
                target_var: IStr::new("T"),
                edge_labels: Some(LabelExpr::Name(IStr::new("feeds"))),
                target_labels: None,
                direction: EdgeDirection::Out,
                target_property_filters: vec![],
                edge_property_filters: vec![],
            },
        ],
        pipeline: vec![PipelineOp::Filter {
            predicate: Expr::Compare(
                Box::new(Expr::Property(
                    Box::new(Expr::Var(edge_var)),
                    IStr::new("weight"),
                )),
                CompareOp::Gt,
                Box::new(Expr::Literal(GqlValue::Float(0.5))),
            ),
        }],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = ExpandFilterPushdownRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(
        result.changed,
        "should push edge property filter into Expand"
    );
    match &result.data.pattern_ops[1] {
        PatternOp::Expand {
            edge_property_filters,
            ..
        } => {
            assert_eq!(edge_property_filters.len(), 1);
            assert_eq!(edge_property_filters[0].key.as_str(), "weight");
        }
        _ => panic!("expected Expand"),
    }
}

#[test]
fn expand_filter_skips_scan_var() {
    // Filter on the scan variable should not be pushed into Expand.
    let scan_var = IStr::new("S");
    let plan = ExecutionPlan {
        pattern_ops: vec![
            PatternOp::LabelScan {
                var: scan_var,
                labels: Some(LabelExpr::Name(IStr::new("sensor"))),
                inline_props: vec![],
                property_filters: vec![],
                index_order: None,
                composite_index_keys: None,
                range_index_hint: None,
                in_list_hint: None,
            },
            PatternOp::Expand {
                source_var: scan_var,
                edge_var: None,
                target_var: IStr::new("T"),
                edge_labels: None,
                target_labels: None,
                direction: EdgeDirection::Out,
                target_property_filters: vec![],
                edge_property_filters: vec![],
            },
        ],
        pipeline: vec![PipelineOp::Filter {
            predicate: Expr::Compare(
                Box::new(Expr::Property(
                    Box::new(Expr::Var(scan_var)),
                    IStr::new("temp"),
                )),
                CompareOp::Eq,
                Box::new(Expr::Literal(GqlValue::Int(72))),
            ),
        }],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = ExpandFilterPushdownRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(
        !result.changed,
        "filter on scan var should not be pushed into Expand"
    );
    assert_eq!(
        result.data.pipeline.len(),
        1,
        "filter should remain in pipeline"
    );
}

#[test]
fn expand_filter_no_expand_ops() {
    // Plan with no Expand ops: rule should not fire.
    let plan = ExecutionPlan {
        pattern_ops: vec![PatternOp::LabelScan {
            var: IStr::new("S"),
            labels: Some(LabelExpr::Name(IStr::new("sensor"))),
            inline_props: vec![],
            property_filters: vec![],
            index_order: None,
            composite_index_keys: None,
            range_index_hint: None,
            in_list_hint: None,
        }],
        pipeline: vec![PipelineOp::Filter {
            predicate: Expr::Compare(
                Box::new(Expr::Literal(GqlValue::Int(1))),
                CompareOp::Eq,
                Box::new(Expr::Literal(GqlValue::Int(1))),
            ),
        }],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = ExpandFilterPushdownRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(!result.changed, "no Expand ops means no pushdown");
}

// ── FilterInterleavingRule tests ──────────────────────────────

#[test]
fn filter_interleaving_moves_filter_between_ops() {
    let scan_var = IStr::new("A");
    let target_var = IStr::new("B");
    let second_target = IStr::new("C");
    let plan = ExecutionPlan {
        pattern_ops: vec![
            PatternOp::LabelScan {
                var: scan_var,
                labels: Some(LabelExpr::Name(IStr::new("Person"))),
                inline_props: vec![],
                property_filters: vec![],
                index_order: None,
                composite_index_keys: None,
                range_index_hint: None,
                in_list_hint: None,
            },
            PatternOp::Expand {
                source_var: scan_var,
                edge_var: None,
                target_var,
                edge_labels: None,
                target_labels: None,
                direction: EdgeDirection::Out,
                target_property_filters: vec![],
                edge_property_filters: vec![],
            },
            PatternOp::Expand {
                source_var: target_var,
                edge_var: None,
                target_var: second_target,
                edge_labels: None,
                target_labels: None,
                direction: EdgeDirection::Out,
                target_property_filters: vec![],
                edge_property_filters: vec![],
            },
        ],
        pipeline: vec![
            // Filter on `A` should be interleaved after op[0] (the LabelScan)
            PipelineOp::Filter {
                predicate: Expr::Compare(
                    Box::new(Expr::Property(
                        Box::new(Expr::Var(scan_var)),
                        IStr::new("age"),
                    )),
                    CompareOp::Gt,
                    Box::new(Expr::Literal(GqlValue::Int(30))),
                ),
            },
        ],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = FilterInterleavingRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(
        result.changed,
        "should interleave filter between pattern ops"
    );
    // Filter should now be an IntermediateFilter in pattern_ops
    assert!(
        result
            .data
            .pattern_ops
            .iter()
            .any(|op| matches!(op, PatternOp::IntermediateFilter { .. })),
        "should have IntermediateFilter in pattern ops"
    );
    // Pipeline should have no remaining Filter ops
    assert!(
        !result
            .data
            .pipeline
            .iter()
            .any(|op| matches!(op, PipelineOp::Filter { .. })),
        "filter should be moved out of pipeline"
    );
}

#[test]
fn filter_interleaving_skips_subquery_filter() {
    let scan_var = IStr::new("A");
    let plan = ExecutionPlan {
        pattern_ops: vec![
            PatternOp::LabelScan {
                var: scan_var,
                labels: Some(LabelExpr::Name(IStr::new("Person"))),
                inline_props: vec![],
                property_filters: vec![],
                index_order: None,
                composite_index_keys: None,
                range_index_hint: None,
                in_list_hint: None,
            },
            PatternOp::Expand {
                source_var: scan_var,
                edge_var: None,
                target_var: IStr::new("B"),
                edge_labels: None,
                target_labels: None,
                direction: EdgeDirection::Out,
                target_property_filters: vec![],
                edge_property_filters: vec![],
            },
        ],
        pipeline: vec![PipelineOp::Filter {
            predicate: Expr::Exists {
                pattern: Box::new(crate::ast::pattern::MatchClause {
                    selector: None,
                    match_mode: None,
                    path_mode: PathMode::Walk,
                    optional: false,
                    patterns: vec![],
                    where_clause: None,
                }),
                negated: false,
            },
        }],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = FilterInterleavingRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(
        !result.changed,
        "subquery-containing filters should not be interleaved"
    );
}

#[test]
fn filter_interleaving_skips_single_pattern_op() {
    // With only one pattern op, interleaving adds nothing.
    let scan_var = IStr::new("A");
    let plan = ExecutionPlan {
        pattern_ops: vec![PatternOp::LabelScan {
            var: scan_var,
            labels: Some(LabelExpr::Name(IStr::new("Person"))),
            inline_props: vec![],
            property_filters: vec![],
            index_order: None,
            composite_index_keys: None,
            range_index_hint: None,
            in_list_hint: None,
        }],
        pipeline: vec![PipelineOp::Filter {
            predicate: Expr::Compare(
                Box::new(Expr::Property(
                    Box::new(Expr::Var(scan_var)),
                    IStr::new("age"),
                )),
                CompareOp::Gt,
                Box::new(Expr::Literal(GqlValue::Int(30))),
            ),
        }],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = FilterInterleavingRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    assert!(
        !result.changed,
        "single pattern op should prevent interleaving"
    );
}

#[test]
fn filter_interleaving_preserves_pipeline_order_for_late_binding() {
    // Filter depends on `C` (bound at op[2]): should not be interleaved
    // before the last pattern op if it is the last op.
    let a = IStr::new("A");
    let b = IStr::new("B");
    let c = IStr::new("C");
    let plan = ExecutionPlan {
        pattern_ops: vec![
            PatternOp::LabelScan {
                var: a,
                labels: Some(LabelExpr::Name(IStr::new("X"))),
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
        ],
        pipeline: vec![PipelineOp::Filter {
            predicate: Expr::Compare(
                Box::new(Expr::Property(Box::new(Expr::Var(c)), IStr::new("v"))),
                CompareOp::Eq,
                Box::new(Expr::Literal(GqlValue::Int(1))),
            ),
        }],
        mutations: vec![],
        output_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        display_schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        count_only: false,
    };
    let rule = FilterInterleavingRule;
    let result = rule.rewrite(plan, &OptimizeContext::empty()).unwrap();
    // Variable C is bound after op[2] (last op), so insert_pos = 3 = len.
    // The rule refuses to interleave at the end of the op list.
    assert!(
        !result.changed,
        "filter on last-bound variable should not be interleaved"
    );
}

// ── Integration: full optimizer pipeline tests ─────────────────

#[test]
fn full_optimizer_index_order_via_query() {
    // The planner emits TopK with the sort term referencing the RETURN
    // alias (e.g. Var("TEMP")), not the raw property access. IndexOrderRule
    // requires Property(Var(scan_var), key) to fire. Because of this,
    // index_order is NOT set in the end-to-end pipeline for aliased sorts.
    // The unit tests for IndexOrderRule verify it works with pre-projection
    // sort terms. This test verifies the full pipeline preserves TopK and
    // does not regress.
    let g = setup_graph();
    let plan = plan_for(
        "MATCH (s:sensor) RETURN s.temp AS temp ORDER BY s.temp LIMIT 1",
        &g,
    );
    let opt = GqlOptimizer::with_default_rules();
    let plan = opt.optimize(plan, &OptimizeContext::empty()).unwrap();
    // Verify the LabelScan is preserved and TopK is in the pipeline.
    match &plan.pattern_ops[0] {
        PatternOp::LabelScan { .. } => {}
        _ => panic!("expected LabelScan"),
    }
    assert!(
        plan.pipeline
            .iter()
            .any(|op| matches!(op, PipelineOp::TopK { .. })),
        "full optimizer should preserve TopK in the pipeline"
    );
}

#[test]
fn full_optimizer_range_and_pushdown_combined() {
    let g = setup_graph();
    let plan = plan_for(
        "MATCH (s:sensor) FILTER s.temp >= 70 AND s.temp <= 100 RETURN s",
        &g,
    );
    let opt = GqlOptimizer::with_default_rules();
    let plan = opt.optimize(plan, &OptimizeContext::empty()).unwrap();
    // AND splitting + pushdown + range index should all fire
    match &plan.pattern_ops[0] {
        PatternOp::LabelScan {
            property_filters,
            range_index_hint,
            ..
        } => {
            assert!(
                !property_filters.is_empty(),
                "filters should be pushed down"
            );
            assert!(
                range_index_hint.is_some(),
                "range index hint should be produced"
            );
            let hint = range_index_hint.as_ref().unwrap();
            assert!(hint.lower.is_some(), "lower bound for >= 70");
            assert!(hint.upper.is_some(), "upper bound for <= 100");
        }
        _ => panic!("expected LabelScan"),
    }
    // No filters in pipeline
    assert!(
        !plan
            .pipeline
            .iter()
            .any(|op| matches!(op, PipelineOp::Filter { .. })),
        "all filters should be pushed down"
    );
}

#[test]
fn full_optimizer_in_list_via_query() {
    let g = setup_indexed_graph();
    let plan = plan_for("MATCH (s:sensor) FILTER s.unit IN ['F', 'C'] RETURN s", &g);
    let opt = GqlOptimizer::with_default_rules();
    let plan = opt.optimize(plan, &OptimizeContext::new(&g)).unwrap();
    match &plan.pattern_ops[0] {
        PatternOp::LabelScan { in_list_hint, .. } => {
            assert!(
                in_list_hint.is_some(),
                "IN-list should produce in_list_hint when index exists"
            );
            assert_eq!(in_list_hint.as_ref().unwrap().values.len(), 2);
        }
        _ => panic!("expected LabelScan"),
    }
}

#[test]
fn full_optimizer_in_list_skips_without_index() {
    let g = setup_graph();
    let plan = plan_for("MATCH (s:sensor) FILTER s.unit IN ['F', 'C'] RETURN s", &g);
    let opt = GqlOptimizer::with_default_rules();
    let plan = opt.optimize(plan, &OptimizeContext::new(&g)).unwrap();
    match &plan.pattern_ops[0] {
        PatternOp::LabelScan { in_list_hint, .. } => {
            assert!(
                in_list_hint.is_none(),
                "IN-list must not produce hint without a property index"
            );
        }
        _ => panic!("expected LabelScan"),
    }
    assert!(
        plan.pipeline
            .iter()
            .any(|op| matches!(op, PipelineOp::Filter { .. })),
        "filter must stay in pipeline for runtime eval_in_list"
    );
}

// ── Helper function tests ─────────────────────────────────────

#[test]
fn estimate_predicate_cost_ordering() {
    // Verify relative cost ordering used by PredicateReorderRule
    let literal_cost = estimate_predicate_cost(&Expr::Literal(GqlValue::Int(1)));
    let property_cost = estimate_predicate_cost(&Expr::Property(
        Box::new(Expr::Var(IStr::new("X"))),
        IStr::new("y"),
    ));
    let compare_cost = estimate_predicate_cost(&Expr::Compare(
        Box::new(Expr::Literal(GqlValue::Int(1))),
        CompareOp::Eq,
        Box::new(Expr::Literal(GqlValue::Int(2))),
    ));
    let exists_cost = estimate_predicate_cost(&Expr::Exists {
        pattern: Box::new(crate::ast::pattern::MatchClause {
            selector: None,
            match_mode: None,
            path_mode: PathMode::Walk,
            optional: false,
            patterns: vec![],
            where_clause: None,
        }),
        negated: false,
    });

    assert!(literal_cost < property_cost);
    assert!(property_cost < compare_cost);
    assert!(compare_cost < exists_cost);
}

#[test]
fn contains_subquery_detects_exists() {
    let expr = Expr::Exists {
        pattern: Box::new(crate::ast::pattern::MatchClause {
            selector: None,
            match_mode: None,
            path_mode: PathMode::Walk,
            optional: false,
            patterns: vec![],
            where_clause: None,
        }),
        negated: false,
    };
    assert!(contains_subquery(&expr));
}

#[test]
fn contains_subquery_false_for_simple_compare() {
    let expr = Expr::Compare(
        Box::new(Expr::Literal(GqlValue::Int(1))),
        CompareOp::Eq,
        Box::new(Expr::Literal(GqlValue::Int(1))),
    );
    assert!(!contains_subquery(&expr));
}
