//! EXPLAIN / PROFILE: format execution plans as human-readable text.

use std::fmt::Write;

use crate::ast::pattern::LabelExpr;
use crate::planner::plan::*;

/// Format an ExecutionPlan as human-readable text.
pub fn format_plan(plan: &ExecutionPlan) -> String {
    let mut out = String::with_capacity(256);

    // Pattern phase
    if !plan.pattern_ops.is_empty() {
        out.push_str("Pattern:\n");
        for (i, op) in plan.pattern_ops.iter().enumerate() {
            let _ = writeln!(out, "  {i}: {}", format_pattern_op(op));
        }
    }

    // Mutations
    if !plan.mutations.is_empty() {
        out.push_str("Mutations:\n");
        for (i, m) in plan.mutations.iter().enumerate() {
            let _ = writeln!(out, "  {i}: {}", format_mutation(m));
        }
    }

    // Pipeline phase
    if !plan.pipeline.is_empty() {
        out.push_str("Pipeline:\n");
        for (i, op) in plan.pipeline.iter().enumerate() {
            let _ = writeln!(out, "  {i}: {}", format_pipeline_op(op));
        }
    }

    // Output schema
    let fields: Vec<&str> = plan
        .output_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let _ = writeln!(out, "Output: [{}]", fields.join(", "));

    out
}

fn format_pattern_op(op: &PatternOp) -> String {
    match op {
        PatternOp::LabelScan {
            var,
            labels,
            inline_props,
            property_filters,
            index_order,
            composite_index_keys: _,
            range_index_hint,
            in_list_hint,
        } => {
            let label_str = labels.as_ref().map_or("*".to_string(), format_label_expr);
            let mut filters = Vec::new();
            for (k, _) in inline_props {
                filters.push(format!("{}=", k.as_str()));
            }
            for f in property_filters {
                let op_str = match f.op {
                    crate::ast::expr::CompareOp::Eq => "=",
                    crate::ast::expr::CompareOp::Neq => "<>",
                    crate::ast::expr::CompareOp::Lt => "<",
                    crate::ast::expr::CompareOp::Gt => ">",
                    crate::ast::expr::CompareOp::Lte => "<=",
                    crate::ast::expr::CompareOp::Gte => ">=",
                };
                filters.push(format!("{}{op_str}", f.key.as_str()));
            }
            let props = if filters.is_empty() {
                String::new()
            } else {
                format!(", filters=[{}]", filters.join(", "))
            };
            let idx_str = if let Some(order) = index_order {
                let dir = if order.descending { "DESC" } else { "ASC" };
                format!(
                    ", index_order={} {} LIMIT {}",
                    order.key.as_str(),
                    dir,
                    order.limit
                )
            } else {
                String::new()
            };
            let range_str = if let Some(hint) = range_index_hint {
                let lo = hint
                    .lower
                    .as_ref()
                    .map(|(v, inc)| format!("{}{v}", if *inc { ">=" } else { ">" }))
                    .unwrap_or_default();
                let hi = hint
                    .upper
                    .as_ref()
                    .map(|(v, inc)| format!("{}{v}", if *inc { "<=" } else { "<" }))
                    .unwrap_or_default();
                format!(", range_index={}{lo}{hi}", hint.key.as_str())
            } else {
                String::new()
            };
            let inlist_str = if let Some(hint) = in_list_hint {
                let vals: Vec<String> = hint.values.iter().map(|v| format!("{v}")).collect();
                format!(", in_list={}:[{}]", hint.key.as_str(), vals.join(","))
            } else {
                String::new()
            };
            format!(
                "LabelScan(var={}, labels={label_str}{props}{idx_str}{range_str}{inlist_str})",
                var.as_str()
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
            let edge_str = edge_labels
                .as_ref()
                .map_or("*".to_string(), format_label_expr);
            let target_str = target_labels
                .as_ref()
                .map_or("*".to_string(), format_label_expr);
            let edge_name = edge_var.as_ref().map_or("-", |v| v.as_str());
            let mut extra = String::new();
            if !target_property_filters.is_empty() {
                let f: Vec<String> = target_property_filters
                    .iter()
                    .map(|f| format!("{}{:?}", f.key.as_str(), f.op))
                    .collect();
                let _ = write!(extra, ", target_filter=[{}]", f.join(", "));
            }
            if !edge_property_filters.is_empty() {
                let f: Vec<String> = edge_property_filters
                    .iter()
                    .map(|f| format!("{}{:?}", f.key.as_str(), f.op))
                    .collect();
                let _ = write!(extra, ", edge_filter=[{}]", f.join(", "));
            }
            format!(
                "Expand({} -{edge_name}:{edge_str} {direction:?}-> {target_str}, target_var={}{extra})",
                source_var.as_str(),
                target_var.as_str()
            )
        }
        PatternOp::VarExpand {
            source_var,
            target_var,
            edge_labels,
            min_hops,
            max_hops,
            trail,
            acyclic,
            simple,
            ..
        } => {
            let edge_str = edge_labels
                .as_ref()
                .map_or("*".to_string(), format_label_expr);
            let max = max_hops.map_or("∞".to_string(), |m| m.to_string());
            let mode_str = if *trail {
                ", TRAIL"
            } else if *acyclic {
                ", ACYCLIC"
            } else if *simple {
                ", SIMPLE"
            } else {
                ""
            };
            format!(
                "VarExpand({} -:{edge_str} {{{min_hops},{max}}}{mode_str}-> {})",
                source_var.as_str(),
                target_var.as_str()
            )
        }
        PatternOp::Optional {
            inner_ops,
            new_vars,
            ..
        } => {
            let inner: Vec<String> = inner_ops.iter().map(format_pattern_op).collect();
            let vars: Vec<&str> = new_vars.iter().map(|v| v.as_str()).collect();
            format!(
                "Optional(inner=[{}], new_vars=[{}])",
                inner.join("; "),
                vars.join(", ")
            )
        }
        PatternOp::Join {
            right_start,
            right_end,
            join_vars,
        } => {
            let vars: Vec<&str> = join_vars.iter().map(|v| v.as_str()).collect();
            format!(
                "Join(ops[{right_start}..{right_end}], on=[{}])",
                vars.join(", ")
            )
        }
        PatternOp::CycleJoin {
            bound_var,
            source_var,
            edge_labels,
            direction,
        } => {
            let dir = match direction {
                crate::ast::pattern::EdgeDirection::Out => "->",
                crate::ast::pattern::EdgeDirection::In => "<-",
                crate::ast::pattern::EdgeDirection::Any => "--",
            };
            let labels = edge_labels
                .as_ref()
                .map_or("*".into(), |l| format!("{l:?}"));
            format!("CycleJoin({source_var}{dir}{bound_var}, labels={labels})")
        }
        PatternOp::DifferentEdgesFilter { edge_vars } => {
            let vars: Vec<&str> = edge_vars.iter().map(|v| v.as_str()).collect();
            format!("DifferentEdgesFilter({})", vars.join(", "))
        }
        PatternOp::IntermediateFilter { predicate } => {
            format!("IntermediateFilter({predicate:?})")
        }
        PatternOp::WcoJoin {
            scan_var,
            relations,
            ..
        } => {
            let rels: Vec<String> = relations
                .iter()
                .map(|r| {
                    let label = r.edge_label.map_or("*".into(), |l| l.to_string());
                    format!(
                        "({})--[:{}]->({}))",
                        r.source_var.as_str(),
                        label,
                        r.target_var.as_str()
                    )
                })
                .collect();
            format!(
                "WcoJoin(scan={}, relations=[{}])",
                scan_var.as_str(),
                rels.join(", ")
            )
        }
    }
}

fn format_pipeline_op(op: &PipelineOp) -> String {
    match op {
        PipelineOp::Let { bindings } => {
            let vars: Vec<&str> = bindings.iter().map(|(k, _)| k.as_str()).collect();
            format!("Let({})", vars.join(", "))
        }
        PipelineOp::Filter { .. } => "Filter(...)".to_string(),
        PipelineOp::Sort { terms } => {
            let ts: Vec<String> = terms
                .iter()
                .map(|t| {
                    let dir = if t.descending { "DESC" } else { "ASC" };
                    dir.to_string()
                })
                .collect();
            format!("Sort({})", ts.join(", "))
        }
        PipelineOp::TopK { terms, limit } => {
            let ts: Vec<String> = terms
                .iter()
                .map(|t| {
                    if t.descending {
                        "DESC".to_string()
                    } else {
                        "ASC".to_string()
                    }
                })
                .collect();
            format!("TopK(k={limit}, {})", ts.join(", "))
        }
        PipelineOp::Offset { value } => format!("Offset({value})"),
        PipelineOp::Limit { value } => format!("Limit({value})"),
        PipelineOp::Return {
            projections,
            group_by,
            distinct,
            having,
            all,
        } => {
            let cols: Vec<&str> = projections.iter().map(|p| p.alias.as_str()).collect();
            let mut s = if *all {
                "Return(*)".to_string()
            } else {
                format!("Return({})", cols.join(", "))
            };
            if !group_by.is_empty() {
                let gk: Vec<String> = group_by.iter().map(|g| format!("{g:?}")).collect();
                let _ = write!(s, ", GROUP BY [{}]", gk.join(", "));
            }
            if having.is_some() {
                s.push_str(", HAVING");
            }
            if *distinct {
                s.push_str(", DISTINCT");
            }
            s
        }
        PipelineOp::With {
            projections,
            group_by,
            distinct,
            having,
            where_filter,
        } => {
            let cols: Vec<&str> = projections.iter().map(|p| p.alias.as_str()).collect();
            let mut s = format!("With({})", cols.join(", "));
            if !group_by.is_empty() {
                let gk: Vec<String> = group_by.iter().map(|g| format!("{g:?}")).collect();
                let _ = write!(s, ", GROUP BY [{}]", gk.join(", "));
            }
            if having.is_some() {
                s.push_str(", HAVING");
            }
            if *distinct {
                s.push_str(", DISTINCT");
            }
            if where_filter.is_some() {
                s.push_str(", WHERE");
            }
            s
        }
        PipelineOp::Call { procedure } => {
            format!("Call({})", procedure.name.as_str())
        }
        PipelineOp::Subquery { plan } => {
            let inner: Vec<String> = plan.pipeline.iter().map(format_pipeline_op).collect();
            format!("Subquery(pipeline=[{}])", inner.join("; "))
        }
        PipelineOp::For { var, .. } => format!("For({})", var.as_str()),
        PipelineOp::NestedMatch {
            pattern_ops,
            where_filter,
        } => {
            let ops: Vec<String> = pattern_ops.iter().map(|op| format!("{op:?}")).collect();
            let where_part = if where_filter.is_some() {
                " WHERE ..."
            } else {
                ""
            };
            format!("NestedMatch([{}]{})", ops.join(", "), where_part)
        }
        PipelineOp::ViewScan {
            view_name,
            yields,
            yield_star,
        } => {
            if *yield_star {
                format!("ViewScan({view_name}, yields: [*])")
            } else {
                let cols: Vec<String> = yields
                    .iter()
                    .map(|(name, alias)| match alias {
                        Some(a) => format!("{name} AS {a}"),
                        None => name.to_string(),
                    })
                    .collect();
                format!("ViewScan({view_name}, yields: [{}])", cols.join(", "))
            }
        }
    }
}

fn format_mutation(m: &crate::ast::mutation::MutationOp) -> String {
    use crate::ast::mutation::MutationOp;
    match m {
        MutationOp::SetProperty {
            target, property, ..
        } => {
            format!("SetProperty({}.{})", target.as_str(), property.as_str())
        }
        MutationOp::InsertPattern(p) => {
            format!("InsertPattern({} paths)", p.paths.len())
        }
        MutationOp::SetLabel { target, label } => {
            format!("SetLabel({} IS {})", target.as_str(), label.as_str())
        }
        MutationOp::RemoveLabel { target, label } => {
            format!("RemoveLabel({} IS {})", target.as_str(), label.as_str())
        }
        MutationOp::SetAllProperties { target, properties } => {
            let keys: Vec<&str> = properties.iter().map(|(k, _)| k.as_str()).collect();
            format!(
                "SetAllProperties({} = {{{}}})",
                target.as_str(),
                keys.join(", ")
            )
        }
        MutationOp::RemoveProperty { target, property } => {
            format!("RemoveProperty({}.{})", target.as_str(), property.as_str())
        }
        MutationOp::Delete { target } => format!("Delete({})", target.as_str()),
        MutationOp::DetachDelete { target } => format!("DetachDelete({})", target.as_str()),
        MutationOp::Merge { labels, .. } => {
            let ls: Vec<&str> = labels.iter().map(|l| l.as_str()).collect();
            format!("Merge(:{ls})", ls = ls.join(":"))
        }
    }
}

fn format_label_expr(le: &LabelExpr) -> String {
    match le {
        LabelExpr::Name(name) => name.as_str().to_string(),
        LabelExpr::And(parts) => {
            let ps: Vec<String> = parts.iter().map(format_label_expr).collect();
            ps.join(" & ")
        }
        LabelExpr::Or(parts) => {
            let ps: Vec<String> = parts.iter().map(format_label_expr).collect();
            ps.join(" | ")
        }
        LabelExpr::Not(inner) => format!("!{}", format_label_expr(inner)),
        LabelExpr::Wildcard => "%".to_string(),
        LabelExpr::Concat(parts) => {
            let ps: Vec<String> = parts.iter().map(format_label_expr).collect();
            ps.join(".")
        }
        LabelExpr::Star(inner) => format!("({})*", format_label_expr(inner)),
        LabelExpr::Plus(inner) => format!("({})+", format_label_expr(inner)),
        LabelExpr::Optional(inner) => format!("({})?", format_label_expr(inner)),
    }
}

/// Per-operator profiling stats collected during execution.
#[derive(Debug, Clone)]
pub struct ProfileStats {
    pub operator: String,
    pub rows_in: usize,
    pub rows_out: usize,
    pub duration_us: u64,
}

/// Format plan with profiling stats.
pub fn format_profile(plan: &ExecutionPlan, stats: &[ProfileStats]) -> String {
    let mut out = format_plan(plan);
    out.push_str("\nProfile:\n");
    for s in stats {
        let _ = writeln!(
            out,
            "  {} -- in: {}, out: {}, time: {}\u{00b5}s",
            s.operator, s.rows_in, s.rows_out, s.duration_us
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::statement::GqlStatement;
    use crate::parser::parse_statement;
    use crate::planner;
    use selene_core::{IStr, LabelSet, NodeId, PropertyMap, Value};
    use selene_graph::SeleneGraph;
    use smol_str::SmolStr;

    fn test_graph() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::String(SmolStr::new("S1"))),
                (IStr::new("temp"), Value::Float(72.5)),
            ]),
        )
        .unwrap();
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::String(SmolStr::new("S2"))),
                (IStr::new("temp"), Value::Float(80.0)),
            ]),
        )
        .unwrap();
        m.create_node(
            LabelSet::from_strs(&["building"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("HQ")))]),
        )
        .unwrap();
        m.create_edge(
            NodeId(3),
            IStr::new("contains"),
            NodeId(1),
            PropertyMap::new(),
        )
        .unwrap();
        m.create_edge(
            NodeId(3),
            IStr::new("contains"),
            NodeId(2),
            PropertyMap::new(),
        )
        .unwrap();
        m.commit(0).unwrap();
        g
    }

    #[test]
    fn explain_simple_match() {
        let g = test_graph();
        let stmt = parse_statement("MATCH (s:sensor) RETURN s.name AS name").unwrap();
        let plan = match &stmt {
            GqlStatement::Query(q) => planner::plan_query(q, &g).unwrap(),
            _ => panic!("expected query"),
        };
        let text = format_plan(&plan);
        assert!(text.contains("LabelScan"));
        assert!(text.contains("sensor"));
        assert!(text.contains("Return"));
        assert!(text.contains("NAME"));
    }

    #[test]
    fn explain_filtered_query() {
        let g = test_graph();
        let stmt =
            parse_statement("MATCH (s:sensor) FILTER s.temp > 75 RETURN s.name AS name").unwrap();
        let plan = match &stmt {
            GqlStatement::Query(q) => planner::plan_query(q, &g).unwrap(),
            _ => panic!("expected query"),
        };
        let text = format_plan(&plan);
        // Inequality filter is now pushed into LabelScan's property_filters
        assert!(
            text.contains("filters=") || text.contains("Filter"),
            "got: {text}"
        );
    }

    #[test]
    fn explain_multi_hop() {
        let g = test_graph();
        let stmt = parse_statement("MATCH (b:building)-[:contains]->(s:sensor) RETURN b.name AS building, s.name AS sensor").unwrap();
        let plan = match &stmt {
            GqlStatement::Query(q) => planner::plan_query(q, &g).unwrap(),
            _ => panic!("expected query"),
        };
        let text = format_plan(&plan);
        assert!(text.contains("LabelScan"));
        assert!(text.contains("Expand"));
    }

    #[test]
    fn explain_with_limit() {
        let g = test_graph();
        let stmt = parse_statement("MATCH (s:sensor) RETURN s.name AS name LIMIT 10").unwrap();
        let plan = match &stmt {
            GqlStatement::Query(q) => planner::plan_query(q, &g).unwrap(),
            _ => panic!("expected query"),
        };
        let text = format_plan(&plan);
        assert!(text.contains("Limit(10)"));
    }
}
