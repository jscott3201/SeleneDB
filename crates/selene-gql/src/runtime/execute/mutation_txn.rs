//! Transaction-path mutation handlers: execute mutations within an explicit
//! `TransactionHandle` instead of the auto-commit `SharedGraph::write()` path.

use std::collections::HashMap;

use roaring::RoaringBitmap;
use selene_core::{EdgeId, IStr, LabelSet, NodeId, PropertyMap, Value};

use super::mutation::{
    build_insert_node_data_with_binding, maybe_intern_value, propagate_insert_vars,
};
use crate::ast::mutation::MutationOp;
use crate::runtime::eval::{self};
use crate::types::binding::{Binding, BoundValue};
use crate::types::error::{GqlError, MutationStats};

/// Execute a single mutation within a transaction handle.
pub(super) fn execute_single_mutation_in_txn(
    txn: &mut selene_graph::TransactionHandle<'_>,
    mutation: &MutationOp,
    bindings: &mut Vec<Binding>,
    scope: Option<&RoaringBitmap>,
    stats: &mut MutationStats,
    parameters: Option<&super::ParameterMap>,
) -> Result<(), GqlError> {
    match mutation {
        MutationOp::SetProperty {
            target,
            property,
            value,
        } => {
            txn_set_property(
                txn, bindings, scope, stats, *target, *property, value, parameters,
            )?;
        }

        MutationOp::SetAllProperties { target, properties } => {
            txn_set_all_properties(txn, bindings, scope, stats, *target, properties, parameters)?;
        }
        MutationOp::InsertPattern(pattern) => {
            txn_insert_pattern(txn, bindings, stats, pattern, parameters)?;
        }
        MutationOp::SetLabel { target, label } => {
            let ids: Vec<NodeId> = bindings
                .iter()
                .map(|b| b.get_node_id(target))
                .collect::<Result<_, _>>()?;
            for node_id in ids {
                let lbl = *label;
                txn.mutate(move |m| m.add_label(node_id, lbl))?;
            }
        }
        MutationOp::RemoveLabel { target, label } => {
            let ids: Vec<NodeId> = bindings
                .iter()
                .map(|b| b.get_node_id(target))
                .collect::<Result<_, _>>()?;
            for node_id in ids {
                let lbl_str = label.as_str().to_string();
                txn.mutate(move |m| {
                    m.remove_label(node_id, &lbl_str)?;
                    Ok(())
                })?;
            }
        }
        MutationOp::Delete { target } => {
            txn_delete(txn, bindings, scope, stats, *target, false)?;
        }
        MutationOp::DetachDelete { target } => {
            txn_delete(txn, bindings, scope, stats, *target, true)?;
        }
        MutationOp::RemoveProperty { target, property } => {
            txn_remove_property(txn, bindings, scope, stats, *target, *property)?;
        }
        MutationOp::Merge {
            var,
            labels,
            properties,
            on_create,
            on_match,
        } => {
            let node_id = txn_merge(
                txn, stats, labels, properties, on_create, on_match, parameters,
            )?;
            if let Some(var_name) = var {
                let bindings_was_empty = bindings.is_empty();
                let mut node_var_map = HashMap::new();
                node_var_map.insert(*var_name, node_id);
                propagate_insert_vars(
                    bindings,
                    &node_var_map,
                    &HashMap::new(),
                    &std::collections::HashSet::new(),
                    bindings_was_empty,
                );
            }
        }
    }
    Ok(())
}

// ── Transaction path per-type handlers ──────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn txn_set_property(
    txn: &mut selene_graph::TransactionHandle<'_>,
    bindings: &[Binding],
    scope: Option<&RoaringBitmap>,
    stats: &mut MutationStats,
    target: IStr,
    property: IStr,
    value: &crate::ast::expr::Expr,
    parameters: Option<&super::ParameterMap>,
) -> Result<(), GqlError> {
    use crate::runtime::scope::check_scope;
    let pairs: Vec<(bool, u64, Value)> = {
        let graph = txn.graph();
        let func_reg = crate::runtime::functions::FunctionRegistry::builtins();
        let mut ctx = eval::EvalContext::new(graph, func_reg);
        if let Some(params) = parameters {
            ctx = ctx.with_parameters(params);
        }
        bindings
            .iter()
            .map(|binding| {
                let val = eval::eval_expr_ctx(value, binding, &ctx)?;
                let storage_val = Value::try_from(&val)?;
                match binding.get(&target) {
                    Some(BoundValue::Node(node_id)) => {
                        if let Some(s) = scope {
                            check_scope(*node_id, Some(s))?;
                        }
                        let labels: Vec<IStr> = graph
                            .get_node(*node_id)
                            .map(|n| n.labels.iter().collect())
                            .unwrap_or_default();
                        let storage_val = maybe_intern_value(graph, &labels, property, storage_val);
                        Ok((true, node_id.0, storage_val))
                    }
                    Some(BoundValue::Edge(edge_id)) => {
                        if let Some(s) = scope
                            && let Some(edge) = graph.get_edge(*edge_id)
                        {
                            check_scope(edge.source, Some(s))?;
                            check_scope(edge.target, Some(s))?;
                        }
                        let edge_label: Vec<IStr> = graph
                            .get_edge(*edge_id)
                            .map(|e| vec![e.label])
                            .unwrap_or_default();
                        let storage_val =
                            maybe_intern_value(graph, &edge_label, property, storage_val);
                        Ok((false, edge_id.0, storage_val))
                    }
                    _ => Err(GqlError::type_error(format!(
                        "variable '{}' not bound to node or edge",
                        target.as_str()
                    ))),
                }
            })
            .collect::<Result<_, GqlError>>()?
    };
    for (is_node, id, storage_val) in pairs {
        let prop = property;
        if is_node {
            txn.mutate(move |m| m.set_property(NodeId(id), prop, storage_val))?;
        } else {
            txn.mutate(move |m| m.set_edge_property(selene_core::EdgeId(id), prop, storage_val))?;
        }
        stats.properties_set += 1;
    }
    Ok(())
}

#[allow(clippy::type_complexity)]
fn txn_set_all_properties(
    txn: &mut selene_graph::TransactionHandle<'_>,
    bindings: &[Binding],
    scope: Option<&RoaringBitmap>,
    stats: &mut MutationStats,
    target: IStr,
    properties: &[(IStr, crate::ast::expr::Expr)],
    parameters: Option<&super::ParameterMap>,
) -> Result<(), GqlError> {
    use crate::runtime::scope::check_scope;
    let pairs: Vec<(NodeId, Vec<IStr>, Vec<(IStr, Value)>)> = {
        let graph = txn.graph();
        let func_reg = crate::runtime::functions::FunctionRegistry::builtins();
        let mut ctx = eval::EvalContext::new(graph, func_reg);
        if let Some(params) = parameters {
            ctx = ctx.with_parameters(params);
        }
        bindings
            .iter()
            .map(|binding| {
                let node_id = binding.get_node_id(&target)?;
                if let Some(s) = scope {
                    check_scope(node_id, Some(s))?;
                }
                let existing_keys: Vec<IStr> = graph
                    .get_node(node_id)
                    .map(|n| n.properties.iter().map(|(k, _)| *k).collect())
                    .unwrap_or_default();
                let labels: Vec<IStr> = graph
                    .get_node(node_id)
                    .map(|n| n.labels.iter().collect())
                    .unwrap_or_default();
                let new_props: Vec<(IStr, Value)> = properties
                    .iter()
                    .map(|(key, expr)| {
                        let val = eval::eval_expr_ctx(expr, binding, &ctx)?;
                        let storage_val = Value::try_from(&val)?;
                        let storage_val = maybe_intern_value(graph, &labels, *key, storage_val);
                        Ok((*key, storage_val))
                    })
                    .collect::<Result<_, GqlError>>()?;
                Ok((node_id, existing_keys, new_props))
            })
            .collect::<Result<_, GqlError>>()?
    };
    for (node_id, existing_keys, new_props) in pairs {
        for key in existing_keys {
            let key_str = key.as_str().to_string();
            txn.mutate(move |m| {
                m.remove_property(node_id, &key_str)?;
                Ok(())
            })?;
        }
        for (key, val) in new_props {
            txn.mutate(move |m| m.set_property(node_id, key, val))?;
        }
        stats.properties_set += properties.len();
    }
    Ok(())
}

fn txn_insert_pattern(
    txn: &mut selene_graph::TransactionHandle<'_>,
    bindings: &mut Vec<Binding>,
    stats: &mut MutationStats,
    pattern: &crate::ast::mutation::InsertGraphPattern,
    parameters: Option<&super::ParameterMap>,
) -> Result<(), GqlError> {
    use crate::ast::mutation::InsertElement;
    use crate::ast::pattern::EdgeDirection;

    let mut var_map: HashMap<IStr, NodeId> = HashMap::new();
    let mut edge_var_map: HashMap<IStr, EdgeId> = HashMap::new();

    for binding in bindings.iter() {
        for (key, val) in binding.iter() {
            if let BoundValue::Node(nid) = val {
                var_map.entry(*key).or_insert(*nid);
            }
        }
    }

    let existing_vars: std::collections::HashSet<IStr> = bindings
        .iter()
        .flat_map(|b| b.iter().map(|(k, _)| *k))
        .collect();
    let bindings_was_empty = bindings.is_empty();

    {
        let empty_binding = Binding::empty();
        let binding_ref = bindings.first().map(|b| b as &Binding);

        for path in &pattern.paths {
            let mut prev_node_id: Option<NodeId> = None;
            let mut i = 0;
            while i < path.elements.len() {
                match &path.elements[i] {
                    InsertElement::Node {
                        var,
                        labels,
                        properties,
                    } => {
                        let node_id = if let Some(v) = var {
                            if let Some(&existing) = var_map.get(v) {
                                existing
                            } else {
                                let br = binding_ref.unwrap_or(&empty_binding);
                                let (ls, props) = build_insert_node_data_with_binding(
                                    labels,
                                    properties,
                                    txn.graph(),
                                    br,
                                    parameters,
                                )?;
                                let id = txn.mutate(|m| m.create_node(ls, props))?;
                                var_map.insert(*v, id);
                                stats.nodes_created += 1;
                                id
                            }
                        } else {
                            let br = binding_ref.unwrap_or(&empty_binding);
                            let (ls, props) = build_insert_node_data_with_binding(
                                labels,
                                properties,
                                txn.graph(),
                                br,
                                parameters,
                            )?;
                            let id = txn.mutate(|m| m.create_node(ls, props))?;
                            stats.nodes_created += 1;
                            id
                        };
                        prev_node_id = Some(node_id);
                    }
                    InsertElement::Edge {
                        var,
                        label,
                        direction,
                        properties,
                    } => {
                        let src = prev_node_id
                            .ok_or_else(|| GqlError::internal("INSERT edge: no source node"))?;
                        i += 1;
                        if i >= path.elements.len() {
                            return Err(GqlError::internal("INSERT edge: no target node"));
                        }
                        if let InsertElement::Node {
                            var: tgt_var,
                            labels: tgt_labels,
                            properties: tgt_props,
                        } = &path.elements[i]
                        {
                            let tgt = if let Some(v) = tgt_var {
                                if let Some(&existing) = var_map.get(v) {
                                    existing
                                } else {
                                    let br = binding_ref.unwrap_or(&empty_binding);
                                    let (ls, props) = build_insert_node_data_with_binding(
                                        tgt_labels,
                                        tgt_props,
                                        txn.graph(),
                                        br,
                                        parameters,
                                    )?;
                                    let id = txn.mutate(|m| m.create_node(ls, props))?;
                                    var_map.insert(*v, id);
                                    stats.nodes_created += 1;
                                    id
                                }
                            } else {
                                let br = binding_ref.unwrap_or(&empty_binding);
                                let (ls, props) = build_insert_node_data_with_binding(
                                    tgt_labels,
                                    tgt_props,
                                    txn.graph(),
                                    br,
                                    parameters,
                                )?;
                                let id = txn.mutate(|m| m.create_node(ls, props))?;
                                stats.nodes_created += 1;
                                id
                            };
                            let (s, t) = match direction {
                                EdgeDirection::Out => (src, tgt),
                                EdgeDirection::In => (tgt, src),
                                EdgeDirection::Any => (src, tgt),
                            };
                            let edge_label = label.unwrap_or_else(|| IStr::new(""));
                            let edge_label_slice = [edge_label];
                            let br = binding_ref.unwrap_or(&empty_binding);
                            let (_, edge_props) = build_insert_node_data_with_binding(
                                &edge_label_slice,
                                properties,
                                txn.graph(),
                                br,
                                parameters,
                            )?;
                            let eid =
                                txn.mutate(|m| m.create_edge(s, edge_label, t, edge_props))?;
                            stats.edges_created += 1;
                            if let Some(ev) = var {
                                edge_var_map.insert(*ev, eid);
                            }
                            prev_node_id = Some(tgt);
                        }
                    }
                }
                i += 1;
            }
        }
    }

    propagate_insert_vars(
        bindings,
        &var_map,
        &edge_var_map,
        &existing_vars,
        bindings_was_empty,
    );
    Ok(())
}

/// Collect delete targets from bindings (shared by Delete and DetachDelete).
fn collect_delete_targets(
    bindings: &[Binding],
    scope: Option<&RoaringBitmap>,
    target: IStr,
) -> Result<Vec<(bool, u64)>, GqlError> {
    use crate::runtime::scope::check_scope;
    bindings
        .iter()
        .filter_map(|binding| match binding.get(&target) {
            Some(BoundValue::Node(id)) => {
                if let Some(s) = scope
                    && check_scope(*id, Some(s)).is_err()
                {
                    return None;
                }
                Some(Ok((true, id.0)))
            }
            Some(BoundValue::Edge(id)) => Some(Ok((false, id.0))),
            None => None,
            _ => Some(Err(GqlError::type_error(
                "DELETE target must be a node or edge",
            ))),
        })
        .collect::<Result<Vec<_>, _>>()
}

fn txn_delete(
    txn: &mut selene_graph::TransactionHandle<'_>,
    bindings: &[Binding],
    scope: Option<&RoaringBitmap>,
    stats: &mut MutationStats,
    target: IStr,
    detach: bool,
) -> Result<(), GqlError> {
    let ids = collect_delete_targets(bindings, scope, target)?;
    for (is_node, id) in ids {
        if is_node {
            if !detach {
                let degree = {
                    let g = txn.graph();
                    g.outgoing(NodeId(id)).len() + g.incoming(NodeId(id)).len()
                };
                if degree > 0 {
                    return Err(GqlError::InvalidArgument {
                        message: format!(
                            "cannot delete node {id} with {degree} incident edges, use DETACH DELETE"
                        ),
                    });
                }
            }
            txn.mutate(|m| m.delete_node(NodeId(id)))?;
            stats.nodes_deleted += 1;
        } else {
            txn.mutate(|m| m.delete_edge(selene_core::EdgeId(id)))?;
            stats.edges_deleted += 1;
        }
    }
    Ok(())
}

fn txn_remove_property(
    txn: &mut selene_graph::TransactionHandle<'_>,
    bindings: &[Binding],
    scope: Option<&RoaringBitmap>,
    stats: &mut MutationStats,
    target: IStr,
    property: IStr,
) -> Result<(), GqlError> {
    use crate::runtime::scope::check_scope;
    let ids: Vec<(bool, u64)> = {
        let graph = txn.graph();
        bindings
            .iter()
            .map(|binding| match binding.get(&target) {
                Some(BoundValue::Node(node_id)) => {
                    if let Some(s) = scope {
                        check_scope(*node_id, Some(s))?;
                    }
                    Ok((true, node_id.0))
                }
                Some(BoundValue::Edge(edge_id)) => {
                    if let Some(s) = scope
                        && let Some(edge) = graph.get_edge(*edge_id)
                    {
                        check_scope(edge.source, Some(s))?;
                        check_scope(edge.target, Some(s))?;
                    }
                    Ok((false, edge_id.0))
                }
                _ => Err(GqlError::type_error(format!(
                    "variable '{}' not bound to node or edge",
                    target.as_str()
                ))),
            })
            .collect::<Result<_, GqlError>>()?
    };
    for (is_node, id) in ids {
        let prop_str = property.as_str().to_string();
        if is_node {
            txn.mutate(move |m| {
                m.remove_property(NodeId(id), &prop_str)?;
                Ok(())
            })?;
        } else {
            txn.mutate(move |m| {
                m.remove_edge_property(selene_core::EdgeId(id), &prop_str)?;
                Ok(())
            })?;
        }
        stats.properties_removed += 1;
    }
    Ok(())
}

fn txn_merge(
    txn: &mut selene_graph::TransactionHandle<'_>,
    stats: &mut MutationStats,
    labels: &[IStr],
    properties: &[(IStr, crate::ast::expr::Expr)],
    on_create: &[(IStr, IStr, crate::ast::expr::Expr)],
    on_match: &[(IStr, IStr, crate::ast::expr::Expr)],
    parameters: Option<&super::ParameterMap>,
) -> Result<NodeId, GqlError> {
    let (label_set, match_props, existing) = {
        let graph = txn.graph();
        let func_reg = crate::runtime::functions::FunctionRegistry::builtins();
        let mut ctx = eval::EvalContext::new(graph, func_reg);
        if let Some(params) = parameters {
            ctx = ctx.with_parameters(params);
        }
        let label_set = LabelSet::from_strs(&labels.iter().map(|l| l.as_str()).collect::<Vec<_>>());
        let mut match_props = PropertyMap::new();
        for (key, expr) in properties {
            let val = eval::eval_expr_ctx(expr, &Binding::empty(), &ctx)?;
            let storage_val = Value::try_from(&val)?;
            match_props.insert(*key, storage_val);
        }
        let existing = graph.all_node_ids().find(|&nid| {
            if let Some(node) = graph.get_node(nid) {
                let labels_match = label_set.iter().all(|l| node.labels.contains(l));
                let props_match = match_props.iter().all(|(k, v)| {
                    node.properties
                        .get_by_str(k.as_str())
                        .is_some_and(|pv| pv == v)
                });
                labels_match && props_match
            } else {
                false
            }
        });
        (label_set, match_props, existing)
    };
    let result_node_id = if let Some(node_id) = existing {
        let sets: Vec<(IStr, Value)> = {
            let graph = txn.graph();
            let func_reg = crate::runtime::functions::FunctionRegistry::builtins();
            let mut ctx = eval::EvalContext::new(graph, func_reg);
            if let Some(params) = parameters {
                ctx = ctx.with_parameters(params);
            }
            on_match
                .iter()
                .map(|(_target, prop, expr)| {
                    let val = eval::eval_expr_ctx(expr, &Binding::empty(), &ctx)?;
                    let sv = Value::try_from(&val)?;
                    Ok((*prop, sv))
                })
                .collect::<Result<_, GqlError>>()?
        };
        for (prop, sv) in sets {
            txn.mutate(move |m| m.set_property(node_id, prop, sv))?;
            stats.properties_set += 1;
        }
        node_id
    } else {
        let node_id = txn.mutate(|m| m.create_node(label_set, match_props))?;
        stats.nodes_created += 1;
        let sets: Vec<(IStr, Value)> = {
            let graph = txn.graph();
            let func_reg = crate::runtime::functions::FunctionRegistry::builtins();
            let mut ctx = eval::EvalContext::new(graph, func_reg);
            if let Some(params) = parameters {
                ctx = ctx.with_parameters(params);
            }
            on_create
                .iter()
                .map(|(_target, prop, expr)| {
                    let val = eval::eval_expr_ctx(expr, &Binding::empty(), &ctx)?;
                    let sv = Value::try_from(&val)?;
                    Ok((*prop, sv))
                })
                .collect::<Result<_, GqlError>>()?
        };
        for (prop, sv) in sets {
            txn.mutate(move |m| m.set_property(node_id, prop, sv))?;
            stats.properties_set += 1;
        }
        node_id
    };
    Ok(result_node_id)
}
