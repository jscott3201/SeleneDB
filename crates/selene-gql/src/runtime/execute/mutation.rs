//! Mutation execution: INSERT, SET, DELETE, MERGE, DETACH DELETE.

use std::collections::HashMap;

use roaring::RoaringBitmap;
use selene_core::{EdgeId, IStr, LabelSet, NodeId, PropertyMap, Value};
use selene_graph::{SeleneGraph, SharedGraph};

use crate::ast::mutation::{InsertElement, MutationOp};
use crate::ast::pattern::EdgeDirection;
use crate::runtime::eval::{self};
use crate::types::binding::{Binding, BoundValue};
use crate::types::error::{GqlError, MutationStats};

/// If the property's schema has `dictionary: true` and the value is a String,
/// promote it to InternedStr for memory deduplication.
///
/// Checks both node and edge schemas so that dictionary-flagged properties
/// on edges are also interned.
pub(super) fn maybe_intern_value(
    graph: &SeleneGraph,
    labels: &[IStr],
    key: IStr,
    value: Value,
) -> Value {
    if let Value::String(ref s) = value {
        for label in labels {
            // Check node schemas first, then fall back to edge schemas.
            if let Some(schema) = graph.schema().node_schema(label.as_str())
                && let Some(prop_def) = schema.properties.iter().find(|p| *p.name == *key.as_str())
                && prop_def.dictionary
            {
                return Value::InternedStr(IStr::new(s.as_str()));
            }
            if let Some(schema) = graph.schema().edge_schema(label.as_str())
                && let Some(prop_def) = schema.properties.iter().find(|p| *p.name == *key.as_str())
                && prop_def.dictionary
            {
                return Value::InternedStr(IStr::new(s.as_str()));
            }
        }
    }
    value
}

/// Deferred mutation evaluated against the pre-mutation graph snapshot,
/// then applied in a second phase to guarantee two-phase SET semantics.
///
/// Without two-phase evaluation, `SET a.val = b.val, b.val = a.val` would
/// not swap correctly because the first SET would modify the graph before
/// the second SET reads from it.
pub(super) enum DeferredMutation {
    SetProperty {
        node_id: NodeId,
        key: IStr,
        value: Value,
    },
    SetEdgeProperty {
        edge_id: EdgeId,
        key: IStr,
        value: Value,
    },
    SetAllProperties {
        node_id: NodeId,
        props: Vec<(IStr, Value)>,
    },
    SetLabel {
        node_id: NodeId,
        label: IStr,
    },
    RemoveProperty {
        node_id: NodeId,
        key: IStr,
    },
    RemoveEdgeProperty {
        edge_id: EdgeId,
        key: IStr,
    },
    RemoveLabel {
        node_id: NodeId,
        label: IStr,
    },
}

/// Build node labels + properties from INSERT element, evaluating expressions against binding.
pub(super) fn build_insert_node_data_with_binding(
    labels: &[IStr],
    properties: &[(IStr, crate::ast::expr::Expr)],
    graph: &SeleneGraph,
    binding: &Binding,
) -> Result<(LabelSet, PropertyMap), GqlError> {
    let ls = LabelSet::from_strs(&labels.iter().map(|l| l.as_str()).collect::<Vec<_>>());
    let mut props = PropertyMap::new();
    for (key, expr) in properties {
        let val = eval::eval_expr(expr, binding, graph)?;
        let sv = Value::try_from(&val)
            .map_err(|e| GqlError::internal(format!("property value conversion: {e}")))?;
        let sv = maybe_intern_value(graph, labels, *key, sv);
        props.insert(*key, sv);
    }
    Ok((ls, props))
}

/// Walk INSERT pattern paths, calling create_node/create_edge callbacks for each element.
///
/// Shared logic for auto-commit and transaction InsertPattern handling.
/// Callers provide callbacks that wrap their respective mutation APIs.
/// `binding` is `Some` for MATCH+INSERT (expression evaluation against a row),
/// `None` for INSERT-only (evaluates against empty binding).
/// Walk INSERT pattern paths using a TrackedMutation for node/edge creation.
/// Shared between auto-commit and (potentially) transaction paths.
pub(super) fn walk_insert_paths(
    paths: &[crate::ast::mutation::InsertPathPattern],
    binding: Option<&Binding>,
    graph: &SeleneGraph,
    var_map: &mut HashMap<IStr, NodeId>,
    edge_var_map: &mut HashMap<IStr, EdgeId>,
    stats: &mut MutationStats,
    m: &mut selene_graph::TrackedMutation<'_>,
) -> Result<(), selene_graph::GraphError> {
    let empty_binding = Binding::empty();
    let binding_ref = binding.unwrap_or(&empty_binding);

    for path in paths {
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
                            let (ls, props) = build_insert_node_data_with_binding(
                                labels,
                                properties,
                                graph,
                                binding_ref,
                            )
                            .map_err(to_graph_err)?;
                            stats.properties_set += props.len();
                            let id = m.create_node(ls, props)?;
                            var_map.insert(*v, id);
                            stats.nodes_created += 1;
                            id
                        }
                    } else {
                        let (ls, props) = build_insert_node_data_with_binding(
                            labels,
                            properties,
                            graph,
                            binding_ref,
                        )
                        .map_err(to_graph_err)?;
                        stats.properties_set += props.len();
                        let id = m.create_node(ls, props)?;
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
                    let src = prev_node_id.ok_or_else(|| {
                        selene_graph::GraphError::Other("INSERT edge: no source node".into())
                    })?;
                    i += 1;
                    if i >= path.elements.len() {
                        return Err(selene_graph::GraphError::Other(
                            "INSERT edge: no target node".into(),
                        ));
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
                                let (ls, props) = build_insert_node_data_with_binding(
                                    tgt_labels,
                                    tgt_props,
                                    graph,
                                    binding_ref,
                                )
                                .map_err(to_graph_err)?;
                                stats.properties_set += props.len();
                                let id = m.create_node(ls, props)?;
                                var_map.insert(*v, id);
                                stats.nodes_created += 1;
                                id
                            }
                        } else {
                            let (ls, props) = build_insert_node_data_with_binding(
                                tgt_labels,
                                tgt_props,
                                graph,
                                binding_ref,
                            )
                            .map_err(to_graph_err)?;
                            stats.properties_set += props.len();
                            let id = m.create_node(ls, props)?;
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
                        let (_, edge_props) = build_insert_node_data_with_binding(
                            &edge_label_slice,
                            properties,
                            graph,
                            binding_ref,
                        )
                        .map_err(to_graph_err)?;
                        stats.properties_set += edge_props.len();
                        let eid = m.create_edge(s, edge_label, t, edge_props)?;
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
    Ok(())
}

/// Convert any Display-implementing error into a `GraphError::Other`.
///
/// Reduces repeated `.map_err(|e| selene_graph::GraphError::Other(e.to_string()))`
/// calls throughout mutation code.
pub(super) fn to_graph_err(e: impl std::fmt::Display) -> selene_graph::GraphError {
    selene_graph::GraphError::Other(e.to_string())
}

/// Execute all mutations atomically via SharedGraph::write().
///
/// SET/REMOVE mutations use two-phase evaluation: all expressions are evaluated
/// against the pre-mutation graph snapshot first, then applied in a second phase.
/// This guarantees that `SET a.val = b.val, b.val = a.val` swaps correctly.
///
/// For `InsertPattern` mutations, created node and edge variables are propagated
/// back into `bindings` so subsequent RETURN/pipeline operations can reference them.
pub(super) fn execute_mutations_write(
    shared: &SharedGraph,
    mutations: &[MutationOp],
    bindings: &mut Vec<Binding>,
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
) -> Result<(MutationStats, Vec<selene_core::changeset::Change>), GqlError> {
    use crate::runtime::scope::check_scope;

    let mut stats = MutationStats::default();

    // Two-phase SET semantics require a pre-mutation snapshot so that
    // `SET a.val = b.val, b.val = a.val` reads pre-mutation values.
    // For INSERT/DELETE-only mutations we avoid the clone: temporarily
    // move bindings into the snapshot, then restore after the write.
    let needs_snapshot = mutations.iter().any(|m| {
        matches!(
            m,
            MutationOp::SetProperty { .. }
                | MutationOp::SetAllProperties { .. }
                | MutationOp::RemoveProperty { .. }
                | MutationOp::RemoveLabel { .. }
                | MutationOp::SetLabel { .. }
        )
    });
    let (bindings_snapshot, took_bindings) = if needs_snapshot {
        (bindings.clone(), false)
    } else {
        (std::mem::take(bindings), true)
    };

    // Single write() call wrapping all mutations for atomicity.
    // The closure returns (node_var_map, edge_var_map) so we can update bindings
    // after the write completes.
    let (var_maps, mut changes) = shared
        .write(|m| {
            let mut node_var_map: HashMap<IStr, NodeId> = HashMap::new();
            let mut edge_var_map: HashMap<IStr, EdgeId> = HashMap::new();
            // Per-row INSERT variable maps for proper binding propagation
            let mut per_row_maps: Vec<(HashMap<IStr, NodeId>, HashMap<IStr, EdgeId>)> = Vec::new();

            // ── Evaluate all mutations (immediate + deferred) ─────────
            // SET/REMOVE mutations are collected into `deferred` against the
            // pre-mutation snapshot, then applied after.  This guarantees
            // `SET a.val = b.val, b.val = a.val` swaps correctly.
            let mut deferred: Vec<DeferredMutation> = Vec::new();
            let mut deleted_nodes: std::collections::HashSet<NodeId> =
                std::collections::HashSet::new();
            let mut deleted_edges: std::collections::HashSet<EdgeId> =
                std::collections::HashSet::new();

            for mutation in mutations {
                match mutation {
                    MutationOp::InsertPattern(pattern) => {
                        // Each MATCH row produces its own INSERT (ISO GQL semantics).
                        // For INSERT without MATCH, bindings is empty; run once.
                        let rows: Vec<&Binding> = if bindings_snapshot.is_empty() {
                            vec![]
                        } else {
                            bindings_snapshot.iter().collect()
                        };
                        let run_once = rows.is_empty();
                        let iter_rows: Box<dyn Iterator<Item = Option<&Binding>>> = if run_once {
                            Box::new(std::iter::once(None))
                        } else {
                            Box::new(rows.into_iter().map(Some))
                        };

                        for opt_binding in iter_rows {
                            let mut row_node_map: HashMap<IStr, NodeId> = HashMap::new();
                            if let Some(binding) = opt_binding {
                                for (key, val) in binding.iter() {
                                    if let BoundValue::Node(nid) = val {
                                        row_node_map.insert(*key, *nid);
                                    }
                                }
                            }

                            // Snapshot edge_var_map before this row's INSERT
                            // so we can capture per-row edge variables.
                            let edge_snapshot: HashMap<IStr, EdgeId> = edge_var_map.clone();

                            walk_insert_paths(
                                &pattern.paths,
                                opt_binding,
                                graph,
                                &mut row_node_map,
                                &mut edge_var_map,
                                &mut stats,
                                m,
                            )?;

                            // Diff: edges created by this row's INSERT
                            let row_edges: HashMap<IStr, EdgeId> = edge_var_map
                                .iter()
                                .filter(|(k, v)| edge_snapshot.get(k) != Some(v))
                                .map(|(k, v)| (*k, *v))
                                .collect();

                            per_row_maps.push((row_node_map.clone(), row_edges));
                            for (k, v) in &row_node_map {
                                node_var_map.insert(*k, *v);
                            }
                        }
                    }
                    // ── Evaluate SET/REMOVE against pre-mutation snapshot ──
                    MutationOp::SetProperty {
                        target,
                        property,
                        value,
                    } => {
                        for binding in &bindings_snapshot {
                            let val = eval::eval_expr(value, binding, graph)
                                .map_err(to_graph_err)?;
                            let storage_val = Value::try_from(&val)
                                .map_err(to_graph_err)?;
                            match binding.get(target) {
                                Some(BoundValue::Node(node_id)) => {
                                    if let Some(s) = scope {
                                        check_scope(*node_id, Some(s))
                                            .map_err(to_graph_err)?;
                                    }
                                    deferred.push(DeferredMutation::SetProperty {
                                        node_id: *node_id,
                                        key: *property,
                                        value: storage_val,
                                    });
                                }
                                Some(BoundValue::Edge(edge_id)) => {
                                    if let Some(s) = scope
                                        && let Some(edge) = graph.get_edge(*edge_id) {
                                            check_scope(edge.source, Some(s))
                                                .map_err(to_graph_err)?;
                                            check_scope(edge.target, Some(s))
                                                .map_err(to_graph_err)?;
                                        }
                                    deferred.push(DeferredMutation::SetEdgeProperty {
                                        edge_id: *edge_id,
                                        key: *property,
                                        value: storage_val,
                                    });
                                }
                                _ => {
                                    return Err(selene_graph::GraphError::Other(
                                        format!("SET: variable '{}' not bound to node or edge", target.as_str()),
                                    ));
                                }
                            }
                            stats.properties_set += 1;
                        }
                    }
                    MutationOp::SetAllProperties { target, properties } => {
                        for binding in &bindings_snapshot {
                            let node_id = binding
                                .get_node_id(target)
                                .map_err(to_graph_err)?;
                            if let Some(s) = scope {
                                check_scope(node_id, Some(s))
                                    .map_err(to_graph_err)?;
                            }
                            // Evaluate all property expressions against snapshot
                            let mut prop_pairs = Vec::new();
                            for (key, expr) in properties {
                                let val = eval::eval_expr(expr, binding, graph)
                                    .map_err(to_graph_err)?;
                                let storage_val = Value::try_from(&val)
                                    .map_err(to_graph_err)?;
                                prop_pairs.push((*key, storage_val));
                            }
                            deferred.push(DeferredMutation::SetAllProperties {
                                node_id,
                                props: prop_pairs,
                            });
                            stats.properties_set += properties.len();
                        }
                    }
                    MutationOp::RemoveProperty { target, property } => {
                        for binding in &bindings_snapshot {
                            match binding.get(target) {
                                Some(BoundValue::Node(node_id)) => {
                                    if let Some(s) = scope {
                                        check_scope(*node_id, Some(s))
                                            .map_err(to_graph_err)?;
                                    }
                                    deferred.push(DeferredMutation::RemoveProperty {
                                        node_id: *node_id,
                                        key: *property,
                                    });
                                    stats.properties_removed += 1;
                                }
                                Some(BoundValue::Edge(edge_id)) => {
                                    if let Some(s) = scope
                                        && let Some(edge) = graph.get_edge(*edge_id) {
                                            check_scope(edge.source, Some(s))
                                                .map_err(to_graph_err)?;
                                            check_scope(edge.target, Some(s))
                                                .map_err(to_graph_err)?;
                                        }
                                    deferred.push(DeferredMutation::RemoveEdgeProperty {
                                        edge_id: *edge_id,
                                        key: *property,
                                    });
                                    stats.properties_removed += 1;
                                }
                                _ => {}
                            }
                        }
                    }
                    MutationOp::SetLabel { target, label } => {
                        for binding in &bindings_snapshot {
                            let node_id = binding
                                .get_node_id(target)
                                .map_err(to_graph_err)?;
                            if let Some(s) = scope {
                                check_scope(node_id, Some(s))
                                    .map_err(to_graph_err)?;
                            }
                            deferred.push(DeferredMutation::SetLabel {
                                node_id,
                                label: *label,
                            });
                        }
                    }
                    MutationOp::RemoveLabel { target, label } => {
                        for binding in &bindings_snapshot {
                            let node_id = binding
                                .get_node_id(target)
                                .map_err(to_graph_err)?;
                            if let Some(s) = scope {
                                check_scope(node_id, Some(s))
                                    .map_err(to_graph_err)?;
                            }
                            deferred.push(DeferredMutation::RemoveLabel {
                                node_id,
                                label: *label,
                            });
                        }
                    }
                    MutationOp::Delete { target } => {
                        for binding in &bindings_snapshot {
                            match binding.get(target) {
                                Some(BoundValue::Node(id)) => {
                                    if let Some(s) = scope {
                                        check_scope(*id, Some(s)).map_err(|e| {
                                            selene_graph::GraphError::Other(e.to_string())
                                        })?;
                                    }
                                    // Check degree against the mutated graph (not the
                                    // pre-mutation snapshot) so prior edge deletions in
                                    // the same batch are visible, e.g. DELETE e, a.
                                    let degree = m.graph().outgoing(*id).len() + m.graph().incoming(*id).len();
                                    if degree > 0 {
                                        return Err(selene_graph::GraphError::Other(
                                            format!("cannot delete node {} with {degree} incident edges, use DETACH DELETE", id.0)
                                        ));
                                    }
                                    m.delete_node(*id)?;
                                    deleted_nodes.insert(*id);
                                    stats.nodes_deleted += 1;
                                }
                                Some(BoundValue::Edge(id)) => {
                                    m.delete_edge(*id)?;
                                    deleted_edges.insert(*id);
                                    stats.edges_deleted += 1;
                                }
                                None => {}  // unbound variable, no-op (validated by planner)
                                _ => {
                                    return Err(selene_graph::GraphError::Other(
                                        "DELETE target must be a node or edge".to_string(),
                                    ));
                                }
                            }
                        }
                    }
                    MutationOp::DetachDelete { target } => {
                        for binding in &bindings_snapshot {
                            match binding.get(target) {
                                Some(BoundValue::Node(id)) => {
                                    if let Some(s) = scope {
                                        check_scope(*id, Some(s)).map_err(|e| {
                                            selene_graph::GraphError::Other(e.to_string())
                                        })?;
                                    }
                                    m.delete_node(*id)?; // cascades edges
                                    deleted_nodes.insert(*id);
                                    stats.nodes_deleted += 1;
                                }
                                Some(BoundValue::Edge(id)) => {
                                    m.delete_edge(*id)?;
                                    deleted_edges.insert(*id);
                                    stats.edges_deleted += 1;
                                }
                                None => {}  // unbound variable, no-op (validated by planner)
                                _ => {
                                    return Err(selene_graph::GraphError::Other(
                                        "DELETE target must be a node or edge".to_string(),
                                    ));
                                }
                            }
                        }
                    }
                    MutationOp::Merge { var, labels, properties, on_create, on_match } => {
                        // Try to find existing node matching labels + properties
                        let label_set = LabelSet::from_strs(
                            &labels.iter().map(|l| l.as_str()).collect::<Vec<_>>(),
                        );
                        let label_slice: Vec<IStr> = labels.clone();
                        // Build property map for matching (use live graph, not snapshot)
                        let mut match_props = PropertyMap::new();
                        for (key, expr) in properties {
                            let val =
                                eval::eval_expr(expr, &Binding::empty(), m.graph())
                                    .map_err(to_graph_err)?;
                            let sv =
                                Value::try_from(&val).map_err(to_graph_err)?;
                            let sv = maybe_intern_value(m.graph(), &label_slice, *key, sv);
                            match_props.insert(*key, sv);
                        }

                        // Use label bitmap to narrow the search instead of scanning all nodes.
                        // This is O(matching_labels) instead of O(all_nodes).
                        let existing = {
                            let target_graph = m.graph();
                            let mut candidate_bitmap: Option<RoaringBitmap> = None;
                            for label in &label_slice {
                                if let Some(bm) = target_graph.label_bitmap(label.as_str()) {
                                    candidate_bitmap = Some(match candidate_bitmap {
                                        None => bm.clone(),
                                        Some(acc) => acc & bm,
                                    });
                                } else {
                                    // Label not in graph; no possible match.
                                    candidate_bitmap = None;
                                    break;
                                }
                            }

                            candidate_bitmap.and_then(|bm| {
                                bm.iter().find_map(|nid_u32| {
                                    let nid = NodeId(u64::from(nid_u32));
                                    let node = target_graph.get_node(nid)?;
                                    let props_match = match_props.iter().all(|(k, v)| {
                                        node.properties
                                            .get_by_str(k.as_str())
                                            .is_some_and(|pv| pv == v)
                                    });
                                    if props_match { Some(nid) } else { None }
                                })
                            })
                        };

                        let result_node_id;
                        if let Some(node_id) = existing {
                            result_node_id = node_id;
                            // ON MATCH: apply property sets
                            for (_target, prop, expr) in on_match {
                                let val =
                                    eval::eval_expr(expr, &Binding::empty(), m.graph())
                                        .map_err(to_graph_err)?;
                                let sv =
                                    Value::try_from(&val).map_err(to_graph_err)?;
                                let sv =
                                    maybe_intern_value(m.graph(), &label_slice, *prop, sv);
                                m.set_property(node_id, *prop, sv)?;
                                stats.properties_set += 1;
                            }
                        } else {
                            // CREATE: insert new node with properties
                            result_node_id = m.create_node(label_set, match_props)?;
                            stats.nodes_created += 1;
                            // ON CREATE: apply additional property sets
                            for (_target, prop, expr) in on_create {
                                let val =
                                    eval::eval_expr(expr, &Binding::empty(), m.graph())
                                        .map_err(to_graph_err)?;
                                let sv =
                                    Value::try_from(&val).map_err(to_graph_err)?;
                                let sv =
                                    maybe_intern_value(m.graph(), &label_slice, *prop, sv);
                                m.set_property(result_node_id, *prop, sv)?;
                                stats.properties_set += 1;
                            }
                        }
                        // Bind variable for RETURN clause access
                        if let Some(var_name) = var {
                            node_var_map.insert(*var_name, result_node_id);
                        }
                    }
                }
            }

            // ── Apply deferred SET/REMOVE mutations ──────────────────
            // Skip any deferred mutation targeting a deleted node/edge.
            for dm in deferred {
                match dm {
                    DeferredMutation::SetProperty { node_id, key, value } => {
                        if !deleted_nodes.contains(&node_id) {
                            let value = if let Value::String(ref _s) = value {
                                if let Some(node) = m.graph().get_node(node_id) {
                                    let labels: Vec<IStr> = node.labels.iter().collect();
                                    maybe_intern_value(m.graph(), &labels, key, value)
                                } else {
                                    value
                                }
                            } else {
                                value
                            };
                            m.set_property(node_id, key, value)?;
                        }
                    }
                    DeferredMutation::SetEdgeProperty { edge_id, key, value } => {
                        if !deleted_edges.contains(&edge_id) {
                            let value = if let Value::String(ref _s) = value {
                                if let Some(edge) = m.graph().get_edge(edge_id) {
                                    let labels = vec![edge.label];
                                    maybe_intern_value(m.graph(), &labels, key, value)
                                } else {
                                    value
                                }
                            } else {
                                value
                            };
                            m.set_edge_property(edge_id, key, value)?;
                        }
                    }
                    DeferredMutation::SetAllProperties { node_id, props } => {
                        if !deleted_nodes.contains(&node_id) {
                            // Remove all existing properties from the LIVE graph state
                            // (not the pre-mutation snapshot). This ensures properties
                            // added by earlier deferred mutations are also removed.
                            let existing_keys: Vec<IStr> = m
                                .graph()
                                .get_node(node_id)
                                .map(|n| n.properties.iter().map(|(k, _)| *k).collect())
                                .unwrap_or_default();
                            for key in &existing_keys {
                                m.remove_property(node_id, key.as_str())?;
                            }
                            // Collect labels for dictionary interning
                            let labels: Vec<IStr> = m
                                .graph()
                                .get_node(node_id)
                                .map(|n| n.labels.iter().collect())
                                .unwrap_or_default();
                            // Set new properties, applying dictionary interning
                            for (key, value) in props {
                                let value = maybe_intern_value(m.graph(), &labels, key, value);
                                m.set_property(node_id, key, value)?;
                            }
                        }
                    }
                    DeferredMutation::SetLabel { node_id, label } => {
                        if !deleted_nodes.contains(&node_id) {
                            m.add_label(node_id, label)?;
                        }
                    }
                    DeferredMutation::RemoveProperty { node_id, key } => {
                        if !deleted_nodes.contains(&node_id) {
                            m.remove_property(node_id, key.as_str())?;
                        }
                    }
                    DeferredMutation::RemoveEdgeProperty { edge_id, key } => {
                        if !deleted_edges.contains(&edge_id) {
                            m.remove_edge_property(edge_id, key.as_str())?;
                        }
                    }
                    DeferredMutation::RemoveLabel { node_id, label } => {
                        if !deleted_nodes.contains(&node_id) {
                            m.remove_label(node_id, label.as_str())?;
                        }
                    }
                }
            }

            Ok((node_var_map, edge_var_map, per_row_maps))
        })
        .map_err(GqlError::from)?;

    // Restore bindings if we moved them into the snapshot (non-SET path).
    if took_bindings {
        *bindings = bindings_snapshot;
    }

    // Propagate INSERT variables into bindings for RETURN access.
    let (node_var_map, edge_var_map, per_row_maps) = var_maps;
    if !node_var_map.is_empty() || !edge_var_map.is_empty() {
        // Filter to only newly created variables (not seeded from existing bindings)
        let existing_vars: std::collections::HashSet<IStr> = bindings
            .iter()
            .flat_map(|b| b.iter().map(|(k, _)| *k))
            .collect();

        if bindings.is_empty() {
            // INSERT without MATCH: create a new binding from all INSERT variables
            let mut new_binding = Binding::empty();
            for (var, node_id) in &node_var_map {
                if !existing_vars.contains(var) {
                    new_binding.bind(*var, BoundValue::Node(*node_id));
                }
            }
            for (var, edge_id) in &edge_var_map {
                if !existing_vars.contains(var) {
                    new_binding.bind(*var, BoundValue::Edge(*edge_id));
                }
            }
            bindings.push(new_binding);
        } else if per_row_maps.len() == bindings.len() {
            // MATCH + INSERT with per-row maps: each binding gets its own row's variables
            for (binding, (row_nodes, row_edges)) in bindings.iter_mut().zip(per_row_maps.iter()) {
                for (var, node_id) in row_nodes {
                    if binding.get(var).is_none() {
                        binding.bind(*var, BoundValue::Node(*node_id));
                    }
                }
                for (var, edge_id) in row_edges {
                    if binding.get(var).is_none() {
                        binding.bind(*var, BoundValue::Edge(*edge_id));
                    }
                }
            }
        } else {
            // Fallback: augment each existing binding with shared INSERT variables
            for binding in bindings.iter_mut() {
                for (var, node_id) in &node_var_map {
                    if binding.get(var).is_none() {
                        binding.bind(*var, BoundValue::Node(*node_id));
                    }
                }
                for (var, edge_id) in &edge_var_map {
                    if binding.get(var).is_none() {
                        binding.bind(*var, BoundValue::Edge(*edge_id));
                    }
                }
            }
        }
    }

    // Evaluate triggers against committed changes.
    // Uses inner() for direct graph access since evaluate_triggers creates
    // its own TrackedMutations internally for trigger actions.
    // This is a second write lock. Trigger changes are not in the same
    // WAL entry as the initial mutation. On recovery, triggers re-fire
    // deterministically from WAL replay (once trigger persistence is added).
    // Trigger failures are non-fatal: the primary mutation is already committed
    // and visible to readers. Propagating the error would mislead the caller
    // into thinking the mutation failed when it already succeeded.
    if !changes.is_empty() {
        let mut guard = shared.inner().write();
        if !guard.trigger_registry().is_empty() {
            match crate::runtime::triggers::evaluate_triggers(&mut guard, &changes, 0) {
                Ok(trigger_changes) => {
                    drop(guard);
                    shared.publish_snapshot();
                    // Append trigger-generated changes so the ops layer can
                    // persist them to WAL and broadcast via changelog.
                    changes.extend(trigger_changes);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "trigger evaluation failed after committed mutation");
                }
            }
        }
    }

    Ok((stats, changes))
}

/// Propagate INSERT-created variables into bindings for RETURN access.
pub(super) fn propagate_insert_vars(
    bindings: &mut Vec<Binding>,
    node_var_map: &HashMap<IStr, NodeId>,
    edge_var_map: &HashMap<IStr, EdgeId>,
    existing_vars: &std::collections::HashSet<IStr>,
    bindings_was_empty: bool,
) {
    if node_var_map.is_empty() && edge_var_map.is_empty() {
        return;
    }
    if bindings_was_empty {
        let mut new_binding = Binding::empty();
        for (var, node_id) in node_var_map {
            if !existing_vars.contains(var) {
                new_binding.bind(*var, BoundValue::Node(*node_id));
            }
        }
        for (var, edge_id) in edge_var_map {
            if !existing_vars.contains(var) {
                new_binding.bind(*var, BoundValue::Edge(*edge_id));
            }
        }
        bindings.push(new_binding);
    } else {
        for binding in bindings.iter_mut() {
            for (var, node_id) in node_var_map {
                if binding.get(var).is_none() {
                    binding.bind(*var, BoundValue::Node(*node_id));
                }
            }
            for (var, edge_id) in edge_var_map {
                if binding.get(var).is_none() {
                    binding.bind(*var, BoundValue::Edge(*edge_id));
                }
            }
        }
    }
}

/// Count mutations for stats (without actually executing them).
pub(super) fn count_mutation(mutation: &MutationOp, row_count: usize, stats: &mut MutationStats) {
    match mutation {
        MutationOp::SetProperty { .. } => stats.properties_set += row_count,
        MutationOp::SetAllProperties { properties, .. } => {
            stats.properties_set += row_count * properties.len();
        }
        MutationOp::RemoveProperty { .. } => stats.properties_removed += row_count,
        MutationOp::InsertPattern(p) => {
            for path in &p.paths {
                for elem in &path.elements {
                    match elem {
                        InsertElement::Node { .. } => stats.nodes_created += 1,
                        InsertElement::Edge { .. } => stats.edges_created += 1,
                    }
                }
            }
        }
        MutationOp::SetLabel { .. } | MutationOp::RemoveLabel { .. } => {
            // Label changes don't have dedicated stats counters
        }
        MutationOp::Delete { .. } | MutationOp::DetachDelete { .. } => {
            stats.nodes_deleted += row_count;
        }
        MutationOp::Merge { .. } => {
            stats.nodes_created += 1; // approximate
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::expr::Expr;
    use crate::ast::mutation::{InsertElement, InsertGraphPattern, InsertPathPattern};
    use crate::ast::pattern::EdgeDirection;
    use crate::runtime::execute::MutationBuilder;
    use crate::types::binding::{Binding, BoundValue};
    use crate::types::error::MutationStats;
    use crate::types::value::GqlValue;
    use selene_core::schema::{NodeSchema, PropertyDef, ValueType};
    use selene_core::{EdgeId, IStr, LabelSet, NodeId, PropertyMap, Value};
    use selene_graph::{SeleneGraph, SharedGraph};
    use smol_str::SmolStr;
    use std::collections::HashMap;

    // ── INSERT paths ────────────────────────────────────────────

    #[test]
    fn insert_single_node_with_labels_and_properties() {
        let shared = SharedGraph::new(SeleneGraph::new());
        let result = MutationBuilder::new("INSERT (s:sensor {name: 'TempA', temp: 72.5})")
            .execute(&shared)
            .unwrap();

        assert_eq!(result.mutations.nodes_created, 1);
        assert_eq!(result.mutations.edges_created, 0);

        shared.read(|g| {
            assert_eq!(g.node_count(), 1);
            let node = g.get_node(NodeId(1)).expect("node 1 should exist");
            assert!(node.labels.contains(IStr::new("sensor")));
            assert_eq!(
                node.properties.get_by_str("name"),
                Some(&Value::String(SmolStr::new("TempA")))
            );
        });
    }

    #[test]
    fn insert_node_edge_node_chain() {
        let shared = SharedGraph::new(SeleneGraph::new());
        let result = MutationBuilder::new(
            "INSERT (a:building {name: 'HQ'})-[:contains]->(b:floor {level: 1})",
        )
        .execute(&shared)
        .unwrap();

        assert_eq!(result.mutations.nodes_created, 2);
        assert_eq!(result.mutations.edges_created, 1);

        shared.read(|g| {
            assert_eq!(g.node_count(), 2);
            assert_eq!(g.edge_count(), 1);

            let edge = g.get_edge(EdgeId(1)).expect("edge 1 should exist");
            assert_eq!(edge.label, IStr::new("contains"));
            // Out direction: source=building(1), target=floor(2)
            assert_eq!(edge.source, NodeId(1));
            assert_eq!(edge.target, NodeId(2));
        });
    }

    #[test]
    fn insert_reuses_variable_references() {
        // When the same variable appears twice in an INSERT, the second
        // occurrence should reference the existing node, not create a new one.
        let shared = SharedGraph::new(SeleneGraph::new());
        let result = MutationBuilder::new(
            "INSERT (a:sensor {name: 'S1'})-[:monitors]->(b:zone {name: 'Z1'}), \
             (a)-[:backup]->(c:zone {name: 'Z2'})",
        )
        .execute(&shared)
        .unwrap();

        // a is reused, so only 3 nodes created (a, b, c), not 4
        assert_eq!(result.mutations.nodes_created, 3);
        assert_eq!(result.mutations.edges_created, 2);
        shared.read(|g| {
            assert_eq!(g.node_count(), 3);
        });
    }

    #[test]
    fn insert_edge_with_in_direction() {
        // `<-[:label]-` reverses source and target
        let shared = SharedGraph::new(SeleneGraph::new());
        let result = MutationBuilder::new(
            "INSERT (a:floor {name: 'F1'})<-[:contains]-(b:building {name: 'HQ'})",
        )
        .execute(&shared)
        .unwrap();

        assert_eq!(result.mutations.nodes_created, 2);
        assert_eq!(result.mutations.edges_created, 1);

        shared.read(|g| {
            let edge = g.get_edge(EdgeId(1)).expect("edge should exist");
            // In direction: the arrow points from b to a, so source=b(2), target=a(1)
            assert_eq!(edge.source, NodeId(2));
            assert_eq!(edge.target, NodeId(1));
        });
    }

    #[test]
    fn walk_insert_edge_without_target_returns_error() {
        // Build an InsertPathPattern that has Node + Edge but no trailing Node.
        let path = InsertPathPattern {
            elements: vec![
                InsertElement::Node {
                    var: Some(IStr::new("a")),
                    labels: vec![IStr::new("sensor")],
                    properties: vec![],
                },
                InsertElement::Edge {
                    var: None,
                    label: Some(IStr::new("connects")),
                    direction: EdgeDirection::Out,
                    properties: vec![],
                },
            ],
        };

        let graph = SeleneGraph::new();
        let shared = SharedGraph::new(SeleneGraph::new());
        let mut var_map = HashMap::new();
        let mut edge_var_map = HashMap::new();
        let mut stats = MutationStats::default();

        let result = shared.write(|m| {
            walk_insert_paths(
                &[path],
                None,
                &graph,
                &mut var_map,
                &mut edge_var_map,
                &mut stats,
                m,
            )
        });

        assert!(result.is_err(), "edge without target node should error");
    }

    // ── SET / two-phase semantics ───────────────────────────────

    #[test]
    fn set_property_updates_node() {
        let shared = SharedGraph::new(SeleneGraph::new());
        MutationBuilder::new("INSERT (:sensor {name: 'S1'})")
            .execute(&shared)
            .unwrap();

        let result =
            MutationBuilder::new("MATCH (s:sensor) FILTER s.name = 'S1' SET s.temp = 72.5")
                .execute(&shared)
                .unwrap();

        assert_eq!(result.mutations.properties_set, 1);
        shared.read(|g| {
            let node = g.get_node(NodeId(1)).unwrap();
            assert_eq!(
                node.properties.get_by_str("temp"),
                Some(&Value::Float(72.5))
            );
        });
    }

    #[test]
    fn set_property_on_edge() {
        let shared = SharedGraph::new(SeleneGraph::new());
        MutationBuilder::new(
            "INSERT (a:building {name: 'HQ'})-[e:contains]->(b:floor {name: 'F1'})",
        )
        .execute(&shared)
        .unwrap();

        let result =
            MutationBuilder::new("MATCH (a:building)-[e:contains]->(b:floor) SET e.weight = 1.0")
                .execute(&shared)
                .unwrap();

        assert_eq!(result.mutations.properties_set, 1);
        shared.read(|g| {
            let edge = g.get_edge(EdgeId(1)).unwrap();
            assert_eq!(
                edge.properties.get_by_str("weight"),
                Some(&Value::Float(1.0))
            );
        });
    }

    #[test]
    fn two_phase_swap_semantics() {
        // Two-phase SET evaluation: all RHS expressions are read from the
        // pre-mutation snapshot before any writes occur. This test verifies
        // the invariant by calling execute_mutations_write directly with
        // two SetProperty ops that should swap values.
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::String(SmolStr::new("A"))),
                (IStr::new("val"), Value::Int(10)),
            ]),
        )
        .unwrap(); // Node 1
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::String(SmolStr::new("B"))),
                (IStr::new("val"), Value::Int(20)),
            ]),
        )
        .unwrap(); // Node 2
        m.commit(0).unwrap();

        let shared = SharedGraph::new(g);
        let snapshot = shared.load_snapshot();

        // Build a binding that binds a=Node(1), b=Node(2)
        let mut binding = Binding::empty();
        binding.bind(IStr::new("a"), BoundValue::Node(NodeId(1)));
        binding.bind(IStr::new("b"), BoundValue::Node(NodeId(2)));
        let mut bindings = vec![binding];

        // SET a.val = b.val, SET b.val = a.val
        let mutations = vec![
            MutationOp::SetProperty {
                target: IStr::new("a"),
                property: IStr::new("val"),
                value: Expr::Property(Box::new(Expr::Var(IStr::new("b"))), IStr::new("val")),
            },
            MutationOp::SetProperty {
                target: IStr::new("b"),
                property: IStr::new("val"),
                value: Expr::Property(Box::new(Expr::Var(IStr::new("a"))), IStr::new("val")),
            },
        ];

        let (stats, _changes) =
            execute_mutations_write(&shared, &mutations, &mut bindings, &snapshot, None)
                .expect("swap should succeed");

        assert_eq!(stats.properties_set, 2);

        shared.read(|g| {
            let a = g.get_node(NodeId(1)).unwrap();
            let b = g.get_node(NodeId(2)).unwrap();
            assert_eq!(
                a.properties.get_by_str("val"),
                Some(&Value::Int(20)),
                "a.val should now be 20 (was 10)"
            );
            assert_eq!(
                b.properties.get_by_str("val"),
                Some(&Value::Int(10)),
                "b.val should now be 10 (was 20)"
            );
        });
    }

    #[test]
    fn set_on_unbound_variable_returns_error() {
        let shared = SharedGraph::new(SeleneGraph::new());
        MutationBuilder::new("INSERT (:sensor {name: 'S1'})")
            .execute(&shared)
            .unwrap();

        // 'x' is never bound by the MATCH
        let result = MutationBuilder::new("MATCH (s:sensor) SET x.temp = 100").execute(&shared);

        assert!(result.is_err(), "SET on unbound variable should error");
    }

    #[test]
    fn set_all_properties_replaces_existing() {
        let shared = SharedGraph::new(SeleneGraph::new());
        MutationBuilder::new("INSERT (:sensor {name: 'S1', temp: 72.5, unit: 'F'})")
            .execute(&shared)
            .unwrap();

        // SET s = {status: 'active'} replaces all properties
        let result =
            MutationBuilder::new("MATCH (s:sensor) SET s = {status: 'active'}").execute(&shared);

        assert!(result.is_ok(), "SET all properties should succeed");
        let result = result.unwrap();
        // 1 new property set
        assert_eq!(result.mutations.properties_set, 1);

        shared.read(|g| {
            let node = g.get_node(NodeId(1)).unwrap();
            assert_eq!(
                node.properties.get_by_str("status"),
                Some(&Value::String(SmolStr::new("active")))
            );
            // Old properties should be gone
            assert_eq!(
                node.properties.get_by_str("temp"),
                None,
                "temp should be removed after SET all"
            );
            assert_eq!(
                node.properties.get_by_str("name"),
                None,
                "name should be removed after SET all"
            );
        });
    }

    #[test]
    fn set_label_adds_label() {
        let shared = SharedGraph::new(SeleneGraph::new());
        MutationBuilder::new("INSERT (:sensor {name: 'S1'})")
            .execute(&shared)
            .unwrap();

        MutationBuilder::new("MATCH (s:sensor) SET s IS active")
            .execute(&shared)
            .unwrap();

        shared.read(|g| {
            let node = g.get_node(NodeId(1)).unwrap();
            assert!(
                node.labels.contains(IStr::new("active")),
                "node should have 'active' label after SET IS"
            );
            assert!(
                node.labels.contains(IStr::new("sensor")),
                "original 'sensor' label should still be present"
            );
        });
    }

    #[test]
    fn remove_property_removes_key() {
        let shared = SharedGraph::new(SeleneGraph::new());
        MutationBuilder::new("INSERT (:sensor {name: 'S1', temp: 72.5})")
            .execute(&shared)
            .unwrap();

        let result = MutationBuilder::new("MATCH (s:sensor) REMOVE s.temp")
            .execute(&shared)
            .unwrap();

        assert_eq!(result.mutations.properties_removed, 1);
        shared.read(|g| {
            let node = g.get_node(NodeId(1)).unwrap();
            assert_eq!(
                node.properties.get_by_str("temp"),
                None,
                "temp should be removed"
            );
            assert!(
                node.properties.get_by_str("name").is_some(),
                "name should still exist"
            );
        });
    }

    #[test]
    fn remove_label_removes_label() {
        let shared = SharedGraph::new(SeleneGraph::new());
        // Insert a sensor node, then add the "active" label via SET IS
        MutationBuilder::new("INSERT (:sensor {name: 'S1'})")
            .execute(&shared)
            .unwrap();
        MutationBuilder::new("MATCH (s:sensor) SET s IS active")
            .execute(&shared)
            .unwrap();

        // Verify the label was added
        shared.read(|g| {
            let node = g.get_node(NodeId(1)).unwrap();
            assert!(node.labels.contains(IStr::new("active")));
        });

        // Now remove it
        MutationBuilder::new("MATCH (s:sensor) REMOVE s IS active")
            .execute(&shared)
            .unwrap();

        shared.read(|g| {
            let node = g.get_node(NodeId(1)).unwrap();
            assert!(
                !node.labels.contains(IStr::new("active")),
                "active label should be removed"
            );
            assert!(
                node.labels.contains(IStr::new("sensor")),
                "sensor label should remain"
            );
        });
    }

    // ── DELETE ───────────────────────────────────────────────────

    #[test]
    fn delete_node_removes_from_graph() {
        let shared = SharedGraph::new(SeleneGraph::new());
        MutationBuilder::new("INSERT (:sensor {name: 'S1'})")
            .execute(&shared)
            .unwrap();
        assert_eq!(shared.read(|g| g.node_count()), 1);

        let result = MutationBuilder::new("MATCH (s:sensor) DELETE s")
            .execute(&shared)
            .unwrap();

        assert_eq!(result.mutations.nodes_deleted, 1);
        assert_eq!(shared.read(|g| g.node_count()), 0);
    }

    #[test]
    fn delete_edge_removes_from_graph() {
        let shared = SharedGraph::new(SeleneGraph::new());
        MutationBuilder::new(
            "INSERT (a:building {name: 'HQ'})-[e:contains]->(b:floor {name: 'F1'})",
        )
        .execute(&shared)
        .unwrap();
        assert_eq!(shared.read(|g| g.edge_count()), 1);

        let result = MutationBuilder::new("MATCH (a:building)-[e:contains]->(b:floor) DELETE e")
            .execute(&shared)
            .unwrap();

        assert_eq!(result.mutations.edges_deleted, 1);
        assert_eq!(shared.read(|g| g.edge_count()), 0);
        // Nodes should still exist
        assert_eq!(shared.read(|g| g.node_count()), 2);
    }

    #[test]
    fn delete_node_with_edges_fails() {
        let shared = SharedGraph::new(SeleneGraph::new());
        MutationBuilder::new(
            "INSERT (a:building {name: 'HQ'})-[:contains]->(b:floor {name: 'F1'})",
        )
        .execute(&shared)
        .unwrap();

        let result = MutationBuilder::new("MATCH (a:building) DELETE a").execute(&shared);

        assert!(
            result.is_err(),
            "DELETE on node with incident edges should fail"
        );
        // Graph should be unchanged (atomicity)
        assert_eq!(shared.read(|g| g.node_count()), 2);
        assert_eq!(shared.read(|g| g.edge_count()), 1);
    }

    #[test]
    fn detach_delete_cascades_edges() {
        let shared = SharedGraph::new(SeleneGraph::new());
        MutationBuilder::new(
            "INSERT (a:building {name: 'HQ'})-[:contains]->(b:floor {name: 'F1'})",
        )
        .execute(&shared)
        .unwrap();
        assert_eq!(shared.read(|g| g.node_count()), 2);
        assert_eq!(shared.read(|g| g.edge_count()), 1);

        let result = MutationBuilder::new("MATCH (a:building) DETACH DELETE a")
            .execute(&shared)
            .unwrap();

        assert_eq!(result.mutations.nodes_deleted, 1);
        assert_eq!(shared.read(|g| g.node_count()), 1); // only floor remains
        assert_eq!(shared.read(|g| g.edge_count()), 0); // edge cascaded
    }

    // ── MERGE ───────────────────────────────────────────────────

    #[test]
    fn merge_creates_node_when_no_match() {
        let shared = SharedGraph::new(SeleneGraph::new());

        let result = MutationBuilder::new("MERGE (:sensor {name: 'NewSensor'})").execute(&shared);

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.mutations.nodes_created, 1);

        shared.read(|g| {
            assert_eq!(g.node_count(), 1);
            let node = g.get_node(NodeId(1)).unwrap();
            assert!(node.labels.contains(IStr::new("sensor")));
        });
    }

    #[test]
    fn merge_finds_existing_node_no_duplicate() {
        let shared = SharedGraph::new(SeleneGraph::new());

        // First: create the node
        MutationBuilder::new("INSERT (:sensor {name: 'S1'})")
            .execute(&shared)
            .unwrap();
        assert_eq!(shared.read(|g| g.node_count()), 1);

        // MERGE with same labels + properties should find existing, not create
        let result = MutationBuilder::new("MERGE (:sensor {name: 'S1'})")
            .execute(&shared)
            .unwrap();

        assert_eq!(
            result.mutations.nodes_created, 0,
            "MERGE should not create a duplicate"
        );
        assert_eq!(shared.read(|g| g.node_count()), 1);
    }

    #[test]
    fn merge_on_create_sets_properties_only_on_creation() {
        let shared = SharedGraph::new(SeleneGraph::new());

        let result =
            MutationBuilder::new("MERGE (s:sensor {name: 'S1'}) ON CREATE SET s.status = 'new'")
                .execute(&shared)
                .unwrap();

        assert_eq!(result.mutations.nodes_created, 1);
        shared.read(|g| {
            let node = g.get_node(NodeId(1)).unwrap();
            assert_eq!(
                node.properties.get_by_str("status"),
                Some(&Value::String(SmolStr::new("new")))
            );
        });
    }

    #[test]
    fn merge_on_match_sets_properties_only_on_existing() {
        let shared = SharedGraph::new(SeleneGraph::new());

        // Pre-create the node
        MutationBuilder::new("INSERT (:sensor {name: 'S1', status: 'old'})")
            .execute(&shared)
            .unwrap();

        let result =
            MutationBuilder::new("MERGE (s:sensor {name: 'S1'}) ON MATCH SET s.status = 'updated'")
                .execute(&shared)
                .unwrap();

        assert_eq!(
            result.mutations.nodes_created, 0,
            "should match existing node"
        );
        assert_eq!(result.mutations.properties_set, 1);

        shared.read(|g| {
            let node = g.get_node(NodeId(1)).unwrap();
            assert_eq!(
                node.properties.get_by_str("status"),
                Some(&Value::String(SmolStr::new("updated")))
            );
        });
    }

    // ── Dictionary encoding ─────────────────────────────────────

    #[test]
    fn maybe_intern_promotes_string_with_dictionary_schema() {
        let mut g = SeleneGraph::new();

        // Register a schema with dictionary flag on the "status" property
        let prop = PropertyDef::builder("status", ValueType::String)
            .dictionary()
            .build();
        let schema = NodeSchema::builder("sensor").property(prop).build();
        g.schema_mut()
            .register_node_schema(schema)
            .expect("schema registration");

        let labels = vec![IStr::new("sensor")];
        let key = IStr::new("status");
        let val = Value::String(SmolStr::new("active"));

        let result = maybe_intern_value(&g, &labels, key, val);

        assert!(
            matches!(result, Value::InternedStr(_)),
            "dictionary-flagged String should be promoted to InternedStr"
        );
        // Content should match
        assert_eq!(result, Value::String(SmolStr::new("active")));
    }

    #[test]
    fn maybe_intern_leaves_string_without_dictionary_schema() {
        let g = SeleneGraph::new();
        // No schema registered at all

        let labels = vec![IStr::new("sensor")];
        let key = IStr::new("status");
        let val = Value::String(SmolStr::new("active"));

        let result = maybe_intern_value(&g, &labels, key, val.clone());

        assert!(
            matches!(result, Value::String(_)),
            "without dictionary schema, String should remain String"
        );
    }

    #[test]
    fn maybe_intern_ignores_non_string_values() {
        let mut g = SeleneGraph::new();
        let prop = PropertyDef::builder("temp", ValueType::Float)
            .dictionary()
            .build();
        let schema = NodeSchema::builder("sensor").property(prop).build();
        g.schema_mut()
            .register_node_schema(schema)
            .expect("schema registration");

        let labels = vec![IStr::new("sensor")];
        let key = IStr::new("temp");
        let val = Value::Float(72.5);

        let result = maybe_intern_value(&g, &labels, key, val);

        assert!(
            matches!(result, Value::Float(f) if f == 72.5),
            "non-string values should pass through unchanged"
        );
    }

    // ── MutationStats ───────────────────────────────────────────

    #[test]
    fn stats_count_insert_chain() {
        let shared = SharedGraph::new(SeleneGraph::new());
        let result = MutationBuilder::new(
            "INSERT (a:building {name: 'HQ'})-[:contains]->(b:floor {level: 1})-[:contains]->(c:sensor {name: 'T1'})",
        )
        .execute(&shared)
        .unwrap();

        assert_eq!(result.mutations.nodes_created, 3);
        assert_eq!(result.mutations.edges_created, 2);
        assert_eq!(result.mutations.nodes_deleted, 0);
        assert_eq!(result.mutations.edges_deleted, 0);
    }

    #[test]
    fn count_mutation_helper_counts_insert_elements() {
        let pattern = InsertGraphPattern {
            paths: vec![InsertPathPattern {
                elements: vec![
                    InsertElement::Node {
                        var: Some(IStr::new("a")),
                        labels: vec![IStr::new("sensor")],
                        properties: vec![],
                    },
                    InsertElement::Edge {
                        var: None,
                        label: Some(IStr::new("connects")),
                        direction: EdgeDirection::Out,
                        properties: vec![],
                    },
                    InsertElement::Node {
                        var: Some(IStr::new("b")),
                        labels: vec![IStr::new("zone")],
                        properties: vec![],
                    },
                ],
            }],
        };
        let op = MutationOp::InsertPattern(pattern);
        let mut stats = MutationStats::default();

        count_mutation(&op, 1, &mut stats);

        assert_eq!(stats.nodes_created, 2, "two nodes in the path");
        assert_eq!(stats.edges_created, 1, "one edge in the path");
    }

    #[test]
    fn count_mutation_helper_set_property_scales_with_rows() {
        let op = MutationOp::SetProperty {
            target: IStr::new("s"),
            property: IStr::new("temp"),
            value: Expr::Literal(GqlValue::Float(72.5)),
        };
        let mut stats = MutationStats::default();

        count_mutation(&op, 5, &mut stats);

        assert_eq!(stats.properties_set, 5, "should count one per row");
    }

    #[test]
    fn count_mutation_helper_delete_scales_with_rows() {
        let op = MutationOp::Delete {
            target: IStr::new("s"),
        };
        let mut stats = MutationStats::default();

        count_mutation(&op, 3, &mut stats);

        assert_eq!(stats.nodes_deleted, 3);
    }

    // ── Edge cases ──────────────────────────────────────────────

    #[test]
    fn insert_node_without_properties() {
        let shared = SharedGraph::new(SeleneGraph::new());
        let result = MutationBuilder::new("INSERT (:sensor)")
            .execute(&shared)
            .unwrap();

        assert_eq!(result.mutations.nodes_created, 1);
        shared.read(|g| {
            let node = g.get_node(NodeId(1)).unwrap();
            assert!(node.labels.contains(IStr::new("sensor")));
            assert_eq!(node.properties.len(), 0);
        });
    }

    #[test]
    fn delete_then_insert_in_separate_mutations() {
        // Delete all, then insert fresh: verifies mutation ordering
        let shared = SharedGraph::new(SeleneGraph::new());
        MutationBuilder::new("INSERT (:sensor {name: 'old'})")
            .execute(&shared)
            .unwrap();
        assert_eq!(shared.read(|g| g.node_count()), 1);

        // Delete the old node
        MutationBuilder::new("MATCH (s:sensor) DELETE s")
            .execute(&shared)
            .unwrap();
        assert_eq!(shared.read(|g| g.node_count()), 0);

        // Insert a new one
        MutationBuilder::new("INSERT (:sensor {name: 'new'})")
            .execute(&shared)
            .unwrap();
        assert_eq!(shared.read(|g| g.node_count()), 1);
    }

    #[test]
    fn remove_property_on_edge() {
        let shared = SharedGraph::new(SeleneGraph::new());
        MutationBuilder::new(
            "INSERT (a:building {name: 'HQ'})-[e:contains {weight: 1.0}]->(b:floor {name: 'F1'})",
        )
        .execute(&shared)
        .unwrap();

        shared.read(|g| {
            let edge = g.get_edge(EdgeId(1)).unwrap();
            assert!(edge.properties.get_by_str("weight").is_some());
        });

        MutationBuilder::new("MATCH (a:building)-[e:contains]->(b:floor) REMOVE e.weight")
            .execute(&shared)
            .unwrap();

        shared.read(|g| {
            let edge = g.get_edge(EdgeId(1)).unwrap();
            assert_eq!(
                edge.properties.get_by_str("weight"),
                None,
                "weight should be removed from edge"
            );
        });
    }

    #[test]
    fn insert_multiple_nodes_without_edges() {
        let shared = SharedGraph::new(SeleneGraph::new());
        let result = MutationBuilder::new(
            "INSERT (:sensor {name: 'S1'}), (:sensor {name: 'S2'}), (:sensor {name: 'S3'})",
        )
        .execute(&shared)
        .unwrap();

        assert_eq!(result.mutations.nodes_created, 3);
        assert_eq!(result.mutations.edges_created, 0);
        assert_eq!(shared.read(|g| g.node_count()), 3);
    }

    #[test]
    fn to_graph_err_preserves_message() {
        let err = to_graph_err("test error message");
        match err {
            selene_graph::GraphError::Other(msg) => {
                assert_eq!(msg, "test error message");
            }
            other => panic!("expected GraphError::Other, got {other:?}"),
        }
    }
}
