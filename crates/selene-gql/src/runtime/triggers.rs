//! Trigger evaluator: matches changes against registered triggers and
//! executes their actions within the same graph state.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use selene_core::changeset::Change;
use selene_core::trigger::{TriggerDef, TriggerEvent};
use selene_core::{IStr, NodeId};
use selene_graph::SeleneGraph;

use crate::ast::expr::Expr;
use crate::ast::mutation::MutationPipeline;
use crate::ast::statement::GqlStatement;
use crate::parser::parse_statement;
use crate::runtime::eval;
use crate::runtime::eval::EvalContext;
use crate::runtime::functions::FunctionRegistry;
use crate::types::binding::{Binding, BoundValue};
use crate::types::error::GqlError;
use crate::types::value::GqlValue;

/// Maximum cascade depth to prevent infinite trigger loops.
const MAX_CASCADE_DEPTH: u32 = 8;

// ── Trigger AST caches ──────────────────────────────────────────────────
//
// Condition and action text is re-parsed on every trigger fire. Since
// TriggerDef is serialized in snapshots we cannot add cached AST fields
// to it, so we use module-level caches keyed by the text string.

static CONDITION_CACHE: Mutex<Option<HashMap<String, Expr>>> = Mutex::new(None);
static ACTION_CACHE: Mutex<Option<HashMap<String, MutationPipeline>>> = Mutex::new(None);

/// Clear the trigger AST caches. Call on CREATE/DROP TRIGGER.
pub(crate) fn invalidate_trigger_caches() {
    if let Ok(mut guard) = CONDITION_CACHE.lock() {
        *guard = None;
    }
    if let Ok(mut guard) = ACTION_CACHE.lock() {
        *guard = None;
    }
}

/// Look up or parse+cache a condition expression.
fn cached_condition_expr(condition_text: &str) -> Result<Expr, GqlError> {
    // Fast path: check cache
    if let Ok(guard) = CONDITION_CACHE.lock()
        && let Some(map) = guard.as_ref()
        && let Some(expr) = map.get(condition_text)
    {
        return Ok(expr.clone());
    }

    // Slow path: parse and cache
    let wrapper = format!("MATCH (n) FILTER {condition_text} RETURN n");
    let stmt = parse_statement(&wrapper)?;

    let GqlStatement::Query(pipeline) = &stmt else {
        return Err(GqlError::internal(
            "expected query from trigger condition wrapper",
        ));
    };

    use crate::ast::statement::PipelineStatement;
    let filter_expr = pipeline.statements.iter().find_map(|s| {
        if let PipelineStatement::Filter(expr) = s {
            Some(expr.clone())
        } else {
            None
        }
    });

    let Some(expr) = filter_expr else {
        return Ok(Expr::Literal(GqlValue::Bool(true)));
    };

    if let Ok(mut guard) = CONDITION_CACHE.lock() {
        let map = guard.get_or_insert_with(HashMap::new);
        map.insert(condition_text.to_string(), expr.clone());
    }

    Ok(expr)
}

/// Look up or parse+cache a trigger action's MutationPipeline.
fn cached_action_pipeline(action_text: &str) -> Result<MutationPipeline, GqlError> {
    // Fast path: check cache
    if let Ok(guard) = ACTION_CACHE.lock()
        && let Some(map) = guard.as_ref()
        && let Some(mp) = map.get(action_text)
    {
        return Ok(mp.clone());
    }

    // Slow path: parse and cache
    let stmt = parse_statement(action_text)?;

    let GqlStatement::Mutate(mp) = stmt else {
        return Err(GqlError::internal(
            "trigger action must be a mutation statement",
        ));
    };

    if let Ok(mut guard) = ACTION_CACHE.lock() {
        let map = guard.get_or_insert_with(HashMap::new);
        map.insert(action_text.to_string(), mp.clone());
    }

    Ok(mp)
}

/// Tracks which (trigger_name, node_id) pairs have already fired in this
/// cascade chain to prevent self-recursive loops.
type FiredSet = HashSet<(String, u64)>;

/// Evaluate triggers for a set of changes from a mutation commit.
///
/// For each change, finds matching triggers, evaluates WHEN conditions,
/// and executes action mutations. Recurses for cascading triggers up to
/// MAX_CASCADE_DEPTH.
///
/// The `graph` parameter is the mutable graph state. Trigger actions are
/// executed directly on it via `TrackedMutation`. Returns the accumulated
/// changes from all trigger actions.
pub(crate) fn evaluate_triggers(
    graph: &mut SeleneGraph,
    changes: &[Change],
    depth: u32,
) -> Result<Vec<Change>, GqlError> {
    let mut fired = FiredSet::new();
    evaluate_triggers_inner(graph, changes, depth, &mut fired)
}

fn evaluate_triggers_inner(
    graph: &mut SeleneGraph,
    changes: &[Change],
    depth: u32,
    fired: &mut FiredSet,
) -> Result<Vec<Change>, GqlError> {
    if depth >= MAX_CASCADE_DEPTH {
        return Err(GqlError::internal(format!(
            "trigger cascade depth exceeded maximum ({MAX_CASCADE_DEPTH})"
        )));
    }

    if graph.trigger_registry().is_empty() {
        return Ok(vec![]);
    }

    let mut all_trigger_changes = Vec::new();

    for change in changes {
        // Extract event type, node_id, labels, and optional old value context
        let (event, node_id, change_labels, old_key, old_value) = match change {
            Change::NodeCreated { node_id } => (TriggerEvent::Insert, *node_id, None, None, None),
            Change::PropertySet {
                node_id,
                key,
                old_value,
                ..
            } => (
                TriggerEvent::Set,
                *node_id,
                None,
                Some(*key),
                old_value.clone(),
            ),
            Change::PropertyRemoved {
                node_id,
                key,
                old_value,
            } => (
                TriggerEvent::Remove,
                *node_id,
                None,
                Some(*key),
                old_value.clone(),
            ),
            Change::NodeDeleted { node_id, labels } => {
                (TriggerEvent::Delete, *node_id, Some(labels), None, None)
            }
            _ => continue,
        };

        // For DELETE events, labels come from the Change (node is already removed).
        // For all other events, labels come from the live graph.
        let labels: Vec<IStr> = if let Some(cl) = change_labels {
            cl.clone()
        } else {
            graph
                .get_node(node_id)
                .map(|n| n.labels.iter().collect())
                .unwrap_or_default()
        };

        for label in &labels {
            // Collect matching triggers (clone to release borrow on registry)
            let matching: Vec<TriggerDef> = graph
                .trigger_registry()
                .matching(event, *label)
                .into_iter()
                .cloned()
                .collect();

            for trigger in &matching {
                // Skip if this (trigger, node) pair already fired in this cascade
                let fire_key = (trigger.name.to_string(), node_id.0);
                if !fired.insert(fire_key) {
                    continue;
                }

                // Evaluate WHEN condition (with OLD_VALUE/OLD_KEY if available)
                if let Some(ref condition) = trigger.condition
                    && !evaluate_condition(
                        graph,
                        node_id,
                        condition,
                        old_key.map(|k| k.as_str()),
                        old_value.as_ref(),
                    )?
                {
                    continue;
                }

                // Execute action mutations (with OLD_VALUE/OLD_KEY if available)
                let action_changes = execute_action(
                    graph,
                    node_id,
                    &trigger.action,
                    old_key.map(|k| k.as_str()),
                    old_value.as_ref(),
                )?;

                // Recurse for cascading triggers
                if !action_changes.is_empty() {
                    let cascade =
                        evaluate_triggers_inner(graph, &action_changes, depth + 1, fired)?;
                    all_trigger_changes.extend(action_changes);
                    all_trigger_changes.extend(cascade);
                }
            }
        }
    }

    Ok(all_trigger_changes)
}

/// Evaluate a WHEN condition expression against a triggering node.
///
/// Binds `n` to the triggering node. For PropertySet/Remove events,
/// also binds `OLD_VALUE` (previous property value) and `OLD_KEY` (property name).
fn evaluate_condition(
    graph: &SeleneGraph,
    node_id: NodeId,
    condition_text: &str,
    old_key: Option<&str>,
    old_value: Option<&selene_core::Value>,
) -> Result<bool, GqlError> {
    let expr = cached_condition_expr(condition_text)?;

    let mut binding = Binding::single(IStr::new("N"), BoundValue::Node(node_id));
    // Bind OLD_VALUE and OLD_KEY for PropertySet/Remove triggers
    if let Some(val) = old_value {
        binding.bind(
            IStr::new("OLD_VALUE"),
            BoundValue::Scalar(GqlValue::from(val)),
        );
    }
    if let Some(key) = old_key {
        binding.bind(
            IStr::new("OLD_KEY"),
            BoundValue::Scalar(GqlValue::String(key.into())),
        );
    }
    let func_reg = FunctionRegistry::builtins();
    let ctx = EvalContext::new(graph, func_reg);
    let result = eval::eval_expr_ctx(&expr, &binding, &ctx)?;

    Ok(matches!(result, GqlValue::Bool(true)))
}

/// Execute a trigger's action mutations against the graph.
///
/// Binds `n` to the triggering node. For PropertySet/Remove events,
/// also binds `OLD_VALUE` and `OLD_KEY` for use in action expressions.
fn execute_action(
    graph: &mut SeleneGraph,
    trigger_node_id: NodeId,
    action_text: &str,
    old_key: Option<&str>,
    old_value: Option<&selene_core::Value>,
) -> Result<Vec<Change>, GqlError> {
    let mp = cached_action_pipeline(action_text)?;

    // Plan mutations and pre-evaluate expressions against immutable graph state
    let plan = crate::planner::plan_mutation(&mp, graph)?;
    let mut binding = Binding::single(IStr::new("N"), BoundValue::Node(trigger_node_id));
    if let Some(val) = old_value {
        binding.bind(
            IStr::new("OLD_VALUE"),
            BoundValue::Scalar(GqlValue::from(val)),
        );
    }
    if let Some(key) = old_key {
        binding.bind(
            IStr::new("OLD_KEY"),
            BoundValue::Scalar(GqlValue::String(key.into())),
        );
    }
    let bindings = vec![binding];

    // Pre-resolve all mutation operations to concrete values while we can
    // borrow graph immutably, then execute the resolved ops with mutable access.
    let resolved = resolve_trigger_ops(&plan.mutations, &bindings, graph)?;

    // Execute with mutable access (no immutable borrows held)
    let mut mutation = graph.mutate();
    for op in &resolved {
        execute_resolved_op(&mut mutation, op)?;
    }
    let committed = mutation
        .commit(0)
        .map_err(|e| GqlError::internal(format!("trigger mutation commit: {e}")))?;

    Ok(committed)
}

/// A mutation operation with all expressions pre-resolved to concrete values.
#[allow(clippy::large_enum_variant)]
enum ResolvedOp {
    InsertNode {
        labels: selene_core::LabelSet,
        properties: selene_core::PropertyMap,
    },
    SetProperty {
        node_id: NodeId,
        key: IStr,
        value: selene_core::Value,
    },
}

/// Pre-resolve mutation ops by evaluating expressions against the immutable graph.
fn resolve_trigger_ops(
    ops: &[crate::ast::mutation::MutationOp],
    bindings: &[Binding],
    graph: &SeleneGraph,
) -> Result<Vec<ResolvedOp>, GqlError> {
    use crate::ast::mutation::MutationOp;
    use selene_core::{LabelSet, PropertyMap, Value};

    let mut resolved = Vec::new();
    let empty = Binding::empty();
    let binding = bindings.first().unwrap_or(&empty);

    for op in ops {
        match op {
            MutationOp::InsertPattern(pattern) => {
                // Resolve path-based INSERT pattern (spec §13.2) to ResolvedOps.
                // Each path creates nodes and edges in sequence.
                use crate::ast::mutation::InsertElement;
                for path in &pattern.paths {
                    for element in &path.elements {
                        match element {
                            InsertElement::Node {
                                labels, properties, ..
                            } => {
                                let label_set = LabelSet::from_strs(
                                    &labels.iter().map(|l| l.as_str()).collect::<Vec<_>>(),
                                );
                                let mut props = PropertyMap::new();
                                for (key, expr) in properties {
                                    let val = eval::eval_expr(expr, binding, graph)?;
                                    let sv = Value::try_from(&val).map_err(|e| {
                                        GqlError::internal(format!("trigger value: {e}"))
                                    })?;
                                    props.insert(*key, sv);
                                }
                                resolved.push(ResolvedOp::InsertNode {
                                    labels: label_set,
                                    properties: props,
                                });
                            }
                            InsertElement::Edge { .. } => {
                                // Edge creation in trigger INSERT patterns requires both endpoints
                                // to be already-created nodes. Full edge support would need
                                // two-pass resolution.
                                tracing::warn!(
                                    "INSERT edge in trigger pattern not yet supported, skipping"
                                );
                            }
                        }
                    }
                }
            }
            MutationOp::SetProperty {
                target,
                property,
                value,
            } => {
                if let Some(BoundValue::Node(nid)) = binding.get(target) {
                    let val = eval::eval_expr(value, binding, graph)?;
                    let sv = Value::try_from(&val)
                        .map_err(|e| GqlError::internal(format!("trigger value: {e}")))?;
                    resolved.push(ResolvedOp::SetProperty {
                        node_id: *nid,
                        key: *property,
                        value: sv,
                    });
                }
            }
            _ => {
                // Other mutation types (Delete, etc.) can be added later
            }
        }
    }

    Ok(resolved)
}

/// Execute a pre-resolved mutation operation.
fn execute_resolved_op(
    m: &mut selene_graph::TrackedMutation<'_>,
    op: &ResolvedOp,
) -> Result<(), GqlError> {
    match op {
        ResolvedOp::InsertNode { labels, properties } => {
            m.create_node(labels.clone(), properties.clone())
                .map_err(|e| GqlError::internal(format!("trigger create_node: {e}")))?;
        }
        ResolvedOp::SetProperty {
            node_id,
            key,
            value,
        } => {
            m.set_property(*node_id, *key, value.clone())
                .map_err(|e| GqlError::internal(format!("trigger set_property: {e}")))?;
        }
    }
    Ok(())
}
