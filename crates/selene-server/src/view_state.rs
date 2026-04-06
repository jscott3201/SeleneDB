//! Runtime aggregate state for materialized views.
//!
//! Rebuilt from graph scan on startup, maintained incrementally
//! via changelog subscriber. Registered as a Service and exposed
//! to selene-gql through the ViewProvider OnceLock.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::RwLock;
use roaring::RoaringBitmap;
use selene_core::changeset::Change;
use selene_core::{IStr, NodeId, Value};
use selene_gql::GqlValue;
use selene_gql::types::error::GqlError;
use selene_gql::types::value::{GqlList, GqlType};
use selene_graph::SeleneGraph;
use selene_graph::view_registry::{ViewAggregateKind, ViewDefinition};

// ── Aggregate state ──────────────────────────────────────────────────

/// Per-aggregate-column running state.
#[derive(Debug, Clone)]
pub enum AggregateState {
    Count(i64),
    CountStar(i64),
    Sum(f64),
    Avg {
        sum: f64,
        count: i64,
    },
    Min(Option<Value>),
    Max(Option<Value>),
    Collect(Vec<Value>),
    /// Unsupported aggregate: requires full recomputation on every change.
    FullRecompute,
}

impl AggregateState {
    /// Create a fresh zero-valued state for the given aggregate kind.
    fn new_for(kind: ViewAggregateKind) -> Self {
        match kind {
            ViewAggregateKind::Count => AggregateState::Count(0),
            ViewAggregateKind::CountStar => AggregateState::CountStar(0),
            ViewAggregateKind::Sum => AggregateState::Sum(0.0),
            ViewAggregateKind::Avg => AggregateState::Avg { sum: 0.0, count: 0 },
            ViewAggregateKind::Min => AggregateState::Min(None),
            ViewAggregateKind::Max => AggregateState::Max(None),
            ViewAggregateKind::Collect => AggregateState::Collect(Vec::new()),
            ViewAggregateKind::FullRecompute => AggregateState::FullRecompute,
        }
    }

    /// Convert to the GQL value exposed through the ViewProvider.
    fn to_gql_value(&self) -> GqlValue {
        match self {
            AggregateState::Count(n) | AggregateState::CountStar(n) => GqlValue::Int(*n),
            AggregateState::Sum(v) => GqlValue::Float(*v),
            AggregateState::Avg { sum, count } => {
                if *count == 0 {
                    GqlValue::Null
                } else {
                    GqlValue::Float(*sum / *count as f64)
                }
            }
            AggregateState::Min(opt) | AggregateState::Max(opt) => match opt {
                Some(v) => GqlValue::from(v),
                None => GqlValue::Null,
            },
            AggregateState::Collect(vals) => {
                let elements: Arc<[GqlValue]> = vals.iter().map(GqlValue::from).collect();
                GqlValue::List(GqlList {
                    element_type: GqlType::Nothing,
                    elements,
                })
            }
            AggregateState::FullRecompute => GqlValue::Null,
        }
    }

    /// Accumulate a single property value into this aggregate.
    fn accumulate(&mut self, value: &Value) {
        match self {
            AggregateState::Count(n) => {
                if !matches!(value, Value::Null) {
                    *n += 1;
                }
            }
            AggregateState::CountStar(n) => {
                *n += 1;
            }
            AggregateState::Sum(s) => {
                if let Some(f) = value_to_f64(value) {
                    *s += f;
                }
            }
            AggregateState::Avg { sum, count } => {
                if let Some(f) = value_to_f64(value) {
                    *sum += f;
                    *count += 1;
                }
            }
            AggregateState::Min(current) => {
                if !matches!(value, Value::Null) {
                    *current = Some(match current.take() {
                        None => value.clone(),
                        Some(cur) => {
                            if value_less_than(value, &cur) {
                                value.clone()
                            } else {
                                cur
                            }
                        }
                    });
                }
            }
            AggregateState::Max(current) => {
                if !matches!(value, Value::Null) {
                    *current = Some(match current.take() {
                        None => value.clone(),
                        Some(cur) => {
                            if value_less_than(&cur, value) {
                                value.clone()
                            } else {
                                cur
                            }
                        }
                    });
                }
            }
            AggregateState::Collect(vals) => {
                vals.push(value.clone());
            }
            AggregateState::FullRecompute => {}
        }
    }

    /// Subtract a single property value from this aggregate (for removals).
    fn subtract(&mut self, value: &Value) {
        match self {
            AggregateState::Count(n) => {
                if !matches!(value, Value::Null) {
                    *n = (*n - 1).max(0);
                }
            }
            AggregateState::CountStar(n) => {
                *n = (*n - 1).max(0);
            }
            AggregateState::Sum(s) => {
                if let Some(f) = value_to_f64(value) {
                    *s -= f;
                }
            }
            AggregateState::Avg { sum, count } => {
                if let Some(f) = value_to_f64(value) {
                    *sum -= f;
                    *count = (*count - 1).max(0);
                }
            }
            // Min/Max cannot be decremented cheaply; mark for full recompute.
            AggregateState::Min(_) | AggregateState::Max(_) => {
                // After removing a value that might have been the min/max,
                // we cannot recompute without scanning all members. Fall through
                // to the needs_recompute path handled by the caller.
            }
            AggregateState::Collect(vals) => {
                // Remove the first occurrence of the value.
                if let Some(pos) = vals.iter().position(|v| v == value) {
                    vals.remove(pos);
                }
            }
            AggregateState::FullRecompute => {}
        }
    }

    /// Whether a subtract may have left this state inconsistent.
    ///
    /// For Min/Max, only triggers when the removed value matches the
    /// current extremum (meaning we may have lost it). For Collect,
    /// always triggers because positional removal is unreliable after
    /// modifications.
    fn needs_recompute_after_subtract(&self, removed: &Value) -> bool {
        match self {
            // removed <= current means it could have been the min
            AggregateState::Min(Some(current)) => !value_less_than(current, removed),
            // removed >= current means it could have been the max
            AggregateState::Max(Some(current)) => !value_less_than(removed, current),
            AggregateState::Min(None) | AggregateState::Max(None) => true,
            AggregateState::Collect(_) => true,
            _ => false,
        }
    }
}

// ── ViewState ────────────────────────────────────────────────────────

/// Complete materialized state for one view.
#[derive(Debug, Clone)]
pub struct ViewState {
    /// (alias, aggregate_state) in definition order.
    pub columns: Vec<(String, AggregateState)>,
    /// Node IDs currently tracked by this view.
    pub member_nodes: RoaringBitmap,
}

// ── ViewStateStore ───────────────────────────────────────────────────

/// In-memory store for all materialized view aggregate states.
///
/// Thread-safe via `RwLock`. Updated by the changelog subscriber,
/// read by query procedures through the ViewProvider bridge.
pub struct ViewStateStore {
    states: RwLock<HashMap<String, ViewState>>,
}

impl ViewStateStore {
    pub fn new() -> Self {
        Self {
            states: RwLock::new(HashMap::new()),
        }
    }

    /// Rebuild all view states from graph scan. Called at startup and
    /// when the changelog subscriber detects it has lagged.
    pub fn rebuild_all(&self, views: &[ViewDefinition], graph: &SeleneGraph) {
        let mut states = self.states.write();
        states.clear();

        for def in views {
            let state = build_view_state(def, graph);
            states.insert(def.name.clone(), state);
        }
    }

    /// Build and register a single view (called after CREATE MATERIALIZED VIEW).
    #[allow(dead_code)] // wired in a future wave when mutation paths are connected
    pub fn register_view(&self, def: &ViewDefinition, graph: &SeleneGraph) {
        let state = build_view_state(def, graph);
        self.states.write().insert(def.name.clone(), state);
    }

    /// Remove a view from the store (called after DROP MATERIALIZED VIEW).
    #[allow(dead_code)] // wired in a future wave when mutation paths are connected
    pub fn remove_view(&self, name: &str) {
        self.states.write().remove(name);
    }

    /// Read the current aggregate values for a named view.
    /// Returns (column_alias, gql_value) pairs.
    pub fn read_view(&self, name: &str) -> Option<Vec<(IStr, GqlValue)>> {
        let states = self.states.read();
        let view_state = states.get(name)?;

        let result = view_state
            .columns
            .iter()
            .map(|(alias, agg)| (IStr::new(alias), agg.to_gql_value()))
            .collect();

        Some(result)
    }

    /// Check if a view exists in the store.
    pub fn has_view(&self, name: &str) -> bool {
        self.states.read().contains_key(name)
    }

    /// Apply a batch of changelog changes to all affected views.
    pub fn apply_changes(&self, changes: &[Change], graph: &SeleneGraph, views: &[ViewDefinition]) {
        if views.is_empty() {
            return;
        }

        let mut states = self.states.write();
        let mut needs_full_recompute: Vec<String> = Vec::new();
        // Track nodes freshly added via LabelAdded in this batch so that
        // subsequent PropertySet events in the same batch do not double-count.
        let mut freshly_added: HashSet<u64> = HashSet::new();

        for change in changes {
            match change {
                Change::LabelAdded { node_id, label } => {
                    let label_str = label.as_str();
                    for def in views {
                        if !def.match_labels.iter().any(|l| l == label_str) {
                            continue;
                        }
                        let Some(state) = states.get_mut(&def.name) else {
                            continue;
                        };
                        // Check if node now has all required labels.
                        if node_has_all_labels(*node_id, &def.match_labels, graph) {
                            let id_u32 = node_id.0 as u32;
                            if state.member_nodes.insert(id_u32) {
                                // Newly entered the view: accumulate its properties.
                                accumulate_node(*node_id, def, state, graph);
                                freshly_added.insert(node_id.0);
                            }
                        }
                    }
                }

                Change::LabelRemoved { node_id, label } => {
                    let label_str = label.as_str();
                    for def in views {
                        if !def.match_labels.iter().any(|l| l == label_str) {
                            continue;
                        }
                        let Some(state) = states.get_mut(&def.name) else {
                            continue;
                        };
                        let id_u32 = node_id.0 as u32;
                        if state.member_nodes.remove(id_u32)
                            && subtract_node(*node_id, def, state, graph)
                        {
                            needs_full_recompute.push(def.name.clone());
                        }
                    }
                }

                Change::NodeDeleted { node_id, labels } => {
                    let label_strs: Vec<&str> = labels.iter().map(|l| l.as_str()).collect();
                    for def in views {
                        if !def
                            .match_labels
                            .iter()
                            .any(|l| label_strs.contains(&l.as_str()))
                        {
                            continue;
                        }
                        let Some(state) = states.get_mut(&def.name) else {
                            continue;
                        };
                        let id_u32 = node_id.0 as u32;
                        if state.member_nodes.remove(id_u32) {
                            // Node is deleted so we cannot look up its properties.
                            // Subtract using CountStar decrement only; flag for
                            // recompute if any column needs property data.
                            let has_property_agg =
                                def.aggregates.iter().any(|a| a.source_property.is_some());
                            for (_, agg) in &mut state.columns {
                                if let AggregateState::CountStar(n) = agg {
                                    *n = (*n - 1).max(0);
                                }
                            }
                            if has_property_agg {
                                needs_full_recompute.push(def.name.clone());
                            }
                        }
                    }
                }

                Change::PropertySet {
                    node_id,
                    key,
                    value,
                    old_value,
                } => {
                    // Skip if this node was freshly accumulated via LabelAdded
                    // in the same batch (its properties are already counted).
                    if freshly_added.contains(&node_id.0) {
                        continue;
                    }
                    let key_str = key.as_str();
                    for def in views {
                        let Some(state) = states.get_mut(&def.name) else {
                            continue;
                        };
                        let id_u32 = node_id.0 as u32;
                        if !state.member_nodes.contains(id_u32) {
                            continue;
                        }
                        for (col_idx, agg_def) in def.aggregates.iter().enumerate() {
                            if agg_def.source_property.as_deref() != Some(key_str) {
                                continue;
                            }
                            let (_, agg) = &mut state.columns[col_idx];
                            // Subtract old value, accumulate new.
                            if let Some(old) = old_value {
                                agg.subtract(old);
                                if agg.needs_recompute_after_subtract(old) {
                                    needs_full_recompute.push(def.name.clone());
                                }
                            }
                            agg.accumulate(value);
                        }
                    }
                }

                Change::PropertyRemoved {
                    node_id,
                    key,
                    old_value,
                } => {
                    let key_str = key.as_str();
                    for def in views {
                        let Some(state) = states.get_mut(&def.name) else {
                            continue;
                        };
                        let id_u32 = node_id.0 as u32;
                        if !state.member_nodes.contains(id_u32) {
                            continue;
                        }
                        for (col_idx, agg_def) in def.aggregates.iter().enumerate() {
                            if agg_def.source_property.as_deref() != Some(key_str) {
                                continue;
                            }
                            if let Some(old) = old_value {
                                let (_, agg) = &mut state.columns[col_idx];
                                agg.subtract(old);
                                if agg.needs_recompute_after_subtract(old) {
                                    needs_full_recompute.push(def.name.clone());
                                }
                            }
                        }
                    }
                }

                // Edge changes and NodeCreated do not affect node-based materialized views.
                _ => {}
            }
        }

        // Recompute any views that had min/max subtracted.
        needs_full_recompute.sort();
        needs_full_recompute.dedup();
        for name in needs_full_recompute {
            if let Some(def) = views.iter().find(|d| d.name == name) {
                let fresh = build_view_state(def, graph);
                states.insert(name, fresh);
            }
        }
    }
}

// ── ViewStateService ─────────────────────────────────────────────────

/// Service wrapper for the ViewStateStore, following the VectorStoreService pattern.
/// Holds an `Arc<ViewStateStore>` for sharing with the changelog subscriber
/// and the ViewProvider bridge.
pub struct ViewStateService {
    pub store: Arc<ViewStateStore>,
}

impl ViewStateService {
    pub fn new(store: Arc<ViewStateStore>) -> Self {
        Self { store }
    }
}

impl crate::service_registry::Service for ViewStateService {
    fn name(&self) -> &'static str {
        "materialized_views"
    }

    fn health(&self) -> crate::service_registry::ServiceHealth {
        crate::service_registry::ServiceHealth::Healthy
    }
}

// ── ServerViewProvider ───────────────────────────────────────────────

/// Bridge between the ViewProvider OnceLock in selene-gql and
/// the ViewStateStore in selene-server.
pub struct ServerViewProvider {
    store: Arc<ViewStateStore>,
}

impl ServerViewProvider {
    pub fn new(store: Arc<ViewStateStore>) -> Self {
        Self { store }
    }
}

impl selene_gql::runtime::procedures::view_provider::ViewProvider for ServerViewProvider {
    fn read_view(&self, name: &str) -> Result<Vec<(IStr, GqlValue)>, GqlError> {
        self.store
            .read_view(&name.to_uppercase())
            .ok_or_else(|| GqlError::InvalidArgument {
                message: format!("materialized view '{name}' does not exist"),
            })
    }

    fn view_exists(&self, name: &str) -> bool {
        self.store.has_view(&name.to_uppercase())
    }
}

// ── Internal helpers ─────────────────────────────────────────────────

/// Build a fresh `ViewState` for a single view definition by scanning the graph.
fn build_view_state(def: &ViewDefinition, graph: &SeleneGraph) -> ViewState {
    let mut columns: Vec<(String, AggregateState)> = def
        .aggregates
        .iter()
        .map(|agg| (agg.alias.clone(), AggregateState::new_for(agg.kind)))
        .collect();

    let mut member_nodes = RoaringBitmap::new();

    // Intersect label bitmaps to find candidate nodes.
    let Some(candidate_bitmap) = label_intersection(graph, &def.match_labels) else {
        return ViewState {
            columns,
            member_nodes,
        };
    };

    for id_u32 in &candidate_bitmap {
        let node_id = NodeId(u64::from(id_u32));
        let Some(node) = graph.get_node(node_id) else {
            continue;
        };

        member_nodes.insert(id_u32);

        for (col_idx, agg_def) in def.aggregates.iter().enumerate() {
            let (_, agg) = &mut columns[col_idx];
            if agg_def.kind == ViewAggregateKind::CountStar {
                agg.accumulate(&Value::Null); // CountStar ignores value
            } else {
                let value = agg_def
                    .source_property
                    .as_deref()
                    .and_then(|prop| node.property(prop))
                    .cloned()
                    .unwrap_or(Value::Null);
                agg.accumulate(&value);
            }
        }
    }

    ViewState {
        columns,
        member_nodes,
    }
}

/// Intersect label bitmaps for all labels in a view definition.
/// Returns None if any label has no nodes (empty intersection).
fn label_intersection(graph: &SeleneGraph, labels: &[String]) -> Option<RoaringBitmap> {
    let mut result: Option<RoaringBitmap> = None;
    for label in labels {
        let bm = graph.label_bitmap(label)?;
        result = Some(match result {
            None => bm.clone(),
            Some(acc) => acc & bm,
        });
    }
    result
}

/// Check if a node currently has all the required labels.
fn node_has_all_labels(node_id: NodeId, labels: &[String], graph: &SeleneGraph) -> bool {
    let Some(node) = graph.get_node(node_id) else {
        return false;
    };
    labels.iter().all(|l| node.has_label(l))
}

/// Accumulate a node's properties into the view state (node enters the view).
fn accumulate_node(
    node_id: NodeId,
    def: &ViewDefinition,
    state: &mut ViewState,
    graph: &SeleneGraph,
) {
    let Some(node) = graph.get_node(node_id) else {
        return;
    };

    for (col_idx, agg_def) in def.aggregates.iter().enumerate() {
        let (_, agg) = &mut state.columns[col_idx];
        if agg_def.kind == ViewAggregateKind::CountStar {
            agg.accumulate(&Value::Null);
        } else {
            let value = agg_def
                .source_property
                .as_deref()
                .and_then(|prop| node.property(prop))
                .cloned()
                .unwrap_or(Value::Null);
            agg.accumulate(&value);
        }
    }
}

/// Subtract a node's properties from the view state (node leaves the view).
/// Returns `true` if any column needs a full recompute after subtraction.
fn subtract_node(
    node_id: NodeId,
    def: &ViewDefinition,
    state: &mut ViewState,
    graph: &SeleneGraph,
) -> bool {
    let Some(node) = graph.get_node(node_id) else {
        return false;
    };

    let mut needs_recompute = false;
    for (col_idx, agg_def) in def.aggregates.iter().enumerate() {
        let (_, agg) = &mut state.columns[col_idx];
        if agg_def.kind == ViewAggregateKind::CountStar {
            agg.subtract(&Value::Null);
        } else {
            let value = agg_def
                .source_property
                .as_deref()
                .and_then(|prop| node.property(prop))
                .cloned()
                .unwrap_or(Value::Null);
            agg.subtract(&value);
            if agg.needs_recompute_after_subtract(&value) {
                needs_recompute = true;
            }
        }
    }
    needs_recompute
}

/// Extract a numeric f64 from a Value for sum/avg aggregation.
fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Int(i) => Some(*i as f64),
        Value::UInt(u) => Some(*u as f64),
        Value::Float(f) => Some(*f),
        _ => None,
    }
}

/// Compare two Values: returns true if `a < b` for numeric and string types.
/// Used for incremental min/max tracking.
fn value_less_than(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int(a), Value::Int(b)) => a < b,
        (Value::UInt(a), Value::UInt(b)) => a < b,
        (Value::Float(a), Value::Float(b)) => a < b,
        (Value::Int(a), Value::Float(b)) => (*a as f64) < *b,
        (Value::Float(a), Value::Int(b)) => *a < (*b as f64),
        (Value::UInt(a), Value::Int(b)) => i128::from(*a) < i128::from(*b),
        (Value::Int(a), Value::UInt(b)) => i128::from(*a) < i128::from(*b),
        (Value::UInt(a), Value::Float(b)) => (*a as f64) < *b,
        (Value::Float(a), Value::UInt(b)) => *a < (*b as f64),
        (Value::String(a), Value::String(b)) => a < b,
        (Value::InternedStr(a), Value::InternedStr(b)) => a.as_str() < b.as_str(),
        (Value::String(a), Value::InternedStr(b)) => a.as_str() < b.as_str(),
        (Value::InternedStr(a), Value::String(b)) => a.as_str() < b.as_str(),
        (Value::Timestamp(a), Value::Timestamp(b)) => a < b,
        (Value::Date(a), Value::Date(b)) => a < b,
        _ => false,
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{LabelSet, PropertyMap};
    use selene_graph::view_registry::{ViewAggregate, ViewAggregateKind};

    fn make_graph_with_sensors() -> SeleneGraph {
        let mut graph = SeleneGraph::new();
        let mut m = graph.mutate();
        m.create_node(
            LabelSet::from_strs(&["Sensor"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("temp"), Value::Float(20.0)),
                (IStr::new("name"), Value::String("s1".into())),
            ]),
        )
        .unwrap();
        m.create_node(
            LabelSet::from_strs(&["Sensor"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("temp"), Value::Float(30.0)),
                (IStr::new("name"), Value::String("s2".into())),
            ]),
        )
        .unwrap();
        m.create_node(
            LabelSet::from_strs(&["Zone"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String("z1".into()))]),
        )
        .unwrap();
        m.commit(0).unwrap();
        graph
    }

    fn avg_temp_view() -> ViewDefinition {
        ViewDefinition {
            name: "SENSOR_STATS".to_string(),
            definition_text: "MATCH (s:Sensor) RETURN avg(s.temp) AS avg_temp".to_string(),
            match_labels: vec!["Sensor".to_string()],
            predicate_properties: vec![],
            aggregates: vec![ViewAggregate {
                alias: "avg_temp".to_string(),
                kind: ViewAggregateKind::Avg,
                source_property: Some("temp".to_string()),
            }],
        }
    }

    fn count_star_view() -> ViewDefinition {
        ViewDefinition {
            name: "SENSOR_COUNT".to_string(),
            definition_text: "MATCH (s:Sensor) RETURN count(*) AS total".to_string(),
            match_labels: vec!["Sensor".to_string()],
            predicate_properties: vec![],
            aggregates: vec![ViewAggregate {
                alias: "total".to_string(),
                kind: ViewAggregateKind::CountStar,
                source_property: None,
            }],
        }
    }

    fn multi_agg_view() -> ViewDefinition {
        ViewDefinition {
            name: "SENSOR_MULTI".to_string(),
            definition_text:
                "MATCH (s:Sensor) RETURN count(*) AS total, sum(s.temp) AS sum_temp, min(s.temp) AS min_temp, max(s.temp) AS max_temp"
                    .to_string(),
            match_labels: vec!["Sensor".to_string()],
            predicate_properties: vec![],
            aggregates: vec![
                ViewAggregate {
                    alias: "total".to_string(),
                    kind: ViewAggregateKind::CountStar,
                    source_property: None,
                },
                ViewAggregate {
                    alias: "sum_temp".to_string(),
                    kind: ViewAggregateKind::Sum,
                    source_property: Some("temp".to_string()),
                },
                ViewAggregate {
                    alias: "min_temp".to_string(),
                    kind: ViewAggregateKind::Min,
                    source_property: Some("temp".to_string()),
                },
                ViewAggregate {
                    alias: "max_temp".to_string(),
                    kind: ViewAggregateKind::Max,
                    source_property: Some("temp".to_string()),
                },
            ],
        }
    }

    #[test]
    fn rebuild_avg_view() {
        let graph = make_graph_with_sensors();
        let store = ViewStateStore::new();
        store.rebuild_all(&[avg_temp_view()], &graph);

        let result = store.read_view("SENSOR_STATS").unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0.as_str(), "avg_temp");
        // avg(20.0, 30.0) = 25.0
        assert_eq!(result[0].1, GqlValue::Float(25.0));
    }

    #[test]
    fn rebuild_count_star_view() {
        let graph = make_graph_with_sensors();
        let store = ViewStateStore::new();
        store.rebuild_all(&[count_star_view()], &graph);

        let result = store.read_view("SENSOR_COUNT").unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1, GqlValue::Int(2));
    }

    #[test]
    fn rebuild_multi_agg_view() {
        let graph = make_graph_with_sensors();
        let store = ViewStateStore::new();
        store.rebuild_all(&[multi_agg_view()], &graph);

        let result = store.read_view("SENSOR_MULTI").unwrap();
        assert_eq!(result.len(), 4);
        // count(*) = 2
        assert_eq!(result[0].1, GqlValue::Int(2));
        // sum(temp) = 50.0
        assert_eq!(result[1].1, GqlValue::Float(50.0));
        // min(temp) = 20.0
        assert_eq!(result[2].1, GqlValue::Float(20.0));
        // max(temp) = 30.0
        assert_eq!(result[3].1, GqlValue::Float(30.0));
    }

    #[test]
    fn read_missing_view_returns_none() {
        let store = ViewStateStore::new();
        assert!(store.read_view("NONEXISTENT").is_none());
    }

    #[test]
    fn has_view_check() {
        let graph = make_graph_with_sensors();
        let store = ViewStateStore::new();
        store.rebuild_all(&[avg_temp_view()], &graph);

        assert!(store.has_view("SENSOR_STATS"));
        assert!(!store.has_view("NONEXISTENT"));
    }

    #[test]
    fn register_and_remove_view() {
        let graph = make_graph_with_sensors();
        let store = ViewStateStore::new();

        store.register_view(&avg_temp_view(), &graph);
        assert!(store.has_view("SENSOR_STATS"));

        store.remove_view("SENSOR_STATS");
        assert!(!store.has_view("SENSOR_STATS"));
    }

    #[test]
    fn apply_property_set_updates_avg() {
        let mut graph = make_graph_with_sensors();
        let defs = vec![avg_temp_view()];
        let store = ViewStateStore::new();
        store.rebuild_all(&defs, &graph);

        // Update node 1's temp from 20.0 to 40.0
        let changes = vec![Change::PropertySet {
            node_id: NodeId(1),
            key: IStr::new("temp"),
            value: Value::Float(40.0),
            old_value: Some(Value::Float(20.0)),
        }];

        // Apply: need graph for potential recomputes.
        {
            let mut m = graph.mutate();
            m.set_property(NodeId(1), IStr::new("temp"), Value::Float(40.0))
                .unwrap();
            m.commit(0).unwrap();
        }
        store.apply_changes(&changes, &graph, &defs);

        let result = store.read_view("SENSOR_STATS").unwrap();
        // avg(40.0, 30.0) = 35.0
        assert_eq!(result[0].1, GqlValue::Float(35.0));
    }

    #[test]
    fn apply_label_added_enters_view() {
        let mut graph = make_graph_with_sensors();
        let defs = vec![count_star_view()];
        let store = ViewStateStore::new();
        store.rebuild_all(&defs, &graph);

        // Verify starting count is 2
        let result = store.read_view("SENSOR_COUNT").unwrap();
        assert_eq!(result[0].1, GqlValue::Int(2));

        // Add Sensor label to node 3 (the Zone node)
        {
            let mut m = graph.mutate();
            m.add_label(NodeId(3), IStr::new("Sensor")).unwrap();
            m.commit(0).unwrap();
        }

        let changes = vec![Change::LabelAdded {
            node_id: NodeId(3),
            label: IStr::new("Sensor"),
        }];
        store.apply_changes(&changes, &graph, &defs);

        let result = store.read_view("SENSOR_COUNT").unwrap();
        assert_eq!(result[0].1, GqlValue::Int(3));
    }

    #[test]
    fn apply_label_removed_exits_view() {
        let mut graph = make_graph_with_sensors();
        let defs = vec![count_star_view()];
        let store = ViewStateStore::new();
        store.rebuild_all(&defs, &graph);

        // Remove Sensor label from node 1
        {
            let mut m = graph.mutate();
            m.remove_label(NodeId(1), "Sensor").unwrap();
            m.commit(0).unwrap();
        }

        let changes = vec![Change::LabelRemoved {
            node_id: NodeId(1),
            label: IStr::new("Sensor"),
        }];
        store.apply_changes(&changes, &graph, &defs);

        let result = store.read_view("SENSOR_COUNT").unwrap();
        assert_eq!(result[0].1, GqlValue::Int(1));
    }

    #[test]
    fn apply_node_deleted_exits_view() {
        let graph = make_graph_with_sensors();
        let defs = vec![count_star_view()];
        let store = ViewStateStore::new();
        store.rebuild_all(&defs, &graph);

        let changes = vec![Change::NodeDeleted {
            node_id: NodeId(1),
            labels: vec![IStr::new("Sensor")],
        }];
        store.apply_changes(&changes, &graph, &defs);

        let result = store.read_view("SENSOR_COUNT").unwrap();
        assert_eq!(result[0].1, GqlValue::Int(1));
    }

    #[test]
    fn empty_graph_produces_zero_aggregates() {
        let graph = SeleneGraph::new();
        let store = ViewStateStore::new();
        store.rebuild_all(&[avg_temp_view()], &graph);

        let result = store.read_view("SENSOR_STATS").unwrap();
        // avg with count=0 -> Null
        assert_eq!(result[0].1, GqlValue::Null);
    }

    #[test]
    fn service_trait_impl() {
        use crate::service_registry::Service;
        let svc = ViewStateService::new(Arc::new(ViewStateStore::new()));
        assert_eq!(svc.name(), "materialized_views");
        assert_eq!(
            svc.health(),
            crate::service_registry::ServiceHealth::Healthy
        );
    }

    #[test]
    fn value_comparison_helpers() {
        // value_to_f64
        assert_eq!(value_to_f64(&Value::Int(42)), Some(42.0));
        assert_eq!(value_to_f64(&Value::Float(3.15)), Some(3.15));
        assert_eq!(value_to_f64(&Value::UInt(100)), Some(100.0));
        assert_eq!(value_to_f64(&Value::Null), None);
        assert_eq!(value_to_f64(&Value::String("hello".into())), None);

        // value_less_than
        assert!(value_less_than(&Value::Int(1), &Value::Int(2)));
        assert!(!value_less_than(&Value::Int(2), &Value::Int(1)));
        assert!(value_less_than(&Value::Float(1.0), &Value::Float(2.0)));
        assert!(value_less_than(&Value::Int(1), &Value::Float(2.0)));
    }

    #[test]
    fn collect_aggregate() {
        let graph = make_graph_with_sensors();
        let def = ViewDefinition {
            name: "SENSOR_NAMES".to_string(),
            definition_text: "MATCH (s:Sensor) RETURN collect(s.name) AS names".to_string(),
            match_labels: vec!["Sensor".to_string()],
            predicate_properties: vec![],
            aggregates: vec![ViewAggregate {
                alias: "names".to_string(),
                kind: ViewAggregateKind::Collect,
                source_property: Some("name".to_string()),
            }],
        };

        let store = ViewStateStore::new();
        store.rebuild_all(&[def], &graph);

        let result = store.read_view("SENSOR_NAMES").unwrap();
        assert_eq!(result.len(), 1);
        if let GqlValue::List(list) = &result[0].1 {
            assert_eq!(list.elements.len(), 2);
        } else {
            panic!("expected List");
        }
    }
}
