//! Subscription-based sync filtering for partial graph replication.
//!
//! Defines subscription rules (label + property predicates) and a
//! filter engine that evaluates changes against subscriptions. Used
//! by both hub (pull filtering) and edge (push filtering) sides.

use std::cmp::Ordering;

use roaring::RoaringBitmap;
use smallvec::SmallVec;

use selene_core::{IStr, NodeId, Value, changeset::Change};
use selene_graph::SeleneGraph;
use selene_wire::dto::sync::{PropertyPredicateConfig, SubscriptionConfig, SyncDirectionConfig};

/// Compare two Values for ordering. Returns None if types are incomparable.
fn compare_values(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Int(a), Value::Int(b)) => a.partial_cmp(b),
        (Value::UInt(a), Value::UInt(b)) => a.partial_cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)),
        // UInt cross-type comparisons (promote to i128 for exact Int comparison)
        (Value::UInt(a), Value::Int(b)) => i128::from(*a).partial_cmp(&i128::from(*b)),
        (Value::Int(a), Value::UInt(b)) => i128::from(*a).partial_cmp(&i128::from(*b)),
        (Value::UInt(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::Float(a), Value::UInt(b)) => a.partial_cmp(&(*b as f64)),
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        (Value::InternedStr(a), Value::InternedStr(b)) => Some(a.as_str().cmp(b.as_str())),
        (Value::String(a), Value::InternedStr(b)) => Some(a.as_str().cmp(b.as_str())),
        (Value::InternedStr(a), Value::String(b)) => Some(a.as_str().cmp(b.as_str())),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.partial_cmp(b),
        (Value::Date(a), Value::Date(b)) => a.partial_cmp(b),
        (Value::Duration(a), Value::Duration(b)) => a.partial_cmp(b),
        _ => None,
    }
}

/// Compiled property predicate using interned strings.
#[derive(Debug, Clone)]
pub enum PropertyPredicate {
    Eq { key: IStr, value: Value },
    In { key: IStr, values: Vec<Value> },
    Gt { key: IStr, value: Value },
    Lt { key: IStr, value: Value },
    Gte { key: IStr, value: Value },
    Lte { key: IStr, value: Value },
    IsNotNull { key: IStr },
}

impl PropertyPredicate {
    /// Evaluate this predicate against a property value from the graph.
    /// Returns true if the predicate is satisfied.
    pub(crate) fn evaluate(&self, actual: Option<&Value>) -> bool {
        match self {
            Self::Eq { value, .. } => actual.is_some_and(|v| v == value),
            Self::In { values, .. } => {
                actual.is_some_and(|v| values.iter().any(|candidate| v == candidate))
            }
            Self::Gt { value, .. } => {
                actual.is_some_and(|v| compare_values(v, value) == Some(Ordering::Greater))
            }
            Self::Lt { value, .. } => {
                actual.is_some_and(|v| compare_values(v, value) == Some(Ordering::Less))
            }
            Self::Gte { value, .. } => actual.is_some_and(|v| {
                matches!(
                    compare_values(v, value),
                    Some(Ordering::Greater | Ordering::Equal)
                )
            }),
            Self::Lte { value, .. } => actual.is_some_and(|v| {
                matches!(
                    compare_values(v, value),
                    Some(Ordering::Less | Ordering::Equal)
                )
            }),
            Self::IsNotNull { .. } => actual.is_some_and(|v| !matches!(v, Value::Null)),
        }
    }

    /// The property key this predicate filters on.
    pub(crate) fn key(&self) -> IStr {
        match self {
            Self::Eq { key, .. }
            | Self::In { key, .. }
            | Self::Gt { key, .. }
            | Self::Lt { key, .. }
            | Self::Gte { key, .. }
            | Self::Lte { key, .. }
            | Self::IsNotNull { key } => *key,
        }
    }
}

/// Compiled subscription rule with interned labels.
#[derive(Debug, Clone)]
pub struct CompiledRule {
    labels: SmallVec<[IStr; 4]>,
    predicates: SmallVec<[PropertyPredicate; 4]>,
}

/// Sync direction filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDirection {
    PushOnly,
    PullOnly,
    Bidirectional,
}

/// Compiled subscription definition with interned strings.
#[derive(Debug, Clone)]
pub struct SubscriptionDef {
    pub name: String,
    pub rules: Vec<CompiledRule>,
    pub direction: SyncDirection,
}

impl SubscriptionDef {
    /// Compile a wire-format SubscriptionConfig into an IStr-based runtime form.
    pub fn compile(config: &SubscriptionConfig) -> Self {
        let direction = match config.direction {
            SyncDirectionConfig::PushOnly => SyncDirection::PushOnly,
            SyncDirectionConfig::PullOnly => SyncDirection::PullOnly,
            SyncDirectionConfig::Bidirectional => SyncDirection::Bidirectional,
        };

        let rules = config
            .rules
            .iter()
            .map(|r| CompiledRule {
                labels: r.labels.iter().map(|l| IStr::new(l)).collect(),
                predicates: r
                    .predicates
                    .iter()
                    .map(|p| match p {
                        PropertyPredicateConfig::Eq { key, value } => PropertyPredicate::Eq {
                            key: IStr::new(key),
                            value: value.clone(),
                        },
                        PropertyPredicateConfig::In { key, values } => PropertyPredicate::In {
                            key: IStr::new(key),
                            values: values.clone(),
                        },
                        PropertyPredicateConfig::Gt { key, value } => PropertyPredicate::Gt {
                            key: IStr::new(key),
                            value: value.clone(),
                        },
                        PropertyPredicateConfig::Lt { key, value } => PropertyPredicate::Lt {
                            key: IStr::new(key),
                            value: value.clone(),
                        },
                        PropertyPredicateConfig::Gte { key, value } => PropertyPredicate::Gte {
                            key: IStr::new(key),
                            value: value.clone(),
                        },
                        PropertyPredicateConfig::Lte { key, value } => PropertyPredicate::Lte {
                            key: IStr::new(key),
                            value: value.clone(),
                        },
                        PropertyPredicateConfig::IsNotNull { key } => {
                            PropertyPredicate::IsNotNull {
                                key: IStr::new(key),
                            }
                        }
                    })
                    .collect(),
            })
            .collect();

        Self {
            name: config.name.clone(),
            rules,
            direction,
        }
    }
}

/// Runtime subscription filter with scope bitmap.
///
/// Evaluates changes against compiled subscription rules and maintains
/// a RoaringBitmap of node IDs currently in scope.
pub struct SubscriptionFilter {
    rules: Vec<CompiledRule>,
    scope_bitmap: RoaringBitmap,
    direction: SyncDirection,
}

impl SubscriptionFilter {
    /// Create a new filter from a compiled subscription and initial bitmap.
    pub fn new(def: &SubscriptionDef, bitmap: RoaringBitmap) -> Self {
        Self {
            rules: def.rules.clone(),
            scope_bitmap: bitmap,
            direction: def.direction,
        }
    }

    /// The current scope bitmap (for serialization in handshake response).
    pub fn scope_bitmap(&self) -> &RoaringBitmap {
        &self.scope_bitmap
    }

    /// The sync direction for this subscription.
    pub fn direction(&self) -> SyncDirection {
        self.direction
    }

    /// Convert NodeId to u32 for bitmap operations, with debug assertion.
    fn bitmap_id(node_id: NodeId) -> u32 {
        debug_assert!(
            u32::try_from(node_id.0).is_ok(),
            "NodeId {} exceeds u32 range for RoaringBitmap",
            node_id.0
        );
        node_id.0 as u32
    }

    /// Add a node to the subscription scope.
    pub fn add_to_scope(&mut self, node_id: u32) {
        self.scope_bitmap.insert(node_id);
    }

    /// Remove a node from the subscription scope (only on NodeDeleted).
    pub fn remove_from_scope(&mut self, node_id: u32) {
        self.scope_bitmap.remove(node_id);
    }

    /// Evaluate whether a node matches any subscription rule using graph state.
    pub fn evaluate_node(&self, graph: &SeleneGraph, node_id: NodeId) -> bool {
        let Some(node) = graph.get_node(node_id) else {
            return false;
        };
        self.rules.iter().any(|rule| {
            // Labels: at least one rule label must be present on the node
            let labels_match = rule.labels.iter().any(|rl| node.labels.contains(*rl));
            if !labels_match {
                return false;
            }
            // Predicates: all must match (AND)
            rule.predicates.iter().all(|pred| {
                let actual = node.properties.get(pred.key());
                pred.evaluate(actual)
            })
        })
    }

    /// Check if a single change is in scope (bitmap-only, no graph access).
    ///
    /// NodeId.0 is u64; RoaringBitmap uses u32. Cast is safe for realistic graph sizes.
    pub fn matches(&self, change: &Change) -> bool {
        match change {
            Change::NodeCreated { node_id }
            | Change::NodeDeleted { node_id, .. }
            | Change::PropertySet { node_id, .. }
            | Change::PropertyRemoved { node_id, .. }
            | Change::LabelAdded { node_id, .. }
            | Change::LabelRemoved { node_id, .. } => {
                self.scope_bitmap.contains(Self::bitmap_id(*node_id))
            }
            Change::EdgeCreated { source, target, .. }
            | Change::EdgeDeleted { source, target, .. }
            | Change::EdgePropertySet { source, target, .. }
            | Change::EdgePropertyRemoved { source, target, .. } => {
                self.scope_bitmap.contains(Self::bitmap_id(*source))
                    && self.scope_bitmap.contains(Self::bitmap_id(*target))
            }
        }
    }

    /// Filter a batch of changes with two-pass scope-entry handling.
    ///
    /// Pass 1: Scan for LabelAdded/PropertySet that would bring nodes
    ///         into scope. Update bitmap before filtering.
    /// Pass 2: Filter all changes against the (updated) bitmap.
    ///         Remove deleted nodes after forwarding.
    ///
    /// The graph must reflect the current state (changes already applied).
    pub fn filter_changes(&mut self, changes: &[Change], graph: &SeleneGraph) -> Vec<Change> {
        // Pass 1: Identify scope entries and update bitmap
        for change in changes {
            match change {
                Change::LabelAdded { node_id, .. } | Change::PropertySet { node_id, .. } => {
                    if !self.scope_bitmap.contains(Self::bitmap_id(*node_id))
                        && self.evaluate_node(graph, *node_id)
                    {
                        self.scope_bitmap.insert(Self::bitmap_id(*node_id));
                    }
                }
                _ => {}
            }
        }

        // Pass 2: Filter changes against updated bitmap
        let mut result = Vec::new();
        for change in changes {
            if self.matches(change) {
                result.push(change.clone());
            }
            // Remove from bitmap AFTER forwarding the delete
            if let Change::NodeDeleted { node_id, .. } = change {
                self.scope_bitmap.remove(Self::bitmap_id(*node_id));
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use roaring::RoaringBitmap;
    use selene_core::{EdgeId, LabelSet, NodeId, PropertyMap, Value};
    use selene_graph::{SeleneGraph, SharedGraph};
    use selene_wire::dto::sync::SubscriptionRuleConfig;

    use super::*;

    fn make_sensor_subscription() -> SubscriptionConfig {
        SubscriptionConfig {
            name: "test-sub".to_string(),
            rules: vec![SubscriptionRuleConfig {
                labels: vec!["Sensor".to_string()],
                predicates: vec![PropertyPredicateConfig::Eq {
                    key: "building".to_string(),
                    value: Value::String("HQ".into()),
                }],
            }],
            direction: SyncDirectionConfig::Bidirectional,
        }
    }

    #[test]
    fn compile_subscription_config() {
        let config = make_sensor_subscription();
        let def = SubscriptionDef::compile(&config);

        assert_eq!(def.name, "test-sub");
        assert_eq!(def.rules.len(), 1);
        assert_eq!(def.rules[0].labels.len(), 1);
        assert_eq!(def.rules[0].labels[0].as_str(), "Sensor");
        assert_eq!(def.rules[0].predicates.len(), 1);
        assert!(
            matches!(&def.rules[0].predicates[0], PropertyPredicate::Eq { key, .. } if key.as_str() == "building")
        );
        assert_eq!(def.direction, SyncDirection::Bidirectional);
    }

    #[test]
    fn predicate_eq_matches() {
        let pred = PropertyPredicate::Eq {
            key: IStr::new("building"),
            value: Value::String("HQ".into()),
        };
        assert!(pred.evaluate(Some(&Value::String("HQ".into()))));
        assert!(!pred.evaluate(Some(&Value::String("Annex".into()))));
        assert!(!pred.evaluate(None));
    }

    #[test]
    fn predicate_in_matches() {
        let pred = PropertyPredicate::In {
            key: IStr::new("floor"),
            values: vec![Value::Int(1), Value::Int(2), Value::Int(3)],
        };
        assert!(pred.evaluate(Some(&Value::Int(2))));
        assert!(!pred.evaluate(Some(&Value::Int(5))));
        assert!(!pred.evaluate(None));
    }

    #[test]
    fn predicate_gt_matches() {
        let pred = PropertyPredicate::Gt {
            key: IStr::new("temp"),
            value: Value::Float(20.0),
        };
        assert!(pred.evaluate(Some(&Value::Float(21.5))));
        assert!(!pred.evaluate(Some(&Value::Float(20.0))));
        assert!(!pred.evaluate(Some(&Value::Float(19.0))));
    }

    #[test]
    fn predicate_lt_matches() {
        let pred = PropertyPredicate::Lt {
            key: IStr::new("temp"),
            value: Value::Float(20.0),
        };
        assert!(pred.evaluate(Some(&Value::Float(19.0))));
        assert!(!pred.evaluate(Some(&Value::Float(20.0))));
        assert!(!pred.evaluate(Some(&Value::Float(21.0))));
    }

    #[test]
    fn predicate_gte_matches() {
        let pred = PropertyPredicate::Gte {
            key: IStr::new("temp"),
            value: Value::Float(20.0),
        };
        assert!(pred.evaluate(Some(&Value::Float(20.0))));
        assert!(pred.evaluate(Some(&Value::Float(21.0))));
        assert!(!pred.evaluate(Some(&Value::Float(19.0))));
    }

    #[test]
    fn predicate_lte_matches() {
        let pred = PropertyPredicate::Lte {
            key: IStr::new("temp"),
            value: Value::Float(20.0),
        };
        assert!(pred.evaluate(Some(&Value::Float(20.0))));
        assert!(pred.evaluate(Some(&Value::Float(19.0))));
        assert!(!pred.evaluate(Some(&Value::Float(21.0))));
    }

    #[test]
    fn predicate_is_not_null_matches() {
        let pred = PropertyPredicate::IsNotNull {
            key: IStr::new("name"),
        };
        assert!(pred.evaluate(Some(&Value::String("hello".into()))));
        assert!(!pred.evaluate(Some(&Value::Null)));
        assert!(!pred.evaluate(None));
    }

    #[test]
    fn predicate_cross_type_int_float() {
        let pred = PropertyPredicate::Gt {
            key: IStr::new("val"),
            value: Value::Float(10.5),
        };
        // Int(11) > Float(10.5) via f64 promotion
        assert!(pred.evaluate(Some(&Value::Int(11))));
        assert!(!pred.evaluate(Some(&Value::Int(10))));
    }

    #[test]
    fn predicate_uint_int_cross_type() {
        let pred = PropertyPredicate::Gt {
            key: IStr::new("floor"),
            value: Value::Int(2),
        };
        assert!(pred.evaluate(Some(&Value::UInt(3))));
        assert!(!pred.evaluate(Some(&Value::UInt(1))));
    }

    #[test]
    fn predicate_uint_float_cross_type() {
        let pred = PropertyPredicate::Lt {
            key: IStr::new("temp"),
            value: Value::Float(25.5),
        };
        assert!(pred.evaluate(Some(&Value::UInt(20))));
        assert!(!pred.evaluate(Some(&Value::UInt(30))));
    }

    // ── SubscriptionFilter tests ──────────────────────────────────────────────

    /// Build a SharedGraph with a single Sensor node with building=<building>.
    /// Returns the shared graph and the assigned NodeId.
    fn sensor_shared_graph(building: &str) -> (SharedGraph, NodeId) {
        let building = building.to_string();
        let g = SeleneGraph::new();
        let shared = SharedGraph::new(g);
        let (id, _) = shared
            .write(|m| {
                let mut props = PropertyMap::new();
                props.insert(
                    IStr::new("building"),
                    Value::String(building.clone().into()),
                );
                m.create_node(LabelSet::from_strs(&["Sensor"]), props)
            })
            .unwrap();
        (shared, id)
    }

    fn make_filter(config: &SubscriptionConfig, bitmap: RoaringBitmap) -> SubscriptionFilter {
        let def = SubscriptionDef::compile(config);
        SubscriptionFilter::new(&def, bitmap)
    }

    #[test]
    fn label_only_rule_matches() {
        let config = SubscriptionConfig {
            name: "test".into(),
            rules: vec![SubscriptionRuleConfig {
                labels: vec!["Sensor".into()],
                predicates: vec![],
            }],
            direction: SyncDirectionConfig::Bidirectional,
        };
        let (shared, id) = sensor_shared_graph("HQ");
        let filter = make_filter(&config, RoaringBitmap::new());

        shared.read(|g| {
            assert!(filter.evaluate_node(g, id));
            assert!(!filter.evaluate_node(g, NodeId(999)));
        });
    }

    #[test]
    fn label_plus_eq_predicate_evaluates() {
        let config = make_sensor_subscription();

        let (shared_hq, id_hq) = sensor_shared_graph("HQ");
        let (shared_annex, id_annex) = sensor_shared_graph("Annex");
        let filter = make_filter(&config, RoaringBitmap::new());

        shared_hq.read(|g| assert!(filter.evaluate_node(g, id_hq)));
        shared_annex.read(|g| assert!(!filter.evaluate_node(g, id_annex)));
    }

    #[test]
    fn rules_are_ored() {
        let config = SubscriptionConfig {
            name: "test".into(),
            rules: vec![
                SubscriptionRuleConfig {
                    labels: vec!["Sensor".into()],
                    predicates: vec![PropertyPredicateConfig::Eq {
                        key: "building".into(),
                        value: Value::String("HQ".into()),
                    }],
                },
                SubscriptionRuleConfig {
                    labels: vec!["Equipment".into()],
                    predicates: vec![],
                },
            ],
            direction: SyncDirectionConfig::Bidirectional,
        };

        let g = SeleneGraph::new();
        let shared = SharedGraph::new(g);
        let (ids, _) = shared
            .write(|m| {
                let mut sensor_props = PropertyMap::new();
                sensor_props.insert(IStr::new("building"), Value::String("HQ".into()));
                let sensor_id = m.create_node(LabelSet::from_strs(&["Sensor"]), sensor_props)?;

                let mut equip_props = PropertyMap::new();
                equip_props.insert(IStr::new("type"), Value::String("HVAC".into()));
                let equip_id = m.create_node(LabelSet::from_strs(&["Equipment"]), equip_props)?;

                Ok((sensor_id, equip_id))
            })
            .unwrap();

        let filter = make_filter(&config, RoaringBitmap::new());
        shared.read(|g| {
            assert!(filter.evaluate_node(g, ids.0)); // matches rule 1 (Sensor+HQ)
            assert!(filter.evaluate_node(g, ids.1)); // matches rule 2 (Equipment)
        });
    }

    #[test]
    fn predicates_are_anded() {
        let config = SubscriptionConfig {
            name: "test".into(),
            rules: vec![SubscriptionRuleConfig {
                labels: vec!["Sensor".into()],
                predicates: vec![
                    PropertyPredicateConfig::Eq {
                        key: "building".into(),
                        value: Value::String("HQ".into()),
                    },
                    PropertyPredicateConfig::Gt {
                        key: "floor".into(),
                        value: Value::Int(2),
                    },
                ],
            }],
            direction: SyncDirectionConfig::Bidirectional,
        };

        // Node with building=HQ but no floor property - should NOT match (AND fails)
        let (shared_hq, id_hq) = sensor_shared_graph("HQ");
        let filter = make_filter(&config, RoaringBitmap::new());
        shared_hq.read(|g| assert!(!filter.evaluate_node(g, id_hq)));

        // Node with building=HQ and floor=3 (passes both predicates)
        let g = SeleneGraph::new();
        let shared2 = SharedGraph::new(g);
        let (id2, _) = shared2
            .write(|m| {
                let mut props = PropertyMap::new();
                props.insert(IStr::new("building"), Value::String("HQ".into()));
                props.insert(IStr::new("floor"), Value::Int(3));
                m.create_node(LabelSet::from_strs(&["Sensor"]), props)
            })
            .unwrap();
        shared2.read(|g| assert!(filter.evaluate_node(g, id2)));
    }

    #[test]
    fn edge_both_endpoints_in_bitmap() {
        let config = make_sensor_subscription();
        let (shared, id1) = sensor_shared_graph("HQ");
        let (id2, _) = shared
            .write(|m| {
                let mut props = PropertyMap::new();
                props.insert(IStr::new("building"), Value::String("HQ".into()));
                m.create_node(LabelSet::from_strs(&["Sensor"]), props)
            })
            .unwrap();

        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(id1.0 as u32);
        bitmap.insert(id2.0 as u32);
        let filter = make_filter(&config, bitmap);

        let edge_change = Change::EdgeCreated {
            edge_id: EdgeId(100),
            source: id1,
            target: id2,
            label: IStr::new("feeds"),
        };
        assert!(filter.matches(&edge_change));
    }

    #[test]
    fn edge_one_endpoint_outside_bitmap() {
        let config = make_sensor_subscription();
        let (shared, id1) = sensor_shared_graph("HQ");
        let (id2, _) = shared
            .write(|m| {
                let mut props = PropertyMap::new();
                props.insert(IStr::new("building"), Value::String("HQ".into()));
                m.create_node(LabelSet::from_strs(&["Sensor"]), props)
            })
            .unwrap();

        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(id1.0 as u32);
        // id2 is NOT in bitmap
        let filter = make_filter(&config, bitmap);

        let edge_change = Change::EdgeCreated {
            edge_id: EdgeId(100),
            source: id1,
            target: id2,
            label: IStr::new("feeds"),
        };
        assert!(!filter.matches(&edge_change));
    }

    #[test]
    fn node_created_deferred_without_labels() {
        let config = make_sensor_subscription();
        let filter = make_filter(&config, RoaringBitmap::new());

        let change = Change::NodeCreated { node_id: NodeId(1) };
        assert!(!filter.matches(&change));
    }

    #[test]
    fn soft_boundary_label_removed_stays_in_bitmap() {
        let config = make_sensor_subscription();
        let (_, id) = sensor_shared_graph("HQ");

        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(id.0 as u32);
        let filter = make_filter(&config, bitmap);

        let change = Change::LabelRemoved {
            node_id: id,
            label: IStr::new("Sensor"),
        };
        assert!(filter.matches(&change));
    }

    #[test]
    fn soft_boundary_property_removed_stays_in_bitmap() {
        let config = make_sensor_subscription();
        let (_, id) = sensor_shared_graph("HQ");

        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(id.0 as u32);
        let filter = make_filter(&config, bitmap);

        let change = Change::PropertyRemoved {
            node_id: id,
            key: IStr::new("building"),
            old_value: Some(Value::String("HQ".into())),
        };
        assert!(filter.matches(&change));
    }

    #[test]
    fn node_deleted_removes_from_bitmap() {
        let config = make_sensor_subscription();
        let (_, id) = sensor_shared_graph("HQ");

        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(id.0 as u32);
        let mut filter = make_filter(&config, bitmap);

        let changes = vec![Change::NodeDeleted {
            node_id: id,
            labels: vec![IStr::new("Sensor")],
        }];
        let graph = SeleneGraph::new();
        let filtered = filter.filter_changes(&changes, &graph);

        assert_eq!(filtered.len(), 1); // delete is forwarded
        assert!(!filter.scope_bitmap().contains(id.0 as u32)); // then removed from bitmap
    }

    #[test]
    fn filter_changes_batch_with_scope_entry() {
        let config = make_sensor_subscription();
        let (shared, id) = sensor_shared_graph("HQ");
        let mut filter = make_filter(&config, RoaringBitmap::new());

        let changes = vec![
            Change::NodeCreated { node_id: id },
            Change::LabelAdded {
                node_id: id,
                label: IStr::new("Sensor"),
            },
            Change::PropertySet {
                node_id: id,
                key: IStr::new("building"),
                value: Value::String("HQ".into()),
                old_value: None,
            },
        ];

        shared.read(|g| {
            let filtered = filter.filter_changes(&changes, g);
            assert_eq!(filtered.len(), 3); // all forwarded (two-pass: scope entry in pass 1)
        });
        assert!(filter.scope_bitmap().contains(id.0 as u32));
    }

    #[test]
    fn empty_rules_matches_nothing() {
        let config = SubscriptionConfig {
            name: "empty".into(),
            rules: vec![],
            direction: SyncDirectionConfig::Bidirectional,
        };
        let mut filter = make_filter(&config, RoaringBitmap::new());

        // Use a fresh empty graph -- no nodes, no labels.
        let graph = SeleneGraph::new();
        let changes = vec![
            Change::NodeCreated { node_id: NodeId(1) },
            Change::LabelAdded {
                node_id: NodeId(1),
                label: IStr::new("Sensor"),
            },
        ];
        let filtered = filter.filter_changes(&changes, &graph);
        assert!(filtered.is_empty(), "empty rules should match nothing");
        assert!(
            !filter.scope_bitmap().contains(1),
            "node should not enter scope when there are no rules"
        );
    }

    #[test]
    fn filter_changes_excludes_out_of_scope() {
        let config = make_sensor_subscription();
        let (shared, id) = sensor_shared_graph("Annex"); // wrong building
        let mut filter = make_filter(&config, RoaringBitmap::new());

        let changes = vec![
            Change::NodeCreated { node_id: id },
            Change::LabelAdded {
                node_id: id,
                label: IStr::new("Sensor"),
            },
            Change::PropertySet {
                node_id: id,
                key: IStr::new("building"),
                value: Value::String("Annex".into()),
                old_value: None,
            },
        ];

        shared.read(|g| {
            let filtered = filter.filter_changes(&changes, g);
            assert_eq!(filtered.len(), 0);
        });
        assert!(!filter.scope_bitmap().contains(id.0 as u32));
    }
}
