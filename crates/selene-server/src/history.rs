//! HistoryProvider implementation — bridges changelog + version store to GQL history procedures.

use std::sync::Arc;

use parking_lot::Mutex;
use selene_core::Value;
use selene_core::changeset::Change;
use selene_core::entity::NodeId;
use selene_gql::runtime::procedures::history::{
    HistoryEntry, HistoryProvider, PropertyVersionEntry,
};
use selene_graph::{ChangelogBuffer, SeleneGraph};

/// HistoryProvider backed by the server's ChangelogBuffer and optional VersionStore.
pub struct ChangelogHistoryProvider {
    changelog: Arc<Mutex<ChangelogBuffer>>,
    version_store: Option<Arc<parking_lot::RwLock<crate::version_store::VersionStore>>>,
}

impl ChangelogHistoryProvider {
    pub fn new(changelog: Arc<Mutex<ChangelogBuffer>>) -> Self {
        Self {
            changelog,
            version_store: None,
        }
    }

    /// Attach a version store for point-in-time property queries.
    pub fn with_version_store(
        mut self,
        vs: Arc<parking_lot::RwLock<crate::version_store::VersionStore>>,
    ) -> Self {
        self.version_store = Some(vs);
        self
    }
}

impl HistoryProvider for ChangelogHistoryProvider {
    fn entity_history(
        &self,
        entity_id: u64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: usize,
    ) -> Vec<HistoryEntry> {
        let buf = self.changelog.lock();
        let entries = buf.entity_history(entity_id, start_time, end_time);

        let mut result = Vec::new();
        for entry in entries {
            for change in &entry.changes {
                if change.affected_node_ids().contains(&entity_id) {
                    result.push(change_to_history_entry(change, entry.timestamp_nanos));
                    if result.len() >= limit {
                        return result;
                    }
                }
            }
        }
        result
    }

    fn label_changes(
        &self,
        label: &str,
        since_nanos: i64,
        limit: usize,
        graph: &SeleneGraph,
    ) -> Vec<HistoryEntry> {
        let buf = self.changelog.lock();
        // Label filtering uses the current snapshot, so changes on deleted nodes
        // are omitted (per-entity history is not affected).
        let Some(all_entries) = buf.since(0) else {
            return vec![];
        };

        let mut result = Vec::new();
        for entry in all_entries.iter().rev() {
            if entry.timestamp_nanos < since_nanos {
                break;
            }
            for change in &entry.changes {
                if let Some(nid) = change.node_id() {
                    // Check if the node has the requested label
                    if let Some(node) = graph.get_node(selene_core::NodeId(nid))
                        && node.labels.contains_str(label)
                    {
                        result.push(change_to_history_entry(change, entry.timestamp_nanos));
                        if result.len() >= limit {
                            return result;
                        }
                    }
                }
            }
        }
        result
    }

    fn property_at(
        &self,
        node_id: u64,
        key: &str,
        timestamp: i64,
        graph: &SeleneGraph,
    ) -> Option<Value> {
        let vs = self.version_store.as_ref()?;
        let vs_guard = vs.read();
        let node = graph.get_node(NodeId(node_id))?;
        let current_value = node.property(key);
        let current_updated_at = node.updated_at;
        vs_guard.value_at(
            NodeId(node_id),
            key,
            timestamp,
            current_value,
            current_updated_at,
        )
    }

    fn property_history(
        &self,
        node_id: u64,
        key: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Vec<PropertyVersionEntry> {
        let Some(vs) = self.version_store.as_ref() else {
            return vec![];
        };
        let vs_guard = vs.read();
        vs_guard
            .property_history(NodeId(node_id), key, start_time, end_time)
            .into_iter()
            .map(|pv| PropertyVersionEntry {
                value: pv.value.clone(),
                superseded_at: pv.superseded_at,
            })
            .collect()
    }
}

/// Convert a Change variant to a HistoryEntry.
fn change_to_history_entry(change: &Change, timestamp_nanos: i64) -> HistoryEntry {
    match change {
        Change::NodeCreated { node_id } => HistoryEntry {
            node_id: node_id.0,
            change_type: "NodeCreated",
            key: None,
            old_value: None,
            new_value: None,
            timestamp_nanos,
        },
        Change::NodeDeleted { node_id, .. } => HistoryEntry {
            node_id: node_id.0,
            change_type: "NodeDeleted",
            key: None,
            old_value: None,
            new_value: None,
            timestamp_nanos,
        },
        Change::PropertySet {
            node_id,
            key,
            value,
            old_value,
        } => HistoryEntry {
            node_id: node_id.0,
            change_type: "PropertySet",
            key: Some(key.to_string()),
            old_value: old_value.clone(),
            new_value: Some(value.clone()),
            timestamp_nanos,
        },
        Change::PropertyRemoved {
            node_id,
            key,
            old_value,
        } => HistoryEntry {
            node_id: node_id.0,
            change_type: "PropertyRemoved",
            key: Some(key.to_string()),
            old_value: old_value.clone(),
            new_value: None,
            timestamp_nanos,
        },
        Change::LabelAdded { node_id, label } => HistoryEntry {
            node_id: node_id.0,
            change_type: "LabelAdded",
            key: Some(label.to_string()),
            old_value: None,
            new_value: Some(Value::String(label.as_ref().into())),
            timestamp_nanos,
        },
        Change::LabelRemoved { node_id, label } => HistoryEntry {
            node_id: node_id.0,
            change_type: "LabelRemoved",
            key: Some(label.to_string()),
            old_value: Some(Value::String(label.as_ref().into())),
            new_value: None,
            timestamp_nanos,
        },
        Change::EdgeCreated { source, .. } => HistoryEntry {
            node_id: source.0,
            change_type: "EdgeCreated",
            key: None,
            old_value: None,
            new_value: None,
            timestamp_nanos,
        },
        Change::EdgeDeleted { source, .. } => HistoryEntry {
            node_id: source.0,
            change_type: "EdgeDeleted",
            key: None,
            old_value: None,
            new_value: None,
            timestamp_nanos,
        },
        Change::EdgePropertySet {
            source,
            key,
            value,
            old_value,
            ..
        } => HistoryEntry {
            node_id: source.0,
            change_type: "EdgePropertySet",
            key: Some(key.to_string()),
            old_value: old_value.clone(),
            new_value: Some(value.clone()),
            timestamp_nanos,
        },
        Change::EdgePropertyRemoved {
            source,
            key,
            old_value,
            ..
        } => HistoryEntry {
            node_id: source.0,
            change_type: "EdgePropertyRemoved",
            key: Some(key.to_string()),
            old_value: old_value.clone(),
            new_value: None,
            timestamp_nanos,
        },
    }
}

/// Initialize the history provider from the server's changelog buffer.
pub fn init_history_provider(changelog: Arc<Mutex<ChangelogBuffer>>) {
    let provider = ChangelogHistoryProvider::new(changelog);
    selene_gql::runtime::procedures::history::set_history_provider(Arc::new(provider));
}

/// Initialize the history provider with version store support.
pub fn init_history_provider_with_versions(
    changelog: Arc<Mutex<ChangelogBuffer>>,
    version_store: Arc<parking_lot::RwLock<crate::version_store::VersionStore>>,
) {
    let provider = ChangelogHistoryProvider::new(changelog).with_version_store(version_store);
    selene_gql::runtime::procedures::history::set_history_provider(Arc::new(provider));
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::IStr;
    use selene_core::changeset::Change;
    use selene_core::entity::NodeId;
    use selene_core::value::Value;
    use smol_str::SmolStr;

    fn make_changelog() -> Arc<Mutex<ChangelogBuffer>> {
        Arc::new(Mutex::new(ChangelogBuffer::new(100)))
    }

    #[test]
    fn entity_history_basic() {
        let changelog = make_changelog();
        let provider = ChangelogHistoryProvider::new(Arc::clone(&changelog));

        // Insert a node, then set a property
        {
            let mut buf = changelog.lock();
            buf.append(vec![Change::NodeCreated { node_id: NodeId(1) }], 1_000, 0);
            buf.append(
                vec![Change::PropertySet {
                    node_id: NodeId(1),
                    key: IStr::new("temp"),
                    value: Value::Float(22.5),
                    old_value: None,
                }],
                2_000,
                0,
            );
        }

        let history = provider.entity_history(1, None, None, 100);
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].change_type, "NodeCreated");
        assert_eq!(history[0].timestamp_nanos, 1_000);
        assert_eq!(history[1].change_type, "PropertySet");
        assert_eq!(history[1].key.as_deref(), Some("temp"));
        assert_eq!(history[1].new_value, Some(Value::Float(22.5)));
        assert!(history[1].old_value.is_none());
    }

    #[test]
    fn entity_history_with_old_value() {
        let changelog = make_changelog();
        let provider = ChangelogHistoryProvider::new(Arc::clone(&changelog));

        {
            let mut buf = changelog.lock();
            buf.append(
                vec![Change::PropertySet {
                    node_id: NodeId(1),
                    key: IStr::new("temp"),
                    value: Value::Float(22.5),
                    old_value: None,
                }],
                1_000,
                0,
            );
            buf.append(
                vec![Change::PropertySet {
                    node_id: NodeId(1),
                    key: IStr::new("temp"),
                    value: Value::Float(25.0),
                    old_value: Some(Value::Float(22.5)),
                }],
                2_000,
                0,
            );
        }

        let history = provider.entity_history(1, None, None, 100);
        assert_eq!(history.len(), 2);
        // Second change should have old_value
        assert_eq!(history[1].old_value, Some(Value::Float(22.5)));
        assert_eq!(history[1].new_value, Some(Value::Float(25.0)));
    }

    #[test]
    fn entity_history_time_filter() {
        let changelog = make_changelog();
        let provider = ChangelogHistoryProvider::new(Arc::clone(&changelog));

        {
            let mut buf = changelog.lock();
            buf.append(vec![Change::NodeCreated { node_id: NodeId(1) }], 1_000, 0);
            buf.append(
                vec![Change::PropertySet {
                    node_id: NodeId(1),
                    key: IStr::new("temp"),
                    value: Value::Float(22.5),
                    old_value: None,
                }],
                3_000,
                0,
            );
        }

        // Only changes after timestamp 2000
        let history = provider.entity_history(1, Some(2_000), None, 100);
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].change_type, "PropertySet");
    }

    #[test]
    fn entity_history_limit() {
        let changelog = make_changelog();
        let provider = ChangelogHistoryProvider::new(Arc::clone(&changelog));

        {
            let mut buf = changelog.lock();
            for i in 0..10 {
                buf.append(
                    vec![Change::PropertySet {
                        node_id: NodeId(1),
                        key: IStr::new("temp"),
                        value: Value::Float(i as f64),
                        old_value: None,
                    }],
                    (i + 1) * 1_000,
                    0,
                );
            }
        }

        let history = provider.entity_history(1, None, None, 3);
        assert_eq!(history.len(), 3);
    }

    #[test]
    fn entity_history_empty_for_unknown_node() {
        let changelog = make_changelog();
        let provider = ChangelogHistoryProvider::new(Arc::clone(&changelog));

        let history = provider.entity_history(999, None, None, 100);
        assert!(history.is_empty());
    }

    #[test]
    fn label_changes_filters_by_label() {
        let changelog = make_changelog();
        let provider = ChangelogHistoryProvider::new(Arc::clone(&changelog));

        // Build a graph with labeled nodes
        let mut g = selene_graph::SeleneGraph::new();
        let mut m = g.mutate();
        let id = m
            .create_node(
                selene_core::LabelSet::from_strs(&["sensor"]),
                selene_core::PropertyMap::from_pairs(vec![(
                    selene_core::IStr::new("name"),
                    Value::String(SmolStr::new("T1")),
                )]),
            )
            .unwrap();
        m.commit(0).unwrap();

        // Add a change for this node to the changelog
        {
            let mut buf = changelog.lock();
            buf.append(
                vec![Change::PropertySet {
                    node_id: id,
                    key: IStr::new("temp"),
                    value: Value::Float(22.5),
                    old_value: None,
                }],
                selene_core::entity::now_nanos(),
                0,
            );
        }

        // Query changes for label 'sensor' in the last hour
        let since = selene_core::entity::now_nanos() - 3_600_000_000_000;
        let changes = provider.label_changes("sensor", since, 100, &g);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].change_type, "PropertySet");

        // Query for different label should return empty
        let changes = provider.label_changes("building", since, 100, &g);
        assert!(changes.is_empty());
    }
}
