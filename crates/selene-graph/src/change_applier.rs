//! Change applier -- converts Change events into graph mutations.
//!
//! Used by the replica to replay changes from the primary.
//! Uses `_raw` methods exclusively (no TrackedMutation, no changelog echo,
//! no schema validation, no triggers -- those already ran on the primary).

use selene_core::changeset::Change;
use selene_core::{Edge, LabelSet, Node, Origin, PropertyMap};

use crate::graph::SeleneGraph;

/// Apply a batch of changes to the graph.
///
/// Processes all changes sequentially. Order matters because
/// `NodeCreated` only carries `node_id`, with labels and properties
/// arriving as separate `LabelAdded` and `PropertySet` changes
/// within the same batch.
///
/// The `_origin` parameter tags whether these changes are local or
/// replicated. Not used in the graph layer yet, but establishes the
/// API contract for server-layer WAL tagging.
pub fn apply_changes(graph: &mut SeleneGraph, changes: &[Change], _origin: Origin) {
    for change in changes {
        match change {
            Change::NodeCreated { node_id } => {
                if graph.get_node(*node_id).is_none() {
                    let node = Node::new(*node_id, LabelSet::new(), PropertyMap::new());
                    graph.insert_node_raw(node);
                }
                graph.ensure_next_node_id_above(*node_id);
            }
            Change::LabelAdded { node_id, label } => {
                graph.add_label_raw(*node_id, *label);
            }
            Change::LabelRemoved { node_id, label } => {
                graph.remove_label_raw(*node_id, *label);
            }
            Change::PropertySet {
                node_id,
                key,
                value,
                ..
            } => {
                graph.set_property_raw(*node_id, *key, value.clone());
            }
            Change::PropertyRemoved { node_id, key, .. } => {
                graph.remove_property_raw(*node_id, *key);
            }
            Change::NodeDeleted { node_id, .. } => {
                graph.remove_node_raw(*node_id);
            }
            Change::EdgeCreated {
                edge_id,
                source,
                target,
                label,
            } => {
                if graph.get_edge(*edge_id).is_none() {
                    let edge = Edge::new(*edge_id, *source, *target, *label, PropertyMap::new());
                    graph.insert_edge_raw(edge);
                }
                graph.ensure_next_edge_id_above(*edge_id);
            }
            Change::EdgeDeleted { edge_id, .. } => {
                graph.remove_edge_raw(*edge_id);
            }
            Change::EdgePropertySet {
                edge_id,
                key,
                value,
                ..
            } => {
                graph.set_edge_property_raw(*edge_id, *key, value.clone());
            }
            Change::EdgePropertyRemoved { edge_id, key, .. } => {
                graph.remove_edge_property_raw(*edge_id, *key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use selene_core::IStr;
    use selene_core::Value;
    use selene_core::entity::{EdgeId, NodeId};

    use super::*;

    #[test]
    fn apply_node_created() {
        let mut graph = SeleneGraph::new();
        apply_changes(
            &mut graph,
            &[Change::NodeCreated { node_id: NodeId(1) }],
            Origin::Local,
        );
        assert!(graph.contains_node(NodeId(1)));
        assert_eq!(graph.node_count(), 1);
    }

    #[test]
    fn apply_label_added() {
        let mut graph = SeleneGraph::new();
        apply_changes(
            &mut graph,
            &[
                Change::NodeCreated { node_id: NodeId(1) },
                Change::LabelAdded {
                    node_id: NodeId(1),
                    label: IStr::new("sensor"),
                },
            ],
            Origin::Local,
        );
        let node = graph.get_node(NodeId(1)).unwrap();
        assert!(node.labels.contains(IStr::new("sensor")));
    }

    #[test]
    fn apply_label_removed() {
        let mut graph = SeleneGraph::new();
        apply_changes(
            &mut graph,
            &[
                Change::NodeCreated { node_id: NodeId(1) },
                Change::LabelAdded {
                    node_id: NodeId(1),
                    label: IStr::new("sensor"),
                },
            ],
            Origin::Local,
        );
        apply_changes(
            &mut graph,
            &[Change::LabelRemoved {
                node_id: NodeId(1),
                label: IStr::new("sensor"),
            }],
            Origin::Local,
        );
        let node = graph.get_node(NodeId(1)).unwrap();
        assert!(!node.labels.contains(IStr::new("sensor")));
    }

    #[test]
    fn apply_property_set() {
        let mut graph = SeleneGraph::new();
        apply_changes(
            &mut graph,
            &[
                Change::NodeCreated { node_id: NodeId(1) },
                Change::PropertySet {
                    node_id: NodeId(1),
                    key: IStr::new("temp"),
                    value: Value::Float(22.5),
                    old_value: None,
                },
            ],
            Origin::Local,
        );
        let node = graph.get_node(NodeId(1)).unwrap();
        assert_eq!(
            node.properties.get(IStr::new("temp")),
            Some(&Value::Float(22.5))
        );
    }

    #[test]
    fn apply_property_removed() {
        let mut graph = SeleneGraph::new();
        apply_changes(
            &mut graph,
            &[
                Change::NodeCreated { node_id: NodeId(1) },
                Change::PropertySet {
                    node_id: NodeId(1),
                    key: IStr::new("temp"),
                    value: Value::Float(22.5),
                    old_value: None,
                },
            ],
            Origin::Local,
        );
        apply_changes(
            &mut graph,
            &[Change::PropertyRemoved {
                node_id: NodeId(1),
                key: IStr::new("temp"),
                old_value: Some(Value::Float(22.5)),
            }],
            Origin::Local,
        );
        let node = graph.get_node(NodeId(1)).unwrap();
        assert!(node.properties.get(IStr::new("temp")).is_none());
    }

    #[test]
    fn apply_node_deleted() {
        let mut graph = SeleneGraph::new();
        apply_changes(
            &mut graph,
            &[Change::NodeCreated { node_id: NodeId(1) }],
            Origin::Local,
        );
        apply_changes(
            &mut graph,
            &[Change::NodeDeleted {
                node_id: NodeId(1),
                labels: vec![],
            }],
            Origin::Local,
        );
        assert!(!graph.contains_node(NodeId(1)));
    }

    #[test]
    fn apply_edge_created() {
        let mut graph = SeleneGraph::new();
        apply_changes(
            &mut graph,
            &[
                Change::NodeCreated { node_id: NodeId(1) },
                Change::NodeCreated { node_id: NodeId(2) },
                Change::EdgeCreated {
                    edge_id: EdgeId(1),
                    source: NodeId(1),
                    target: NodeId(2),
                    label: IStr::new("feeds"),
                },
            ],
            Origin::Local,
        );
        let edge = graph.get_edge(EdgeId(1)).unwrap();
        assert_eq!(edge.source, NodeId(1));
        assert_eq!(edge.target, NodeId(2));
    }

    #[test]
    fn apply_edge_deleted() {
        let mut graph = SeleneGraph::new();
        apply_changes(
            &mut graph,
            &[
                Change::NodeCreated { node_id: NodeId(1) },
                Change::NodeCreated { node_id: NodeId(2) },
                Change::EdgeCreated {
                    edge_id: EdgeId(1),
                    source: NodeId(1),
                    target: NodeId(2),
                    label: IStr::new("feeds"),
                },
            ],
            Origin::Local,
        );
        apply_changes(
            &mut graph,
            &[Change::EdgeDeleted {
                edge_id: EdgeId(1),
                source: NodeId(1),
                target: NodeId(2),
                label: IStr::new("feeds"),
            }],
            Origin::Local,
        );
        assert!(graph.get_edge(EdgeId(1)).is_none());
    }

    #[test]
    fn apply_edge_property_set() {
        let mut graph = SeleneGraph::new();
        apply_changes(
            &mut graph,
            &[
                Change::NodeCreated { node_id: NodeId(1) },
                Change::NodeCreated { node_id: NodeId(2) },
                Change::EdgeCreated {
                    edge_id: EdgeId(1),
                    source: NodeId(1),
                    target: NodeId(2),
                    label: IStr::new("feeds"),
                },
                Change::EdgePropertySet {
                    edge_id: EdgeId(1),
                    source: NodeId(1),
                    target: NodeId(2),
                    key: IStr::new("weight"),
                    value: Value::Float(3.15),
                    old_value: None,
                },
            ],
            Origin::Local,
        );
        let edge = graph.get_edge(EdgeId(1)).unwrap();
        assert_eq!(
            edge.properties.get(IStr::new("weight")),
            Some(&Value::Float(3.15))
        );
    }

    #[test]
    fn apply_edge_property_removed() {
        let mut graph = SeleneGraph::new();
        apply_changes(
            &mut graph,
            &[
                Change::NodeCreated { node_id: NodeId(1) },
                Change::NodeCreated { node_id: NodeId(2) },
                Change::EdgeCreated {
                    edge_id: EdgeId(1),
                    source: NodeId(1),
                    target: NodeId(2),
                    label: IStr::new("feeds"),
                },
                Change::EdgePropertySet {
                    edge_id: EdgeId(1),
                    source: NodeId(1),
                    target: NodeId(2),
                    key: IStr::new("weight"),
                    value: Value::Float(3.15),
                    old_value: None,
                },
            ],
            Origin::Local,
        );
        apply_changes(
            &mut graph,
            &[Change::EdgePropertyRemoved {
                edge_id: EdgeId(1),
                source: NodeId(1),
                target: NodeId(2),
                key: IStr::new("weight"),
                old_value: Some(Value::Float(3.15)),
            }],
            Origin::Local,
        );
        let edge = graph.get_edge(EdgeId(1)).unwrap();
        assert!(edge.properties.get(IStr::new("weight")).is_none());
    }

    #[test]
    fn replica_promotion_ids_dont_collide() {
        let mut graph = SeleneGraph::new();
        let changes: Vec<Change> = (1..=50)
            .map(|i| Change::NodeCreated { node_id: NodeId(i) })
            .collect();
        apply_changes(&mut graph, &changes, Origin::Replicated);

        let new_id = graph.allocate_node_id().unwrap();
        assert!(
            new_id.0 > 50,
            "replica promotion must not reuse IDs: got {}",
            new_id.0
        );

        let edge_changes: Vec<Change> = (1..=30)
            .map(|i| Change::EdgeCreated {
                edge_id: EdgeId(i),
                source: NodeId(1),
                target: NodeId(2),
                label: IStr::new("test"),
            })
            .collect();
        apply_changes(&mut graph, &edge_changes, Origin::Replicated);
        let new_eid = graph.allocate_edge_id().unwrap();
        assert!(
            new_eid.0 > 30,
            "replica promotion must not reuse edge IDs: got {}",
            new_eid.0
        );
    }

    #[test]
    fn apply_node_created_twice_is_idempotent() {
        let mut graph = SeleneGraph::new();
        let sensor = IStr::new("Sensor");
        apply_changes(
            &mut graph,
            &[
                Change::NodeCreated { node_id: NodeId(1) },
                Change::LabelAdded {
                    node_id: NodeId(1),
                    label: sensor,
                },
            ],
            Origin::Local,
        );
        // Apply NodeCreated for the same ID again.
        apply_changes(
            &mut graph,
            &[Change::NodeCreated { node_id: NodeId(1) }],
            Origin::Local,
        );
        assert_eq!(graph.node_count(), 1);
        let bitmap = graph.label_bitmap("Sensor").expect("bitmap must exist");
        assert_eq!(
            bitmap.len(),
            1,
            "label bitmap must contain exactly one entry"
        );
        assert!(bitmap.contains(1));
    }

    #[test]
    fn apply_edge_created_twice_is_idempotent() {
        let mut graph = SeleneGraph::new();
        apply_changes(
            &mut graph,
            &[
                Change::NodeCreated { node_id: NodeId(1) },
                Change::NodeCreated { node_id: NodeId(2) },
                Change::EdgeCreated {
                    edge_id: EdgeId(1),
                    source: NodeId(1),
                    target: NodeId(2),
                    label: IStr::new("feeds"),
                },
            ],
            Origin::Local,
        );
        // Apply EdgeCreated for the same ID again.
        apply_changes(
            &mut graph,
            &[Change::EdgeCreated {
                edge_id: EdgeId(1),
                source: NodeId(1),
                target: NodeId(2),
                label: IStr::new("feeds"),
            }],
            Origin::Local,
        );
        assert_eq!(graph.edge_count(), 1);
        let outgoing = graph.outgoing(NodeId(1));
        assert_eq!(
            outgoing.iter().filter(|e| **e == EdgeId(1)).count(),
            1,
            "adjacency list must contain edge exactly once"
        );
    }

    #[test]
    fn apply_full_node_lifecycle() {
        let mut graph = SeleneGraph::new();
        apply_changes(
            &mut graph,
            &[
                Change::NodeCreated { node_id: NodeId(1) },
                Change::LabelAdded {
                    node_id: NodeId(1),
                    label: IStr::new("sensor"),
                },
                Change::LabelAdded {
                    node_id: NodeId(1),
                    label: IStr::new("point"),
                },
                Change::PropertySet {
                    node_id: NodeId(1),
                    key: IStr::new("name"),
                    value: Value::str("Zone-A Temp"),
                    old_value: None,
                },
                Change::PropertySet {
                    node_id: NodeId(1),
                    key: IStr::new("unit"),
                    value: Value::str("°F"),
                    old_value: None,
                },
            ],
            Origin::Local,
        );

        let node = graph.get_node(NodeId(1)).unwrap();
        assert!(node.labels.contains(IStr::new("sensor")));
        assert!(node.labels.contains(IStr::new("point")));
        assert_eq!(
            node.properties.get(IStr::new("name")),
            Some(&Value::str("Zone-A Temp"))
        );
        assert_eq!(
            node.properties.get(IStr::new("unit")),
            Some(&Value::str("°F"))
        );
    }
}
