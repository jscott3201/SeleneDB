//! Change types for tracking graph mutations.

use serde::{Deserialize, Serialize};

use crate::IStr;
use crate::Value;
use crate::entity::{EdgeId, NodeId};

/// A single change within a mutation commit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Change {
    NodeCreated {
        node_id: NodeId,
    },
    NodeDeleted {
        node_id: NodeId,
        /// Labels captured before removal (for trigger evaluation and changelog).
        labels: Vec<IStr>,
    },
    PropertySet {
        node_id: NodeId,
        key: IStr,
        value: Value,
        /// Previous value before this SET (None if property was new).
        old_value: Option<Value>,
    },
    PropertyRemoved {
        node_id: NodeId,
        key: IStr,
        /// The value that was removed.
        old_value: Option<Value>,
    },
    LabelAdded {
        node_id: NodeId,
        label: IStr,
    },
    LabelRemoved {
        node_id: NodeId,
        label: IStr,
    },
    EdgeCreated {
        edge_id: EdgeId,
        source: NodeId,
        target: NodeId,
        label: IStr,
    },
    EdgeDeleted {
        edge_id: EdgeId,
        source: NodeId,
        target: NodeId,
        label: IStr,
    },
    EdgePropertySet {
        edge_id: EdgeId,
        source: NodeId,
        target: NodeId,
        key: IStr,
        value: Value,
        /// Previous value before this SET (None if property was new).
        old_value: Option<Value>,
    },
    EdgePropertyRemoved {
        edge_id: EdgeId,
        source: NodeId,
        target: NodeId,
        key: IStr,
        /// The value that was removed.
        old_value: Option<Value>,
    },
}

impl Change {
    /// Extract the primary node ID affected by this change.
    ///
    /// For edge changes, returns the source node only. Use `affected_node_ids()`
    /// when you need both source and target.
    pub fn node_id(&self) -> Option<u64> {
        match self {
            Change::NodeCreated { node_id } => Some(node_id.0),
            Change::NodeDeleted { node_id, .. } => Some(node_id.0),
            Change::PropertySet { node_id, .. } => Some(node_id.0),
            Change::PropertyRemoved { node_id, .. } => Some(node_id.0),
            Change::LabelAdded { node_id, .. } => Some(node_id.0),
            Change::LabelRemoved { node_id, .. } => Some(node_id.0),
            Change::EdgeCreated { source, .. } => Some(source.0),
            Change::EdgeDeleted { source, .. } => Some(source.0),
            Change::EdgePropertySet { source, .. } => Some(source.0),
            Change::EdgePropertyRemoved { source, .. } => Some(source.0),
        }
    }

    /// All node IDs affected by this change.
    ///
    /// For edge changes, returns both source AND target so the changelog
    /// per-entity index tracks edge operations under both endpoints.
    pub fn affected_node_ids(&self) -> smallvec::SmallVec<[u64; 2]> {
        use smallvec::smallvec;
        match self {
            Change::NodeCreated { node_id } => smallvec![node_id.0],
            Change::NodeDeleted { node_id, .. } => smallvec![node_id.0],
            Change::PropertySet { node_id, .. } => smallvec![node_id.0],
            Change::PropertyRemoved { node_id, .. } => smallvec![node_id.0],
            Change::LabelAdded { node_id, .. } => smallvec![node_id.0],
            Change::LabelRemoved { node_id, .. } => smallvec![node_id.0],
            Change::EdgeCreated { source, target, .. } => smallvec![source.0, target.0],
            Change::EdgeDeleted { source, target, .. } => smallvec![source.0, target.0],
            Change::EdgePropertySet { source, target, .. } => smallvec![source.0, target.0],
            Change::EdgePropertyRemoved { source, target, .. } => smallvec![source.0, target.0],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn change_construction() {
        let changes = [
            Change::NodeCreated { node_id: NodeId(1) },
            Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("temp"),
                value: Value::Float(21.5),
                old_value: None,
            },
        ];
        assert_eq!(changes.len(), 2);
    }

    #[test]
    fn label_added_variant() {
        let change = Change::LabelAdded {
            node_id: NodeId(42),
            label: IStr::new("Sensor"),
        };
        if let Change::LabelAdded { node_id, label } = &change {
            assert_eq!(node_id.0, 42);
            assert_eq!(label.as_str(), "Sensor");
        } else {
            panic!("expected LabelAdded");
        }
    }

    #[test]
    fn label_removed_variant() {
        let change = Change::LabelRemoved {
            node_id: NodeId(7),
            label: IStr::new("Offline"),
        };
        if let Change::LabelRemoved { node_id, label } = &change {
            assert_eq!(node_id.0, 7);
            assert_eq!(label.as_str(), "Offline");
        } else {
            panic!("expected LabelRemoved");
        }
    }

    #[test]
    fn edge_created_with_details() {
        let change = Change::EdgeCreated {
            edge_id: EdgeId(100),
            source: NodeId(1),
            target: NodeId(2),
            label: IStr::new("feeds"),
        };
        if let Change::EdgeCreated {
            edge_id,
            source,
            target,
            label,
        } = &change
        {
            assert_eq!(edge_id.0, 100);
            assert_eq!(source.0, 1);
            assert_eq!(target.0, 2);
            assert_eq!(label.as_str(), "feeds");
        } else {
            panic!("expected EdgeCreated");
        }
    }

    #[test]
    fn edge_property_set_variant() {
        let change = Change::EdgePropertySet {
            edge_id: EdgeId(200),
            source: NodeId(1),
            target: NodeId(2),
            key: IStr::new("weight"),
            value: Value::Float(3.14),
            old_value: None,
        };
        if let Change::EdgePropertySet {
            edge_id,
            key,
            value,
            ..
        } = &change
        {
            assert_eq!(edge_id.0, 200);
            assert_eq!(key.as_str(), "weight");
            assert!(matches!(value, Value::Float(_)));
        } else {
            panic!("expected EdgePropertySet");
        }
    }
}
