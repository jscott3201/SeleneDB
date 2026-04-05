//! Core graph data model -- Node and Edge types.

use std::sync::Arc;

use crate::interner::IStr;
use crate::label_set::LabelSet;
use crate::property_map::PropertyMap;
use crate::value::Value;

/// Unique node identifier.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct NodeId(pub u64);

/// Unique edge identifier.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct EdgeId(pub u64);

/// A node in the property graph.
///
/// Nodes have zero or more labels and arbitrary key-value properties.
/// Labels are stored in a sorted `LabelSet` (SmallVec-backed).
/// Properties are stored in a sorted `PropertyMap` (SmallVec-backed).
/// The `cached_json` field caches the JSON representation of properties
/// for fast repeated reads (invalidated on mutation).
#[derive(Clone)]
pub struct Node {
    pub id: NodeId,
    pub labels: LabelSet,
    pub properties: PropertyMap,
    pub created_at: i64,
    pub updated_at: i64,
    pub version: u64,
    /// Cached plain-JSON representation of properties. `None` = dirty.
    /// Populated lazily on first read, cleared on property mutation.
    pub cached_json: Option<Arc<str>>,
}

/// An edge in the property graph.
///
/// Edges are first-class: they have their own label, properties, and identity.
/// The label is an `IStr` (interned string -- Copy, Eq via integer comparison).
#[derive(Debug, Clone)]
pub struct Edge {
    pub id: EdgeId,
    pub source: NodeId,
    pub target: NodeId,
    pub label: IStr,
    pub properties: PropertyMap,
    pub created_at: i64,
}

/// Current time in nanoseconds since Unix epoch.
///
/// Saturates to `i64::MAX` if the system clock is far enough in the future
/// that the nanosecond count overflows `i64` (year ~2262).
pub fn now_nanos() -> i64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    i64::try_from(nanos).unwrap_or(i64::MAX)
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("id", &self.id)
            .field("labels", &self.labels)
            .field("properties", &self.properties)
            .field("version", &self.version)
            .finish()
    }
}

impl Node {
    pub fn new(id: NodeId, labels: LabelSet, properties: PropertyMap) -> Self {
        let now = now_nanos();
        Self {
            id,
            labels,
            properties,
            created_at: now,
            updated_at: now,
            version: 1,
            cached_json: None,
        }
    }

    pub fn has_label(&self, label: &str) -> bool {
        self.labels.contains_str(label)
    }

    pub fn property(&self, key: &str) -> Option<&Value> {
        self.properties.get_by_str(key)
    }

    /// Invalidate the cached JSON (call after any property mutation).
    pub fn invalidate_json_cache(&mut self) {
        self.cached_json = None;
    }

    /// Set the cached JSON representation of properties.
    pub fn set_json_cache(&mut self, json: Arc<str>) {
        self.cached_json = Some(json);
    }
}

impl Edge {
    pub fn new(
        id: EdgeId,
        source: NodeId,
        target: NodeId,
        label: IStr,
        properties: PropertyMap,
    ) -> Self {
        Self {
            id,
            source,
            target,
            label,
            properties,
            created_at: now_nanos(),
        }
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node:{}", self.0)
    }
}

impl std::fmt::Display for EdgeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "edge:{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_creation_with_labels() {
        let node = Node::new(
            NodeId(1),
            LabelSet::from_strs(&["sensor", "temperature"]),
            PropertyMap::new(),
        );
        assert_eq!(node.id, NodeId(1));
        assert!(node.has_label("sensor"));
        assert!(node.has_label("temperature"));
        assert!(!node.has_label("actuator"));
        assert_eq!(node.version, 1);
    }

    #[test]
    fn node_property_access() {
        let props = PropertyMap::from_pairs(vec![
            (IStr::new("unit"), Value::str("°F")),
            (IStr::new("value"), Value::Float(72.5)),
        ]);
        let node = Node::new(NodeId(1), LabelSet::new(), props);
        assert_eq!(node.property("unit"), Some(&Value::str("°F")));
        assert_eq!(node.property("value"), Some(&Value::Float(72.5)));
        assert_eq!(node.property("missing"), None);
    }

    #[test]
    fn edge_creation() {
        let edge = Edge::new(
            EdgeId(1),
            NodeId(10),
            NodeId(20),
            IStr::new("contains"),
            PropertyMap::new(),
        );
        assert_eq!(edge.source, NodeId(10));
        assert_eq!(edge.target, NodeId(20));
        assert_eq!(edge.label.as_str(), "contains");
    }

    #[test]
    fn node_id_display() {
        assert_eq!(format!("{}", NodeId(42)), "node:42");
    }

    #[test]
    fn edge_id_display() {
        assert_eq!(format!("{}", EdgeId(7)), "edge:7");
    }
}
