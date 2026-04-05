//! Factory functions for creating test nodes.

use selene_core::{IStr, LabelSet, Node, NodeId, PropertyMap, Value};
use smol_str::SmolStr;

/// Create a minimal test node with the given ID and labels.
pub fn test_node(id: u64, labels: &[&str]) -> Node {
    Node::new(NodeId(id), LabelSet::from_strs(labels), PropertyMap::new())
}

/// Create a test node with properties.
pub fn test_node_with_props(id: u64, labels: &[&str], props: &[(&str, Value)]) -> Node {
    let prop_map = PropertyMap::from_pairs(props.iter().map(|(k, v)| (IStr::new(k), v.clone())));
    Node::new(NodeId(id), LabelSet::from_strs(labels), prop_map)
}

/// Create a batch of test sensor nodes.
pub fn test_sensors(count: u64, label: &str) -> Vec<Node> {
    (1..=count)
        .map(|i| {
            test_node_with_props(
                i,
                &[label, "sensor"],
                &[
                    (
                        "display_name",
                        Value::String(SmolStr::new(format!("{label}-{i}"))),
                    ),
                    ("current_value", Value::Float(20.0 + (i as f64) * 0.1)),
                ],
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_with_labels() {
        let node = test_node(1, &["sensor", "temperature"]);
        assert_eq!(node.id, NodeId(1));
        assert!(node.has_label("sensor"));
        assert!(node.has_label("temperature"));
        assert!(!node.has_label("actuator"));
        assert!(node.properties.is_empty());
    }

    #[test]
    fn test_node_with_properties() {
        let node = test_node_with_props(
            1,
            &["ahu"],
            &[
                ("name", Value::str("AHU-1")),
                ("capacity_kw", Value::Float(500.0)),
            ],
        );
        assert_eq!(node.property("name"), Some(&Value::str("AHU-1")));
    }

    #[test]
    fn test_batch_sensors() {
        let sensors = test_sensors(100, "temperature_sensor");
        assert_eq!(sensors.len(), 100);
        assert!(sensors[0].has_label("temperature_sensor"));
        assert!(sensors[0].has_label("sensor"));
    }
}
