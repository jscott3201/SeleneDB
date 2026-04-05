//! SOSA observation materialization.
//!
//! Maintains one `sosa:Observation` node per sensor-property pair containing
//! the latest time-series reading. Each Observation node carries:
//!
//! - `observedProperty` -- the property name (string)
//! - `simpleResult`     -- the latest reading (float)
//! - `resultTime`       -- the reading timestamp in nanoseconds (Timestamp)
//!
//! The node is linked to its sensor via a `madeBySensor` edge:
//!
//! ```text
//! (Observation) --madeBySensor--> (Sensor)
//! ```
//!
//! When `materialize_observations = true` in the server's RDF config, a background
//! task calls [`upsert_observation`] on each TS write. This makes the current
//! sensor state visible to SPARQL queries without materializing the full TS
//! history.

use selene_core::NodeId;
use selene_core::interner::IStr;
use selene_core::label_set::LabelSet;
use selene_core::property_map::PropertyMap;
use selene_core::value::Value;
use selene_graph::{GraphError, SharedGraph};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const OBSERVATION_LABEL: &str = "Observation";
const PROP_OBSERVED_PROPERTY: &str = "observedProperty";
const PROP_SIMPLE_RESULT: &str = "simpleResult";
const PROP_RESULT_TIME: &str = "resultTime";
const EDGE_MADE_BY_SENSOR: &str = "madeBySensor";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Create or update the `sosa:Observation` node for a (sensor, property) pair.
///
/// **Algorithm:**
/// 1. Load a read-only snapshot of the graph.
/// 2. Scan all nodes with label `"Observation"` to find the existing node for
///    this `(sensor_node_id, property)` pair. An Observation matches if it has
///    an outgoing `madeBySensor` edge to `sensor_node_id` and its
///    `observedProperty` property equals `property`.
/// 3. If a match is found: update `simpleResult` and `resultTime` in a write
///    transaction.
/// 4. If no match is found: create a new `Observation` node with the full
///    property set and a `madeBySensor` edge to the sensor.
///
/// **Scale note:** The linear scan over Observation nodes is acceptable for
/// building-scale deployments (hundreds of sensors x 2-3 properties = ~1000
/// observations). An index can be added later if this becomes a bottleneck.
pub fn upsert_observation(
    shared: &SharedGraph,
    sensor_node_id: NodeId,
    property: &str,
    value: f64,
    timestamp_nanos: i64,
) -> Result<(), GraphError> {
    // Find-and-update inside a single write lock to prevent a race where two
    // concurrent callers both see "no existing observation" and create duplicates.
    shared.write(|m| {
        let existing_obs_id = find_observation(m.graph(), sensor_node_id, property);

        if let Some(obs_id) = existing_obs_id {
            // Update existing Observation.
            m.set_property(obs_id, IStr::new(PROP_SIMPLE_RESULT), Value::Float(value))?;
            m.set_property(
                obs_id,
                IStr::new(PROP_RESULT_TIME),
                Value::Timestamp(timestamp_nanos),
            )?;
        } else {
            // Create a new Observation node + madeBySensor edge.
            let labels = LabelSet::from_strs(&[OBSERVATION_LABEL]);
            let props = PropertyMap::from_pairs(vec![
                (IStr::new(PROP_OBSERVED_PROPERTY), Value::str(property)),
                (IStr::new(PROP_SIMPLE_RESULT), Value::Float(value)),
                (
                    IStr::new(PROP_RESULT_TIME),
                    Value::Timestamp(timestamp_nanos),
                ),
            ]);
            let obs_id = m.create_node(labels, props)?;
            m.create_edge(
                obs_id,
                IStr::new(EDGE_MADE_BY_SENSOR),
                sensor_node_id,
                PropertyMap::new(),
            )?;
        }
        Ok(())
    })?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Search for an existing Observation node for the given (sensor, property) pair.
///
/// Returns `Some(NodeId)` if found, `None` otherwise.
///
/// Scans all Observation-labelled nodes and checks:
/// - Has an outgoing `madeBySensor` edge whose target is `sensor_node_id`.
/// - Has `observedProperty` equal to `property`.
fn find_observation(
    graph: &selene_graph::SeleneGraph,
    sensor_node_id: NodeId,
    property: &str,
) -> Option<NodeId> {
    for obs_id in graph.nodes_by_label(OBSERVATION_LABEL) {
        // Check observedProperty first -- cheap property lookup before edge scan.
        let Some(node) = graph.get_node(obs_id) else {
            continue;
        };
        let obs_prop = node.properties.get(IStr::new(PROP_OBSERVED_PROPERTY));
        let matches_property = match obs_prop {
            Some(Value::String(s)) => s.as_str() == property,
            Some(Value::InternedStr(s)) => s.as_str() == property,
            _ => false,
        };
        if !matches_property {
            continue;
        }

        // Check outgoing edges for a madeBySensor edge to this sensor.
        for &edge_id in graph.outgoing(obs_id) {
            if let Some(edge) = graph.get_edge(edge_id)
                && edge.label.as_str() == EDGE_MADE_BY_SENSOR
                && edge.target == sensor_node_id
            {
                return Some(obs_id);
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use selene_graph::{SeleneGraph, SharedGraph};

    fn make_sensor(shared: &SharedGraph) -> NodeId {
        let (id, _) = shared
            .write(|m| {
                m.create_node(
                    LabelSet::from_strs(&["Sensor"]),
                    PropertyMap::from_pairs(vec![(IStr::new("unit"), Value::str("degC"))]),
                )
            })
            .unwrap();
        id
    }

    #[test]
    fn creates_new_observation() {
        let shared = SharedGraph::new(SeleneGraph::new());
        let sensor_id = make_sensor(&shared);

        upsert_observation(&shared, sensor_id, "temperature", 21.5, 1_000_000_000)
            .expect("upsert should succeed");

        // Verify one Observation node was created.
        let obs_count = shared.read(|g| g.nodes_by_label(OBSERVATION_LABEL).count());
        assert_eq!(obs_count, 1, "expected 1 Observation node");

        // Verify properties on the Observation node.
        shared.read(|g| {
            let obs_id = g.nodes_by_label(OBSERVATION_LABEL).next().unwrap();
            let node = g.get_node(obs_id).unwrap();
            assert_eq!(
                node.properties.get(IStr::new(PROP_OBSERVED_PROPERTY)),
                Some(&Value::str("temperature")),
            );
            assert_eq!(
                node.properties.get(IStr::new(PROP_SIMPLE_RESULT)),
                Some(&Value::Float(21.5)),
            );
            assert_eq!(
                node.properties.get(IStr::new(PROP_RESULT_TIME)),
                Some(&Value::Timestamp(1_000_000_000)),
            );
        });
    }

    #[test]
    fn updates_existing_observation() {
        let shared = SharedGraph::new(SeleneGraph::new());
        let sensor_id = make_sensor(&shared);

        // First upsert creates the node.
        upsert_observation(&shared, sensor_id, "temperature", 21.5, 1_000_000_000).unwrap();
        // Second upsert updates it.
        upsert_observation(&shared, sensor_id, "temperature", 22.0, 2_000_000_000).unwrap();

        // Still only one Observation node.
        let obs_count = shared.read(|g| g.nodes_by_label(OBSERVATION_LABEL).count());
        assert_eq!(
            obs_count, 1,
            "expected exactly 1 Observation node after update"
        );

        // Updated values.
        shared.read(|g| {
            let obs_id = g.nodes_by_label(OBSERVATION_LABEL).next().unwrap();
            let node = g.get_node(obs_id).unwrap();
            assert_eq!(
                node.properties.get(IStr::new(PROP_SIMPLE_RESULT)),
                Some(&Value::Float(22.0)),
            );
            assert_eq!(
                node.properties.get(IStr::new(PROP_RESULT_TIME)),
                Some(&Value::Timestamp(2_000_000_000)),
            );
        });
    }

    #[test]
    fn separate_properties_get_separate_observations() {
        let shared = SharedGraph::new(SeleneGraph::new());
        let sensor_id = make_sensor(&shared);

        upsert_observation(&shared, sensor_id, "temperature", 21.5, 1_000_000_000).unwrap();
        upsert_observation(&shared, sensor_id, "humidity", 55.0, 1_000_000_001).unwrap();

        let obs_count = shared.read(|g| g.nodes_by_label(OBSERVATION_LABEL).count());
        assert_eq!(
            obs_count, 2,
            "expected 2 Observation nodes for 2 properties"
        );
    }

    #[test]
    fn separate_sensors_get_separate_observations() {
        let shared = SharedGraph::new(SeleneGraph::new());
        let sensor_a = make_sensor(&shared);
        let sensor_b = make_sensor(&shared);

        upsert_observation(&shared, sensor_a, "temperature", 21.5, 1_000_000_000).unwrap();
        upsert_observation(&shared, sensor_b, "temperature", 19.0, 1_000_000_001).unwrap();

        let obs_count = shared.read(|g| g.nodes_by_label(OBSERVATION_LABEL).count());
        assert_eq!(obs_count, 2, "expected 2 Observation nodes for 2 sensors");
    }

    #[test]
    fn made_by_sensor_edge_is_created() {
        let shared = SharedGraph::new(SeleneGraph::new());
        let sensor_id = make_sensor(&shared);

        upsert_observation(&shared, sensor_id, "co2", 450.0, 1_000_000_000).unwrap();

        shared.read(|g| {
            let obs_id = g.nodes_by_label(OBSERVATION_LABEL).next().unwrap();
            let has_edge = g.outgoing(obs_id).iter().any(|&eid| {
                g.get_edge(eid).is_some_and(|e| {
                    e.label.as_str() == EDGE_MADE_BY_SENSOR && e.target == sensor_id
                })
            });
            assert!(
                has_edge,
                "expected madeBySensor edge from Observation to Sensor"
            );
        });
    }
}
