//! Integration tests — realistic building graph scenarios.

use std::collections::HashMap;
use std::sync::Arc;

use selene_core::schema::{NodeSchema, PropertyDef, ValidationMode, ValueType};
use selene_core::{IStr, LabelSet, NodeId, PropertyMap, Value};
use selene_graph::algorithms::{bfs, containment_children, containment_walk_up};
use selene_graph::snapshot::{slice_containment, slice_full};
use selene_graph::{GraphError, SchemaValidator, SeleneGraph, SharedGraph};

fn labels(names: &[&str]) -> LabelSet {
    LabelSet::from_strs(names)
}

fn named(name: &str) -> PropertyMap {
    PropertyMap::from_pairs(vec![(IStr::new("name"), Value::str(name))])
}

/// Build a realistic 6-level containment hierarchy:
///   site(1) → building(2) → floor(3) → zone(5) → equip(7) → point(9)
///                           → floor(4) → zone(6) → equip(8) → point(10)
///   Plus "feeds" edges between some equipment.
fn build_building_graph() -> SeleneGraph {
    let mut g = SeleneGraph::new();

    let mut m = g.mutate();

    // Level 1: site
    let site = m
        .create_node(labels(&["site"]), named("Campus HQ"))
        .unwrap();

    // Level 2: building
    let bldg = m
        .create_node(
            labels(&["building"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::str("Building A")),
                (IStr::new("floors"), Value::Int(2)),
            ]),
        )
        .unwrap();

    // Level 3: floors
    let floor1 = m
        .create_node(
            labels(&["floor"]),
            PropertyMap::from_pairs(vec![(IStr::new("number"), Value::Int(1))]),
        )
        .unwrap();
    let floor2 = m
        .create_node(
            labels(&["floor"]),
            PropertyMap::from_pairs(vec![(IStr::new("number"), Value::Int(2))]),
        )
        .unwrap();

    // Level 4: zones
    let zone1 = m
        .create_node(labels(&["zone"]), PropertyMap::new())
        .unwrap();
    let zone2 = m
        .create_node(labels(&["zone"]), PropertyMap::new())
        .unwrap();

    // Level 5: equipment
    let ahu1 = m
        .create_node(
            labels(&["equipment", "ahu"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::str("AHU-1")),
                (IStr::new("capacity_kw"), Value::Float(100.0)),
            ]),
        )
        .unwrap();
    let ahu2 = m
        .create_node(
            labels(&["equipment", "ahu"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::str("AHU-2")),
                (IStr::new("capacity_kw"), Value::Float(150.0)),
            ]),
        )
        .unwrap();

    // Level 6: points (sensors)
    let temp1 = m
        .create_node(
            labels(&["point", "sensor", "temperature_sensor"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("unit"), Value::str("°F")),
                (IStr::new("current_value"), Value::Float(72.5)),
            ]),
        )
        .unwrap();
    let temp2 = m
        .create_node(
            labels(&["point", "sensor", "temperature_sensor"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("unit"), Value::str("°F")),
                (IStr::new("current_value"), Value::Float(68.3)),
            ]),
        )
        .unwrap();

    // Containment edges
    let c = IStr::new("contains");
    m.create_edge(site, c, bldg, PropertyMap::new()).unwrap();
    m.create_edge(bldg, c, floor1, PropertyMap::new()).unwrap();
    m.create_edge(bldg, c, floor2, PropertyMap::new()).unwrap();
    m.create_edge(floor1, c, zone1, PropertyMap::new()).unwrap();
    m.create_edge(floor2, c, zone2, PropertyMap::new()).unwrap();
    m.create_edge(zone1, c, ahu1, PropertyMap::new()).unwrap();
    m.create_edge(zone2, c, ahu2, PropertyMap::new()).unwrap();
    m.create_edge(ahu1, c, temp1, PropertyMap::new()).unwrap();
    m.create_edge(ahu2, c, temp2, PropertyMap::new()).unwrap();

    // Feeds edges (AHU-1 feeds AHU-2)
    m.create_edge(
        ahu1,
        IStr::new("feeds"),
        ahu2,
        PropertyMap::from_pairs(vec![(IStr::new("medium"), Value::str("chilled_water"))]),
    )
    .unwrap();

    m.commit(0).unwrap();
    g
}

#[test]
fn containment_walk_from_sensor_to_site() {
    let g = build_building_graph();
    let path = containment_walk_up(&g, NodeId(9));
    assert_eq!(path.len(), 6);
    assert_eq!(path[0], NodeId(9));
    assert_eq!(path[5], NodeId(1));
}

#[test]
fn containment_children_from_site_gets_all() {
    let g = build_building_graph();
    let children = containment_children(&g, NodeId(1), None);
    assert_eq!(children.len(), 9);
}

#[test]
fn containment_children_depth_2() {
    let g = build_building_graph();
    let children = containment_children(&g, NodeId(1), Some(2));
    assert_eq!(children.len(), 3);
}

#[test]
fn bfs_feeds_traversal() {
    let g = build_building_graph();
    let fed = bfs(&g, NodeId(7), Some("feeds"), 5);
    assert_eq!(fed.len(), 1);
    assert_eq!(fed[0], NodeId(8));
}

#[test]
fn full_graph_slice_export() {
    let g = build_building_graph();
    let s = slice_full(&g);
    assert_eq!(s.nodes.len(), 10);
    assert_eq!(s.edges.len(), 10);
}

#[test]
fn containment_slice_for_floor() {
    let g = build_building_graph();
    let s = slice_containment(&g, NodeId(3), None);
    assert_eq!(s.nodes.len(), 4);
}

#[test]
fn shared_graph_concurrent_access() {
    let g = build_building_graph();
    let shared = SharedGraph::new(g);

    shared
        .write(|m| {
            m.set_property(NodeId(9), IStr::new("current_value"), Value::Float(75.0))?;
            Ok(())
        })
        .unwrap();

    let val = shared.read(|g| {
        g.get_node(NodeId(9))
            .and_then(|n| n.property("current_value").cloned())
    });
    assert_eq!(val, Some(Value::Float(75.0)));
}

#[test]
fn shared_graph_write_error_rolls_back() {
    let g = build_building_graph();
    let shared = SharedGraph::new(g);
    let before = shared.read(selene_graph::SeleneGraph::node_count);

    let result = shared.write(|m| {
        m.create_node(labels(&["new_node"]), PropertyMap::new())?;
        m.delete_node(NodeId(999))?;
        Ok(())
    });

    assert!(result.is_err());
    let after = shared.read(selene_graph::SeleneGraph::node_count);
    assert_eq!(before, after);
}

#[test]
fn schema_validation_with_brick_style_schemas() {
    let mut validator = SchemaValidator::new(ValidationMode::Strict);
    validator
        .register_node_schema(NodeSchema {
            label: Arc::from("temperature_sensor"),
            parent: None,
            properties: vec![PropertyDef::simple("unit", ValueType::String, true)],
            valid_edge_labels: vec![],
            description: "Temperature sensor".into(),
            annotations: HashMap::from([(Arc::from("brick"), Value::str("Temperature_Sensor"))]),
            version: Default::default(),
            validation_mode: None,
            key_properties: vec![],
        })
        .unwrap();

    let mut g = SeleneGraph::with_config(validator, 1000);

    // Valid sensor — should succeed
    {
        let mut m = g.mutate();
        m.create_node(
            labels(&["temperature_sensor"]),
            PropertyMap::from_pairs(vec![(IStr::new("unit"), Value::str("°F"))]),
        )
        .unwrap();
        m.commit(0).unwrap();
    }

    // Invalid sensor (missing required "unit") — should fail in strict mode
    {
        let mut m = g.mutate();
        m.create_node(labels(&["temperature_sensor"]), PropertyMap::new())
            .unwrap();
        let result = m.commit(0);
        assert!(matches!(result, Err(GraphError::SchemaViolation(_))));
    }

    assert_eq!(g.node_count(), 1);
}

#[test]
fn changelog_captures_all_mutations() {
    let mut g = SeleneGraph::new();

    {
        let mut m = g.mutate();
        m.create_node(labels(&["sensor"]), PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();
    }

    {
        let mut m = g.mutate();
        m.set_property(NodeId(1), IStr::new("value"), Value::Float(72.0))
            .unwrap();
        m.commit(0).unwrap();
    }

    let entries = g.changelog().since(0).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].sequence, 1);
    assert_eq!(entries[1].sequence, 2);
}

#[tokio::test]
async fn concurrent_readers_and_writer() {
    let g = build_building_graph();
    let shared = SharedGraph::new(g);

    let mut handles = vec![];
    for _ in 0..10 {
        let s = shared.clone();
        handles.push(tokio::spawn(async move {
            s.read(selene_graph::SeleneGraph::node_count)
        }));
    }

    for h in handles {
        assert_eq!(h.await.unwrap(), 10);
    }

    shared
        .write(|m| {
            m.create_node(labels(&["new"]), PropertyMap::new())?;
            Ok(())
        })
        .unwrap();

    assert_eq!(shared.read(selene_graph::SeleneGraph::node_count), 11);
}

#[test]
fn index_consistency_after_complex_operations() {
    let mut g = SeleneGraph::new();

    {
        let mut m = g.mutate();
        let n1 = m
            .create_node(labels(&["a", "b"]), PropertyMap::new())
            .unwrap();
        let n2 = m
            .create_node(labels(&["b", "c"]), PropertyMap::new())
            .unwrap();
        let n3 = m.create_node(labels(&["a"]), PropertyMap::new()).unwrap();
        m.create_edge(n1, IStr::new("link"), n2, PropertyMap::new())
            .unwrap();
        m.create_edge(n2, IStr::new("link"), n3, PropertyMap::new())
            .unwrap();
        m.create_edge(n1, IStr::new("other"), n3, PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();
    }

    {
        let mut m = g.mutate();
        m.delete_node(NodeId(2)).unwrap();
        m.commit(0).unwrap();
    }

    g.assert_indexes_consistent();
    assert_eq!(g.node_count(), 2);
    assert_eq!(g.edge_count(), 1);
}
