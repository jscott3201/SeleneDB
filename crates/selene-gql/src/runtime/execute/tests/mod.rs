use super::*;
use selene_core::{LabelSet, NodeId, PropertyMap, Value};
use smol_str::SmolStr;

mod advanced;
mod clauses;
mod ddl;
mod mutations;
mod predicates;
mod queries;
mod subqueries;

fn setup_graph() -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();

    // Building hierarchy: building -> floor -> 2 sensors
    m.create_node(
        LabelSet::from_strs(&["building"]),
        PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("HQ")))]),
    )
    .unwrap(); // Node 1

    m.create_node(
        LabelSet::from_strs(&["floor"]),
        PropertyMap::from_pairs(vec![(
            IStr::new("name"),
            Value::String(SmolStr::new("Floor-1")),
        )]),
    )
    .unwrap(); // Node 2

    m.create_node(
        LabelSet::from_strs(&["sensor"]),
        PropertyMap::from_pairs(vec![
            (
                IStr::new("name"),
                Value::String(SmolStr::new("TempSensor-1")),
            ),
            (IStr::new("temp"), Value::Float(72.5)),
        ]),
    )
    .unwrap(); // Node 3

    m.create_node(
        LabelSet::from_strs(&["sensor"]),
        PropertyMap::from_pairs(vec![
            (
                IStr::new("name"),
                Value::String(SmolStr::new("TempSensor-2")),
            ),
            (IStr::new("temp"), Value::Float(80.0)),
        ]),
    )
    .unwrap(); // Node 4

    // building -contains-> floor
    m.create_edge(
        NodeId(1),
        IStr::new("contains"),
        NodeId(2),
        PropertyMap::new(),
    )
    .unwrap();
    // floor -contains-> sensor1
    m.create_edge(
        NodeId(2),
        IStr::new("contains"),
        NodeId(3),
        PropertyMap::new(),
    )
    .unwrap();
    // floor -contains-> sensor2
    m.create_edge(
        NodeId(2),
        IStr::new("contains"),
        NodeId(4),
        PropertyMap::new(),
    )
    .unwrap();

    m.commit(0).unwrap();
    g
}
