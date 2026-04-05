//! Reference building model: scalable deterministic test data.
//!
//! `reference_building(scale)` generates a building campus graph with:
//! - **Containment hierarchy:** campus → buildings → floors → zones → equipment → sensors
//! - **HVAC overlay:** AHUs, VAVs, feeds/serves/returns_to edges (cycles for SCC)
//! - **Electrical overlay:** panels, circuits, breakers, powers edges
//! - **Monitoring overlay:** BMS servers, monitors edges (cross-hierarchy)
//! - **Spatial adjacency:** adjacent_to edges between zones on the same floor
//! - **Alarm network:** alarm panels with alarm_for edges to equipment
//!
//! Scale controls the number of buildings (and thus floors, zones, etc.).
//! Scale=1 gives the base model (~200 nodes), scale=5 ~1000, scale=10 ~2000.
//! IDs are deterministic: same scale always produces the same graph.

use std::sync::Arc;

use selene_core::schema::{NodeSchema, PropertyDef, ValueType};
use selene_core::{IStr, LabelSet, NodeId, PropertyMap, Value};
use selene_graph::SeleneGraph;

/// Build the reference building model at scale=1.
///
/// Returns the standard ~38-node graph used by tests and benchmarks.
pub fn build() -> SeleneGraph {
    reference_building(1)
}

/// Sequential ID allocator for deterministic node ID assignment.
struct IdAlloc {
    next: u64,
}

impl IdAlloc {
    fn new() -> Self {
        Self { next: 1 }
    }
    fn alloc(&mut self) -> u64 {
        let id = self.next;
        self.next += 1;
        id
    }
}

/// Build a scalable reference building graph.
///
/// - `scale=1`: 1 main building + 1 parking garage (~200 nodes, ~350 edges)
/// - `scale=5`: 5 buildings + 1 parking garage (~1000 nodes)
/// - `scale=10`: 10 buildings + 1 parking garage (~2000 nodes)
///
/// Each building has 3 floors × 2 zones × (1 VAV + 3 sensors) = 36 leaf nodes,
/// plus AHUs, electrical panels, BMS server, alarm panel.
///
/// Five distinct graph overlays are generated:
/// 1. **Containment** (contains edges) - spatial hierarchy
/// 2. **HVAC** (feeds, serves, returns_to) - air handling cycles
/// 3. **Electrical** (powers) - panel/circuit/breaker tree
/// 4. **Monitoring** (monitors, isPointOf) - cross-hierarchy
/// 5. **Spatial** (adjacent_to) - zone adjacency on same floor
/// 6. **Alarm** (alarm_for) - alarm panels covering equipment
pub fn reference_building(scale: usize) -> SeleneGraph {
    let scale = scale.max(1);
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    let mut ids = IdAlloc::new();

    let s = |v: &str| Value::str(v);
    let f = |v: f64| Value::Float(v);
    let i = |v: i64| Value::Int(v);

    let labels = |names: &[&str]| LabelSet::from_strs(names);
    let props = |pairs: &[(&str, Value)]| {
        PropertyMap::from_pairs(pairs.iter().map(|(k, v)| (IStr::new(k), v.clone())))
    };

    let c = IStr::new("contains");
    let feeds = IStr::new("feeds");
    let serves = IStr::new("serves");
    let monitors = IStr::new("monitors");
    let returns_to = IStr::new("returns_to");
    let is_point_of = IStr::new("isPointOf");
    let adjacent_to = IStr::new("adjacent_to");
    let powers = IStr::new("powers");
    let alarm_for = IStr::new("alarm_for");

    let campus_id = ids.alloc();
    m.create_node(
        labels(&["campus"]),
        props(&[
            ("name", s("Northgate Campus")),
            ("city", s("Austin")),
            ("area_sqft", f(250_000.0 * scale as f64)),
        ]),
    )
    .unwrap();

    let num_buildings = scale;
    for bldg_idx in 0..num_buildings {
        let bldg_id = ids.alloc();
        let bldg_name = if bldg_idx == 0 {
            "Building North".to_string()
        } else {
            format!("Building-{}", bldg_idx + 1)
        };
        m.create_node(
            labels(&["building"]),
            props(&[
                ("name", s(&bldg_name)),
                ("floors", i(3)),
                ("year_built", i(2018 + bldg_idx as i64)),
            ]),
        )
        .unwrap();
        m.create_edge(NodeId(campus_id), c, NodeId(bldg_id), PropertyMap::new())
            .unwrap();

        let mut floor_ids = Vec::new();
        for floor_num in 1..=3i64 {
            let floor_id = ids.alloc();
            floor_ids.push(floor_id);
            m.create_node(
                labels(&["floor"]),
                props(&[
                    ("name", s(&format!("{bldg_name} F{floor_num}"))),
                    ("level", i(floor_num)),
                ]),
            )
            .unwrap();
            m.create_edge(NodeId(bldg_id), c, NodeId(floor_id), PropertyMap::new())
                .unwrap();
        }

        let mut zone_ids = Vec::new();
        let mut zone_counter: usize = 0;
        for (fi, &floor_id) in floor_ids.iter().enumerate() {
            let floor_num = fi + 1;
            let mut floor_zone_ids = Vec::new();
            for zone_letter in ['A', 'B'] {
                let zone_id = ids.alloc();
                zone_ids.push(zone_id);
                floor_zone_ids.push(zone_id);
                let zone_type_val = if zone_counter.is_multiple_of(2) {
                    Value::InternedStr(IStr::new("office"))
                } else {
                    Value::InternedStr(IStr::new("conference"))
                };
                m.create_node(
                    labels(&["zone"]),
                    props(&[
                        (
                            "name",
                            s(&format!(
                                "B{}-F{floor_num}-Zone-{zone_letter}",
                                bldg_idx + 1
                            )),
                        ),
                        ("zone_type", zone_type_val),
                        ("area_sqft", f(2500.0)),
                        ("occupied", Value::Bool(false)),
                    ]),
                )
                .unwrap();
                zone_counter += 1;
                m.create_edge(NodeId(floor_id), c, NodeId(zone_id), PropertyMap::new())
                    .unwrap();
            }
            if floor_zone_ids.len() == 2 {
                m.create_edge(
                    NodeId(floor_zone_ids[0]),
                    adjacent_to,
                    NodeId(floor_zone_ids[1]),
                    props(&[("distance_ft", f(50.0))]),
                )
                .unwrap();
                m.create_edge(
                    NodeId(floor_zone_ids[1]),
                    adjacent_to,
                    NodeId(floor_zone_ids[0]),
                    props(&[("distance_ft", f(50.0))]),
                )
                .unwrap();
            }
        }

        let mut equip_counter: u64 = 0;
        let ahu1_id = ids.alloc();
        m.create_node(
            labels(&["ahu", "equipment"]),
            props(&[
                ("name", s(&format!("B{}-AHU-1", bldg_idx + 1))),
                ("supply_cfm", f(15000.0)),
                ("model", s("Carrier 39M")),
                ("enabled", Value::Bool(true)),
                (
                    "commissioned_at",
                    Value::Timestamp(1_704_067_200_000_000_000),
                ),
                (
                    "maintenance_interval",
                    Value::Duration(7_776_000_000_000_000),
                ),
                (
                    "serial_number",
                    Value::UInt(1_000_000 + bldg_idx as u64 * 100 + equip_counter),
                ),
            ]),
        )
        .unwrap();
        equip_counter += 1;
        m.create_edge(NodeId(bldg_id), c, NodeId(ahu1_id), PropertyMap::new())
            .unwrap();

        let ahu2_id = ids.alloc();
        m.create_node(
            labels(&["ahu", "equipment"]),
            props(&[
                ("name", s(&format!("B{}-AHU-2", bldg_idx + 1))),
                ("supply_cfm", f(10000.0)),
                ("model", s("Trane IntelliPak")),
                ("enabled", Value::Bool(true)),
                (
                    "commissioned_at",
                    Value::Timestamp(1_704_067_200_000_000_000),
                ),
                (
                    "maintenance_interval",
                    Value::Duration(7_776_000_000_000_000),
                ),
                (
                    "serial_number",
                    Value::UInt(1_000_000 + bldg_idx as u64 * 100 + equip_counter),
                ),
            ]),
        )
        .unwrap();
        equip_counter += 1;
        m.create_edge(NodeId(bldg_id), c, NodeId(ahu2_id), PropertyMap::new())
            .unwrap();

        let mut vav_ids = Vec::new();
        for (zi, &zone_id) in zone_ids.iter().enumerate() {
            let vav_id = ids.alloc();
            vav_ids.push(vav_id);
            m.create_node(
                labels(&["vav", "equipment"]),
                props(&[
                    ("name", s(&format!("B{}-VAV-{}", bldg_idx + 1, zi + 1))),
                    ("max_cfm", f(2000.0)),
                    ("enabled", Value::Bool(true)),
                    (
                        "serial_number",
                        Value::UInt(1_000_000 + bldg_idx as u64 * 100 + equip_counter),
                    ),
                ]),
            )
            .unwrap();
            equip_counter += 1;
            m.create_edge(NodeId(zone_id), c, NodeId(vav_id), PropertyMap::new())
                .unwrap();

            m.create_edge(NodeId(vav_id), serves, NodeId(zone_id), PropertyMap::new())
                .unwrap();
        }

        for &vav_id in &vav_ids[..4.min(vav_ids.len())] {
            m.create_edge(
                NodeId(ahu1_id),
                feeds,
                NodeId(vav_id),
                props(&[
                    ("capacity_pct", f(100.0)),
                    ("pipe_length_ft", f(25.0 + (vav_id % 10) as f64 * 5.0)),
                ]),
            )
            .unwrap();
        }
        for &vav_id in &vav_ids[4..] {
            m.create_edge(
                NodeId(ahu2_id),
                feeds,
                NodeId(vav_id),
                props(&[
                    ("capacity_pct", f(100.0)),
                    ("pipe_length_ft", f(30.0 + (vav_id % 10) as f64 * 5.0)),
                ]),
            )
            .unwrap();
        }

        for (zi, &zone_id) in zone_ids.iter().enumerate() {
            let ahu_id = if zi < 4 { ahu1_id } else { ahu2_id };
            m.create_edge(
                NodeId(zone_id),
                returns_to,
                NodeId(ahu_id),
                PropertyMap::new(),
            )
            .unwrap();
        }

        let mut sensor_ids = Vec::new();
        for (zi, &zone_id) in zone_ids.iter().enumerate() {
            let temp_id = ids.alloc();
            sensor_ids.push(temp_id);
            m.create_node(
                labels(&["sensor", "temperature_sensor"]),
                props(&[
                    ("name", s(&format!("B{}-Temp-{}", bldg_idx + 1, zi + 1))),
                    ("unit", s("°F")),
                    ("accuracy", f(0.5)),
                    (
                        "last_calibrated",
                        Value::Timestamp(1_719_792_000_000_000_000),
                    ),
                    ("install_date", Value::Date(19905)),
                ]),
            )
            .unwrap();
            m.create_edge(NodeId(zone_id), c, NodeId(temp_id), PropertyMap::new())
                .unwrap();

            let hum_id = ids.alloc();
            sensor_ids.push(hum_id);
            let mut hum_props = vec![
                ("name", s(&format!("B{}-Hum-{}", bldg_idx + 1, zi + 1))),
                ("unit", s("%RH")),
                (
                    "last_calibrated",
                    Value::Timestamp(1_719_792_000_000_000_000),
                ),
                ("install_date", Value::Date(19905)),
            ];
            if zi % 3 != 0 {
                hum_props.push(("calibration_date", Value::Date(20254)));
            }
            m.create_node(labels(&["sensor", "humidity_sensor"]), props(&hum_props))
                .unwrap();
            m.create_edge(NodeId(zone_id), c, NodeId(hum_id), PropertyMap::new())
                .unwrap();

            let co2_id = ids.alloc();
            sensor_ids.push(co2_id);
            m.create_node(
                labels(&["sensor", "co2_sensor"]),
                props(&[
                    ("name", s(&format!("B{}-CO2-{}", bldg_idx + 1, zi + 1))),
                    ("unit", s("ppm")),
                    (
                        "last_calibrated",
                        Value::Timestamp(1_719_792_000_000_000_000),
                    ),
                    ("install_date", Value::Date(19905)),
                ]),
            )
            .unwrap();
            m.create_edge(NodeId(zone_id), c, NodeId(co2_id), PropertyMap::new())
                .unwrap();

            let vav_id = vav_ids[zi];
            m.create_edge(
                NodeId(temp_id),
                is_point_of,
                NodeId(vav_id),
                PropertyMap::new(),
            )
            .unwrap();
            m.create_edge(
                NodeId(hum_id),
                is_point_of,
                NodeId(vav_id),
                PropertyMap::new(),
            )
            .unwrap();
            m.create_edge(
                NodeId(co2_id),
                is_point_of,
                NodeId(vav_id),
                PropertyMap::new(),
            )
            .unwrap();
        }

        let bms_id = ids.alloc();
        m.create_node(
            labels(&["server", "equipment"]),
            props(&[
                ("name", s(&format!("B{}-BMS-Server", bldg_idx + 1))),
                ("ip", s(&format!("10.0.{}.100", bldg_idx + 1))),
                ("protocol", s("BACnet")),
                (
                    "firmware_hash",
                    Value::Bytes(Arc::from(
                        [0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04].as_slice(),
                    )),
                ),
                (
                    "protocols",
                    Value::List(Arc::from([
                        Value::String("BACnet".into()),
                        Value::String("Modbus".into()),
                        Value::String("MQTT".into()),
                    ])),
                ),
            ]),
        )
        .unwrap();
        m.create_edge(NodeId(bldg_id), c, NodeId(bms_id), PropertyMap::new())
            .unwrap();
        m.create_edge(
            NodeId(bms_id),
            monitors,
            NodeId(ahu1_id),
            PropertyMap::new(),
        )
        .unwrap();
        m.create_edge(
            NodeId(bms_id),
            monitors,
            NodeId(ahu2_id),
            PropertyMap::new(),
        )
        .unwrap();

        let panel_id = ids.alloc();
        m.create_node(
            labels(&["electrical_panel", "equipment"]),
            props(&[
                ("name", s(&format!("B{}-Panel-Main", bldg_idx + 1))),
                ("voltage", i(480)),
                ("phase", i(3)),
            ]),
        )
        .unwrap();
        m.create_edge(NodeId(bldg_id), c, NodeId(panel_id), PropertyMap::new())
            .unwrap();

        for (fi, &floor_id) in floor_ids.iter().enumerate() {
            let circuit_id = ids.alloc();
            m.create_node(
                labels(&["circuit", "equipment"]),
                props(&[
                    ("name", s(&format!("B{}-Circuit-F{}", bldg_idx + 1, fi + 1))),
                    ("amperage", i(100)),
                ]),
            )
            .unwrap();
            m.create_edge(NodeId(floor_id), c, NodeId(circuit_id), PropertyMap::new())
                .unwrap();
            m.create_edge(
                NodeId(panel_id),
                powers,
                NodeId(circuit_id),
                props(&[("load_pct", f(75.0))]),
            )
            .unwrap();

            let zone_start = fi * 2;
            for bi in 0..2 {
                let breaker_id = ids.alloc();
                m.create_node(
                    labels(&["breaker", "equipment"]),
                    props(&[
                        (
                            "name",
                            s(&format!("B{}-Breaker-F{}-{}", bldg_idx + 1, fi + 1, bi + 1)),
                        ),
                        ("amperage", i(20)),
                        ("status", s("closed")),
                    ]),
                )
                .unwrap();
                m.create_edge(
                    NodeId(circuit_id),
                    c,
                    NodeId(breaker_id),
                    PropertyMap::new(),
                )
                .unwrap();
                if zone_start + bi < vav_ids.len() {
                    m.create_edge(
                        NodeId(breaker_id),
                        powers,
                        NodeId(vav_ids[zone_start + bi]),
                        props(&[("load_pct", f(60.0))]),
                    )
                    .unwrap();
                }
            }
        }

        let alarm_id = ids.alloc();
        m.create_node(
            labels(&["alarm", "safety", "equipment"]),
            props(&[
                ("name", s(&format!("B{}-Fire-Alarm", bldg_idx + 1))),
                ("status", s("normal")),
                ("active_alarms", i(0)),
            ]),
        )
        .unwrap();
        m.create_edge(NodeId(bldg_id), c, NodeId(alarm_id), PropertyMap::new())
            .unwrap();

        m.create_edge(
            NodeId(alarm_id),
            alarm_for,
            NodeId(ahu1_id),
            PropertyMap::new(),
        )
        .unwrap();
        m.create_edge(
            NodeId(alarm_id),
            alarm_for,
            NodeId(ahu2_id),
            PropertyMap::new(),
        )
        .unwrap();
    }

    let parking_id = ids.alloc();
    m.create_node(
        labels(&["building", "parking"]),
        props(&[("name", s("Parking Garage")), ("capacity", i(200))]),
    )
    .unwrap();
    m.create_edge(NodeId(campus_id), c, NodeId(parking_id), PropertyMap::new())
        .unwrap();

    for level in 1..=2i64 {
        let level_id = ids.alloc();
        m.create_node(
            labels(&["floor"]),
            props(&[
                ("name", s(&format!("Parking Level {level}"))),
                ("level", i(-level)),
            ]),
        )
        .unwrap();
        m.create_edge(NodeId(parking_id), c, NodeId(level_id), PropertyMap::new())
            .unwrap();

        let occ_id = ids.alloc();
        m.create_node(
            labels(&["sensor", "occupancy_sensor"]),
            props(&[
                ("name", s(&format!("ParkingOccupancy-L{level}"))),
                ("unit", s("count")),
            ]),
        )
        .unwrap();
        m.create_edge(NodeId(level_id), c, NodeId(occ_id), PropertyMap::new())
            .unwrap();
    }

    m.commit(0).unwrap();
    register_schemas(&mut g);
    g
}

/// Register schema definitions for key node types in the reference building.
///
/// Schemas give benchmarks and tests a realistic validation surface. They are
/// registered after the initial commit so node data is already present (the
/// default validation mode is Warn, not Strict).
fn register_schemas(g: &mut SeleneGraph) {
    let sv = g.schema_mut();

    // Sensor schema: key property `name`, typed properties for units and calibration.
    sv.register_node_schema(
        NodeSchema::builder("sensor")
            .property(
                PropertyDef::builder("name", ValueType::String)
                    .required(true)
                    .build(),
            )
            .property(PropertyDef::simple("unit", ValueType::String, false))
            .property(PropertyDef::simple("accuracy", ValueType::Float, false))
            .property(PropertyDef::simple("install_date", ValueType::Date, false))
            .property(PropertyDef::simple(
                "last_calibrated",
                ValueType::ZonedDateTime,
                false,
            ))
            .key_property("name")
            .description("Point-type sensor (temperature, humidity, CO2, occupancy)")
            .build(),
    )
    .unwrap();

    // Zone schema: dictionary-encoded zone_type for memory-efficient enum strings.
    sv.register_node_schema(
        NodeSchema::builder("zone")
            .property(PropertyDef::simple("name", ValueType::String, true))
            .property(
                PropertyDef::builder("zone_type", ValueType::String)
                    .dictionary()
                    .build(),
            )
            .property(PropertyDef::simple("area_sqft", ValueType::Float, false))
            .property(PropertyDef::simple("occupied", ValueType::Bool, false))
            .description("Spatial zone within a floor")
            .build(),
    )
    .unwrap();

    // Equipment schema: shared base for AHU, VAV, panel, breaker, etc.
    sv.register_node_schema(
        NodeSchema::builder("equipment")
            .property(PropertyDef::simple("name", ValueType::String, true))
            .property(PropertyDef::simple("enabled", ValueType::Bool, false))
            .property(PropertyDef::simple("serial_number", ValueType::UInt, false))
            .description("Mechanical, electrical, or controls equipment")
            .build(),
    )
    .unwrap();

    // Building schema: top-level containment entity.
    sv.register_node_schema(
        NodeSchema::builder("building")
            .property(PropertyDef::simple("name", ValueType::String, true))
            .property(PropertyDef::simple("floors", ValueType::Int, false))
            .property(PropertyDef::simple("year_built", ValueType::Int, false))
            .description("Physical building structure")
            .build(),
    )
    .unwrap();
}

/// Validation query with expected result bounds.
pub struct ValidationQuery {
    pub name: &'static str,
    pub query: &'static str,
    pub min_rows: usize,
    pub max_rows: usize,
}

/// Phase 0 validation queries. Work against any scale >= 1.
pub fn phase0_queries() -> Vec<ValidationQuery> {
    vec![
        ValidationQuery {
            name: "count_all_nodes",
            query: "MATCH (n) RETURN count(*) AS total",
            min_rows: 1,
            max_rows: 1,
        },
        ValidationQuery {
            name: "count_sensors",
            query: "MATCH (s:sensor) RETURN count(*) AS total",
            min_rows: 1,
            max_rows: 1,
        },
        ValidationQuery {
            name: "list_buildings",
            query: "MATCH (b:building) RETURN b.name AS name",
            min_rows: 2,
            max_rows: 100, // at least main + parking
        },
        ValidationQuery {
            name: "filter_temp_sensors",
            query: "MATCH (s:temperature_sensor) RETURN s.name AS name, s.unit AS unit",
            min_rows: 6,
            max_rows: 10000,
        },
        ValidationQuery {
            name: "order_by_limit",
            query: "MATCH (f:floor) RETURN f.name AS name, f.level AS level ORDER BY f.level ASC LIMIT 3",
            min_rows: 1,
            max_rows: 3,
        },
        ValidationQuery {
            name: "edge_traversal_contains",
            query: "MATCH (c:campus)-[:contains]->(b:building) RETURN c.name AS campus, b.name AS building",
            min_rows: 2,
            max_rows: 100,
        },
        ValidationQuery {
            name: "two_hop_campus_to_floor",
            query: "MATCH (c:campus)-[:contains]->(b:building)-[:contains]->(f:floor) RETURN c.name AS campus, f.name AS floor",
            min_rows: 3,
            max_rows: 10000,
        },
        ValidationQuery {
            name: "var_length_containment",
            query: "MATCH (c:campus)-[:contains]->{1,4}(s:sensor) RETURN s.name AS sensor",
            min_rows: 12,
            max_rows: 100_000,
        },
        ValidationQuery {
            name: "feeds_relationship",
            query: "MATCH (a:ahu)-[:feeds]->(v:vav) RETURN a.name AS ahu, v.name AS vav",
            min_rows: 6,
            max_rows: 10000,
        },
        ValidationQuery {
            name: "hvac_cycle_returns_to",
            query: "MATCH (z:zone)-[:returns_to]->(a:ahu) RETURN z.name AS zone, a.name AS ahu",
            min_rows: 6,
            max_rows: 10000,
        },
        ValidationQuery {
            name: "aggregation_avg",
            query: "MATCH (v:vav) RETURN avg(v.max_cfm) AS avg_cfm",
            min_rows: 1,
            max_rows: 1,
        },
        ValidationQuery {
            name: "multi_label_node",
            query: "MATCH (e:alarm&safety) RETURN e.name AS name",
            min_rows: 1,
            max_rows: 100,
        },
        ValidationQuery {
            name: "cross_hierarchy_monitors",
            query: "MATCH (s:server)-[:monitors]->(a:ahu) RETURN s.name AS server, a.name AS ahu",
            min_rows: 2,
            max_rows: 10000,
        },
        ValidationQuery {
            name: "parking_disconnected",
            query: "MATCH (p:parking)-[:contains]->(f:floor)-[:contains]->(s:sensor) RETURN s.name AS sensor",
            min_rows: 2,
            max_rows: 2,
        },
    ]
}

/// Phase 3 validation queries testing algorithm-relevant graph features.
pub fn phase3_queries() -> Vec<ValidationQuery> {
    vec![
        ValidationQuery {
            name: "zone_adjacency",
            query: "MATCH (z1:zone)-[:adjacent_to]->(z2:zone) RETURN z1.name AS `from`, z2.name AS `to`",
            min_rows: 6,
            max_rows: 100_000, // 2 per floor * 3 floors * scale, bidirectional
        },
        ValidationQuery {
            name: "electrical_powers",
            query: "MATCH (p:electrical_panel)-[:powers]->(c:circuit) RETURN p.name AS panel, c.name AS circuit",
            min_rows: 3,
            max_rows: 10000,
        },
        ValidationQuery {
            name: "alarm_coverage",
            query: "MATCH (a:alarm)-[:alarm_for]->(e:ahu) RETURN a.name AS alarm, e.name AS ahu",
            min_rows: 2,
            max_rows: 10000,
        },
        ValidationQuery {
            name: "co2_sensors",
            query: "MATCH (s:co2_sensor) RETURN count(*) AS total",
            min_rows: 1,
            max_rows: 1,
        },
        ValidationQuery {
            name: "breaker_powers_vav",
            query: "MATCH (b:breaker)-[:powers]->(v:vav) RETURN b.name AS breaker, v.name AS vav",
            min_rows: 6,
            max_rows: 10000,
        },
        ValidationQuery {
            name: "weighted_feeds",
            query: "MATCH (a:ahu)-[e:feeds]->(v:vav) RETURN a.name AS ahu, v.name AS vav, e.pipe_length_ft AS length",
            min_rows: 6,
            max_rows: 10000,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_gql::QueryBuilder;

    #[test]
    fn build_creates_expected_counts() {
        let g = build();
        assert!(
            g.node_count() >= 35,
            "expected at least 35 nodes, got {}",
            g.node_count()
        );
        assert!(
            g.edge_count() >= 50,
            "expected at least 50 edges, got {}",
            g.edge_count()
        );
    }

    #[test]
    fn build_is_deterministic() {
        let g1 = build();
        let g2 = build();
        assert_eq!(g1.node_count(), g2.node_count());
        assert_eq!(g1.edge_count(), g2.edge_count());
    }

    #[test]
    fn phase0_validation_queries_pass() {
        let g = build();
        for vq in phase0_queries() {
            let result = QueryBuilder::new(vq.query, &g)
                .execute()
                .unwrap_or_else(|e| panic!("{}: query failed: {e}", vq.name));
            let rows = result.row_count();
            assert!(
                rows >= vq.min_rows && rows <= vq.max_rows,
                "{}: expected {}-{} rows, got {rows}",
                vq.name,
                vq.min_rows,
                vq.max_rows
            );
        }
    }

    #[test]
    fn phase3_validation_queries_pass() {
        let g = reference_building(1);
        for vq in phase3_queries() {
            let result = QueryBuilder::new(vq.query, &g)
                .execute()
                .unwrap_or_else(|e| panic!("{}: query failed: {e}", vq.name));
            let rows = result.row_count();
            assert!(
                rows >= vq.min_rows && rows <= vq.max_rows,
                "{}: expected {}-{} rows, got {rows}",
                vq.name,
                vq.min_rows,
                vq.max_rows
            );
        }
    }

    #[test]
    fn scale_increases_graph_size() {
        let g1 = reference_building(1);
        let g2 = reference_building(3);
        let g5 = reference_building(5);

        // More buildings = more nodes
        assert!(g2.node_count() > g1.node_count());
        assert!(g5.node_count() > g2.node_count());

        // Scale=1 should have at least 50 nodes (1 building with full overlays + parking)
        assert!(g1.node_count() >= 50, "scale=1: {} nodes", g1.node_count());
        // Scale=5 should have ~5x
        assert!(g5.node_count() >= 200, "scale=5: {} nodes", g5.node_count());
    }

    #[test]
    fn reference_building_has_multiple_overlays() {
        let g = reference_building(1);

        // Check each overlay exists via edge labels
        let edge_labels: std::collections::HashSet<String> = g
            .all_edge_bitmap()
            .iter()
            .filter_map(|eid| g.get_edge(selene_core::EdgeId(u64::from(eid))))
            .map(|e| e.label.as_str().to_string())
            .collect();

        assert!(
            edge_labels.contains("contains"),
            "missing containment overlay"
        );
        assert!(edge_labels.contains("feeds"), "missing HVAC feeds");
        assert!(edge_labels.contains("serves"), "missing HVAC serves");
        assert!(
            edge_labels.contains("returns_to"),
            "missing HVAC returns_to"
        );
        assert!(
            edge_labels.contains("monitors"),
            "missing monitoring overlay"
        );
        assert!(
            edge_labels.contains("isPointOf"),
            "missing monitoring isPointOf"
        );
        assert!(
            edge_labels.contains("adjacent_to"),
            "missing spatial adjacency"
        );
        assert!(edge_labels.contains("powers"), "missing electrical overlay");
        assert!(edge_labels.contains("alarm_for"), "missing alarm overlay");
    }

    #[test]
    fn reference_building_scale_deterministic() {
        let g1a = reference_building(3);
        let g1b = reference_building(3);
        assert_eq!(g1a.node_count(), g1b.node_count());
        assert_eq!(g1a.edge_count(), g1b.edge_count());
    }
}
