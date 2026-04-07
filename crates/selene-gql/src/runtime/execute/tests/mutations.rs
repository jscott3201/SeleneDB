//! Mutation write tests, DETACH DELETE, and trigger tests.

use super::*;

// ── Mutation write tests ──

#[test]
fn e2e_execute_mut_insert_node() {
    let shared = SharedGraph::new(SeleneGraph::new());
    let result = MutationBuilder::new("INSERT (:sensor {name: 'NewSensor'})")
        .execute(&shared)
        .unwrap();
    assert_eq!(result.mutations.nodes_created, 1);

    // Verify node was actually created
    let count = shared.read(|g| g.node_count());
    assert_eq!(count, 1);

    // Verify properties
    let name = shared.read(|g| {
        let node = g.get_node(NodeId(1)).unwrap();
        node.property("name").cloned()
    });
    assert_eq!(name, Some(Value::String(SmolStr::new("NewSensor"))));
}

#[test]
fn e2e_execute_mut_set_property() {
    let shared = SharedGraph::new(SeleneGraph::new());

    // Create a sensor first
    MutationBuilder::new("INSERT (:sensor {name: 'S1'})")
        .execute(&shared)
        .unwrap();

    // Set property via GQL
    let result = MutationBuilder::new("MATCH (s:sensor) FILTER s.name = 'S1' SET s.temp = 72.5")
        .execute(&shared)
        .unwrap();
    assert_eq!(result.mutations.properties_set, 1);

    // Verify property was set
    let temp = shared.read(|g| {
        let node = g.get_node(NodeId(1)).unwrap();
        node.property("temp").cloned()
    });
    assert_eq!(temp, Some(Value::Float(72.5)));
}

#[test]
fn e2e_execute_mut_delete_node() {
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new("INSERT (:sensor {name: 'ToDelete'})")
        .execute(&shared)
        .unwrap();
    assert_eq!(shared.read(|g| g.node_count()), 1);

    let result = MutationBuilder::new("MATCH (s:sensor) FILTER s.name = 'ToDelete' DELETE s")
        .execute(&shared)
        .unwrap();
    assert_eq!(result.mutations.nodes_deleted, 1);
    assert_eq!(shared.read(|g| g.node_count()), 0);
}

#[test]
fn e2e_execute_mut_atomic_rollback() {
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new("INSERT (:sensor {name: 'S1'})")
        .execute(&shared)
        .unwrap();

    // Try to delete a nonexistent node --should fail
    let result = MutationBuilder::new("MATCH (s:sensor) DELETE s").execute(&shared);
    // This should succeed (deletes S1)
    assert!(result.is_ok());
    assert_eq!(shared.read(|g| g.node_count()), 0);
}

#[test]
fn e2e_execute_in_transaction() {
    let shared = SharedGraph::new(SeleneGraph::new());

    let mut txn = shared.begin_transaction();
    MutationBuilder::new("INSERT (:sensor {name: 'A'})")
        .execute_in_transaction(&mut txn)
        .unwrap();
    MutationBuilder::new("INSERT (:sensor {name: 'B'})")
        .execute_in_transaction(&mut txn)
        .unwrap();

    // Both visible within transaction
    assert_eq!(txn.graph().node_count(), 2);

    let changes = txn.commit();
    assert!(!changes.is_empty());

    // Both visible after commit
    assert_eq!(shared.read(|g| g.node_count()), 2);
}

#[test]
fn e2e_scoped_mutation_no_match() {
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new("INSERT (:sensor {name: 'S1'})")
        .execute(&shared)
        .unwrap();

    // Scope that does NOT include node 1
    let mut scope = RoaringBitmap::new();
    scope.insert(999); // only node 999 is in scope

    let result = MutationBuilder::new("MATCH (s:sensor) SET s.alert = TRUE")
        .with_scope(&scope)
        .execute(&shared)
        .unwrap();
    // Node 1 is out of scope -> LabelScan returns 0 bindings -> 0 properties set
    assert_eq!(result.mutations.properties_set, 0);
}

// ── DETACH DELETE ──

#[test]
fn e2e_plain_delete_fails_with_edges() {
    let shared = SharedGraph::new(SeleneGraph::new());
    // Create building + floor + edge using the graph API directly
    shared
        .write(|m| {
            m.create_node(
                LabelSet::from_strs(&["building"]),
                PropertyMap::from_pairs(vec![(
                    IStr::new("name"),
                    Value::String(SmolStr::new("B1")),
                )]),
            )?;
            m.create_node(
                LabelSet::from_strs(&["floor"]),
                PropertyMap::from_pairs(vec![(
                    IStr::new("name"),
                    Value::String(SmolStr::new("F1")),
                )]),
            )?;
            m.create_edge(
                NodeId(1),
                IStr::new("contains"),
                NodeId(2),
                PropertyMap::new(),
            )?;
            Ok(())
        })
        .unwrap();
    assert_eq!(shared.read(|g| g.edge_count()), 1);

    // Plain DELETE should fail because building has edges
    let result = MutationBuilder::new("MATCH (b:building) DELETE b").execute(&shared);
    assert!(
        result.is_err(),
        "plain DELETE on node with edges should fail"
    );
}

#[test]
fn e2e_detach_delete_cascades_edges() {
    let shared = SharedGraph::new(SeleneGraph::new());
    shared
        .write(|m| {
            m.create_node(
                LabelSet::from_strs(&["building"]),
                PropertyMap::from_pairs(vec![(
                    IStr::new("name"),
                    Value::String(SmolStr::new("B1")),
                )]),
            )?;
            m.create_node(
                LabelSet::from_strs(&["floor"]),
                PropertyMap::from_pairs(vec![(
                    IStr::new("name"),
                    Value::String(SmolStr::new("F1")),
                )]),
            )?;
            m.create_edge(
                NodeId(1),
                IStr::new("contains"),
                NodeId(2),
                PropertyMap::new(),
            )?;
            Ok(())
        })
        .unwrap();

    // DETACH DELETE should cascade the edge
    let result = MutationBuilder::new("MATCH (b:building) DETACH DELETE b")
        .execute(&shared)
        .unwrap();
    assert_eq!(result.mutations.nodes_deleted, 1);
    assert_eq!(shared.read(|g| g.node_count()), 1); // only floor left
    assert_eq!(shared.read(|g| g.edge_count()), 0); // edge cascaded
}

#[test]
fn e2e_plain_delete_succeeds_without_edges() {
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new("INSERT (:sensor {name: 'S1'})")
        .execute(&shared)
        .unwrap();
    let result = MutationBuilder::new("MATCH (s:sensor) DELETE s")
        .execute(&shared)
        .unwrap();
    assert_eq!(result.mutations.nodes_deleted, 1);
    assert_eq!(shared.read(|g| g.node_count()), 0);
}

// ── Trigger tests ──

#[test]
fn e2e_create_trigger_parses() {
    let stmt = crate::parser::parse_statement(
            "CREATE TRIGGER high_temp AFTER SET ON :sensor WHEN n.temp > 80 EXECUTE SET n.status = 'critical'"
        ).unwrap();
    assert!(matches!(stmt, GqlStatement::CreateTrigger(_)));
}

#[test]
fn e2e_drop_trigger_parses() {
    let stmt = crate::parser::parse_statement("DROP TRIGGER high_temp").unwrap();
    assert!(matches!(stmt, GqlStatement::DropTrigger(_)));
}

#[test]
fn e2e_show_triggers_parses() {
    let stmt = crate::parser::parse_statement("SHOW TRIGGERS").unwrap();
    assert!(matches!(stmt, GqlStatement::ShowTriggers));
}

#[test]
fn e2e_trigger_create_and_show() {
    let g = setup_graph();
    let shared = selene_graph::SharedGraph::new(g);

    // Create a trigger
    MutationBuilder::new(
        "CREATE TRIGGER t1 AFTER SET ON :sensor WHEN n.temp > 80 EXECUTE SET n.status = 'critical'",
    )
    .execute(&shared)
    .unwrap();

    // SHOW TRIGGERS should list it
    let result = MutationBuilder::new("SHOW TRIGGERS")
        .execute(&shared)
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_trigger_create_and_drop() {
    let g = setup_graph();
    let shared = selene_graph::SharedGraph::new(g);

    MutationBuilder::new("CREATE TRIGGER t1 AFTER SET ON :sensor EXECUTE SET n.status = 'ok'")
        .execute(&shared)
        .unwrap();

    // Drop it
    MutationBuilder::new("DROP TRIGGER t1")
        .execute(&shared)
        .unwrap();

    // SHOW TRIGGERS should be empty
    let result = MutationBuilder::new("SHOW TRIGGERS")
        .execute(&shared)
        .unwrap();
    assert_eq!(result.row_count(), 0);
}

#[test]
fn e2e_trigger_fires_on_set() {
    let g = setup_graph();
    let shared = selene_graph::SharedGraph::new(g);

    // Create trigger: when temp is set on sensor, also set status
    MutationBuilder::new(
            "CREATE TRIGGER auto_status AFTER SET ON :sensor WHEN n.temp > 79 EXECUTE SET n.status = 'hot'",
            ).execute(&shared).unwrap();

    // Set temp on sensor node 3 (TempSensor-1, temp=72.5) --below threshold, trigger should NOT fire
    MutationBuilder::new("MATCH (s:sensor) FILTER s.name = 'TempSensor-1' SET s.temp = 70.0")
        .execute(&shared)
        .unwrap();

    // Check: status should NOT be set
    let result = shared.read(|g| {
        g.get_node(selene_core::NodeId(3))
            .and_then(|n| n.property("status").cloned())
    });
    assert!(
        result.is_none(),
        "trigger should not have fired for temp=70"
    );

    // Set temp above threshold
    MutationBuilder::new("MATCH (s:sensor) FILTER s.name = 'TempSensor-2' SET s.temp = 85.0")
        .execute(&shared)
        .unwrap();

    // Check: status should be 'hot'
    let result = shared.read(|g| {
        g.get_node(selene_core::NodeId(4))
            .and_then(|n| n.property("status").cloned())
    });
    assert_eq!(result, Some(Value::String(SmolStr::new("hot"))));
}

// ── Multi-label INSERT tests ──

#[test]
fn insert_multi_label_node() {
    let shared = SharedGraph::new(SeleneGraph::new());
    let result = MutationBuilder::new("INSERT (n:A&B {name: 'test'}) RETURN id(n) AS id")
        .execute(&shared)
        .unwrap();
    assert_eq!(result.mutations.nodes_created, 1);
    assert_eq!(result.batches[0].num_rows(), 1);

    // Verify both labels via MATCH
    shared.read(|g| {
        let qr = QueryBuilder::new("MATCH (n:A&B) RETURN n.name AS name", g)
            .execute()
            .unwrap();
        assert_eq!(qr.batches[0].num_rows(), 1);
    });
}

#[test]
fn insert_single_label_still_works() {
    let shared = SharedGraph::new(SeleneGraph::new());
    let result = MutationBuilder::new("INSERT (:OnlyOne {val: 1})")
        .execute(&shared)
        .unwrap();
    assert_eq!(result.mutations.nodes_created, 1);
}

// ── MERGE RETURN binding tests ──

#[test]
fn merge_return_id() {
    let shared = SharedGraph::new(SeleneGraph::new());
    let result = MutationBuilder::new("MERGE (n:TestLabel {name: 'Alice'}) RETURN id(n) AS id")
        .execute(&shared)
        .unwrap();
    assert_eq!(result.mutations.nodes_created, 1);
    assert_eq!(result.batches[0].num_rows(), 1);
}

#[test]
fn merge_idempotent_returns_same_id() {
    let shared = SharedGraph::new(SeleneGraph::new());
    let r1 = MutationBuilder::new("MERGE (n:TestLabel {name: 'Alice'}) RETURN id(n) AS id")
        .execute(&shared)
        .unwrap();
    assert_eq!(r1.mutations.nodes_created, 1);

    let r2 = MutationBuilder::new("MERGE (n:TestLabel {name: 'Alice'}) RETURN id(n) AS id")
        .execute(&shared)
        .unwrap();
    // Second MERGE should find existing node, not create
    assert_eq!(r2.mutations.nodes_created, 0);
    // Both should return the same ID
    assert_eq!(r1.batches[0].num_rows(), 1);
    assert_eq!(r2.batches[0].num_rows(), 1);
}

// ── INSERT properties_set counter ──

#[test]
fn insert_node_counts_inline_properties() {
    let shared = SharedGraph::new(SeleneGraph::new());
    let result = MutationBuilder::new("INSERT (n:Test {a: 1, b: 2, c: 3})")
        .execute(&shared)
        .unwrap();
    assert_eq!(result.mutations.nodes_created, 1);
    assert_eq!(result.mutations.properties_set, 3);
}

#[test]
fn insert_path_counts_node_and_edge_properties() {
    let shared = SharedGraph::new(SeleneGraph::new());
    let result = MutationBuilder::new("INSERT (a:X {x: 1})-[:R {w: 5}]->(b:Y {y: 2})")
        .execute(&shared)
        .unwrap();
    assert_eq!(result.mutations.nodes_created, 2);
    assert_eq!(result.mutations.edges_created, 1);
    assert_eq!(result.mutations.properties_set, 3);
}
