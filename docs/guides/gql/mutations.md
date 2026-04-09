# GQL Mutations

SeleneDB uses GQL (ISO 39075) as its sole mutation interface. All graph modifications -- creating nodes and edges, updating properties, adding labels, and deleting elements -- flow through GQL mutation statements. This guide covers every mutation operation with working examples drawn from building automation and IoT domains.

## INSERT Nodes

The `INSERT` statement creates new nodes with labels and properties. Labels appear after a colon inside the node pattern, and properties are specified as a key-value map inside curly braces.

```gql
INSERT (:sensor {name: 'temp-1', unit: 'degF'})
```

Multiple labels can be assigned by joining them with `&:`:

```gql
INSERT (:sensor&:equipment {name: 'VAV-101', type: 'variable_air_volume'})
```

When an `INSERT` includes a variable, subsequent operations in the same statement can reference it. A `RETURN` clause after an `INSERT` can project properties of the newly created node:

```gql
INSERT (s:sensor {name: 'temp-2', unit: 'degC'}) RETURN s.name AS created
```

## INSERT Edges

Edges connect two existing or newly created nodes. To link nodes that already exist in the graph, use `MATCH` to bind them first, then `INSERT` the edge:

```gql
MATCH (s:sensor), (e:equipment)
FILTER s.name = 'temp-1' AND e.name = 'AHU-1'
INSERT (s)-[:isPointOf]->(e)
```

To create nodes and an edge in a single statement, write the full path pattern. SeleneDB creates all elements in one atomic operation:

```gql
INSERT (s:sensor {name: 'flow-1'})-[:isPointOf]->(e:equipment {name: 'AHU-2'})
```

Variables in INSERT path patterns are shared across the statement. If the same variable appears twice, the second occurrence references the node created by the first -- it does not create a duplicate:

```gql
INSERT (a:sensor {name: 'A'})-[:KNOWS]->(b:sensor {name: 'B'})
RETURN a.name AS ANAME, b.name AS BNAME
```

### MATCH + INSERT (Per-Row Insertion)

When `INSERT` follows a `MATCH`, it executes once for each matched row. This is useful for bulk relationship creation:

```gql
MATCH (s:sensor)
INSERT (s)-[:monitored_by]->(a:alert {level: 'info'})
```

If the MATCH produces two rows, this creates two `alert` nodes and two `monitored_by` edges.

## SET Properties

The `SET` statement modifies properties on nodes or edges that were bound by a preceding `MATCH`. Specify the target variable, property name, and new value:

```gql
MATCH (n:sensor)
FILTER n.name = 'temp-1'
SET n.status = 'active'
```

Multiple properties can be set in a single statement by separating them with commas:

```gql
MATCH (n:sensor)
FILTER n.name = 'temp-1'
SET n.status = 'active', n.lastCalibrated = '2026-03-29'
```

Edge properties work the same way -- bind the edge variable in the MATCH pattern and set properties on it:

```gql
MATCH (s:sensor)-[r:isPointOf]->(e:equipment)
FILTER s.name = 'temp-1'
SET r.weight = 1.0
```

### Two-Phase SET Semantics

SeleneDB evaluates all SET expressions against the pre-mutation graph snapshot before applying any changes. This guarantees that value swaps work correctly:

```gql
MATCH (a:pair)-[:link]->(b:pair)
SET a.val = b.val, b.val = a.val
```

Both `a.val` and `b.val` are read from the original state, so the values swap as expected.

## SET ALL (Replace Properties)

`SET ALL` replaces the entire property map of a node. All existing properties are removed and replaced with the specified set:

```gql
MATCH (n:sensor)
FILTER n.name = 'temp-1'
SET n = {name: 'temp-1-updated', status: 'active'}
```

After this operation, `n` has exactly two properties (`name` and `status`). Any properties that existed before -- such as `unit` -- are removed.

## SET Label

Add a label to an existing node using `SET` with the `IS` keyword or colon syntax:

```gql
MATCH (n:sensor)
FILTER n.name = 'temp-1'
SET n IS critical
```

The colon syntax also works:

```gql
MATCH (n:sensor) FILTER n.name = 'temp-1' SET n:critical
```

## REMOVE Properties

The `REMOVE` statement deletes a property from a node or edge:

```gql
MATCH (n:sensor)
FILTER n.name = 'temp-1'
REMOVE n.oldCalibration
```

Multiple properties can be removed in one statement:

```gql
MATCH (n:sensor) FILTER n.name = 'temp-1'
REMOVE n.oldCalibration, n.deprecated
```

Edge properties can also be removed:

```gql
MATCH (s:sensor)-[r:isPointOf]->(e:equipment)
FILTER s.name = 'temp-1'
REMOVE r.weight
```

## REMOVE Labels

Remove a label from a node using `REMOVE` with the `IS` keyword or colon syntax:

```gql
MATCH (n:sensor) FILTER n.name = 'temp-1'
REMOVE n IS deprecated
```

Or equivalently:

```gql
MATCH (n:sensor) FILTER n.name = 'temp-1'
REMOVE n:deprecated
```

## DELETE and DETACH DELETE

### DELETE

`DELETE` removes a node or edge from the graph. For nodes, the operation fails if the node has any incident edges (incoming or outgoing). This prevents orphaned edges:

```gql
MATCH (n:sensor) FILTER n.name = 'temp-1'
DELETE n
```

If the node has edges, SeleneDB returns an error:

```
cannot delete node 1 with 2 incident edges, use DETACH DELETE
```

Edges can be deleted directly:

```gql
MATCH (s:sensor)-[r:isPointOf]->(e:equipment)
FILTER s.name = 'temp-1'
DELETE r
```

### DETACH DELETE

`DETACH DELETE` removes a node and automatically cascades deletion to all connected edges:

```gql
MATCH (b:building) FILTER b.name = 'HQ'
DETACH DELETE b
```

This removes the building node and every edge connected to it, regardless of direction.

## MERGE

`MERGE` performs an upsert -- it matches an existing node or creates a new one if no match is found. Optional `ON CREATE SET` and `ON MATCH SET` clauses apply properties conditionally:

```gql
MERGE (n:sensor {name: 'temp-1'})
ON CREATE SET n.created = '2026-03-29'
ON MATCH SET n.lastSeen = '2026-03-29'
```

## Atomicity and Auto-Commit

Each GQL mutation statement executes atomically. All mutation operations within a single statement are wrapped in one write transaction. If any operation fails, the entire statement rolls back -- no partial changes are applied to the graph.

For example, if a `SET` followed by a `DELETE` appears in the same mutation block, they execute within the same atomic write:

```gql
MATCH (n:sensor) FILTER n.name = 'temp-1'
SET n.status = 'retired'
DETACH DELETE n
```

## Explicit Transactions

Explicit transactions are available through the programmatic API (`MutationBuilder`), not through GQL syntax. The `MutationBuilder` supports two execution modes:

**Auto-commit** -- each call to `execute()` is an independent atomic operation:

```rust
MutationBuilder::new("INSERT (:sensor {name: 'T1'})")
    .execute(&shared)?;
```

**Transaction** -- multiple statements can be grouped into a single transaction. All statements see each other's changes, and the transaction commits only when `commit()` is called:

```rust
let mut txn = shared.begin_transaction();

MutationBuilder::new("INSERT (:sensor {name: 'A'})")
    .execute_in_transaction(&mut txn)?;

MutationBuilder::new("INSERT (:sensor {name: 'B'})")
    .execute_in_transaction(&mut txn)?;

// Both nodes become visible to readers only after commit
let changes = txn.commit();
```

If a transaction is dropped without calling `commit()`, all changes are discarded.

## Schema Validation

When a node or edge schema is registered, SeleneDB validates properties on every write operation (INSERT, SET, SET ALL). Schema validation includes:

- **Type checking** -- property values must match the declared type in the schema definition.
- **Required properties** -- schemas can mark properties as required; INSERT and SET ALL operations that omit required properties are rejected.
- **Dictionary encoding** -- properties flagged with `dictionary: true` in the schema automatically promote `String` values to `InternedStr` on write. This applies to both node and edge schemas and reduces memory usage for enum-like properties (such as `status`, `unit`, or `type` fields). The promotion is transparent to queries -- `String` and `InternedStr` values compare as equal.

## Mutation Statistics

Every mutation operation returns a `MutationStats` object reporting what changed:

| Field | Description |
|-------|-------------|
| `nodes_created` | Number of nodes inserted |
| `edges_created` | Number of edges inserted |
| `nodes_deleted` | Number of nodes deleted |
| `edges_deleted` | Number of edges deleted |
| `properties_set` | Number of property SET operations |
| `properties_removed` | Number of property REMOVE operations |

These statistics are available on the `GqlResult.mutations` field returned by every mutation execution.

## Change Tracking

SeleneDB tracks individual changes within each mutation for downstream consumers (triggers, WAL, CDC replicas). Each atomic write produces a list of `Change` events:

| Change Type | Fields |
|-------------|--------|
| `NodeCreated` | node_id |
| `NodeDeleted` | node_id, labels (captured before removal) |
| `PropertySet` | node_id, key, value, old_value |
| `PropertyRemoved` | node_id, key, old_value |
| `LabelAdded` | node_id, label |
| `LabelRemoved` | node_id, label |
| `EdgeCreated` | edge_id, source, target, label |
| `EdgeDeleted` | edge_id, source, target, label |
| `EdgePropertySet` | edge_id, source, target, key, value, old_value |
| `EdgePropertyRemoved` | edge_id, source, target, key, old_value |
