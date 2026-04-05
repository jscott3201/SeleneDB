# GQL Queries

This guide covers read-only GQL queries in Selene: pattern matching, filtering, projection, aggregation, and sorting. For mutations (INSERT, SET, DELETE), see the [mutations guide](mutations.md).

## Basic MATCH

The `MATCH` clause finds nodes and edges that satisfy a given pattern. Every query starts with a `MATCH` (or with `RETURN` for computed expressions).

Match all nodes in the graph:

```gql
MATCH (n) RETURN n.id AS id, n.name AS name
```

Match nodes with a specific label. Labels are prefixed with a colon:

```gql
MATCH (s:sensor) RETURN s.name AS name
```

Combine a label filter with a property filter using `FILTER`:

```gql
MATCH (s:sensor) FILTER s.unit = '°F' RETURN s.name AS name, s.temp AS temp
```

Inline property maps provide shorthand for equality filters. The following query returns only the sensor named `TempSensor-1`:

```gql
MATCH (s:sensor {name: 'TempSensor-1'}) RETURN s.temp AS temp
```

## Edge patterns

Edges are written inside square brackets between two node patterns. An arrow indicates direction.

Match outgoing edges:

```gql
MATCH (b:building)-[e:contains]->(f:floor)
RETURN b.name AS building, f.name AS floor
```

Match incoming edges (reverse arrow):

```gql
MATCH (s:sensor)<-[e:contains]-(f:floor)
RETURN s.name AS sensor, f.name AS floor
```

Match edges in either direction (no arrowhead):

```gql
MATCH (a)-[e:contains]-(b)
RETURN a.name AS from, b.name AS to
```

Chain multiple edge patterns for multi-hop traversals:

```gql
MATCH (b:building)-[:contains]->(f:floor)-[:contains]->(s:sensor)
RETURN b.name AS building, s.name AS sensor
```

## Variable-length paths

Variable-length paths match chains of edges with a specified depth range. Selene uses quantifier syntax with curly braces after the edge pattern.

Match all nodes reachable from a building within 1 to 3 hops:

```gql
MATCH (b:building)-[:contains]->{1,3}(s)
RETURN b.name AS building, s.id AS node_id
```

Match only sensor nodes reachable within 1 to 3 hops. Intermediate nodes (such as floors) are traversed but not emitted:

```gql
MATCH (b:building)-[:contains]->{1,3}(s:sensor)
RETURN b.name AS building, s.name AS sensor
```

The `+` quantifier means one or more hops (equivalent to `{1,}`):

```gql
MATCH (s:sensor)<-[:contains]-+(b)
RETURN s.name AS sensor, b.name AS ancestor
```

Additional quantifiers:

| Syntax | Meaning |
|--------|---------|
| `{2,5}` | Between 2 and 5 hops |
| `{3}` | Exactly 3 hops |
| `{2,}` | 2 or more hops |
| `{,4}` | Up to 4 hops |
| `*` | Zero or more hops |
| `+` | One or more hops |
| `?` | Zero or one hop |

Variable-length paths use TRAIL semantics by default: each edge may appear at most once per path. This prevents infinite loops in cyclic graphs. You can specify a different path mode explicitly with the `WALK`, `ACYCLIC`, `SIMPLE`, or `TRAIL` keyword before the pattern.

## FILTER

The `FILTER` clause restricts results to rows that satisfy a boolean expression. It can appear after `MATCH` or between pipeline stages.

**Comparison operators:** `=`, `<>`, `<`, `>`, `<=`, `>=`

```gql
MATCH (s:sensor) FILTER s.temp > 75 RETURN s.name AS name
```

**Boolean operators:** `AND`, `OR`, `NOT`, `XOR`

```gql
MATCH (s:sensor) FILTER s.temp > 70 AND s.temp < 80
RETURN s.name AS name
```

**NULL checks:** `IS NULL`, `IS NOT NULL`

```gql
MATCH (s:sensor) FILTER s.alert IS NOT NULL RETURN s.name AS name
```

**LIKE:** pattern matching with `%` (any sequence) and `_` (single character) wildcards. Case-sensitive.

```gql
MATCH (s:sensor) FILTER s.name LIKE 'Temp%' RETURN s.name AS name
```

```gql
MATCH (s:sensor) FILTER s.name NOT LIKE '%Humidity%' RETURN s.name AS name
```

**IN:** check membership in a list.

```gql
MATCH (s:sensor) FILTER s.unit IN ['°F', '°C'] RETURN s.name AS name
```

**BETWEEN:** range check (inclusive on both ends).

```gql
MATCH (s:sensor) FILTER s.temp BETWEEN 68 AND 76 RETURN s.name AS name
```

**String matching:** `STARTS WITH`, `ENDS WITH`, `CONTAINS`

```gql
MATCH (s:sensor) FILTER s.name STARTS WITH 'Temp' RETURN s.name AS name
```

Note: `FILTER` and `WHERE` serve the same purpose. `FILTER` is a standalone pipeline clause. `WHERE` appears inline after `MATCH` or inside `WITH`. Both accept the same expressions.

## RETURN

The `RETURN` clause defines which columns appear in the result.

Return specific properties with aliases:

```gql
MATCH (s:sensor) RETURN s.name AS sensor_name, s.temp AS temperature
```

Return computed expressions:

```gql
MATCH (s:sensor)
RETURN s.name AS name, s.temp * 1.8 + 32 AS fahrenheit
```

Return all bound variables with `*`:

```gql
MATCH (s:sensor) RETURN *
```

When using `RETURN *`, each bound variable (nodes, edges) becomes a column serialized as JSON.

## LET bindings

The `LET` clause introduces named bindings for intermediate computed values. This is useful for reusing a computed expression in multiple places without repeating it.

```gql
MATCH (s:sensor)
LET fahrenheit = s.temp * 1.8 + 32
RETURN s.name AS name, fahrenheit AS temp_f
```

LET bindings can reference prior bindings and be used in subsequent FILTER clauses:

```gql
MATCH (s:sensor)
LET f = s.temp * 1.8 + 32
FILTER f > 160
RETURN s.name AS name, f AS fahrenheit
```

## Aggregation

Aggregation functions reduce multiple rows into summary values. When an aggregation function appears in RETURN without a GROUP BY clause, all matching rows are aggregated into a single result row.

**count(\*)** -- count all matching rows:

```gql
MATCH (s:sensor) RETURN count(*) AS total
```

**sum, avg, min, max** -- numeric aggregations:

```gql
MATCH (s:sensor) RETURN avg(s.temp) AS avg_temp
```

```gql
MATCH (s:sensor) RETURN min(s.temp) AS lowest, max(s.temp) AS highest
```

**collect** -- gather values into a list:

```gql
MATCH (s:sensor) RETURN collect(s.name) AS names
```

**GROUP BY** -- group rows before aggregating. The GROUP BY clause appears after the projection list:

```gql
MATCH (s:sensor)
RETURN s.unit AS unit, avg(s.temp) AS avg_temp, count(*) AS cnt
GROUP BY s.unit
```

**HAVING** -- filter groups after aggregation:

```gql
MATCH (i:item)
RETURN i.cat AS cat, sum(i.val) AS total
GROUP BY i
HAVING total > 10
```

Available aggregate functions:

| Function | Description |
|----------|-------------|
| `count(*)` | Count of rows |
| `count(expr)` | Count of non-null values |
| `sum(expr)` | Sum of numeric values |
| `avg(expr)` / `average(expr)` | Arithmetic mean |
| `min(expr)` | Minimum value |
| `max(expr)` | Maximum value |
| `collect(expr)` | Collect values into a list |
| `collect_list(expr)` | Alias for collect |
| `stddev_pop(expr)` | Population standard deviation |
| `stddev_samp(expr)` | Sample standard deviation |

All aggregate functions except `count(*)` accept an optional `DISTINCT` keyword: `count(DISTINCT s.unit)`.

## DISTINCT

The `DISTINCT` keyword eliminates duplicate rows from the result set.

```gql
MATCH (s:sensor) RETURN DISTINCT s.unit AS unit
```

DISTINCT applies after projection, so two rows with the same projected values collapse into one.

## ORDER BY, LIMIT, and OFFSET

**ORDER BY** sorts results. The default direction is ascending. Use `DESC` for descending.

```gql
MATCH (s:sensor) RETURN s.name AS name, s.temp AS temp
ORDER BY s.temp DESC
```

**LIMIT** restricts the number of returned rows:

```gql
MATCH (s:sensor) RETURN s.name AS name, s.temp AS temp
ORDER BY s.temp DESC LIMIT 1
```

**OFFSET** skips a number of rows before returning results. Combined with LIMIT, this provides pagination:

```gql
MATCH (s:sensor) RETURN s.name AS name
ORDER BY s.name ASC LIMIT 10 OFFSET 5
```

ORDER BY can reference columns not present in the RETURN clause. In that case, the sort column is used internally but does not appear in the output:

```gql
MATCH (s:sensor) RETURN s.name AS name ORDER BY s.temp DESC
```

**NULLS ordering:** append `NULLS FIRST` or `NULLS LAST` to control where null values appear in the sort order.

```gql
MATCH (s:sensor) RETURN s.name AS name, s.alert AS alert
ORDER BY s.alert NULLS LAST
```

## EXISTS subqueries

The `EXISTS` predicate tests whether a subquery pattern has at least one match. It returns a boolean and is typically used inside a `FILTER` clause.

Find buildings that contain at least one floor:

```gql
MATCH (b:building)
FILTER EXISTS { MATCH (b)-[:contains]->(f:floor) }
RETURN b.name AS name
```

The negated form `NOT EXISTS` finds entities that lack a particular relationship:

```gql
MATCH (s:sensor)
FILTER NOT EXISTS { MATCH (s)-[:monitors]->(e:equipment) }
RETURN s.name AS unassigned
```

The subquery inside `EXISTS` uses the same MATCH syntax as the outer query. Variables bound in the outer MATCH (like `b` or `s` above) are visible inside the subquery, which allows correlated filtering.

## count(\*) short-circuit

When the only column in RETURN is `count(*)`, Selene applies an optimization: it counts matching entities directly from the label bitmap index without materializing individual result rows. This avoids building bindings, evaluating property expressions, and allocating per-row output.

```gql
MATCH (s:sensor) RETURN count(*) AS total
```

The short-circuit fires when `count(*)` is the sole projection and there are no pipeline stages (LET, ORDER BY, etc.) between MATCH and RETURN that would require row-level data. The result is a single row with the count value.
