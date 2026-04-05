# GQL Query Optimizer

Selene's GQL engine includes a rule-based optimizer that rewrites execution plans before they run. The optimizer applies up to 8 rules in a fixed-point loop, repeating until no rule produces a change or the iteration limit (8 passes) is reached. This guide covers how to inspect query plans, what each optimizer rule does, the index types available, and how the plan cache works.

## Inspecting Query Plans

### EXPLAIN

EXPLAIN shows the execution plan the optimizer produces for a query, without running it. This reveals which optimizer rules fired and how the engine will execute the query.

**HTTP API:**

```bash
curl -X POST http://localhost:8080/gql \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (s:sensor) FILTER s.temp > 72 RETURN s.name", "explain": true}'
```

**CLI:**

```bash
selene --insecure gql --explain "MATCH (s:sensor) FILTER s.temp > 72 RETURN s.name"
```

The output shows the plan's pattern operations (LabelScan, Expand, VarExpand) and pipeline stages (Filter, Sort, Limit, Return). Look for pushed-down filters, index hints, and TopK fusion to confirm the optimizer is working.

### PROFILE

PROFILE executes the query and annotates each operator with per-operator timing. Use this to identify bottlenecks in real workloads.

**HTTP API:**

```bash
curl -X POST http://localhost:8080/gql \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (s:sensor) FILTER s.temp > 72 RETURN s.name", "profile": true}'
```

**CLI:**

```bash
selene --insecure gql --profile "MATCH (s:sensor) FILTER s.temp > 72 RETURN s.name"
```

---

## Optimizer Rules

The optimizer runs the following 8 rules in order. Each rule examines the execution plan and rewrites it if an optimization opportunity exists.

### 1. ConstantFolding

Evaluates constant expressions at plan time so they do not execute per-row at runtime.

**What it does:**
- Folds arithmetic on literals: `70 + 2` becomes `72`
- Folds comparisons on literals: `10 > 5` becomes `true`
- Folds string concatenation on literals: `'hello' || ' world'` becomes `'hello world'`
- Simplifies boolean logic: `true AND x` becomes `x`, `false AND x` becomes `false`
- Eliminates double negation: `NOT NOT x` becomes `x`
- Folds negation of literals: `-(-5)` becomes `5`

**When it activates:** Any FILTER, LET, RETURN, or WITH expression contains subexpressions where both operands are literals.

**Example:**

```gql
-- Before: FILTER s.temp > 70 + 2
-- After:  FILTER s.temp > 72
MATCH (s:sensor) FILTER s.temp > 70 + 2 RETURN s
```

### 2. AndSplitting

Splits AND-connected filter predicates into separate FILTER operators so that each conjunct can be independently pushed down or reordered.

**What it does:** Recursively splits `FILTER a AND b AND c` into three separate `FILTER a`, `FILTER b`, `FILTER c` pipeline stages.

**When it activates:** A FILTER predicate contains one or more AND operators.

**Example:**

```gql
-- Before: FILTER s.temp > 72 AND s.unit = 'F'
-- After:  FILTER s.temp > 72
--         FILTER s.unit = 'F'
MATCH (s:sensor) FILTER s.temp > 72 AND s.unit = 'F' RETURN s
```

This splitting enables the FilterPushdown rule to push each predicate independently into the pattern scan.

### 3. FilterPushdown

Moves simple property comparison filters from the pipeline into the pattern scan, where they are applied during node iteration rather than as a post-filter. This reduces the number of bindings materialized.

**What it does:** Detects FILTER predicates of the form `var.property <op> literal` (or the reversed form `literal <op> var.property`) and pushes them into the LabelScan's `property_filters` list. The filter is removed from the pipeline. Supported operators: `=`, `!=`, `<`, `>`, `<=`, `>=`.

**When it activates:** A FILTER compares a property of a LabelScan variable against a literal value.

**Example:**

```gql
-- Before: LabelScan(s:sensor) -> FILTER s.temp > 72
-- After:  LabelScan(s:sensor, property_filters: [temp > 72])
MATCH (s:sensor) FILTER s.temp > 72 RETURN s
```

On a 10K-node graph with selective filters, pushdown reduces scan time from hundreds of microseconds to tens of microseconds.

### 4. SymmetryBreaking

Eliminates redundant pattern permutations from undirected edge matches. Without this rule, a pattern like `(a:sensor)-[:NEAR]-(b:sensor)` produces both `(a=1, b=2)` and `(a=2, b=1)` for each pair.

**What it does:** Injects a `a.id < b.id` filter at the front of the pipeline for undirected Expand patterns where both endpoints share the same label.

**When it activates:** The pattern contains an undirected edge (`-[]-`) where both the source and target variables have the same simple label, and no existing filter already compares their IDs.

**Example:**

```gql
-- Before: MATCH (a:sensor)-[:NEAR]-(b:sensor) -- 2N results
-- After:  MATCH (a:sensor)-[:NEAR]-(b:sensor) FILTER a.id < b.id -- N results
MATCH (a:sensor)-[:NEAR]-(b:sensor) RETURN a, b
```

This produces a roughly 2x speedup on symmetric pattern queries.

### 5. PredicateReorder

Reorders consecutive FILTER operators by estimated evaluation cost so that cheap predicates run first. This allows inexpensive filters to discard rows before expensive predicates evaluate.

**What it does:** Finds contiguous runs of two or more FILTER operators in the pipeline and sorts them by estimated cost. The cost model scores expressions from cheapest to most expensive:

| Cost | Expression type |
|------|-----------------|
| 1 | Literals, variables |
| 2 | IS NULL checks |
| 3 | Property access |
| 4 | Equality/comparison |
| 5 | BETWEEN |
| 6 | IN list |
| 8 | LIKE |
| 8 | STARTS WITH, ENDS WITH, CONTAINS |
| 10 | Function calls |
| 20 | EXISTS, COUNT subqueries |

**When it activates:** Two or more consecutive FILTER operators exist in the pipeline after AndSplitting and FilterPushdown have run.

### 6. TopK

Fuses adjacent ORDER BY and LIMIT operators into a single TopK operator that uses a bounded binary heap. Instead of sorting the entire result set and then truncating, TopK maintains only the top-k elements during processing.

**What it does:** Detects a Sort immediately followed by a Limit in the pipeline and replaces both with a single TopK operator.

**When it activates:** The pipeline contains a Sort operator immediately followed by a Limit operator.

**Example:**

```gql
-- Before: Sort(s.temp DESC) -> Limit(10)
-- After:  TopK(s.temp DESC, 10)
MATCH (s:sensor) RETURN s.name, s.temp ORDER BY s.temp DESC LIMIT 10
```

### 7. IndexOrder

Leverages a BTreeMap property index to serve ORDER BY + LIMIT directly from the index, skipping the sort step entirely. The index scan produces results already in sorted order.

**What it does:** When a TopK operator sorts by a single property on the LabelScan variable and no filters exist on the scan, the rule sets an `index_order` hint on the LabelScan. At execution time, if a BTreeMap index exists for (label, property), the scan reads from the index in sorted order and stops after k results. The TopK remains in the pipeline as a safety net for the case where no index exists at runtime.

**When it activates:** The plan has exactly one LabelScan with no inline properties or pushed-down filters, and a TopK with a single sort term that references a property of the scan variable.

**Example:**

```gql
-- With an index on sensor.temp:
-- Scan reads directly from BTreeMap in sorted order, stops after 5 rows
MATCH (s:sensor) RETURN s.name, s.temp ORDER BY s.temp DESC LIMIT 5
```

### 8. CompositeIndexLookup

Enables multi-property index lookups when a LabelScan has two or more inline property equalities that could match a composite index.

**What it does:** Detects LabelScan operations with 2+ literal equality inline properties and sets a `composite_index_keys` hint. At execution time, the scan executor checks if a CompositeTypedIndex exists for the (label, keys) combination and performs a direct lookup instead of scanning all nodes.

**When it activates:** A LabelScan has a simple label and 2+ inline property equalities with literal values.

**Example:**

```gql
-- With a composite index on sensor(floor, zone):
-- Direct lookup in the composite index instead of scanning all sensor nodes
MATCH (s:sensor {floor: 3, zone: 'A'}) RETURN s.name
```

---

## Index Types

Selene maintains three types of indexes that the optimizer and executor use to accelerate queries.

### RoaringBitmap Label Indexes

Every node label is backed by a RoaringBitmap that tracks which node IDs carry that label. When the executor processes a `MATCH (n:sensor)` pattern, it reads the bitmap for "sensor" rather than scanning all nodes. Bitmap intersection handles multi-label patterns like `MATCH (n:sensor&equipment)` efficiently.

### TypedIndex (Single-Property)

A BTreeMap-backed index on a single property, created from schema definitions where `indexed: true`. Values are stored in their native sort order:

- **String properties:** lexicographic order via `BTreeMap<SmolStr, Vec<NodeId>>`
- **Int properties:** numeric order via `BTreeMap<i64, Vec<NodeId>>`
- **UInt properties:** numeric order via `BTreeMap<u64, Vec<NodeId>>`
- **Float properties:** numeric order via `BTreeMap<OrderedFloat<f64>, Vec<NodeId>>`

Point lookups take approximately 2 ns. Range queries iterate the BTree range. The IndexOrder optimizer rule uses these indexes to serve ORDER BY + LIMIT without sorting.

### CompositeTypedIndex (Multi-Property)

A composite index over two or more properties, created from schema definitions with composite key definitions. Lookups match all index keys simultaneously. The CompositeIndexLookup optimizer rule detects matching inline property equalities and routes them to these indexes.

Composite index lookups take approximately 84 ns, compared to sequential property filtering which requires scanning all nodes with the given label.

---

## Plan Cache

Selene caches parsed GQL statements by query hash to avoid re-parsing identical queries. The cache holds up to 256 entries; when full, the least-recently-used entry is evicted.

**Performance characteristics:**

| Operation | Cost |
|-----------|------|
| Cache hit | ~19 ns |
| CALL fast-parse path | ~500 ns |
| Full parse (simple query) | ~7 us |

### How it works

1. Each query string is hashed (using the default hasher).
2. On a hit, the cached `Arc<GqlStatement>` is returned without re-parsing.
3. The cache stores parsed ASTs, not execution plans. Plans are generated fresh from the AST on each execution, which allows the optimizer to observe current graph state.

### Generation Invalidation

The cache tracks the graph's generation counter. When the graph's generation changes (due to schema modifications, node/edge creation or deletion), the entire cache is invalidated on the next lookup. This ensures that type resolution and schema-dependent optimizations remain correct after structural changes.

CALL procedures use a fast-parse path that bypasses both the full PEG grammar parser and the plan cache. The fast-parse path detects the `CALL ... YIELD ...` pattern and extracts the procedure name, arguments, and yield columns directly, reducing parse time to approximately 500 ns.
