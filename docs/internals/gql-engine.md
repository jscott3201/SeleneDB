# GQL Engine Internals

This document is a deep dive into Selene's GQL execution engine (`selene-gql`). It traces a query from text input through parsing, planning, optimization, pattern matching, pipeline execution, and result construction. It also covers the mutation path and the plan cache.

## Query Lifecycle

Every GQL query follows this pipeline:

```
GQL text
   |
   v
Parser (Pest PEG grammar)
   |
   v
AST (typed tree: GqlStatement)
   |
   v
Planner (AST -> ExecutionPlan)
   |
   v
Optimizer (8 rewrite rules, fixed-point loop)
   |
   v
Pattern Executor (materialized bindings from graph traversal)
   |
   v
Pipeline Executor (streaming transforms: LET, FILTER, ORDER BY, RETURN)
   |
   v
Arrow RecordBatch (columnar result)
```

The entry point is `QueryBuilder::new(gql, &graph).execute()` for reads and `MutationBuilder::new(gql).execute(&shared)` for writes. Both are defined in `crates/selene-gql/src/runtime/execute/mod.rs`.

## Parser

### Grammar

The grammar is a Pest PEG file aligned with ISO/IEC 39075:2024 (GQL). It defines rules for statements, patterns, expressions, mutations, DDL, and transaction control.

```
crates/selene-gql/src/parser/grammar.pest
```

The top-level rule is `gql_statement`, which dispatches to `ddl_statement`, `transaction_control`, `select_stmt`, `mutation_pipeline`, `composite_query`, `chained_query`, or `query_pipeline`.

Keywords use Pest's case-insensitive syntax (`^"MATCH"`, `^"RETURN"`). This avoids writing separate rules for each casing variant.

### Keyword Word Boundaries

A common PEG pitfall is matching keywords as prefixes of longer identifiers -- "MATCH" accidentally matching inside "MATCHING". SeleneDB's grammar prevents this by structuring rules so that keywords are followed by required structural elements (whitespace, parentheses, or end-of-input) rather than bare identifier characters. The `ident` rule explicitly excludes reserved words.

### AST Builder

The parser module contains four AST builder files that convert Pest parse trees into typed AST nodes:

| File | Responsibility |
|------|---------------|
| `build.rs` | Top-level dispatch: statement type routing, DDL construction |
| `build_match.rs` | MATCH clause: patterns, label expressions, quantifiers |
| `build_expr.rs` | Expressions: binary ops, function calls, aggregates, literals |
| `build_clause.rs` | Pipeline clauses: LET, FILTER, ORDER BY, RETURN, CALL, WITH |

All identifiers are interned to `IStr` at parse time. This means no string comparisons occur during expression evaluation -- only integer key comparisons.

An expression depth limit (128 levels) prevents stack overflow from pathologically nested expressions. Each parenthesized expression traverses roughly 13 grammar levels in the precedence-climbing chain, so 10 nested parentheses reach the limit.

```
crates/selene-gql/src/parser/mod.rs  (parse_statement function)
```

## AST Types

The AST is defined across four files in `crates/selene-gql/src/ast/`:

### GqlStatement

The top-level enum. Variants:

- `Query(QueryPipeline)` -- read-only query pipeline
- `Chained { blocks }` -- `NEXT`-separated pipelines
- `Composite { first, rest }` -- `UNION`/`INTERSECT`/`EXCEPT` set operations
- `Mutate(MutationPipeline)` -- mutations (INSERT, SET, DELETE)
- `StartTransaction`, `Commit`, `Rollback` -- transaction control
- DDL variants: `CreateNodeType`, `CreateEdgeType`, `CreateGraph`, `CreateIndex`, `CreateUser`, `CreateRole`, `CreateTrigger`, `CreateProcedure`, `GrantRole`, `RevokeRole`, and their corresponding `Drop`/`Show` counterparts

```
crates/selene-gql/src/ast/statement.rs
```

### QueryPipeline

A sequence of `PipelineStatement` values. Each statement transforms the working table:

- `Match(MatchClause)` -- graph pattern matching
- `Let(Vec<LetBinding>)` -- variable binding
- `Filter(Expr)` -- row filtering
- `OrderBy(Vec<OrderTerm>)` -- sorting
- `Offset(u64)`, `Limit(u64)` -- pagination
- `Return(ReturnClause)` -- terminal projection with GROUP BY, DISTINCT, HAVING
- `With(WithClause)` -- intermediate scope-resetting projection
- `Call(ProcedureCall)` -- inline procedure call with YIELD
- `Subquery(QueryPipeline)` -- `CALL { subquery }` per input row
- `For { var, list_expr }` -- list unwinding (also parses `UNWIND expr AS var`)

### Pattern Types

Patterns describe graph structures to find. The hierarchy:

```
MatchClause
  |- selector: Option<PathSelector>     (ANY SHORTEST, ALL SHORTEST)
  |- match_mode: Option<MatchMode>      (DIFFERENT EDGES, REPEATABLE ELEMENTS)
  |- path_mode: PathMode                (Walk, Trail, Acyclic, Simple)
  |- optional: bool                     (OPTIONAL modifier)
  |- patterns: Vec<GraphPattern>        (comma-separated patterns)
  |- where_clause: Option<Expr>         (statement-level WHERE)

GraphPattern
  |- elements: Vec<PatternElement>      (alternating Node, Edge, Node, ...)
  |- path_var: Option<IStr>             (path variable binding: p = ...)

PatternElement::Node(NodePattern)
  |- var, labels, properties, where_clause

PatternElement::Edge(EdgePattern)
  |- var, labels, direction, quantifier, properties, where_clause
```

`LabelExpr` supports boolean composition (`AND`, `OR`, `NOT`) and RPQ extensions (`Concat`, `Star`, `Plus`, `Optional`). At execution time, label expressions compile to RoaringBitmap operations.

```
crates/selene-gql/src/ast/pattern.rs
```

### Expression Types

The `Expr` enum has ~40 variants covering all value-producing expressions in GQL. The design deliberately uses a flat enum to avoid pointer-chasing during evaluation, with heavy variants (`Case`, `Between`, `Trim`) boxed to keep common-case size manageable.

Key variant groups:

- **Leaf:** `Literal`, `Var`, `Parameter`
- **Access:** `Property`, `TemporalProperty`, `ListAccess`
- **Binary:** `Compare`, `Arithmetic`, `Logic`, `Concat`, `StringMatch`
- **Unary:** `Not`, `Negate`, `IsNull`, `Labels`
- **Function/Aggregate:** `Function`, `Aggregate`, `Cast`
- **Predicate:** `InList`, `Like`, `Between`, `Exists`, `CountSubquery`, `IsLabeled`, `IsTyped`, `IsDirected`, `IsTruthValue`, `IsNormalized`, `IsSourceOf`, `IsDestinationOf`
- **Subquery:** `ValueSubquery`, `CollectSubquery`
- **Construction:** `ListConstruct`, `RecordConstruct`
- **Conditional:** `Case`

Every expression can report its inferred output type via `infer_type()` and whether it contains aggregates via `is_aggregate()`. The planner uses `is_aggregate()` to determine GROUP BY vs simple projection.

```
crates/selene-gql/src/ast/expr.rs
```

### Mutation Types

`MutationPipeline` represents the mutation path: optional MATCH pipeline for target selection, one or more `MutationOp` values, and optional RETURN.

`MutationOp` variants: `InsertPattern`, `SetProperty`, `SetAllProperties`, `SetLabel`, `RemoveProperty`, `RemoveLabel`, `Delete`, `DetachDelete`, `Merge`.

`InsertPattern` supports path-based composite insertion per GQL spec section 13.2 -- a single INSERT can create nodes and edges together as `(:A)-[:REL]->(:B)`.

```
crates/selene-gql/src/ast/mutation.rs
```

## Planner

The planner converts AST into an `ExecutionPlan` -- a list of `PatternOp` values (graph traversal) followed by `PipelineOp` values (streaming transforms).

```
crates/selene-gql/src/planner/mod.rs  (plan_query function)
```

### Pattern Planning

For each `MatchClause`, the planner:

1. Extracts the first node pattern as a `LabelScan` root.
2. Chains subsequent edge-node pairs as `Expand` or `VarExpand` operations.
3. Detects cycle closures (when the last edge targets an already-bound variable) and emits `CycleJoin`.
4. For comma-separated patterns, detects shared variables and emits `Join` operations. No shared variables produce a cartesian product.
5. For `OPTIONAL MATCH`, wraps inner ops in `Optional` with join variables for left-outer-join semantics.

### Pipeline Planning

Each `PipelineStatement` maps directly to a `PipelineOp`:

- `Let` -> `PipelineOp::Let`
- `Filter` -> `PipelineOp::Filter`
- `OrderBy` -> `PipelineOp::Sort`
- `Return` -> `PipelineOp::Return`
- `Call` -> `PipelineOp::Call`

The `MATCH WHERE` predicate becomes a pipeline `Filter` appended after pattern ops, keeping it separate from inline pattern property filters.

### ExecutionPlan

```
crates/selene-gql/src/planner/plan.rs
```

The plan has two phases:

- **Phase 1: Pattern ops** -- produces materialized `Vec<Binding>` from graph traversal.
- **Phase 2: Pipeline ops** -- streaming transforms (except Sort, which is a pipeline breaker).

The plan also carries `count_only: bool` -- set when the query is a pure `MATCH (...) RETURN count(*)` with no GROUP BY, HAVING, or DISTINCT. The executor can skip binding materialization and return bitmap cardinality directly.

## Optimizer

The optimizer runs rules in a fixed-point loop until no rule changes the plan (or max 8 iterations). Each rule implements the `GqlOptimizerRule` trait and returns `Transformed<ExecutionPlan>` indicating whether the plan changed.

```
crates/selene-gql/src/planner/optimize.rs  (GqlOptimizer struct)
```

### Rule Pipeline

The 8 rules execute in this order:

| # | Rule | What it does |
|---|------|-------------|
| 1 | `ConstantFoldingRule` | Evaluates constant expressions at plan time (e.g., `1 + 2` becomes `3`). Reduces runtime work. |
| 2 | `AndSplittingRule` | Splits `a AND b AND c` filters into separate `Filter` pipeline ops. This creates individual predicates that other rules (FilterPushdown, CompositeIndexLookup) can act on independently. |
| 3 | `FilterPushdownRule` | Moves property comparison filters (equality and inequality: `=`, `!=`, `<`, `>`, `<=`, `>=`) from the pipeline into pattern scan ops as `property_filters`. Filtering at scan time avoids creating bindings that would be immediately discarded. |
| 4 | `SymmetryBreakingRule` | For undirected patterns where both endpoints have the same label (e.g., `(a:L)-[:E]-(b:L)`), injects a `lo.id < hi.id` filter to eliminate symmetric duplicates. Yields a ~2x speedup on affected queries. |
| 5 | `PredicateReorderRule` | Reorders predicates within a filter so that cheaper checks (equality, NULL checks) run before expensive ones (string operations, function calls). |
| 6 | `TopKRule` | Fuses adjacent `Sort` + `Limit` into a single `TopK` op. TopK uses a bounded binary heap: O(N log K) instead of O(N log N) full sort. |
| 7 | `IndexOrderRule` | When ORDER BY + LIMIT can be served by a BTreeMap property index, annotates the LabelScan with `index_order`. The scan reads pre-sorted entries directly from the index, avoiding the sort entirely. |
| 8 | `CompositeIndexLookupRule` | Detects when pushed-down filters match a composite index key set and annotates the LabelScan with `composite_index_keys` for multi-property lookup. |

Rules interact constructively. For example, `AndSplittingRule` creates separate filters that `FilterPushdownRule` can then push into pattern scans, which in turn creates opportunities for `CompositeIndexLookupRule`.

## Pattern Executor

The pattern executor produces `Vec<Binding>` by traversing the graph according to the plan's `PatternOp` sequence.

### LabelScan

Starting point for all pattern matching. Resolves a `LabelExpr` to a `RoaringBitmap` of matching node IDs, then iterates the bitmap to create one `Binding` per matching node.

Label expression resolution:

- `Name(n)` -> bitmap lookup from `graph.label_bitmap(n)`
- `Or(items)` -> fold with bitmap union (`|`)
- `And(items)` -> fold with bitmap intersection (`&`)
- `Not(inner)` -> all-nodes bitmap minus the inner bitmap
- `Wildcard` -> all-nodes bitmap

If auth scope is active, the scope bitmap is intersected as an implicit AND -- only nodes within the user's authorized scope are returned.

Pushed-down property filters from the optimizer are applied during scan before creating bindings. This avoids allocating `Binding` structs for nodes that would immediately be filtered out.

```
crates/selene-gql/src/pattern/scan.rs  (resolve_label_expr, execute_scan)
```

### Expand

Single-hop edge traversal. For each input binding, looks up the source node's adjacency list, filters by edge label and target node labels, and produces extended bindings with the edge variable and target node variable bound.

Two traversal paths:

- **CSR fast path.** When a `CsrAdjacency` is available (pre-built compressed sparse row), neighbors are read from contiguous slices. Edge labels are stored inline in `CsrNeighbor`, so simple label matching avoids `graph.get_edge()` entirely.
- **Adjacency list fallback.** Uses the `ImblMap<NodeId, Vec<EdgeId>>` adjacency index. Each `EdgeId` requires a `graph.get_edge()` call for label checking.

Direction handling:

- `Out` -- uses outgoing adjacency.
- `In` -- uses incoming adjacency (or lazy incoming CSR).
- `Any` -- checks both directions, deduplicating if the same edge appears in both.

```
crates/selene-gql/src/pattern/expand.rs
```

### VarExpand

Variable-length path expansion for quantified patterns like `-[:contains]->{1,5}`.

Uses BFS with a parent-pointer arena to avoid per-hop path cloning. Each `BfsEntry` is 24 bytes (node, edge, parent index, depth). Paths are reconstructed only when emitting results, by walking parent pointers.

Path mode semantics:

- **TRAIL** (default for quantified patterns): No repeated edges. Uses `imbl::HashSet` for O(log n) structural sharing of visited-edge sets across BFS branches.
- **ACYCLIC**: No repeated nodes (except start/end). Checks via parent-pointer walk -- O(depth).
- **SIMPLE**: No repeated nodes at all. Strictest mode.

Target label optimization: when the target node pattern has labels, VarExpand prunes BFS branches early if the frontier node's labels do not match. This avoids expanding irrelevant subtrees.

Shortest path: `ANY SHORTEST` emits only one path at minimum depth. `ALL SHORTEST` emits all paths at minimum depth. Both terminate BFS at the first depth where results are found.

```
crates/selene-gql/src/pattern/varlength.rs
```

### Join

Joins results from comma-separated patterns (independent graph pattern components within a single MATCH).

When patterns share variables, Join builds a hash index on the right side keyed by shared variable values, then probes with the left side. This is a standard hash equi-join.

When no variables are shared, Join produces a cartesian product (capped at 100,000 results to prevent runaway queries).

```
crates/selene-gql/src/pattern/join.rs
```

## Pipeline Executor

After pattern matching produces `Vec<Binding>`, the pipeline executor applies transforms in sequence. Each `PipelineOp` takes bindings and produces bindings.

```
crates/selene-gql/src/pipeline/stages.rs
```

### Streaming Stages

These stages process one binding at a time without materializing the full result:

- **LET:** Evaluates expressions and binds results to new variables. Extends each binding with new `(var, Scalar(value))` entries.
- **FILTER:** Evaluates a predicate using three-valued logic (Trilean). Only `TRUE` rows pass -- `FALSE` and `UNKNOWN` (NULL) are filtered out.
- **OFFSET:** Skips the first N bindings.
- **LIMIT:** Takes at most N bindings.
- **FOR:** Unwinding -- for each binding, evaluates the list expression and emits one binding per element.

### Pipeline Breakers

These stages must materialize all input before producing output:

- **Sort:** Full sort by ORDER BY terms. Supports multi-key sorting with ASC/DESC and NULLS FIRST/LAST per key.
- **TopK:** Fused Sort + Limit using a bounded binary heap. O(N log K) instead of O(N log N). Produced by the `TopKRule` optimizer.
- **Return with GROUP BY:** Groups bindings by GROUP BY keys, evaluates aggregates per group, applies HAVING filter, then projects. DISTINCT deduplicates the final result.

### Three-Valued Logic (Trilean)

GQL uses three-valued logic for NULL handling:

```
TRUE  AND UNKNOWN = UNKNOWN
FALSE AND UNKNOWN = FALSE
TRUE  OR  UNKNOWN = TRUE
FALSE OR  UNKNOWN = UNKNOWN
NOT UNKNOWN       = UNKNOWN
```

FILTER passes only `TRUE` rows. `FALSE` and `UNKNOWN` are both filtered out. This means `FILTER x > 5` excludes rows where `x` is NULL, matching SQL/GQL semantics.

DISTINCT and GROUP BY use "distinctness" rather than equality: NULL is NOT distinct from NULL. Two NULLs are grouped together.

```
crates/selene-gql/src/types/trilean.rs
```

## EvalContext

`EvalContext` carries the evaluation environment for expression evaluation:

- A reference to the `SeleneGraph` snapshot (for property lookups from lazy NodeId/EdgeId references).
- The `FunctionRegistry` (101 scalar functions: string, math, temporal, type conversion, etc.).
- Optional parameters map, temporal resolver, auth scope bitmap, and execution options.
- Expression evaluation depth counter (guards against stack overflow at depth 128).

```
crates/selene-gql/src/runtime/eval.rs
crates/selene-gql/src/runtime/mod.rs
```

## Binding Type

`Binding` is the row type during GQL execution. It maps variable names (`IStr`) to `BoundValue` variants:

```rust
enum BoundValue {
    Node(NodeId),       // lazy reference -- properties resolved on access
    Edge(EdgeId),       // lazy reference
    Scalar(GqlValue),   // computed value from LET, aggregation, or literal
    Path(GqlPath),      // path from variable-length patterns
    Group(Vec<EdgeId>), // group list for horizontal aggregation
}
```

Variables are stored in a sorted `SmallVec<[(IStr, BoundValue); 8]>`. Sorted order enables binary-search lookup. `SmallVec<8>` avoids heap allocation for the common case (most GQL queries bind fewer than 8 variables).

### Lazy References

The lazy design is critical for performance. During pattern matching, the executor creates potentially thousands of bindings. If each binding eagerly cloned the node's `PropertyMap`, the cost would be proportional to total_bindings * avg_property_count. Instead, bindings store only the `NodeId` (8 bytes). Properties are resolved from the graph snapshot only when a pipeline stage (FILTER, LET, RETURN) accesses them.

This means nodes that get filtered out never pay the cost of property resolution.

```
crates/selene-gql/src/types/binding.rs
```

## Plan Cache

The plan cache stores parsed `Arc<GqlStatement>` values keyed by query text hash. This avoids re-parsing identical queries across repeated executions.

Cache characteristics:

- **Capacity:** 256 entries with LRU eviction (O(n) scan at this small capacity -- a linked-list LRU is not worth the complexity).
- **Invalidation:** The entire cache clears when the graph's generation counter changes (schema modification). This ensures cached ASTs do not reference stale type information.
- **Scope-independent:** Auth scope bitmaps are applied at execution time, not cached. The same parsed AST serves queries from different auth contexts.

### CALL Fast-Parse Path

Standalone `CALL name(args) YIELD cols` queries bypass both the PEG parser and the cache entirely. A hand-written parser in `try_fast_parse_call()` handles literal arguments (integers, floats, strings, booleans, NULL) and YIELD clauses in ~500 ns -- roughly 10-20x faster than the PEG parser (~10 us).

The fast path falls through to the PEG parser for anything more complex: variable references in arguments, MATCH + CALL pipelines, or expressions.

The produced AST is identical to what the PEG parser would produce -- test `call_fast_parse_matches_peg_parser` verifies this.

```
crates/selene-gql/src/runtime/cache.rs
```

## Mutation Path

### Mutation Flow

Mutations follow this sequence:

1. **Parse** the GQL text into a `GqlStatement::Mutate(MutationPipeline)`.
2. **Plan** the mutation: the optional MATCH clause becomes pattern + pipeline ops, the mutation ops are carried through.
3. **Execute pattern** against a snapshot to find target bindings (for SET/DELETE/REMOVE mutations that need targets from MATCH).
4. **Evaluate mutations** against the pre-mutation snapshot. For SET operations, expressions are evaluated before any writes occur. This enables two-phase SET semantics -- `SET a.val = b.val, b.val = a.val` swaps correctly because both reads happen before either write.
5. **Apply mutations** inside a `SharedGraph::write()` call (auto-commit) or within a `TransactionHandle` (explicit transaction). All mutations execute atomically through `TrackedMutation`.
6. **Collect changes** from `TrackedMutation::commit()` for WAL persistence.
7. **Execute RETURN** if present, projecting the post-mutation state.

### Auto-Commit vs Explicit Transactions

- `MutationBuilder::new(gql).execute(&shared)` wraps all mutations in a single `SharedGraph::write()`. Atomic: all succeed or all roll back.
- `MutationBuilder::new(gql).execute_in_transaction(&mut txn)` runs mutations within an existing `TransactionHandle`. The caller controls commit/rollback scope across multiple statements.

### Dictionary Encoding on Write

When a property's schema has `dictionary: true`, the mutation executor checks string values and promotes them to `Value::InternedStr(IStr)` before writing. This happens in `maybe_intern_value()` and applies to both node and edge schemas, for both INSERT and SET operations.

### TrackedMutation Integration

Every graph write goes through `TrackedMutation`, which records `Change` events (for WAL and changelog subscribers) and `RollbackEntry` values (for undo on failure). This is the same mechanism described in the Architecture Overview -- the GQL mutation executor builds on this primitive rather than implementing its own change tracking.

```
crates/selene-gql/src/runtime/execute/mutation.rs
```
