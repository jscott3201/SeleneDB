//! Benchmarks for selene-gql: GQL query engine performance baseline.
//!
//! Covers: parsing, pattern matching, pipeline, end-to-end queries,
//! mutations, predicates, advanced pipeline, vector search, and caching.
//!
//! Run: cargo bench -p selene-gql

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use selene_core::geometry::GeometryValue;
use selene_core::schema::{NodeSchema, PropertyDef, ValueType};
use selene_core::{IStr, LabelSet, NodeId, PropertyMap, Value};
use selene_graph::{SeleneGraph, SharedGraph};
use selene_ts::{HotTier, TimeSample, TsConfig, WarmTierConfig};

use selene_gql::{GqlOptions, MutationBuilder, QueryBuilder};
use selene_testing::bench_profiles::bench_profile;
use selene_testing::bench_scaling::build_scaled_graph;

// ── Configuration ──────────────────────────────────────────────────

fn profile_criterion() -> Criterion {
    bench_profile().into_criterion()
}

fn profile_scales() -> Vec<u64> {
    bench_profile().scales().to_vec()
}

/// Build a graph with `n` sensor nodes, each having a 384-dim random vector property.
///
/// Uses deterministic pseudo-random vectors (seeded by node index) for reproducibility.
/// No external model needed — vectors are pre-computed.
fn build_vector_graph(n: u64) -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let prop = IStr::new("embedding");

    for i in 0..n {
        let mut m = g.mutate();
        // Deterministic pseudo-random 384-dim vector (seeded by index)
        let vec_data: Vec<f32> = (0..384)
            .map(|d| {
                let seed = (i as f32 + 1.0) * (d as f32 + 1.0);
                (seed * 0.618_034).fract() * 2.0 - 1.0 // [-1, 1] range
            })
            .collect();
        let mut props = PropertyMap::new();
        props.insert(IStr::new("name"), Value::String(format!("vec-{i}").into()));
        props.insert(prop, Value::Vector(Arc::from(vec_data)));
        m.create_node(LabelSet::from_strs(&["sensor"]), props)
            .unwrap();
        m.commit(0).unwrap();
    }
    g
}

// ═══════════════════════════════════════════════════════════════════
// Benchmark Groups
// ═══════════════════════════════════════════════════════════════════

// ── Parsing ────────────────────────────────────────────────────────

fn bench_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_parse");

    let queries = [
        ("simple", "MATCH (n) RETURN n"),
        ("labeled", "MATCH (s:sensor) RETURN s.name"),
        (
            "filter",
            "MATCH (s:sensor) FILTER s.temp > 72 RETURN s.name, s.temp",
        ),
        (
            "complex",
            "MATCH (b:building)-[:contains]->(f:floor)-[:contains]->(s:sensor) \
             LET building_name = b.name \
             FILTER s.temp > 72 \
             RETURN building_name, s.name, s.temp \
             ORDER BY s.temp DESC \
             LIMIT 10",
        ),
        (
            "var_length",
            "MATCH TRAIL (b:building)-[:contains]->{1,5}(s:sensor) \
             RETURN b.name, s.name",
        ),
        // Phase 0 syntax
        (
            "like_between",
            "MATCH (s:sensor) \
             FILTER s.name LIKE 'Temp%' AND s.accuracy BETWEEN 0.1 AND 0.9 \
             RETURN s.name",
        ),
        (
            "different_edges",
            "MATCH DIFFERENT EDGES (a:ahu)-[:feeds]->(v:vav), (a)-[:feeds]->(v2:vav) \
             RETURN a.name, v.name, v2.name",
        ),
        (
            "quantifier_shortcuts",
            "MATCH (b:building)-[:contains]->{1,5}(s:sensor) \
             RETURN count(*) AS n",
        ),
        (
            "unwind",
            "UNWIND [1, 2, 3] AS x MATCH (s:sensor) RETURN x, count(*) AS n GROUP BY x",
        ),
        (
            "type_ddl",
            "CREATE NODE TYPE :equipment (name :: STRING NOT NULL, status :: STRING DEFAULT 'active')",
        ),
    ];

    for (name, query) in &queries {
        group.bench_function(*name, |b| {
            b.iter(|| selene_gql::parse_statement(query).unwrap());
        });
    }

    group.finish();
}

// ── Pattern Matching ───────────────────────────────────────────────

fn bench_label_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_label_scan");

    for &n in &profile_scales() {
        let g = build_scaled_graph(n);
        group.throughput(Throughput::Elements(n));

        group.bench_with_input(BenchmarkId::new("all_sensors", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new("MATCH (s:sensor) RETURN count(*) AS n", g)
                    .execute()
                    .unwrap()
            });
        });

        group.bench_with_input(BenchmarkId::new("all_nodes", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new("MATCH (n) RETURN count(*) AS n", g)
                    .execute()
                    .unwrap()
            });
        });
    }

    group.finish();
}

fn bench_expand(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_expand");

    for &n in &profile_scales() {
        let g = build_scaled_graph(n);
        group.throughput(Throughput::Elements(n));

        group.bench_with_input(BenchmarkId::new("single_hop", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (b:building)-[:contains]->(f:floor) RETURN b.name, f.name",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        group.bench_with_input(BenchmarkId::new("two_hop", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (b:building)-[:contains]->(f:floor)-[:contains]->(s:sensor) \
                     RETURN b.name, s.name",
                    g,
                )
                .execute()
                .unwrap()
            });
        });
    }

    group.finish();
}

fn bench_var_expand(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_var_expand");

    for &n in &profile_scales() {
        let g = build_scaled_graph(n);
        group.throughput(Throughput::Elements(n));

        group.bench_with_input(BenchmarkId::new("depth_1_3", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (b:building)-[:contains]->{1,3}(s:sensor) \
                     RETURN count(*) AS n",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        group.bench_with_input(BenchmarkId::new("trail_depth_1_5", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH TRAIL (b:building)-[:contains]->{1,5}(s) \
                     RETURN count(*) AS n",
                    g,
                )
                .execute()
                .unwrap()
            });
        });
    }

    group.finish();
}

// ── Pipeline ───────────────────────────────────────────────────────

fn bench_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_filter");

    for &n in &profile_scales() {
        let g = build_scaled_graph(n);
        group.throughput(Throughput::Elements(n));

        // Both filters use the same property (accuracy: Float) on the same nodes
        // (temperature_sensor) for an apples-to-apples comparison of range vs equality.
        group.bench_with_input(BenchmarkId::new("property_gt", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (s:temperature_sensor) FILTER s.accuracy > 0.3 RETURN count(*) AS n",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        group.bench_with_input(BenchmarkId::new("property_eq", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (s:temperature_sensor) FILTER s.accuracy = 0.5 RETURN count(*) AS n",
                    g,
                )
                .execute()
                .unwrap()
            });
        });
    }

    group.finish();
}

fn bench_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_sort");

    for &n in &profile_scales() {
        let g = build_scaled_graph(n);
        group.throughput(Throughput::Elements(n));

        group.bench_with_input(BenchmarkId::new("order_by_property", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (s:sensor) RETURN s.name, s.temp ORDER BY s.temp DESC",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        group.bench_with_input(BenchmarkId::new("order_by_limit", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (s:sensor) RETURN s.name, s.temp ORDER BY s.temp DESC LIMIT 10",
                    g,
                )
                .execute()
                .unwrap()
            });
        });
    }

    group.finish();
}

fn bench_aggregation(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_aggregation");

    for &n in &profile_scales() {
        let g = build_scaled_graph(n);
        group.throughput(Throughput::Elements(n));

        group.bench_with_input(BenchmarkId::new("count_star", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new("MATCH (s:sensor) RETURN count(*) AS n", g)
                    .execute()
                    .unwrap()
            });
        });

        group.bench_with_input(BenchmarkId::new("avg", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new("MATCH (s:sensor) RETURN avg(s.temp) AS avg_temp", g)
                    .execute()
                    .unwrap()
            });
        });

        group.bench_with_input(BenchmarkId::new("group_by", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (s:sensor) \
                     LET floor = s.floor_id \
                     RETURN floor, count(*) AS n, avg(s.temp) AS avg_temp \
                     GROUP BY floor",
                    g,
                )
                .execute()
                .unwrap()
            });
        });
    }

    group.finish();
}

// ── Predicates (Phase 0) ─────────────────────────────────────────

fn bench_predicates(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_predicates");

    for &n in &profile_scales() {
        let g = build_scaled_graph(n);
        group.throughput(Throughput::Elements(n));

        // LIKE — regex-based pattern matching with thread-local cache
        // Sensor names follow pattern: B{n}-Temp-{n}, B{n}-Hum-{n}, B{n}-CO2-{n}
        group.bench_with_input(BenchmarkId::new("like_prefix", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (s:sensor) FILTER s.name LIKE 'B1-%' RETURN count(*) AS n",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        group.bench_with_input(BenchmarkId::new("like_contains", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (s:sensor) FILTER s.name LIKE '%Temp%' RETURN count(*) AS n",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        // NOT LIKE
        group.bench_with_input(BenchmarkId::new("not_like", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (s:sensor) FILTER s.name NOT LIKE '%CO2%' RETURN count(*) AS n",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        // BETWEEN — inclusive range comparison on float property
        group.bench_with_input(BenchmarkId::new("between_float", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (s:temperature_sensor) \
                     FILTER s.accuracy BETWEEN 0.2 AND 0.8 \
                     RETURN count(*) AS n",
                    g,
                )
                .execute()
                .unwrap()
            });
        });
    }

    group.finish();
}

// ── Advanced Pipeline (Phase 0) ──────────────────────────────────

fn bench_advanced_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_advanced_pipeline");

    for &n in &profile_scales() {
        let g = build_scaled_graph(n);
        group.throughput(Throughput::Elements(n));

        // WITH clause — intermediate projection with downstream filter
        // Uses s.accuracy (Float, 0.5 on temperature_sensor nodes) — a real property
        group.bench_with_input(BenchmarkId::new("with_clause", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (b:building)-[:contains]->(f:floor)-[:contains]->(s:temperature_sensor) \
                     WITH b.name AS bname, s.accuracy AS acc \
                     FILTER acc > 0.3 \
                     RETURN bname, acc",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        // HAVING — post-aggregation filter
        // GROUP BY requires variable names (not expressions), so use LET binding
        group.bench_with_input(BenchmarkId::new("having", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (b:building)-[:contains]->(f:floor)-[:contains]->(s:sensor) \
                     LET bname = b.name \
                     RETURN bname, count(*) AS sensor_count \
                     GROUP BY bname \
                     HAVING count(*) > 5",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        // UNWIND — explode a collected list
        // Uses s.accuracy (Float, 0.5 on temperature_sensor) so filter hits real data
        group.bench_with_input(BenchmarkId::new("unwind", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "UNWIND [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0] AS threshold \
                     MATCH (s:temperature_sensor) \
                     FILTER s.accuracy > threshold \
                     RETURN threshold, count(*) AS n \
                     GROUP BY threshold",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        // NULLS FIRST — null sort ordering
        // calibration_date exists only on some humidity_sensor nodes; most rows are NULL,
        // which is ideal for exercising the NULLS FIRST/LAST sort comparator.
        group.bench_with_input(BenchmarkId::new("nulls_first", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (s:sensor) \
                     RETURN s.name, s.calibration_date \
                     ORDER BY s.calibration_date ASC NULLS FIRST \
                     LIMIT 20",
                    g,
                )
                .execute()
                .unwrap()
            });
        });
    }

    group.finish();
}

// ── End-to-End ─────────────────────────────────────────────────────

fn bench_e2e(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_e2e");

    for &n in &profile_scales() {
        let g = build_scaled_graph(n);
        group.throughput(Throughput::Elements(n));

        group.bench_with_input(BenchmarkId::new("simple_return", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new("MATCH (s:sensor) RETURN s.name AS name", g)
                    .execute()
                    .unwrap()
            });
        });

        group.bench_with_input(BenchmarkId::new("filter_sort_limit", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (s:sensor) \
                     FILTER s.temp > 72 \
                     RETURN s.name, s.temp \
                     ORDER BY s.temp DESC \
                     LIMIT 10",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        group.bench_with_input(BenchmarkId::new("two_hop_with_filter", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (b:building)-[:contains]->(f:floor)-[:contains]->(s:sensor) \
                     FILTER s.temp > 75 \
                     RETURN b.name, f.name, s.name, s.temp",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        group.bench_with_input(BenchmarkId::new("inline_properties", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (s:sensor {unit: '°F'}) \
                     RETURN s.name, s.temp",
                    g,
                )
                .execute()
                .unwrap()
            });
        });
    }

    group.finish();
}

// ── Mutations ──────────────────────────────────────────────────────

fn bench_mutations(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_mutations");

    group.bench_function("insert_node", |b| {
        let shared = SharedGraph::new(SeleneGraph::new());
        b.iter(|| {
            MutationBuilder::new("INSERT (:sensor {name: 'BenchSensor', temp: 72.5})")
                .execute(&shared)
                .unwrap()
        });
    });

    group.bench_function("insert_and_read", |b| {
        b.iter_custom(|iters| {
            let shared = SharedGraph::new(SeleneGraph::new());
            let start = std::time::Instant::now();
            for _ in 0..iters {
                MutationBuilder::new("INSERT (:sensor {name: 'S', temp: 72.5})")
                    .execute(&shared)
                    .unwrap();
            }
            // Read back
            let snapshot = shared.load_snapshot();
            QueryBuilder::new("MATCH (s:sensor) RETURN count(*) AS n", &snapshot)
                .execute()
                .unwrap();
            start.elapsed()
        });
    });

    // SET — property update on existing nodes
    // Sensor names follow pattern: B{n}-Temp-{n} (e.g., "B1-Temp-1")
    group.bench_function("set_property", |b| {
        let shared = SharedGraph::new(build_scaled_graph(1_000));
        b.iter(|| {
            MutationBuilder::new(
                "MATCH (s:sensor) \
                 FILTER s.name = 'B1-Temp-1' \
                 SET s.accuracy = 0.99 \
                 RETURN s.name",
            )
            .execute(&shared)
            .unwrap()
        });
    });

    // DETACH DELETE — remove node and cascade incident edges
    // No LIMIT in mutation pipeline grammar, so insert one and delete one per iter
    group.bench_function("detach_delete", |b| {
        let shared = SharedGraph::new(SeleneGraph::new());
        b.iter(|| {
            MutationBuilder::new("INSERT (:disposable {x: 1})")
                .execute(&shared)
                .unwrap();
            MutationBuilder::new("MATCH (d:disposable) DETACH DELETE d")
                .execute(&shared)
                .unwrap();
        });
    });

    group.finish();
}

// ── Transactions ──────────────────────────────────────────────────

fn bench_transactions(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_transactions");

    // Multi-statement explicit transaction: insert + update + read
    // TransactionHandle::commit() returns Vec<Change> (not Result), no .unwrap() needed
    group.bench_function("multi_statement_txn", |b| {
        let shared = SharedGraph::new(SeleneGraph::new());
        b.iter(|| {
            let mut txn = shared.begin_transaction();
            MutationBuilder::new("INSERT (:sensor {name: 'TxnSensor', accuracy: 0.5})")
                .execute_in_transaction(&mut txn)
                .unwrap();
            MutationBuilder::new(
                "MATCH (s:sensor {name: 'TxnSensor'}) SET s.accuracy = 0.99 RETURN s.accuracy",
            )
            .execute_in_transaction(&mut txn)
            .unwrap();
            let _changes = txn.commit();
        });
    });

    // Transaction rollback cost — Drop impl handles rollback (no explicit method)
    group.bench_function("txn_rollback", |b| {
        let shared = SharedGraph::new(SeleneGraph::new());
        b.iter(|| {
            let mut txn = shared.begin_transaction();
            MutationBuilder::new("INSERT (:sensor {name: 'WillRollback', accuracy: 0.0})")
                .execute_in_transaction(&mut txn)
                .unwrap();
            drop(txn); // Rollback via Drop — no explicit rollback() method
        });
    });

    group.finish();
}

// ── Plan Cache ─────────────────────────────────────────────────────

fn bench_plan_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_plan_cache");
    let g = build_scaled_graph(1000);

    group.bench_function("cache_miss_parse", |b| {
        let cache = selene_gql::PlanCache::new();
        b.iter(|| {
            cache.clear();
            cache
                .get_or_parse("MATCH (s:sensor) RETURN s.name", g.generation())
                .unwrap()
        });
    });

    group.bench_function("cache_hit", |b| {
        let cache = selene_gql::PlanCache::new();
        cache
            .get_or_parse("MATCH (s:sensor) RETURN s.name", g.generation())
            .unwrap();
        b.iter(|| {
            cache
                .get_or_parse("MATCH (s:sensor) RETURN s.name", g.generation())
                .unwrap()
        });
    });

    group.finish();
}

// ── Phase 5: Cyclic Patterns, RPQ, VALUE/COLLECT ──────────────────

fn bench_phase5(c: &mut Criterion) {
    let p = bench_profile();
    let scales = p.scales().to_vec();
    let mut group = c.benchmark_group("gql_phase5");

    for &n in &scales {
        let g = build_scaled_graph(n);
        group.throughput(Throughput::Elements(g.node_count() as u64));

        // CycleJoin (Phase 5C) — HVAC cycle: zone→ahu→vav→zone
        group.bench_with_input(BenchmarkId::new("cyclejoin_hvac", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (z:zone)-[:returns_to]->(a:ahu)-[:feeds]->(v:vav)-[:serves]->(z) \
                     RETURN z.name, a.name, v.name",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        // RPQ (Phase 5D) — var-length containment path
        group.bench_with_input(BenchmarkId::new("rpq_containment", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (c:campus)-[:contains]->{1,5}(s:sensor) \
                     RETURN count(*) AS n",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        // RPQ alternation — crosses containment + HVAC overlays
        group.bench_with_input(BenchmarkId::new("rpq_alternation", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (b:building)-[:contains|feeds]->{1,4}(n) \
                     RETURN count(*) AS n",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        // VALUE/COLLECT (Phase 5E)
        group.bench_with_input(BenchmarkId::new("collect_zones", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (b:building)-[:contains]->(f:floor)-[:contains]->(z:zone) \
                     RETURN b.name, COLLECT_LIST(z.name) AS zones",
                    g,
                )
                .execute()
                .unwrap()
            });
        });
    }

    group.finish();
}

// ── Subqueries and Match Modes (Phase 0) ─────────────────────────

fn bench_subqueries(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_subqueries");

    for &n in &profile_scales() {
        let g = build_scaled_graph(n);
        group.throughput(Throughput::Elements(n));

        // COUNT subquery — correlated, executed per outer row
        group.bench_with_input(BenchmarkId::new("count_subquery", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (b:building) \
                     FILTER COUNT { MATCH (b)-[:contains]->(f:floor) } > 2 \
                     RETURN b.name",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        // DIFFERENT EDGES — prevent edge reuse across patterns
        group.bench_with_input(BenchmarkId::new("different_edges", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH DIFFERENT EDGES \
                     (a:ahu)-[:feeds]->(v1:vav), \
                     (a)-[:feeds]->(v2:vav) \
                     FILTER v1 <> v2 \
                     RETURN a.name, v1.name, v2.name",
                    g,
                )
                .execute()
                .unwrap()
            });
        });
    }

    group.finish();
}

// ── Execution Options ────────────────────────────────────────────

fn bench_options(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_options");

    let g = build_scaled_graph(1_000);

    // Strict coercion mode — measures overhead of type checking
    // Uses s.accuracy (Float, 0.5 on temperature_sensor) — a real property
    group.bench_function("strict_coercion", |b| {
        let opts = GqlOptions {
            strict_coercion: true,
            ..Default::default()
        };
        b.iter(|| {
            QueryBuilder::new(
                "MATCH (s:temperature_sensor) FILTER s.accuracy > 0.3 RETURN s.name, s.accuracy LIMIT 10",
                &g,
            )
            .with_options(&opts)
            .execute()
            .unwrap()
        });
    });

    // Baseline without strict coercion for comparison
    group.bench_function("default_coercion", |b| {
        b.iter(|| {
            QueryBuilder::new(
                "MATCH (s:temperature_sensor) FILTER s.accuracy > 0.3 RETURN s.name, s.accuracy LIMIT 10",
                &g,
            )
            .execute()
            .unwrap()
        });
    });

    group.finish();
}

// ── Factorized Execution ─────────────────────────────────────────

fn bench_factorized(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_factorized");

    let g = build_scaled_graph(1_000);
    let opts_fact = GqlOptions {
        factorized: true,
        ..Default::default()
    };

    // Two-hop: building -> floor (flat vs factorized)
    group.bench_function("two_hop_flat", |b| {
        b.iter(|| {
            QueryBuilder::new(
                "MATCH (a:building)-[:contains]->(b:floor) RETURN a.name, b.name",
                &g,
            )
            .execute()
            .unwrap()
        });
    });

    group.bench_function("two_hop_factorized", |b| {
        b.iter(|| {
            QueryBuilder::new(
                "MATCH (a:building)-[:contains]->(b:floor) RETURN a.name, b.name",
                &g,
            )
            .with_options(&opts_fact)
            .execute()
            .unwrap()
        });
    });

    // Three-hop: building -> floor -> sensor (flat vs factorized)
    group.bench_function("three_hop_flat", |b| {
        b.iter(|| {
            QueryBuilder::new(
                "MATCH (a:building)-[:contains]->(b:floor)-[:contains]->(c:temperature_sensor) RETURN a.name, c.name",
                &g,
            )
            .execute()
            .unwrap()
        });
    });

    group.bench_function("three_hop_factorized", |b| {
        b.iter(|| {
            QueryBuilder::new(
                "MATCH (a:building)-[:contains]->(b:floor)-[:contains]->(c:temperature_sensor) RETURN a.name, c.name",
                &g,
            )
            .with_options(&opts_fact)
            .execute()
            .unwrap()
        });
    });

    // Three-hop with filter (flat vs factorized)
    group.bench_function("three_hop_filter_flat", |b| {
        b.iter(|| {
            QueryBuilder::new(
                "MATCH (a:building)-[:contains]->(b:floor)-[:contains]->(c:temperature_sensor) WHERE c.accuracy > 0.3 RETURN a.name, c.name LIMIT 10",
                &g,
            )
            .execute()
            .unwrap()
        });
    });

    group.bench_function("three_hop_filter_factorized", |b| {
        b.iter(|| {
            QueryBuilder::new(
                "MATCH (a:building)-[:contains]->(b:floor)-[:contains]->(c:temperature_sensor) WHERE c.accuracy > 0.3 RETURN a.name, c.name LIMIT 10",
                &g,
            )
            .with_options(&opts_fact)
            .execute()
            .unwrap()
        });
    });

    group.finish();
}

// ── Vector Search ────────────────────────────────────────────────

fn bench_vector(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_vector");

    // ── Raw math benchmarks (independent of graph size) ──────────
    // These measure the NAIVE baseline. After SIMD optimization,
    // the similarNodes benchmarks below show the end-to-end improvement.
    {
        // 384-dim vectors matching all-MiniLM-L6-v2 embedding size
        let a: Vec<f32> = (0..384)
            .map(|i| (i as f32 * 0.618).fract() * 2.0 - 1.0)
            .collect();
        let b: Vec<f32> = (0..384)
            .map(|i| (i as f32 * 0.317).fract() * 2.0 - 1.0)
            .collect();

        group.bench_function("dot_product_384_naive", |bench| {
            bench.iter(|| {
                let dot: f32 = a.iter().zip(&b).map(|(x, y)| x * y).sum();
                std::hint::black_box(dot)
            });
        });

        group.bench_function("cosine_similarity_384_naive", |bench| {
            bench.iter(|| {
                let dot: f32 = a.iter().zip(&b).map(|(x, y)| x * y).sum();
                let mag_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
                let mag_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
                std::hint::black_box(dot / (mag_a * mag_b))
            });
        });
    }

    // Fixed vector scales for meaningful comparison (separate from graph scales)
    let vector_scales: Vec<u64> = vec![1_000, 10_000];

    for &n in &vector_scales {
        let g = build_vector_graph(n);
        group.throughput(Throughput::Elements(n));

        // Raw cosine similarity (scalar function on two node vectors)
        // Uses two separate MATCH clauses to avoid cartesian product
        group.bench_with_input(BenchmarkId::new("cosine_similarity", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (a:sensor {name: 'vec-0'}) \
                     MATCH (b:sensor {name: 'vec-1'}) \
                     RETURN cosine_similarity(a.embedding, b.embedding) AS sim",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        // graph.similarNodes — find top-10 similar to node 1
        // Uses node ID (integer literal) — avoids the CALL parameter limitation
        // with vector types. Exercises the same top_k_cosine_scan hot path.
        group.bench_with_input(BenchmarkId::new("similar_nodes_top10", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "CALL graph.similarNodes(1, 'embedding', 10) YIELD nodeId, score",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        // graph.similarNodes — top-50 (larger k)
        group.bench_with_input(BenchmarkId::new("similar_nodes_top50", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "CALL graph.similarNodes(1, 'embedding', 50) YIELD nodeId, score",
                    g,
                )
                .execute()
                .unwrap()
            });
        });
    }

    group.finish();
}

// ── TS Procedures ─────────────────────────────────────────────────

/// Build a scaled graph with time-series data populated in the hot tier.
///
/// Uses `build_scaled_graph` for the property graph, then appends 100
/// samples per sensor node (1 per minute) to the hot tier. Returns the
/// first sensor `NodeId` for use in benchmark queries.
fn build_graph_with_ts(n: u64) -> (SeleneGraph, HotTier, NodeId) {
    let g = build_scaled_graph(n);
    let config = TsConfig {
        warm_tier: Some(WarmTierConfig {
            downsample_interval_secs: 60,
            retention_hours: 24,
            ddsketch_enabled: true,
            hourly: None,
        }),
        ..TsConfig::default()
    };
    let hot = HotTier::new(config);

    let now = selene_core::now_nanos();
    let sensor_bitmap = g
        .label_bitmap("sensor")
        .expect("graph must have :sensor nodes");
    let mut first_sensor = NodeId(0);
    let mut first = true;

    for raw_id in sensor_bitmap {
        let nid = NodeId(u64::from(raw_id));
        if first {
            first_sensor = nid;
            first = false;
        }
        for j in 0..100i64 {
            hot.append(
                nid,
                "temperature",
                TimeSample {
                    timestamp_nanos: now - (100 - j) * 60_000_000_000,
                    value: 72.0 + (j as f64 * 0.1).sin() * 3.0,
                },
            );
        }
    }

    (g, hot, first_sensor)
}

fn bench_ts_procedures(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_ts_procedures");

    for &n in &profile_scales() {
        let (g, hot, first_sensor) = build_graph_with_ts(n);
        let sid = first_sensor.0;
        group.throughput(Throughput::Elements(n));

        // ts.latest -- O(1) last-value cache lookup
        let q_latest = format!("CALL ts.latest({sid}, 'temperature') YIELD timestamp, value");
        group.bench_with_input(
            BenchmarkId::new("ts_latest", n),
            &(&g, &hot),
            |b, (g, hot)| {
                b.iter(|| {
                    QueryBuilder::new(&q_latest, g)
                        .with_hot_tier(hot)
                        .execute()
                        .unwrap()
                });
            },
        );

        // ts.range -- scan samples within absolute time range
        let q_range = format!(
            "CALL ts.range({sid}, 'temperature', 0, 9223372036854775000) YIELD timestamp, value"
        );
        group.bench_with_input(
            BenchmarkId::new("ts_range", n),
            &(&g, &hot),
            |b, (g, hot)| {
                b.iter(|| {
                    QueryBuilder::new(&q_range, g)
                        .with_hot_tier(hot)
                        .execute()
                        .unwrap()
                });
            },
        );

        // ts.aggregate -- scalar aggregate over a 1-hour window
        let q_agg =
            format!("CALL ts.aggregate({sid}, 'temperature', 3600000000000, 'avg') YIELD value");
        group.bench_with_input(
            BenchmarkId::new("ts_aggregate", n),
            &(&g, &hot),
            |b, (g, hot)| {
                b.iter(|| {
                    QueryBuilder::new(&q_agg, g)
                        .with_hot_tier(hot)
                        .execute()
                        .unwrap()
                });
            },
        );

        // ts.scopedAggregate -- BFS from building root + aggregate descendants
        // Node 2 is the first building in the reference building model.
        let q_scoped = "CALL ts.scopedAggregate(2, 5, 'temperature', 'avg', 3600000000000) \
             YIELD value, nodeCount, sampleCount"
            .to_string();
        group.bench_with_input(
            BenchmarkId::new("ts_scoped_aggregate", n),
            &(&g, &hot),
            |b, (g, hot)| {
                b.iter(|| {
                    QueryBuilder::new(&q_scoped, g)
                        .with_hot_tier(hot)
                        .execute()
                        .unwrap()
                });
            },
        );

        // ts.anomalies -- Z-score per sample against mean/stddev
        let q_anomalies = format!(
            "CALL ts.anomalies({sid}, 'temperature', 2.0, 3600000000000) \
             YIELD timestamp, value, z_score"
        );
        group.bench_with_input(
            BenchmarkId::new("ts_anomalies", n),
            &(&g, &hot),
            |b, (g, hot)| {
                b.iter(|| {
                    QueryBuilder::new(&q_anomalies, g)
                        .with_hot_tier(hot)
                        .execute()
                        .unwrap()
                });
            },
        );

        // ts.peerAnomalies -- BFS neighborhood comparison
        let q_peer = format!(
            "CALL ts.peerAnomalies({sid}, 'temperature', 2, 2.0) \
             YIELD nodeId, value, z_score"
        );
        group.bench_with_input(
            BenchmarkId::new("ts_peer_anomalies", n),
            &(&g, &hot),
            |b, (g, hot)| {
                b.iter(|| {
                    QueryBuilder::new(&q_peer, g)
                        .with_hot_tier(hot)
                        .execute()
                        .unwrap()
                });
            },
        );
    }

    group.finish();
}

// ── DISTINCT & OFFSET ───────────────────────────────────────────

fn bench_distinct_offset(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_distinct_offset");
    for &scale in &profile_scales() {
        let graph = build_scaled_graph(scale);
        group.throughput(Throughput::Elements(scale));

        group.bench_with_input(BenchmarkId::new("distinct_unit", scale), &graph, |b, g| {
            b.iter(|| {
                QueryBuilder::new("MATCH (s:sensor) RETURN DISTINCT s.unit", g)
                    .execute()
                    .unwrap()
            });
        });

        group.bench_with_input(BenchmarkId::new("offset_skip_50", scale), &graph, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (s:sensor) RETURN s.name ORDER BY s.name OFFSET 50 LIMIT 10",
                    g,
                )
                .execute()
                .unwrap()
            });
        });
    }
    group.finish();
}

// ── Dictionary Encoding ─────────────────────────────────────────

/// Build a graph with a schema that has `dictionary: true` on a property.
/// Returns a SharedGraph ready for mutation benchmarks.
fn build_dictionary_graph() -> SharedGraph {
    let mut g = SeleneGraph::new();
    g.schema_mut()
        .register_node_schema(
            NodeSchema::builder("device")
                .property(PropertyDef::simple("name", ValueType::String, true))
                .property(
                    PropertyDef::builder("status", ValueType::String)
                        .dictionary()
                        .build(),
                )
                .build(),
        )
        .unwrap();
    SharedGraph::new(g)
}

fn bench_dictionary(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_dictionary");

    // INSERT with dictionary-encoded property
    group.bench_function("insert_dict_property", |b| {
        let shared = build_dictionary_graph();
        b.iter(|| {
            MutationBuilder::new("INSERT (:device {name: 'D1', status: 'active'})")
                .execute(&shared)
                .unwrap()
        });
    });

    // INSERT without dictionary (baseline for comparison)
    group.bench_function("insert_regular_property", |b| {
        let shared = SharedGraph::new(SeleneGraph::new());
        b.iter(|| {
            MutationBuilder::new("INSERT (:device {name: 'D1', status: 'active'})")
                .execute(&shared)
                .unwrap()
        });
    });

    // SET on dictionary-encoded property
    group.bench_function("set_dict_property", |b| {
        let shared = build_dictionary_graph();
        MutationBuilder::new("INSERT (:device {name: 'Target', status: 'idle'})")
            .execute(&shared)
            .unwrap();
        b.iter(|| {
            MutationBuilder::new(
                "MATCH (d:device {name: 'Target'}) SET d.status = 'active' RETURN d.name",
            )
            .execute(&shared)
            .unwrap()
        });
    });

    // SET without dictionary (baseline)
    group.bench_function("set_regular_property", |b| {
        let shared = SharedGraph::new(SeleneGraph::new());
        MutationBuilder::new("INSERT (:device {name: 'Target', status: 'idle'})")
            .execute(&shared)
            .unwrap();
        b.iter(|| {
            MutationBuilder::new(
                "MATCH (d:device {name: 'Target'}) SET d.status = 'active' RETURN d.name",
            )
            .execute(&shared)
            .unwrap()
        });
    });

    group.finish();
}

// ── Spatial ───────────────────────────────────────────────────────

/// Build a fixture of N sensor points scattered across a 0.5°×0.5° WGS84
/// box around NYC, plus `polygon_count` square zone polygons tiling the
/// same region. Deterministic for reproducibility.
fn build_spatial_graph(n_points: u64, polygon_count: usize) -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();

    // Anchor point: lower-Manhattan-ish.
    let lng0 = -74.05_f64;
    let lat0 = 40.60_f64;
    let span = 0.5_f64;

    // Reference building (anchored to one corner).
    m.create_node(
        LabelSet::from_strs(&["building"]),
        PropertyMap::from_pairs(vec![
            ("name".into(), Value::str("HQ")),
            (
                "location".into(),
                Value::geometry(GeometryValue::point_wgs84(lng0 + 0.25, lat0 + 0.25)),
            ),
        ]),
    )
    .unwrap();

    // Sensor points scattered deterministically.
    for i in 0..n_points {
        let fx = (i as f64 * 0.618_034).fract();
        let fy = ((i as f64) * 0.414_214).fract();
        let lng = lng0 + fx * span;
        let lat = lat0 + fy * span;
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![
                ("name".into(), Value::String(format!("s-{i}").into())),
                (
                    "location".into(),
                    Value::geometry(GeometryValue::point_wgs84(lng, lat)),
                ),
            ]),
        )
        .unwrap();
    }

    // Square zone polygons tiling the region. Integer grid to avoid
    // floating boundary ambiguity.
    let side = ((polygon_count as f64).sqrt().ceil() as usize).max(1);
    let cell = span / side as f64;
    let mut placed = 0;
    'outer: for iy in 0..side {
        for ix in 0..side {
            if placed >= polygon_count {
                break 'outer;
            }
            let x0 = lng0 + ix as f64 * cell;
            let y0 = lat0 + iy as f64 * cell;
            let x1 = x0 + cell;
            let y1 = y0 + cell;
            let poly = GeometryValue::from_geojson(&format!(
                r#"{{"type":"Polygon","coordinates":[[[{x0},{y0}],[{x1},{y0}],[{x1},{y1}],[{x0},{y1}],[{x0},{y0}]]]}}"#
            ))
            .unwrap();
            m.create_node(
                LabelSet::from_strs(&["zone"]),
                PropertyMap::from_pairs(vec![
                    ("name".into(), Value::String(format!("z-{placed}").into())),
                    ("boundary".into(), Value::geometry(poly)),
                ]),
            )
            .unwrap();
            placed += 1;
        }
    }

    m.commit(0).unwrap();
    g
}

fn bench_spatial(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_spatial");

    for &n in &profile_scales() {
        // Keep polygon count bounded — point-in-polygon is O(points × polygons)
        // without an index, so we'd rather show per-point cost grow linearly
        // than inflate the cartesian cost quadratically.
        let polygon_count = 20;
        let g = build_spatial_graph(n, polygon_count);
        group.throughput(Throughput::Elements(n));

        // Distance sort — pure measurement, single building binding on the
        // left and every sensor on the right.
        group.bench_with_input(BenchmarkId::new("distance_sort", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (b:building), (s:sensor) \
                     RETURN s.name AS name, ST_Distance(b.location, s.location) AS d \
                     ORDER BY d ASC LIMIT 10",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        // Radius filter — the common "sensors within N meters of HQ" shape.
        group.bench_with_input(BenchmarkId::new("dwithin_5km", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (b:building), (s:sensor) \
                     WHERE ST_DWithin(b.location, s.location, 5000.0) \
                     RETURN count(*) AS n",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        // Point-in-polygon over every (zone, sensor) pair.
        group.bench_with_input(BenchmarkId::new("contains_pairs", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (z:zone), (s:sensor) \
                     WHERE ST_Contains(z.boundary, s.location) \
                     RETURN count(*) AS n",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        // Polygon–polygon intersection across zones (bbox pre-filter path).
        group.bench_with_input(BenchmarkId::new("intersects_zones", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (a:zone), (b:zone) \
                     WHERE id(a) < id(b) AND ST_Intersects(a.boundary, b.boundary) \
                     RETURN count(*) AS n",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        // Envelope — cheap per-geometry measurement over all zones.
        group.bench_with_input(BenchmarkId::new("envelope_zones", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new("MATCH (z:zone) RETURN ST_Envelope(z.boundary) AS bb", g)
                    .execute()
                    .unwrap()
            });
        });
    }

    group.finish();
}

// ═══════════════════════════════════════════════════════════════════
// Registration
// ═══════════════════════════════════════════════════════════════════

criterion_group! {
    name = parsing;
    config = profile_criterion();
    targets = bench_parse
}

criterion_group! {
    name = pattern_matching;
    config = profile_criterion();
    targets = bench_label_scan, bench_expand, bench_var_expand
}

criterion_group! {
    name = pipeline;
    config = profile_criterion();
    targets = bench_filter, bench_sort, bench_aggregation
}

criterion_group! {
    name = predicates;
    config = profile_criterion();
    targets = bench_predicates
}

criterion_group! {
    name = advanced_pipeline;
    config = profile_criterion();
    targets = bench_advanced_pipeline
}

criterion_group! {
    name = end_to_end;
    config = profile_criterion();
    targets = bench_e2e
}

criterion_group! {
    name = mutations;
    config = profile_criterion();
    targets = bench_mutations
}

criterion_group! {
    name = transactions;
    config = profile_criterion();
    targets = bench_transactions
}

criterion_group! {
    name = caching;
    config = profile_criterion();
    targets = bench_plan_cache
}

criterion_group! {
    name = subqueries;
    config = profile_criterion();
    targets = bench_subqueries
}

criterion_group! {
    name = options;
    config = profile_criterion();
    targets = bench_options
}

criterion_group! {
    name = phase5;
    config = profile_criterion();
    targets = bench_phase5
}

criterion_group! {
    name = vector;
    config = profile_criterion();
    targets = bench_vector
}

criterion_group! {
    name = ts_procedures;
    config = profile_criterion();
    targets = bench_ts_procedures
}

criterion_group! {
    name = distinct_offset;
    config = profile_criterion();
    targets = bench_distinct_offset
}

criterion_group! {
    name = dictionary;
    config = profile_criterion();
    targets = bench_dictionary
}

// ── Optimizer Validation ──────────────────────────────────────────

/// Build a scaled graph with an indexed `accuracy` property on sensors.
/// This enables the range index and in-list optimizations to fire.
fn build_indexed_graph(target_nodes: u64) -> SeleneGraph {
    let mut g = build_scaled_graph(target_nodes);

    // Register sensor schema with indexed accuracy property
    let schema = NodeSchema::builder("sensor")
        .property(
            PropertyDef::builder("name", ValueType::String)
                .required(true)
                .build(),
        )
        .property(PropertyDef::simple("unit", ValueType::String, false))
        .property(
            PropertyDef::builder("accuracy", ValueType::Float)
                .indexed()
                .build(),
        )
        .property(PropertyDef::simple("install_date", ValueType::Date, false))
        .property(PropertyDef::simple(
            "last_calibrated",
            ValueType::ZonedDateTime,
            false,
        ))
        .build();
    let _ = g.schema_mut().register_node_schema(schema);

    // Also index temperature_sensor accuracy
    let ts_schema = NodeSchema::builder("temperature_sensor")
        .property(
            PropertyDef::builder("name", ValueType::String)
                .required(true)
                .build(),
        )
        .property(PropertyDef::simple("unit", ValueType::String, false))
        .property(
            PropertyDef::builder("accuracy", ValueType::Float)
                .indexed()
                .build(),
        )
        .build();
    let _ = g.schema_mut().register_node_schema(ts_schema);

    // Rebuild property indexes to pick up the indexed flag
    g.build_property_indexes();
    g
}

fn bench_optimizer_validation(c: &mut Criterion) {
    let mut group = c.benchmark_group("gql_optimizer");

    for &n in &profile_scales() {
        let g = build_indexed_graph(n);
        let g_plain = build_scaled_graph(n);
        group.throughput(Throughput::Elements(n));

        // Range index scan: FILTER on indexed numeric property
        group.bench_with_input(BenchmarkId::new("range_indexed_gt", n), &g, |b, g| {
            b.iter(|| {
                QueryBuilder::new(
                    "MATCH (s:temperature_sensor) FILTER s.accuracy > 0.5 RETURN count(*) AS n",
                    g,
                )
                .execute()
                .unwrap()
            });
        });

        // Range filter without index (baseline comparison)
        group.bench_with_input(
            BenchmarkId::new("range_no_index_gt", n),
            &g_plain,
            |b, g| {
                b.iter(|| {
                    QueryBuilder::new(
                        "MATCH (s:temperature_sensor) FILTER s.accuracy > 0.5 RETURN count(*) AS n",
                        g,
                    )
                    .execute()
                    .unwrap()
                });
            },
        );

        // IN-list with indexed property
        group.bench_with_input(
            BenchmarkId::new("inlist_indexed_5", n),
            &g,
            |b, g| {
                b.iter(|| {
                    QueryBuilder::new(
                        "MATCH (s:sensor) FILTER s.unit IN ['°F', '°C', 'ppm', 'Pa', '%RH'] RETURN count(*) AS n",
                        g,
                    )
                    .execute()
                    .unwrap()
                });
            },
        );

        // EXISTS semi-join (early termination)
        group.bench_with_input(
            BenchmarkId::new("exists_semijoin", n),
            &g_plain,
            |b, g| {
                b.iter(|| {
                    QueryBuilder::new(
                        "MATCH (b:building) FILTER EXISTS { MATCH (b)-[:contains]->(f:floor) } RETURN b.name",
                        g,
                    )
                    .execute()
                    .unwrap()
                });
            },
        );

        // Two-hop with filter on intermediate node (exercises filter interleaving)
        group.bench_with_input(
            BenchmarkId::new("two_hop_interleaved_filter", n),
            &g_plain,
            |b, g| {
                b.iter(|| {
                    QueryBuilder::new(
                        "MATCH (b:building)-[:contains]->(f:floor)-[:contains]->(s:sensor) \
                         FILTER f.name = 'B1-Floor-1' \
                         RETURN s.name",
                        g,
                    )
                    .execute()
                    .unwrap()
                });
            },
        );

        // Multi-predicate with skewed selectivity
        group.bench_with_input(
            BenchmarkId::new("multi_predicate_skewed", n),
            &g_plain,
            |b, g| {
                b.iter(|| {
                    QueryBuilder::new(
                        "MATCH (s:sensor) FILTER s.unit = '°F' AND s.accuracy > 0.3 RETURN count(*) AS n",
                        g,
                    )
                    .execute()
                    .unwrap()
                });
            },
        );

        // Expand filter pushdown: target node filter
        group.bench_with_input(
            BenchmarkId::new("expand_target_filter", n),
            &g_plain,
            |b, g| {
                b.iter(|| {
                    QueryBuilder::new(
                        "MATCH (b:building)-[:contains]->(f:floor) \
                         FILTER f.name = 'B1-Floor-1' \
                         RETURN b.name, f.name",
                        g,
                    )
                    .execute()
                    .unwrap()
                });
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = optimizer_validation;
    config = profile_criterion();
    targets = bench_optimizer_validation
}

criterion_group! {
    name = factorized;
    config = profile_criterion();
    targets = bench_factorized
}

criterion_group! {
    name = spatial;
    config = profile_criterion();
    targets = bench_spatial
}

criterion_main!(
    parsing,
    pattern_matching,
    pipeline,
    predicates,
    advanced_pipeline,
    end_to_end,
    mutations,
    transactions,
    caching,
    subqueries,
    options,
    phase5,
    vector,
    ts_procedures,
    distinct_offset,
    dictionary,
    optimizer_validation,
    factorized,
    spatial
);
