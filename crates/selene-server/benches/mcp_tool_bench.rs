//! Benchmarks for the MCP tool hot path.
//!
//! MCP tool handlers are thin wrappers around the `ops::*` functions.
//! Benching at the ops layer isolates the regression signal to the code
//! the tool path exercises (scope checks, GQL pipeline, serialization)
//! without HTTP round-trip noise.
//!
//! Run: cargo bench -p selene-server

use std::collections::HashMap;
use std::sync::Arc;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use selene_core::{LabelSet, PropertyMap, Value};
use selene_server::auth::AuthContext;
use selene_server::bootstrap::ServerState;
use selene_server::ops;
use selene_testing::bench_profiles::bench_profile;

fn profile_criterion() -> Criterion {
    bench_profile().into_criterion()
}

fn profile_scales() -> Vec<u64> {
    bench_profile().scales().to_vec()
}

/// Spin up a `ServerState` in a temp dir and populate it with `n` sensor
/// nodes plus a handful of buildings and edges, so reads exercise realistic
/// label indexes and property access.
fn build_state(rt: &Runtime, n: u64) -> (ServerState, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let state = rt.block_on(ServerState::for_testing(dir.path()));

    state
        .graph()
        .write(|m| {
            for i in 0..n {
                let mut props = PropertyMap::new();
                props.insert(
                    selene_core::IStr::new("name"),
                    Value::String(format!("sensor-{i}").into()),
                );
                props.insert(
                    selene_core::IStr::new("temp"),
                    Value::Float(68.0 + (i % 10) as f64),
                );
                m.create_node(LabelSet::from_strs(&["sensor"]), props)?;
            }
            Ok(())
        })
        .unwrap();

    (state, dir)
}

fn admin() -> AuthContext {
    AuthContext::dev_admin()
}

// ── Metadata reads: graph_stats / health ──────────────────────────

fn bench_graph_stats(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mcp_graph_stats");

    for &n in &profile_scales() {
        let (state, _dir) = build_state(&rt, n);
        let auth = admin();
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let stats = ops::graph_stats::graph_stats(&state, &auth);
                std::hint::black_box(stats);
            });
        });
    }
    group.finish();
}

fn bench_health(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (state, _dir) = build_state(&rt, 1_000);
    let mut group = c.benchmark_group("mcp_health");
    group.bench_function("single", |b| {
        b.iter(|| {
            let h = ops::health::health(&state);
            std::hint::black_box(h);
        });
    });
    group.finish();
}

// ── Paginated list_nodes ───────────────────────────────────────────

fn bench_list_nodes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mcp_list_nodes");

    for &n in &profile_scales() {
        let (state, _dir) = build_state(&rt, n);
        let auth = admin();
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let result = ops::nodes::list_nodes(&state, &auth, Some("sensor"), 100, 0)
                    .expect("list_nodes");
                std::hint::black_box(result);
            });
        });
    }
    group.finish();
}

// ── Parameterized GQL read (the gql_query MCP tool path) ──────────

fn bench_gql_read(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mcp_gql_read");

    for &n in &profile_scales() {
        let (state, _dir) = build_state(&rt, n);
        let auth = admin();

        let mut params: HashMap<String, Value> = HashMap::new();
        params.insert("threshold".into(), Value::Float(70.0));

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let result = ops::gql::execute_gql(
                    &state,
                    &auth,
                    "MATCH (s:sensor) FILTER s.temp > $threshold RETURN s.name AS name, s.temp AS temp LIMIT 50",
                    Some(&params),
                    false,
                    false,
                    ops::gql::ResultFormat::Json,
                )
                .expect("gql read");
                std::hint::black_box(result);
            });
        });
    }
    group.finish();
}

// ── create_node via GQL mutation path ──────────────────────────────
//
// Uses `iter_batched` so every iteration runs against a freshly built
// state of constant size. Without that, each INSERT would grow the
// underlying graph/WAL/indexes during the measurement window, so the
// sample would drift upward as the run progressed and would no longer
// reflect a steady-state insert cost.

fn bench_create_node(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mcp_create_node");
    let auth = admin();

    group.bench_function("single_insert", |b| {
        b.iter_batched(
            || {
                // Seed 100 nodes (not 1K): fresh state per iteration keeps
                // every sample at a constant starting size, and 100 is
                // enough to exercise label-index and WAL code paths without
                // dominating the routine with setup variance.
                let (state, dir) = build_state(&rt, 100);
                (Arc::new(state), dir)
            },
            |(state, _dir)| {
                let mut params: HashMap<String, Value> = HashMap::new();
                params.insert("name".into(), Value::String("bench-node".into()));
                let result = ops::gql::execute_gql(
                    &state,
                    &auth,
                    "INSERT (n:bench {name: $name}) RETURN id(n) AS id",
                    Some(&params),
                    false,
                    false,
                    ops::gql::ResultFormat::Json,
                )
                .expect("create_node");
                std::hint::black_box(result);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = profile_criterion();
    targets = bench_graph_stats, bench_health, bench_list_nodes, bench_gql_read, bench_create_node
}
criterion_main!(benches);
