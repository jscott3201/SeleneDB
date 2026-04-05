//! Benchmarks for selene-graph: CRUD, index lookups, traversal, concurrency.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use selene_core::schema::ValueType;
use selene_core::{IStr, LabelSet, NodeId, PropertyMap, Value};
use selene_graph::algorithms::bfs;
use selene_graph::typed_index::{CompositeTypedIndex, TypedIndex};
use selene_graph::{SeleneGraph, SharedGraph};
use selene_testing::bench_profiles::bench_profile;
use selene_testing::bench_scaling::build_scaled_graph;

fn profile_scales() -> Vec<u64> {
    bench_profile().scales().to_vec()
}

fn profile_criterion() -> Criterion {
    bench_profile().into_criterion()
}

// ── Node CRUD ────────────────────────────────────────────────────────────

fn bench_node_create(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_create");
    for &scale in &profile_scales() {
        group.throughput(Throughput::Elements(scale));
        group.bench_with_input(BenchmarkId::from_parameter(scale), &scale, |b, &n| {
            b.iter(|| {
                let mut g = SeleneGraph::new();
                for i in 1..=n {
                    let mut m = g.mutate();
                    m.create_node(LabelSet::from_strs(&["sensor"]), PropertyMap::new())
                        .unwrap();
                    m.commit(0).unwrap();
                    std::hint::black_box(i);
                }
            });
        });
    }
    group.finish();
}

fn bench_node_get_by_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_get_by_id");
    for &scale in &profile_scales() {
        let g = build_scaled_graph(scale);
        group.bench_with_input(BenchmarkId::from_parameter(scale), &scale, |b, &n| {
            b.iter(|| {
                for i in 1..=n {
                    std::hint::black_box(g.get_node(NodeId(i)));
                }
            });
        });
    }
    group.finish();
}

fn bench_node_update_property(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_update_property");
    for &scale in &profile_scales() {
        let mut g = build_scaled_graph(scale);
        group.throughput(Throughput::Elements(scale.min(100)));
        group.bench_with_input(BenchmarkId::from_parameter(scale), &scale, |b, &n| {
            b.iter(|| {
                let mut m = g.mutate();
                for i in 1..=n.min(100) {
                    m.set_property(NodeId(i), IStr::new("value"), Value::Float(72.5))
                        .unwrap();
                }
                m.commit(0).unwrap();
            });
        });
    }
    group.finish();
}

// ── Index Lookups ────────────────────────────────────────────────────────

fn bench_label_index_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("label_index_lookup");
    for &scale in &profile_scales() {
        let g = build_scaled_graph(scale);
        group.bench_with_input(BenchmarkId::from_parameter(scale), &scale, |b, &_n| {
            b.iter(|| {
                let count = g.nodes_by_label("sensor").count();
                std::hint::black_box(count);
            });
        });
    }
    group.finish();
}

// ── BFS Traversal ────────────────────────────────────────────────────────

fn bench_bfs_traversal(c: &mut Criterion) {
    let mut group = c.benchmark_group("bfs_traversal");
    for depth in [1u32, 3, 10, 50] {
        for &scale in &profile_scales() {
            let g = build_scaled_graph(scale);
            let param = format!("d{depth}_n{scale}");
            group.bench_with_input(
                BenchmarkId::new("bfs", &param),
                &(scale, depth),
                |b, &(_n, d)| {
                    b.iter(|| {
                        let result = bfs(&g, NodeId(1), Some("contains"), d);
                        std::hint::black_box(result.len());
                    });
                },
            );
        }
    }
    group.finish();
}

// ── Concurrent Reads ─────────────────────────────────────────────────────

fn bench_concurrent_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_reads");
    for &scale in &profile_scales() {
        let graph = build_scaled_graph(scale);
        let shared = SharedGraph::new(graph);
        group.throughput(Throughput::Elements(10));
        group.bench_with_input(BenchmarkId::from_parameter(scale), &shared, |b, shared| {
            b.iter(|| {
                std::thread::scope(|s| {
                    let handles: Vec<_> = (0..10)
                        .map(|_| s.spawn(|| shared.read(|g| std::hint::black_box(g.node_count()))))
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            });
        });
    }
    group.finish();
}

// ── Edge Create + Node Remove ────────────────────────────────────

fn bench_edge_create(c: &mut Criterion) {
    let mut group = c.benchmark_group("edge_create");
    for &scale in &profile_scales() {
        let n = scale.min(1000) as usize;
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::from_parameter(scale), |b| {
            b.iter_with_setup(
                || {
                    let mut g = SeleneGraph::new();
                    let mut m = g.mutate();
                    for _ in 0..(n * 2) {
                        m.create_node(LabelSet::from_strs(&["node"]), PropertyMap::new())
                            .unwrap();
                    }
                    m.commit(0).unwrap();
                    g
                },
                |mut g| {
                    let mut m = g.mutate();
                    for i in 0..n as u64 {
                        m.create_edge(
                            NodeId(i * 2 + 1),
                            IStr::new("connects"),
                            NodeId(i * 2 + 2),
                            PropertyMap::new(),
                        )
                        .unwrap();
                    }
                    m.commit(0).unwrap();
                },
            );
        });
    }
    group.finish();
}

fn bench_node_remove(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_remove");
    for &scale in &profile_scales() {
        group.throughput(Throughput::Elements(scale.min(100)));
        group.bench_function(BenchmarkId::from_parameter(scale), |b| {
            b.iter_with_setup(
                || {
                    let g = build_scaled_graph(scale);
                    let ids: Vec<_> = g.nodes_by_label("sensor").take(100).collect();
                    (g, ids)
                },
                |(mut g, ids)| {
                    let mut m = g.mutate();
                    for nid in ids {
                        m.delete_node(nid).unwrap();
                    }
                    m.commit(0).unwrap();
                },
            );
        });
    }
    group.finish();
}

// ── Mutation + Rollback ──────────────────────────────────────────────────

fn bench_mutation_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("mutation_commit");
    for &ops in &[10u64, 100, 1_000] {
        let mut g = build_scaled_graph(1_000);
        group.bench_with_input(BenchmarkId::from_parameter(ops), &ops, |b, &n| {
            b.iter(|| {
                let mut m = g.mutate();
                for i in 1..=n {
                    m.set_property(
                        NodeId((i % 1000) + 1),
                        IStr::new("bench"),
                        Value::Int(i as i64),
                    )
                    .unwrap_or(());
                }
                let changes = m.commit(0).unwrap();
                std::hint::black_box(changes.len());
            });
        });
    }
    group.finish();
}

fn bench_mutation_rollback(c: &mut Criterion) {
    let mut group = c.benchmark_group("mutation_rollback");
    for &ops in &[10u64, 100, 1_000] {
        let mut g = build_scaled_graph(1_000);
        group.bench_with_input(BenchmarkId::from_parameter(ops), &ops, |b, &n| {
            b.iter(|| {
                let mut m = g.mutate();
                for i in 1..=n {
                    m.set_property(
                        NodeId((i % 1000) + 1),
                        IStr::new("bench"),
                        Value::Int(i as i64),
                    )
                    .unwrap_or(());
                }
                drop(m);
            });
        });
    }
    group.finish();
}

// ── TypedIndex ──────────────────────────────────────────────────────

/// Build a TypedIndex of Float values from sensor accuracy properties.
fn build_accuracy_index(g: &SeleneGraph) -> TypedIndex {
    let accuracy_key = IStr::new("accuracy");
    let mut idx = TypedIndex::new_for_type(&ValueType::Float);
    for nid in g.nodes_by_label("sensor") {
        if let Some(node) = g.get_node(nid)
            && let Some(val) = node.properties.get(accuracy_key)
        {
            idx.insert(val, nid);
        }
    }
    idx
}

fn bench_typed_index_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("typed_index_build");
    for &scale in &profile_scales() {
        let g = build_scaled_graph(scale);
        let sensor_count = g.nodes_by_label("sensor").count() as u64;
        group.throughput(Throughput::Elements(sensor_count));
        group.bench_with_input(BenchmarkId::from_parameter(scale), &g, |b, g| {
            b.iter(|| {
                let idx = build_accuracy_index(g);
                std::hint::black_box(idx.is_empty());
            });
        });
    }
    group.finish();
}

fn bench_typed_index_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("typed_index_lookup");
    for &scale in &profile_scales() {
        let g = build_scaled_graph(scale);
        let idx = build_accuracy_index(&g);
        group.bench_with_input(BenchmarkId::from_parameter(scale), &idx, |b, idx| {
            b.iter(|| {
                let result = idx.lookup(&Value::Float(0.5));
                std::hint::black_box(result.map(|v| v.len()));
            });
        });
    }
    group.finish();
}

fn bench_typed_index_iter_asc(c: &mut Criterion) {
    let mut group = c.benchmark_group("typed_index_iter_asc");
    for &scale in &profile_scales() {
        let g = build_scaled_graph(scale);
        let idx = build_accuracy_index(&g);
        group.bench_with_input(BenchmarkId::from_parameter(scale), &idx, |b, idx| {
            b.iter(|| {
                let mut count = 0u64;
                idx.iter_asc(|_nid| {
                    count += 1;
                    true
                });
                std::hint::black_box(count);
            });
        });
    }
    group.finish();
}

fn bench_composite_index_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("composite_index_build");
    let unit_key = IStr::new("unit");
    let accuracy_key = IStr::new("accuracy");

    for &scale in &profile_scales() {
        let g = build_scaled_graph(scale);
        let sensor_count = g.nodes_by_label("sensor").count() as u64;
        group.throughput(Throughput::Elements(sensor_count));
        group.bench_with_input(BenchmarkId::from_parameter(scale), &g, |b, g| {
            b.iter(|| {
                let mut cidx = CompositeTypedIndex::new(vec![unit_key, accuracy_key]);
                for nid in g.nodes_by_label("sensor") {
                    if let Some(node) = g.get_node(nid) {
                        let unit = node
                            .properties
                            .get(unit_key)
                            .cloned()
                            .unwrap_or(Value::Null);
                        let acc = node
                            .properties
                            .get(accuracy_key)
                            .cloned()
                            .unwrap_or(Value::Null);
                        cidx.insert(&[&unit, &acc], nid);
                    }
                }
                std::hint::black_box(cidx.is_empty());
            });
        });
    }
    group.finish();
}

fn bench_composite_index_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("composite_index_lookup");
    let unit_key = IStr::new("unit");
    let accuracy_key = IStr::new("accuracy");

    for &scale in &profile_scales() {
        let g = build_scaled_graph(scale);
        let mut cidx = CompositeTypedIndex::new(vec![unit_key, accuracy_key]);
        for nid in g.nodes_by_label("sensor") {
            if let Some(node) = g.get_node(nid) {
                let unit = node
                    .properties
                    .get(unit_key)
                    .cloned()
                    .unwrap_or(Value::Null);
                let acc = node
                    .properties
                    .get(accuracy_key)
                    .cloned()
                    .unwrap_or(Value::Null);
                cidx.insert(&[&unit, &acc], nid);
            }
        }
        let unit_val = Value::String("°F".into());
        let acc_val = Value::Float(0.5);
        group.bench_with_input(BenchmarkId::from_parameter(scale), &cidx, |b, cidx| {
            b.iter(|| {
                let result = cidx.lookup(&[&unit_val, &acc_val]);
                std::hint::black_box(result.map(|v| v.len()));
            });
        });
    }
    group.finish();
}

criterion_group! {
    name = crud;
    config = profile_criterion();
    targets = bench_node_create, bench_node_get_by_id, bench_node_update_property
}
criterion_group! {
    name = indexes;
    config = profile_criterion();
    targets = bench_label_index_lookup
}
criterion_group! {
    name = traversal;
    config = profile_criterion();
    targets = bench_bfs_traversal
}
criterion_group! {
    name = concurrency;
    config = profile_criterion();
    targets = bench_concurrent_reads
}
criterion_group! {
    name = mutations;
    config = profile_criterion();
    targets = bench_mutation_commit, bench_mutation_rollback
}
criterion_group! {
    name = graph_ops;
    config = profile_criterion();
    targets = bench_edge_create, bench_node_remove
}
criterion_group! {
    name = typed_indexes;
    config = profile_criterion();
    targets = bench_typed_index_build, bench_typed_index_lookup, bench_typed_index_iter_asc,
              bench_composite_index_build, bench_composite_index_lookup
}
criterion_main!(
    crud,
    indexes,
    traversal,
    concurrency,
    mutations,
    graph_ops,
    typed_indexes
);
