//! Benchmarks for selene-algorithms: projections, structural, pathfinding, centrality, community.
//!
//! Run: cargo bench -p selene-algorithms
//! Quick: SELENE_BENCH_PROFILE=quick cargo bench -p selene-algorithms

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use selene_algorithms::projection::ProjectionConfig;
use selene_algorithms::{
    ContainmentIndex, GraphProjection, apsp, articulation_points, betweenness, bridges, dijkstra,
    label_propagation, louvain, pagerank, scc, scc_count, sssp, topological_sort, triangle_count,
    validate, wcc, wcc_count,
};
use selene_core::{IStr, NodeId};
use selene_testing::bench_profiles::{BenchProfile, bench_profile};
use selene_testing::bench_scaling::build_scaled_graph;

fn profile_scales() -> Vec<u64> {
    bench_profile().scales().to_vec()
}

fn profile_criterion() -> Criterion {
    bench_profile().into_criterion()
}

/// Build a projection over the full graph (all labels, all edge types, unweighted).
fn full_projection(graph: &selene_graph::SeleneGraph) -> GraphProjection {
    GraphProjection::build(
        graph,
        &ProjectionConfig {
            name: "bench_full".to_string(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        },
        None,
    )
}

/// Build a weighted projection using pipe_length_ft on feeds edges.
fn weighted_projection(graph: &selene_graph::SeleneGraph) -> GraphProjection {
    GraphProjection::build(
        graph,
        &ProjectionConfig {
            name: "bench_weighted".to_string(),
            node_labels: vec![],
            edge_labels: vec![
                IStr::new("feeds"),
                IStr::new("contains"),
                IStr::new("powers"),
            ],
            weight_property: Some(IStr::new("pipe_length_ft")),
        },
        None,
    )
}

/// Build a containment-only projection (acyclic — no HVAC cycles).
fn containment_projection(graph: &selene_graph::SeleneGraph) -> GraphProjection {
    GraphProjection::build(
        graph,
        &ProjectionConfig {
            name: "bench_containment".to_string(),
            node_labels: vec![],
            edge_labels: vec![IStr::new("contains")],
            weight_property: None,
        },
        None,
    )
}

// ── Projection Build ───────────────────────────────────────────────

fn bench_projection_build(c: &mut Criterion) {
    let scales = profile_scales();
    let mut group = c.benchmark_group("algo_projection_build");

    for &n in &scales {
        let g = build_scaled_graph(n);
        group.throughput(Throughput::Elements(g.node_count() as u64));

        group.bench_with_input(BenchmarkId::new("full", n), &g, |b, g| {
            b.iter(|| std::hint::black_box(full_projection(g)));
        });

        group.bench_with_input(BenchmarkId::new("containment_only", n), &g, |b, g| {
            b.iter(|| std::hint::black_box(containment_projection(g)));
        });

        group.bench_with_input(BenchmarkId::new("weighted", n), &g, |b, g| {
            b.iter(|| std::hint::black_box(weighted_projection(g)));
        });
    }

    group.finish();
}

// ── Structural ─────────────────────────────────────────────────────

fn bench_structural(c: &mut Criterion) {
    let scales = profile_scales();
    let mut group = c.benchmark_group("algo_structural");

    for &n in &scales {
        let g = build_scaled_graph(n);
        let proj = full_projection(&g);
        let containment_proj = containment_projection(&g);
        group.throughput(Throughput::Elements(g.node_count() as u64));

        group.bench_with_input(BenchmarkId::new("wcc", n), &proj, |b, proj| {
            b.iter(|| std::hint::black_box(wcc(proj)));
        });

        group.bench_with_input(BenchmarkId::new("wcc_count", n), &proj, |b, proj| {
            b.iter(|| std::hint::black_box(wcc_count(proj)));
        });

        group.bench_with_input(BenchmarkId::new("scc", n), &proj, |b, proj| {
            b.iter(|| std::hint::black_box(scc(proj)));
        });

        group.bench_with_input(BenchmarkId::new("scc_count", n), &proj, |b, proj| {
            b.iter(|| std::hint::black_box(scc_count(proj)));
        });

        // Topo sort on containment (acyclic) — should succeed
        group.bench_with_input(
            BenchmarkId::new("topo_sort_acyclic", n),
            &containment_proj,
            |b, proj| b.iter(|| std::hint::black_box(topological_sort(proj))),
        );

        // Topo sort on full graph (has HVAC cycles) — should return error
        group.bench_with_input(BenchmarkId::new("topo_sort_cyclic", n), &proj, |b, proj| {
            b.iter(|| std::hint::black_box(topological_sort(proj)));
        });

        group.bench_with_input(
            BenchmarkId::new("articulation_points", n),
            &proj,
            |b, proj| b.iter(|| std::hint::black_box(articulation_points(proj))),
        );

        group.bench_with_input(BenchmarkId::new("bridges", n), &proj, |b, proj| {
            b.iter(|| std::hint::black_box(bridges(proj)));
        });

        group.bench_with_input(BenchmarkId::new("validate", n), &proj, |b, proj| {
            b.iter(|| std::hint::black_box(validate(proj)));
        });

        // ContainmentIndex takes raw &SeleneGraph, not a projection
        group.bench_with_input(BenchmarkId::new("containment_index", n), &g, |b, g| {
            b.iter(|| std::hint::black_box(ContainmentIndex::build(g)));
        });
    }

    group.finish();
}

// ── Pathfinding ────────────────────────────────────────────────────

fn bench_pathfinding(c: &mut Criterion) {
    let scales = profile_scales();
    let mut group = c.benchmark_group("algo_pathfinding");

    for &n in &scales {
        let g = build_scaled_graph(n);
        let proj = weighted_projection(&g);
        group.throughput(Throughput::Elements(g.node_count() as u64));

        // Dijkstra: campus (node 1) to a deep sensor node
        let target_id = NodeId(g.node_count() as u64 / 2);
        group.bench_with_input(BenchmarkId::new("dijkstra", n), &proj, |b, proj| {
            b.iter(|| std::hint::black_box(dijkstra(proj, NodeId(1), target_id)));
        });

        // SSSP from campus root
        group.bench_with_input(BenchmarkId::new("sssp", n), &proj, |b, proj| {
            b.iter(|| std::hint::black_box(sssp(proj, NodeId(1))));
        });

        // APSP — skip at >= 10K (O(n^2) memory, OOM risk on M1 Air)
        if !BenchProfile::should_skip_expensive(n, 10_000) {
            group.bench_with_input(BenchmarkId::new("apsp", n), &proj, |b, proj| {
                b.iter(|| std::hint::black_box(apsp(proj, 5_000)));
            });
        }
    }

    group.finish();
}

// ── Centrality ─────────────────────────────────────────────────────

fn bench_centrality(c: &mut Criterion) {
    let scales = profile_scales();
    let mut group = c.benchmark_group("algo_centrality");

    for &n in &scales {
        let g = build_scaled_graph(n);
        let proj = full_projection(&g);
        group.throughput(Throughput::Elements(g.node_count() as u64));

        group.bench_with_input(BenchmarkId::new("pagerank", n), &proj, |b, proj| {
            b.iter(|| std::hint::black_box(pagerank(proj, 0.85, 20)));
        });

        // Betweenness — skip at >= 10K (O(VE) Brandes, too slow for bench window)
        if !BenchProfile::should_skip_expensive(n, 10_000) {
            group.bench_with_input(BenchmarkId::new("betweenness", n), &proj, |b, proj| {
                b.iter(|| std::hint::black_box(betweenness(proj, None)));
            });
        }
    }

    group.finish();
}

// ── Community ──────────────────────────────────────────────────────

fn bench_community(c: &mut Criterion) {
    let scales = profile_scales();
    let mut group = c.benchmark_group("algo_community");

    for &n in &scales {
        let g = build_scaled_graph(n);
        let proj = full_projection(&g);
        group.throughput(Throughput::Elements(g.node_count() as u64));

        group.bench_with_input(BenchmarkId::new("louvain", n), &proj, |b, proj| {
            b.iter(|| std::hint::black_box(louvain(proj)));
        });

        group.bench_with_input(
            BenchmarkId::new("label_propagation", n),
            &proj,
            |b, proj| b.iter(|| std::hint::black_box(label_propagation(proj, 10))),
        );

        group.bench_with_input(BenchmarkId::new("triangle_count", n), &proj, |b, proj| {
            b.iter(|| std::hint::black_box(triangle_count(proj)));
        });
    }

    group.finish();
}

// ═══════════════════════════════════════════════════════════════════
// Registration
// ═══════════════════════════════════════════════════════════════════

criterion_group! {
    name = projections;
    config = profile_criterion();
    targets = bench_projection_build
}

criterion_group! {
    name = structural;
    config = profile_criterion();
    targets = bench_structural
}

criterion_group! {
    name = pathfinding;
    config = profile_criterion();
    targets = bench_pathfinding
}

criterion_group! {
    name = centrality;
    config = profile_criterion();
    targets = bench_centrality
}

criterion_group! {
    name = community;
    config = profile_criterion();
    targets = bench_community
}

criterion_main!(projections, structural, pathfinding, centrality, community);
