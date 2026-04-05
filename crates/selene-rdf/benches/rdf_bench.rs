//! Benchmarks for selene-rdf: export, import, and SPARQL query.
//!
//! Uses `reference_building(10)` (~486 nodes). The GQL benchmarks in selene-gql
//! use a 10K-node stress profile. Direct latency comparisons between SPARQL
//! and GQL benchmarks require running both at the same graph scale.
//!
//! Run (without SPARQL): cargo bench -p selene-rdf -- --test
//! Run (with SPARQL):    cargo bench -p selene-rdf --features sparql -- --test

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use selene_graph::SharedGraph;
use selene_rdf::RdfFormat;
use selene_rdf::export::{export_ntriples, export_turtle};
use selene_rdf::import::import_rdf;
use selene_rdf::namespace::RdfNamespace;
use selene_rdf::ontology::OntologyStore;
use selene_testing::bench_profiles::bench_profile;
use selene_testing::reference_building::reference_building;

fn profile_criterion() -> Criterion {
    bench_profile().into_criterion()
}

/// RDF-appropriate building scales (number of buildings, not raw node counts).
/// Each building produces ~50 nodes, so these map to ~50, ~250, ~500 nodes.
fn rdf_scales() -> Vec<usize> {
    vec![1, 5, 10]
}

fn test_ns() -> RdfNamespace {
    RdfNamespace::new("https://example.com/building/")
}

// ── Export: Turtle ───────────────────────────────────────────────────

fn bench_export_turtle(c: &mut Criterion) {
    let mut group = c.benchmark_group("rdf_export_turtle");
    let ns = test_ns();

    for &scale in &rdf_scales() {
        let g = reference_building(scale);
        group.throughput(Throughput::Elements(g.node_count() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(scale), &g, |b, g| {
            b.iter(|| {
                let bytes = export_turtle(g, &ns).unwrap();
                std::hint::black_box(bytes.len());
            });
        });
    }
    group.finish();
}

// ── Export: N-Triples ───────────────────────────────────────────────

fn bench_export_ntriples(c: &mut Criterion) {
    let mut group = c.benchmark_group("rdf_export_ntriples");
    let ns = test_ns();

    for &scale in &rdf_scales() {
        let g = reference_building(scale);
        group.throughput(Throughput::Elements(g.node_count() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(scale), &g, |b, g| {
            b.iter(|| {
                let bytes = export_ntriples(g, &ns).unwrap();
                std::hint::black_box(bytes.len());
            });
        });
    }
    group.finish();
}

// ── Import: Turtle roundtrip ────────────────────────────────────────

fn bench_import_turtle(c: &mut Criterion) {
    let mut group = c.benchmark_group("rdf_import_turtle");
    let ns = test_ns();

    for &scale in &rdf_scales() {
        let g = reference_building(scale);
        let turtle_bytes = export_turtle(&g, &ns).unwrap();
        group.throughput(Throughput::Bytes(turtle_bytes.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(scale),
            &turtle_bytes,
            |b, data| {
                b.iter(|| {
                    let target = selene_graph::SeleneGraph::new();
                    let shared = SharedGraph::new(target);
                    let mut ontology = OntologyStore::new();
                    let result =
                        import_rdf(data, RdfFormat::Turtle, None, &shared, &ns, &mut ontology)
                            .unwrap();
                    std::hint::black_box(result.nodes_created);
                });
            },
        );
    }
    group.finish();
}

// ── SPARQL (feature-gated) ──────────────────────────────────────────

#[cfg(feature = "sparql")]
fn bench_sparql(c: &mut Criterion) {
    use selene_graph::CsrAdjacency;
    use selene_rdf::sparql::{SparqlResultFormat, execute_sparql};

    let mut group = c.benchmark_group("rdf_sparql");
    let ns = test_ns();

    for &scale in &rdf_scales() {
        let g = reference_building(scale);
        let csr = CsrAdjacency::build(&g);
        let ontology = OntologyStore::new();
        group.throughput(Throughput::Elements(g.node_count() as u64));

        // SELECT all sensors
        group.bench_with_input(
            BenchmarkId::new("select_sensors", scale),
            &(&g, &csr, &ontology),
            |b, (g, csr, ont)| {
                b.iter(|| {
                    let (bytes, _ct) = execute_sparql(
                        g,
                        csr,
                        &ns,
                        Some(*ont),
                        "SELECT ?s WHERE { ?s a <https://example.com/building/type/sensor> }",
                        SparqlResultFormat::Json,
                    )
                    .unwrap();
                    std::hint::black_box(bytes.len());
                });
            },
        );

        // SELECT with property filter
        group.bench_with_input(
            BenchmarkId::new("select_with_property", scale),
            &(&g, &csr, &ontology),
            |b, (g, csr, ont)| {
                b.iter(|| {
                    let (bytes, _ct) = execute_sparql(
                        g,
                        csr,
                        &ns,
                        Some(*ont),
                        "SELECT ?s ?unit WHERE { \
                            ?s a <https://example.com/building/type/sensor> . \
                            ?s <https://example.com/building/prop/unit> ?unit \
                        }",
                        SparqlResultFormat::Json,
                    )
                    .unwrap();
                    std::hint::black_box(bytes.len());
                });
            },
        );

        // Two-hop: zones contained in buildings
        group.bench_with_input(
            BenchmarkId::new("two_hop_containment", scale),
            &(&g, &csr, &ontology),
            |b, (g, csr, ont)| {
                b.iter(|| {
                    let (bytes, _ct) = execute_sparql(
                        g,
                        csr,
                        &ns,
                        Some(*ont),
                        "SELECT ?b ?z WHERE { \
                            ?b a <https://example.com/building/type/building> . \
                            ?b <https://example.com/building/rel/contains> ?f . \
                            ?f <https://example.com/building/rel/contains> ?z . \
                            ?z a <https://example.com/building/type/zone> \
                        }",
                        SparqlResultFormat::Json,
                    )
                    .unwrap();
                    std::hint::black_box(bytes.len());
                });
            },
        );

        // Edge traversal: equipment monitored by servers
        group.bench_with_input(
            BenchmarkId::new("edge_traversal", scale),
            &(&g, &csr, &ontology),
            |b, (g, csr, ont)| {
                b.iter(|| {
                    let (bytes, _ct) = execute_sparql(
                        g,
                        csr,
                        &ns,
                        Some(*ont),
                        "SELECT ?srv ?equip WHERE { \
                            ?srv a <https://example.com/building/type/server> . \
                            ?srv <https://example.com/building/rel/monitors> ?equip \
                        }",
                        SparqlResultFormat::Json,
                    )
                    .unwrap();
                    std::hint::black_box(bytes.len());
                });
            },
        );

        // COUNT aggregation
        group.bench_with_input(
            BenchmarkId::new("count_sensors", scale),
            &(&g, &csr, &ontology),
            |b, (g, csr, ont)| {
                b.iter(|| {
                    let (bytes, _ct) = execute_sparql(
                        g,
                        csr,
                        &ns,
                        Some(*ont),
                        "SELECT (COUNT(?s) AS ?count) WHERE { \
                            ?s a <https://example.com/building/type/sensor> \
                        }",
                        SparqlResultFormat::Json,
                    )
                    .unwrap();
                    std::hint::black_box(bytes.len());
                });
            },
        );

        // CSR build (isolated) -- measures the per-query CSR construction cost
        group.bench_with_input(BenchmarkId::new("csr_build", scale), &g, |b, g| {
            b.iter(|| std::hint::black_box(CsrAdjacency::build(g)));
        });
    }
    group.finish();
}

// ── Registration ────────────────────────────────────────────────────

criterion_group! {
    name = export;
    config = profile_criterion();
    targets = bench_export_turtle, bench_export_ntriples
}

criterion_group! {
    name = import;
    config = profile_criterion();
    targets = bench_import_turtle
}

#[cfg(feature = "sparql")]
criterion_group! {
    name = sparql;
    config = profile_criterion();
    targets = bench_sparql
}

#[cfg(not(feature = "sparql"))]
criterion_main!(export, import);

#[cfg(feature = "sparql")]
criterion_main!(export, import, sparql);
