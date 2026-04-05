//! Standalone memory profiling binary.
//!
//! Usage: cargo run -p selene-testing --bin memory_profile -- <target_nodes>
//! Wrap with /usr/bin/time to measure peak RSS.

use selene_testing::bench_scaling::build_scaled_graph_with_summary;

fn main() {
    let target: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.replace('_', "").parse().ok())
        .unwrap_or(10_000);

    eprintln!("Building graph targeting {target} nodes...");
    let (g, summary) = build_scaled_graph_with_summary(target);
    eprintln!(
        "  scale={}, nodes={}, edges={}",
        summary.scale, summary.actual_nodes, summary.actual_edges
    );

    eprintln!("Building full projection...");
    let proj = selene_algorithms::GraphProjection::build(
        &g,
        &selene_algorithms::ProjectionConfig {
            name: "memory_test".to_string(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        },
        None,
    );
    eprintln!(
        "  projection nodes={}, edges={}",
        proj.node_count(),
        proj.edge_count()
    );

    eprintln!("Running GQL query...");
    let result = selene_gql::QueryBuilder::new("MATCH (s:sensor) RETURN count(*) AS n", &g)
        .execute()
        .unwrap();
    eprintln!("  sensors={}", result.row_count());

    println!(
        "MEMORY_PROFILE target={} scale={} nodes={} edges={} proj_nodes={} proj_edges={}",
        target,
        summary.scale,
        summary.actual_nodes,
        summary.actual_edges,
        proj.node_count(),
        proj.edge_count(),
    );
}
