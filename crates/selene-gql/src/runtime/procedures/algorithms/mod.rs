//! Algorithm procedures: GQL CALL/YIELD wrappers for selene-algorithms.
//!
//! Projection management: graph.project, graph.drop, graph.listProjections
//! Structural: graph.wcc, graph.scc, graph.topoSort, graph.articulationPoints,
//!             graph.bridges, graph.validate, graph.isAncestor

mod centrality;
mod community;
mod pathfinding;
mod projection;
mod structural;

#[cfg(test)]
mod tests;

pub(crate) use centrality::{GraphBetweenness, GraphPagerank};
pub(crate) use community::{GraphLabelPropagation, GraphLouvain, GraphTriangleCount};
pub(crate) use pathfinding::{GraphApsp, GraphShortestPath, GraphSssp};
pub(crate) use projection::{GraphDrop, GraphListProjections, GraphProject};
pub(crate) use structural::{
    GraphArticulationPoints, GraphBridges, GraphIsAncestor, GraphScc, GraphTopoSort, GraphValidate,
    GraphWcc,
};

use selene_algorithms::ProjectionCatalog;
use selene_core::IStr;
use selene_graph::SeleneGraph;

use crate::types::error::GqlError;
use crate::types::value::GqlValue;

// Re-export Procedure trait and GqlType so tests (via `use super::*`) can see them.
#[cfg(test)]
pub(crate) use super::Procedure;
#[cfg(test)]
pub(crate) use crate::types::value::GqlType;

use parking_lot::RwLock;
use std::sync::Arc;

/// Shared projection catalog. Thread-safe, passed to all algorithm procedures.
///
/// Wrapped in `Arc<RwLock>` because procedures take `&self` and the catalog
/// needs interior mutability for project/drop operations.
pub(crate) type SharedCatalog = Arc<RwLock<ProjectionCatalog>>;

/// Create a new shared catalog for use with algorithm procedures.
pub(crate) fn new_shared_catalog() -> SharedCatalog {
    Arc::new(RwLock::new(ProjectionCatalog::new()))
}

// ── Helper: extract projection name from args ───────────────────────

fn extract_name(args: &[GqlValue], idx: usize, proc_name: &str) -> Result<String, GqlError> {
    args.get(idx)
        .ok_or_else(|| GqlError::InvalidArgument {
            message: format!(
                "{proc_name} requires projection name as argument {}",
                idx + 1
            ),
        })?
        .as_str()
        .map(|s| s.to_string())
}

fn extract_optional_string_list(args: &[GqlValue], idx: usize) -> Vec<IStr> {
    args.get(idx)
        .and_then(|v| match v {
            GqlValue::Null => None,
            GqlValue::List(list) => Some(
                list.elements
                    .iter()
                    .filter_map(|item| item.as_str().ok().map(IStr::new))
                    .collect(),
            ),
            GqlValue::String(s) => Some(vec![IStr::new(s.as_str())]),
            _ => None,
        })
        .unwrap_or_default()
}

// ── Helper: get or build projection ─────────────────────────────────

fn get_projection_or_build(
    catalog: &SharedCatalog,
    args: &[GqlValue],
    graph: &SeleneGraph,
) -> Result<String, GqlError> {
    let name = extract_name(args, 0, "algorithm")?;

    // Ensure the projection exists and is fresh. If it was created by
    // graph.project() but the graph mutated since, rebuild it from its
    // original config so the user's label/edge filters are preserved.
    catalog.write().ensure_fresh(graph, &name);

    Ok(name)
}

/// Register all algorithm procedures in the registry.
pub(crate) fn register_algorithm_procedures(
    registry: &mut super::ProcedureRegistry,
    catalog: SharedCatalog,
) {
    // Projection management
    registry.register(Arc::new(GraphProject {
        catalog: catalog.clone(),
    }));
    registry.register(Arc::new(GraphDrop {
        catalog: catalog.clone(),
    }));
    registry.register(Arc::new(GraphListProjections {
        catalog: catalog.clone(),
    }));
    // Structural
    registry.register(Arc::new(GraphWcc {
        catalog: catalog.clone(),
    }));
    registry.register(Arc::new(GraphScc {
        catalog: catalog.clone(),
    }));
    registry.register(Arc::new(GraphTopoSort {
        catalog: catalog.clone(),
    }));
    registry.register(Arc::new(GraphArticulationPoints {
        catalog: catalog.clone(),
    }));
    registry.register(Arc::new(GraphBridges {
        catalog: catalog.clone(),
    }));
    registry.register(Arc::new(GraphValidate {
        catalog: catalog.clone(),
    }));
    registry.register(Arc::new(GraphIsAncestor));
    // Path finding
    registry.register(Arc::new(GraphShortestPath {
        catalog: catalog.clone(),
    }));
    registry.register(Arc::new(GraphSssp {
        catalog: catalog.clone(),
    }));
    registry.register(Arc::new(GraphApsp {
        catalog: catalog.clone(),
    }));
    // Centrality
    registry.register(Arc::new(GraphPagerank {
        catalog: catalog.clone(),
    }));
    registry.register(Arc::new(GraphBetweenness {
        catalog: catalog.clone(),
    }));
    // Community detection
    registry.register(Arc::new(GraphLabelPropagation {
        catalog: catalog.clone(),
    }));
    registry.register(Arc::new(GraphLouvain {
        catalog: catalog.clone(),
    }));
    registry.register(Arc::new(GraphTriangleCount {
        catalog: catalog.clone(),
    }));
}
