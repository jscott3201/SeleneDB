//! Procedure trait and registry for CALL/YIELD execution.
//!
//! Built-in procedures implement the [`Procedure`] trait and are registered
//! in [`ProcedureRegistry`]. Organized by domain:
//! - `ts` -- time-series (ts.range, ts.latest, ts.aggregate, ts.window, ts.downsample, ts.history, ts.fullRange, ts.trends)
//! - `ts_percentile` -- quantile queries from warm tier DDSketch (ts.percentile)
//! - `ts_scoped` -- BFS-scoped TS aggregation (ts.scopedAggregate)
//! - `ts_anomalies` -- Z-score anomaly detection (ts.anomalies, ts.peerAnomalies)
//! - `graph` -- schema introspection (graph.labels, graph.edge_types, graph.node_count, graph.edge_count, graph.schema, graph.constraints, graph.discoverSchema)
//! - `schema_audit` -- migration progress (graph.schemaAudit, graph.schemaAuditDetails)
//! - `algorithms` -- graph algorithms (WCC, SCC, topoSort, projections, etc.)
//! - `vector` -- vector search (graph.vectorSearch, graph.similarNodes, graph.scopedVectorSearch, graph.semanticSearch, graph.rebuildVectorIndex)
//! - `search` -- full-text search (graph.textSearch, graph.hybridSearch)
//! - `history` -- temporal queries (graph.history, graph.changes, graph.propertyAt, graph.propertyHistory)

pub mod algorithms;
pub mod community_search;
pub mod graph;
#[cfg(feature = "ai")]
pub mod graphrag;
pub mod history;
#[cfg(feature = "ai")]
pub mod memory;
pub mod rdf;
pub mod schema_audit;
pub mod schema_dump;
pub mod search;
pub mod ts;
pub mod ts_aggregate;
pub mod ts_anomalies;
pub mod ts_gaps;
pub mod ts_history_provider;
pub mod ts_percentile;
pub mod ts_range;
pub mod ts_scoped;
pub mod ts_tiers;
pub mod vector;
pub mod vector_provider;
pub mod view_provider;

use std::collections::HashMap;
use std::sync::Arc;

use selene_core::IStr;
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::SmallVec;

use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

/// A single result row from a procedure.
pub type ProcedureRow = SmallVec<[(IStr, GqlValue); 4]>;

/// Parameter definition for procedure signature.
pub struct ProcedureParam {
    pub name: &'static str,
    pub typ: GqlType,
}

/// Column definition for YIELD.
pub struct YieldColumn {
    pub name: &'static str,
    pub typ: GqlType,
}

/// Procedure signature: parameters and yield columns.
pub struct ProcedureSignature {
    pub params: Vec<ProcedureParam>,
    pub yields: Vec<YieldColumn>,
}

/// A callable procedure for CALL/YIELD in GQL.
pub trait Procedure: Send + Sync {
    fn name(&self) -> &'static str;
    fn signature(&self) -> ProcedureSignature;
    fn execute(
        &self,
        args: &[GqlValue],
        graph: &SeleneGraph,
        hot_tier: Option<&HotTier>,
        scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError>;
}

/// Registry of available procedures, keyed by name.
pub struct ProcedureRegistry {
    procedures: HashMap<IStr, Arc<dyn Procedure>>,
}

impl Default for ProcedureRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcedureRegistry {
    pub fn new() -> Self {
        Self {
            procedures: HashMap::new(),
        }
    }

    /// Return a reference to the lazily-initialized static builtin registry.
    pub fn builtins() -> &'static Self {
        use std::sync::OnceLock;
        static INSTANCE: OnceLock<ProcedureRegistry> = OnceLock::new();
        INSTANCE.get_or_init(ProcedureRegistry::with_builtins)
    }

    /// Create a registry with all built-in procedures.
    pub fn with_builtins() -> Self {
        let mut reg = Self::new();
        // Time-series
        reg.register(Arc::new(ts::TsRange));
        reg.register(Arc::new(ts::TsLatest));
        reg.register(Arc::new(ts::TsValueAt));
        reg.register(Arc::new(ts::TsAggregate));
        reg.register(Arc::new(ts::TsWindow));
        reg.register(Arc::new(ts::TsDownsample));
        reg.register(Arc::new(ts::TsHistory));
        reg.register(Arc::new(ts::TsFullRange));
        reg.register(Arc::new(ts::TsTrends));
        reg.register(Arc::new(ts_percentile::TsPercentile));
        reg.register(Arc::new(ts_scoped::TsScopedAggregate));
        reg.register(Arc::new(ts_gaps::TsGaps));
        reg.register(Arc::new(ts_anomalies::TsAnomalies));
        reg.register(Arc::new(ts_anomalies::TsPeerAnomalies));
        // Schema introspection
        reg.register(Arc::new(graph::GraphLabels));
        reg.register(Arc::new(graph::GraphEdgeTypes));
        reg.register(Arc::new(graph::GraphNodeCount));
        reg.register(Arc::new(graph::GraphEdgeCount));
        // Schema introspection + discovery
        reg.register(Arc::new(graph::GraphSchema));
        reg.register(Arc::new(graph::GraphConstraints));
        reg.register(Arc::new(graph::GraphDiscoverSchema));
        // Graph algorithms
        let catalog = algorithms::new_shared_catalog();
        algorithms::register_algorithm_procedures(&mut reg, catalog.clone());
        // Vector search
        reg.register(Arc::new(vector::VectorSearch));
        reg.register(Arc::new(vector::SimilarNodes));
        reg.register(Arc::new(vector::ScopedVectorSearch));
        reg.register(Arc::new(vector::RebuildVectorIndex));
        #[cfg(feature = "vector")]
        reg.register(Arc::new(vector::SemanticSearch));
        // Community-enhanced RAG (vector + Louvain)
        #[cfg(feature = "vector")]
        reg.register(Arc::new(community_search::CommunitySearch {
            catalog: catalog.clone(),
        }));
        // Full-text search
        reg.register(Arc::new(search::TextSearch));
        #[cfg(feature = "vector")]
        reg.register(Arc::new(search::HybridSearch));
        // Change history + temporal versioning
        reg.register(Arc::new(history::GraphHistory));
        reg.register(Arc::new(history::GraphChanges));
        reg.register(Arc::new(history::GraphPropertyAt));
        reg.register(Arc::new(history::GraphPropertyHistory));
        // RDF import/export + SPARQL
        reg.register(Arc::new(rdf::ExportRdf));
        reg.register(Arc::new(rdf::ImportRdf));
        reg.register(Arc::new(rdf::SparqlQuery));
        // Schema audit
        reg.register(Arc::new(schema_audit::SchemaAudit));
        reg.register(Arc::new(schema_audit::SchemaAuditDetails));
        // Schema dump (LLM-friendly)
        reg.register(Arc::new(schema_dump::SchemaDump));
        // GraphRAG hybrid retriever (ai feature)
        #[cfg(feature = "ai")]
        reg.register(Arc::new(graphrag::GraphRagSearch));
        // Agent memory recall (ai feature)
        #[cfg(feature = "ai")]
        reg.register(Arc::new(memory::MemoryRecall));
        reg
    }

    pub fn register(&mut self, proc: Arc<dyn Procedure>) {
        self.procedures.insert(IStr::new(proc.name()), proc);
    }

    pub fn get(&self, name: &IStr) -> Option<&Arc<dyn Procedure>> {
        self.procedures.get(name)
    }
}
