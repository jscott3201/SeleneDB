//! RDF service -- server-side RdfProvider implementation + service wrapper.
//!
//! Bridges selene-rdf (import/export) with selene-gql (GQL procedures) via
//! the RdfProvider OnceLock pattern, following the same approach as
//! VectorProvider and SearchProvider.

use std::sync::Arc;

use arc_swap::ArcSwap;
use parking_lot::RwLock;
use selene_graph::{CsrAdjacency, SharedGraph};
use selene_rdf::namespace::RdfNamespace;
use selene_rdf::ontology::OntologyStore;

use crate::service_registry::{Service, ServiceHealth};

/// Server-side implementation of `RdfProvider` for GQL procedures.
///
/// Holds a SharedGraph (for import mutations), RdfNamespace (from config),
/// an Arc<RwLock<OntologyStore>> (for ontology access), and a shared
/// generation-gated CSR cache for SPARQL query acceleration.
pub struct ServerRdfProvider {
    graph: SharedGraph,
    namespace: RdfNamespace,
    ontology: Arc<RwLock<OntologyStore>>,
    csr_cache: Arc<ArcSwap<(u64, Arc<CsrAdjacency>)>>,
}

impl ServerRdfProvider {
    pub fn new(
        graph: SharedGraph,
        namespace: RdfNamespace,
        ontology: Arc<RwLock<OntologyStore>>,
        csr_cache: Arc<ArcSwap<(u64, Arc<CsrAdjacency>)>>,
    ) -> Self {
        Self {
            graph,
            namespace,
            ontology,
            csr_cache,
        }
    }
}

impl selene_gql::runtime::procedures::rdf::RdfProvider for ServerRdfProvider {
    fn export(&self, format: &str, include_all_graphs: bool) -> Result<Vec<u8>, String> {
        let fmt: selene_rdf::RdfFormat = format.parse()?;

        let snap = self.graph.load_snapshot();
        let ontology_guard = self.ontology.read();
        let ontology_ref = if ontology_guard.is_empty() {
            None
        } else {
            Some(&*ontology_guard)
        };

        selene_rdf::export::export_graph(
            &snap,
            &self.namespace,
            fmt,
            ontology_ref,
            include_all_graphs,
        )
        .map_err(|e| e.to_string())
    }

    fn import(
        &self,
        data: &[u8],
        format: &str,
        target_graph: Option<&str>,
    ) -> Result<selene_gql::runtime::procedures::rdf::RdfImportStats, String> {
        let fmt: selene_rdf::RdfFormat = format.parse()?;

        let mut ontology = self.ontology.write();

        let result = selene_rdf::import::import_rdf(
            data,
            fmt,
            target_graph,
            &self.graph,
            &self.namespace,
            &mut ontology,
        )
        .map_err(|e| e.to_string())?;

        Ok(selene_gql::runtime::procedures::rdf::RdfImportStats {
            nodes_created: result.nodes_created,
            edges_created: result.edges_created,
            labels_added: result.labels_added,
            properties_set: result.properties_set,
            ontology_triples_loaded: result.ontology_triples_loaded,
        })
    }

    fn sparql(&self, query: &str) -> Result<String, String> {
        let snap = self.graph.load_snapshot();
        let csr = crate::bootstrap::get_or_build_csr(&self.csr_cache, &snap);
        let ontology_guard = self.ontology.read();
        let ontology_ref = if ontology_guard.is_empty() {
            None
        } else {
            Some(&*ontology_guard)
        };

        let (bytes, _content_type) = selene_rdf::sparql::execute_sparql(
            &snap,
            &csr,
            &self.namespace,
            ontology_ref,
            query,
            selene_rdf::sparql::SparqlResultFormat::Json,
        )
        .map_err(|e| e.to_string())?;

        String::from_utf8(bytes).map_err(|e| format!("SPARQL result is not valid UTF-8: {e}"))
    }
}

// ── Service wrapper ──────────────────────────────────────────────────

/// RDF ontology as a registered service in the ServiceRegistry.
pub struct RdfOntologyService {
    #[allow(dead_code)] // Held as service registration; accessed via ServiceRegistry
    pub ontology: Arc<RwLock<OntologyStore>>,
}

impl RdfOntologyService {
    pub fn new(ontology: Arc<RwLock<OntologyStore>>) -> Self {
        Self { ontology }
    }
}

impl Service for RdfOntologyService {
    fn name(&self) -> &'static str {
        "rdf"
    }

    fn health(&self) -> ServiceHealth {
        ServiceHealth::Healthy
    }
}
