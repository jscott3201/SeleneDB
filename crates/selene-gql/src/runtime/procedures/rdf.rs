//! RDF import/export procedures: graph.exportRdf and graph.importRdf.
//!
//! These procedures delegate to an RdfProvider set via a static OnceLock
//! (same pattern as SearchProvider and VectorProvider). The RdfProvider is
//! set at server startup from bootstrap.rs.

use std::sync::{Arc, OnceLock};

use selene_core::IStr;
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;

use super::{Procedure, ProcedureParam, ProcedureRow, ProcedureSignature, YieldColumn};
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

// ── RdfProvider trait ──────────────────────────────────────────────

/// Import statistics returned by the provider.
#[derive(Debug, Default)]
pub struct RdfImportStats {
    pub nodes_created: usize,
    pub edges_created: usize,
    pub labels_added: usize,
    pub properties_set: usize,
    pub ontology_triples_loaded: usize,
}

/// Trait for RDF operations (decouples selene-gql from selene-rdf/selene-server).
pub trait RdfProvider: Send + Sync {
    /// Export the graph to RDF bytes in the given format.
    ///
    /// `format` is one of "turtle", "ntriples", "nquads".
    /// `include_all_graphs` controls whether ontology quads are included (N-Quads only).
    fn export(&self, format: &str, include_all_graphs: bool) -> Result<Vec<u8>, String>;

    /// Import RDF data into the graph and/or ontology store.
    ///
    /// `data` is the raw RDF bytes.
    /// `format` is one of "turtle", "ntriples", "nquads".
    /// `target_graph` is optional; "ontology" routes all triples to the ontology store.
    fn import(
        &self,
        data: &[u8],
        format: &str,
        target_graph: Option<&str>,
    ) -> Result<RdfImportStats, String>;

    /// Execute a SPARQL query and return JSON-serialized results.
    ///
    /// Returns the serialized result as a string. Only available when the
    /// server was compiled with SPARQL support (`--features rdf-sparql`).
    fn sparql(&self, query: &str) -> Result<String, String>;
}

static RDF_PROVIDER: OnceLock<Arc<dyn RdfProvider>> = OnceLock::new();

/// Set the RDF provider. Called once at server startup.
pub fn set_rdf_provider(provider: Arc<dyn RdfProvider>) {
    let _ = RDF_PROVIDER.set(provider);
}

fn get_rdf_provider() -> Result<&'static Arc<dyn RdfProvider>, GqlError> {
    RDF_PROVIDER.get().ok_or_else(|| GqlError::InvalidArgument {
        message: "RDF not available (--features rdf not enabled or provider not initialized)"
            .into(),
    })
}

// ── graph.exportRdf ────────────────────────────────────────────────

/// `CALL graph.exportRdf('turtle')` or `CALL graph.exportRdf('nquads', true)`
///
/// Returns a single row with `data` (the serialized RDF as a string) and
/// `format` (the format used).
pub struct ExportRdf;

impl Procedure for ExportRdf {
    fn name(&self) -> &'static str {
        "graph.exportRdf"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![ProcedureParam {
                name: "format",
                typ: GqlType::String,
            }],
            yields: vec![
                YieldColumn {
                    name: "data",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "format",
                    typ: GqlType::String,
                },
            ],
        }
    }

    fn execute(
        &self,
        args: &[GqlValue],
        _graph: &SeleneGraph,
        _hot_tier: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        if args.is_empty() {
            return Err(GqlError::InvalidArgument {
                message: "graph.exportRdf requires 1 argument: format".into(),
            });
        }

        let format = args[0].as_str()?;
        let include_all = args
            .get(1)
            .and_then(|v| match v {
                GqlValue::Bool(b) => Some(*b),
                _ => None,
            })
            .unwrap_or(false);

        let provider = get_rdf_provider()?;
        let bytes = provider
            .export(format, include_all)
            .map_err(|e| GqlError::InvalidArgument { message: e })?;

        let data = String::from_utf8(bytes).map_err(|e| GqlError::InvalidArgument {
            message: format!("RDF output is not valid UTF-8: {e}"),
        })?;

        Ok(vec![smallvec![
            (IStr::new("data"), GqlValue::String(data.into())),
            (IStr::new("format"), GqlValue::String(format.into())),
        ]])
    }
}

// ── graph.importRdf ────────────────────────────────────────────────

/// `CALL graph.importRdf(data, 'turtle')` or `CALL graph.importRdf(data, 'turtle', 'ontology')`
///
/// Returns a single row with import statistics.
pub struct ImportRdf;

impl Procedure for ImportRdf {
    fn name(&self) -> &'static str {
        "graph.importRdf"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "data",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "format",
                    typ: GqlType::String,
                },
            ],
            yields: vec![
                YieldColumn {
                    name: "nodesCreated",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "edgesCreated",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "labelsAdded",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "propertiesSet",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "ontologyTriplesLoaded",
                    typ: GqlType::Int,
                },
            ],
        }
    }

    fn execute(
        &self,
        args: &[GqlValue],
        _graph: &SeleneGraph,
        _hot_tier: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        if args.len() < 2 {
            return Err(GqlError::InvalidArgument {
                message: "graph.importRdf requires 2 arguments: data, format".into(),
            });
        }

        let data = args[0].as_str()?;
        let format = args[1].as_str()?;
        let target_graph = args.get(2).and_then(|v| v.as_str().ok());

        let provider = get_rdf_provider()?;
        let stats = provider
            .import(data.as_bytes(), format, target_graph)
            .map_err(|e| GqlError::InvalidArgument { message: e })?;

        Ok(vec![smallvec![
            (
                IStr::new("nodesCreated"),
                GqlValue::Int(stats.nodes_created as i64)
            ),
            (
                IStr::new("edgesCreated"),
                GqlValue::Int(stats.edges_created as i64)
            ),
            (
                IStr::new("labelsAdded"),
                GqlValue::Int(stats.labels_added as i64)
            ),
            (
                IStr::new("propertiesSet"),
                GqlValue::Int(stats.properties_set as i64)
            ),
            (
                IStr::new("ontologyTriplesLoaded"),
                GqlValue::Int(stats.ontology_triples_loaded as i64)
            ),
        ]])
    }
}

// -- graph.sparql -----------------------------------------------------------

/// `CALL graph.sparql('SELECT ?s WHERE { ?s a <selene:type/Sensor> }') YIELD results`
///
/// Executes a SPARQL query against the current graph (viewed as an RDF dataset)
/// and returns the serialized results as a JSON string.
pub struct SparqlQuery;

impl Procedure for SparqlQuery {
    fn name(&self) -> &'static str {
        "graph.sparql"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![ProcedureParam {
                name: "query",
                typ: GqlType::String,
            }],
            yields: vec![YieldColumn {
                name: "results",
                typ: GqlType::String,
            }],
        }
    }

    fn execute(
        &self,
        args: &[GqlValue],
        _graph: &SeleneGraph,
        _hot_tier: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        if args.is_empty() {
            return Err(GqlError::InvalidArgument {
                message: "graph.sparql requires 1 argument: query".into(),
            });
        }

        let query = args[0].as_str()?;

        let provider = get_rdf_provider()?;
        let json = provider
            .sparql(query)
            .map_err(|e| GqlError::InvalidArgument { message: e })?;

        Ok(vec![smallvec![(
            IStr::new("results"),
            GqlValue::String(json.into()),
        )]])
    }
}
