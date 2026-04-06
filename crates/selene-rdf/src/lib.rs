#![forbid(unsafe_code)]
//! RDF import/export and SPARQL query adapter for Selene.
//!
//! Phase 1: Turtle, N-Triples, N-Quads import/export via oxrdf + oxttl.
//! Phase 2: SPARQL query via QueryableDataset adapter (--features sparql).

pub mod export;
pub mod import;
pub mod mapping;
pub mod namespace;
pub mod observation;
pub mod ontology;
pub mod terms;

pub mod adapter;
pub mod sparql;

/// Supported RDF serialization formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdfFormat {
    Turtle,
    NTriples,
    NQuads,
}

impl std::str::FromStr for RdfFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "turtle" | "ttl" => Ok(Self::Turtle),
            "ntriples" | "nt" => Ok(Self::NTriples),
            "nquads" | "nq" => Ok(Self::NQuads),
            _ => Err(format!("unsupported RDF format: {s}")),
        }
    }
}

impl RdfFormat {
    pub fn content_type(&self) -> &'static str {
        match self {
            Self::Turtle => "text/turtle",
            Self::NTriples => "application/n-triples",
            Self::NQuads => "application/n-quads",
        }
    }
}

/// Default maximum number of quads allowed during a single RDF import.
pub const DEFAULT_MAX_QUADS: usize = 1_000_000;

/// Error type for RDF operations.
#[derive(Debug, thiserror::Error)]
pub enum RdfError {
    #[error("RDF parse error: {0}")]
    Parse(String),
    #[error("RDF serialization error: {0}")]
    Serialize(String),
    #[error("graph error: {0}")]
    Graph(#[from] selene_graph::GraphError),
    #[error("namespace error: {0}")]
    Namespace(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("import exceeds maximum quad count ({0})")]
    TooManyQuads(usize),
}

/// Result of an RDF import operation.
#[derive(Debug, Default)]
pub struct RdfImportResult {
    pub nodes_created: usize,
    pub edges_created: usize,
    pub labels_added: usize,
    pub properties_set: usize,
    pub ontology_triples_loaded: usize,
}
