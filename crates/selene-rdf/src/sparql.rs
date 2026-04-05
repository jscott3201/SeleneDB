//! SPARQL query execution convenience layer.
//!
//! Wraps spargebra (parsing), spareval (evaluation), and sparesults (serialization)
//! into a single `execute_sparql()` entry point. The adapter module provides the
//! `SeleneDataset` that bridges Selene's property graph to the spareval engine.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use oxrdf::Variable;
use sparesults::{QueryResultsFormat, QueryResultsSerializer};
use spareval::{QueryEvaluator, QueryResults};
use spargebra::{Query, SparqlParser};

use selene_graph::SeleneGraph;
use selene_graph::csr::CsrAdjacency;

use crate::adapter::SeleneDataset;
use crate::namespace::RdfNamespace;
use crate::ontology::OntologyStore;

/// Global SPARQL parse cache. Keyed by query string hash, stores parsed ASTs.
/// Bounded to prevent unbounded memory growth in long-running servers.
static SPARQL_CACHE: OnceLock<Mutex<HashMap<u64, Arc<Query>>>> = OnceLock::new();

/// Maximum number of cached SPARQL query ASTs before eviction.
const SPARQL_CACHE_CAPACITY: usize = 512;

/// Parse a SPARQL query, returning a cached AST on cache hit.
fn get_or_parse_query(query_str: &str) -> Result<Arc<Query>, SparqlError> {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    query_str.hash(&mut hasher);
    let key = hasher.finish();

    let cache = SPARQL_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    {
        let guard = cache.lock().unwrap();
        if let Some(q) = guard.get(&key) {
            return Ok(Arc::clone(q));
        }
    }

    let parsed = SparqlParser::new()
        .parse_query(query_str)
        .map_err(|e| SparqlError::Parse(e.to_string()))?;
    let arc = Arc::new(parsed);

    let mut guard = cache.lock().unwrap();
    // Clear-when-full eviction: the cache refills with the active working set.
    if guard.len() >= SPARQL_CACHE_CAPACITY {
        guard.clear();
    }
    guard.insert(key, Arc::clone(&arc));
    Ok(arc)
}

/// Supported SPARQL result formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SparqlResultFormat {
    Json,
    Xml,
    Csv,
    Tsv,
}

impl std::str::FromStr for SparqlResultFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" | "sparql-results+json" => Ok(Self::Json),
            "xml" | "sparql-results+xml" => Ok(Self::Xml),
            "csv" => Ok(Self::Csv),
            "tsv" => Ok(Self::Tsv),
            _ => Err(format!("unsupported SPARQL result format: {s}")),
        }
    }
}

impl SparqlResultFormat {
    fn to_sparesults_format(self) -> QueryResultsFormat {
        match self {
            Self::Json => QueryResultsFormat::Json,
            Self::Xml => QueryResultsFormat::Xml,
            Self::Csv => QueryResultsFormat::Csv,
            Self::Tsv => QueryResultsFormat::Tsv,
        }
    }

    /// The HTTP content type for this format.
    pub fn content_type(self) -> &'static str {
        self.to_sparesults_format().media_type()
    }
}

/// Execute a SPARQL query against a Selene graph and return serialized results.
///
/// For SELECT/ASK queries, results are serialized in the SPARQL Results format
/// (JSON, XML, CSV, or TSV). For CONSTRUCT/DESCRIBE queries, results are
/// serialized as N-Triples (the format parameter is ignored for graph results).
///
/// Returns the serialized bytes and content type string.
pub fn execute_sparql(
    graph: &SeleneGraph,
    csr: &CsrAdjacency,
    namespace: &RdfNamespace,
    ontology: Option<&OntologyStore>,
    query_str: &str,
    format: SparqlResultFormat,
) -> Result<(Vec<u8>, &'static str), SparqlError> {
    // Parse the SPARQL query (cached).
    let query = get_or_parse_query(query_str)?;

    // Build the dataset adapter.
    let dataset = SeleneDataset::new(graph, csr, namespace, ontology);

    // Execute the query. `execute` takes the dataset by value.
    let results = QueryEvaluator::new()
        .prepare(&query)
        .execute(dataset)
        .map_err(|e| SparqlError::Evaluation(e.to_string()))?;

    // Serialize the results.
    match results {
        QueryResults::Solutions(solutions) => {
            let variables: Vec<Variable> = solutions.variables().to_vec();
            let sparesults_format = format.to_sparesults_format();
            let content_type = format.content_type();

            let mut buffer = Vec::with_capacity(4096);
            let serializer = QueryResultsSerializer::from_format(sparesults_format);
            let mut writer = serializer
                .serialize_solutions_to_writer(&mut buffer, variables)
                .map_err(|e| SparqlError::Serialization(e.to_string()))?;

            for solution in solutions {
                let solution = solution.map_err(|e| SparqlError::Evaluation(e.to_string()))?;
                writer
                    .serialize(&solution)
                    .map_err(|e| SparqlError::Serialization(e.to_string()))?;
            }
            writer
                .finish()
                .map_err(|e| SparqlError::Serialization(e.to_string()))?;

            Ok((buffer, content_type))
        }
        QueryResults::Boolean(value) => {
            let sparesults_format = format.to_sparesults_format();
            let content_type = format.content_type();

            let mut buffer = Vec::with_capacity(256);
            let serializer = QueryResultsSerializer::from_format(sparesults_format);
            serializer
                .serialize_boolean_to_writer(&mut buffer, value)
                .map_err(|e| SparqlError::Serialization(e.to_string()))?;

            Ok((buffer, content_type))
        }
        QueryResults::Graph(triples) => {
            // CONSTRUCT/DESCRIBE results: serialize as N-Triples.
            use std::io::Write;
            let mut buffer = Vec::with_capacity(4096);
            for triple in triples {
                let triple = triple.map_err(|e| SparqlError::Evaluation(e.to_string()))?;
                writeln!(&mut buffer, "{triple} .")
                    .map_err(|e: std::io::Error| SparqlError::Serialization(e.to_string()))?;
            }
            Ok((buffer, "application/n-triples"))
        }
    }
}

/// Errors from SPARQL query execution.
#[derive(Debug, thiserror::Error)]
pub enum SparqlError {
    #[error("SPARQL parse error: {0}")]
    Parse(String),
    #[error("SPARQL evaluation error: {0}")]
    Evaluation(String),
    #[error("SPARQL serialization error: {0}")]
    Serialization(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    use selene_core::interner::IStr;
    use selene_core::label_set::LabelSet;
    use selene_core::property_map::PropertyMap;
    use selene_core::value::Value;
    use selene_graph::SeleneGraph;
    use selene_graph::csr::CsrAdjacency;

    use crate::namespace::RdfNamespace;

    const NS: &str = "https://example.com/building/";

    /// Build a small test graph:
    ///   Node 1: labels=["Sensor"], properties={unit: "degC"}
    ///   Node 2: labels=["Room"], properties={name: "Lab"}
    ///   Edge:   node1 -[locatedIn]-> node2
    fn build_graph() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        let n1 = m
            .create_node(
                LabelSet::from_strs(&["Sensor"]),
                PropertyMap::from_pairs([(IStr::new("unit"), Value::String("degC".into()))]),
            )
            .unwrap();
        let n2 = m
            .create_node(
                LabelSet::from_strs(&["Room"]),
                PropertyMap::from_pairs([(IStr::new("name"), Value::String("Lab".into()))]),
            )
            .unwrap();
        m.create_edge(n1, IStr::new("locatedIn"), n2, PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();
        g
    }

    const TYPE_SENSOR: &str = "https://example.com/building/type/Sensor";
    const TYPE_THERMOSTAT: &str = "https://example.com/building/type/Thermostat";

    /// SELECT query returning all sensors.
    fn select_sensors() -> String {
        format!("SELECT ?s WHERE {{ ?s a <{TYPE_SENSOR}> }}")
    }

    #[test]
    fn select_returns_json() {
        let g = build_graph();
        let csr = CsrAdjacency::build(&g);
        let ns = RdfNamespace::new(NS);
        let (body, ct) = execute_sparql(
            &g,
            &csr,
            &ns,
            None,
            &select_sensors(),
            SparqlResultFormat::Json,
        )
        .expect("query should succeed");
        assert_eq!(ct, "application/sparql-results+json");
        let text = String::from_utf8(body).unwrap();
        assert!(
            text.contains("node/1"),
            "result should reference node/1: {text}"
        );
    }

    #[test]
    fn ask_true() {
        let g = build_graph();
        let csr = CsrAdjacency::build(&g);
        let ns = RdfNamespace::new(NS);
        let query = format!("ASK {{ ?s a <{TYPE_SENSOR}> }}");
        let (body, _ct) = execute_sparql(&g, &csr, &ns, None, &query, SparqlResultFormat::Json)
            .expect("query should succeed");
        let text = String::from_utf8(body).unwrap();
        assert!(text.contains("true"), "ASK should return true: {text}");
    }

    #[test]
    fn ask_false() {
        let g = build_graph();
        let csr = CsrAdjacency::build(&g);
        let ns = RdfNamespace::new(NS);
        let query = format!("ASK {{ ?s a <{TYPE_THERMOSTAT}> }}");
        let (body, _ct) = execute_sparql(&g, &csr, &ns, None, &query, SparqlResultFormat::Json)
            .expect("query should succeed");
        let text = String::from_utf8(body).unwrap();
        assert!(text.contains("false"), "ASK should return false: {text}");
    }

    #[test]
    fn construct_returns_ntriples() {
        let g = build_graph();
        let csr = CsrAdjacency::build(&g);
        let ns = RdfNamespace::new(NS);
        let query =
            format!("CONSTRUCT {{ ?s a <{TYPE_SENSOR}> }} WHERE {{ ?s a <{TYPE_SENSOR}> }}");
        let (_body, ct) = execute_sparql(&g, &csr, &ns, None, &query, SparqlResultFormat::Json)
            .expect("query should succeed");
        assert_eq!(ct, "application/n-triples");
    }

    #[test]
    fn parse_cache_hit() {
        let g = build_graph();
        let csr = CsrAdjacency::build(&g);
        let ns = RdfNamespace::new(NS);
        let query = select_sensors();
        let (body1, _) = execute_sparql(&g, &csr, &ns, None, &query, SparqlResultFormat::Json)
            .expect("first call should succeed");
        let (body2, _) = execute_sparql(&g, &csr, &ns, None, &query, SparqlResultFormat::Json)
            .expect("second call (cache hit) should succeed");
        assert_eq!(body1, body2, "cached and uncached results should match");
    }

    #[test]
    fn parse_error_returns_err() {
        let g = build_graph();
        let csr = CsrAdjacency::build(&g);
        let ns = RdfNamespace::new(NS);
        let result = execute_sparql(
            &g,
            &csr,
            &ns,
            None,
            "NOT VALID SPARQL",
            SparqlResultFormat::Json,
        );
        assert!(result.is_err(), "invalid SPARQL should return an error");
        let err = result.unwrap_err();
        assert!(
            matches!(err, SparqlError::Parse(_)),
            "error should be SparqlError::Parse, got: {err:?}"
        );
    }

    #[test]
    fn xml_format() {
        let g = build_graph();
        let csr = CsrAdjacency::build(&g);
        let ns = RdfNamespace::new(NS);
        let (_body, ct) = execute_sparql(
            &g,
            &csr,
            &ns,
            None,
            &select_sensors(),
            SparqlResultFormat::Xml,
        )
        .expect("query should succeed");
        assert_eq!(ct, "application/sparql-results+xml");
    }

    #[test]
    fn csv_format() {
        let g = build_graph();
        let csr = CsrAdjacency::build(&g);
        let ns = RdfNamespace::new(NS);
        let (_body, ct) = execute_sparql(
            &g,
            &csr,
            &ns,
            None,
            &select_sensors(),
            SparqlResultFormat::Csv,
        )
        .expect("query should succeed");
        assert_eq!(ct, "text/csv; charset=utf-8");
    }
}
