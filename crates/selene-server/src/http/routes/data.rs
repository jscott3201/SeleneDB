//! Data import/export handlers: CSV, RDF, SPARQL, ReactFlow, time-series, snapshots.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use selene_wire::dto::ts::TsSampleDto;
use serde::Deserialize;

use super::reject_if_replica;
use crate::bootstrap::ServerState;
use crate::http::auth::HttpAuth;
use crate::http::error::HttpError;
use crate::ops::{self, OpError};

// ── Time-Series ──────────────────────────────────────────────────────

#[derive(Deserialize)]
pub(in crate::http) struct TsWriteBody {
    samples: Vec<TsSampleDto>,
}

pub(in crate::http) async fn ts_write(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Json(body): Json<TsWriteBody>,
) -> Result<impl IntoResponse, HttpError> {
    let auth = auth.0;
    let st = Arc::clone(&state);
    let samples = body.samples;
    let count = state
        .mutation_batcher
        .submit(move || ops::ts::ts_write(&st, &auth, samples))
        .await
        .map_err(HttpError::from_graph_error)??;
    Ok(Json(serde_json::json!({ "written": count })))
}

#[derive(Deserialize)]
pub(in crate::http) struct TsQueryParams {
    #[serde(default)]
    start: Option<i64>,
    #[serde(default)]
    end: Option<i64>,
    #[serde(default)]
    limit: Option<usize>,
}

pub(in crate::http) async fn ts_query(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Path((entity_id, property)): Path<(u64, String)>,
    Query(params): Query<TsQueryParams>,
) -> Result<impl IntoResponse, HttpError> {
    let auth = auth.0;
    let start = params.start.unwrap_or(0);
    let end = params.end.unwrap_or(i64::MAX);
    let samples = ops::ts::ts_range(
        &state,
        &auth,
        entity_id,
        &property,
        start,
        end,
        params.limit,
    )?;
    Ok(Json(samples))
}

// ── React Flow ──────────────────────────────────────────────────────

#[derive(Deserialize)]
pub(in crate::http) struct RFExportQuery {
    #[serde(default)]
    label: Option<String>,
}

pub(in crate::http) async fn reactflow_export(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Query(params): Query<RFExportQuery>,
) -> Result<impl IntoResponse, HttpError> {
    let auth = auth.0;
    let graph = ops::reactflow::export_reactflow(&state, &auth, params.label.as_deref());
    Ok(Json(serde_json::json!(graph)))
}

pub(in crate::http) async fn reactflow_import(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Json(graph): Json<ops::reactflow::RFGraph>,
) -> Result<impl IntoResponse, HttpError> {
    reject_if_replica(&state)?;
    let auth = auth.0;
    let st = Arc::clone(&state);
    let result = state
        .mutation_batcher
        .submit(move || ops::reactflow::import_reactflow(&st, &auth, graph))
        .await
        .map_err(HttpError::from_graph_error)??;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "nodes_created": result.nodes_created,
            "edges_created": result.edges_created,
            "id_map": result.id_map,
        })),
    ))
}

// ── CSV Import/Export ───────────────────────────────────────────────

#[derive(Deserialize)]
pub(in crate::http) struct CsvImportParams {
    label: Option<String>,
    delimiter: Option<String>,
    parent_id_column: Option<String>,
    /// "nodes" or "edges"
    #[serde(default = "default_csv_type")]
    r#type: String,
    /// For edges: source column name
    source_column: Option<String>,
    /// For edges: target column name
    target_column: Option<String>,
    /// For edges: label column name
    label_column: Option<String>,
}

fn default_csv_type() -> String {
    "nodes".into()
}

pub(in crate::http) async fn csv_import(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Query(params): Query<CsvImportParams>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, HttpError> {
    reject_if_replica(&state)?;
    let reader = std::io::Cursor::new(body);
    let delimiter = params
        .delimiter
        .as_deref()
        .unwrap_or(",")
        .as_bytes()
        .first()
        .copied()
        .unwrap_or(b',');

    let auth = auth.0;
    let st = Arc::clone(&state);
    let is_edges = params.r#type == "edges";

    let result = if is_edges {
        let config = ops::csv_io::CsvEdgeImportConfig {
            source_column: params.source_column.unwrap_or_else(|| "source_id".into()),
            target_column: params.target_column.unwrap_or_else(|| "target_id".into()),
            label_column: params.label_column.unwrap_or_else(|| "label".into()),
            delimiter,
        };
        state
            .mutation_batcher
            .submit(move || ops::csv_io::import_edges_csv(&st, &auth, reader, &config))
            .await
            .map_err(HttpError::from_graph_error)?
    } else {
        let label = params.label.ok_or_else(|| {
            HttpError::bad_request("label query parameter required for node import")
        })?;
        let config = ops::csv_io::CsvNodeImportConfig {
            label,
            delimiter,
            column_mappings: None,
            parent_id_column: params.parent_id_column,
        };
        state
            .mutation_batcher
            .submit(move || ops::csv_io::import_nodes_csv(&st, &auth, reader, &config))
            .await
            .map_err(HttpError::from_graph_error)?
    };

    match result {
        Ok(r) => Ok((StatusCode::CREATED, Json(serde_json::json!(r)))),
        Err(e) => Err(HttpError::from(e)),
    }
}

#[derive(Deserialize)]
pub(in crate::http) struct CsvExportParams {
    label: Option<String>,
    /// "nodes" (default) or "edges".
    #[serde(default = "default_csv_type")]
    r#type: String,
}

pub(in crate::http) async fn csv_export(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Query(params): Query<CsvExportParams>,
) -> Result<impl IntoResponse, HttpError> {
    let csv_data = if params.r#type == "edges" {
        ops::csv_io::export_edges_csv(&state, &auth.0, params.label.as_deref())
            .map_err(HttpError::from)?
    } else {
        ops::csv_io::export_nodes_csv(&state, &auth.0, params.label.as_deref())
            .map_err(HttpError::from)?
    };

    Ok((
        StatusCode::OK,
        [("content-type", "text/csv; charset=utf-8")],
        csv_data,
    ))
}

// ── RDF Import/Export ──────────────────────────────────────────────

#[derive(Deserialize)]
pub(in crate::http) struct RdfExportParams {
    format: Option<String>,
    graphs: Option<String>,
}

pub(in crate::http) async fn export_rdf(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Query(params): Query<RdfExportParams>,
) -> Result<impl IntoResponse, HttpError> {
    let _ = auth.0;
    let format_str = params.format.as_deref().unwrap_or("turtle");
    let format: selene_rdf::RdfFormat = format_str
        .parse()
        .map_err(|e: String| HttpError::bad_request(e))?;

    let ns = &state.rdf_namespace;
    let snap = state.graph.load_snapshot();
    let include_all = params.graphs.as_deref() == Some("all");

    let ontology_ref = state.rdf_ontology.as_ref().map(|o| o.read());

    let data =
        selene_rdf::export::export_graph(&snap, ns, format, ontology_ref.as_deref(), include_all)
            .map_err(|e| HttpError(OpError::Internal(e.to_string())))?;

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, format.content_type())],
        data,
    ))
}

#[derive(Deserialize)]
pub(in crate::http) struct RdfImportParams {
    format: Option<String>,
    graph: Option<String>,
}

pub(in crate::http) async fn import_rdf(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Query(params): Query<RdfImportParams>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, HttpError> {
    reject_if_replica(&state)?;
    let _ = auth.0;

    let format_str = params.format.as_deref().unwrap_or("turtle");
    let format: selene_rdf::RdfFormat = format_str
        .parse()
        .map_err(|e: String| HttpError::bad_request(e))?;

    let target_graph = params.graph.clone();
    let ns = state.rdf_namespace.clone();

    // Take write lock on ontology for the duration of the import.
    let ontology_arc = state
        .rdf_ontology
        .as_ref()
        .ok_or_else(|| {
            HttpError(OpError::Internal(
                "RDF ontology store not initialized".into(),
            ))
        })?
        .clone();

    let graph = state.graph.clone();
    let st = Arc::clone(&state);

    let result = st
        .mutation_batcher
        .submit(move || {
            let mut ontology = ontology_arc.write();
            selene_rdf::import::import_rdf(
                &body,
                format,
                target_graph.as_deref(),
                &graph,
                &ns,
                &mut ontology,
            )
            .map_err(|e| selene_graph::error::GraphError::Other(e.to_string()))
        })
        .await
        .map_err(HttpError::from_graph_error)?
        .map_err(|e| HttpError(crate::ops::graph_err(e)))?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "nodes_created": result.nodes_created,
            "edges_created": result.edges_created,
            "labels_added": result.labels_added,
            "properties_set": result.properties_set,
            "ontology_triples_loaded": result.ontology_triples_loaded,
        })),
    ))
}

// -- SPARQL Protocol endpoint ------------------------------------------------
//
// Implements the SPARQL 1.1 Protocol (W3C Recommendation):
//   GET  /sparql?query=...                    (URL-encoded query)
//   POST /sparql  Content-Type: application/sparql-query       (raw body)
//   POST /sparql  Content-Type: application/x-www-form-urlencoded  (query=... in body)
//
// Response format is determined by (in priority order):
//   1. `?format=` query parameter (convenience extension)
//   2. HTTP `Accept` header (SPARQL Protocol standard)
//   3. Default: application/sparql-results+json

#[derive(Deserialize)]
pub(in crate::http) struct SparqlQueryParams {
    query: Option<String>,
    format: Option<String>,
}

/// Resolve the result format from `?format=` param or `Accept` header.
fn resolve_sparql_format(
    format_param: Option<&str>,
    accept_header: Option<&str>,
) -> selene_rdf::sparql::SparqlResultFormat {
    use selene_rdf::sparql::SparqlResultFormat;
    // Priority 1: explicit ?format= parameter
    if let Some(f) = format_param
        && let Ok(fmt) = f.parse()
    {
        return fmt;
    }
    // Priority 2: Accept header content negotiation
    if let Some(accept) = accept_header {
        // Check each media type in the Accept header (simplified: no q-value weighting)
        for media in accept.split(',').map(str::trim) {
            let media_type = media.split(';').next().unwrap_or("").trim();
            match media_type {
                "application/sparql-results+json" | "application/json" => {
                    return SparqlResultFormat::Json;
                }
                "application/sparql-results+xml" | "application/xml" => {
                    return SparqlResultFormat::Xml;
                }
                "text/csv" => return SparqlResultFormat::Csv,
                "text/tab-separated-values" => return SparqlResultFormat::Tsv,
                _ => {}
            }
        }
    }
    // Default
    SparqlResultFormat::Json
}

/// `GET /sparql?query=SELECT...&format=json`
///
/// Without a `query` parameter, returns the SPARQL Service Description (Turtle)
/// per the W3C SPARQL 1.1 Service Description spec.
pub(in crate::http) async fn sparql_get(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    headers: axum::http::HeaderMap,
    Query(params): Query<SparqlQueryParams>,
) -> Result<axum::response::Response, HttpError> {
    let _ = auth.0;

    // No query parameter: return Service Description
    if params.query.is_none() {
        return Ok(sparql_service_description(&state));
    }

    let query_str = params.query.as_deref().unwrap();
    let accept = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok());
    let format = resolve_sparql_format(params.format.as_deref(), accept);
    execute_sparql_handler(&state, query_str, format)
}

/// `POST /sparql`
///
/// Accepts three content types per the SPARQL Protocol:
/// - `application/sparql-query`: raw SPARQL query in the body
/// - `application/sparql-update`: raw SPARQL Update in the body
/// - `application/x-www-form-urlencoded`: `query=...` or `update=...` in body
pub(in crate::http) async fn sparql_post(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    headers: axum::http::HeaderMap,
    Query(params): Query<SparqlQueryParams>,
    body: axum::body::Bytes,
) -> Result<axum::response::Response, HttpError> {
    let _ = auth.0;

    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    // SPARQL Update path
    if content_type.starts_with("application/sparql-update") {
        let update_str = std::str::from_utf8(&body)
            .map_err(|_| HttpError::bad_request("request body is not valid UTF-8"))?;
        if update_str.is_empty() {
            return Err(HttpError::bad_request("empty SPARQL Update body"));
        }
        return execute_sparql_update_handler(&state, update_str);
    }

    // Form-encoded path: check for `update=` or `query=` field
    if content_type.starts_with("application/x-www-form-urlencoded") {
        let body_str = std::str::from_utf8(&body)
            .map_err(|_| HttpError::bad_request("request body is not valid UTF-8"))?;
        // Check for update= field first (SPARQL Update via form)
        if let Ok(update_str) = form_decode_field(body_str, "update") {
            return execute_sparql_update_handler(&state, &update_str);
        }
        // Fall through to query= field
        let query_str = form_decode_field(body_str, "query").map_err(|_| {
            HttpError::bad_request("missing 'query' or 'update' field in form-encoded body")
        })?;
        let accept = headers
            .get(axum::http::header::ACCEPT)
            .and_then(|v| v.to_str().ok());
        let format = resolve_sparql_format(params.format.as_deref(), accept);
        return execute_sparql_handler(&state, &query_str, format);
    }

    // Default: raw SPARQL query in body (application/sparql-query)
    let query_str = std::str::from_utf8(&body)
        .map_err(|_| HttpError::bad_request("request body is not valid UTF-8"))?;
    if query_str.is_empty() {
        return Err(HttpError::bad_request("empty SPARQL query body"));
    }
    let accept = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok());
    let format = resolve_sparql_format(params.format.as_deref(), accept);
    execute_sparql_handler(&state, query_str, format)
}

/// Extract a named field from a URL-encoded form body.
fn form_decode_field(body: &str, field_name: &str) -> Result<String, HttpError> {
    for (key, value) in form_urlencoded::parse(body.as_bytes()) {
        if key == field_name {
            if value.is_empty() {
                return Err(HttpError::bad_request(format!(
                    "empty '{field_name}' field in form body"
                )));
            }
            return Ok(value.into_owned());
        }
    }
    Err(HttpError::bad_request(format!(
        "missing '{field_name}' field in form-encoded body"
    )))
}

/// Execute a SPARQL Update and return a JSON summary.
fn execute_sparql_update_handler(
    state: &ServerState,
    update_str: &str,
) -> Result<axum::response::Response, HttpError> {
    let ns = &state.rdf_namespace;
    let mut graph = state.graph.inner().write();
    let result = selene_rdf::update::execute_update(&mut graph, ns, update_str)
        .map_err(|e| HttpError(OpError::QueryError(e.to_string())))?;
    drop(graph);
    state.graph.publish_snapshot();

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "success",
            "nodes_created": result.nodes_created,
            "properties_set": result.properties_set,
            "properties_removed": result.properties_removed,
            "labels_added": result.labels_added,
            "labels_removed": result.labels_removed,
            "edges_created": result.edges_created,
            "edges_deleted": result.edges_deleted,
        })),
    )
        .into_response())
}

/// SPARQL 1.1 Service Description (W3C Recommendation).
///
/// Returned as Turtle when GET /sparql has no query parameter. Describes
/// the endpoint's supported features, result formats, and update capabilities.
fn sparql_service_description(state: &ServerState) -> axum::response::Response {
    let base = format!(
        "http://{}:{}/sparql",
        state.config.http.listen_addr.ip(),
        state.config.http.listen_addr.port()
    );
    let turtle = format!(
        r"@prefix sd: <http://www.w3.org/ns/sparql-service-description#> .
@prefix void: <http://rdfs.org/ns/void#> .

<{base}> a sd:Service ;
    sd:endpoint <{base}> ;
    sd:supportedLanguage sd:SPARQL11Query, sd:SPARQL11Update ;
    sd:resultFormat
        <http://www.w3.org/ns/formats/SPARQL_Results_JSON> ,
        <http://www.w3.org/ns/formats/SPARQL_Results_XML> ,
        <http://www.w3.org/ns/formats/SPARQL_Results_CSV> ,
        <http://www.w3.org/ns/formats/SPARQL_Results_TSV> ,
        <http://www.w3.org/ns/formats/N-Triples> ;
    sd:feature sd:BasicFederatedQuery ;
    sd:defaultDataset [
        a sd:Dataset ;
        sd:defaultGraph [
            a sd:Graph ;
            void:triples {triples}
        ]
    ] .
",
        triples = state.graph.read(|g| g.node_count() + g.edge_count())
    );
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/turtle; charset=utf-8",
        )],
        turtle,
    )
        .into_response()
}

fn execute_sparql_handler(
    state: &ServerState,
    query_str: &str,
    format: selene_rdf::sparql::SparqlResultFormat,
) -> Result<axum::response::Response, HttpError> {
    let ns = &state.rdf_namespace;
    let snap = state.graph.load_snapshot();
    let csr = crate::bootstrap::get_or_build_csr(&state.csr_cache, &snap);

    let ontology_ref = state.rdf_ontology.as_ref().map(|o| o.read());

    let (bytes, content_type) = selene_rdf::sparql::execute_sparql(
        &snap,
        &csr,
        ns,
        ontology_ref.as_deref(),
        query_str,
        format,
    )
    .map_err(|e| HttpError(OpError::QueryError(e.to_string())))?;

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, content_type)],
        bytes,
    )
        .into_response())
}

// ── Snapshot export ────────────────────────────────────────────────

/// Export the graph as a portable binary snapshot.
///
/// Returns the snapshot as an `application/octet-stream` download with a
/// `Content-Disposition` header for the filename. The snapshot is self-contained
/// (nodes, edges, schemas, triggers, HNSW indexes, views) and can be used to
/// restore the graph on another instance by placing it in the data directory.
pub(in crate::http) async fn snapshot_export(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
) -> Result<impl IntoResponse, HttpError> {
    // Snapshot export is an admin-only operation.
    if !auth.0.is_admin() {
        return Err(HttpError(OpError::AuthDenied));
    }

    // UUID-like temp path prevents race conditions from concurrent exports.
    let unique_id = format!(
        "{}-{:x}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let tmp_path = state
        .config
        .data_dir
        .join(format!("export-{unique_id}.snap.tmp"));

    let st = Arc::clone(&state);
    let export_path = tmp_path.clone();
    let file_path = tokio::task::spawn_blocking(move || -> Result<std::path::PathBuf, OpError> {
        crate::tasks::export_snapshot_to_path(&st, &export_path)
            .map_err(|e| OpError::Internal(format!("snapshot export: {e}")))?;
        Ok(export_path)
    })
    .await
    .map_err(|e| HttpError(OpError::Internal(format!("spawn: {e}"))))??;

    // Stream the file instead of reading it all into memory.
    let file = tokio::fs::File::open(&file_path)
        .await
        .map_err(|e| HttpError(OpError::Internal(format!("open snapshot: {e}"))))?;
    let metadata = file
        .metadata()
        .await
        .map_err(|e| HttpError(OpError::Internal(format!("stat snapshot: {e}"))))?;
    let stream = tokio_util::io::ReaderStream::new(file);
    let body = axum::body::Body::from_stream(stream);

    // Schedule cleanup of the temp file after response is sent.
    let cleanup = file_path.clone();
    tokio::spawn(async move {
        // Wait a bit to ensure the stream has been consumed.
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        let _ = tokio::fs::remove_file(&cleanup).await;
    });

    let filename = format!("selene-snapshot-{}.snap", chrono_filename());
    Ok((
        StatusCode::OK,
        [
            (
                axum::http::header::CONTENT_TYPE,
                "application/octet-stream".to_owned(),
            ),
            (
                axum::http::header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
            (
                axum::http::header::CONTENT_LENGTH,
                metadata.len().to_string(),
            ),
        ],
        body,
    )
        .into_response())
}

fn chrono_filename() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{secs}")
}
