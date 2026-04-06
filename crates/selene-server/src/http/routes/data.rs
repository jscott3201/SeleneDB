//! Data import/export handlers: CSV, RDF, SPARQL, ReactFlow, time-series.

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

#[derive(Deserialize)]
pub(in crate::http) struct SparqlQueryParams {
    query: Option<String>,
    format: Option<String>,
}

/// `GET /sparql?query=SELECT...&format=json`
pub(in crate::http) async fn sparql_get(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Query(params): Query<SparqlQueryParams>,
) -> Result<axum::response::Response, HttpError> {
    let _ = auth.0;
    let query_str = params
        .query
        .ok_or_else(|| HttpError::bad_request("missing required 'query' parameter"))?;
    let format_str = params.format.as_deref().unwrap_or("json").to_owned();
    execute_sparql_handler(&state, &query_str, &format_str)
}

/// `POST /sparql` (Content-Type: application/sparql-query, body = SPARQL query string)
pub(in crate::http) async fn sparql_post(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Query(params): Query<SparqlQueryParams>,
    body: axum::body::Bytes,
) -> Result<axum::response::Response, HttpError> {
    let _ = auth.0;
    let query_str = std::str::from_utf8(&body)
        .map_err(|_| HttpError::bad_request("request body is not valid UTF-8"))?;
    if query_str.is_empty() {
        return Err(HttpError::bad_request("empty SPARQL query body"));
    }
    let format_str = params.format.as_deref().unwrap_or("json").to_owned();
    execute_sparql_handler(&state, query_str, &format_str)
}

fn execute_sparql_handler(
    state: &ServerState,
    query_str: &str,
    format_str: &str,
) -> Result<axum::response::Response, HttpError> {
    let format: selene_rdf::sparql::SparqlResultFormat = format_str
        .parse()
        .map_err(|e: String| HttpError::bad_request(e))?;

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
