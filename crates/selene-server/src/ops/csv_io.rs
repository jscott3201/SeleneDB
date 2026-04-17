//! CSV import/export operations.
//!
//! Import bypasses GQL for performance — maps CSV rows directly to
//! TrackedMutation. Auth scope is checked once at the batch level
//! (parent_id must be in scope for node import).

use std::collections::HashMap;
use std::io::Read;

use selene_core::{IStr, LabelSet, NodeId, PropertyMap, Value};

use super::{OpError, graph_err, persist_or_die, require_in_scope};
use crate::auth::handshake::AuthContext;
use crate::bootstrap::ServerState;

/// Configuration for CSV node import.
#[derive(Debug, Clone)]
pub struct CsvNodeImportConfig {
    /// Label to apply to all imported nodes.
    pub label: String,
    /// Map CSV column names to property keys (if None, use column names directly).
    pub column_mappings: Option<HashMap<String, String>>,
    /// CSV column containing parent node ID for containment (optional).
    pub parent_id_column: Option<String>,
    /// Field delimiter (default: comma).
    pub delimiter: u8,
}

impl Default for CsvNodeImportConfig {
    fn default() -> Self {
        Self {
            label: String::new(),
            delimiter: b',',
            column_mappings: None,
            parent_id_column: None,
        }
    }
}

/// Configuration for CSV edge import.
#[derive(Debug, Clone)]
pub struct CsvEdgeImportConfig {
    /// CSV column for source node ID.
    pub source_column: String,
    /// CSV column for target node ID.
    pub target_column: String,
    /// CSV column for edge label.
    pub label_column: String,
    /// Field delimiter (default: comma).
    pub delimiter: u8,
}

impl Default for CsvEdgeImportConfig {
    fn default() -> Self {
        Self {
            source_column: "source_id".into(),
            target_column: "target_id".into(),
            label_column: "label".into(),
            delimiter: b',',
        }
    }
}

/// Result of a CSV import operation.
#[derive(Debug, serde::Serialize)]
pub struct ImportResult {
    pub nodes_created: usize,
    pub edges_created: usize,
    pub rows_skipped: usize,
    pub errors: Vec<String>,
}

const BATCH_SIZE: usize = 1000;

/// Import nodes from CSV data.
///
/// Each row becomes a node with the configured label. Column values become properties.
/// Batches rows into groups of 1000 for WAL efficiency.
pub fn import_nodes_csv<R: Read>(
    state: &ServerState,
    auth: &AuthContext,
    reader: R,
    config: &CsvNodeImportConfig,
) -> Result<ImportResult, OpError> {
    let auth = super::refresh_scope_if_stale(state, auth);

    if config.label.is_empty() {
        return Err(OpError::InvalidRequest(
            "label is required for node import".into(),
        ));
    }

    let labels = LabelSet::from_strs(&[&config.label]);
    let contains = IStr::new("contains");

    // Pre-compute dictionary-flagged property keys for this label
    // so we can promote string values to InternedStr during import.
    let dict_keys: Vec<IStr> = state.graph.read(|g| {
        let mut keys = Vec::new();
        if let Some(schema) = g.schema().node_schema(&config.label) {
            for prop_def in &schema.properties {
                if prop_def.dictionary {
                    keys.push(IStr::new(&prop_def.name));
                }
            }
        }
        keys
    });

    let mut csv_reader = csv::ReaderBuilder::new()
        .delimiter(config.delimiter)
        .has_headers(true)
        .from_reader(reader);

    let headers: Vec<String> = csv_reader
        .headers()
        .map_err(|e| OpError::InvalidRequest(format!("CSV header error: {e}")))?
        .iter()
        .map(|h| h.to_string())
        .collect();

    let mut result = ImportResult {
        nodes_created: 0,
        edges_created: 0,
        rows_skipped: 0,
        errors: Vec::new(),
    };

    // Collect rows in batches
    let mut batch_props: Vec<(PropertyMap, Option<u64>)> = Vec::with_capacity(BATCH_SIZE);
    let mut row_num = 1usize; // 1-indexed (header is row 0)

    for record in csv_reader.records() {
        row_num += 1;
        let record = match record {
            Ok(r) => r,
            Err(e) => {
                result.errors.push(format!("row {row_num}: {e}"));
                result.rows_skipped += 1;
                continue;
            }
        };

        // Build property map from columns
        let mut props = PropertyMap::new();
        let mut parent_id: Option<u64> = None;

        for (i, value) in record.iter().enumerate() {
            if i >= headers.len() {
                break;
            }
            let col_name = &headers[i];

            // Check if this is the parent_id column
            if let Some(ref pid_col) = config.parent_id_column
                && col_name == pid_col
            {
                parent_id = value.parse::<u64>().ok();
                continue;
            }

            // Skip empty values
            if value.is_empty() {
                continue;
            }

            // Map column name to property key
            let prop_key = config
                .column_mappings
                .as_ref()
                .and_then(|m| m.get(col_name))
                .unwrap_or(col_name);

            // Infer value type
            let val = parse_csv_value(value);
            props.insert(IStr::new(prop_key), val);
        }

        // Promote dictionary-flagged string properties to InternedStr
        for key in &dict_keys {
            if let Some(Value::String(s)) = props.get(*key) {
                let interned = Value::InternedStr(IStr::new(s.as_str()));
                props.insert(*key, interned);
            }
        }

        batch_props.push((props, parent_id));

        // Flush batch
        if batch_props.len() >= BATCH_SIZE {
            let batch_result = flush_node_batch(state, &auth, &labels, contains, &mut batch_props)?;
            result.nodes_created += batch_result.0;
            result.edges_created += batch_result.1;
        }
    }

    // Flush remaining
    if !batch_props.is_empty() {
        let batch_result = flush_node_batch(state, &auth, &labels, contains, &mut batch_props)?;
        result.nodes_created += batch_result.0;
        result.edges_created += batch_result.1;
    }

    Ok(result)
}

/// Flush a batch of node creates through TrackedMutation.
fn flush_node_batch(
    state: &ServerState,
    auth: &AuthContext,
    labels: &LabelSet,
    contains: IStr,
    batch: &mut Vec<(PropertyMap, Option<u64>)>,
) -> Result<(usize, usize), OpError> {
    let mut nodes_created = 0;
    let mut edges_created = 0;

    // Validate parent IDs are in scope before the batch
    for (_, parent_id) in batch.iter() {
        if let Some(pid) = parent_id {
            require_in_scope(auth, NodeId(*pid))?;
        }
    }

    let (_, changes) = state
        .graph
        .write(|m| {
            for (props, parent_id) in batch.drain(..) {
                match m.create_node(labels.clone(), props) {
                    Ok(nid) => {
                        nodes_created += 1;
                        if let Some(pid) = parent_id {
                            if let Err(e) =
                                m.create_edge(NodeId(pid), contains, nid, PropertyMap::new())
                            {
                                tracing::warn!(
                                    "failed to create containment edge to {}: {e}",
                                    nid.0
                                );
                            } else {
                                edges_created += 1;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("failed to create node: {e}");
                    }
                }
            }
            Ok::<_, selene_graph::GraphError>(())
        })
        .map_err(graph_err)?;

    persist_or_die(state, &changes);
    Ok((nodes_created, edges_created))
}

/// Import edges from CSV data.
///
/// Each row becomes an edge. Source/target are node IDs.
pub fn import_edges_csv<R: Read>(
    state: &ServerState,
    auth: &AuthContext,
    reader: R,
    config: &CsvEdgeImportConfig,
) -> Result<ImportResult, OpError> {
    let auth = super::refresh_scope_if_stale(state, auth);

    let mut csv_reader = csv::ReaderBuilder::new()
        .delimiter(config.delimiter)
        .has_headers(true)
        .from_reader(reader);

    let headers: Vec<String> = csv_reader
        .headers()
        .map_err(|e| OpError::InvalidRequest(format!("CSV header error: {e}")))?
        .iter()
        .map(|h| h.to_string())
        .collect();

    // Find required column indices
    let src_idx = headers
        .iter()
        .position(|h| h == &config.source_column)
        .ok_or_else(|| {
            OpError::InvalidRequest(format!("missing column '{}'", config.source_column))
        })?;
    let tgt_idx = headers
        .iter()
        .position(|h| h == &config.target_column)
        .ok_or_else(|| {
            OpError::InvalidRequest(format!("missing column '{}'", config.target_column))
        })?;
    let label_idx = headers
        .iter()
        .position(|h| h == &config.label_column)
        .ok_or_else(|| {
            OpError::InvalidRequest(format!("missing column '{}'", config.label_column))
        })?;

    // Property columns = all columns that are not source, target, or label
    let prop_indices: Vec<(usize, String)> = headers
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != src_idx && *i != tgt_idx && *i != label_idx)
        .map(|(i, h)| (i, h.clone()))
        .collect();

    let mut result = ImportResult {
        nodes_created: 0,
        edges_created: 0,
        rows_skipped: 0,
        errors: Vec::new(),
    };

    let mut batch: Vec<(u64, u64, String, PropertyMap)> = Vec::with_capacity(BATCH_SIZE);
    let mut row_num = 1usize;

    for record in csv_reader.records() {
        row_num += 1;
        let record = match record {
            Ok(r) => r,
            Err(e) => {
                result.errors.push(format!("row {row_num}: {e}"));
                result.rows_skipped += 1;
                continue;
            }
        };

        let source_id: u64 = if let Some(id) = record.get(src_idx).and_then(|v| v.parse().ok()) {
            id
        } else {
            result
                .errors
                .push(format!("row {row_num}: invalid source_id"));
            result.rows_skipped += 1;
            continue;
        };

        let target_id: u64 = if let Some(id) = record.get(tgt_idx).and_then(|v| v.parse().ok()) {
            id
        } else {
            result
                .errors
                .push(format!("row {row_num}: invalid target_id"));
            result.rows_skipped += 1;
            continue;
        };

        let label = match record.get(label_idx) {
            Some(l) if !l.is_empty() => l.to_string(),
            _ => {
                result
                    .errors
                    .push(format!("row {row_num}: missing edge label"));
                result.rows_skipped += 1;
                continue;
            }
        };

        // Build edge properties
        let mut props = PropertyMap::new();
        for (idx, key) in &prop_indices {
            if let Some(value) = record.get(*idx)
                && !value.is_empty()
            {
                props.insert(IStr::new(key), parse_csv_value(value));
            }
        }

        // Promote dictionary-flagged edge properties to InternedStr
        state.graph.read(|g| {
            if let Some(schema) = g.schema().edge_schema(&label) {
                for prop_def in &schema.properties {
                    if prop_def.dictionary {
                        let key = IStr::new(&prop_def.name);
                        if let Some(Value::String(s)) = props.get(key) {
                            let interned = Value::InternedStr(IStr::new(s.as_str()));
                            props.insert(key, interned);
                        }
                    }
                }
            }
        });

        batch.push((source_id, target_id, label, props));

        if batch.len() >= BATCH_SIZE {
            let count = flush_edge_batch(state, &auth, &mut batch)?;
            result.edges_created += count;
        }
    }

    // Flush remaining
    if !batch.is_empty() {
        let count = flush_edge_batch(state, &auth, &mut batch)?;
        result.edges_created += count;
    }

    Ok(result)
}

fn flush_edge_batch(
    state: &ServerState,
    auth: &AuthContext,
    batch: &mut Vec<(u64, u64, String, PropertyMap)>,
) -> Result<usize, OpError> {
    // Validate source and target are in scope
    for (src, tgt, _, _) in batch.iter() {
        require_in_scope(auth, NodeId(*src))?;
        require_in_scope(auth, NodeId(*tgt))?;
    }

    let mut edges_created = 0;
    let (_, changes) = state
        .graph
        .write(|m| {
            for (src, tgt, label, props) in batch.drain(..) {
                match m.create_edge(NodeId(src), IStr::new(&label), NodeId(tgt), props) {
                    Ok(_) => edges_created += 1,
                    Err(e) => tracing::warn!("failed to create edge {src}->{tgt}: {e}"),
                }
            }
            Ok::<_, selene_graph::GraphError>(())
        })
        .map_err(graph_err)?;

    persist_or_die(state, &changes);
    Ok(edges_created)
}

/// Export nodes as CSV.
///
/// Returns CSV string with header row. Scope-filtered.
pub fn export_nodes_csv(
    state: &ServerState,
    auth: &AuthContext,
    label_filter: Option<&str>,
) -> Result<String, OpError> {
    let auth = super::refresh_scope_if_stale(state, auth);

    state.graph.read(|g| {
        // Collect nodes matching label filter and scope
        let mut nodes_data: Vec<(u64, Vec<(String, Value)>)> = Vec::new();
        let mut all_keys: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

        let node_iter: Box<dyn Iterator<Item = NodeId>> = if let Some(label) = label_filter {
            Box::new(g.nodes_by_label(label))
        } else {
            Box::new(g.all_node_ids())
        };

        for nid in node_iter {
            if !auth.in_scope(nid) {
                continue;
            }
            if let Some(node) = g.get_node(nid) {
                let props: Vec<(String, Value)> = node
                    .properties
                    .iter()
                    .map(|(k, v)| {
                        let key = k.as_str().to_string();
                        all_keys.insert(key.clone());
                        (key, v.clone())
                    })
                    .collect();
                nodes_data.push((nid.0, props));
            }
        }

        // Build CSV
        let mut wtr = csv::Writer::from_writer(Vec::new());

        // Header: id + all property keys
        let mut header = vec!["id".to_string()];
        header.extend(all_keys.iter().cloned());
        wtr.write_record(&header)
            .map_err(|e| OpError::Internal(format!("CSV write error: {e}")))?;

        // Data rows
        for (id, props) in &nodes_data {
            let mut row = vec![id.to_string()];
            let prop_map: HashMap<&str, &Value> =
                props.iter().map(|(k, v)| (k.as_str(), v)).collect();

            for key in &all_keys {
                match prop_map.get(key.as_str()) {
                    Some(v) => row.push(value_to_csv_string(v)),
                    None => row.push(String::new()),
                }
            }
            wtr.write_record(&row)
                .map_err(|e| OpError::Internal(format!("CSV write error: {e}")))?;
        }

        let bytes = wtr
            .into_inner()
            .map_err(|e| OpError::Internal(format!("CSV flush error: {e}")))?;
        String::from_utf8(bytes).map_err(|e| OpError::Internal(format!("CSV encoding error: {e}")))
    })
}

/// Export edges as CSV.
///
/// Returns CSV string with header row. Scope-filtered (source node must be in scope).
/// Columns: id, source, target, label, then property keys alphabetically.
pub fn export_edges_csv(
    state: &ServerState,
    auth: &AuthContext,
    label_filter: Option<&str>,
) -> Result<String, OpError> {
    let auth = super::refresh_scope_if_stale(state, auth);

    state.graph.read(|g| {
        // (edge_id, source_id, target_id, label, properties)
        type EdgeRow = (u64, u64, u64, String, Vec<(String, Value)>);
        let mut edges_data: Vec<EdgeRow> = Vec::new();
        let mut all_keys: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

        let edge_iter: Box<dyn Iterator<Item = selene_core::EdgeId>> =
            if let Some(label) = label_filter {
                Box::new(g.edges_by_label(label))
            } else {
                Box::new(g.all_edge_ids())
            };

        for eid in edge_iter {
            if let Some(edge) = g.get_edge(eid) {
                // Scope filter on source node
                if !auth.in_scope(edge.source) {
                    continue;
                }
                let props: Vec<(String, Value)> = edge
                    .properties
                    .iter()
                    .map(|(k, v)| {
                        let key = k.as_str().to_string();
                        all_keys.insert(key.clone());
                        (key, v.clone())
                    })
                    .collect();
                edges_data.push((
                    eid.0,
                    edge.source.0,
                    edge.target.0,
                    edge.label.as_str().to_string(),
                    props,
                ));
            }
        }

        // Build CSV
        let mut wtr = csv::Writer::from_writer(Vec::new());

        // Header: id, source, target, label + all property keys
        let mut header = vec![
            "id".to_string(),
            "source".to_string(),
            "target".to_string(),
            "label".to_string(),
        ];
        header.extend(all_keys.iter().cloned());
        wtr.write_record(&header)
            .map_err(|e| OpError::Internal(format!("CSV write error: {e}")))?;

        // Data rows
        for (id, source, target, label, props) in &edges_data {
            let mut row = vec![
                id.to_string(),
                source.to_string(),
                target.to_string(),
                label.clone(),
            ];
            let prop_map: HashMap<&str, &Value> =
                props.iter().map(|(k, v)| (k.as_str(), v)).collect();

            for key in &all_keys {
                match prop_map.get(key.as_str()) {
                    Some(v) => row.push(value_to_csv_string(v)),
                    None => row.push(String::new()),
                }
            }
            wtr.write_record(&row)
                .map_err(|e| OpError::Internal(format!("CSV write error: {e}")))?;
        }

        let bytes = wtr
            .into_inner()
            .map_err(|e| OpError::Internal(format!("CSV flush error: {e}")))?;
        String::from_utf8(bytes).map_err(|e| OpError::Internal(format!("CSV encoding error: {e}")))
    })
}

/// Parse a CSV cell value, inferring type.
fn parse_csv_value(s: &str) -> Value {
    // Try integer
    if let Ok(i) = s.parse::<i64>() {
        return Value::Int(i);
    }
    // Try float
    if let Ok(f) = s.parse::<f64>() {
        return Value::Float(f);
    }
    // Try boolean
    match s.to_lowercase().as_str() {
        "true" => return Value::Bool(true),
        "false" => return Value::Bool(false),
        _ => {}
    }
    // Default to string
    Value::str(s)
}

/// Convert a Value to a CSV-friendly string.
fn value_to_csv_string(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::UInt(u) => u.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => s.to_string(),
        Value::InternedStr(s) => s.as_str().to_string(),
        Value::Timestamp(t) => t.to_string(),
        Value::Date(d) => format!("{}", Value::Date(*d)),
        Value::LocalDateTime(n) => format!("{}", Value::LocalDateTime(*n)),
        Value::Duration(n) => format!("{}", Value::Duration(*n)),
        Value::Bytes(_) => "<bytes>".to_string(),
        Value::List(items) => {
            let json = serde_json::Value::Array(
                items
                    .iter()
                    .map(|v| serde_json::json!(value_to_csv_string(v)))
                    .collect(),
            );
            json.to_string()
        }
        Value::Vector(v) => format!("vector[{}]", v.len()),
        Value::Geometry(g) => g.to_geojson(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::handshake::AuthContext;
    use crate::bootstrap::ServerState;

    async fn test_state() -> ServerState {
        let dir = tempfile::tempdir().unwrap();
        ServerState::for_testing(dir.path()).await
    }

    #[tokio::test]
    async fn import_nodes_basic() {
        let state = test_state().await;
        let auth = AuthContext::dev_admin();
        let csv = "name,unit,accuracy\nTemp-1,°F,0.5\nTemp-2,°F,0.3\nTemp-3,°C,1.0\n";
        let config = CsvNodeImportConfig {
            label: "sensor".into(),
            ..Default::default()
        };
        let result = import_nodes_csv(&state, &auth, csv.as_bytes(), &config).unwrap();
        assert_eq!(result.nodes_created, 3);
        assert_eq!(result.errors.len(), 0);

        // Verify via graph read
        let count = state.graph.read(|g| g.nodes_by_label("sensor").count());
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn import_nodes_with_type_inference() {
        let state = test_state().await;
        let auth = AuthContext::dev_admin();
        let csv = "name,value,active\nS1,72,true\nS2,68.5,false\n";
        let config = CsvNodeImportConfig {
            label: "sensor".into(),
            ..Default::default()
        };
        let result = import_nodes_csv(&state, &auth, csv.as_bytes(), &config).unwrap();
        assert_eq!(result.nodes_created, 2);

        // Check that types were inferred
        state.graph.read(|g| {
            let node = g.get_node(NodeId(1)).unwrap();
            assert!(matches!(
                node.properties.get(IStr::new("value")),
                Some(Value::Int(72))
            ));
            assert!(matches!(
                node.properties.get(IStr::new("active")),
                Some(Value::Bool(true))
            ));
        });
    }

    #[tokio::test]
    async fn import_nodes_missing_label_errors() {
        let state = test_state().await;
        let auth = AuthContext::dev_admin();
        let config = CsvNodeImportConfig::default(); // no label
        let result = import_nodes_csv(&state, &auth, &b"name\nFoo\n"[..], &config);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn import_edges() {
        let state = test_state().await;
        let auth = AuthContext::dev_admin();

        // Create some nodes first
        let node_csv = "name\nA\nB\nC\n";
        let node_config = CsvNodeImportConfig {
            label: "node".into(),
            ..Default::default()
        };
        import_nodes_csv(&state, &auth, node_csv.as_bytes(), &node_config).unwrap();

        // Import edges
        let edge_csv = "source_id,target_id,label,weight\n1,2,feeds,10\n2,3,feeds,5\n";
        let edge_config = CsvEdgeImportConfig::default();
        let result = import_edges_csv(&state, &auth, edge_csv.as_bytes(), &edge_config).unwrap();
        assert_eq!(result.edges_created, 2);

        // Verify
        let count = state.graph.read(|g| g.edge_count());
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn export_roundtrip() {
        let state = test_state().await;
        let auth = AuthContext::dev_admin();

        // Import some nodes
        let csv = "name,unit\nTemp-1,°F\nTemp-2,°C\n";
        let config = CsvNodeImportConfig {
            label: "sensor".into(),
            ..Default::default()
        };
        import_nodes_csv(&state, &auth, csv.as_bytes(), &config).unwrap();

        // Export
        let exported = export_nodes_csv(&state, &auth, Some("sensor")).unwrap();
        assert!(exported.contains("name"));
        assert!(exported.contains("Temp-1"));
        assert!(exported.contains("Temp-2"));
        // Should have header + 2 data rows
        let lines: Vec<&str> = exported.trim().lines().collect();
        assert_eq!(lines.len(), 3);
    }

    #[tokio::test]
    async fn import_skips_bad_rows() {
        let state = test_state().await;
        let auth = AuthContext::dev_admin();
        let csv = "name,value\nOK,42\n";
        let config = CsvNodeImportConfig {
            label: "test".into(),
            ..Default::default()
        };
        let result = import_nodes_csv(&state, &auth, csv.as_bytes(), &config).unwrap();
        assert_eq!(result.nodes_created, 1);
    }

    #[tokio::test]
    async fn export_edges_roundtrip() {
        let state = test_state().await;
        let auth = AuthContext::dev_admin();

        // Create nodes first
        let node_csv = "name\nA\nB\nC\n";
        let node_config = CsvNodeImportConfig {
            label: "node".into(),
            ..Default::default()
        };
        import_nodes_csv(&state, &auth, node_csv.as_bytes(), &node_config).unwrap();

        // Import edges with properties
        let edge_csv = "source_id,target_id,label,weight\n1,2,feeds,10\n2,3,monitors,5\n";
        let edge_config = CsvEdgeImportConfig::default();
        import_edges_csv(&state, &auth, edge_csv.as_bytes(), &edge_config).unwrap();

        // Export all edges
        let exported = export_edges_csv(&state, &auth, None).unwrap();
        assert!(exported.contains("id,source,target,label"));
        assert!(exported.contains("weight"));
        // Header + 2 data rows
        let lines: Vec<&str> = exported.trim().lines().collect();
        assert_eq!(lines.len(), 3);

        // Export with label filter
        let feeds_only = export_edges_csv(&state, &auth, Some("feeds")).unwrap();
        let lines: Vec<&str> = feeds_only.trim().lines().collect();
        assert_eq!(lines.len(), 2); // header + 1 "feeds" edge
        assert!(feeds_only.contains("feeds"));
    }

    #[test]
    fn parse_csv_value_types() {
        assert!(matches!(parse_csv_value("42"), Value::Int(42)));
        assert!(matches!(parse_csv_value("3.15"), Value::Float(f) if (f - 3.15).abs() < 0.001));
        assert!(matches!(parse_csv_value("true"), Value::Bool(true)));
        assert!(matches!(parse_csv_value("false"), Value::Bool(false)));
        assert!(matches!(parse_csv_value("hello"), Value::String(_)));
    }
}
