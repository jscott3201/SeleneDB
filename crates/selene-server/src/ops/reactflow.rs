//! React Flow import/export operations.
//!
//! Converts between Selene's graph model and React Flow's node/edge format,
//! enabling visual graph editors to work with Selene data directly.

use std::collections::{HashMap, HashSet};

use selene_core::{NodeId, Value};
use serde::{Deserialize, Serialize};

use schemars::JsonSchema;

use super::{OpError, edge_to_dto, graph_err, json_to_value, node_to_dto, persist_or_die};
use crate::auth::handshake::AuthContext;
use crate::bootstrap::ServerState;

// ── React Flow Types ─────────────────────────────────────────────────

/// React Flow node (subset of fields relevant for Selene interop).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RFNode {
    pub id: String,
    #[serde(default)]
    pub position: RFPosition,
    #[serde(default)]
    pub data: serde_json::Value,
    #[serde(rename = "type", default)]
    pub node_type: Option<String>,
    #[serde(default)]
    pub style: Option<serde_json::Value>,
    #[serde(rename = "parentId", default)]
    pub parent_id: Option<String>,
}

/// React Flow edge.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RFEdge {
    pub id: String,
    pub source: String,
    pub target: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(rename = "type", default)]
    pub edge_type: Option<String>,
    #[serde(default)]
    pub animated: Option<bool>,
    #[serde(default)]
    pub data: Option<serde_json::Value>,
}

/// React Flow position.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct RFPosition {
    pub x: f64,
    pub y: f64,
}

/// React Flow graph (the format React Flow expects).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RFGraph {
    pub nodes: Vec<RFNode>,
    pub edges: Vec<RFEdge>,
}

// ── Export ────────────────────────────────────────────────────────────

/// Export the Selene graph (or a filtered subset) as React Flow format.
pub fn export_reactflow(
    state: &ServerState,
    auth: &AuthContext,
    label_filter: Option<&str>,
) -> RFGraph {
    state.graph.read(|g| {
        let node_ids: Vec<NodeId> = if let Some(label) = label_filter {
            g.nodes_by_label(label).collect()
        } else {
            g.all_node_ids().collect()
        };

        // Filter by scope
        let node_ids: Vec<NodeId> = if auth.is_admin() {
            node_ids
        } else {
            node_ids
                .into_iter()
                .filter(|id| auth.in_scope(*id))
                .collect()
        };

        let node_id_set: HashSet<u64> = node_ids.iter().map(|n| n.0).collect();

        // Auto-layout: simple grid layout based on containment depth
        let mut rf_nodes = Vec::with_capacity(node_ids.len());
        for (idx, &nid) in node_ids.iter().enumerate() {
            if let Some(node) = g.get_node(nid) {
                let dto = node_to_dto(node);

                // Convert properties to plain JSON for React Flow data
                let mut data = serde_json::Map::new();
                data.insert(
                    "label".into(),
                    serde_json::Value::String(
                        dto.properties
                            .get("name")
                            .and_then(|v| v.as_str().map(|s| s.to_string()))
                            .unwrap_or_else(|| {
                                format!(
                                    "{} {}",
                                    dto.labels.first().cloned().unwrap_or_default(),
                                    dto.id
                                )
                            }),
                    ),
                );
                data.insert("selene_id".into(), serde_json::json!(dto.id));
                data.insert("labels".into(), serde_json::json!(dto.labels));
                for (k, v) in &dto.properties {
                    data.insert(k.clone(), super::value_to_json(v));
                }

                // Simple grid layout: 250px spacing
                let col = idx % 4;
                let row = idx / 4;

                rf_nodes.push(RFNode {
                    id: dto.id.to_string(),
                    position: RFPosition {
                        x: col as f64 * 250.0,
                        y: row as f64 * 150.0,
                    },
                    data: serde_json::Value::Object(data),
                    node_type: dto.labels.first().cloned(),
                    style: None,
                    parent_id: None,
                });
            }
        }

        // Collect edges between the exported nodes
        let mut rf_edges = Vec::new();
        for eid in g.all_edge_ids() {
            if let Some(edge) = g.get_edge(eid)
                && node_id_set.contains(&edge.source.0)
                && node_id_set.contains(&edge.target.0)
            {
                let dto = edge_to_dto(edge);
                let mut edge_data = serde_json::Map::new();
                for (k, v) in &dto.properties {
                    edge_data.insert(k.clone(), super::value_to_json(v));
                }

                rf_edges.push(RFEdge {
                    id: format!("e{}", dto.id),
                    source: dto.source.to_string(),
                    target: dto.target.to_string(),
                    label: Some(dto.label.clone()),
                    edge_type: Some(if dto.label == "contains" {
                        "smoothstep".into()
                    } else {
                        "default".into()
                    }),
                    animated: Some(dto.label == "feeds"),
                    data: if edge_data.is_empty() {
                        None
                    } else {
                        Some(serde_json::Value::Object(edge_data))
                    },
                });
            }
        }

        RFGraph {
            nodes: rf_nodes,
            edges: rf_edges,
        }
    })
}

// ── Import ────────────────────────────────────────────────────────────

/// Result of importing a React Flow graph.
#[derive(Debug, Serialize)]
pub struct ImportResult {
    pub nodes_created: usize,
    pub edges_created: usize,
    pub id_map: HashMap<String, u64>,
}

/// Import a React Flow graph into Selene.
///
/// Creates nodes and edges from React Flow format. Node `data` fields become
/// Selene properties. The `label` in data becomes the Selene label.
/// Returns a mapping from React Flow IDs to Selene IDs.
pub fn import_reactflow(
    state: &ServerState,
    auth: &AuthContext,
    graph: RFGraph,
) -> Result<ImportResult, OpError> {
    // Require at least Operator role for graph mutations
    if matches!(
        auth.role,
        crate::auth::Role::Reader | crate::auth::Role::Device
    ) {
        return Err(OpError::AuthDenied);
    }
    let mut id_map: HashMap<String, u64> = HashMap::new();
    let mut nodes_created = 0;
    let mut edges_created = 0;

    // Phase 1: Create all nodes
    for rf_node in &graph.nodes {
        let mut label_strs: Vec<String> = Vec::new();

        // Use node_type as primary label
        if let Some(ref nt) = rf_node.node_type {
            label_strs.push(nt.clone());
        }

        // Also check data.labels array
        if let Some(data_labels) = rf_node.data.get("labels").and_then(|v| v.as_array()) {
            for l in data_labels {
                if let Some(s) = l.as_str() {
                    label_strs.push(s.to_string());
                }
            }
        }

        if label_strs.is_empty() {
            label_strs.push("node".to_string());
        }

        let label_refs: Vec<&str> = label_strs.iter().map(|s| s.as_str()).collect();
        let labels = selene_core::LabelSet::from_strs(&label_refs);

        // Convert data fields to Selene properties (skip metadata fields)
        let mut props = selene_core::PropertyMap::new();
        if let Some(obj) = rf_node.data.as_object() {
            for (k, v) in obj {
                if matches!(k.as_str(), "label" | "labels" | "selene_id") {
                    continue;
                }
                props.insert(selene_core::IStr::new(k), json_to_value(v.clone()));
            }
        }

        // Store position as properties so it round-trips
        props.insert(
            selene_core::IStr::new("_rf_x"),
            Value::Float(rf_node.position.x),
        );
        props.insert(
            selene_core::IStr::new("_rf_y"),
            Value::Float(rf_node.position.y),
        );

        // Promote dictionary-flagged string properties to InternedStr
        state.graph.read(|g| {
            for label_str in &label_strs {
                if let Some(schema) = g.schema().node_schema(label_str) {
                    for prop_def in &schema.properties {
                        if prop_def.dictionary {
                            let key = selene_core::IStr::new(&prop_def.name);
                            if let Some(Value::String(s)) = props.get(key) {
                                let interned =
                                    Value::InternedStr(selene_core::IStr::new(s.as_str()));
                                props.insert(key, interned);
                            }
                        }
                    }
                }
            }
        });

        let (node_id, changes) = state
            .graph
            .write(|m| m.create_node(labels, props))
            .map_err(graph_err)?;

        persist_or_die(state, &changes);
        id_map.insert(rf_node.id.clone(), node_id.0);
        nodes_created += 1;
    }

    // Phase 2: Create edges (resolve RF IDs to Selene IDs)
    for rf_edge in &graph.edges {
        let source_id = id_map.get(&rf_edge.source).ok_or_else(|| {
            OpError::InvalidRequest(format!(
                "edge source '{}' not found in imported nodes",
                rf_edge.source
            ))
        })?;
        let target_id = id_map.get(&rf_edge.target).ok_or_else(|| {
            OpError::InvalidRequest(format!(
                "edge target '{}' not found in imported nodes",
                rf_edge.target
            ))
        })?;

        let label = rf_edge.label.clone().unwrap_or_else(|| "connected".into());

        let mut props = selene_core::PropertyMap::new();
        if let Some(ref data) = rf_edge.data
            && let Some(obj) = data.as_object()
        {
            for (k, v) in obj {
                props.insert(selene_core::IStr::new(k), json_to_value(v.clone()));
            }
        }

        let (_, changes) = state
            .graph
            .write(|m| {
                m.create_edge(
                    NodeId(*source_id),
                    selene_core::IStr::new(&label),
                    NodeId(*target_id),
                    props,
                )
            })
            .map_err(graph_err)?;

        persist_or_die(state, &changes);
        edges_created += 1;
    }

    Ok(ImportResult {
        nodes_created,
        edges_created,
        id_map,
    })
}
