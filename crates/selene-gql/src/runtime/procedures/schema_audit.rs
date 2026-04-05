//! Schema audit procedures for migration progress tracking.
//!
//! - `graph.schemaAudit` -- per-label conformance summary (total, conforming, non-conforming, schema version)
//! - `graph.schemaAuditDetails` -- per-property issue breakdown (property, issue type, count)

use std::collections::HashMap;

use selene_core::IStr;
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;
use smol_str::SmolStr;

use super::{Procedure, ProcedureParam, ProcedureRow, ProcedureSignature, YieldColumn};
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Extract the property name from a validation issue message.
///
/// All messages produced by `check_properties` follow the pattern:
///   "property '<name>' ..."
/// or
///   "required property '<name>' ..."
///
/// Returns the content between the first pair of single-quotes, or `"unknown"`
/// if the message does not match the expected format.
fn extract_property_name(message: &str) -> &str {
    if let Some(start) = message.find('\'') {
        let after = &message[start + 1..];
        if let Some(end) = after.find('\'') {
            return &after[..end];
        }
    }
    "unknown"
}

/// Classify a validation issue message into a short, human-readable issue type.
///
/// Message formats from `check_properties` and its helpers:
/// - `"required property '...' is missing"` -> `"missing_required"`
/// - `"property '...' expected type ... but got ..."` -> `"type_mismatch"`
/// - `"property '...' value ... is below minimum ..."` -> `"below_minimum"`
/// - `"property '...' value ... exceeds maximum ..."` -> `"exceeds_maximum"`
/// - `"property '...' length ... is below minimum ..."` -> `"length_below_minimum"`
/// - `"property '...' length ... exceeds maximum ..."` -> `"length_exceeds_maximum"`
/// - `"property '...' value not in allowed set"` -> `"not_in_allowed_set"`
/// - `"property '...' value does not match pattern '...'"` -> `"pattern_mismatch"`
/// - `"property '...' has invalid regex pattern '...'..."` -> `"invalid_pattern"`
/// - anything else -> `"other"`
fn classify_issue(message: &str) -> &'static str {
    if message.contains("is missing") {
        "missing_required"
    } else if message.contains("expected type") {
        "type_mismatch"
    } else if message.contains("length") && message.contains("is below minimum") {
        "length_below_minimum"
    } else if message.contains("length") && message.contains("exceeds maximum") {
        "length_exceeds_maximum"
    } else if message.contains("is below minimum") {
        "below_minimum"
    } else if message.contains("exceeds maximum") {
        "exceeds_maximum"
    } else if message.contains("not in allowed set") {
        "not_in_allowed_set"
    } else if message.contains("does not match pattern") {
        "pattern_mismatch"
    } else if message.contains("has invalid regex pattern") {
        "invalid_pattern"
    } else {
        "other"
    }
}

// ── SchemaAudit ───────────────────────────────────────────────────────────────

/// `CALL graph.schemaAudit('Label') YIELD label, totalNodes, conforming, nonConforming, schemaVersion`
///
/// Scans all nodes with the given label and validates each against the current
/// schema. Returns a single summary row with conformance counts and the schema
/// version. Returns zero counts when the label has no registered schema or no
/// matching nodes.
pub struct SchemaAudit;

impl Procedure for SchemaAudit {
    fn name(&self) -> &'static str {
        "graph.schemaAudit"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![ProcedureParam {
                name: "label",
                typ: GqlType::String,
            }],
            yields: vec![
                YieldColumn {
                    name: "label",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "totalNodes",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "conforming",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "nonConforming",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "schemaVersion",
                    typ: GqlType::String,
                },
            ],
        }
    }

    fn execute(
        &self,
        args: &[GqlValue],
        graph: &SeleneGraph,
        _ht: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        let label = match args.first() {
            Some(GqlValue::String(s)) => s.as_str().to_string(),
            _ => {
                return Err(GqlError::InvalidArgument {
                    message: "graph.schemaAudit requires a String label argument".into(),
                });
            }
        };

        let schema_version = graph
            .schema()
            .node_schema(&label)
            .map_or_else(|| "none".to_string(), |s| s.version.to_string());

        let mut total: i64 = 0;
        let mut conforming: i64 = 0;

        for node_id in graph.nodes_by_label(&label) {
            if let Some(node_ref) = graph.get_node(node_id) {
                total += 1;
                let node = node_ref.to_owned_node();
                if graph.schema().validate_node(&node).is_empty() {
                    conforming += 1;
                }
            }
        }

        let non_conforming = total - conforming;

        let row: ProcedureRow = smallvec![
            (IStr::new("label"), GqlValue::String(SmolStr::new(&label))),
            (IStr::new("totalNodes"), GqlValue::Int(total)),
            (IStr::new("conforming"), GqlValue::Int(conforming)),
            (IStr::new("nonConforming"), GqlValue::Int(non_conforming)),
            (
                IStr::new("schemaVersion"),
                GqlValue::String(SmolStr::new(&schema_version))
            ),
        ];

        Ok(vec![row])
    }
}

// ── SchemaAuditDetails ────────────────────────────────────────────────────────

/// `CALL graph.schemaAuditDetails('Label') YIELD property, issueType, count`
///
/// Scans all nodes with the given label and validates each against the current
/// schema. Aggregates validation issues by `(property, issue_type)` and yields
/// one row per distinct combination, ordered by count descending.
pub struct SchemaAuditDetails;

impl Procedure for SchemaAuditDetails {
    fn name(&self) -> &'static str {
        "graph.schemaAuditDetails"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![ProcedureParam {
                name: "label",
                typ: GqlType::String,
            }],
            yields: vec![
                YieldColumn {
                    name: "property",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "issueType",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "count",
                    typ: GqlType::Int,
                },
            ],
        }
    }

    fn execute(
        &self,
        args: &[GqlValue],
        graph: &SeleneGraph,
        _ht: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        let label = match args.first() {
            Some(GqlValue::String(s)) => s.as_str().to_string(),
            _ => {
                return Err(GqlError::InvalidArgument {
                    message: "graph.schemaAuditDetails requires a String label argument".into(),
                });
            }
        };

        // Aggregate issue counts keyed by (property_name, issue_type).
        let mut counts: HashMap<(String, &'static str), i64> = HashMap::new();

        for node_id in graph.nodes_by_label(&label) {
            if let Some(node_ref) = graph.get_node(node_id) {
                let node = node_ref.to_owned_node();
                for issue in graph.schema().validate_node(&node) {
                    let prop = extract_property_name(&issue.message).to_string();
                    let issue_type = classify_issue(&issue.message);
                    *counts.entry((prop, issue_type)).or_insert(0) += 1;
                }
            }
        }

        // Sort by count descending, then property + issue_type for determinism.
        let mut entries: Vec<((String, &'static str), i64)> = counts.into_iter().collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

        let rows = entries
            .into_iter()
            .map(|((prop, issue_type), count)| {
                smallvec![
                    (IStr::new("property"), GqlValue::String(SmolStr::new(&prop))),
                    (
                        IStr::new("issueType"),
                        GqlValue::String(SmolStr::new(issue_type))
                    ),
                    (IStr::new("count"), GqlValue::Int(count)),
                ]
            })
            .collect();

        Ok(rows)
    }
}
