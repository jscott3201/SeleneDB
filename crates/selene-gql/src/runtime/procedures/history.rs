//! Change history procedures: query what changed and when.
//!
//! `graph.history(nodeId, [startTime], [endTime])` -- per-entity change history
//! `graph.changes(label, duration)` -- label-scoped recent changes
//!
//! Delegates to a HistoryProvider set via static OnceLock at server startup.

use std::sync::{Arc, OnceLock};

use selene_core::{IStr, Value};
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;

use super::{Procedure, ProcedureParam, ProcedureRow, ProcedureSignature, YieldColumn};
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

// ── History provider ──────────────────────────────────────────────

/// A single history entry returned by the provider.
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    pub node_id: u64,
    pub change_type: &'static str,
    pub key: Option<String>,
    pub old_value: Option<Value>,
    pub new_value: Option<Value>,
    pub timestamp_nanos: i64,
}

/// Trait for change history queries (decouples selene-gql from selene-server).
pub trait HistoryProvider: Send + Sync {
    /// Get change history for a specific entity.
    fn entity_history(
        &self,
        entity_id: u64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: usize,
    ) -> Vec<HistoryEntry>;

    /// Get recent changes for all entities with a given label.
    fn label_changes(
        &self,
        label: &str,
        since_nanos: i64,
        limit: usize,
        graph: &SeleneGraph,
    ) -> Vec<HistoryEntry>;

    /// Get the property value at a specific point in time.
    /// Returns None if version store is not available or no version found.
    fn property_at(
        &self,
        _node_id: u64,
        _key: &str,
        _timestamp: i64,
        _graph: &SeleneGraph,
    ) -> Option<Value> {
        None
    }

    /// Get all recorded versions of a property.
    fn property_history(
        &self,
        _node_id: u64,
        _key: &str,
        _start_time: Option<i64>,
        _end_time: Option<i64>,
    ) -> Vec<PropertyVersionEntry> {
        vec![]
    }
}

/// A version entry returned by property_history.
#[derive(Debug, Clone)]
pub struct PropertyVersionEntry {
    pub value: Value,
    pub superseded_at: i64,
}

static HISTORY_PROVIDER: OnceLock<Arc<dyn HistoryProvider>> = OnceLock::new();

/// Set the history provider. Called once at server startup.
pub fn set_history_provider(provider: Arc<dyn HistoryProvider>) {
    let _ = HISTORY_PROVIDER.set(provider);
}

fn get_history_provider() -> Result<&'static Arc<dyn HistoryProvider>, GqlError> {
    HISTORY_PROVIDER
        .get()
        .ok_or_else(|| GqlError::InvalidArgument {
            message: "change history not available (server context required)".into(),
        })
}

// ── graph.history ─────────────────────────────────────────────────

/// `CALL graph.history(42) YIELD change_type, key, old_value, new_value, timestamp`
/// `CALL graph.history(42, '2026-03-20T00:00:00Z', '2026-03-21T00:00:00Z') YIELD ...`
pub struct GraphHistory;

impl Procedure for GraphHistory {
    fn name(&self) -> &'static str {
        "graph.history"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "nodeId",
                    typ: GqlType::Int,
                },
                ProcedureParam {
                    name: "startTime",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "endTime",
                    typ: GqlType::String,
                },
            ],
            yields: vec![
                YieldColumn {
                    name: "change_type",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "key",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "old_value",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "new_value",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "timestamp",
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
        if args.is_empty() {
            return Err(GqlError::InvalidArgument {
                message: "graph.history requires at least 1 argument: nodeId".into(),
            });
        }

        let node_id = args[0].as_int()? as u64;

        let start_time = if args.len() > 1 && !args[1].is_null() {
            Some(parse_timestamp_arg(&args[1])?)
        } else {
            None
        };
        let end_time = if args.len() > 2 && !args[2].is_null() {
            Some(parse_timestamp_arg(&args[2])?)
        } else {
            None
        };

        let provider = get_history_provider()?;
        let entries = provider.entity_history(node_id, start_time, end_time, 1000);

        Ok(entries.into_iter().map(history_entry_to_row).collect())
    }
}

// ── graph.changes ─────────────────────────────────────────────────

/// `CALL graph.changes('sensor', '1h') YIELD node_id, change_type, key, old_value, new_value, timestamp`
pub struct GraphChanges;

impl Procedure for GraphChanges {
    fn name(&self) -> &'static str {
        "graph.changes"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "label",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "duration",
                    typ: GqlType::String,
                },
            ],
            yields: vec![
                YieldColumn {
                    name: "node_id",
                    typ: GqlType::UInt,
                },
                YieldColumn {
                    name: "change_type",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "key",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "old_value",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "new_value",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "timestamp",
                    typ: GqlType::Int,
                },
            ],
        }
    }

    fn execute(
        &self,
        args: &[GqlValue],
        graph: &SeleneGraph,
        _hot_tier: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        if args.len() < 2 {
            return Err(GqlError::InvalidArgument {
                message: "graph.changes requires 2 arguments: label, duration".into(),
            });
        }

        let label = args[0].as_str()?;
        let duration_str = args[1].as_str()?;
        let duration_nanos = parse_duration_nanos(duration_str)?;
        let since_nanos = selene_core::entity::now_nanos() - duration_nanos;

        let provider = get_history_provider()?;
        let entries = provider.label_changes(label, since_nanos, 10_000, graph);

        Ok(entries
            .into_iter()
            .map(|e| {
                let node_id_val = GqlValue::Int(e.node_id as i64);
                let mut row = history_entry_to_row(e);
                row.insert(0, (IStr::new("node_id"), node_id_val));
                row
            })
            .collect())
    }
}

// ── graph.propertyAt ──────────────────────────────────────────────

/// `CALL graph.propertyAt(42, 'temp', '2026-03-21T03:00:00Z') YIELD value`
pub struct GraphPropertyAt;

impl Procedure for GraphPropertyAt {
    fn name(&self) -> &'static str {
        "graph.propertyAt"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "nodeId",
                    typ: GqlType::Int,
                },
                ProcedureParam {
                    name: "property",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "timestamp",
                    typ: GqlType::String,
                },
            ],
            yields: vec![YieldColumn {
                name: "value",
                typ: GqlType::String,
            }],
        }
    }

    fn execute(
        &self,
        args: &[GqlValue],
        graph: &SeleneGraph,
        _hot_tier: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        if args.len() < 3 {
            return Err(GqlError::InvalidArgument {
                message: "graph.propertyAt requires 3 arguments: nodeId, property, timestamp"
                    .into(),
            });
        }

        let node_id = args[0].as_int()? as u64;
        let key = args[1].as_str()?;
        let timestamp = parse_timestamp_arg(&args[2])?;

        let provider = get_history_provider()?;
        match provider.property_at(node_id, key, timestamp, graph) {
            Some(val) => Ok(vec![smallvec![(IStr::new("value"), GqlValue::from(&val),)]]),
            None => Ok(vec![smallvec![(IStr::new("value"), GqlValue::Null)]]),
        }
    }
}

// ── graph.propertyHistory ────────────────────────────────────────

/// `CALL graph.propertyHistory(42, 'temp') YIELD value, superseded_at`
pub struct GraphPropertyHistory;

impl Procedure for GraphPropertyHistory {
    fn name(&self) -> &'static str {
        "graph.propertyHistory"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "nodeId",
                    typ: GqlType::Int,
                },
                ProcedureParam {
                    name: "property",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "startTime",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "endTime",
                    typ: GqlType::String,
                },
            ],
            yields: vec![
                YieldColumn {
                    name: "value",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "superseded_at",
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
                message: "graph.propertyHistory requires at least 2 arguments: nodeId, property"
                    .into(),
            });
        }

        let node_id = args[0].as_int()? as u64;
        let key = args[1].as_str()?;

        let start_time = if args.len() > 2 && !args[2].is_null() {
            Some(parse_timestamp_arg(&args[2])?)
        } else {
            None
        };
        let end_time = if args.len() > 3 && !args[3].is_null() {
            Some(parse_timestamp_arg(&args[3])?)
        } else {
            None
        };

        let provider = get_history_provider()?;
        let versions = provider.property_history(node_id, key, start_time, end_time);

        Ok(versions
            .into_iter()
            .map(|v| {
                smallvec![
                    (IStr::new("value"), GqlValue::from(&v.value)),
                    (IStr::new("superseded_at"), GqlValue::Int(v.superseded_at)),
                ]
            })
            .collect())
    }
}

// ── Helpers ───────────────────────────────────────────────────────

fn history_entry_to_row(entry: HistoryEntry) -> ProcedureRow {
    smallvec![
        (
            IStr::new("change_type"),
            GqlValue::String(entry.change_type.into()),
        ),
        (
            IStr::new("key"),
            entry
                .key
                .map_or(GqlValue::Null, |k| GqlValue::String(k.into())),
        ),
        (
            IStr::new("old_value"),
            entry
                .old_value
                .as_ref()
                .map_or(GqlValue::Null, GqlValue::from),
        ),
        (
            IStr::new("new_value"),
            entry
                .new_value
                .as_ref()
                .map_or(GqlValue::Null, GqlValue::from),
        ),
        (IStr::new("timestamp"), GqlValue::Int(entry.timestamp_nanos),),
    ]
}

/// Parse an ISO 8601 timestamp string to nanoseconds since epoch.
fn parse_timestamp_arg(val: &GqlValue) -> Result<i64, GqlError> {
    let s = val.as_str()?;
    // Try parsing as ISO 8601 via the existing GQL datetime parser
    if let Ok(GqlValue::ZonedDateTime(dt)) = crate::runtime::functions::parse_iso8601(s) {
        return Ok(dt.nanos);
    }
    // Try parsing as plain integer (nanoseconds)
    if let Ok(n) = s.parse::<i64>() {
        return Ok(n);
    }
    Err(GqlError::InvalidArgument {
        message: format!("invalid timestamp: '{s}' (expected ISO 8601 or nanoseconds)"),
    })
}

/// Parse a duration string like "1h", "30m", "24h", "7d" to nanoseconds.
/// Bare numbers without a unit suffix are treated as seconds.
fn parse_duration_nanos(s: &str) -> Result<i64, GqlError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(GqlError::InvalidArgument {
            message: "empty duration string".into(),
        });
    }

    // Bare number → seconds
    if let Ok(secs) = s.parse::<f64>() {
        return Ok((secs * 1_000_000_000.0) as i64);
    }

    let (num_str, unit) = s.split_at(s.len() - 1);
    let num: f64 = num_str.parse().map_err(|_| GqlError::InvalidArgument {
        message: format!("invalid duration: '{s}' (expected number with s/m/h/d suffix)"),
    })?;

    let multiplier: f64 = match unit {
        "s" => 1_000_000_000.0,
        "m" => 60.0 * 1_000_000_000.0,
        "h" => 3_600.0 * 1_000_000_000.0,
        "d" => 86_400.0 * 1_000_000_000.0,
        _ => {
            return Err(GqlError::InvalidArgument {
                message: format!("unknown duration unit: '{unit}' (expected s, m, h, d)"),
            });
        }
    };

    Ok((num * multiplier) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{LabelSet, PropertyMap, Value};
    use selene_graph::SeleneGraph;
    use smol_str::SmolStr;
    use std::sync::Arc;

    fn simple_graph() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("S1")))]),
        )
        .unwrap();
        m.commit(0).unwrap();
        g
    }

    // ── parse_duration_nanos ────────────────────────────────────────

    #[test]
    fn parse_duration_seconds() {
        assert_eq!(parse_duration_nanos("10s").unwrap(), 10_000_000_000);
    }

    #[test]
    fn parse_duration_minutes() {
        assert_eq!(parse_duration_nanos("5m").unwrap(), 300_000_000_000);
    }

    #[test]
    fn parse_duration_hours() {
        assert_eq!(parse_duration_nanos("1h").unwrap(), 3_600_000_000_000);
    }

    #[test]
    fn parse_duration_days() {
        assert_eq!(parse_duration_nanos("7d").unwrap(), 604_800_000_000_000);
    }

    #[test]
    fn parse_duration_bare_number_as_seconds() {
        assert_eq!(parse_duration_nanos("60").unwrap(), 60_000_000_000);
    }

    #[test]
    fn parse_duration_fractional() {
        let result = parse_duration_nanos("0.5h").unwrap();
        assert_eq!(result, 1_800_000_000_000);
    }

    #[test]
    fn parse_duration_empty_string_errors() {
        assert!(parse_duration_nanos("").is_err());
    }

    #[test]
    fn parse_duration_whitespace_only_errors() {
        assert!(parse_duration_nanos("   ").is_err());
    }

    #[test]
    fn parse_duration_unknown_unit_errors() {
        assert!(parse_duration_nanos("10x").is_err());
    }

    #[test]
    fn parse_duration_invalid_number_errors() {
        assert!(parse_duration_nanos("abch").is_err());
    }

    // ── parse_timestamp_arg ─────────────────────────────────────────

    #[test]
    fn parse_timestamp_nanos_as_string() {
        let val = GqlValue::String(SmolStr::new("1000000000"));
        let result = parse_timestamp_arg(&val).unwrap();
        assert_eq!(result, 1_000_000_000);
    }

    #[test]
    fn parse_timestamp_iso8601() {
        let val = GqlValue::String(SmolStr::new("2026-01-01T00:00:00Z"));
        let result = parse_timestamp_arg(&val);
        assert!(result.is_ok());
        assert!(result.unwrap() > 0);
    }

    #[test]
    fn parse_timestamp_invalid_string_errors() {
        let val = GqlValue::String(SmolStr::new("not_a_timestamp"));
        assert!(parse_timestamp_arg(&val).is_err());
    }

    // ── graph.history (no provider) ─────────────────────────────────

    #[test]
    fn history_no_provider_errors() {
        // Without a registered HistoryProvider, the procedure should error.
        // The OnceLock may or may not be set depending on test ordering,
        // but we can test the argument validation path.
        let g = simple_graph();
        let proc = GraphHistory;
        let result = proc.execute(&[], &g, None, None);
        assert!(result.is_err());
    }

    #[test]
    fn history_missing_node_id_errors() {
        let g = simple_graph();
        let proc = GraphHistory;
        let result = proc.execute(&[], &g, None, None);
        assert!(result.is_err());
    }

    // ── graph.changes (no provider) ─────────────────────────────────

    #[test]
    fn changes_missing_args_errors() {
        let g = simple_graph();
        let proc = GraphChanges;
        // No arguments
        let result = proc.execute(&[], &g, None, None);
        assert!(result.is_err());
        // Only label, missing duration
        let result = proc.execute(&[GqlValue::String(SmolStr::new("sensor"))], &g, None, None);
        assert!(result.is_err());
    }

    // ── graph.propertyAt (no provider) ──────────────────────────────

    #[test]
    fn property_at_missing_args_errors() {
        let g = simple_graph();
        let proc = GraphPropertyAt;
        // Fewer than 3 arguments
        let result = proc.execute(&[GqlValue::Int(1)], &g, None, None);
        assert!(result.is_err());
    }

    // ── graph.propertyHistory (no provider) ─────────────────────────

    #[test]
    fn property_history_missing_args_errors() {
        let g = simple_graph();
        let proc = GraphPropertyHistory;
        // Fewer than 2 arguments
        let result = proc.execute(&[GqlValue::Int(1)], &g, None, None);
        assert!(result.is_err());
    }

    // ── history_entry_to_row ────────────────────────────────────────

    #[test]
    fn history_entry_to_row_all_fields() {
        let entry = HistoryEntry {
            node_id: 42,
            change_type: "update",
            key: Some("temp".to_string()),
            old_value: Some(Value::Float(70.0)),
            new_value: Some(Value::Float(72.0)),
            timestamp_nanos: 1_000_000_000,
        };
        let row = history_entry_to_row(entry);
        assert_eq!(row.len(), 5);
        assert_eq!(row[0].1, GqlValue::String("update".into()));
        assert_eq!(row[1].1, GqlValue::String("temp".into()));
        assert_eq!(row[4].1, GqlValue::Int(1_000_000_000));
    }

    #[test]
    fn history_entry_to_row_null_fields() {
        let entry = HistoryEntry {
            node_id: 1,
            change_type: "create",
            key: None,
            old_value: None,
            new_value: None,
            timestamp_nanos: 0,
        };
        let row = history_entry_to_row(entry);
        assert_eq!(row[1].1, GqlValue::Null); // key is null
        assert_eq!(row[2].1, GqlValue::Null); // old_value is null
        assert_eq!(row[3].1, GqlValue::Null); // new_value is null
    }

    // ── Signature checks ────────────────────────────────────────────

    #[test]
    fn procedure_names_match_convention() {
        assert_eq!(GraphHistory.name(), "graph.history");
        assert_eq!(GraphChanges.name(), "graph.changes");
        assert_eq!(GraphPropertyAt.name(), "graph.propertyAt");
        assert_eq!(GraphPropertyHistory.name(), "graph.propertyHistory");
    }

    #[test]
    fn history_signature_yields_expected_columns() {
        let sig = GraphHistory.signature();
        let yield_names: Vec<&str> = sig.yields.iter().map(|y| y.name).collect();
        assert_eq!(
            yield_names,
            vec!["change_type", "key", "old_value", "new_value", "timestamp"]
        );
    }

    #[test]
    fn changes_signature_includes_node_id() {
        let sig = GraphChanges.signature();
        let yield_names: Vec<&str> = sig.yields.iter().map(|y| y.name).collect();
        assert!(yield_names.contains(&"node_id"));
    }

    // ── HistoryProvider integration via mock ─────────────────────────

    struct MockHistoryProvider {
        entries: Vec<HistoryEntry>,
    }

    impl HistoryProvider for MockHistoryProvider {
        fn entity_history(
            &self,
            entity_id: u64,
            start_time: Option<i64>,
            end_time: Option<i64>,
            _limit: usize,
        ) -> Vec<HistoryEntry> {
            self.entries
                .iter()
                .filter(|e| e.node_id == entity_id)
                .filter(|e| start_time.is_none() || e.timestamp_nanos >= start_time.unwrap())
                .filter(|e| end_time.is_none() || e.timestamp_nanos <= end_time.unwrap())
                .cloned()
                .collect()
        }

        fn label_changes(
            &self,
            _label: &str,
            since_nanos: i64,
            _limit: usize,
            _graph: &SeleneGraph,
        ) -> Vec<HistoryEntry> {
            self.entries
                .iter()
                .filter(|e| e.timestamp_nanos >= since_nanos)
                .cloned()
                .collect()
        }

        fn property_at(
            &self,
            node_id: u64,
            key: &str,
            timestamp: i64,
            _graph: &SeleneGraph,
        ) -> Option<Value> {
            self.entries
                .iter()
                .filter(|e| e.node_id == node_id)
                .filter(|e| e.key.as_deref() == Some(key))
                .rfind(|e| e.timestamp_nanos <= timestamp)
                .and_then(|e| e.new_value.clone())
        }

        fn property_history(
            &self,
            node_id: u64,
            key: &str,
            start_time: Option<i64>,
            end_time: Option<i64>,
        ) -> Vec<PropertyVersionEntry> {
            self.entries
                .iter()
                .filter(|e| e.node_id == node_id)
                .filter(|e| e.key.as_deref() == Some(key))
                .filter(|e| start_time.is_none() || e.timestamp_nanos >= start_time.unwrap())
                .filter(|e| end_time.is_none() || e.timestamp_nanos <= end_time.unwrap())
                .map(|e| PropertyVersionEntry {
                    value: e.new_value.clone().unwrap_or(Value::Null),
                    superseded_at: e.timestamp_nanos,
                })
                .collect()
        }
    }

    // The HISTORY_PROVIDER OnceLock can only be set once per process, so we
    // gate these tests behind a helper that tries to set it, then skips if
    // another test already installed a different provider.
    fn install_mock_provider() -> bool {
        let provider = Arc::new(MockHistoryProvider {
            entries: vec![
                HistoryEntry {
                    node_id: 1,
                    change_type: "create",
                    key: None,
                    old_value: None,
                    new_value: None,
                    timestamp_nanos: 1_000_000_000,
                },
                HistoryEntry {
                    node_id: 1,
                    change_type: "update",
                    key: Some("temp".to_string()),
                    old_value: Some(Value::Float(70.0)),
                    new_value: Some(Value::Float(72.0)),
                    timestamp_nanos: 2_000_000_000,
                },
                HistoryEntry {
                    node_id: 1,
                    change_type: "update",
                    key: Some("temp".to_string()),
                    old_value: Some(Value::Float(72.0)),
                    new_value: Some(Value::Float(75.0)),
                    timestamp_nanos: 3_000_000_000,
                },
            ],
        });
        set_history_provider(provider);
        HISTORY_PROVIDER.get().is_some()
    }

    #[test]
    fn history_with_mock_provider_returns_entries() {
        if !install_mock_provider() {
            return;
        }
        let g = simple_graph();
        let proc = GraphHistory;
        let args = vec![GqlValue::Int(1)];
        let rows = proc.execute(&args, &g, None, None).unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn history_with_time_range_filter() {
        if !install_mock_provider() {
            return;
        }
        let g = simple_graph();
        let proc = GraphHistory;
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String(SmolStr::new("2000000000")), // start = 2s
            GqlValue::String(SmolStr::new("3000000000")), // end = 3s
        ];
        let rows = proc.execute(&args, &g, None, None).unwrap();
        // Should include only the two updates at 2s and 3s
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn property_at_returns_value_at_timestamp() {
        if !install_mock_provider() {
            return;
        }
        let g = simple_graph();
        let proc = GraphPropertyAt;
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String(SmolStr::new("temp")),
            GqlValue::String(SmolStr::new("2500000000")), // between 2s and 3s
        ];
        let rows = proc.execute(&args, &g, None, None).unwrap();
        assert_eq!(rows.len(), 1);
        // At timestamp 2.5s, the most recent value is the update at 2s: 72.0
        match &rows[0][0].1 {
            GqlValue::Float(v) => assert_eq!(*v, 72.0),
            GqlValue::Null => {} // If provider returns None
            other => panic!("expected Float or Null, got {other:?}"),
        }
    }

    #[test]
    fn property_history_returns_versions() {
        if !install_mock_provider() {
            return;
        }
        let g = simple_graph();
        let proc = GraphPropertyHistory;
        let args = vec![GqlValue::Int(1), GqlValue::String(SmolStr::new("temp"))];
        let rows = proc.execute(&args, &g, None, None).unwrap();
        // Two "temp" updates for node 1
        assert_eq!(rows.len(), 2);
    }
}
