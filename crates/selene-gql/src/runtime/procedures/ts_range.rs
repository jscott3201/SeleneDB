//! Point-query time-series procedures: ts.range, ts.latest, ts.valueAt.
//!
//! These procedures retrieve raw samples or single-point lookups from the
//! hot tier, with optional gap-fill interpolation.

use selene_core::{IStr, NodeId};
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;

use super::*;
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue, ZonedDateTime};

use super::ts::{extract_duration, extract_timestamp};

// ── ts.range ───────────────────────────────────────────────────────

/// ts.range(entity_id, property, start, end) -> timestamp, value
///
/// Returns raw samples from the hot tier within an absolute time range.
pub struct TsRange;

impl Procedure for TsRange {
    fn name(&self) -> &'static str {
        "ts.range"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "entity_id",
                    typ: GqlType::Int,
                },
                ProcedureParam {
                    name: "property",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "start",
                    typ: GqlType::ZonedDateTime,
                },
                ProcedureParam {
                    name: "end",
                    typ: GqlType::ZonedDateTime,
                },
            ],
            yields: vec![
                YieldColumn {
                    name: "timestamp",
                    typ: GqlType::ZonedDateTime,
                },
                YieldColumn {
                    name: "value",
                    typ: GqlType::Float,
                },
            ],
        }
    }

    fn execute(
        &self,
        args: &[GqlValue],
        _graph: &SeleneGraph,
        hot_tier: Option<&HotTier>,
        scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        let hot = hot_tier.ok_or_else(|| {
            GqlError::internal("time-series not available (no hot tier configured)")
        })?;

        let entity_id = args[0].as_int()?;

        // Auth scope check
        if let Some(scope) = scope
            && !scope.contains(entity_id as u32)
        {
            return Ok(vec![]);
        }

        let property = args[1].as_str()?;
        let start_nanos = extract_timestamp(&args[2])?;
        let end_nanos = extract_timestamp(&args[3])?;

        let samples = hot.range(NodeId(entity_id as u64), property, start_nanos, end_nanos);

        // Gap-fill mode: 6 params = (entity_id, property, start, end, fill, interval)
        if args.len() >= 6 {
            let fill = match args[4].as_str()?.to_uppercase().as_str() {
                "LOCF" => selene_core::FillStrategy::Locf,
                "LINEAR" => selene_core::FillStrategy::Linear,
                other => {
                    return Err(GqlError::type_error(format!(
                        "unknown fill strategy: '{other}'"
                    )));
                }
            };
            let interval_nanos = extract_duration(&args[5])?;
            if interval_nanos <= 0 {
                return Err(GqlError::type_error(
                    "interval must be positive".to_string(),
                ));
            }

            // Clock-aligned output timestamps
            let aligned_start = (start_nanos / interval_nanos) * interval_nanos;
            let mut t = if aligned_start < start_nanos {
                aligned_start + interval_nanos
            } else {
                aligned_start
            };

            let node_id = NodeId(entity_id as u64);
            let mut output = Vec::new();

            while t <= end_nanos {
                // Binary search within raw samples for the bracket
                let before_idx = samples.partition_point(|s| s.timestamp_nanos <= t);
                let before = if before_idx > 0 {
                    Some(&samples[before_idx - 1])
                } else {
                    None
                };

                let (value, interpolated) = match before {
                    Some(sample) if sample.timestamp_nanos == t => (sample.value, false),
                    Some(before_sample) => match fill {
                        selene_core::FillStrategy::Locf => (before_sample.value, true),
                        selene_core::FillStrategy::Linear => {
                            let after = samples.get(before_idx);
                            match after {
                                Some(after_sample) => {
                                    let t0 = before_sample.timestamp_nanos as f64;
                                    let t1 = after_sample.timestamp_nanos as f64;
                                    let frac = (t as f64 - t0) / (t1 - t0);
                                    let v = before_sample.value
                                        + frac * (after_sample.value - before_sample.value);
                                    (v, true)
                                }
                                None => (before_sample.value, true), // LOCF fallback
                            }
                        }
                    },
                    None => {
                        // No sample in range before this point; check hot tier for
                        // a sample before the query range (LOCF from prior data)
                        if let Some(s) = hot.sample_at_or_before(node_id, property, t) {
                            (s.value, true)
                        } else {
                            t += interval_nanos;
                            continue; // No data available for this point
                        }
                    }
                };

                output.push(smallvec![
                    (
                        IStr::new("timestamp"),
                        GqlValue::ZonedDateTime(ZonedDateTime::from_nanos_utc(t)),
                    ),
                    (IStr::new("value"), GqlValue::Float(value)),
                    (IStr::new("interpolated"), GqlValue::Bool(interpolated)),
                ]);
                t += interval_nanos;
            }

            return Ok(output);
        }

        // Raw mode (4 params): existing behavior
        Ok(samples
            .into_iter()
            .map(|s| {
                smallvec![
                    (
                        IStr::new("timestamp"),
                        GqlValue::ZonedDateTime(ZonedDateTime::from_nanos_utc(s.timestamp_nanos))
                    ),
                    (IStr::new("value"), GqlValue::Float(s.value)),
                ]
            })
            .collect())
    }
}

// ── ts.latest ──────────────────────────────────────────────────────

/// ts.latest(entity_id, property) -> timestamp, value
///
/// Returns the most recent sample.
pub struct TsLatest;

impl Procedure for TsLatest {
    fn name(&self) -> &'static str {
        "ts.latest"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "entity_id",
                    typ: GqlType::Int,
                },
                ProcedureParam {
                    name: "property",
                    typ: GqlType::String,
                },
            ],
            yields: vec![
                YieldColumn {
                    name: "timestamp",
                    typ: GqlType::ZonedDateTime,
                },
                YieldColumn {
                    name: "value",
                    typ: GqlType::Float,
                },
            ],
        }
    }

    fn execute(
        &self,
        args: &[GqlValue],
        _graph: &SeleneGraph,
        hot_tier: Option<&HotTier>,
        scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        let hot = hot_tier.ok_or_else(|| GqlError::internal("time-series not available"))?;

        let entity_id = args
            .first()
            .ok_or_else(|| GqlError::InvalidArgument {
                message: "ts.latest requires entity_id".into(),
            })?
            .as_int()?;

        // Auth scope check
        if let Some(scope) = scope
            && !scope.contains(entity_id as u32)
        {
            return Ok(vec![]);
        }

        let property = args
            .get(1)
            .ok_or_else(|| GqlError::InvalidArgument {
                message: "ts.latest requires property".into(),
            })?
            .as_str()?;

        match hot.latest(NodeId(entity_id as u64), property) {
            Some(s) => Ok(vec![smallvec![
                (
                    IStr::new("timestamp"),
                    GqlValue::ZonedDateTime(ZonedDateTime::from_nanos_utc(s.timestamp_nanos))
                ),
                (IStr::new("value"), GqlValue::Float(s.value)),
            ]]),
            None => Ok(vec![]), // No data -> zero rows (inner-join semantics)
        }
    }
}

// ── ts.valueAt ────────────────────────────────────────────────────

/// ts.valueAt(entity_id, property, timestamp, `[fill_strategy]`) -> value, actual_timestamp, interpolated
///
/// Returns the value at an exact timestamp or interpolates using the
/// specified fill strategy. Resolution: explicit param > schema default > none.
pub struct TsValueAt;

impl Procedure for TsValueAt {
    fn name(&self) -> &'static str {
        "ts.valueAt"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "entity_id",
                    typ: GqlType::Int,
                },
                ProcedureParam {
                    name: "property",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "timestamp",
                    typ: GqlType::ZonedDateTime,
                },
            ],
            yields: vec![
                YieldColumn {
                    name: "value",
                    typ: GqlType::Float,
                },
                YieldColumn {
                    name: "actual_timestamp",
                    typ: GqlType::ZonedDateTime,
                },
                YieldColumn {
                    name: "interpolated",
                    typ: GqlType::Bool,
                },
            ],
        }
    }

    fn execute(
        &self,
        args: &[GqlValue],
        graph: &SeleneGraph,
        hot_tier: Option<&HotTier>,
        scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        let hot = hot_tier.ok_or_else(|| {
            GqlError::internal("time-series not available (no hot tier configured)")
        })?;

        let entity_id = args[0].as_int()?;
        if let Some(scope) = scope
            && !scope.contains(entity_id as u32)
        {
            return Ok(vec![]);
        }

        let property = args[1].as_str()?;
        let ts = extract_timestamp(&args[2])?;

        // Determine fill strategy: explicit arg > schema default > none
        let fill = if args.len() > 3 {
            match args[3].as_str()?.to_uppercase().as_str() {
                "LOCF" => Some(selene_core::FillStrategy::Locf),
                "LINEAR" => Some(selene_core::FillStrategy::Linear),
                "NONE" => None,
                other => {
                    return Err(GqlError::type_error(format!(
                        "unknown fill strategy: '{other}', expected LOCF, LINEAR, or NONE"
                    )));
                }
            }
        } else {
            resolve_schema_fill(graph, NodeId(entity_id as u64), property)
        };

        let node_id = NodeId(entity_id as u64);
        let before = hot.sample_at_or_before(node_id, property, ts);

        match before {
            Some(sample) if sample.timestamp_nanos == ts => {
                // Exact match
                Ok(vec![smallvec![
                    (IStr::new("value"), GqlValue::Float(sample.value)),
                    (
                        IStr::new("actual_timestamp"),
                        GqlValue::ZonedDateTime(ZonedDateTime::from_nanos_utc(ts)),
                    ),
                    (IStr::new("interpolated"), GqlValue::Bool(false)),
                ]])
            }
            Some(before_sample) => match fill {
                Some(selene_core::FillStrategy::Locf) => Ok(vec![smallvec![
                    (IStr::new("value"), GqlValue::Float(before_sample.value)),
                    (
                        IStr::new("actual_timestamp"),
                        GqlValue::ZonedDateTime(ZonedDateTime::from_nanos_utc(
                            before_sample.timestamp_nanos,
                        )),
                    ),
                    (IStr::new("interpolated"), GqlValue::Bool(true)),
                ]]),
                Some(selene_core::FillStrategy::Linear) => {
                    let after = hot.sample_after(node_id, property, ts);
                    match after {
                        Some(after_sample) => {
                            let t0 = before_sample.timestamp_nanos as f64;
                            let t1 = after_sample.timestamp_nanos as f64;
                            let t = ts as f64;
                            let frac = (t - t0) / (t1 - t0);
                            let value = before_sample.value
                                + frac * (after_sample.value - before_sample.value);
                            Ok(vec![smallvec![
                                (IStr::new("value"), GqlValue::Float(value)),
                                (
                                    IStr::new("actual_timestamp"),
                                    GqlValue::ZonedDateTime(ZonedDateTime::from_nanos_utc(ts)),
                                ),
                                (IStr::new("interpolated"), GqlValue::Bool(true)),
                            ]])
                        }
                        // No sample after: fall back to LOCF
                        None => Ok(vec![smallvec![
                            (IStr::new("value"), GqlValue::Float(before_sample.value)),
                            (
                                IStr::new("actual_timestamp"),
                                GqlValue::ZonedDateTime(ZonedDateTime::from_nanos_utc(
                                    before_sample.timestamp_nanos,
                                )),
                            ),
                            (IStr::new("interpolated"), GqlValue::Bool(true)),
                        ]]),
                    }
                }
                None => Ok(vec![]), // No fill, no exact match
            },
            None => Ok(vec![]), // No samples at or before ts
        }
    }
}

/// Look up the schema-defined fill strategy for a property on a node.
fn resolve_schema_fill(
    graph: &SeleneGraph,
    node_id: NodeId,
    property: &str,
) -> Option<selene_core::FillStrategy> {
    let node = graph.get_node(node_id)?;
    for label in node.labels.iter() {
        if let Some(schema) = graph.schema().node_schema(label.as_str())
            && let Some(prop_def) = schema.properties.iter().find(|p| *p.name == *property)
        {
            return prop_def.fill;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{LabelSet, PropertyMap, Value};
    use selene_ts::{HotTier, TimeSample, TsConfig};
    use smol_str::SmolStr;

    fn setup() -> (SeleneGraph, HotTier) {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("S1")))]),
        )
        .unwrap();
        m.commit(0).unwrap();

        let hot = HotTier::new(TsConfig::default());
        let now = selene_core::now_nanos();
        // Add 10 samples over the last 10 minutes
        for i in 0..10 {
            hot.append(
                NodeId(1),
                "temperature",
                TimeSample {
                    timestamp_nanos: now - (10 - i) * 60_000_000_000, // each 1 min apart
                    value: 70.0 + i as f64,
                },
            );
        }

        (g, hot)
    }

    #[test]
    fn ts_latest() {
        let (g, hot) = setup();
        let proc = TsLatest;
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String(SmolStr::new("temperature")),
        ];
        let rows = proc.execute(&args, &g, Some(&hot), None).unwrap();
        assert_eq!(rows.len(), 1);
        // Latest value is 79.0 (70 + 9)
        match &rows[0][1].1 {
            GqlValue::Float(v) => assert_eq!(*v, 79.0),
            other => panic!("expected Float, got {other:?}"),
        }
    }

    #[test]
    fn ts_latest_no_data() {
        let (g, hot) = setup();
        let proc = TsLatest;
        let args = vec![
            GqlValue::Int(999), // nonexistent entity
            GqlValue::String(SmolStr::new("temperature")),
        ];
        let rows = proc.execute(&args, &g, Some(&hot), None).unwrap();
        assert_eq!(rows.len(), 0); // No data -> empty (inner-join filters out)
    }

    #[test]
    fn ts_range() {
        let (g, hot) = setup();
        let proc = TsRange;
        let now = selene_core::now_nanos();
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String(SmolStr::new("temperature")),
            GqlValue::Int(now - 5 * 60_000_000_000), // 5 minutes ago
            GqlValue::Int(now),
        ];
        let rows = proc.execute(&args, &g, Some(&hot), None).unwrap();
        // 5 minutes covers roughly 5 samples (1 per minute)
        assert!(rows.len() >= 4 && rows.len() <= 6);
    }

    #[test]
    fn ts_no_hot_tier_error() {
        let g = SeleneGraph::new();
        let proc = TsLatest;
        let args = vec![GqlValue::Int(1), GqlValue::String(SmolStr::new("temp"))];
        let result = proc.execute(&args, &g, None, None);
        assert!(result.is_err());
    }

    // ── Auth scope tests ─────────────────────────────────────────────

    fn setup_with_two_entities() -> (SeleneGraph, HotTier) {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("S1")))]),
        )
        .unwrap();
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("S2")))]),
        )
        .unwrap();
        m.commit(0).unwrap();

        let hot = HotTier::new(TsConfig::default());
        let now = selene_core::now_nanos();
        for i in 0..5 {
            hot.append(
                NodeId(1),
                "temp",
                TimeSample {
                    timestamp_nanos: now - (5 - i) * 60_000_000_000,
                    value: 70.0 + i as f64,
                },
            );
            hot.append(
                NodeId(2),
                "temp",
                TimeSample {
                    timestamp_nanos: now - (5 - i) * 60_000_000_000,
                    value: 50.0 + i as f64,
                },
            );
        }
        (g, hot)
    }

    fn scope_for(ids: &[u32]) -> roaring::RoaringBitmap {
        let mut bm = roaring::RoaringBitmap::new();
        for &id in ids {
            bm.insert(id);
        }
        bm
    }

    #[test]
    fn ts_latest_respects_scope() {
        let (g, hot) = setup_with_two_entities();
        let scope = scope_for(&[1]); // only entity 1

        // Entity 1: in scope -> data returned
        let rows = TsLatest
            .execute(
                &[GqlValue::Int(1), GqlValue::String(SmolStr::new("temp"))],
                &g,
                Some(&hot),
                Some(&scope),
            )
            .unwrap();
        assert_eq!(rows.len(), 1);

        // Entity 2: out of scope -> empty
        let rows = TsLatest
            .execute(
                &[GqlValue::Int(2), GqlValue::String(SmolStr::new("temp"))],
                &g,
                Some(&hot),
                Some(&scope),
            )
            .unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn ts_range_respects_scope() {
        let (g, hot) = setup_with_two_entities();
        let scope = scope_for(&[1]);
        let now = selene_core::now_nanos();

        let rows = TsRange
            .execute(
                &[
                    GqlValue::Int(2),
                    GqlValue::String(SmolStr::new("temp")),
                    GqlValue::Int(0),
                    GqlValue::Int(now),
                ],
                &g,
                Some(&hot),
                Some(&scope),
            )
            .unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn ts_scope_none_allows_all() {
        let (g, hot) = setup_with_two_entities();
        // No scope -> all entities accessible
        let rows = TsLatest
            .execute(
                &[GqlValue::Int(2), GqlValue::String(SmolStr::new("temp"))],
                &g,
                Some(&hot),
                None,
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
    }
}
