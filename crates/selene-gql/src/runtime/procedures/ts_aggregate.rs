//! Aggregation time-series procedures: ts.aggregate, ts.window.
//!
//! These procedures compute scalar or windowed aggregates over raw hot-tier
//! samples within a lookback duration.

use selene_core::{IStr, NodeId};
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;

use super::*;
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue, ZonedDateTime};

use super::ts::extract_duration;

// ── ts.aggregate ───────────────────────────────────────────────────

/// ts.aggregate(entity_id, property, duration, agg_fn) -> value
///
/// Returns a scalar aggregate over a time range.
pub struct TsAggregate;

impl Procedure for TsAggregate {
    fn name(&self) -> &'static str {
        "ts.aggregate"
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
                    name: "duration",
                    typ: GqlType::ZonedDateTime,
                },
                ProcedureParam {
                    name: "agg_fn",
                    typ: GqlType::String,
                },
            ],
            yields: vec![YieldColumn {
                name: "value",
                typ: GqlType::Float,
            }],
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

        if args.len() < 4 {
            return Err(GqlError::InvalidArgument {
                message: "ts.aggregate requires 4 arguments: entity_id, property, duration, agg_fn"
                    .into(),
            });
        }

        let entity_id = args[0].as_int()?;

        // Auth scope check
        if let Some(scope) = scope
            && !scope.contains(entity_id as u32)
        {
            return Ok(vec![]);
        }

        let property = args[1].as_str()?;
        let duration_nanos = extract_duration(&args[2])?;
        let agg_fn = args[3].as_str()?;

        let now = selene_core::now_nanos();
        let start = now - duration_nanos;
        let samples = hot.range(NodeId(entity_id as u64), property, start, now);

        if samples.is_empty() {
            return Ok(vec![]);
        }

        let result = match agg_fn {
            "avg" => {
                let sum: f64 = samples.iter().map(|s| s.value).sum();
                sum / samples.len() as f64
            }
            "sum" => samples.iter().map(|s| s.value).sum(),
            "min" => samples
                .iter()
                .map(|s| s.value)
                .fold(f64::INFINITY, f64::min),
            "max" => samples
                .iter()
                .map(|s| s.value)
                .fold(f64::NEG_INFINITY, f64::max),
            "count" => samples.len() as f64,
            "twa" => {
                let window_end = now;
                // Check for a sample before the window to establish initial value
                let left_boundary =
                    hot.sample_at_or_before(NodeId(entity_id as u64), property, start - 1);
                let total_duration = duration_nanos as f64;

                // Build effective samples: optional left boundary + actual samples
                let mut effective: Vec<selene_ts::TimeSample> = Vec::new();
                if let Some(lb) = left_boundary {
                    effective.push(selene_ts::TimeSample {
                        timestamp_nanos: start,
                        value: lb.value,
                    });
                }
                effective.extend(samples.iter().copied());

                if effective.is_empty() || total_duration <= 0.0 {
                    return Ok(vec![]);
                }

                // Weight each sample by the duration until the next
                let mut weighted_sum = 0.0_f64;
                for i in 0..effective.len() {
                    let next_ts = if i + 1 < effective.len() {
                        effective[i + 1].timestamp_nanos
                    } else {
                        window_end
                    };
                    let duration = (next_ts - effective[i].timestamp_nanos) as f64;
                    weighted_sum += effective[i].value * duration;
                }

                weighted_sum / total_duration
            }
            _ => {
                return Err(GqlError::InvalidArgument {
                    message: format!("unknown aggregation function: '{agg_fn}'"),
                });
            }
        };

        Ok(vec![smallvec![(
            IStr::new("value"),
            GqlValue::Float(result)
        ),]])
    }
}

// ── ts.window ──────────────────────────────────────────────────────

/// ts.window(entity_id, property, window_size, agg_fn, duration) -> window_start, window_end, value
///
/// Tumbling window aggregation returning one row per window.
pub struct TsWindow;

impl Procedure for TsWindow {
    fn name(&self) -> &'static str {
        "ts.window"
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
                    name: "window_size",
                    typ: GqlType::ZonedDateTime,
                },
                ProcedureParam {
                    name: "agg_fn",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "duration",
                    typ: GqlType::ZonedDateTime,
                },
            ],
            yields: vec![
                YieldColumn {
                    name: "window_start",
                    typ: GqlType::ZonedDateTime,
                },
                YieldColumn {
                    name: "window_end",
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

        let entity_id = args[0].as_int()?;

        // Auth scope check
        if let Some(scope) = scope
            && !scope.contains(entity_id as u32)
        {
            return Ok(vec![]);
        }

        let property = args[1].as_str()?;
        let window_nanos = extract_duration(&args[2])?;
        let agg_fn = args[3].as_str()?;
        let duration_nanos = extract_duration(&args[4])?;

        let now = selene_core::now_nanos();
        let start = now - duration_nanos;
        let samples = hot.range(NodeId(entity_id as u64), property, start, now);

        if samples.is_empty() || window_nanos == 0 {
            return Ok(vec![]);
        }

        // Group samples into tumbling windows using BTreeMap for sorted output
        // and correct handling of unsorted input
        let min_ts = samples.first().map_or(now, |s| s.timestamp_nanos);
        let mut window_map: std::collections::BTreeMap<i64, (i64, Vec<f64>)> =
            std::collections::BTreeMap::new();

        for sample in &samples {
            let window_idx = (sample.timestamp_nanos - min_ts) / window_nanos;
            let window_start = min_ts + window_idx * window_nanos;
            let window_end = window_start + window_nanos;
            window_map
                .entry(window_start)
                .or_insert_with(|| (window_end, Vec::new()))
                .1
                .push(sample.value);
        }

        // Aggregate each window
        window_map
            .into_iter()
            .map(|(ws, (we, values))| {
                let agg_value = match agg_fn {
                    "avg" => values.iter().sum::<f64>() / values.len() as f64,
                    "sum" => values.iter().sum(),
                    "min" => values.iter().copied().fold(f64::INFINITY, f64::min),
                    "max" => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
                    "count" => values.len() as f64,
                    _ => {
                        return Err(GqlError::InvalidArgument {
                            message: format!("unknown aggregation: '{agg_fn}'"),
                        });
                    }
                };

                Ok(smallvec![
                    (
                        IStr::new("window_start"),
                        GqlValue::ZonedDateTime(ZonedDateTime::from_nanos_utc(ws))
                    ),
                    (
                        IStr::new("window_end"),
                        GqlValue::ZonedDateTime(ZonedDateTime::from_nanos_utc(we))
                    ),
                    (IStr::new("value"), GqlValue::Float(agg_value)),
                ])
            })
            .collect()
    }
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
        for i in 0..10 {
            hot.append(
                NodeId(1),
                "temperature",
                TimeSample {
                    timestamp_nanos: now - (10 - i) * 60_000_000_000,
                    value: 70.0 + i as f64,
                },
            );
        }

        (g, hot)
    }

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
    fn ts_aggregate_avg() {
        let (g, hot) = setup();
        let proc = TsAggregate;
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String(SmolStr::new("temperature")),
            GqlValue::String(SmolStr::new("15m")), // covers all 10 samples
            GqlValue::String(SmolStr::new("avg")),
        ];
        let rows = proc.execute(&args, &g, Some(&hot), None).unwrap();
        assert_eq!(rows.len(), 1);
        match &rows[0][0].1 {
            GqlValue::Float(v) => {
                // avg of 70..79 = 74.5
                assert!((*v - 74.5).abs() < 0.1);
            }
            other => panic!("expected Float, got {other:?}"),
        }
    }

    #[test]
    fn ts_aggregate_min_max() {
        let (g, hot) = setup();
        let proc = TsAggregate;

        let min_args = vec![
            GqlValue::Int(1),
            GqlValue::String(SmolStr::new("temperature")),
            GqlValue::String(SmolStr::new("15m")),
            GqlValue::String(SmolStr::new("min")),
        ];
        let rows = proc.execute(&min_args, &g, Some(&hot), None).unwrap();
        match &rows[0][0].1 {
            GqlValue::Float(v) => assert_eq!(*v, 70.0),
            other => panic!("expected Float, got {other:?}"),
        }

        let max_args = vec![
            GqlValue::Int(1),
            GqlValue::String(SmolStr::new("temperature")),
            GqlValue::String(SmolStr::new("15m")),
            GqlValue::String(SmolStr::new("max")),
        ];
        let rows = proc.execute(&max_args, &g, Some(&hot), None).unwrap();
        match &rows[0][0].1 {
            GqlValue::Float(v) => assert_eq!(*v, 79.0),
            other => panic!("expected Float, got {other:?}"),
        }
    }

    #[test]
    fn ts_window() {
        let (g, hot) = setup();
        let proc = TsWindow;
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String(SmolStr::new("temperature")),
            GqlValue::String(SmolStr::new("5m")), // 5-minute windows
            GqlValue::String(SmolStr::new("avg")),
            GqlValue::String(SmolStr::new("15m")), // lookback duration
        ];
        let rows = proc.execute(&args, &g, Some(&hot), None).unwrap();
        // 10 minutes of data in 5-minute windows -> 2 windows
        assert!(!rows.is_empty());
        // Each row should have window_start, window_end, value
        assert_eq!(rows[0].len(), 3);
    }

    #[test]
    fn ts_aggregate_respects_scope() {
        let (g, hot) = setup_with_two_entities();
        let scope = scope_for(&[1]);

        let rows = TsAggregate
            .execute(
                &[
                    GqlValue::Int(2),
                    GqlValue::String(SmolStr::new("temp")),
                    GqlValue::String(SmolStr::new("1h")),
                    GqlValue::String(SmolStr::new("avg")),
                ],
                &g,
                Some(&hot),
                Some(&scope),
            )
            .unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn ts_window_respects_scope() {
        let (g, hot) = setup_with_two_entities();
        let scope = scope_for(&[1]);

        let rows = TsWindow
            .execute(
                &[
                    GqlValue::Int(2),
                    GqlValue::String(SmolStr::new("temp")),
                    GqlValue::String(SmolStr::new("5m")),
                    GqlValue::String(SmolStr::new("avg")),
                    GqlValue::String(SmolStr::new("1h")),
                ],
                &g,
                Some(&hot),
                Some(&scope),
            )
            .unwrap();
        assert!(rows.is_empty());
    }
}
