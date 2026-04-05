//! Gap detection procedure: ts.gaps.

use selene_core::{IStr, NodeId};
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;

use super::*;
use crate::runtime::procedures::ts::{
    extract_duration, extract_timestamp, resolve_schema_interval,
};
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue, ZonedDateTime};

/// ts.gaps(entity_id, property, start, end, `[threshold]`) -> gap_start, gap_end, duration_nanos
///
/// Detects gaps in a time series where the interval between consecutive
/// samples exceeds the threshold. Threshold resolution: explicit param >
/// 3x schema expected_interval > error.
pub struct TsGaps;

impl Procedure for TsGaps {
    fn name(&self) -> &'static str {
        "ts.gaps"
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
                    name: "gap_start",
                    typ: GqlType::ZonedDateTime,
                },
                YieldColumn {
                    name: "gap_end",
                    typ: GqlType::ZonedDateTime,
                },
                YieldColumn {
                    name: "duration_nanos",
                    typ: GqlType::Int,
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
        let start_nanos = extract_timestamp(&args[2])?;
        let end_nanos = extract_timestamp(&args[3])?;

        // Resolve threshold: explicit param > 3x schema interval > error
        let threshold_nanos = if args.len() > 4 {
            extract_duration(&args[4])?
        } else {
            let node_id = NodeId(entity_id as u64);
            let interval = resolve_schema_interval(graph, node_id, property);
            match interval {
                Some(nanos) => nanos * 3,
                None => {
                    return Err(GqlError::internal(
                        "no gap threshold specified and no expected interval in schema",
                    ));
                }
            }
        };

        let samples = hot.range(NodeId(entity_id as u64), property, start_nanos, end_nanos);

        let mut gaps = Vec::new();
        for window in samples.windows(2) {
            let delta = window[1].timestamp_nanos - window[0].timestamp_nanos;
            if delta > threshold_nanos {
                gaps.push(smallvec![
                    (
                        IStr::new("gap_start"),
                        GqlValue::ZonedDateTime(ZonedDateTime::from_nanos_utc(
                            window[0].timestamp_nanos,
                        )),
                    ),
                    (
                        IStr::new("gap_end"),
                        GqlValue::ZonedDateTime(ZonedDateTime::from_nanos_utc(
                            window[1].timestamp_nanos,
                        )),
                    ),
                    (IStr::new("duration_nanos"), GqlValue::Int(delta)),
                ]);
            }
        }

        Ok(gaps)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{IStr, LabelSet, NodeId, PropertyMap, Value};
    use selene_graph::SeleneGraph;
    use selene_ts::{HotTier, TimeSample, TsConfig};
    use smol_str::SmolStr;

    const SECOND: i64 = 1_000_000_000;
    const MINUTE: i64 = 60 * SECOND;

    fn setup_graph_with_ts(samples: Vec<(i64, f64)>) -> (SeleneGraph, HotTier) {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("S1")))]),
        )
        .unwrap();
        m.commit(0).unwrap();

        let hot = HotTier::new(TsConfig::default());
        for (ts, val) in samples {
            hot.append(
                NodeId(1),
                "temperature",
                TimeSample {
                    timestamp_nanos: ts,
                    value: val,
                },
            );
        }

        (g, hot)
    }

    fn base_time() -> i64 {
        // Use a fixed base time to avoid clock dependency
        1_700_000_000 * SECOND
    }

    // ── Gap detection ───────────────────────────────────────────────

    #[test]
    fn detects_gap_in_samples() {
        let base = base_time();
        // 5 samples at 1-minute intervals, then a 10-minute gap, then 1 more
        let samples = vec![
            (base, 70.0),
            (base + MINUTE, 71.0),
            (base + 2 * MINUTE, 72.0),
            (base + 3 * MINUTE, 73.0),
            (base + 4 * MINUTE, 74.0),
            (base + 14 * MINUTE, 75.0), // 10-minute gap
        ];
        let (g, hot) = setup_graph_with_ts(samples);

        let proc = TsGaps;
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String(SmolStr::new("temperature")),
            GqlValue::Int(base),
            GqlValue::Int(base + 20 * MINUTE),
            GqlValue::Int(5 * MINUTE), // threshold: 5 minutes
        ];
        let rows = proc.execute(&args, &g, Some(&hot), None).unwrap();

        // One gap between the 4-minute mark and 14-minute mark
        assert_eq!(rows.len(), 1);

        match &rows[0][2].1 {
            GqlValue::Int(duration) => {
                assert_eq!(*duration, 10 * MINUTE);
            }
            other => panic!("expected Int, got {other:?}"),
        }
    }

    #[test]
    fn no_gaps_in_continuous_data() {
        let base = base_time();
        let samples: Vec<(i64, f64)> = (0..10)
            .map(|i| (base + i * MINUTE, 70.0 + i as f64))
            .collect();
        let (g, hot) = setup_graph_with_ts(samples);

        let proc = TsGaps;
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String(SmolStr::new("temperature")),
            GqlValue::Int(base),
            GqlValue::Int(base + 15 * MINUTE),
            GqlValue::Int(5 * MINUTE), // threshold higher than interval
        ];
        let rows = proc.execute(&args, &g, Some(&hot), None).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn multiple_gaps_detected() {
        let base = base_time();
        let samples = vec![
            (base, 70.0),
            (base + MINUTE, 71.0),
            (base + 11 * MINUTE, 72.0), // gap 1: 10 min
            (base + 12 * MINUTE, 73.0),
            (base + 22 * MINUTE, 74.0), // gap 2: 10 min
        ];
        let (g, hot) = setup_graph_with_ts(samples);

        let proc = TsGaps;
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String(SmolStr::new("temperature")),
            GqlValue::Int(base),
            GqlValue::Int(base + 30 * MINUTE),
            GqlValue::Int(5 * MINUTE),
        ];
        let rows = proc.execute(&args, &g, Some(&hot), None).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn gap_start_and_end_are_correct() {
        let base = base_time();
        let samples = vec![
            (base, 70.0),
            (base + 10 * MINUTE, 71.0), // 10-min gap
        ];
        let (g, hot) = setup_graph_with_ts(samples);

        let proc = TsGaps;
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String(SmolStr::new("temperature")),
            GqlValue::Int(base),
            GqlValue::Int(base + 15 * MINUTE),
            GqlValue::Int(5 * MINUTE),
        ];
        let rows = proc.execute(&args, &g, Some(&hot), None).unwrap();
        assert_eq!(rows.len(), 1);

        // gap_start should be the timestamp of the sample before the gap
        match &rows[0][0].1 {
            GqlValue::ZonedDateTime(zdt) => assert_eq!(zdt.nanos, base),
            other => panic!("expected ZonedDateTime, got {other:?}"),
        }
        // gap_end should be the timestamp of the sample after the gap
        match &rows[0][1].1 {
            GqlValue::ZonedDateTime(zdt) => assert_eq!(zdt.nanos, base + 10 * MINUTE),
            other => panic!("expected ZonedDateTime, got {other:?}"),
        }
    }

    #[test]
    fn empty_data_no_gaps() {
        let (g, hot) = setup_graph_with_ts(vec![]);
        let base = base_time();
        let proc = TsGaps;
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String(SmolStr::new("temperature")),
            GqlValue::Int(base),
            GqlValue::Int(base + 10 * MINUTE),
            GqlValue::Int(5 * MINUTE),
        ];
        let rows = proc.execute(&args, &g, Some(&hot), None).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn single_sample_no_gaps() {
        let base = base_time();
        let (g, hot) = setup_graph_with_ts(vec![(base, 70.0)]);
        let proc = TsGaps;
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String(SmolStr::new("temperature")),
            GqlValue::Int(base),
            GqlValue::Int(base + 10 * MINUTE),
            GqlValue::Int(5 * MINUTE),
        ];
        let rows = proc.execute(&args, &g, Some(&hot), None).unwrap();
        assert!(rows.is_empty());
    }

    // ── Error paths ─────────────────────────────────────────────────

    #[test]
    fn no_hot_tier_errors() {
        let g = SeleneGraph::new();
        let proc = TsGaps;
        let base = base_time();
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String(SmolStr::new("temperature")),
            GqlValue::Int(base),
            GqlValue::Int(base + 10 * MINUTE),
            GqlValue::Int(5 * MINUTE),
        ];
        let result = proc.execute(&args, &g, None, None);
        assert!(result.is_err());
    }

    #[test]
    fn no_threshold_and_no_schema_interval_errors() {
        let base = base_time();
        let (g, hot) = setup_graph_with_ts(vec![(base, 70.0)]);
        let proc = TsGaps;
        // Only 4 args, no explicit threshold, and no schema interval registered
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String(SmolStr::new("temperature")),
            GqlValue::Int(base),
            GqlValue::Int(base + 10 * MINUTE),
        ];
        let result = proc.execute(&args, &g, Some(&hot), None);
        assert!(result.is_err());
    }

    #[test]
    fn scope_filters_out_entity() {
        let base = base_time();
        let (g, hot) = setup_graph_with_ts(vec![(base, 70.0), (base + 10 * MINUTE, 71.0)]);
        let proc = TsGaps;

        // Create a scope that does not include node 1
        let scope = roaring::RoaringBitmap::new(); // empty scope
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String(SmolStr::new("temperature")),
            GqlValue::Int(base),
            GqlValue::Int(base + 15 * MINUTE),
            GqlValue::Int(5 * MINUTE),
        ];
        let rows = proc.execute(&args, &g, Some(&hot), Some(&scope)).unwrap();
        // Entity filtered out by scope, returns empty
        assert!(rows.is_empty());
    }

    // ── Signature ───────────────────────────────────────────────────

    #[test]
    fn ts_gaps_procedure_name() {
        assert_eq!(TsGaps.name(), "ts.gaps");
    }

    #[test]
    fn ts_gaps_yields_three_columns() {
        let sig = TsGaps.signature();
        assert_eq!(sig.yields.len(), 3);
        assert_eq!(sig.yields[0].name, "gap_start");
        assert_eq!(sig.yields[1].name, "gap_end");
        assert_eq!(sig.yields[2].name, "duration_nanos");
    }
}
