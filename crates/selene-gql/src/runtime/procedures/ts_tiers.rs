//! Multi-tier time-series procedures: ts.downsample, ts.history, ts.fullRange, ts.trends.
//!
//! These procedures query warm (pre-aggregated) and cold (Parquet) tiers,
//! or merge across tiers for unified time range access.

use selene_core::{IStr, NodeId};
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;

use super::*;
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue, ZonedDateTime};

use super::ts::extract_timestamp;

// ── ts.downsample ─────────────────────────────────────────────────

/// ts.downsample(entity_id, property, start, end) -> window_start, min, max, avg, count
///
/// Returns pre-computed warm tier aggregates (one row per tumbling window).
pub struct TsDownsample;

impl Procedure for TsDownsample {
    fn name(&self) -> &'static str {
        "ts.downsample"
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
                    name: "window_start",
                    typ: GqlType::ZonedDateTime,
                },
                YieldColumn {
                    name: "min",
                    typ: GqlType::Float,
                },
                YieldColumn {
                    name: "max",
                    typ: GqlType::Float,
                },
                YieldColumn {
                    name: "avg",
                    typ: GqlType::Float,
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
        _graph: &SeleneGraph,
        hot_tier: Option<&HotTier>,
        scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        let hot = hot_tier.ok_or_else(|| GqlError::internal("time-series not available"))?;

        let entity_id = args[0].as_int()?;

        if let Some(scope) = scope
            && !scope.contains(entity_id as u32)
        {
            return Ok(vec![]);
        }

        let property = args[1].as_str()?;
        let start_nanos = extract_timestamp(&args[2])?;
        let end_nanos = extract_timestamp(&args[3])?;

        let Some(warm) = hot.warm_tier() else {
            return Ok(vec![]);
        };

        let aggregates = warm.range(NodeId(entity_id as u64), property, start_nanos, end_nanos);

        Ok(aggregates
            .into_iter()
            .map(|a| {
                smallvec![
                    (
                        IStr::new("window_start"),
                        GqlValue::ZonedDateTime(ZonedDateTime::from_nanos_utc(
                            a.window_start_nanos
                        ))
                    ),
                    (IStr::new("min"), GqlValue::Float(a.min)),
                    (IStr::new("max"), GqlValue::Float(a.max)),
                    (IStr::new("avg"), GqlValue::Float(a.avg())),
                    (IStr::new("count"), GqlValue::Int(i64::from(a.count))),
                ]
            })
            .collect())
    }
}

// ── ts.history ────────────────────────────────────────────────────

/// ts.history(entity_id, property, start, end) -> timestamp, value
///
/// Returns historical samples from the cold tier (Parquet files on disk).
/// Requires a TsHistoryProvider registered at server startup.
pub struct TsHistory;

impl Procedure for TsHistory {
    fn name(&self) -> &'static str {
        "ts.history"
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
        _hot_tier: Option<&HotTier>,
        scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        let entity_id = args[0].as_int()?;

        if let Some(scope) = scope
            && !scope.contains(entity_id as u32)
        {
            return Ok(vec![]);
        }

        let property = args[1].as_str()?;
        let start_nanos = extract_timestamp(&args[2])?;
        let end_nanos = extract_timestamp(&args[3])?;

        let provider = super::ts_history_provider::get_ts_history_provider()?;
        let samples = provider.query(entity_id as u64, property, start_nanos, end_nanos);

        Ok(samples
            .into_iter()
            .map(|(ts, val)| {
                smallvec![
                    (
                        IStr::new("timestamp"),
                        GqlValue::ZonedDateTime(ZonedDateTime::from_nanos_utc(ts))
                    ),
                    (IStr::new("value"), GqlValue::Float(val)),
                ]
            })
            .collect())
    }
}

// ── ts.fullRange ──────────────────────────────────────────────────

/// ts.fullRange(entity_id, property, start, end) -> timestamp, value
///
/// Queries both hot (in-memory) and cold (Parquet) tiers, merges by
/// timestamp, and deduplicates so each timestamp appears at most once.
/// Hot tier values take precedence over cold on duplicate timestamps.
pub struct TsFullRange;

impl Procedure for TsFullRange {
    fn name(&self) -> &'static str {
        "ts.fullRange"
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

        // Collect (timestamp_nanos, value) pairs from both tiers.
        // Use a BTreeMap keyed by timestamp so results are sorted and
        // hot tier entries overwrite cold tier entries on collision.
        let mut merged = std::collections::BTreeMap::<i64, f64>::new();

        // Cold tier first (lower priority on duplicate timestamps)
        if let Ok(provider) = super::ts_history_provider::get_ts_history_provider() {
            for (ts, val) in provider.query(entity_id as u64, property, start_nanos, end_nanos) {
                merged.insert(ts, val);
            }
        }

        // Hot tier second (overwrites cold on duplicate timestamps)
        if let Some(hot) = hot_tier {
            for s in hot.range(NodeId(entity_id as u64), property, start_nanos, end_nanos) {
                merged.insert(s.timestamp_nanos, s.value);
            }
        }

        Ok(merged
            .into_iter()
            .map(|(ts, val)| {
                smallvec![
                    (
                        IStr::new("timestamp"),
                        GqlValue::ZonedDateTime(ZonedDateTime::from_nanos_utc(ts))
                    ),
                    (IStr::new("value"), GqlValue::Float(val)),
                ]
            })
            .collect())
    }
}

// ── ts.trends ─────────────────────────────────────────────────────

/// ts.trends(entity_id, property, start, end) -> window_start, min, max, avg, count
///
/// Returns hourly aggregates from the hierarchical warm tier for month-scale dashboards.
pub struct TsTrends;

impl Procedure for TsTrends {
    fn name(&self) -> &'static str {
        "ts.trends"
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
                    name: "window_start",
                    typ: GqlType::ZonedDateTime,
                },
                YieldColumn {
                    name: "min",
                    typ: GqlType::Float,
                },
                YieldColumn {
                    name: "max",
                    typ: GqlType::Float,
                },
                YieldColumn {
                    name: "avg",
                    typ: GqlType::Float,
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
        _graph: &SeleneGraph,
        hot_tier: Option<&HotTier>,
        scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        let hot = hot_tier.ok_or_else(|| GqlError::internal("time-series not available"))?;

        let entity_id = args[0].as_int()?;

        if let Some(scope) = scope
            && !scope.contains(entity_id as u32)
        {
            return Ok(vec![]);
        }

        let property = args[1].as_str()?;
        let start_nanos = extract_timestamp(&args[2])?;
        let end_nanos = extract_timestamp(&args[3])?;

        let Some(warm) = hot.warm_tier() else {
            return Ok(vec![]);
        };
        let Some(hourly) = warm.hourly_tier() else {
            return Ok(vec![]);
        };

        let aggregates = hourly.range(NodeId(entity_id as u64), property, start_nanos, end_nanos);

        Ok(aggregates
            .into_iter()
            .map(|a| {
                smallvec![
                    (
                        IStr::new("window_start"),
                        GqlValue::ZonedDateTime(ZonedDateTime::from_nanos_utc(
                            a.window_start_nanos
                        ))
                    ),
                    (IStr::new("min"), GqlValue::Float(a.min)),
                    (IStr::new("max"), GqlValue::Float(a.max)),
                    (IStr::new("avg"), GqlValue::Float(a.avg())),
                    (IStr::new("count"), GqlValue::Int(i64::from(a.count))),
                ]
            })
            .collect())
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
    fn ts_downsample_no_warm_tier_returns_empty() {
        // Default config has warm_tier = None
        let (g, hot) = setup();
        let rows = TsDownsample
            .execute(
                &[
                    GqlValue::Int(1),
                    GqlValue::String(SmolStr::new("temperature")),
                    GqlValue::Int(0),
                    GqlValue::Int(i64::MAX),
                ],
                &g,
                Some(&hot),
                None,
            )
            .unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn ts_downsample_with_warm_tier() {
        use selene_ts::WarmTierConfig;
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("S1")))]),
        )
        .unwrap();
        m.commit(0).unwrap();

        let hot = HotTier::new(TsConfig {
            warm_tier: Some(WarmTierConfig {
                downsample_interval_secs: 60,
                retention_hours: 24,
                ddsketch_enabled: true,
                hourly: None,
            }),
            ..TsConfig::default()
        });

        // Append samples spanning 3 minutes to get at least 2 finalized windows
        let base = 1000 * 60_000_000_000i64; // minute 1000
        for i in 0..3 {
            hot.append(
                NodeId(1),
                "temp",
                TimeSample {
                    timestamp_nanos: base + i * 60_000_000_000 + 1,
                    value: 70.0 + i as f64,
                },
            );
        }

        let rows = TsDownsample
            .execute(
                &[
                    GqlValue::Int(1),
                    GqlValue::String(SmolStr::new("temp")),
                    GqlValue::Int(0),
                    GqlValue::Int(i64::MAX),
                ],
                &g,
                Some(&hot),
                None,
            )
            .unwrap();
        // At least 2 windows (minute 1000 finalized + minute 1001 finalized, minute 1002 current)
        assert!(rows.len() >= 2, "expected >=2 windows, got {}", rows.len());
        // Each row has 5 columns: window_start, min, max, avg, count
        assert_eq!(rows[0].len(), 5);
    }

    // ── ts.fullRange tests ──────────────────────────────────────────

    #[test]
    fn ts_full_range_hot_only() {
        // Without a cold tier provider, fullRange should still return hot tier data.
        let (g, hot) = setup();
        let now = selene_core::now_nanos();
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String(SmolStr::new("temperature")),
            GqlValue::Int(now - 5 * 60_000_000_000),
            GqlValue::Int(now),
        ];
        let rows = TsFullRange.execute(&args, &g, Some(&hot), None).unwrap();
        assert!(
            rows.len() >= 4 && rows.len() <= 6,
            "got {} rows",
            rows.len()
        );
        // Each row has 2 columns: timestamp, value
        assert_eq!(rows[0].len(), 2);
    }

    #[test]
    fn ts_full_range_no_tiers_returns_empty() {
        // No hot tier and no cold tier provider: returns empty, not an error.
        let (g, _hot) = setup();
        let now = selene_core::now_nanos();
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String(SmolStr::new("temperature")),
            GqlValue::Int(now - 5 * 60_000_000_000),
            GqlValue::Int(now),
        ];
        let rows = TsFullRange.execute(&args, &g, None, None).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn ts_full_range_respects_scope() {
        let (g, hot) = setup_with_two_entities();
        let scope = scope_for(&[1]);
        let now = selene_core::now_nanos();

        // Entity 2 out of scope: returns empty
        let rows = TsFullRange
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

        // Entity 1 in scope: returns data
        let rows = TsFullRange
            .execute(
                &[
                    GqlValue::Int(1),
                    GqlValue::String(SmolStr::new("temp")),
                    GqlValue::Int(0),
                    GqlValue::Int(now),
                ],
                &g,
                Some(&hot),
                Some(&scope),
            )
            .unwrap();
        assert!(!rows.is_empty());
    }

    #[test]
    fn ts_full_range_sorted_output() {
        let (g, hot) = setup();
        let now = selene_core::now_nanos();
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String(SmolStr::new("temperature")),
            GqlValue::Int(0),
            GqlValue::Int(now),
        ];
        let rows = TsFullRange.execute(&args, &g, Some(&hot), None).unwrap();
        // Verify timestamps are in ascending order
        let timestamps: Vec<i64> = rows
            .iter()
            .map(|r| match &r[0].1 {
                GqlValue::ZonedDateTime(zdt) => zdt.nanos,
                other => panic!("expected ZonedDateTime, got {other:?}"),
            })
            .collect();
        for w in timestamps.windows(2) {
            assert!(w[0] <= w[1], "timestamps not sorted: {} > {}", w[0], w[1]);
        }
    }
}
