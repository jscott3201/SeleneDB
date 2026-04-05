//! ts.percentile -- streaming quantile queries from warm tier DDSketch.

use selene_core::{IStr, NodeId};
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;

use super::*;
use crate::runtime::procedures::ts::extract_duration;
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

/// ts.percentile(entity_id, property, quantile, duration) -> value
///
/// Returns a percentile value from the warm tier's DDSketch accumulators.
/// Quantile is a float between 0.0 and 1.0 (e.g. 0.95 for p95).
///
/// Pre-computed quantiles (p50, p90, p95, p99) are snapshotted per window.
/// The procedure returns the closest pre-computed quantile, weighted by
/// sample count across all windows in the requested duration.
pub struct TsPercentile;

impl Procedure for TsPercentile {
    fn name(&self) -> &'static str {
        "ts.percentile"
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
                    name: "quantile",
                    typ: GqlType::Float,
                },
                ProcedureParam {
                    name: "duration",
                    typ: GqlType::ZonedDateTime,
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

        let entity_id = args[0].as_int()?;

        if let Some(scope) = scope
            && !scope.contains(entity_id as u32)
        {
            return Ok(vec![]);
        }

        let property = args[1].as_str()?;
        let quantile = args[2].as_float()?;

        if !(0.0..=1.0).contains(&quantile) {
            return Err(GqlError::InvalidArgument {
                message: format!("quantile must be between 0.0 and 1.0, got {quantile}"),
            });
        }

        let duration_nanos = extract_duration(&args[3])?;
        let now = selene_core::now_nanos();
        let start = now - duration_nanos;

        let Some(warm) = hot.warm_tier() else {
            return Ok(vec![]);
        };

        let aggregates = warm.range(NodeId(entity_id as u64), property, start, now);

        if aggregates.is_empty() {
            return Ok(vec![]);
        }

        // Pre-computed quantile points and their tuple index
        let quantile_points: [(f64, usize); 4] = [(0.50, 0), (0.90, 1), (0.95, 2), (0.99, 3)];

        // Find the closest pre-computed quantile
        let closest_idx = quantile_points
            .iter()
            .min_by(|a, b| {
                (a.0 - quantile)
                    .abs()
                    .partial_cmp(&(b.0 - quantile).abs())
                    .unwrap()
            })
            .unwrap()
            .1;

        // Weighted average across windows (weighted by sample count)
        let mut weighted_sum = 0.0;
        let mut total_count = 0u64;

        for agg in &aggregates {
            if let Some(q) = agg.quantiles {
                let vals = [q.0, q.1, q.2, q.3];
                weighted_sum += vals[closest_idx] * f64::from(agg.count);
                total_count += u64::from(agg.count);
            }
        }

        if total_count == 0 {
            return Ok(vec![]);
        }

        let result = weighted_sum / total_count as f64;

        Ok(vec![smallvec![(
            IStr::new("value"),
            GqlValue::Float(result)
        )]])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{LabelSet, PropertyMap, Value};
    use selene_ts::config::WarmTierConfig;
    use selene_ts::{TimeSample, TsConfig};
    use smol_str::SmolStr;

    fn setup_with_warm() -> (SeleneGraph, HotTier) {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("S1")))]),
        )
        .unwrap();
        m.commit(0).unwrap();

        let config = TsConfig {
            warm_tier: Some(WarmTierConfig {
                downsample_interval_secs: 60,
                retention_hours: 24,
                ddsketch_enabled: true,
                hourly: None,
            }),
            ..TsConfig::default()
        };
        let hot = HotTier::new(config);

        // Add 100 samples within the same minute window
        let now = selene_core::now_nanos();
        let base = now - 30_000_000_000; // 30 seconds ago
        for i in 0..100 {
            hot.append(
                NodeId(1),
                "temperature",
                TimeSample {
                    timestamp_nanos: base + i * 100_000_000, // 100ms apart
                    value: i as f64 + 1.0,
                },
            );
        }
        // Force window finalization by writing to next window
        hot.append(
            NodeId(1),
            "temperature",
            TimeSample {
                timestamp_nanos: base + 61_000_000_000,
                value: 50.0,
            },
        );

        (g, hot)
    }

    #[test]
    fn percentile_p50() {
        let (g, hot) = setup_with_warm();
        let proc = TsPercentile;
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String("temperature".into()),
            GqlValue::Float(0.50),
            GqlValue::Int(3_600_000_000_000),
        ];
        let result = proc.execute(&args, &g, Some(&hot), None).unwrap();
        assert_eq!(result.len(), 1);
        let val = result[0][0].1.as_float().unwrap();
        // p50 of 1..100 should be around 50
        assert!(val > 40.0 && val < 60.0, "p50 = {val}");
    }

    #[test]
    fn percentile_invalid_quantile() {
        let (g, hot) = setup_with_warm();
        let proc = TsPercentile;
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String("temperature".into()),
            GqlValue::Float(1.5),
            GqlValue::Int(3_600_000_000_000),
        ];
        assert!(proc.execute(&args, &g, Some(&hot), None).is_err());
    }

    #[test]
    fn percentile_scope_filtered() {
        let (g, hot) = setup_with_warm();
        let proc = TsPercentile;
        let mut scope = roaring::RoaringBitmap::new();
        scope.insert(999);
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String("temperature".into()),
            GqlValue::Float(0.95),
            GqlValue::Int(3_600_000_000_000),
        ];
        let result = proc.execute(&args, &g, Some(&hot), Some(&scope)).unwrap();
        assert!(result.is_empty());
    }
}
