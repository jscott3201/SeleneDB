//! Anomaly detection procedures using Z-score analysis.
//!
//! ts.anomalies -- per-entity Z-score against recent warm-tier statistics.
//! ts.peerAnomalies -- graph-aware peer comparison via BFS neighborhood.

use selene_core::{IStr, NodeId};
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;

use super::*;
use crate::runtime::procedures::ts::extract_duration;
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue, ZonedDateTime};

// ── ts.anomalies ──────────────────────────────────────────────────

/// ts.anomalies(entity_id, property, threshold, duration) -> timestamp, value, z_score
///
/// Compares recent hot-tier samples against warm-tier mean/stddev.
/// Returns samples where |z_score| > threshold.
pub struct TsAnomalies;

impl Procedure for TsAnomalies {
    fn name(&self) -> &'static str {
        "ts.anomalies"
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
                    name: "threshold",
                    typ: GqlType::Float,
                },
                ProcedureParam {
                    name: "duration",
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
                YieldColumn {
                    name: "z_score",
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

        if let Some(scope) = scope
            && !scope.contains(entity_id as u32)
        {
            return Ok(vec![]);
        }

        let property = args[1].as_str()?;
        let threshold = args[2].as_float()?;
        let duration_nanos = extract_duration(&args[3])?;

        if threshold <= 0.0 {
            return Err(GqlError::InvalidArgument {
                message: format!("threshold must be positive, got {threshold}"),
            });
        }

        let now = selene_core::now_nanos();
        let start = now - duration_nanos;
        let node_id = NodeId(entity_id as u64);

        // Get warm-tier statistics for baseline mean and stddev
        let (mean, stddev) = match hot.warm_tier() {
            Some(warm) => {
                let aggregates = warm.range(node_id, property, start, now);
                if aggregates.is_empty() {
                    // No warm data, fall back to computing from raw samples
                    compute_stats_from_hot(hot, node_id, property, start, now)
                } else {
                    combined_stats(&aggregates)
                }
            }
            None => compute_stats_from_hot(hot, node_id, property, start, now),
        };

        if stddev < f64::EPSILON {
            return Ok(vec![]);
        }

        // Scan hot tier for individual samples that exceed threshold
        let samples = hot.range(node_id, property, start, now);
        let mut anomalies = Vec::new();

        for sample in &samples {
            let z = (sample.value - mean) / stddev;
            if z.abs() > threshold {
                anomalies.push(smallvec![
                    (
                        IStr::new("timestamp"),
                        GqlValue::ZonedDateTime(ZonedDateTime::from_nanos_utc(
                            sample.timestamp_nanos,
                        ))
                    ),
                    (IStr::new("value"), GqlValue::Float(sample.value)),
                    (IStr::new("z_score"), GqlValue::Float(z)),
                ]);
            }
        }

        Ok(anomalies)
    }
}

// ── ts.peerAnomalies ──────────────────────────────────────────────

/// ts.peerAnomalies(nodeId, property, maxHops, threshold) -> node_id, value, z_score
///
/// Compare a node's latest reading against its graph neighborhood.
/// BFS from the target node, collect latest values from peers with
/// the same property, flag those that deviate beyond the threshold.
pub struct TsPeerAnomalies;

impl Procedure for TsPeerAnomalies {
    fn name(&self) -> &'static str {
        "ts.peerAnomalies"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "node_id",
                    typ: GqlType::Int,
                },
                ProcedureParam {
                    name: "property",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "maxHops",
                    typ: GqlType::Int,
                },
                ProcedureParam {
                    name: "threshold",
                    typ: GqlType::Float,
                },
            ],
            yields: vec![
                YieldColumn {
                    name: "node_id",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "value",
                    typ: GqlType::Float,
                },
                YieldColumn {
                    name: "z_score",
                    typ: GqlType::Float,
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
        let hot = hot_tier.ok_or_else(|| GqlError::internal("time-series not available"))?;

        let node_id = args[0].as_int()? as u64;
        let property = args[1].as_str()?;
        let max_hops = args[2].as_int()? as u32;
        let threshold = args[3].as_float()?;

        if let Some(scope) = scope
            && !scope.contains(node_id as u32)
        {
            return Ok(vec![]);
        }

        if threshold <= 0.0 {
            return Err(GqlError::InvalidArgument {
                message: format!("threshold must be positive, got {threshold}"),
            });
        }

        // BFS to collect neighborhood (including self)
        let mut neighbors =
            selene_graph::algorithms::traversal::bfs(graph, NodeId(node_id), None, max_hops);
        neighbors.push(NodeId(node_id));

        // Collect latest values for each neighbor
        let mut readings: Vec<(NodeId, f64)> = Vec::new();
        for nid in &neighbors {
            if let Some(scope) = scope
                && !scope.contains(nid.0 as u32)
            {
                continue;
            }
            if let Some(sample) = hot.latest(*nid, property) {
                readings.push((*nid, sample.value));
            }
        }

        if readings.len() < 2 {
            return Ok(vec![]);
        }

        // Population mean and stddev across all peer latest values
        let sum: f64 = readings.iter().map(|(_, v)| v).sum();
        let mean = sum / readings.len() as f64;
        let variance: f64 = readings
            .iter()
            .map(|(_, v)| (v - mean).powi(2))
            .sum::<f64>()
            / readings.len() as f64;
        let stddev = variance.sqrt();

        if stddev < f64::EPSILON {
            return Ok(vec![]);
        }

        // Return nodes that exceed the threshold, sorted by |z_score| descending
        let mut anomalies: Vec<(NodeId, f64, f64)> = readings
            .iter()
            .filter_map(|(nid, val)| {
                let z = (val - mean) / stddev;
                if z.abs() > threshold {
                    Some((*nid, *val, z))
                } else {
                    None
                }
            })
            .collect();

        anomalies.sort_by(|a, b| b.2.abs().partial_cmp(&a.2.abs()).unwrap());

        Ok(anomalies
            .into_iter()
            .map(|(nid, val, z)| {
                smallvec![
                    (IStr::new("node_id"), GqlValue::Int(nid.0 as i64)),
                    (IStr::new("value"), GqlValue::Float(val)),
                    (IStr::new("z_score"), GqlValue::Float(z)),
                ]
            })
            .collect())
    }
}

// ── Shared helpers ────────────────────────────────────────────────

/// Compute mean and stddev directly from hot tier raw samples.
fn compute_stats_from_hot(
    hot: &HotTier,
    node_id: NodeId,
    property: &str,
    start: i64,
    end: i64,
) -> (f64, f64) {
    let samples = hot.range(node_id, property, start, end);
    if samples.len() < 2 {
        return (0.0, 0.0);
    }
    let sum: f64 = samples.iter().map(|s| s.value).sum();
    let mean = sum / samples.len() as f64;
    let variance = samples
        .iter()
        .map(|s| (s.value - mean).powi(2))
        .sum::<f64>()
        / samples.len() as f64;
    (mean, variance.sqrt())
}

/// Compute combined mean and population stddev from a set of WarmAggregates.
///
/// Uses the parallel variance formula: combined_variance =
/// (1/N) * sum_i [ n_i * sigma_i^2 + n_i * (mu_i - mu)^2 ]
fn combined_stats(aggregates: &[selene_ts::WarmAggregate]) -> (f64, f64) {
    let total_count: u64 = aggregates.iter().map(|a| u64::from(a.count)).sum();
    if total_count == 0 {
        return (0.0, 0.0);
    }

    let total_sum: f64 = aggregates.iter().map(|a| a.sum).sum();
    let mean = total_sum / total_count as f64;

    let mut combined_ss: f64 = 0.0;
    for agg in aggregates {
        let n = f64::from(agg.count);
        let window_variance = agg.stddev.map_or(0.0, |s| s * s);
        let window_mean = agg.avg();
        combined_ss += n * window_variance + n * (window_mean - mean).powi(2);
    }
    let stddev = (combined_ss / total_count as f64).sqrt();
    (mean, stddev)
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{LabelSet, PropertyMap, Value};
    use selene_ts::{TimeSample, TsConfig};
    use smol_str::SmolStr;

    fn setup_anomaly() -> (SeleneGraph, HotTier) {
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

        // Normal readings: ~72 degrees with small variance
        for i in 0..20 {
            hot.append(
                NodeId(1),
                "temperature",
                TimeSample {
                    timestamp_nanos: now - (25 - i) * 60_000_000_000,
                    value: 72.0 + (i % 3) as f64 - 1.0, // 71, 72, 73 repeating
                },
            );
        }
        // Anomalous reading: spike to 95
        hot.append(
            NodeId(1),
            "temperature",
            TimeSample {
                timestamp_nanos: now - 60_000_000_000,
                value: 95.0,
            },
        );
        // Anomalous reading: drop to 50
        hot.append(
            NodeId(1),
            "temperature",
            TimeSample {
                timestamp_nanos: now - 30_000_000_000,
                value: 50.0,
            },
        );

        (g, hot)
    }

    #[test]
    fn detects_anomalies() {
        let (g, hot) = setup_anomaly();
        let proc = TsAnomalies;
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String("temperature".into()),
            GqlValue::Float(2.0),
            GqlValue::Int(3_600_000_000_000),
        ];
        let result = proc.execute(&args, &g, Some(&hot), None).unwrap();
        assert!(!result.is_empty(), "expected anomalies, got none");
        for row in &result {
            let z = row[2].1.as_float().unwrap();
            assert!(z.abs() > 2.0, "z_score should exceed threshold");
        }
    }

    #[test]
    fn no_anomalies_with_high_threshold() {
        let (g, hot) = setup_anomaly();
        let proc = TsAnomalies;
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String("temperature".into()),
            GqlValue::Float(100.0),
            GqlValue::Int(3_600_000_000_000),
        ];
        let result = proc.execute(&args, &g, Some(&hot), None).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn invalid_threshold() {
        let (g, hot) = setup_anomaly();
        let proc = TsAnomalies;
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String("temperature".into()),
            GqlValue::Float(-1.0),
            GqlValue::Int(3_600_000_000_000),
        ];
        assert!(proc.execute(&args, &g, Some(&hot), None).is_err());
    }

    #[test]
    fn combined_stats_correctness() {
        use selene_ts::WarmAggregate;
        let aggs = vec![
            WarmAggregate {
                window_start_nanos: 0,
                min: 10.0,
                max: 30.0,
                sum: 60.0,
                count: 3,
                stddev: Some(8.165),
                quantiles: None,
            },
            WarmAggregate {
                window_start_nanos: 60_000_000_000,
                min: 20.0,
                max: 40.0,
                sum: 90.0,
                count: 3,
                stddev: Some(8.165),
                quantiles: None,
            },
        ];
        let (mean, stddev) = combined_stats(&aggs);
        assert!((mean - 25.0).abs() < 0.01, "mean = {mean}");
        assert!(stddev > 0.0, "stddev should be positive");
    }

    fn setup_peer_anomaly() -> (SeleneGraph, HotTier) {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();

        // AHU node (root)
        let ahu = m
            .create_node(
                LabelSet::from_strs(&["ahu"]),
                PropertyMap::from_pairs(vec![(
                    IStr::new("name"),
                    Value::String(SmolStr::new("AHU-1")),
                )]),
            )
            .unwrap();

        // 4 room sensors
        let mut rooms = Vec::new();
        for i in 0..4 {
            let room = m
                .create_node(
                    LabelSet::from_strs(&["sensor"]),
                    PropertyMap::from_pairs(vec![(
                        IStr::new("name"),
                        Value::String(SmolStr::new(format!("Room-{}", 301 + i))),
                    )]),
                )
                .unwrap();
            m.create_edge(ahu, IStr::new("serves"), room, PropertyMap::new())
                .unwrap();
            rooms.push(room);
        }
        m.commit(0).unwrap();

        let hot = HotTier::new(TsConfig::default());
        let now = selene_core::now_nanos();

        // Rooms 301-303: normal temperature ~72
        for (i, &room) in rooms[0..3].iter().enumerate() {
            hot.append(
                room,
                "temperature",
                TimeSample {
                    timestamp_nanos: now - 60_000_000_000,
                    value: 72.0 + i as f64,
                },
            );
        }
        // Room 304: anomalous temperature 95
        hot.append(
            rooms[3],
            "temperature",
            TimeSample {
                timestamp_nanos: now - 60_000_000_000,
                value: 95.0,
            },
        );

        (g, hot)
    }

    #[test]
    fn peer_anomaly_detects_outlier() {
        let (g, hot) = setup_peer_anomaly();
        let proc = TsPeerAnomalies;
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String("temperature".into()),
            GqlValue::Int(2),
            GqlValue::Float(1.5),
        ];
        let result = proc.execute(&args, &g, Some(&hot), None).unwrap();
        assert!(!result.is_empty(), "should detect outlier");
        let outlier_id = result[0][0].1.as_int().unwrap();
        let z_score = result[0][2].1.as_float().unwrap();
        assert_eq!(outlier_id, 5); // room 304 is node 5
        assert!(z_score > 1.5, "z_score should exceed threshold");
    }

    #[test]
    fn peer_anomaly_no_data() {
        let (g, hot) = setup_peer_anomaly();
        let proc = TsPeerAnomalies;
        let args = vec![
            GqlValue::Int(1),
            GqlValue::String("nonexistent".into()),
            GqlValue::Int(2),
            GqlValue::Float(2.0),
        ];
        let result = proc.execute(&args, &g, Some(&hot), None).unwrap();
        assert!(result.is_empty());
    }
}
