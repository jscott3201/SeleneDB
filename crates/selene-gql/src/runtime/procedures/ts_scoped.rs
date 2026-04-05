//! ts.scopedAggregate -- BFS-scoped time-series aggregation.
//!
//! Traverses the graph from a root node via containment edges, collects
//! all descendant sensor nodes, and aggregates their time-series data.

use selene_core::{IStr, NodeId};
use selene_graph::SeleneGraph;
use selene_graph::algorithms::traversal::bfs;
use selene_ts::HotTier;
use smallvec::smallvec;

use super::*;
use crate::runtime::procedures::ts::extract_duration;
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

/// ts.scopedAggregate(rootNodeId, maxHops, property, aggFn, duration) -> value, nodeCount, sampleCount
///
/// BFS from a root node, collect descendant nodes, aggregate their
/// time-series data for the given property and duration.
pub struct TsScopedAggregate;

impl Procedure for TsScopedAggregate {
    fn name(&self) -> &'static str {
        "ts.scopedAggregate"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "rootNodeId",
                    typ: GqlType::Int,
                },
                ProcedureParam {
                    name: "maxHops",
                    typ: GqlType::Int,
                },
                ProcedureParam {
                    name: "property",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "aggFn",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "duration",
                    typ: GqlType::ZonedDateTime,
                },
            ],
            yields: vec![
                YieldColumn {
                    name: "value",
                    typ: GqlType::Float,
                },
                YieldColumn {
                    name: "nodeCount",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "sampleCount",
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
        let hot = hot_tier.ok_or_else(|| GqlError::internal("time-series not available"))?;

        let root_id = args[0].as_int()? as u64;
        let max_hops = args[1].as_int()? as u32;
        let property = args[2].as_str()?;
        let agg_fn = args[3].as_str()?;
        let duration_nanos = extract_duration(&args[4])?;

        // Auth scope check on root
        if let Some(scope) = scope
            && !scope.contains(root_id as u32)
        {
            return Ok(vec![]);
        }

        let now = selene_core::now_nanos();
        let start = now - duration_nanos;

        // BFS to collect descendant nodes
        let neighbors = bfs(graph, NodeId(root_id), None, max_hops);

        // Collect all samples from descendants, filtering by scope
        let mut all_values: Vec<f64> = Vec::new();
        let mut node_count = 0i64;

        for node_id in &neighbors {
            if let Some(scope) = scope
                && !scope.contains(node_id.0 as u32)
            {
                continue;
            }
            let samples = hot.range(*node_id, property, start, now);
            if !samples.is_empty() {
                node_count += 1;
                all_values.extend(samples.iter().map(|s| s.value));
            }
        }

        if all_values.is_empty() {
            return Ok(vec![]);
        }

        let sample_count = all_values.len() as i64;

        let result = match agg_fn {
            "avg" => all_values.iter().sum::<f64>() / all_values.len() as f64,
            "sum" => all_values.iter().sum(),
            "min" => all_values.iter().copied().fold(f64::INFINITY, f64::min),
            "max" => all_values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
            "count" => all_values.len() as f64,
            _ => {
                return Err(GqlError::InvalidArgument {
                    message: format!("unknown aggregation function: '{agg_fn}'"),
                });
            }
        };

        Ok(vec![smallvec![
            (IStr::new("value"), GqlValue::Float(result)),
            (IStr::new("nodeCount"), GqlValue::Int(node_count)),
            (IStr::new("sampleCount"), GqlValue::Int(sample_count)),
        ]])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{LabelSet, PropertyMap, Value};
    use selene_ts::{TimeSample, TsConfig};
    use smol_str::SmolStr;

    fn setup_scoped() -> (SeleneGraph, HotTier) {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();

        // Root node (building)
        let building = m
            .create_node(
                LabelSet::from_strs(&["building"]),
                PropertyMap::from_pairs(vec![(
                    IStr::new("name"),
                    Value::String(SmolStr::new("HQ")),
                )]),
            )
            .unwrap();

        // Two sensor nodes
        let s1 = m
            .create_node(
                LabelSet::from_strs(&["sensor"]),
                PropertyMap::from_pairs(vec![(
                    IStr::new("name"),
                    Value::String(SmolStr::new("S1")),
                )]),
            )
            .unwrap();
        let s2 = m
            .create_node(
                LabelSet::from_strs(&["sensor"]),
                PropertyMap::from_pairs(vec![(
                    IStr::new("name"),
                    Value::String(SmolStr::new("S2")),
                )]),
            )
            .unwrap();

        // Containment edges: building -> sensor
        m.create_edge(building, IStr::new("contains"), s1, PropertyMap::new())
            .unwrap();
        m.create_edge(building, IStr::new("contains"), s2, PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();

        let hot = HotTier::new(TsConfig::default());
        let now = selene_core::now_nanos();

        // S1: temperatures 70, 72, 74
        for (i, val) in [70.0, 72.0, 74.0].iter().enumerate() {
            hot.append(
                s1,
                "temperature",
                TimeSample {
                    timestamp_nanos: now - (3 - i as i64) * 60_000_000_000,
                    value: *val,
                },
            );
        }
        // S2: temperatures 80, 82, 84
        for (i, val) in [80.0, 82.0, 84.0].iter().enumerate() {
            hot.append(
                s2,
                "temperature",
                TimeSample {
                    timestamp_nanos: now - (3 - i as i64) * 60_000_000_000,
                    value: *val,
                },
            );
        }

        (g, hot)
    }

    #[test]
    fn scoped_aggregate_avg() {
        let (g, hot) = setup_scoped();
        let proc = TsScopedAggregate;
        let args = vec![
            GqlValue::Int(1), // building node ID
            GqlValue::Int(2), // max hops
            GqlValue::String("temperature".into()),
            GqlValue::String("avg".into()),
            GqlValue::Int(3_600_000_000_000), // 1 hour
        ];
        let result = proc.execute(&args, &g, Some(&hot), None).unwrap();
        assert_eq!(result.len(), 1);
        let val = result[0][0].1.as_float().unwrap();
        // avg of [70,72,74,80,82,84] = 462/6 = 77.0
        assert!((val - 77.0).abs() < 0.01, "expected 77.0, got {val}");
        let node_count = result[0][1].1.as_int().unwrap();
        assert_eq!(node_count, 2);
        let sample_count = result[0][2].1.as_int().unwrap();
        assert_eq!(sample_count, 6);
    }

    #[test]
    fn scoped_aggregate_no_data() {
        let (g, hot) = setup_scoped();
        let proc = TsScopedAggregate;
        let args = vec![
            GqlValue::Int(1),
            GqlValue::Int(2),
            GqlValue::String("nonexistent".into()),
            GqlValue::String("avg".into()),
            GqlValue::Int(3_600_000_000_000),
        ];
        let result = proc.execute(&args, &g, Some(&hot), None).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn scoped_aggregate_scope_filtered() {
        let (g, hot) = setup_scoped();
        let proc = TsScopedAggregate;
        let mut scope = roaring::RoaringBitmap::new();
        scope.insert(1); // only building in scope
        let args = vec![
            GqlValue::Int(1),
            GqlValue::Int(2),
            GqlValue::String("temperature".into()),
            GqlValue::String("avg".into()),
            GqlValue::Int(3_600_000_000_000),
        ];
        let result = proc.execute(&args, &g, Some(&hot), Some(&scope)).unwrap();
        assert!(result.is_empty());
    }
}
