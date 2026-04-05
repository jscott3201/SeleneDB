//! Pattern join -- joining two pattern branches on shared variables.
//!
//! Used for comma-separated patterns in MATCH:
//!   MATCH (p)-[:workAt]->(c), (p)-[:livesIn]->(city)
//! Both branches bind 'p' -- joined on that variable.

use std::collections::HashMap;

use selene_core::IStr;

use crate::types::binding::{Binding, BoundValue};
use crate::types::chunk::{ChunkSchema, ColumnBuilder, ColumnKind, DataChunk};
use crate::types::error::GqlError;

/// Columnar hash join on shared variable columns.
///
/// Converts both DataChunks to bindings, delegates to `execute_join`, then
/// converts the result back to a DataChunk. Phase 2 will hash directly from
/// column arrays for true columnar join performance.
pub(crate) fn execute_join_chunk(
    left: &DataChunk,
    right: &DataChunk,
    join_vars: &[IStr],
) -> Result<DataChunk, GqlError> {
    let left_bindings = left.to_bindings();
    let right_bindings = right.to_bindings();
    let result = execute_join(&left_bindings, &right_bindings, join_vars)?;
    Ok(bindings_to_chunk_generic(&result))
}

/// Convert arbitrary bindings to a DataChunk by inferring column types from
/// the first binding's values.
pub(crate) fn bindings_to_chunk_generic(bindings: &[Binding]) -> DataChunk {
    if bindings.is_empty() {
        return DataChunk::from_builders(vec![], ChunkSchema::new(), 0);
    }

    let len = bindings.len();
    let first = &bindings[0];
    let mut schema = ChunkSchema::new();
    let mut builders: Vec<ColumnBuilder> = Vec::new();

    for (var, val) in first.iter() {
        let (kind, builder) = match val {
            BoundValue::Node(_) => (ColumnKind::NodeId, ColumnBuilder::new_node_ids(len)),
            BoundValue::Edge(_) => (ColumnKind::EdgeId, ColumnBuilder::new_edge_ids(len)),
            BoundValue::Scalar(gv) => match gv {
                crate::types::value::GqlValue::Int(_) => {
                    (ColumnKind::Int64, ColumnBuilder::new_int64(len))
                }
                crate::types::value::GqlValue::UInt(_) => {
                    (ColumnKind::UInt64, ColumnBuilder::new_uint64(len))
                }
                crate::types::value::GqlValue::Float(_) => {
                    (ColumnKind::Float64, ColumnBuilder::new_float64(len))
                }
                crate::types::value::GqlValue::Bool(_) => {
                    (ColumnKind::Bool, ColumnBuilder::new_bool(len))
                }
                crate::types::value::GqlValue::String(_) => {
                    (ColumnKind::Utf8, ColumnBuilder::new_utf8())
                }
                _ => (ColumnKind::Values, ColumnBuilder::new_values(len)),
            },
            BoundValue::Path(_) | BoundValue::Group(_) => {
                (ColumnKind::Values, ColumnBuilder::new_values(len))
            }
        };
        schema.extend(*var, kind);
        builders.push(builder);
    }

    for binding in bindings {
        for (slot, (var, _)) in first.iter().enumerate() {
            match binding.get(var) {
                Some(bv) => builders[slot].append_bound_value(bv),
                None => builders[slot].append_null(),
            }
        }
    }

    DataChunk::from_builders(builders, schema, len)
}

/// Join two sets of bindings on shared variables (equi-join).
///
/// For each left binding, finds right bindings where all shared variables
/// have the same value. Produces merged bindings.
pub(crate) fn execute_join(
    left: &[Binding],
    right: &[Binding],
    join_vars: &[IStr],
) -> Result<Vec<Binding>, GqlError> {
    if join_vars.is_empty() {
        // No shared variables -- cartesian product
        return execute_cartesian_product(left, right);
    }

    // Build a hash index on the right side keyed by join variable values
    let mut right_index: HashMap<smallvec::SmallVec<[u64; 4]>, Vec<usize>> = HashMap::new();
    for (idx, binding) in right.iter().enumerate() {
        let key = join_key(binding, join_vars);
        right_index.entry(key).or_default().push(idx);
    }

    // Probe with left side
    let mut output = Vec::new();
    for left_binding in left {
        let key = join_key(left_binding, join_vars);
        if let Some(right_indices) = right_index.get(&key) {
            for &ri in right_indices {
                let mut merged = left_binding.clone();
                merged.merge(&right[ri]);
                output.push(merged);
            }
        }
    }

    Ok(output)
}

/// Cartesian product -- every left binding combined with every right binding.
pub(crate) fn execute_cartesian_product(
    left: &[Binding],
    right: &[Binding],
) -> Result<Vec<Binding>, GqlError> {
    const MAX_CARTESIAN: usize = 100_000;
    let product_size =
        left.len()
            .checked_mul(right.len())
            .ok_or_else(|| GqlError::ResourcesExhausted {
                message: "cartesian product size overflow".into(),
            })?;
    if product_size > MAX_CARTESIAN {
        return Err(GqlError::ResourcesExhausted {
            message: format!(
                "cartesian product {} x {} = {} exceeds limit ({})",
                left.len(),
                right.len(),
                product_size,
                MAX_CARTESIAN
            ),
        });
    }
    let mut output = Vec::with_capacity(product_size);
    for lb in left {
        for rb in right {
            let mut merged = lb.clone();
            merged.merge(rb);
            output.push(merged);
        }
    }
    Ok(output)
}

/// Compute a hash key from a binding's values for the join variables.
/// Uses the raw u64 representation of bound values for fast hashing.
fn join_key(binding: &Binding, join_vars: &[IStr]) -> smallvec::SmallVec<[u64; 4]> {
    join_vars
        .iter()
        .map(|var| match binding.get(var) {
            Some(BoundValue::Node(id)) => id.0,
            Some(BoundValue::Edge(id)) => id.0,
            Some(BoundValue::Scalar(val)) => val.distinctness_key(),
            _ => 0, // unbound or complex -- treat as same group
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::value::GqlValue;
    use selene_core::NodeId;

    #[test]
    fn join_on_shared_node() {
        // Left: [{p: Node(1), c: Node(10)}, {p: Node(2), c: Node(20)}]
        // Right: [{p: Node(1), city: Node(100)}, {p: Node(3), city: Node(300)}]
        // Join on p: only p=Node(1) matches
        let left = vec![
            {
                let mut b = Binding::empty();
                b.bind(IStr::new("p"), BoundValue::Node(NodeId(1)));
                b.bind(IStr::new("c"), BoundValue::Node(NodeId(10)));
                b
            },
            {
                let mut b = Binding::empty();
                b.bind(IStr::new("p"), BoundValue::Node(NodeId(2)));
                b.bind(IStr::new("c"), BoundValue::Node(NodeId(20)));
                b
            },
        ];
        let right = vec![
            {
                let mut b = Binding::empty();
                b.bind(IStr::new("p"), BoundValue::Node(NodeId(1)));
                b.bind(IStr::new("city"), BoundValue::Node(NodeId(100)));
                b
            },
            {
                let mut b = Binding::empty();
                b.bind(IStr::new("p"), BoundValue::Node(NodeId(3)));
                b.bind(IStr::new("city"), BoundValue::Node(NodeId(300)));
                b
            },
        ];

        let result = execute_join(&left, &right, &[IStr::new("p")]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get_node_id(&IStr::new("p")).unwrap(), NodeId(1));
        assert_eq!(result[0].get_node_id(&IStr::new("c")).unwrap(), NodeId(10));
        assert_eq!(
            result[0].get_node_id(&IStr::new("city")).unwrap(),
            NodeId(100)
        );
    }

    #[test]
    fn join_multiple_matches() {
        // Left: [{p: Node(1)}, {p: Node(1)}]
        // Right: [{p: Node(1), x: Node(10)}]
        // Both lefts match the right → 2 results
        let left = vec![
            Binding::single(IStr::new("p"), BoundValue::Node(NodeId(1))),
            Binding::single(IStr::new("p"), BoundValue::Node(NodeId(1))),
        ];
        let right = vec![{
            let mut b = Binding::empty();
            b.bind(IStr::new("p"), BoundValue::Node(NodeId(1)));
            b.bind(IStr::new("x"), BoundValue::Node(NodeId(10)));
            b
        }];

        let result = execute_join(&left, &right, &[IStr::new("p")]).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn join_no_matches() {
        let left = vec![Binding::single(IStr::new("p"), BoundValue::Node(NodeId(1)))];
        let right = vec![Binding::single(IStr::new("p"), BoundValue::Node(NodeId(2)))];

        let result = execute_join(&left, &right, &[IStr::new("p")]).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn cartesian_product() {
        let left = vec![
            Binding::single(IStr::new("a"), BoundValue::Node(NodeId(1))),
            Binding::single(IStr::new("a"), BoundValue::Node(NodeId(2))),
        ];
        let right = vec![
            Binding::single(IStr::new("b"), BoundValue::Node(NodeId(10))),
            Binding::single(IStr::new("b"), BoundValue::Node(NodeId(20))),
        ];

        let result = execute_cartesian_product(&left, &right).unwrap();
        assert_eq!(result.len(), 4); // 2 × 2
    }

    #[test]
    fn join_empty_vars_is_cartesian() {
        let left = vec![Binding::single(IStr::new("a"), BoundValue::Node(NodeId(1)))];
        let right = vec![Binding::single(
            IStr::new("b"),
            BoundValue::Node(NodeId(10)),
        )];

        let result = execute_join(&left, &right, &[]).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].contains(&IStr::new("a")));
        assert!(result[0].contains(&IStr::new("b")));
    }

    #[test]
    fn join_on_scalar() {
        let left = vec![{
            let mut b = Binding::empty();
            b.bind(IStr::new("x"), BoundValue::Scalar(GqlValue::Int(42)));
            b.bind(IStr::new("a"), BoundValue::Node(NodeId(1)));
            b
        }];
        let right = vec![{
            let mut b = Binding::empty();
            b.bind(IStr::new("x"), BoundValue::Scalar(GqlValue::Int(42)));
            b.bind(IStr::new("b"), BoundValue::Node(NodeId(2)));
            b
        }];

        let result = execute_join(&left, &right, &[IStr::new("x")]).unwrap();
        assert_eq!(result.len(), 1);
    }
}
