//! Typed property index -- BTreeMap variants per value type for correct sort order.
//!
//! String properties use lexicographic order (BTreeMap<SmolStr, _>).
//! Numeric properties use native ordering (BTreeMap<i64, _>, BTreeMap<u64, _>,
//! `BTreeMap<OrderedFloat<f64>, _>`). This ensures `ORDER BY n.temperature`
//! sorts numerically, not lexicographically.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Bound;

use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;
use selene_core::IStr;
use selene_core::entity::NodeId;
use selene_core::schema::ValueType;
use selene_core::value::Value;
use smol_str::SmolStr;

/// Range operation for `can_satisfy` checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeOp {
    Gt,
    Lt,
    Gte,
    Lte,
    Eq,
}

/// Selectivity estimation operation.
#[derive(Debug, Clone)]
pub enum SelectivityOp<'a> {
    Eq(&'a Value),
    Range {
        lower: Option<&'a Value>,
        upper: Option<&'a Value>,
    },
    In(usize),
    IsNotNull,
}

/// A type-aware property index backed by BTreeMap.
///
/// Each variant stores values in their native sort order.
/// Created from the schema's `ValueType` at index build time.
#[derive(Clone)]
pub enum TypedIndex {
    /// String/VARCHAR properties -- lexicographic order.
    String(BTreeMap<SmolStr, Vec<NodeId>>),
    /// Signed integer properties -- numeric order.
    Int(BTreeMap<i64, Vec<NodeId>>),
    /// Unsigned integer properties -- numeric order.
    UInt(BTreeMap<u64, Vec<NodeId>>),
    /// Float/Double properties -- numeric order (NaN handled via OrderedFloat).
    Float(BTreeMap<OrderedFloat<f64>, Vec<NodeId>>),
}

impl TypedIndex {
    /// Create an empty index for the given value type.
    pub fn new_for_type(vt: &ValueType) -> Self {
        match vt {
            ValueType::Int => TypedIndex::Int(BTreeMap::new()),
            ValueType::UInt => TypedIndex::UInt(BTreeMap::new()),
            ValueType::Float => TypedIndex::Float(BTreeMap::new()),
            // All other types (String, Bool, Date, etc.) use string representation
            _ => TypedIndex::String(BTreeMap::new()),
        }
    }

    /// Insert a node ID under the given value.
    pub fn insert(&mut self, value: &Value, node_id: NodeId) {
        match self {
            TypedIndex::Int(map) => {
                if let Some(i) = value_to_i64(value) {
                    map.entry(i).or_default().push(node_id);
                }
            }
            TypedIndex::UInt(map) => {
                if let Some(u) = value_to_u64(value) {
                    map.entry(u).or_default().push(node_id);
                }
            }
            TypedIndex::Float(map) => {
                if let Some(f) = value_to_f64(value) {
                    map.entry(OrderedFloat(f)).or_default().push(node_id);
                }
            }
            TypedIndex::String(map) => {
                let key = value_to_smolstr(value);
                map.entry(key).or_default().push(node_id);
            }
        }
    }

    /// Remove a node ID from the given value's entry.
    pub fn remove(&mut self, value: &Value, node_id: NodeId) {
        match self {
            TypedIndex::Int(map) => {
                if let Some(i) = value_to_i64(value)
                    && let Some(ids) = map.get_mut(&i)
                {
                    ids.retain(|id| *id != node_id);
                    if ids.is_empty() {
                        map.remove(&i);
                    }
                }
            }
            TypedIndex::UInt(map) => {
                if let Some(u) = value_to_u64(value)
                    && let Some(ids) = map.get_mut(&u)
                {
                    ids.retain(|id| *id != node_id);
                    if ids.is_empty() {
                        map.remove(&u);
                    }
                }
            }
            TypedIndex::Float(map) => {
                if let Some(f) = value_to_f64(value) {
                    let key = OrderedFloat(f);
                    if let Some(ids) = map.get_mut(&key) {
                        ids.retain(|id| *id != node_id);
                        if ids.is_empty() {
                            map.remove(&key);
                        }
                    }
                }
            }
            TypedIndex::String(map) => {
                let key = value_to_smolstr(value);
                if let Some(ids) = map.get_mut(&key) {
                    ids.retain(|id: &NodeId| *id != node_id);
                    if ids.is_empty() {
                        map.remove(&key);
                    }
                }
            }
        }
    }

    /// Look up node IDs by exact value match.
    pub fn lookup(&self, value: &Value) -> Option<&Vec<NodeId>> {
        match self {
            TypedIndex::Int(map) => value_to_i64(value).and_then(|i| map.get(&i)),
            TypedIndex::UInt(map) => value_to_u64(value).and_then(|u| map.get(&u)),
            TypedIndex::Float(map) => value_to_f64(value).and_then(|f| map.get(&OrderedFloat(f))),
            TypedIndex::String(map) => {
                let key = value_to_smolstr(value);
                map.get(&key)
            }
        }
    }

    /// Iterate all (value, node_ids) pairs in ascending order.
    /// Calls the closure with each node ID in sorted value order.
    /// Returns early if the closure returns `false` (limit reached).
    pub fn iter_asc(&self, mut f: impl FnMut(NodeId) -> bool) {
        match self {
            TypedIndex::Int(map) => iter_node_ids(map.values(), &mut f),
            TypedIndex::UInt(map) => iter_node_ids(map.values(), &mut f),
            TypedIndex::Float(map) => iter_node_ids(map.values(), &mut f),
            TypedIndex::String(map) => iter_node_ids(map.values(), &mut f),
        }
    }

    /// Iterate all (value, node_ids) pairs in descending order.
    /// Calls the closure with each node ID in reverse sorted value order.
    /// Returns early if the closure returns `false` (limit reached).
    pub fn iter_desc(&self, mut f: impl FnMut(NodeId) -> bool) {
        match self {
            TypedIndex::Int(map) => iter_node_ids(map.values().rev(), &mut f),
            TypedIndex::UInt(map) => iter_node_ids(map.values().rev(), &mut f),
            TypedIndex::Float(map) => iter_node_ids(map.values().rev(), &mut f),
            TypedIndex::String(map) => iter_node_ids(map.values().rev(), &mut f),
        }
    }

    /// Check whether the index contains any entries.
    pub fn is_empty(&self) -> bool {
        match self {
            TypedIndex::Int(m) => m.is_empty(),
            TypedIndex::UInt(m) => m.is_empty(),
            TypedIndex::Float(m) => m.is_empty(),
            TypedIndex::String(m) => m.is_empty(),
        }
    }

    /// Collect NodeIds matching a range predicate into a RoaringBitmap.
    ///
    /// Uses BTreeMap `range()` for O(result_set) instead of O(N) full iteration.
    /// `lower` is an optional (value, inclusive) lower bound; `upper` is the same
    /// for the upper bound. Both `None` means unbounded in that direction.
    ///
    /// Returns an empty bitmap if the bound values cannot be converted to the
    /// index's key type (e.g., a String bound on an Int index).
    pub fn range_to_bitmap(
        &self,
        lower: Option<(&Value, bool)>,
        upper: Option<(&Value, bool)>,
    ) -> RoaringBitmap {
        match self {
            TypedIndex::Int(map) => {
                let lo = match lower {
                    Some((val, inclusive)) => match value_to_i64(val) {
                        Some(v) if inclusive => Bound::Included(v),
                        Some(v) => Bound::Excluded(v),
                        None => return RoaringBitmap::new(),
                    },
                    None => Bound::Unbounded,
                };
                let hi = match upper {
                    Some((val, inclusive)) => match value_to_i64(val) {
                        Some(v) if inclusive => Bound::Included(v),
                        Some(v) => Bound::Excluded(v),
                        None => return RoaringBitmap::new(),
                    },
                    None => Bound::Unbounded,
                };
                collect_range_bitmap(map.range((lo, hi)))
            }
            TypedIndex::UInt(map) => {
                let lo = match lower {
                    Some((val, inclusive)) => match value_to_u64(val) {
                        Some(v) if inclusive => Bound::Included(v),
                        Some(v) => Bound::Excluded(v),
                        None => return RoaringBitmap::new(),
                    },
                    None => Bound::Unbounded,
                };
                let hi = match upper {
                    Some((val, inclusive)) => match value_to_u64(val) {
                        Some(v) if inclusive => Bound::Included(v),
                        Some(v) => Bound::Excluded(v),
                        None => return RoaringBitmap::new(),
                    },
                    None => Bound::Unbounded,
                };
                collect_range_bitmap(map.range((lo, hi)))
            }
            TypedIndex::Float(map) => {
                let lo = match lower {
                    Some((val, inclusive)) => match value_to_f64(val) {
                        Some(v) if inclusive => Bound::Included(OrderedFloat(v)),
                        Some(v) => Bound::Excluded(OrderedFloat(v)),
                        None => return RoaringBitmap::new(),
                    },
                    None => Bound::Unbounded,
                };
                let hi = match upper {
                    Some((val, inclusive)) => match value_to_f64(val) {
                        Some(v) if inclusive => Bound::Included(OrderedFloat(v)),
                        Some(v) => Bound::Excluded(OrderedFloat(v)),
                        None => return RoaringBitmap::new(),
                    },
                    None => Bound::Unbounded,
                };
                collect_range_bitmap(map.range((lo, hi)))
            }
            TypedIndex::String(map) => {
                let lo_str;
                let lo = match lower {
                    Some((val, inclusive)) => {
                        lo_str = value_to_smolstr(val);
                        if inclusive {
                            Bound::Included(&lo_str)
                        } else {
                            Bound::Excluded(&lo_str)
                        }
                    }
                    None => Bound::Unbounded,
                };
                let hi_str;
                let hi = match upper {
                    Some((val, inclusive)) => {
                        hi_str = value_to_smolstr(val);
                        if inclusive {
                            Bound::Included(&hi_str)
                        } else {
                            Bound::Excluded(&hi_str)
                        }
                    }
                    None => Bound::Unbounded,
                };
                collect_range_bitmap(map.range::<SmolStr, _>((lo, hi)))
            }
        }
    }

    // ── Statistics methods ──────────────────────────────────────

    /// Return the smallest indexed value, or `None` if the index is empty.
    pub fn min_value(&self) -> Option<Value> {
        match self {
            TypedIndex::Int(map) => map.keys().next().map(|k| Value::Int(*k)),
            TypedIndex::UInt(map) => map.keys().next().map(|k| Value::UInt(*k)),
            TypedIndex::Float(map) => map.keys().next().map(|k| Value::Float(k.0)),
            TypedIndex::String(map) => map.keys().next().map(|k| Value::String(k.clone())),
        }
    }

    /// Return the largest indexed value, or `None` if the index is empty.
    pub fn max_value(&self) -> Option<Value> {
        match self {
            TypedIndex::Int(map) => map.keys().next_back().map(|k| Value::Int(*k)),
            TypedIndex::UInt(map) => map.keys().next_back().map(|k| Value::UInt(*k)),
            TypedIndex::Float(map) => map.keys().next_back().map(|k| Value::Float(k.0)),
            TypedIndex::String(map) => map.keys().next_back().map(|k| Value::String(k.clone())),
        }
    }

    /// Number of distinct indexed values (BTreeMap key count).
    pub fn distinct_count(&self) -> usize {
        match self {
            TypedIndex::Int(map) => map.len(),
            TypedIndex::UInt(map) => map.len(),
            TypedIndex::Float(map) => map.len(),
            TypedIndex::String(map) => map.len(),
        }
    }

    /// Total number of (value, node) pairs across all buckets.
    pub fn total_count(&self) -> usize {
        match self {
            TypedIndex::Int(map) => map.values().map(|v| v.len()).sum(),
            TypedIndex::UInt(map) => map.values().map(|v| v.len()).sum(),
            TypedIndex::Float(map) => map.values().map(|v| v.len()).sum(),
            TypedIndex::String(map) => map.values().map(|v| v.len()).sum(),
        }
    }

    /// Zone-map check: can the range [min, max] of this index possibly satisfy
    /// the given predicate?  Returns `false` when the predicate is provably
    /// unsatisfiable (the entire index falls outside the query value), `true`
    /// otherwise (including when the index is empty -- caller must handle that).
    pub fn can_satisfy(&self, op: RangeOp, query: &Value) -> bool {
        let Some(min) = self.min_value() else {
            return false;
        };
        let Some(max) = self.max_value() else {
            return false;
        };

        match op {
            // Can any indexed value satisfy `indexed_val > query`?
            // Only impossible when query >= max (i.e., max <= query).
            RangeOp::Gt => {
                matches!(Self::compare_values(query, &max), Some(Ordering::Less))
            }
            // Can any indexed value satisfy `indexed_val < query`?
            // Only impossible when query <= min (i.e., min >= query).
            RangeOp::Lt => {
                matches!(Self::compare_values(query, &min), Some(Ordering::Greater))
            }
            // Can any indexed value satisfy `indexed_val >= query`?
            // Only impossible when query > max.
            RangeOp::Gte => {
                matches!(
                    Self::compare_values(query, &max),
                    Some(Ordering::Less | Ordering::Equal)
                )
            }
            // Can any indexed value satisfy `indexed_val <= query`?
            // Only impossible when query < min.
            RangeOp::Lte => {
                matches!(
                    Self::compare_values(query, &min),
                    Some(Ordering::Greater | Ordering::Equal)
                )
            }
            // Eq: query must be within [min, max]
            RangeOp::Eq => {
                let above_min = matches!(
                    Self::compare_values(query, &min),
                    Some(Ordering::Greater | Ordering::Equal)
                );
                let below_max = matches!(
                    Self::compare_values(query, &max),
                    Some(Ordering::Less | Ordering::Equal)
                );
                above_min && below_max
            }
        }
    }

    /// Estimate the fraction of indexed nodes that match the given operation.
    ///
    /// Returns a value in `[0.0, 1.0]`. Returns `0.0` for an empty index.
    pub fn selectivity(&self, op: &SelectivityOp<'_>) -> f64 {
        let distinct = self.distinct_count();
        if distinct == 0 {
            return 0.0;
        }

        match op {
            SelectivityOp::IsNotNull => 1.0,

            SelectivityOp::Eq(val) => {
                // Check whether this value is actually in the index for a
                // tighter bound; fall back to uniform 1/distinct.
                if self.lookup(val).is_some() {
                    // Exact hit: use actual bucket size / total.
                    let bucket = self.lookup(val).map_or(0, |v| v.len());
                    let total = self.total_count();
                    if total == 0 {
                        0.0
                    } else {
                        bucket as f64 / total as f64
                    }
                } else {
                    (1.0_f64 / distinct as f64).min(1.0)
                }
            }

            SelectivityOp::In(n) => ((*n as f64) / distinct as f64).min(1.0),

            SelectivityOp::Range { lower, upper } => {
                let Some(min_val) = self.min_value() else {
                    return 0.0;
                };
                let Some(max_val) = self.max_value() else {
                    return 0.0;
                };

                // For string indexes fall back to 1/3 heuristic per open-ended range.
                let index_span = Self::numeric_range(&min_val, &max_val);
                if index_span <= 0.0 {
                    // Single-value index.
                    return if let (None, None) = (lower, upper) {
                        1.0
                    } else {
                        // Just check if the single value satisfies the range.
                        let lo_ok = lower.is_none_or(|lo| {
                            matches!(
                                Self::compare_values(&min_val, lo),
                                Some(Ordering::Greater | Ordering::Equal)
                            )
                        });
                        let hi_ok = upper.is_none_or(|hi| {
                            matches!(
                                Self::compare_values(&min_val, hi),
                                Some(Ordering::Less | Ordering::Equal)
                            )
                        });
                        if lo_ok && hi_ok { 1.0 } else { 0.0 }
                    };
                }

                let lo_f = lower
                    .and_then(Self::to_f64)
                    .map(|v| v.max(Self::to_f64(&min_val).unwrap_or(v)));
                let hi_f = upper
                    .and_then(Self::to_f64)
                    .map(|v| v.min(Self::to_f64(&max_val).unwrap_or(v)));

                match (lo_f, hi_f) {
                    (Some(lo), Some(hi)) => {
                        if hi <= lo {
                            0.0
                        } else {
                            ((hi - lo) / index_span).clamp(0.0, 1.0)
                        }
                    }
                    (Some(lo), None) => {
                        ((Self::to_f64(&max_val).unwrap_or(lo) - lo) / index_span).clamp(0.0, 1.0)
                    }
                    (None, Some(hi)) => {
                        let min_f = Self::to_f64(&min_val).unwrap_or(hi);
                        ((hi - min_f) / index_span).clamp(0.0, 1.0)
                    }
                    (None, None) => 1.0,
                }
            }
        }
    }

    // ── Private helpers ─────────────────────────────────────────

    /// Cross-type numeric comparison for Value pairs.
    ///
    /// Promotes Int/UInt pairs via i128 to avoid sign errors.
    /// Int/Float and UInt/Float use f64 promotion.
    /// Returns `None` for incomparable types (e.g., String vs Int).
    fn compare_values(a: &Value, b: &Value) -> Option<Ordering> {
        match (a, b) {
            (Value::Int(x), Value::Int(y)) => Some(x.cmp(y)),
            (Value::UInt(x), Value::UInt(y)) => Some(x.cmp(y)),
            (Value::Float(x), Value::Float(y)) => x.partial_cmp(y),

            // Cross Int/UInt via i128 to handle sign correctly.
            (Value::Int(x), Value::UInt(y)) => {
                let xi = i128::from(*x);
                let yi = i128::from(*y);
                Some(xi.cmp(&yi))
            }
            (Value::UInt(x), Value::Int(y)) => {
                let xi = i128::from(*x);
                let yi = i128::from(*y);
                Some(xi.cmp(&yi))
            }

            // Cross Int/Float.
            (Value::Int(x), Value::Float(y)) => (*x as f64).partial_cmp(y),
            (Value::Float(x), Value::Int(y)) => x.partial_cmp(&(*y as f64)),

            // Cross UInt/Float.
            (Value::UInt(x), Value::Float(y)) => (*x as f64).partial_cmp(y),
            (Value::Float(x), Value::UInt(y)) => x.partial_cmp(&(*y as f64)),

            // String vs String (SmolStr / InternedStr).
            (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
            (Value::String(x), Value::InternedStr(y)) => Some(x.as_str().cmp(y.as_str())),
            (Value::InternedStr(x), Value::String(y)) => Some(x.as_str().cmp(y.as_str())),
            (Value::InternedStr(x), Value::InternedStr(y)) => Some(x.as_str().cmp(y.as_str())),

            _ => None,
        }
    }

    /// Convert a Value to f64 for range interpolation.
    fn to_f64(value: &Value) -> Option<f64> {
        match value {
            Value::Int(i) => Some(*i as f64),
            Value::UInt(u) => Some(*u as f64),
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Compute max - min as f64 for range span calculation.
    fn numeric_range(min: &Value, max: &Value) -> f64 {
        match (Self::to_f64(min), Self::to_f64(max)) {
            (Some(lo), Some(hi)) => (hi - lo).max(0.0),
            _ => 0.0,
        }
    }
}

// ── Composite index ─────────────────────────────────────────────

/// Composite index for multi-property compound lookups.
///
/// Keys are tuples of stringified property values joined with a null separator.
/// This allows compound queries like `WHERE type = "temp" AND floor = 3`
/// to be answered in a single index lookup.
#[derive(Clone)]
pub struct CompositeTypedIndex {
    /// Properties that form the composite key, in order.
    properties: Vec<IStr>,
    /// Map from composite key string to node IDs.
    entries: BTreeMap<SmolStr, Vec<NodeId>>,
}

impl CompositeTypedIndex {
    /// Create an empty composite index for the given property names.
    pub fn new(properties: Vec<IStr>) -> Self {
        Self {
            properties,
            entries: BTreeMap::new(),
        }
    }

    /// Build the composite key from property values.
    ///
    /// Each value is prefixed with a single-char type discriminant so that
    /// values of different types with identical string representations (e.g.,
    /// `Int(3)` vs `Float(3.0)` which both display as `"3"`) produce distinct
    /// keys. Values are separated by null bytes.
    fn make_key(values: &[&Value]) -> SmolStr {
        let mut key = String::new();
        for (i, v) in values.iter().enumerate() {
            if i > 0 {
                key.push('\0');
            }
            let tag = match v {
                Value::Null => "n",
                Value::Bool(_) => "b",
                Value::Int(_) => "i",
                Value::UInt(_) => "u",
                Value::Float(_) => "f",
                Value::String(_) | Value::InternedStr(_) => "s",
                Value::Timestamp(_) => "t",
                Value::Bytes(_) => "x",
                Value::List(_) => "l",
                Value::Date(_) => "d",
                Value::LocalDateTime(_) => "D",
                Value::Duration(_) => "r",
                Value::Vector(_) => "v",
                Value::Geometry(_) => "g",
            };
            key.push_str(tag);
            key.push(':');
            key.push_str(&v.to_string());
        }
        SmolStr::new(key)
    }

    /// Insert a node with the given property values.
    ///
    /// The `values` slice must be in the same order as the `properties`
    /// passed to `new`.
    pub fn insert(&mut self, values: &[&Value], node_id: NodeId) {
        let key = Self::make_key(values);
        self.entries.entry(key).or_default().push(node_id);
    }

    /// Remove a node from the given composite key.
    pub fn remove(&mut self, values: &[&Value], node_id: NodeId) {
        let key = Self::make_key(values);
        if let Some(ids) = self.entries.get_mut(&key) {
            ids.retain(|id: &NodeId| *id != node_id);
            if ids.is_empty() {
                self.entries.remove(&key);
            }
        }
    }

    /// Look up node IDs by exact composite match.
    pub fn lookup(&self, values: &[&Value]) -> Option<&Vec<NodeId>> {
        let key = Self::make_key(values);
        self.entries.get(&key)
    }

    /// The property names that form this composite key.
    pub fn properties(&self) -> &[IStr] {
        &self.properties
    }

    /// Check whether the index contains any entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// ── Iteration helper ────────────────────────────────────────────

/// Iterate node IDs from an iterator of `Vec<NodeId>` buckets.
/// Calls `f` for each node ID; returns early if `f` returns `false`.
fn iter_node_ids<'a>(
    values: impl Iterator<Item = &'a Vec<NodeId>>,
    f: &mut impl FnMut(NodeId) -> bool,
) {
    for ids in values {
        for &nid in ids {
            if !f(nid) {
                return;
            }
        }
    }
}

/// Collect all NodeIds from a BTreeMap range iterator into a RoaringBitmap.
fn collect_range_bitmap<'a, K: 'a>(
    range: impl Iterator<Item = (&'a K, &'a Vec<NodeId>)>,
) -> RoaringBitmap {
    let mut bitmap = RoaringBitmap::new();
    for (_, ids) in range {
        for &nid in ids {
            bitmap.insert(nid.0 as u32);
        }
    }
    bitmap
}

// ── Value conversion helpers ─────────────────────────────────────

fn value_to_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Int(i) => Some(*i),
        Value::UInt(u) => i64::try_from(*u).ok(),
        Value::Float(f) => Some(*f as i64),
        _ => None,
    }
}

fn value_to_u64(value: &Value) -> Option<u64> {
    match value {
        Value::UInt(u) => Some(*u),
        Value::Int(i) if *i >= 0 => Some(*i as u64),
        _ => None,
    }
}

fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Float(f) => Some(*f),
        Value::Int(i) => Some(*i as f64),
        Value::UInt(u) => Some(*u as f64),
        _ => None,
    }
}

fn value_to_smolstr(value: &Value) -> SmolStr {
    match value {
        Value::String(s) => s.clone(),
        Value::InternedStr(s) => SmolStr::new(s.as_str()),
        other => SmolStr::new(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_index_sorts_numerically() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Int);
        idx.insert(&Value::Int(100), NodeId(1));
        idx.insert(&Value::Int(2), NodeId(2));
        idx.insert(&Value::Int(50), NodeId(3));
        idx.insert(&Value::Int(10), NodeId(4));

        let mut order = Vec::new();
        idx.iter_asc(|nid| {
            order.push(nid.0);
            true
        });
        assert_eq!(order, vec![2, 4, 3, 1]); // 2, 10, 50, 100
    }

    #[test]
    fn int_index_descending() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Int);
        idx.insert(&Value::Int(100), NodeId(1));
        idx.insert(&Value::Int(2), NodeId(2));
        idx.insert(&Value::Int(50), NodeId(3));

        let mut order = Vec::new();
        idx.iter_desc(|nid| {
            order.push(nid.0);
            true
        });
        assert_eq!(order, vec![1, 3, 2]); // 100, 50, 2
    }

    #[test]
    fn float_index_sorts_numerically() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Float);
        idx.insert(&Value::Float(72.5), NodeId(1));
        idx.insert(&Value::Float(9.0), NodeId(2));
        idx.insert(&Value::Float(100.0), NodeId(3));

        let mut order = Vec::new();
        idx.iter_asc(|nid| {
            order.push(nid.0);
            true
        });
        assert_eq!(order, vec![2, 1, 3]); // 9.0, 72.5, 100.0
    }

    #[test]
    fn string_index_sorts_lexicographically() {
        let mut idx = TypedIndex::new_for_type(&ValueType::String);
        idx.insert(&Value::String(SmolStr::new("banana")), NodeId(1));
        idx.insert(&Value::String(SmolStr::new("apple")), NodeId(2));
        idx.insert(&Value::String(SmolStr::new("cherry")), NodeId(3));

        let mut order = Vec::new();
        idx.iter_asc(|nid| {
            order.push(nid.0);
            true
        });
        assert_eq!(order, vec![2, 1, 3]); // apple, banana, cherry
    }

    #[test]
    fn insert_remove_lookup() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Int);
        idx.insert(&Value::Int(42), NodeId(1));
        idx.insert(&Value::Int(42), NodeId(2));

        assert_eq!(idx.lookup(&Value::Int(42)).map(|v| v.len()), Some(2));

        idx.remove(&Value::Int(42), NodeId(1));
        assert_eq!(idx.lookup(&Value::Int(42)).map(|v| v.len()), Some(1));

        idx.remove(&Value::Int(42), NodeId(2));
        assert!(idx.lookup(&Value::Int(42)).is_none());
    }

    #[test]
    fn iter_with_limit() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Int);
        for i in 0..100 {
            idx.insert(&Value::Int(i), NodeId(i as u64));
        }

        let mut collected = Vec::new();
        idx.iter_asc(|nid| {
            collected.push(nid.0);
            collected.len() < 5
        });
        assert_eq!(collected.len(), 5);
        assert_eq!(collected, vec![0, 1, 2, 3, 4]);
    }

    // ── Composite index tests ───────────────────────────────────

    #[test]
    fn composite_index_lookup() {
        let mut idx = CompositeTypedIndex::new(vec![IStr::new("type"), IStr::new("floor")]);

        idx.insert(
            &[&Value::String(SmolStr::new("temp")), &Value::Int(3)],
            NodeId(1),
        );
        idx.insert(
            &[&Value::String(SmolStr::new("temp")), &Value::Int(5)],
            NodeId(2),
        );
        idx.insert(
            &[&Value::String(SmolStr::new("humidity")), &Value::Int(3)],
            NodeId(3),
        );

        // Exact compound match
        let result = idx.lookup(&[&Value::String(SmolStr::new("temp")), &Value::Int(3)]);
        assert_eq!(result.map(|v| v.as_slice()), Some(&[NodeId(1)][..]));

        // Different floor
        let result = idx.lookup(&[&Value::String(SmolStr::new("temp")), &Value::Int(5)]);
        assert_eq!(result.map(|v| v.as_slice()), Some(&[NodeId(2)][..]));

        // Different type, same floor
        let result = idx.lookup(&[&Value::String(SmolStr::new("humidity")), &Value::Int(3)]);
        assert_eq!(result.map(|v| v.as_slice()), Some(&[NodeId(3)][..]));

        // No match
        let result = idx.lookup(&[&Value::String(SmolStr::new("temp")), &Value::Int(7)]);
        assert!(result.is_none());
    }

    #[test]
    fn composite_index_remove() {
        let mut idx = CompositeTypedIndex::new(vec![IStr::new("a"), IStr::new("b")]);

        idx.insert(&[&Value::Int(1), &Value::Int(2)], NodeId(10));
        idx.insert(&[&Value::Int(1), &Value::Int(2)], NodeId(20));

        assert_eq!(
            idx.lookup(&[&Value::Int(1), &Value::Int(2)])
                .map(|v| v.len()),
            Some(2),
        );

        idx.remove(&[&Value::Int(1), &Value::Int(2)], NodeId(10));
        assert_eq!(
            idx.lookup(&[&Value::Int(1), &Value::Int(2)])
                .map(|v| v.len()),
            Some(1),
        );

        idx.remove(&[&Value::Int(1), &Value::Int(2)], NodeId(20));
        assert!(idx.lookup(&[&Value::Int(1), &Value::Int(2)]).is_none());
        assert!(idx.is_empty());
    }

    #[test]
    fn composite_index_properties_accessor() {
        let props = vec![IStr::new("x"), IStr::new("y"), IStr::new("z")];
        let idx = CompositeTypedIndex::new(props.clone());
        assert_eq!(idx.properties(), &props[..]);
        assert!(idx.is_empty());
    }

    // ── Range bitmap tests ──────────────────────────────────────

    #[test]
    fn range_to_bitmap_int_lower_exclusive() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Int);
        idx.insert(&Value::Int(10), NodeId(1));
        idx.insert(&Value::Int(20), NodeId(2));
        idx.insert(&Value::Int(30), NodeId(3));
        idx.insert(&Value::Int(40), NodeId(4));

        // > 20 should return nodes 3, 4 (values 30, 40)
        let bitmap = idx.range_to_bitmap(Some((&Value::Int(20), false)), None);
        assert!(!bitmap.contains(1));
        assert!(!bitmap.contains(2));
        assert!(bitmap.contains(3));
        assert!(bitmap.contains(4));
    }

    #[test]
    fn range_to_bitmap_int_lower_inclusive() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Int);
        idx.insert(&Value::Int(10), NodeId(1));
        idx.insert(&Value::Int(20), NodeId(2));
        idx.insert(&Value::Int(30), NodeId(3));

        // >= 20 should return nodes 2, 3
        let bitmap = idx.range_to_bitmap(Some((&Value::Int(20), true)), None);
        assert!(!bitmap.contains(1));
        assert!(bitmap.contains(2));
        assert!(bitmap.contains(3));
    }

    #[test]
    fn range_to_bitmap_int_upper_exclusive() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Int);
        idx.insert(&Value::Int(10), NodeId(1));
        idx.insert(&Value::Int(20), NodeId(2));
        idx.insert(&Value::Int(30), NodeId(3));

        // < 20 should return node 1 (value 10)
        let bitmap = idx.range_to_bitmap(None, Some((&Value::Int(20), false)));
        assert!(bitmap.contains(1));
        assert!(!bitmap.contains(2));
        assert!(!bitmap.contains(3));
    }

    #[test]
    fn range_to_bitmap_int_both_bounds() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Int);
        for i in 0..10 {
            idx.insert(&Value::Int(i * 10), NodeId(i as u64));
        }

        // >= 30 AND < 70 should return nodes 3, 4, 5, 6 (values 30, 40, 50, 60)
        let bitmap = idx.range_to_bitmap(
            Some((&Value::Int(30), true)),
            Some((&Value::Int(70), false)),
        );
        assert_eq!(bitmap.len(), 4);
        assert!(bitmap.contains(3));
        assert!(bitmap.contains(4));
        assert!(bitmap.contains(5));
        assert!(bitmap.contains(6));
    }

    #[test]
    fn range_to_bitmap_float() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Float);
        idx.insert(&Value::Float(0.1), NodeId(1));
        idx.insert(&Value::Float(0.5), NodeId(2));
        idx.insert(&Value::Float(0.9), NodeId(3));

        // > 0.3 should return nodes 2, 3
        let bitmap = idx.range_to_bitmap(Some((&Value::Float(0.3), false)), None);
        assert!(!bitmap.contains(1));
        assert!(bitmap.contains(2));
        assert!(bitmap.contains(3));
    }

    #[test]
    fn range_to_bitmap_empty_range() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Int);
        idx.insert(&Value::Int(10), NodeId(1));
        idx.insert(&Value::Int(20), NodeId(2));

        // > 100 should return empty
        let bitmap = idx.range_to_bitmap(Some((&Value::Int(100), false)), None);
        assert!(bitmap.is_empty());
    }

    #[test]
    fn range_to_bitmap_unbounded() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Int);
        idx.insert(&Value::Int(10), NodeId(1));
        idx.insert(&Value::Int(20), NodeId(2));

        // No bounds -- all nodes
        let bitmap = idx.range_to_bitmap(None, None);
        assert_eq!(bitmap.len(), 2);
    }

    #[test]
    fn range_to_bitmap_multiple_nodes_per_value() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Int);
        idx.insert(&Value::Int(50), NodeId(1));
        idx.insert(&Value::Int(50), NodeId(2));
        idx.insert(&Value::Int(50), NodeId(3));
        idx.insert(&Value::Int(100), NodeId(4));

        // >= 50 should return all 4 nodes
        let bitmap = idx.range_to_bitmap(Some((&Value::Int(50), true)), None);
        assert_eq!(bitmap.len(), 4);
    }

    #[test]
    fn range_to_bitmap_string_lexicographic() {
        let mut idx = TypedIndex::new_for_type(&ValueType::String);
        idx.insert(&Value::String(SmolStr::new("alpha")), NodeId(1));
        idx.insert(&Value::String(SmolStr::new("beta")), NodeId(2));
        idx.insert(&Value::String(SmolStr::new("gamma")), NodeId(3));

        // >= "beta" should return nodes 2, 3
        let bitmap = idx.range_to_bitmap(Some((&Value::String(SmolStr::new("beta")), true)), None);
        assert!(!bitmap.contains(1));
        assert!(bitmap.contains(2));
        assert!(bitmap.contains(3));
    }

    // ── Statistics tests ────────────────────────────────────────

    #[test]
    fn min_max_values() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Int);
        idx.insert(&Value::Int(30), NodeId(1));
        idx.insert(&Value::Int(10), NodeId(2));
        idx.insert(&Value::Int(20), NodeId(3));

        assert_eq!(idx.min_value(), Some(Value::Int(10)));
        assert_eq!(idx.max_value(), Some(Value::Int(30)));
    }

    #[test]
    fn distinct_and_total_count() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Int);
        idx.insert(&Value::Int(1), NodeId(1));
        idx.insert(&Value::Int(1), NodeId(2)); // same key, second node
        idx.insert(&Value::Int(2), NodeId(3));

        assert_eq!(idx.distinct_count(), 2); // keys: 1, 2
        assert_eq!(idx.total_count(), 3); // nodes: 1, 2, 3
    }

    #[test]
    fn can_satisfy_gt_above_max() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Int);
        idx.insert(&Value::Int(10), NodeId(1));
        idx.insert(&Value::Int(20), NodeId(2));

        // query > 20 (the max) -- nothing can satisfy
        assert!(!idx.can_satisfy(RangeOp::Gt, &Value::Int(20)));
        // query > 100 -- nothing can satisfy
        assert!(!idx.can_satisfy(RangeOp::Gt, &Value::Int(100)));
    }

    #[test]
    fn can_satisfy_lt_below_min() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Int);
        idx.insert(&Value::Int(10), NodeId(1));
        idx.insert(&Value::Int(20), NodeId(2));

        // query < 10 (the min) -- nothing can satisfy
        assert!(!idx.can_satisfy(RangeOp::Lt, &Value::Int(10)));
        // query < 5 -- nothing can satisfy
        assert!(!idx.can_satisfy(RangeOp::Lt, &Value::Int(5)));
    }

    #[test]
    fn can_satisfy_in_range() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Int);
        idx.insert(&Value::Int(10), NodeId(1));
        idx.insert(&Value::Int(20), NodeId(2));

        assert!(idx.can_satisfy(RangeOp::Gt, &Value::Int(5)));
        assert!(idx.can_satisfy(RangeOp::Lt, &Value::Int(25)));
        assert!(idx.can_satisfy(RangeOp::Gte, &Value::Int(15)));
        assert!(idx.can_satisfy(RangeOp::Lte, &Value::Int(15)));
    }

    #[test]
    fn can_satisfy_eq_outside_range() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Int);
        idx.insert(&Value::Int(10), NodeId(1));
        idx.insert(&Value::Int(20), NodeId(2));

        assert!(!idx.can_satisfy(RangeOp::Eq, &Value::Int(5)));
        assert!(!idx.can_satisfy(RangeOp::Eq, &Value::Int(100)));
        assert!(idx.can_satisfy(RangeOp::Eq, &Value::Int(15)));
        assert!(idx.can_satisfy(RangeOp::Eq, &Value::Int(10)));
        assert!(idx.can_satisfy(RangeOp::Eq, &Value::Int(20)));
    }

    #[test]
    fn can_satisfy_empty_index() {
        let idx = TypedIndex::new_for_type(&ValueType::Int);

        assert!(!idx.can_satisfy(RangeOp::Gt, &Value::Int(0)));
        assert!(!idx.can_satisfy(RangeOp::Lt, &Value::Int(0)));
        assert!(!idx.can_satisfy(RangeOp::Gte, &Value::Int(0)));
        assert!(!idx.can_satisfy(RangeOp::Lte, &Value::Int(0)));
        assert!(!idx.can_satisfy(RangeOp::Eq, &Value::Int(0)));
    }

    #[test]
    fn selectivity_eq_uniform() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Int);
        for i in 0..10_i64 {
            idx.insert(&Value::Int(i), NodeId(i as u64));
        }

        // Value not present -- uniform 1/distinct
        let sel = idx.selectivity(&SelectivityOp::Eq(&Value::Int(999)));
        let expected = 1.0 / 10.0;
        assert!(
            (sel - expected).abs() < 1e-9,
            "expected {expected}, got {sel}"
        );
    }

    #[test]
    fn selectivity_in_list() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Int);
        for i in 0..20_i64 {
            idx.insert(&Value::Int(i), NodeId(i as u64));
        }

        // IN list of 4 values out of 20 distinct
        let sel = idx.selectivity(&SelectivityOp::In(4));
        let expected = 4.0 / 20.0;
        assert!(
            (sel - expected).abs() < 1e-9,
            "expected {expected}, got {sel}"
        );
    }

    #[test]
    fn selectivity_range_interpolation() {
        let mut idx = TypedIndex::new_for_type(&ValueType::Int);
        // Values 0..=100 (101 distinct)
        for i in 0..=100_i64 {
            idx.insert(&Value::Int(i), NodeId(i as u64));
        }

        // Range [25, 75] out of [0, 100] spans 50/100 = 0.5
        let sel = idx.selectivity(&SelectivityOp::Range {
            lower: Some(&Value::Int(25)),
            upper: Some(&Value::Int(75)),
        });
        let expected = (75.0 - 25.0) / (100.0 - 0.0);
        assert!(
            (sel - expected).abs() < 0.01,
            "expected ~{expected}, got {sel}"
        );
    }

    #[test]
    fn selectivity_empty_index() {
        let idx = TypedIndex::new_for_type(&ValueType::Int);

        assert_eq!(idx.selectivity(&SelectivityOp::IsNotNull), 0.0);
        assert_eq!(idx.selectivity(&SelectivityOp::Eq(&Value::Int(1))), 0.0);
        assert_eq!(idx.selectivity(&SelectivityOp::In(5)), 0.0);
        assert_eq!(
            idx.selectivity(&SelectivityOp::Range {
                lower: None,
                upper: None
            }),
            0.0
        );
    }

    #[test]
    fn composite_index_null_separator_prevents_collision() {
        // Verify that "ab" + "c" does not collide with "a" + "bc"
        let mut idx = CompositeTypedIndex::new(vec![IStr::new("p1"), IStr::new("p2")]);

        idx.insert(
            &[
                &Value::String(SmolStr::new("ab")),
                &Value::String(SmolStr::new("c")),
            ],
            NodeId(1),
        );
        idx.insert(
            &[
                &Value::String(SmolStr::new("a")),
                &Value::String(SmolStr::new("bc")),
            ],
            NodeId(2),
        );

        let r1 = idx.lookup(&[
            &Value::String(SmolStr::new("ab")),
            &Value::String(SmolStr::new("c")),
        ]);
        assert_eq!(r1.map(|v| v.as_slice()), Some(&[NodeId(1)][..]));

        let r2 = idx.lookup(&[
            &Value::String(SmolStr::new("a")),
            &Value::String(SmolStr::new("bc")),
        ]);
        assert_eq!(r2.map(|v| v.as_slice()), Some(&[NodeId(2)][..]));
    }
}
