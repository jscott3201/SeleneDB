//! Batch property gathering from the graph for columnar evaluation.
//!
//! `PropertyGatherer` batch-reads a single property across a column of entity
//! IDs. Instead of N individual `graph.get_node(id)?.property(key)` calls
//! interleaved with expression evaluation, the gatherer reads all values in
//! one pass and returns a typed `Column`.
//!
//! Type specialization: when all non-null values share the same type (common
//! for schema-constrained properties), the gatherer emits a native Arrow column
//! (Int64, Float64, Utf8, Bool). Mixed-type results fall back to
//! `Column::Values` for full GqlValue fidelity.

use std::sync::Arc;

use arrow::array::{
    Array, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, UInt64Array, UInt64Builder,
};
use selene_core::IStr;
use selene_graph::SeleneGraph;

use crate::types::chunk::Column;
use crate::types::value::GqlValue;

/// Batch property reader for columnar expression evaluation.
///
/// Implementations gather a single named property across a column of entity
/// IDs, producing a typed `Column` result. The selection slice restricts
/// which rows are actually read (unselected rows produce null).
pub(crate) trait PropertyGatherer: Send + Sync {
    /// Gather a node property for each ID in the column.
    ///
    /// `ids` is the full physical column of NodeIds. `selection` restricts
    /// which physical rows to read: `None` means all rows, `Some(indices)`
    /// means only those physical indices. Non-selected rows are null in the
    /// output column.
    fn gather_node_property(
        &self,
        ids: &UInt64Array,
        selection: Option<&[u32]>,
        key: IStr,
    ) -> Column;

    /// Gather an edge property for each ID in the column.
    fn gather_edge_property(
        &self,
        ids: &UInt64Array,
        selection: Option<&[u32]>,
        key: IStr,
    ) -> Column;
}

/// Graph-backed property gatherer that reads from a `SeleneGraph` snapshot.
pub(crate) struct GraphPropertyGatherer<'a> {
    graph: &'a SeleneGraph,
}

impl<'a> GraphPropertyGatherer<'a> {
    pub fn new(graph: &'a SeleneGraph) -> Self {
        Self { graph }
    }
}

impl PropertyGatherer for GraphPropertyGatherer<'_> {
    fn gather_node_property(
        &self,
        ids: &UInt64Array,
        selection: Option<&[u32]>,
        key: IStr,
    ) -> Column {
        let len = ids.len();

        // Special property: .id returns the node ID itself
        if key.as_str() == "id" {
            return gather_id_property(ids, selection, len);
        }

        // First pass: collect GqlValues and track type homogeneity
        let mut values = vec![GqlValue::Null; len];
        let mut type_tracker = TypeTracker::new();

        let iter: Box<dyn Iterator<Item = usize>> = match selection {
            Some(indices) => Box::new(indices.iter().map(|&i| i as usize)),
            None => Box::new(0..len),
        };

        for i in iter {
            if ids.is_null(i) {
                continue;
            }
            let nid = selene_core::NodeId(ids.value(i));
            if let Some(node) = self.graph.get_node(nid) {
                let val = match node.properties.get(key) {
                    Some(v) => GqlValue::from(v),
                    None => {
                        // Schema default fallback for lazy migration
                        match self.graph.schema().property_default(node.labels, key) {
                            Some(v) => GqlValue::from(&v),
                            None => GqlValue::Null,
                        }
                    }
                };
                type_tracker.observe(&val);
                values[i] = val;
            }
        }

        specialize_column(values, &type_tracker)
    }

    fn gather_edge_property(
        &self,
        ids: &UInt64Array,
        selection: Option<&[u32]>,
        key: IStr,
    ) -> Column {
        let len = ids.len();

        // Special property: .id returns the edge ID itself
        if key.as_str() == "id" {
            return gather_id_property(ids, selection, len);
        }

        let mut values = vec![GqlValue::Null; len];
        let mut type_tracker = TypeTracker::new();

        let iter: Box<dyn Iterator<Item = usize>> = match selection {
            Some(indices) => Box::new(indices.iter().map(|&i| i as usize)),
            None => Box::new(0..len),
        };

        for i in iter {
            if ids.is_null(i) {
                continue;
            }
            let eid = selene_core::EdgeId(ids.value(i));
            if let Some(edge) = self.graph.get_edge(eid) {
                // Edge special properties
                let val = match key.as_str() {
                    "source" => GqlValue::Node(edge.source),
                    "target" => GqlValue::Node(edge.target),
                    "label" => GqlValue::String(edge.label.as_str().into()),
                    _ => match edge.properties.get(key) {
                        Some(v) => GqlValue::from(v),
                        None => GqlValue::Null,
                    },
                };
                type_tracker.observe(&val);
                values[i] = val;
            }
        }

        specialize_column(values, &type_tracker)
    }
}

/// Gather .id as a UInt64 column (reuses the IDs array, selecting active rows).
fn gather_id_property(ids: &UInt64Array, selection: Option<&[u32]>, len: usize) -> Column {
    let mut builder = UInt64Builder::with_capacity(len);
    match selection {
        None => {
            for i in 0..len {
                if ids.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(ids.value(i));
                }
            }
        }
        Some(indices) => {
            let mut active = std::collections::HashSet::new();
            for &idx in indices {
                active.insert(idx as usize);
            }
            for i in 0..len {
                if active.contains(&i) && !ids.is_null(i) {
                    builder.append_value(ids.value(i));
                } else {
                    builder.append_null();
                }
            }
        }
    }
    Column::UInt64(Arc::new(builder.finish()))
}

// ---------------------------------------------------------------------------
// Type specialization
// ---------------------------------------------------------------------------

/// Tracks the types of non-null values observed during gathering.
/// Used to decide whether the output column can use a native Arrow type.
///
/// Bit flags are cleaner than an enum here because a single gather pass
/// can observe multiple types (mixed-type columns). The bitmask lets us
/// detect homogeneity in O(1) after the scan.
#[allow(clippy::struct_excessive_bools)]
struct TypeTracker {
    has_int: bool,
    has_uint: bool,
    has_float: bool,
    has_bool: bool,
    has_string: bool,
    has_other: bool,
    non_null_count: usize,
}

impl TypeTracker {
    fn new() -> Self {
        Self {
            has_int: false,
            has_uint: false,
            has_float: false,
            has_bool: false,
            has_string: false,
            has_other: false,
            non_null_count: 0,
        }
    }

    fn observe(&mut self, val: &GqlValue) {
        if val.is_null() {
            return;
        }
        self.non_null_count += 1;
        match val {
            GqlValue::Int(_) => self.has_int = true,
            GqlValue::UInt(_) => self.has_uint = true,
            GqlValue::Float(_) => self.has_float = true,
            GqlValue::Bool(_) => self.has_bool = true,
            GqlValue::String(_) => self.has_string = true,
            _ => self.has_other = true,
        }
    }

    /// True if all non-null values share a single type that maps to a native
    /// Arrow column. Returns the detected type, or None if mixed/other.
    fn homogeneous_type(&self) -> Option<HomogeneousType> {
        if self.non_null_count == 0 || self.has_other {
            return None;
        }

        let type_count = u8::from(self.has_int)
            + u8::from(self.has_uint)
            + u8::from(self.has_float)
            + u8::from(self.has_bool)
            + u8::from(self.has_string);

        if type_count == 1 {
            if self.has_int {
                return Some(HomogeneousType::Int64);
            }
            if self.has_uint {
                return Some(HomogeneousType::UInt64);
            }
            if self.has_float {
                return Some(HomogeneousType::Float64);
            }
            if self.has_bool {
                return Some(HomogeneousType::Bool);
            }
            if self.has_string {
                return Some(HomogeneousType::Utf8);
            }
        }

        // Numeric promotion: Int + Float or UInt + Float -> Float64
        if type_count == 2 && self.has_float && (self.has_int || self.has_uint) {
            return Some(HomogeneousType::Float64);
        }

        None
    }
}

enum HomogeneousType {
    Int64,
    UInt64,
    Float64,
    Bool,
    Utf8,
}

/// Convert a Vec<GqlValue> to a specialized Column when type-homogeneous,
/// or Column::Values as fallback.
fn specialize_column(values: Vec<GqlValue>, tracker: &TypeTracker) -> Column {
    match tracker.homogeneous_type() {
        Some(HomogeneousType::Int64) => {
            let mut builder = Int64Builder::with_capacity(values.len());
            for v in &values {
                match v {
                    GqlValue::Int(i) => builder.append_value(*i),
                    _ => builder.append_null(),
                }
            }
            Column::Int64(Arc::new(builder.finish()))
        }
        Some(HomogeneousType::UInt64) => {
            let mut builder = UInt64Builder::with_capacity(values.len());
            for v in &values {
                match v {
                    GqlValue::UInt(u) => builder.append_value(*u),
                    _ => builder.append_null(),
                }
            }
            Column::UInt64(Arc::new(builder.finish()))
        }
        Some(HomogeneousType::Float64) => {
            let mut builder = Float64Builder::with_capacity(values.len());
            for v in &values {
                match v {
                    GqlValue::Float(f) => builder.append_value(*f),
                    GqlValue::Int(i) => builder.append_value(*i as f64),
                    GqlValue::UInt(u) => builder.append_value(*u as f64),
                    _ => builder.append_null(),
                }
            }
            Column::Float64(Arc::new(builder.finish()))
        }
        Some(HomogeneousType::Bool) => {
            let mut builder = BooleanBuilder::with_capacity(values.len());
            for v in &values {
                match v {
                    GqlValue::Bool(b) => builder.append_value(*b),
                    _ => builder.append_null(),
                }
            }
            Column::Bool(Arc::new(builder.finish()))
        }
        Some(HomogeneousType::Utf8) => {
            let mut builder = StringBuilder::new();
            for v in &values {
                match v {
                    GqlValue::String(s) => builder.append_value(s.as_str()),
                    _ => builder.append_null(),
                }
            }
            Column::Utf8(Arc::new(builder.finish()))
        }
        None => Column::Values(Arc::from(values)),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{IStr, LabelSet, PropertyMap, Value};
    use selene_graph::SeleneGraph;
    use smol_str::SmolStr;

    fn test_graph() -> SeleneGraph {
        let mut graph = SeleneGraph::new();
        let mut m = graph.mutate();

        let n0 = m
            .create_node(
                LabelSet::from_strs(&["Person"]),
                PropertyMap::from_pairs(vec![
                    (IStr::new("name"), Value::String(SmolStr::new("alice"))),
                    (IStr::new("score"), Value::Int(100)),
                ]),
            )
            .unwrap();

        let n1 = m
            .create_node(
                LabelSet::from_strs(&["Person"]),
                PropertyMap::from_pairs(vec![
                    (IStr::new("name"), Value::String(SmolStr::new("bob"))),
                    (IStr::new("score"), Value::Int(200)),
                ]),
            )
            .unwrap();

        m.create_node(
            LabelSet::from_strs(&["Person"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::String(SmolStr::new("carol"))),
                (IStr::new("score"), Value::Int(300)),
            ]),
        )
        .unwrap();

        m.create_edge(n0, IStr::new("KNOWS"), n1, PropertyMap::new())
            .unwrap();

        m.commit(0).unwrap();
        graph
    }

    #[test]
    fn gather_node_string_property() {
        let graph = test_graph();
        let gatherer = GraphPropertyGatherer::new(&graph);
        let ids = UInt64Array::from(vec![1, 2, 3]);

        let col = gatherer.gather_node_property(&ids, None, IStr::new("name"));

        // All strings -> should produce Utf8 column
        match &col {
            Column::Utf8(arr) => {
                assert_eq!(arr.value(0), "alice");
                assert_eq!(arr.value(1), "bob");
                assert_eq!(arr.value(2), "carol");
            }
            other => panic!("expected Utf8, got {:?}", other.kind()),
        }
    }

    #[test]
    fn gather_node_int_property() {
        let graph = test_graph();
        let gatherer = GraphPropertyGatherer::new(&graph);
        let ids = UInt64Array::from(vec![1, 2, 3]);

        let col = gatherer.gather_node_property(&ids, None, IStr::new("score"));

        match &col {
            Column::Int64(arr) => {
                assert_eq!(arr.value(0), 100);
                assert_eq!(arr.value(1), 200);
                assert_eq!(arr.value(2), 300);
            }
            other => panic!("expected Int64, got {:?}", other.kind()),
        }
    }

    #[test]
    fn gather_node_id_property() {
        let graph = test_graph();
        let gatherer = GraphPropertyGatherer::new(&graph);
        let ids = UInt64Array::from(vec![1, 3]);

        let col = gatherer.gather_node_property(&ids, None, IStr::new("id"));

        match &col {
            Column::UInt64(arr) => {
                assert_eq!(arr.value(0), 1);
                assert_eq!(arr.value(1), 3);
            }
            other => panic!("expected UInt64, got {:?}", other.kind()),
        }
    }

    #[test]
    fn gather_node_missing_property_returns_null() {
        let graph = test_graph();
        let gatherer = GraphPropertyGatherer::new(&graph);
        let ids = UInt64Array::from(vec![1, 2]);

        let col = gatherer.gather_node_property(&ids, None, IStr::new("nonexistent"));

        // All null -> Values column with nulls
        assert_eq!(col.len(), 2);
        assert!(col.is_null(0));
        assert!(col.is_null(1));
    }

    #[test]
    fn gather_with_selection() {
        let graph = test_graph();
        let gatherer = GraphPropertyGatherer::new(&graph);
        let ids = UInt64Array::from(vec![1, 2, 3]);

        // Only read rows 0 and 2
        let col = gatherer.gather_node_property(&ids, Some(&[0, 2]), IStr::new("score"));

        assert_eq!(col.len(), 3);
        // Row 1 should be null (not selected)
        assert!(col.is_null(1));
    }

    #[test]
    fn gather_edge_property() {
        let graph = test_graph();
        let gatherer = GraphPropertyGatherer::new(&graph);

        // EdgeId(1) is the KNOWS edge
        let ids = UInt64Array::from(vec![1u64]);

        let col = gatherer.gather_edge_property(&ids, None, IStr::new("label"));
        match &col {
            Column::Utf8(arr) => assert_eq!(arr.value(0), "KNOWS"),
            other => panic!("expected Utf8, got {:?}", other.kind()),
        }
    }

    #[test]
    fn type_tracker_homogeneous() {
        let mut t = TypeTracker::new();
        t.observe(&GqlValue::Int(1));
        t.observe(&GqlValue::Int(2));
        t.observe(&GqlValue::Null);
        assert!(matches!(t.homogeneous_type(), Some(HomogeneousType::Int64)));
    }

    #[test]
    fn type_tracker_numeric_promotion() {
        let mut t = TypeTracker::new();
        t.observe(&GqlValue::Int(1));
        t.observe(&GqlValue::Float(2.0));
        assert!(matches!(
            t.homogeneous_type(),
            Some(HomogeneousType::Float64)
        ));
    }

    #[test]
    fn type_tracker_mixed_non_promotable() {
        let mut t = TypeTracker::new();
        t.observe(&GqlValue::Int(1));
        t.observe(&GqlValue::String("x".into()));
        assert!(t.homogeneous_type().is_none());
    }
}
