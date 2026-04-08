//! Columnar execution types for vectorized GQL processing.
//!
//! `DataChunk` replaces row-at-a-time `Binding` execution with Arrow-backed
//! columnar storage. Each variable in a GQL query maps to a `Column` slot
//! in the chunk. Pattern operators produce and consume DataChunks, avoiding
//! per-row `Binding::clone()` overhead.
//!
//! Key types:
//! - `Column`: Arrow-backed column with semantic variants (NodeId, EdgeId, scalars)
//! - `SelectionVector`: tracks active rows without data movement
//! - `ChunkSchema`: maps variable names to column slots
//! - `ColumnBuilder`: type-safe Arrow builder wrapper
//! - `DataChunk`: the main columnar batch flowing through the operator pipeline

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, StringBuilder,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use selene_core::{EdgeId, IStr, NodeId};
use smallvec::SmallVec;

use super::binding::{Binding, BoundValue};
use super::error::GqlError;
use super::value::GqlValue;

// Re-export types extracted to sibling modules so existing `use chunk::*` paths work.
pub(crate) use super::column_builder::ColumnBuilder;
pub(crate) use super::selection_vector::{SelectionIter, SelectionVector};

// ---------------------------------------------------------------------------
// ColumnKind
// ---------------------------------------------------------------------------

/// Semantic type tag for a column slot in `ChunkSchema`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ColumnKind {
    NodeId,
    EdgeId,
    Int64,
    UInt64,
    Float64,
    Bool,
    Utf8,
    Null,
    /// Heterogeneous GqlValue column (Path, Group, Record, Vector, List,
    /// Bytes, or mixed-type expression results).
    Values,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
impl ColumnKind {
    /// Map to an Arrow DataType for RecordBatch schema generation.
    ///
    /// Values columns serialize to Utf8 (matching the existing first-row-wins
    /// Arrow materialization behavior for complex types).
    pub fn arrow_data_type(&self) -> DataType {
        match self {
            Self::NodeId | Self::UInt64 => DataType::UInt64,
            Self::EdgeId => DataType::UInt64,
            Self::Int64 => DataType::Int64,
            Self::Float64 => DataType::Float64,
            Self::Bool => DataType::Boolean,
            Self::Utf8 | Self::Null | Self::Values => DataType::Utf8,
        }
    }
}

// ---------------------------------------------------------------------------
// Column
// ---------------------------------------------------------------------------

/// Arrow-backed column with semantic variants for graph entity IDs.
///
/// `NodeIds` and `EdgeIds` are both `UInt64Array` underneath but carry
/// distinct semantics: the execution engine uses the variant tag to know
/// whether a column holds node references or edge references without
/// consulting the schema.
#[derive(Debug, Clone)]
pub(crate) enum Column {
    /// Node ID references (lazy; properties resolved on demand).
    NodeIds(Arc<UInt64Array>),
    /// Edge ID references (lazy; properties resolved on demand).
    EdgeIds(Arc<UInt64Array>),
    /// Signed 64-bit integers (INT, TIMESTAMP nanos, DURATION nanos).
    Int64(Arc<Int64Array>),
    /// Unsigned 64-bit integers (UINT).
    UInt64(Arc<UInt64Array>),
    /// 64-bit floating point (FLOAT).
    Float64(Arc<Float64Array>),
    /// Boolean values.
    Bool(Arc<BooleanArray>),
    /// UTF-8 strings (STRING, interned strings display form).
    Utf8(Arc<StringArray>),
    /// All-null placeholder (for OPTIONAL unmatched branches).
    Null(usize),
    /// Heterogeneous values: Path, Group, Record, Vector, List, Bytes,
    /// or mixed-type expression results (e.g. CASE WHEN branches with
    /// different types). Preserves full GqlValue fidelity for round-trip
    /// through `to_bindings()` and RowView access.
    Values(Arc<[GqlValue]>),
}

#[allow(dead_code)]
impl Column {
    /// Number of physical rows in this column.
    pub fn len(&self) -> usize {
        match self {
            Self::NodeIds(a) => a.len(),
            Self::EdgeIds(a) => a.len(),
            Self::Int64(a) => a.len(),
            Self::UInt64(a) => a.len(),
            Self::Float64(a) => a.len(),
            Self::Bool(a) => a.len(),
            Self::Utf8(a) => a.len(),
            Self::Null(n) => *n,
            Self::Values(v) => v.len(),
        }
    }

    /// True if this column has zero rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// True if the value at `idx` is null.
    pub fn is_null(&self, idx: usize) -> bool {
        match self {
            Self::NodeIds(a) => a.is_null(idx),
            Self::EdgeIds(a) => a.is_null(idx),
            Self::Int64(a) => a.is_null(idx),
            Self::UInt64(a) => a.is_null(idx),
            Self::Float64(a) => a.is_null(idx),
            Self::Bool(a) => a.is_null(idx),
            Self::Utf8(a) => a.is_null(idx),
            Self::Null(_) => true,
            Self::Values(v) => v[idx].is_null(),
        }
    }

    /// Borrow as a generic Arrow array reference.
    ///
    /// For `Values` columns, materializes to a `StringArray` via `Display`
    /// (matching the existing Arrow output behavior for complex types).
    pub fn as_array_ref(&self) -> ArrayRef {
        match self {
            Self::NodeIds(a) => Arc::clone(a) as ArrayRef,
            Self::EdgeIds(a) => Arc::clone(a) as ArrayRef,
            Self::Int64(a) => Arc::clone(a) as ArrayRef,
            Self::UInt64(a) => Arc::clone(a) as ArrayRef,
            Self::Float64(a) => Arc::clone(a) as ArrayRef,
            Self::Bool(a) => Arc::clone(a) as ArrayRef,
            Self::Utf8(a) => Arc::clone(a) as ArrayRef,
            Self::Null(n) => {
                // Produce a null-filled StringArray to match ColumnKind::Null's
                // arrow_data_type() of DataType::Utf8. Using NullArray here would
                // cause a type mismatch when paired with a Utf8 schema field.
                let mut builder = StringBuilder::new();
                for _ in 0..*n {
                    builder.append_null();
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            Self::Values(v) => {
                let mut builder = StringBuilder::new();
                for val in v.iter() {
                    match val {
                        GqlValue::Null => builder.append_null(),
                        GqlValue::String(s) => builder.append_value(s.as_str()),
                        other => builder.append_value(format!("{other}")),
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
        }
    }

    /// Select specific rows by index, producing a new dense column.
    ///
    /// Uses Arrow's `take` kernel for zero-copy-friendly gathering.
    pub fn gather(&self, indices: &[u32]) -> Self {
        if indices.is_empty() {
            return self.empty_like();
        }
        let idx_array = UInt32Array::from(indices.to_vec());
        match self {
            Self::NodeIds(a) => {
                let taken = arrow::compute::take(a.as_ref(), &idx_array, None).unwrap();
                Self::NodeIds(Arc::new(
                    taken
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .clone(),
                ))
            }
            Self::EdgeIds(a) => {
                let taken = arrow::compute::take(a.as_ref(), &idx_array, None).unwrap();
                Self::EdgeIds(Arc::new(
                    taken
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .clone(),
                ))
            }
            Self::Int64(a) => {
                let taken = arrow::compute::take(a.as_ref(), &idx_array, None).unwrap();
                Self::Int64(Arc::new(
                    taken.as_any().downcast_ref::<Int64Array>().unwrap().clone(),
                ))
            }
            Self::UInt64(a) => {
                let taken = arrow::compute::take(a.as_ref(), &idx_array, None).unwrap();
                Self::UInt64(Arc::new(
                    taken
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .clone(),
                ))
            }
            Self::Float64(a) => {
                let taken = arrow::compute::take(a.as_ref(), &idx_array, None).unwrap();
                Self::Float64(Arc::new(
                    taken
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .clone(),
                ))
            }
            Self::Bool(a) => {
                let taken = arrow::compute::take(a.as_ref(), &idx_array, None).unwrap();
                Self::Bool(Arc::new(
                    taken
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .clone(),
                ))
            }
            Self::Utf8(a) => {
                let taken = arrow::compute::take(a.as_ref(), &idx_array, None).unwrap();
                Self::Utf8(Arc::new(
                    taken
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .clone(),
                ))
            }
            Self::Null(_) => Self::Null(indices.len()),
            Self::Values(v) => {
                let gathered: Vec<GqlValue> =
                    indices.iter().map(|&i| v[i as usize].clone()).collect();
                Self::Values(Arc::from(gathered))
            }
        }
    }

    /// Produce an empty column of the same variant.
    fn empty_like(&self) -> Self {
        match self {
            Self::NodeIds(_) => Self::NodeIds(Arc::new(UInt64Array::from(Vec::<u64>::new()))),
            Self::EdgeIds(_) => Self::EdgeIds(Arc::new(UInt64Array::from(Vec::<u64>::new()))),
            Self::Int64(_) => Self::Int64(Arc::new(Int64Array::from(Vec::<i64>::new()))),
            Self::UInt64(_) => Self::UInt64(Arc::new(UInt64Array::from(Vec::<u64>::new()))),
            Self::Float64(_) => Self::Float64(Arc::new(Float64Array::from(Vec::<f64>::new()))),
            Self::Bool(_) => Self::Bool(Arc::new(BooleanArray::from(Vec::<bool>::new()))),
            Self::Utf8(_) => Self::Utf8(Arc::new(StringArray::from(Vec::<&str>::new()))),
            Self::Null(_) => Self::Null(0),
            Self::Values(_) => Self::Values(Arc::from(Vec::<GqlValue>::new())),
        }
    }

    /// The `ColumnKind` that describes this column's semantic type.
    pub fn kind(&self) -> ColumnKind {
        match self {
            Self::NodeIds(_) => ColumnKind::NodeId,
            Self::EdgeIds(_) => ColumnKind::EdgeId,
            Self::Int64(_) => ColumnKind::Int64,
            Self::UInt64(_) => ColumnKind::UInt64,
            Self::Float64(_) => ColumnKind::Float64,
            Self::Bool(_) => ColumnKind::Bool,
            Self::Utf8(_) => ColumnKind::Utf8,
            Self::Null(_) => ColumnKind::Null,
            Self::Values(_) => ColumnKind::Values,
        }
    }
}

// ---------------------------------------------------------------------------
// ChunkSchema
// ---------------------------------------------------------------------------

/// Maps variable names to column slots in a `DataChunk`.
///
/// Variables are stored in insertion order (not sorted) since column indices
/// must be stable across operators. Uses `SmallVec<8>` to avoid heap
/// allocation for typical queries with fewer than 8 bound variables.
#[derive(Debug, Clone)]
pub(crate) struct ChunkSchema {
    slots: SmallVec<[(IStr, ColumnKind); 8]>,
}

#[allow(dead_code, clippy::trivially_copy_pass_by_ref)]
impl ChunkSchema {
    /// Create an empty schema (no columns).
    pub fn new() -> Self {
        Self {
            slots: SmallVec::new(),
        }
    }

    /// Look up the column slot index for a variable name.
    pub fn slot_of(&self, var: &IStr) -> Option<usize> {
        self.slots.iter().position(|(name, _)| name == var)
    }

    /// Add a new variable-column mapping. Returns the slot index.
    pub fn extend(&mut self, var: IStr, kind: ColumnKind) -> usize {
        let idx = self.slots.len();
        self.slots.push((var, kind));
        idx
    }

    /// Number of columns in the schema.
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// True if the schema has no columns.
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Iterator over variable names in slot order.
    pub fn column_names(&self) -> impl Iterator<Item = &IStr> {
        self.slots.iter().map(|(name, _)| name)
    }

    /// Iterator over (name, kind) pairs in slot order.
    pub fn iter(&self) -> impl Iterator<Item = (&IStr, &ColumnKind)> {
        self.slots.iter().map(|(name, kind)| (name, kind))
    }

    /// Get the kind of column at a given slot.
    pub fn kind_at(&self, slot: usize) -> Option<&ColumnKind> {
        self.slots.get(slot).map(|(_, k)| k)
    }

    /// Get the variable name at a given slot.
    pub fn name_at(&self, slot: usize) -> Option<&IStr> {
        self.slots.get(slot).map(|(n, _)| n)
    }

    /// Build an Arrow Schema from this ChunkSchema (for RecordBatch output).
    pub fn to_arrow_schema(&self) -> Schema {
        let fields: Vec<Field> = self
            .slots
            .iter()
            .map(|(name, kind)| Field::new(name.as_str(), kind.arrow_data_type(), true))
            .collect();
        Schema::new(fields)
    }
}

impl Default for ChunkSchema {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// DataChunk
// ---------------------------------------------------------------------------

/// Columnar batch of rows flowing through the GQL execution pipeline.
///
/// Each column corresponds to a bound variable (node, edge, or scalar).
/// The `SelectionVector` tracks which physical rows are active; filter
/// operations update the selection without moving data (zero-copy).
///
/// DataChunk is the primary data structure for the vectorized execution
/// engine, replacing `Vec<Binding>` in the row-at-a-time model.
#[derive(Debug, Clone)]
pub(crate) struct DataChunk {
    columns: SmallVec<[Column; 8]>,
    schema: ChunkSchema,
    selection: SelectionVector,
    /// Physical row count (before selection filtering).
    len: usize,
}

#[allow(dead_code, clippy::trivially_copy_pass_by_ref)]
impl DataChunk {
    /// Create a unit chunk: one row, zero columns.
    ///
    /// This seeds the pattern operator pipeline. The single row represents
    /// the implicit "start" of pattern matching (analogous to a single
    /// empty `Binding`).
    pub fn unit() -> Self {
        Self {
            columns: SmallVec::new(),
            schema: ChunkSchema::new(),
            selection: SelectionVector::all(1),
            len: 1,
        }
    }

    /// Build a DataChunk from finished column builders.
    ///
    /// Consumes each builder to produce a Column, then wraps them with the
    /// given schema. All builders must produce columns of the same length.
    pub fn from_builders(builders: Vec<ColumnBuilder>, schema: ChunkSchema, len: usize) -> Self {
        let columns: SmallVec<[Column; 8]> = builders.into_iter().map(|b| b.finish()).collect();
        debug_assert!(
            columns.iter().all(|c| c.len() == len),
            "all columns must have the same length"
        );
        Self {
            columns,
            schema,
            selection: SelectionVector::all(len),
            len,
        }
    }

    /// Build a DataChunk from pre-built columns.
    pub fn from_columns(columns: SmallVec<[Column; 8]>, schema: ChunkSchema, len: usize) -> Self {
        debug_assert!(
            columns.len() == schema.len(),
            "column count ({}) must match schema count ({})",
            columns.len(),
            schema.len()
        );
        debug_assert!(
            columns.iter().all(|c| c.len() == len),
            "all columns must have the same length"
        );
        Self {
            columns,
            schema,
            selection: SelectionVector::all(len),
            len,
        }
    }

    /// Physical row count (before selection).
    pub fn len(&self) -> usize {
        self.len
    }

    /// True if the chunk has zero physical rows.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Number of active (non-filtered) rows.
    pub fn active_len(&self) -> usize {
        self.selection.active_len(self.len)
    }

    /// Iterator over active row indices.
    pub fn active_indices(&self) -> SelectionIter<'_> {
        self.selection.active_indices(self.len)
    }

    /// Borrow a column by slot index.
    pub fn column(&self, slot: usize) -> &Column {
        &self.columns[slot]
    }

    /// Borrow all columns.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Borrow the schema.
    pub fn schema(&self) -> &ChunkSchema {
        &self.schema
    }

    /// Mutable reference to the selection vector.
    pub fn selection_mut(&mut self) -> &mut SelectionVector {
        &mut self.selection
    }

    /// Borrow the selection vector.
    pub fn selection(&self) -> &SelectionVector {
        &self.selection
    }

    /// Look up a node ID column by variable name.
    ///
    /// Returns an error if the variable is not found or is not a NodeId column.
    pub fn node_id_column(&self, var: &IStr) -> Result<&UInt64Array, GqlError> {
        let slot = self
            .schema
            .slot_of(var)
            .ok_or_else(|| GqlError::internal(format!("variable '{var}' not in chunk schema")))?;
        match &self.columns[slot] {
            Column::NodeIds(arr) => Ok(arr.as_ref()),
            other => Err(GqlError::type_error(format!(
                "variable '{var}' is {:?}, not NodeIds",
                other.kind()
            ))),
        }
    }

    /// Look up an edge ID column by variable name.
    pub fn edge_id_column(&self, var: &IStr) -> Result<&UInt64Array, GqlError> {
        let slot = self
            .schema
            .slot_of(var)
            .ok_or_else(|| GqlError::internal(format!("variable '{var}' not in chunk schema")))?;
        match &self.columns[slot] {
            Column::EdgeIds(arr) => Ok(arr.as_ref()),
            other => Err(GqlError::type_error(format!(
                "variable '{var}' is {:?}, not EdgeIds",
                other.kind()
            ))),
        }
    }

    /// Append a new column to the chunk, extending the schema.
    pub fn append_column(&mut self, var: IStr, column: Column) {
        debug_assert_eq!(
            column.len(),
            self.len,
            "appended column length must match chunk length"
        );
        let kind = column.kind();
        self.schema.extend(var, kind);
        self.columns.push(column);
    }

    /// Reorder rows by gathering all columns at the given indices.
    ///
    /// Produces a dense chunk (no selection vector) with rows in the order
    /// specified by `indices`. Used by Sort to apply a permutation after
    /// sorting an index array by pre-evaluated sort keys.
    pub fn gather_rows(&self, indices: &[u32]) -> Self {
        let new_len = indices.len();
        let columns: SmallVec<[Column; 8]> =
            self.columns.iter().map(|c| c.gather(indices)).collect();

        Self {
            columns,
            schema: self.schema.clone(),
            selection: SelectionVector::all(new_len),
            len: new_len,
        }
    }

    /// Produce a dense chunk with only active rows, applying the selection.
    ///
    /// If the chunk is already dense, returns a clone. Otherwise, gathers
    /// each column at the active indices.
    pub fn compact(&self) -> Self {
        if self.selection.is_dense() {
            return self.clone();
        }

        let indices: Vec<u32> = self.active_indices().map(|i| i as u32).collect();
        let new_len = indices.len();

        let columns: SmallVec<[Column; 8]> =
            self.columns.iter().map(|c| c.gather(&indices)).collect();

        Self {
            columns,
            schema: self.schema.clone(),
            selection: SelectionVector::all(new_len),
            len: new_len,
        }
    }

    /// Convert to an Arrow RecordBatch.
    ///
    /// Dense chunks are zero-copy (columns are already `Arc<Array>`).
    /// Sparse chunks are compacted first.
    pub fn to_record_batch(&self, arrow_schema: &Arc<Schema>) -> Result<RecordBatch, GqlError> {
        if self.active_len() == 0 {
            return Ok(RecordBatch::new_empty(Arc::clone(arrow_schema)));
        }

        let chunk = if self.selection.is_dense() {
            std::borrow::Cow::Borrowed(self)
        } else {
            std::borrow::Cow::Owned(self.compact())
        };

        let arrays: Vec<ArrayRef> = chunk.columns.iter().map(|c| c.as_array_ref()).collect();

        RecordBatch::try_new(Arc::clone(arrow_schema), arrays)
            .map_err(|e| GqlError::internal(format!("Arrow RecordBatch error: {e}")))
    }

    /// Convert to an Arrow RecordBatch using the chunk's own schema.
    pub fn to_record_batch_auto(&self) -> Result<RecordBatch, GqlError> {
        let schema = Arc::new(self.schema.to_arrow_schema());
        self.to_record_batch(&schema)
    }

    /// Escape hatch: convert active rows back to `Vec<Binding>`.
    ///
    /// Used at the mutation boundary where the mutation engine still
    /// operates on Binding rows. This is the Phase 1 adapter; Phase 2
    /// may remove the need for this entirely.
    pub fn to_bindings(&self) -> Vec<Binding> {
        let mut bindings = Vec::with_capacity(self.active_len());

        for row_idx in self.active_indices() {
            let mut binding = Binding::empty();

            for (slot, (name, kind)) in self.schema.iter().enumerate() {
                let col = &self.columns[slot];
                let value = column_to_bound_value(col, *kind, row_idx);
                binding.bind(*name, value);
            }

            bindings.push(binding);
        }

        bindings
    }

    /// Number of columns in the chunk.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Borrow a single row as a `RowView`.
    ///
    /// `row_idx` is a physical row index (not filtered by selection).
    /// Callers should iterate `active_indices()` and pass each index here.
    pub fn row_view(&self, row_idx: usize) -> RowView<'_> {
        debug_assert!(
            row_idx < self.len,
            "row_idx {row_idx} out of bounds (len {})",
            self.len
        );
        RowView {
            chunk: self,
            row: row_idx,
        }
    }
}

// ---------------------------------------------------------------------------
// RowView
// ---------------------------------------------------------------------------

/// A borrowed view of a single row within a `DataChunk`.
///
/// RowView is the Phase 1 bridge between columnar storage and the existing
/// row-at-a-time expression evaluator. It reads column values at a fixed
/// physical row index without copying the entire row upfront.
///
/// Hot-path accessors (`get_node_id`, `get_edge_id`) read directly from
/// Arrow arrays with zero allocation. The `to_binding()` method materializes
/// a full `Binding` for passing to `eval_expr_ctx`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RowView<'a> {
    chunk: &'a DataChunk,
    row: usize,
}

#[allow(
    dead_code,
    clippy::trivially_copy_pass_by_ref,
    clippy::wrong_self_convention
)]
impl<'a> RowView<'a> {
    /// Read a variable's value from the row, returning an owned `GqlValue`.
    ///
    /// Returns `None` if the variable is not in the schema.
    /// Returns `Some(GqlValue::Null)` if the value is null.
    pub fn get(&self, var: &IStr) -> Option<GqlValue> {
        let slot = self.chunk.schema.slot_of(var)?;
        let col = &self.chunk.columns[slot];
        let kind = self.chunk.schema.slots[slot].1;
        Some(column_to_gql_value(col, kind, self.row))
    }

    /// Extract a NodeId directly from a NodeIds column. Zero allocation.
    pub fn get_node_id(&self, var: &IStr) -> Result<NodeId, GqlError> {
        let slot = self
            .chunk
            .schema
            .slot_of(var)
            .ok_or_else(|| GqlError::internal(format!("unbound variable '{var}'")))?;
        match &self.chunk.columns[slot] {
            Column::NodeIds(arr) => {
                if arr.is_null(self.row) {
                    Err(GqlError::type_error(format!(
                        "variable '{var}' is null at row {}",
                        self.row
                    )))
                } else {
                    Ok(NodeId(arr.value(self.row)))
                }
            }
            other => Err(GqlError::type_error(format!(
                "variable '{var}' is {:?}, not NodeIds",
                other.kind()
            ))),
        }
    }

    /// Extract an EdgeId directly from an EdgeIds column. Zero allocation.
    pub fn get_edge_id(&self, var: &IStr) -> Result<EdgeId, GqlError> {
        let slot = self
            .chunk
            .schema
            .slot_of(var)
            .ok_or_else(|| GqlError::internal(format!("unbound variable '{var}'")))?;
        match &self.chunk.columns[slot] {
            Column::EdgeIds(arr) => {
                if arr.is_null(self.row) {
                    Err(GqlError::type_error(format!(
                        "variable '{var}' is null at row {}",
                        self.row
                    )))
                } else {
                    Ok(EdgeId(arr.value(self.row)))
                }
            }
            other => Err(GqlError::type_error(format!(
                "variable '{var}' is {:?}, not EdgeIds",
                other.kind()
            ))),
        }
    }

    /// Materialize this row as a `Binding` for the expression evaluator.
    ///
    /// This is the main Phase 1 adapter path: RowView -> Binding -> eval_expr_ctx.
    /// Allocates a SmallVec for the variable map (stack-allocated for <8 vars).
    pub fn to_binding(&self) -> Binding {
        let mut binding = Binding::empty();
        for (slot, (name, kind)) in self.chunk.schema.iter().enumerate() {
            let col = &self.chunk.columns[slot];
            let value = column_to_bound_value(col, *kind, self.row);
            binding.bind(*name, value);
        }
        binding
    }

    /// The physical row index this view points to.
    pub fn row_index(&self) -> usize {
        self.row
    }

    /// Borrow the parent DataChunk.
    pub fn chunk(&self) -> &'a DataChunk {
        self.chunk
    }
}

/// Extract a `GqlValue` from a column at a given row index (public API for `eval_vec`).
///
/// Infers the kind from the column variant. For callers that already have
/// the `ColumnKind`, the private `column_to_gql_value` is slightly cheaper.
pub(crate) fn column_to_gql_value_pub(col: &Column, row: usize) -> GqlValue {
    column_to_gql_value(col, col.kind(), row)
}

/// Extract a `GqlValue` from a column at a given row index.
fn column_to_gql_value(col: &Column, kind: ColumnKind, row: usize) -> GqlValue {
    if col.is_null(row) {
        return GqlValue::Null;
    }
    match (col, kind) {
        (Column::NodeIds(arr), _) => GqlValue::Node(NodeId(arr.value(row))),
        (Column::EdgeIds(arr), _) => GqlValue::Edge(EdgeId(arr.value(row))),
        (Column::Int64(arr), _) => GqlValue::Int(arr.value(row)),
        (Column::UInt64(arr), _) => GqlValue::UInt(arr.value(row)),
        (Column::Float64(arr), _) => GqlValue::Float(arr.value(row)),
        (Column::Bool(arr), _) => GqlValue::Bool(arr.value(row)),
        (Column::Utf8(arr), _) => GqlValue::String(arr.value(row).into()),
        (Column::Null(_), _) => GqlValue::Null,
        (Column::Values(v), _) => v[row].clone(),
    }
}

/// Extract a `BoundValue` from a column at a given row index.
fn column_to_bound_value(col: &Column, kind: ColumnKind, row: usize) -> BoundValue {
    if col.is_null(row) {
        return BoundValue::Scalar(GqlValue::Null);
    }

    match (col, kind) {
        (Column::NodeIds(arr), ColumnKind::NodeId) => BoundValue::Node(NodeId(arr.value(row))),
        (Column::EdgeIds(arr), ColumnKind::EdgeId) => BoundValue::Edge(EdgeId(arr.value(row))),
        (Column::Int64(arr), _) => BoundValue::Scalar(GqlValue::Int(arr.value(row))),
        (Column::UInt64(arr), _) => BoundValue::Scalar(GqlValue::UInt(arr.value(row))),
        (Column::Float64(arr), _) => BoundValue::Scalar(GqlValue::Float(arr.value(row))),
        (Column::Bool(arr), _) => BoundValue::Scalar(GqlValue::Bool(arr.value(row))),
        (Column::Utf8(arr), _) => BoundValue::Scalar(GqlValue::String(arr.value(row).into())),
        (Column::Null(_), _) => BoundValue::Scalar(GqlValue::Null),
        (Column::Values(v), _) => gql_value_to_bound_value(&v[row]),
        // Fallback for mismatched column/kind combos
        _ => BoundValue::Scalar(GqlValue::Null),
    }
}

/// Convert a `GqlValue` back to a `BoundValue`.
///
/// Recovers the original BoundValue variant for graph-native types
/// (Node, Edge, Path). Other types become `BoundValue::Scalar`.
fn gql_value_to_bound_value(val: &GqlValue) -> BoundValue {
    match val {
        GqlValue::Node(id) => BoundValue::Node(*id),
        GqlValue::Edge(id) => BoundValue::Edge(*id),
        GqlValue::Path(p) => BoundValue::Path(p.clone()),
        other => BoundValue::Scalar(other.clone()),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use arrow::array::UInt64Builder;

    use super::*;

    #[test]
    fn column_gather_node_ids() {
        let arr = UInt64Array::from(vec![10, 20, 30, 40, 50]);
        let col = Column::NodeIds(Arc::new(arr));
        let gathered = col.gather(&[1, 3]);

        assert_eq!(gathered.len(), 2);
        match &gathered {
            Column::NodeIds(arr) => {
                assert_eq!(arr.value(0), 20);
                assert_eq!(arr.value(1), 40);
            }
            _ => panic!("expected NodeIds"),
        }
    }

    #[test]
    fn column_gather_empty_indices() {
        let arr = UInt64Array::from(vec![10, 20, 30]);
        let col = Column::NodeIds(Arc::new(arr));
        let gathered = col.gather(&[]);
        assert_eq!(gathered.len(), 0);
    }

    #[test]
    fn column_gather_int64() {
        let arr = Int64Array::from(vec![100, 200, 300]);
        let col = Column::Int64(Arc::new(arr));
        let gathered = col.gather(&[0, 2]);

        match &gathered {
            Column::Int64(arr) => {
                assert_eq!(arr.value(0), 100);
                assert_eq!(arr.value(1), 300);
            }
            _ => panic!("expected Int64"),
        }
    }

    #[test]
    fn column_gather_utf8() {
        let arr = StringArray::from(vec!["alpha", "beta", "gamma"]);
        let col = Column::Utf8(Arc::new(arr));
        let gathered = col.gather(&[2, 0]);

        match &gathered {
            Column::Utf8(arr) => {
                assert_eq!(arr.value(0), "gamma");
                assert_eq!(arr.value(1), "alpha");
            }
            _ => panic!("expected Utf8"),
        }
    }

    #[test]
    fn column_gather_null() {
        let col = Column::Null(5);
        let gathered = col.gather(&[0, 2, 4]);
        assert_eq!(gathered.len(), 3);
        assert!(gathered.is_null(0));
    }

    #[test]
    fn column_is_null() {
        let mut builder = UInt64Builder::with_capacity(3);
        builder.append_value(10);
        builder.append_null();
        builder.append_value(30);
        let arr = builder.finish();
        let col = Column::NodeIds(Arc::new(arr));

        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
    }

    #[test]
    fn column_kind() {
        let col = Column::NodeIds(Arc::new(UInt64Array::from(vec![1])));
        assert_eq!(col.kind(), ColumnKind::NodeId);

        let col = Column::Utf8(Arc::new(StringArray::from(vec!["a"])));
        assert_eq!(col.kind(), ColumnKind::Utf8);
    }

    // ---- SelectionVector ----

    #[test]
    fn selection_vector_all_active() {
        let sel = SelectionVector::all(5);
        assert!(sel.is_dense());
        assert_eq!(sel.active_len(5), 5);

        let indices: Vec<usize> = sel.active_indices(5).collect();
        assert_eq!(indices, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn selection_vector_filter() {
        let mut sel = SelectionVector::all(5);
        // Keep rows 0, 2, 4 (even indices)
        sel.apply_bool_mask(&[true, false, true, false, true], 5);

        assert!(!sel.is_dense());
        assert_eq!(sel.active_len(5), 3);

        let indices: Vec<usize> = sel.active_indices(5).collect();
        assert_eq!(indices, vec![0, 2, 4]);
    }

    #[test]
    fn selection_vector_double_filter() {
        let mut sel = SelectionVector::all(5);
        sel.apply_bool_mask(&[true, false, true, false, true], 5);
        // Now active = [0, 2, 4]. Filter again: keep first and third.
        sel.apply_bool_mask(&[true, false, true], 5);

        let indices: Vec<usize> = sel.active_indices(5).collect();
        assert_eq!(indices, vec![0, 4]);
    }

    #[test]
    fn selection_vector_truncate() {
        let mut sel = SelectionVector::all(10);
        sel.truncate(3, 10);

        assert_eq!(sel.active_len(10), 3);
        let indices: Vec<usize> = sel.active_indices(10).collect();
        assert_eq!(indices, vec![0, 1, 2]);
    }

    #[test]
    fn selection_vector_truncate_sparse() {
        let mut sel = SelectionVector::from_indices(vec![2, 5, 8, 11]);
        sel.truncate(2, 12);

        assert_eq!(sel.active_len(12), 2);
        let indices: Vec<usize> = sel.active_indices(12).collect();
        assert_eq!(indices, vec![2, 5]);
    }

    #[test]
    fn selection_vector_skip() {
        let mut sel = SelectionVector::all(5);
        sel.skip(2, 5);

        assert_eq!(sel.active_len(5), 3);
        let indices: Vec<usize> = sel.active_indices(5).collect();
        assert_eq!(indices, vec![2, 3, 4]);
    }

    #[test]
    fn selection_vector_skip_sparse() {
        let mut sel = SelectionVector::from_indices(vec![1, 3, 5, 7]);
        sel.skip(2, 8);

        let indices: Vec<usize> = sel.active_indices(8).collect();
        assert_eq!(indices, vec![5, 7]);
    }

    #[test]
    fn selection_vector_none() {
        let sel = SelectionVector::none();
        assert_eq!(sel.active_len(0), 0);
        assert_eq!(sel.active_indices(0).count(), 0);
    }

    #[test]
    fn selection_vector_empty_all() {
        let sel = SelectionVector::all(0);
        assert_eq!(sel.active_len(0), 0);
        assert_eq!(sel.active_indices(0).count(), 0);
    }

    // ---- ChunkSchema ----

    #[test]
    fn chunk_schema_slot_of() {
        let mut schema = ChunkSchema::new();
        schema.extend(IStr::new("n"), ColumnKind::NodeId);
        schema.extend(IStr::new("e"), ColumnKind::EdgeId);
        schema.extend(IStr::new("m"), ColumnKind::NodeId);

        assert_eq!(schema.slot_of(&IStr::new("n")), Some(0));
        assert_eq!(schema.slot_of(&IStr::new("e")), Some(1));
        assert_eq!(schema.slot_of(&IStr::new("m")), Some(2));
        assert_eq!(schema.slot_of(&IStr::new("x")), None);
    }

    #[test]
    fn chunk_schema_len_and_names() {
        let mut schema = ChunkSchema::new();
        assert!(schema.is_empty());

        schema.extend(IStr::new("a"), ColumnKind::Int64);
        schema.extend(IStr::new("b"), ColumnKind::Utf8);

        assert_eq!(schema.len(), 2);
        let names: Vec<&str> = schema.column_names().map(|s| s.as_str()).collect();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[test]
    fn chunk_schema_kind_at() {
        let mut schema = ChunkSchema::new();
        schema.extend(IStr::new("n"), ColumnKind::NodeId);
        schema.extend(IStr::new("x"), ColumnKind::Float64);

        assert_eq!(schema.kind_at(0), Some(&ColumnKind::NodeId));
        assert_eq!(schema.kind_at(1), Some(&ColumnKind::Float64));
        assert_eq!(schema.kind_at(2), None);
    }

    #[test]
    fn chunk_schema_to_arrow() {
        let mut schema = ChunkSchema::new();
        schema.extend(IStr::new("id"), ColumnKind::UInt64);
        schema.extend(IStr::new("name"), ColumnKind::Utf8);
        schema.extend(IStr::new("active"), ColumnKind::Bool);

        let arrow_schema = schema.to_arrow_schema();
        assert_eq!(arrow_schema.fields().len(), 3);
        assert_eq!(arrow_schema.field(0).name(), "id");
        assert_eq!(*arrow_schema.field(0).data_type(), DataType::UInt64);
        assert_eq!(*arrow_schema.field(1).data_type(), DataType::Utf8);
        assert_eq!(*arrow_schema.field(2).data_type(), DataType::Boolean);
    }

    // ---- ColumnBuilder ----

    #[test]
    fn column_builder_node_ids() {
        let mut builder = ColumnBuilder::new_node_ids(3);
        builder.append_node_id(NodeId(10));
        builder.append_node_id(NodeId(20));
        builder.append_null();
        let col = builder.finish();

        assert_eq!(col.len(), 3);
        assert!(!col.is_null(0));
        assert!(col.is_null(2));

        match &col {
            Column::NodeIds(arr) => {
                assert_eq!(arr.value(0), 10);
                assert_eq!(arr.value(1), 20);
            }
            _ => panic!("expected NodeIds"),
        }
    }

    #[test]
    fn column_builder_gql_values() {
        let mut builder = ColumnBuilder::new_int64(3);
        builder.append_gql_value(&GqlValue::Int(42));
        builder.append_gql_value(&GqlValue::UInt(7));
        builder.append_gql_value(&GqlValue::Null);
        let col = builder.finish();

        match &col {
            Column::Int64(arr) => {
                assert_eq!(arr.value(0), 42);
                assert_eq!(arr.value(1), 7); // UInt coerced to i64
                assert!(arr.is_null(2));
            }
            _ => panic!("expected Int64"),
        }
    }

    #[test]
    fn column_builder_for_kind() {
        let builder = ColumnBuilder::for_kind(ColumnKind::Float64, 10);
        assert_eq!(builder.kind(), ColumnKind::Float64);

        let builder = ColumnBuilder::for_kind(ColumnKind::EdgeId, 5);
        assert_eq!(builder.kind(), ColumnKind::EdgeId);
    }

    // ---- DataChunk ----

    #[test]
    fn datachunk_unit() {
        let chunk = DataChunk::unit();
        assert_eq!(chunk.len(), 1);
        assert_eq!(chunk.active_len(), 1);
        assert_eq!(chunk.column_count(), 0);
        assert!(chunk.schema().is_empty());
    }

    #[test]
    fn datachunk_from_builders() {
        let mut schema = ChunkSchema::new();
        schema.extend(IStr::new("n"), ColumnKind::NodeId);
        schema.extend(IStr::new("val"), ColumnKind::Int64);

        let mut b1 = ColumnBuilder::new_node_ids(3);
        b1.append_node_id(NodeId(1));
        b1.append_node_id(NodeId(2));
        b1.append_node_id(NodeId(3));

        let mut b2 = ColumnBuilder::new_int64(3);
        b2.append_gql_value(&GqlValue::Int(10));
        b2.append_gql_value(&GqlValue::Int(20));
        b2.append_gql_value(&GqlValue::Int(30));

        let chunk = DataChunk::from_builders(vec![b1, b2], schema, 3);

        assert_eq!(chunk.len(), 3);
        assert_eq!(chunk.active_len(), 3);
        assert_eq!(chunk.column_count(), 2);

        let nids = chunk.node_id_column(&IStr::new("n")).unwrap();
        assert_eq!(nids.value(0), 1);
        assert_eq!(nids.value(2), 3);
    }

    #[test]
    fn datachunk_compact() {
        let mut schema = ChunkSchema::new();
        schema.extend(IStr::new("n"), ColumnKind::NodeId);

        let mut b = ColumnBuilder::new_node_ids(5);
        for id in [10, 20, 30, 40, 50] {
            b.append_node_id(NodeId(id));
        }

        let mut chunk = DataChunk::from_builders(vec![b], schema, 5);

        // Filter to keep only rows 1 and 3
        chunk
            .selection_mut()
            .apply_bool_mask(&[false, true, false, true, false], 5);
        assert_eq!(chunk.active_len(), 2);

        let compacted = chunk.compact();
        assert_eq!(compacted.len(), 2);
        assert_eq!(compacted.active_len(), 2);
        assert!(compacted.selection().is_dense());

        let nids = compacted.node_id_column(&IStr::new("n")).unwrap();
        assert_eq!(nids.value(0), 20);
        assert_eq!(nids.value(1), 40);
    }

    #[test]
    fn datachunk_compact_already_dense() {
        let mut schema = ChunkSchema::new();
        schema.extend(IStr::new("n"), ColumnKind::NodeId);

        let mut b = ColumnBuilder::new_node_ids(2);
        b.append_node_id(NodeId(1));
        b.append_node_id(NodeId(2));

        let chunk = DataChunk::from_builders(vec![b], schema, 2);
        let compacted = chunk.compact();

        assert_eq!(compacted.len(), 2);
        assert!(compacted.selection().is_dense());
    }

    #[test]
    fn datachunk_to_record_batch_dense() {
        let mut schema = ChunkSchema::new();
        schema.extend(IStr::new("id"), ColumnKind::UInt64);
        schema.extend(IStr::new("name"), ColumnKind::Utf8);

        let mut b1 = ColumnBuilder::new_uint64(2);
        b1.append_gql_value(&GqlValue::UInt(1));
        b1.append_gql_value(&GqlValue::UInt(2));

        let mut b2 = ColumnBuilder::new_utf8();
        b2.append_gql_value(&GqlValue::String("alice".into()));
        b2.append_gql_value(&GqlValue::String("bob".into()));

        let chunk = DataChunk::from_builders(vec![b1, b2], schema, 2);

        let arrow_schema = Arc::new(chunk.schema().to_arrow_schema());
        let batch = chunk.to_record_batch(&arrow_schema).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);

        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
        assert_eq!(names.value(1), "bob");
    }

    #[test]
    fn datachunk_to_record_batch_sparse() {
        let mut schema = ChunkSchema::new();
        schema.extend(IStr::new("val"), ColumnKind::Int64);

        let mut b = ColumnBuilder::new_int64(4);
        for v in [10, 20, 30, 40] {
            b.append_gql_value(&GqlValue::Int(v));
        }

        let mut chunk = DataChunk::from_builders(vec![b], schema, 4);
        chunk
            .selection_mut()
            .apply_bool_mask(&[true, false, true, false], 4);

        let arrow_schema = Arc::new(chunk.schema().to_arrow_schema());
        let batch = chunk.to_record_batch(&arrow_schema).unwrap();

        assert_eq!(batch.num_rows(), 2);
        let vals = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(vals.value(0), 10);
        assert_eq!(vals.value(1), 30);
    }

    #[test]
    fn datachunk_to_record_batch_empty() {
        let schema = ChunkSchema::new();
        let chunk = DataChunk::from_builders(vec![], schema, 0);

        let arrow_schema = Arc::new(chunk.schema().to_arrow_schema());
        let batch = chunk.to_record_batch(&arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn datachunk_to_bindings() {
        let mut schema = ChunkSchema::new();
        schema.extend(IStr::new("n"), ColumnKind::NodeId);
        schema.extend(IStr::new("score"), ColumnKind::Float64);

        let mut b1 = ColumnBuilder::new_node_ids(3);
        b1.append_node_id(NodeId(10));
        b1.append_node_id(NodeId(20));
        b1.append_node_id(NodeId(30));

        let mut b2 = ColumnBuilder::new_float64(3);
        b2.append_gql_value(&GqlValue::Float(1.5));
        b2.append_gql_value(&GqlValue::Float(2.5));
        b2.append_gql_value(&GqlValue::Float(3.5));

        let mut chunk = DataChunk::from_builders(vec![b1, b2], schema, 3);
        // Filter: keep only rows 0 and 2
        chunk
            .selection_mut()
            .apply_bool_mask(&[true, false, true], 3);

        let bindings = chunk.to_bindings();
        assert_eq!(bindings.len(), 2);

        assert_eq!(
            bindings[0].get_node_id(&IStr::new("n")).unwrap(),
            NodeId(10)
        );
        assert_eq!(
            bindings[1].get_node_id(&IStr::new("n")).unwrap(),
            NodeId(30)
        );

        match bindings[0].get(&IStr::new("score")) {
            Some(BoundValue::Scalar(GqlValue::Float(f))) => assert_eq!(*f, 1.5),
            _ => panic!("expected Float"),
        }
    }

    #[test]
    fn datachunk_to_bindings_all_types() {
        let mut schema = ChunkSchema::new();
        schema.extend(IStr::new("n"), ColumnKind::NodeId);
        schema.extend(IStr::new("e"), ColumnKind::EdgeId);
        schema.extend(IStr::new("i"), ColumnKind::Int64);
        schema.extend(IStr::new("u"), ColumnKind::UInt64);
        schema.extend(IStr::new("f"), ColumnKind::Float64);
        schema.extend(IStr::new("b"), ColumnKind::Bool);
        schema.extend(IStr::new("s"), ColumnKind::Utf8);

        let mut bn = ColumnBuilder::new_node_ids(1);
        bn.append_node_id(NodeId(1));
        let mut be = ColumnBuilder::new_edge_ids(1);
        be.append_edge_id(EdgeId(2));
        let mut bi = ColumnBuilder::new_int64(1);
        bi.append_gql_value(&GqlValue::Int(-42));
        let mut bu = ColumnBuilder::new_uint64(1);
        bu.append_gql_value(&GqlValue::UInt(99));
        let mut bf = ColumnBuilder::new_float64(1);
        bf.append_gql_value(&GqlValue::Float(3.15));
        let mut bb = ColumnBuilder::new_bool(1);
        bb.append_gql_value(&GqlValue::Bool(true));
        let mut bs = ColumnBuilder::new_utf8();
        bs.append_gql_value(&GqlValue::String("hello".into()));

        let chunk = DataChunk::from_builders(vec![bn, be, bi, bu, bf, bb, bs], schema, 1);
        let bindings = chunk.to_bindings();
        assert_eq!(bindings.len(), 1);

        let b = &bindings[0];
        assert_eq!(b.get_node_id(&IStr::new("n")).unwrap(), NodeId(1));
        assert_eq!(b.get_edge_id(&IStr::new("e")).unwrap(), EdgeId(2));
        assert!(matches!(
            b.get(&IStr::new("i")),
            Some(BoundValue::Scalar(GqlValue::Int(-42)))
        ));
        assert!(matches!(
            b.get(&IStr::new("u")),
            Some(BoundValue::Scalar(GqlValue::UInt(99)))
        ));
        assert!(matches!(
            b.get(&IStr::new("b")),
            Some(BoundValue::Scalar(GqlValue::Bool(true)))
        ));
        match b.get(&IStr::new("s")) {
            Some(BoundValue::Scalar(GqlValue::String(s))) => assert_eq!(s.as_str(), "hello"),
            _ => panic!("expected String"),
        }
    }

    #[test]
    fn datachunk_append_column() {
        let mut chunk = DataChunk::unit();

        let mut b = ColumnBuilder::new_node_ids(1);
        b.append_node_id(NodeId(42));
        chunk.append_column(IStr::new("n"), b.finish());

        assert_eq!(chunk.column_count(), 1);
        assert_eq!(chunk.schema().slot_of(&IStr::new("n")), Some(0));

        let nids = chunk.node_id_column(&IStr::new("n")).unwrap();
        assert_eq!(nids.value(0), 42);
    }

    #[test]
    fn datachunk_node_id_column_wrong_type() {
        let mut schema = ChunkSchema::new();
        schema.extend(IStr::new("x"), ColumnKind::Int64);

        let mut b = ColumnBuilder::new_int64(1);
        b.append_gql_value(&GqlValue::Int(1));
        let chunk = DataChunk::from_builders(vec![b], schema, 1);

        assert!(chunk.node_id_column(&IStr::new("x")).is_err());
        assert!(chunk.node_id_column(&IStr::new("missing")).is_err());
    }

    #[test]
    fn datachunk_edge_id_column() {
        let mut schema = ChunkSchema::new();
        schema.extend(IStr::new("e"), ColumnKind::EdgeId);

        let mut b = ColumnBuilder::new_edge_ids(2);
        b.append_edge_id(EdgeId(100));
        b.append_edge_id(EdgeId(200));
        let chunk = DataChunk::from_builders(vec![b], schema, 2);

        let eids = chunk.edge_id_column(&IStr::new("e")).unwrap();
        assert_eq!(eids.value(0), 100);
        assert_eq!(eids.value(1), 200);
    }

    // ---- Column::Values ----

    #[test]
    fn column_values_round_trip_path() {
        use crate::types::value::GqlPath;

        let path = GqlPath::from_nodes_and_edges(&[NodeId(1), NodeId(2)], &[EdgeId(10)]);
        let col = Column::Values(Arc::from(vec![
            GqlValue::Path(path),
            GqlValue::Null,
            GqlValue::Int(42),
        ]));

        assert_eq!(col.len(), 3);
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
        assert_eq!(col.kind(), ColumnKind::Values);

        // Round-trip through to_bindings
        let mut schema = ChunkSchema::new();
        schema.extend(IStr::new("p"), ColumnKind::Values);
        let chunk = DataChunk::from_columns(smallvec::smallvec![col], schema, 3);
        let bindings = chunk.to_bindings();
        assert_eq!(bindings.len(), 3);

        // Path preserved
        match bindings[0].get(&IStr::new("p")) {
            Some(BoundValue::Path(p)) => assert_eq!(p.edge_count(), 1),
            other => panic!("expected Path, got {other:?}"),
        }
        // Null preserved
        match bindings[1].get(&IStr::new("p")) {
            Some(BoundValue::Scalar(GqlValue::Null)) => {}
            other => panic!("expected Null, got {other:?}"),
        }
        // Int preserved as Scalar
        match bindings[2].get(&IStr::new("p")) {
            Some(BoundValue::Scalar(GqlValue::Int(42))) => {}
            other => panic!("expected Int(42), got {other:?}"),
        }
    }

    #[test]
    fn column_values_gather() {
        let col = Column::Values(Arc::from(vec![
            GqlValue::Int(10),
            GqlValue::String("hello".into()),
            GqlValue::Float(3.15),
            GqlValue::Bool(true),
        ]));

        let gathered = col.gather(&[1, 3]);
        assert_eq!(gathered.len(), 2);

        match &gathered {
            Column::Values(v) => {
                assert!(matches!(&v[0], GqlValue::String(s) if s == "hello"));
                assert!(matches!(&v[1], GqlValue::Bool(true)));
            }
            _ => panic!("expected Values"),
        }
    }

    #[test]
    fn column_values_as_array_ref() {
        let col = Column::Values(Arc::from(vec![
            GqlValue::Int(42),
            GqlValue::Null,
            GqlValue::String("test".into()),
        ]));

        let arr = col.as_array_ref();
        let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_arr.len(), 3);
        assert_eq!(str_arr.value(0), "42");
        assert!(str_arr.is_null(1));
        assert_eq!(str_arr.value(2), "test");
    }

    #[test]
    fn column_builder_values() {
        let mut builder = ColumnBuilder::new_values(3);
        builder.append_gql_value(&GqlValue::Int(1));
        builder.append_gql_value(&GqlValue::String("two".into()));
        builder.append_null();
        let col = builder.finish();

        assert_eq!(col.len(), 3);
        assert_eq!(col.kind(), ColumnKind::Values);
        assert!(col.is_null(2));

        match &col {
            Column::Values(v) => {
                assert!(matches!(&v[0], GqlValue::Int(1)));
                assert!(matches!(&v[1], GqlValue::String(s) if s == "two"));
                assert!(matches!(&v[2], GqlValue::Null));
            }
            _ => panic!("expected Values"),
        }
    }

    #[test]
    fn column_builder_append_bound_value() {
        use crate::types::value::GqlPath;

        let mut builder = ColumnBuilder::new_values(4);
        builder.append_bound_value(&BoundValue::Node(NodeId(1)));
        builder.append_bound_value(&BoundValue::Edge(EdgeId(2)));
        builder.append_bound_value(&BoundValue::Scalar(GqlValue::Float(3.15)));
        let path = GqlPath::from_nodes_and_edges(&[NodeId(10), NodeId(20)], &[EdgeId(100)]);
        builder.append_bound_value(&BoundValue::Path(path));
        let col = builder.finish();

        match &col {
            Column::Values(v) => {
                assert!(matches!(&v[0], GqlValue::Node(NodeId(1))));
                assert!(matches!(&v[1], GqlValue::Edge(EdgeId(2))));
                assert!(matches!(&v[2], GqlValue::Float(f) if *f == 3.15));
                assert!(matches!(&v[3], GqlValue::Path(_)));
            }
            _ => panic!("expected Values"),
        }
    }

    // ---- RowView ----

    fn make_test_chunk() -> DataChunk {
        let mut schema = ChunkSchema::new();
        schema.extend(IStr::new("n"), ColumnKind::NodeId);
        schema.extend(IStr::new("e"), ColumnKind::EdgeId);
        schema.extend(IStr::new("score"), ColumnKind::Float64);
        schema.extend(IStr::new("name"), ColumnKind::Utf8);

        let mut bn = ColumnBuilder::new_node_ids(3);
        bn.append_node_id(NodeId(10));
        bn.append_node_id(NodeId(20));
        bn.append_node_id(NodeId(30));

        let mut be = ColumnBuilder::new_edge_ids(3);
        be.append_edge_id(EdgeId(100));
        be.append_edge_id(EdgeId(200));
        be.append_edge_id(EdgeId(300));

        let mut bf = ColumnBuilder::new_float64(3);
        bf.append_gql_value(&GqlValue::Float(1.5));
        bf.append_gql_value(&GqlValue::Float(2.5));
        bf.append_gql_value(&GqlValue::Float(3.5));

        let mut bs = ColumnBuilder::new_utf8();
        bs.append_gql_value(&GqlValue::String("alice".into()));
        bs.append_gql_value(&GqlValue::String("bob".into()));
        bs.append_gql_value(&GqlValue::String("carol".into()));

        DataChunk::from_builders(vec![bn, be, bf, bs], schema, 3)
    }

    #[test]
    fn row_view_get_node_id() {
        let chunk = make_test_chunk();
        let row = chunk.row_view(1);

        assert_eq!(row.get_node_id(&IStr::new("n")).unwrap(), NodeId(20));
        assert_eq!(row.row_index(), 1);
    }

    #[test]
    fn row_view_get_edge_id() {
        let chunk = make_test_chunk();
        let row = chunk.row_view(2);

        assert_eq!(row.get_edge_id(&IStr::new("e")).unwrap(), EdgeId(300));
    }

    #[test]
    fn row_view_get_node_id_wrong_type() {
        let chunk = make_test_chunk();
        let row = chunk.row_view(0);

        // "score" is Float64, not NodeIds
        assert!(row.get_node_id(&IStr::new("score")).is_err());
        // "missing" doesn't exist
        assert!(row.get_node_id(&IStr::new("missing")).is_err());
    }

    #[test]
    fn row_view_get_value() {
        let chunk = make_test_chunk();
        let row = chunk.row_view(0);

        assert_eq!(row.get(&IStr::new("score")), Some(GqlValue::Float(1.5)));
        assert!(matches!(
            row.get(&IStr::new("name")),
            Some(GqlValue::String(s)) if s == "alice"
        ));
        assert_eq!(row.get(&IStr::new("missing")), None);
    }

    #[test]
    fn row_view_to_binding_round_trip() {
        let chunk = make_test_chunk();
        let row = chunk.row_view(1);
        let binding = row.to_binding();

        // Verify all four variables round-trip correctly
        assert_eq!(binding.get_node_id(&IStr::new("n")).unwrap(), NodeId(20));
        assert_eq!(binding.get_edge_id(&IStr::new("e")).unwrap(), EdgeId(200));
        match binding.get(&IStr::new("score")) {
            Some(BoundValue::Scalar(GqlValue::Float(f))) => assert_eq!(*f, 2.5),
            other => panic!("expected Float(2.5), got {other:?}"),
        }
        match binding.get(&IStr::new("name")) {
            Some(BoundValue::Scalar(GqlValue::String(s))) => assert_eq!(s.as_str(), "bob"),
            other => panic!("expected String(\"bob\"), got {other:?}"),
        }
    }

    #[test]
    fn row_view_with_values_column() {
        use crate::types::value::GqlPath;

        let path = GqlPath::from_nodes_and_edges(&[NodeId(1), NodeId(2)], &[EdgeId(10)]);
        let mut schema = ChunkSchema::new();
        schema.extend(IStr::new("p"), ColumnKind::Values);

        let col = Column::Values(Arc::from(vec![GqlValue::Path(path)]));
        let chunk = DataChunk::from_columns(smallvec::smallvec![col], schema, 1);

        let row = chunk.row_view(0);

        // get() returns GqlValue::Path
        match row.get(&IStr::new("p")) {
            Some(GqlValue::Path(p)) => assert_eq!(p.edge_count(), 1),
            other => panic!("expected Path, got {other:?}"),
        }

        // to_binding() produces BoundValue::Path
        let binding = row.to_binding();
        match binding.get(&IStr::new("p")) {
            Some(BoundValue::Path(p)) => assert_eq!(p.edge_count(), 1),
            other => panic!("expected BoundValue::Path, got {other:?}"),
        }
    }

    #[test]
    fn row_view_active_iteration() {
        let mut chunk = make_test_chunk();
        // Filter to keep only rows 0 and 2
        chunk
            .selection_mut()
            .apply_bool_mask(&[true, false, true], 3);

        let mut node_ids = Vec::new();
        for i in chunk.active_indices() {
            let row = chunk.row_view(i);
            node_ids.push(row.get_node_id(&IStr::new("n")).unwrap());
        }
        assert_eq!(node_ids, vec![NodeId(10), NodeId(30)]);
    }
}
