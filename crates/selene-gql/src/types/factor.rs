//! Factorized columnar representation for multi-hop graph patterns.
//!
//! Instead of materializing the full Cartesian product of multi-hop
//! traversals (O(n^k) rows), factorized execution keeps each expansion
//! level at its natural cardinality and links child rows to parent rows
//! via `parent_indices`. A 3-hop pattern with fanout 10 stores
//! 100 + 1,000 + 10,000 rows of unique data instead of replicating
//! anchor data across all 10,000 flat rows.
//!
//! Key types:
//! - `FactorLevel`: one expansion hop's columns + parent linkage
//! - `LevelSchema`: variable-to-slot mapping within a single level
//! - `FactorizedChunk`: stack of levels representing a full multi-hop match
//! - `FactorizedRowView`: per-row accessor that walks parent chains

use std::sync::Arc;

use arrow::array::Array;
use selene_core::{EdgeId, IStr, NodeId};
use smallvec::SmallVec;

use super::chunk::{ChunkSchema, Column, ColumnKind, DataChunk, SelectionVector};
use super::error::GqlError;

// ---------------------------------------------------------------------------
// LevelSchema
// ---------------------------------------------------------------------------

/// Maps variable names to column slots within a single [`FactorLevel`].
///
/// Each level introduces a small number of variables (typically 1-2:
/// target node + optional edge). Uses `SmallVec<4>` since levels rarely
/// have more than 3-4 columns.
#[derive(Debug, Clone)]
pub(crate) struct LevelSchema {
    slots: SmallVec<[(IStr, ColumnKind); 4]>,
}

#[allow(dead_code, clippy::trivially_copy_pass_by_ref)]
impl LevelSchema {
    /// Create an empty schema.
    pub fn new() -> Self {
        Self {
            slots: SmallVec::new(),
        }
    }

    /// Add a variable-column mapping. Returns the slot index within this level.
    pub fn push(&mut self, var: IStr, kind: ColumnKind) -> usize {
        let idx = self.slots.len();
        self.slots.push((var, kind));
        idx
    }

    /// Look up the column slot index for a variable name.
    pub fn slot_of(&self, var: &IStr) -> Option<usize> {
        self.slots.iter().position(|(name, _)| name == var)
    }

    /// Number of columns in this level.
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// True if the schema has no columns.
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Iterator over (name, kind) pairs in slot order.
    pub fn iter(&self) -> impl Iterator<Item = (&IStr, &ColumnKind)> {
        self.slots.iter().map(|(name, kind)| (name, kind))
    }

    /// Get the variable name at a given slot.
    pub fn name_at(&self, slot: usize) -> Option<&IStr> {
        self.slots.get(slot).map(|(n, _)| n)
    }

    /// Get the column kind at a given slot.
    pub fn kind_at(&self, slot: usize) -> Option<&ColumnKind> {
        self.slots.get(slot).map(|(_, k)| k)
    }
}

impl Default for LevelSchema {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// FactorLevel
// ---------------------------------------------------------------------------

/// One level in a factorized multi-hop result.
///
/// Owns the columns introduced at this hop (e.g., edge variable + target
/// variable for an Expand) plus a parent linkage array that maps each row
/// to a row in the previous level. For the root level (LabelScan output),
/// `parent_indices` is `None`.
///
/// `parent_indices[i] = j` means row `i` in this level was produced by
/// expanding from row `j` in the parent level.
#[derive(Debug, Clone)]
pub(crate) struct FactorLevel {
    /// Columns introduced at this hop (never includes parent columns).
    pub columns: SmallVec<[Column; 4]>,
    /// Schema mapping variable names to column slots within this level.
    pub schema: LevelSchema,
    /// Physical row count (before selection filtering).
    pub len: usize,
    /// Active row selection (same semantics as DataChunk's SelectionVector).
    pub selection: SelectionVector,
    /// Maps each row in this level to its parent row in the previous level.
    /// `None` for the root level.
    pub parent_indices: Option<Arc<[u32]>>,
}

#[allow(dead_code)]
impl FactorLevel {
    /// Create the root level from a LabelScan column.
    pub fn root(var: IStr, column: Column) -> Self {
        let len = column.len();
        let mut schema = LevelSchema::new();
        schema.push(var, column.kind());
        Self {
            columns: SmallVec::from_elem(column, 1),
            schema,
            len,
            selection: SelectionVector::all(len),
            parent_indices: None,
        }
    }

    /// Create an expansion level with parent linkage.
    pub fn expansion(
        columns: SmallVec<[Column; 4]>,
        schema: LevelSchema,
        len: usize,
        parent_indices: Arc<[u32]>,
    ) -> Self {
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
        debug_assert_eq!(
            parent_indices.len(),
            len,
            "parent_indices length must match level length"
        );
        Self {
            columns,
            schema,
            len,
            selection: SelectionVector::all(len),
            parent_indices: Some(parent_indices),
        }
    }

    /// Number of active (non-filtered) rows.
    pub fn active_len(&self) -> usize {
        self.selection.active_len(self.len)
    }

    /// Borrow a column by slot index.
    pub fn column(&self, slot: usize) -> &Column {
        &self.columns[slot]
    }

    /// Read a node ID at a physical row index from a column slot.
    pub fn get_node_id(&self, col_slot: usize, row: usize) -> Result<NodeId, GqlError> {
        match &self.columns[col_slot] {
            Column::NodeIds(arr) => {
                if arr.is_null(row) {
                    Err(GqlError::internal("null node ID in factorized level"))
                } else {
                    Ok(NodeId(arr.value(row)))
                }
            }
            other => Err(GqlError::type_error(format!(
                "expected NodeIds column, got {:?}",
                other.kind()
            ))),
        }
    }

    /// Read an edge ID at a physical row index from a column slot.
    pub fn get_edge_id(&self, col_slot: usize, row: usize) -> Result<EdgeId, GqlError> {
        match &self.columns[col_slot] {
            Column::EdgeIds(arr) => {
                if arr.is_null(row) {
                    Err(GqlError::internal("null edge ID in factorized level"))
                } else {
                    Ok(EdgeId(arr.value(row)))
                }
            }
            other => Err(GqlError::type_error(format!(
                "expected EdgeIds column, got {:?}",
                other.kind()
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// FactorizedChunk
// ---------------------------------------------------------------------------

/// A factorized columnar result spanning multiple expansion hops.
///
/// `levels[0]` is the root scan (e.g., variable `a`).
/// `levels[1]` is the first Expand output (e.g., `b` via `e1`).
/// `levels\[k\]` is the k-th Expand output.
///
/// To reconstruct logical row `i` of the deepest level, walk
/// `parent_indices` back to the root:
///   `level[k].row(i) + level[k-1].row(level[k].parent_indices[i]) + ...`
///
/// `active_len()` reflects the active rows in the deepest level.
#[derive(Debug, Clone)]
pub(crate) struct FactorizedChunk {
    pub levels: SmallVec<[FactorLevel; 4]>,
}

#[allow(dead_code, clippy::trivially_copy_pass_by_ref)]
impl FactorizedChunk {
    /// Create a factorized chunk from a root level.
    pub fn from_root(root: FactorLevel) -> Self {
        debug_assert!(
            root.parent_indices.is_none(),
            "root level must not have parent_indices"
        );
        Self {
            levels: SmallVec::from_elem(root, 1),
        }
    }

    /// Add a new expansion level.
    pub fn push_level(&mut self, level: FactorLevel) {
        debug_assert!(
            level.parent_indices.is_some(),
            "non-root level must have parent_indices"
        );
        self.levels.push(level);
    }

    /// Number of expansion levels (including root).
    pub fn depth(&self) -> usize {
        self.levels.len()
    }

    /// Borrow the deepest (most recently expanded) level.
    pub fn deepest(&self) -> &FactorLevel {
        self.levels
            .last()
            .expect("FactorizedChunk must have at least one level")
    }

    /// Mutable reference to the deepest level.
    pub fn deepest_mut(&mut self) -> &mut FactorLevel {
        self.levels
            .last_mut()
            .expect("FactorizedChunk must have at least one level")
    }

    /// Logical row count (active rows in the deepest level).
    pub fn active_len(&self) -> usize {
        self.deepest().active_len()
    }

    /// Physical row count of the deepest level.
    pub fn deepest_len(&self) -> usize {
        self.deepest().len
    }

    /// Find which level and column slot a variable lives in.
    ///
    /// Searches from deepest to root, returning the first match.
    pub fn find_var(&self, var: &IStr) -> Option<(usize, usize)> {
        for (level_idx, level) in self.levels.iter().enumerate().rev() {
            if let Some(slot) = level.schema.slot_of(var) {
                return Some((level_idx, slot));
            }
        }
        None
    }

    /// Resolve a physical row index from the deepest level back to
    /// a row index at `target_level` by walking `parent_indices`.
    ///
    /// If `deepest_row` is in level `k` and we want level `t` (where t < k),
    /// we follow: `level[k].parent_indices -> level[k-1].parent_indices -> ...`
    pub fn resolve_row_at_level(&self, mut row: usize, target_level: usize) -> usize {
        let deepest = self.levels.len() - 1;
        // Walk from deepest back to target_level
        for level_idx in (target_level + 1..=deepest).rev() {
            row = self.levels[level_idx]
                .parent_indices
                .as_ref()
                .map_or(row, |p| p[row] as usize);
        }
        row
    }

    /// Resolve a node ID for a variable at a given deepest-level row.
    pub fn resolve_node_id(&self, var: &IStr, deepest_row: usize) -> Result<NodeId, GqlError> {
        let (level_idx, col_slot) = self.find_var(var).ok_or_else(|| {
            GqlError::internal(format!("variable '{var}' not in factorized chunk"))
        })?;
        let row_in_level = self.resolve_row_at_level(deepest_row, level_idx);
        self.levels[level_idx].get_node_id(col_slot, row_in_level)
    }

    /// Resolve an edge ID for a variable at a given deepest-level row.
    pub fn resolve_edge_id(&self, var: &IStr, deepest_row: usize) -> Result<EdgeId, GqlError> {
        let (level_idx, col_slot) = self.find_var(var).ok_or_else(|| {
            GqlError::internal(format!("variable '{var}' not in factorized chunk"))
        })?;
        let row_in_level = self.resolve_row_at_level(deepest_row, level_idx);
        self.levels[level_idx].get_edge_id(col_slot, row_in_level)
    }

    /// Apply a boolean mask to the deepest level's selection vector.
    ///
    /// The mask is indexed by position within the active set of the
    /// deepest level. Parent levels are unaffected.
    pub fn filter_deepest(&mut self, mask: &[bool]) {
        let deep = self.deepest_mut();
        let phys_len = deep.len;
        deep.selection.apply_bool_mask(mask, phys_len);
    }

    /// Apply a BooleanArray filter to the deepest level.
    pub fn filter_deepest_bool_column(&mut self, col: &arrow::array::BooleanArray) {
        let deep = self.deepest_mut();
        let phys_len = deep.len;
        deep.selection.apply_bool_column(col, phys_len);
    }

    /// Gather a column from a specific level, resolved for all active
    /// rows in the deepest level. The output has one value per active
    /// deepest-level row, following parent_indices as needed.
    ///
    /// Used by property gathering to resolve ancestor variable properties.
    pub fn gather_column_for_active_rows(&self, level_idx: usize, col_slot: usize) -> Column {
        let deepest = self.levels.len() - 1;
        let deep_level = &self.levels[deepest];
        let source_col = &self.levels[level_idx].columns[col_slot];

        if level_idx == deepest {
            // Same level: just gather by active indices if sparse
            if deep_level.selection.is_dense() {
                return source_col.clone();
            }
            let indices: Vec<u32> = deep_level
                .selection
                .active_indices(deep_level.len)
                .map(|i| i as u32)
                .collect();
            return source_col.gather(&indices);
        }

        // Different level: resolve each active deepest row to the ancestor level
        let indices: Vec<u32> = deep_level
            .selection
            .active_indices(deep_level.len)
            .map(|row| self.resolve_row_at_level(row, level_idx) as u32)
            .collect();
        source_col.gather(&indices)
    }

    /// Flatten to a regular `DataChunk` by materializing the full
    /// Cartesian product through parent_indices.
    ///
    /// This is the correctness baseline: the output is identical to what
    /// the flat expand pipeline would produce. Called at Sort, GroupBy,
    /// RETURN, and other flatten-boundary operators.
    pub fn flatten(&self) -> DataChunk {
        let deepest = self.levels.len() - 1;
        let deep_level = &self.levels[deepest];
        let active_count = deep_level.active_len();

        if active_count == 0 {
            // Build empty chunk with full schema and matching empty columns
            let schema = self.build_flat_schema();
            let columns: SmallVec<[Column; 8]> = schema.iter().map(|_| Column::Null(0)).collect();
            return DataChunk::from_columns(columns, schema, 0);
        }

        // Collect active row indices from deepest level
        let active_rows: Vec<usize> = deep_level
            .selection
            .active_indices(deep_level.len)
            .collect();

        // For each level, gather columns at the resolved row indices
        let mut all_columns: SmallVec<[Column; 8]> = SmallVec::new();
        let mut schema = ChunkSchema::new();

        for (level_idx, level) in self.levels.iter().enumerate() {
            // Resolve each active deepest row to this level's row index
            let resolved_indices: Vec<u32> = if level_idx == deepest {
                active_rows.iter().map(|&r| r as u32).collect()
            } else {
                active_rows
                    .iter()
                    .map(|&r| self.resolve_row_at_level(r, level_idx) as u32)
                    .collect()
            };

            for (col_slot, (var, kind)) in level.schema.iter().enumerate() {
                let gathered = level.columns[col_slot].gather(&resolved_indices);
                all_columns.push(gathered);
                schema.extend(*var, *kind);
            }
        }

        DataChunk::from_columns(all_columns, schema, active_count)
    }

    /// Build the combined flat schema from all levels.
    fn build_flat_schema(&self) -> ChunkSchema {
        let mut schema = ChunkSchema::new();
        for level in &self.levels {
            for (var, kind) in level.schema.iter() {
                schema.extend(*var, *kind);
            }
        }
        schema
    }

    /// Create a DataChunk view of just the deepest level's columns.
    ///
    /// Used by eval_vec to evaluate expressions that only reference
    /// variables introduced at the deepest level.
    pub fn deepest_as_chunk(&self) -> DataChunk {
        let deep = self.deepest();
        let mut schema = ChunkSchema::new();
        let mut columns: SmallVec<[Column; 8]> = SmallVec::new();
        for (col_slot, (var, kind)) in deep.schema.iter().enumerate() {
            columns.push(deep.columns[col_slot].clone());
            schema.extend(*var, *kind);
        }
        let mut chunk = DataChunk::from_columns(columns, schema, deep.len);
        // Preserve the selection vector
        if let Some(indices) = deep.selection.indices() {
            *chunk.selection_mut() = SelectionVector::from_indices(indices.to_vec());
        }
        chunk
    }
}

// ---------------------------------------------------------------------------
// FactorizedRowView
// ---------------------------------------------------------------------------

/// Per-row accessor for a factorized chunk that resolves variables
/// from ancestor levels by walking `parent_indices`.
///
/// `deepest_row` is a physical row index in the deepest level.
/// Variable lookups find the level owning the variable, then resolve
/// the row index through the parent chain.
pub(crate) struct FactorizedRowView<'a> {
    chunk: &'a FactorizedChunk,
    deepest_row: usize,
}

#[allow(dead_code, clippy::trivially_copy_pass_by_ref)]
impl<'a> FactorizedRowView<'a> {
    /// Create a row view for a specific deepest-level row.
    pub fn new(chunk: &'a FactorizedChunk, deepest_row: usize) -> Self {
        Self { chunk, deepest_row }
    }

    /// Get a node ID for a variable.
    pub fn get_node_id(&self, var: &IStr) -> Result<NodeId, GqlError> {
        self.chunk.resolve_node_id(var, self.deepest_row)
    }

    /// Get an edge ID for a variable.
    pub fn get_edge_id(&self, var: &IStr) -> Result<EdgeId, GqlError> {
        self.chunk.resolve_edge_id(var, self.deepest_row)
    }

    /// Get the raw column value for a variable at this row.
    pub fn get_column_value(&self, var: &IStr) -> Result<&Column, GqlError> {
        let (level_idx, col_slot) = self.chunk.find_var(var).ok_or_else(|| {
            GqlError::internal(format!("variable '{var}' not in factorized chunk"))
        })?;
        Ok(&self.chunk.levels[level_idx].columns[col_slot])
    }

    /// Get the resolved row index for a variable (in that variable's level).
    pub fn resolved_row_for_var(&self, var: &IStr) -> Result<(usize, usize), GqlError> {
        let (level_idx, col_slot) = self.chunk.find_var(var).ok_or_else(|| {
            GqlError::internal(format!("variable '{var}' not in factorized chunk"))
        })?;
        let row = self.chunk.resolve_row_at_level(self.deepest_row, level_idx);
        Ok((row, col_slot))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::UInt64Array;

    fn make_node_ids(ids: &[u64]) -> Column {
        Column::NodeIds(Arc::new(UInt64Array::from(ids.to_vec())))
    }

    fn make_edge_ids(ids: &[u64]) -> Column {
        Column::EdgeIds(Arc::new(UInt64Array::from(ids.to_vec())))
    }

    #[test]
    fn test_level_schema_basic() {
        let mut schema = LevelSchema::new();
        let a = IStr::new("a");
        let b = IStr::new("b");

        assert_eq!(schema.push(a, ColumnKind::NodeId), 0);
        assert_eq!(schema.push(b, ColumnKind::EdgeId), 1);
        assert_eq!(schema.len(), 2);
        assert_eq!(schema.slot_of(&a), Some(0));
        assert_eq!(schema.slot_of(&b), Some(1));
        assert_eq!(schema.slot_of(&IStr::new("c")), None);
    }

    #[test]
    fn test_factor_level_root() {
        let a = IStr::new("a");
        let col = make_node_ids(&[1, 2, 3]);
        let level = FactorLevel::root(a, col);

        assert_eq!(level.len, 3);
        assert_eq!(level.active_len(), 3);
        assert!(level.parent_indices.is_none());
        assert_eq!(level.schema.slot_of(&a), Some(0));
    }

    #[test]
    fn test_factor_level_expansion() {
        let e1 = IStr::new("e1");
        let b = IStr::new("b");

        let mut schema = LevelSchema::new();
        schema.push(e1, ColumnKind::EdgeId);
        schema.push(b, ColumnKind::NodeId);

        let edge_col = make_edge_ids(&[10, 11, 12, 13, 14]);
        let target_col = make_node_ids(&[20, 21, 22, 23, 24]);
        let parent_idx: Arc<[u32]> = Arc::from([0u32, 0, 1, 1, 2]);

        let level = FactorLevel::expansion(
            SmallVec::from_vec(vec![edge_col, target_col]),
            schema,
            5,
            parent_idx,
        );

        assert_eq!(level.len, 5);
        assert_eq!(level.active_len(), 5);
        assert!(level.parent_indices.is_some());
        assert_eq!(level.get_edge_id(0, 2).unwrap(), EdgeId(12));
        assert_eq!(level.get_node_id(1, 3).unwrap(), NodeId(23));
    }

    #[test]
    fn test_factorized_chunk_find_var() {
        let a = IStr::new("a");
        let e1 = IStr::new("e1");
        let b = IStr::new("b");

        // Root: a
        let root = FactorLevel::root(a, make_node_ids(&[1, 2, 3]));
        let mut chunk = FactorizedChunk::from_root(root);

        // Level 1: e1, b
        let mut schema1 = LevelSchema::new();
        schema1.push(e1, ColumnKind::EdgeId);
        schema1.push(b, ColumnKind::NodeId);

        chunk.push_level(FactorLevel::expansion(
            SmallVec::from_vec(vec![
                make_edge_ids(&[10, 11, 12, 13, 14]),
                make_node_ids(&[20, 21, 22, 23, 24]),
            ]),
            schema1,
            5,
            Arc::from([0u32, 0, 1, 1, 2]),
        ));

        assert_eq!(chunk.find_var(&a), Some((0, 0)));
        assert_eq!(chunk.find_var(&e1), Some((1, 0)));
        assert_eq!(chunk.find_var(&b), Some((1, 1)));
        assert_eq!(chunk.find_var(&IStr::new("c")), None);
        assert_eq!(chunk.depth(), 2);
        assert_eq!(chunk.active_len(), 5);
    }

    #[test]
    fn test_resolve_row_at_level() {
        let a = IStr::new("a");
        let b = IStr::new("b");
        let c = IStr::new("c");

        // Root: 3 nodes [1, 2, 3]
        let root = FactorLevel::root(a, make_node_ids(&[1, 2, 3]));
        let mut chunk = FactorizedChunk::from_root(root);

        // Level 1: 5 expansions, parents [0, 0, 1, 1, 2]
        let mut s1 = LevelSchema::new();
        s1.push(b, ColumnKind::NodeId);
        chunk.push_level(FactorLevel::expansion(
            SmallVec::from_vec(vec![make_node_ids(&[10, 11, 12, 13, 14])]),
            s1,
            5,
            Arc::from([0u32, 0, 1, 1, 2]),
        ));

        // Level 2: 8 expansions, parents [0, 0, 1, 2, 2, 3, 3, 4]
        let mut s2 = LevelSchema::new();
        s2.push(c, ColumnKind::NodeId);
        chunk.push_level(FactorLevel::expansion(
            SmallVec::from_vec(vec![make_node_ids(&[
                100, 101, 102, 103, 104, 105, 106, 107,
            ])]),
            s2,
            8,
            Arc::from([0u32, 0, 1, 2, 2, 3, 3, 4]),
        ));

        // Row 0 in level 2 -> parent 0 in level 1 -> parent 0 in level 0
        assert_eq!(chunk.resolve_row_at_level(0, 2), 0);
        assert_eq!(chunk.resolve_row_at_level(0, 1), 0);
        assert_eq!(chunk.resolve_row_at_level(0, 0), 0);

        // Row 5 in level 2 -> parent 3 in level 1 -> parent 1 in level 0
        assert_eq!(chunk.resolve_row_at_level(5, 2), 5);
        assert_eq!(chunk.resolve_row_at_level(5, 1), 3);
        assert_eq!(chunk.resolve_row_at_level(5, 0), 1);

        // Row 7 in level 2 -> parent 4 in level 1 -> parent 2 in level 0
        assert_eq!(chunk.resolve_row_at_level(7, 1), 4);
        assert_eq!(chunk.resolve_row_at_level(7, 0), 2);
    }

    #[test]
    fn test_resolve_node_id_through_levels() {
        let a = IStr::new("a");
        let b = IStr::new("b");

        let root = FactorLevel::root(a, make_node_ids(&[100, 200, 300]));
        let mut chunk = FactorizedChunk::from_root(root);

        let mut s1 = LevelSchema::new();
        s1.push(b, ColumnKind::NodeId);
        chunk.push_level(FactorLevel::expansion(
            SmallVec::from_vec(vec![make_node_ids(&[10, 11, 12, 13, 14])]),
            s1,
            5,
            Arc::from([0u32, 0, 1, 1, 2]),
        ));

        // Resolve 'a' from deepest row 3 -> parent 1 -> node 200
        assert_eq!(chunk.resolve_node_id(&a, 3).unwrap(), NodeId(200));
        // Resolve 'b' from deepest row 3 -> same level -> node 13
        assert_eq!(chunk.resolve_node_id(&b, 3).unwrap(), NodeId(13));
    }

    #[test]
    fn test_flatten_single_level() {
        let a = IStr::new("a");
        let root = FactorLevel::root(a, make_node_ids(&[1, 2, 3]));
        let chunk = FactorizedChunk::from_root(root);

        let flat = chunk.flatten();
        assert_eq!(flat.active_len(), 3);
        assert_eq!(flat.schema().slot_of(&a), Some(0));

        let col = flat.node_id_column(&a).unwrap();
        assert_eq!(col.value(0), 1);
        assert_eq!(col.value(1), 2);
        assert_eq!(col.value(2), 3);
    }

    #[test]
    fn test_flatten_two_levels() {
        let a = IStr::new("a");
        let b = IStr::new("b");

        // Root: [1, 2]
        let root = FactorLevel::root(a, make_node_ids(&[1, 2]));
        let mut chunk = FactorizedChunk::from_root(root);

        // Level 1: [10, 11, 12], parents [0, 0, 1]
        // Flat: a=[1,1,2], b=[10,11,12]
        let mut s1 = LevelSchema::new();
        s1.push(b, ColumnKind::NodeId);
        chunk.push_level(FactorLevel::expansion(
            SmallVec::from_vec(vec![make_node_ids(&[10, 11, 12])]),
            s1,
            3,
            Arc::from([0u32, 0, 1]),
        ));

        let flat = chunk.flatten();
        assert_eq!(flat.active_len(), 3);

        let col_a = flat.node_id_column(&a).unwrap();
        assert_eq!(col_a.value(0), 1); // parent of row 0 = 0 -> a[0] = 1
        assert_eq!(col_a.value(1), 1); // parent of row 1 = 0 -> a[0] = 1
        assert_eq!(col_a.value(2), 2); // parent of row 2 = 1 -> a[1] = 2

        let col_b = flat.node_id_column(&b).unwrap();
        assert_eq!(col_b.value(0), 10);
        assert_eq!(col_b.value(1), 11);
        assert_eq!(col_b.value(2), 12);
    }

    #[test]
    fn test_flatten_three_levels() {
        let a = IStr::new("a");
        let b = IStr::new("b");
        let c = IStr::new("c");

        // Root: [1, 2]
        let root = FactorLevel::root(a, make_node_ids(&[1, 2]));
        let mut chunk = FactorizedChunk::from_root(root);

        // Level 1: [10, 11, 12], parents [0, 0, 1]
        let mut s1 = LevelSchema::new();
        s1.push(b, ColumnKind::NodeId);
        chunk.push_level(FactorLevel::expansion(
            SmallVec::from_vec(vec![make_node_ids(&[10, 11, 12])]),
            s1,
            3,
            Arc::from([0u32, 0, 1]),
        ));

        // Level 2: [100, 101, 102, 103], parents [0, 1, 1, 2]
        let mut s2 = LevelSchema::new();
        s2.push(c, ColumnKind::NodeId);
        chunk.push_level(FactorLevel::expansion(
            SmallVec::from_vec(vec![make_node_ids(&[100, 101, 102, 103])]),
            s2,
            4,
            Arc::from([0u32, 1, 1, 2]),
        ));

        let flat = chunk.flatten();
        assert_eq!(flat.active_len(), 4);

        let col_a = flat.node_id_column(&a).unwrap();
        // row 0: c=100, parent=0 in L1 (b=10), parent=0 in L0 (a=1)
        assert_eq!(col_a.value(0), 1);
        // row 1: c=101, parent=1 in L1 (b=11), parent=0 in L0 (a=1)
        assert_eq!(col_a.value(1), 1);
        // row 2: c=102, parent=1 in L1 (b=11), parent=0 in L0 (a=1)
        assert_eq!(col_a.value(2), 1);
        // row 3: c=103, parent=2 in L1 (b=12), parent=1 in L0 (a=2)
        assert_eq!(col_a.value(3), 2);

        let col_b = flat.node_id_column(&b).unwrap();
        assert_eq!(col_b.value(0), 10);
        assert_eq!(col_b.value(1), 11);
        assert_eq!(col_b.value(2), 11);
        assert_eq!(col_b.value(3), 12);

        let col_c = flat.node_id_column(&c).unwrap();
        assert_eq!(col_c.value(0), 100);
        assert_eq!(col_c.value(1), 101);
        assert_eq!(col_c.value(2), 102);
        assert_eq!(col_c.value(3), 103);
    }

    #[test]
    fn test_flatten_with_selection() {
        let a = IStr::new("a");
        let b = IStr::new("b");

        let root = FactorLevel::root(a, make_node_ids(&[1, 2]));
        let mut chunk = FactorizedChunk::from_root(root);

        let mut s1 = LevelSchema::new();
        s1.push(b, ColumnKind::NodeId);
        chunk.push_level(FactorLevel::expansion(
            SmallVec::from_vec(vec![make_node_ids(&[10, 11, 12])]),
            s1,
            3,
            Arc::from([0u32, 0, 1]),
        ));

        // Filter: keep only row 1 (b=11, a=1) and row 2 (b=12, a=2)
        chunk.filter_deepest(&[false, true, true]);

        let flat = chunk.flatten();
        assert_eq!(flat.active_len(), 2);

        let col_a = flat.node_id_column(&a).unwrap();
        assert_eq!(col_a.value(0), 1);
        assert_eq!(col_a.value(1), 2);

        let col_b = flat.node_id_column(&b).unwrap();
        assert_eq!(col_b.value(0), 11);
        assert_eq!(col_b.value(1), 12);
    }

    #[test]
    fn test_flatten_empty() {
        let a = IStr::new("a");
        let root = FactorLevel::root(a, make_node_ids(&[1, 2, 3]));
        let mut chunk = FactorizedChunk::from_root(root);

        // Filter out all rows
        chunk.filter_deepest(&[false, false, false]);

        let flat = chunk.flatten();
        assert_eq!(flat.active_len(), 0);
    }

    /// Regression test: flatten of a multi-level factorized chunk where all
    /// deepest rows are filtered out must produce a DataChunk whose column
    /// count matches its schema. Before the fix, flatten() returned a chunk
    /// with a populated schema but zero columns, causing an index-out-of-bounds
    /// panic on any downstream `.column(slot)` access.
    #[test]
    fn test_flatten_empty_multi_level_columns_match_schema() {
        let a = IStr::new("a");
        let e1 = IStr::new("e1");
        let b = IStr::new("b");

        let root = FactorLevel::root(a, make_node_ids(&[1, 2]));
        let mut chunk = FactorizedChunk::from_root(root);

        let mut s1 = LevelSchema::new();
        s1.push(e1, ColumnKind::EdgeId);
        s1.push(b, ColumnKind::NodeId);
        chunk.push_level(FactorLevel::expansion(
            SmallVec::from_vec(vec![
                make_edge_ids(&[50, 51, 52]),
                make_node_ids(&[10, 11, 12]),
            ]),
            s1,
            3,
            Arc::from([0u32, 0, 1]),
        ));

        // Filter out all deepest rows
        chunk.filter_deepest(&[false, false, false]);

        let flat = chunk.flatten();
        assert_eq!(flat.active_len(), 0);
        // Schema should list all 3 variables
        assert!(flat.schema().slot_of(&a).is_some());
        assert!(flat.schema().slot_of(&e1).is_some());
        assert!(flat.schema().slot_of(&b).is_some());
        // Columns vec must match schema length (the bug: was 0 columns)
        assert_eq!(flat.columns().len(), flat.schema().len());
        // Accessing columns by slot must not panic
        for slot in 0..flat.schema().len() {
            let _col = flat.column(slot);
        }
    }

    #[test]
    fn test_factorized_row_view() {
        let a = IStr::new("a");
        let b = IStr::new("b");

        let root = FactorLevel::root(a, make_node_ids(&[100, 200]));
        let mut chunk = FactorizedChunk::from_root(root);

        let mut s1 = LevelSchema::new();
        s1.push(b, ColumnKind::NodeId);
        chunk.push_level(FactorLevel::expansion(
            SmallVec::from_vec(vec![make_node_ids(&[10, 11, 12])]),
            s1,
            3,
            Arc::from([0u32, 0, 1]),
        ));

        let view = FactorizedRowView::new(&chunk, 2);
        assert_eq!(view.get_node_id(&a).unwrap(), NodeId(200));
        assert_eq!(view.get_node_id(&b).unwrap(), NodeId(12));
    }

    #[test]
    fn test_gather_column_for_active_rows() {
        let a = IStr::new("a");
        let b = IStr::new("b");

        let root = FactorLevel::root(a, make_node_ids(&[100, 200, 300]));
        let mut chunk = FactorizedChunk::from_root(root);

        let mut s1 = LevelSchema::new();
        s1.push(b, ColumnKind::NodeId);
        chunk.push_level(FactorLevel::expansion(
            SmallVec::from_vec(vec![make_node_ids(&[10, 11, 12, 13, 14])]),
            s1,
            5,
            Arc::from([0u32, 0, 1, 1, 2]),
        ));

        // Filter to keep rows 1, 3, 4
        chunk.filter_deepest(&[false, true, false, true, true]);

        // Gather 'a' column for active deepest rows
        let gathered = chunk.gather_column_for_active_rows(0, 0);
        match &gathered {
            Column::NodeIds(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr.value(0), 100); // row 1 -> parent 0 -> a=100
                assert_eq!(arr.value(1), 200); // row 3 -> parent 1 -> a=200
                assert_eq!(arr.value(2), 300); // row 4 -> parent 2 -> a=300
            }
            _ => panic!("expected NodeIds column"),
        }
    }

    #[test]
    fn test_deepest_as_chunk() {
        let a = IStr::new("a");
        let b = IStr::new("b");

        let root = FactorLevel::root(a, make_node_ids(&[1, 2]));
        let mut chunk = FactorizedChunk::from_root(root);

        let mut s1 = LevelSchema::new();
        s1.push(b, ColumnKind::NodeId);
        chunk.push_level(FactorLevel::expansion(
            SmallVec::from_vec(vec![make_node_ids(&[10, 11, 12])]),
            s1,
            3,
            Arc::from([0u32, 0, 1]),
        ));

        let deep_chunk = chunk.deepest_as_chunk();
        assert_eq!(deep_chunk.len(), 3);
        assert_eq!(deep_chunk.active_len(), 3);
        assert!(deep_chunk.schema().slot_of(&b).is_some());
        assert!(deep_chunk.schema().slot_of(&a).is_none()); // a is in root, not deepest
    }

    #[test]
    fn test_flatten_with_edge_columns() {
        let a = IStr::new("a");
        let e1 = IStr::new("e1");
        let b = IStr::new("b");

        let root = FactorLevel::root(a, make_node_ids(&[1, 2]));
        let mut chunk = FactorizedChunk::from_root(root);

        let mut s1 = LevelSchema::new();
        s1.push(e1, ColumnKind::EdgeId);
        s1.push(b, ColumnKind::NodeId);
        chunk.push_level(FactorLevel::expansion(
            SmallVec::from_vec(vec![
                make_edge_ids(&[50, 51, 52]),
                make_node_ids(&[10, 11, 12]),
            ]),
            s1,
            3,
            Arc::from([0u32, 0, 1]),
        ));

        let flat = chunk.flatten();
        assert_eq!(flat.active_len(), 3);
        assert_eq!(flat.schema().slot_of(&a), Some(0));
        assert_eq!(flat.schema().slot_of(&e1), Some(1));
        assert_eq!(flat.schema().slot_of(&b), Some(2));

        let col_e = flat.edge_id_column(&e1).unwrap();
        assert_eq!(col_e.value(0), 50);
        assert_eq!(col_e.value(1), 51);
        assert_eq!(col_e.value(2), 52);
    }
}
