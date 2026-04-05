//! Selection vector for tracking active rows in columnar batches.
//!
//! `SelectionVector` avoids data movement by recording which rows in a
//! `DataChunk` are active. Filters update the selection instead of
//! copying columns, achieving zero-copy filtering.

use arrow::array::{Array, BooleanArray};

// ---------------------------------------------------------------------------
// SelectionVector
// ---------------------------------------------------------------------------

/// Tracks which rows in a `DataChunk` are active.
///
/// `None` means all rows are active (dense). `Some(indices)` holds sorted
/// indices of active rows. Filters update the selection vector instead of
/// moving data, so filtering is zero-copy.
#[derive(Debug, Clone)]
pub(crate) struct SelectionVector(Option<Vec<u32>>);

#[allow(dead_code)]
impl SelectionVector {
    /// All rows active (dense). `len` is the total physical row count.
    pub fn all(len: usize) -> Self {
        if len == 0 {
            Self(Some(Vec::new()))
        } else {
            Self(None)
        }
    }

    /// No rows active (empty result).
    pub fn none() -> Self {
        Self(Some(Vec::new()))
    }

    /// Create from an explicit list of active row indices.
    pub fn from_indices(indices: Vec<u32>) -> Self {
        Self(Some(indices))
    }

    /// Number of active rows. Requires `physical_len` when dense (None).
    pub fn active_len(&self, physical_len: usize) -> usize {
        match &self.0 {
            None => physical_len,
            Some(indices) => indices.len(),
        }
    }

    /// True if this is the dense (all-active) representation.
    pub fn is_dense(&self) -> bool {
        self.0.is_none()
    }

    /// Iterator over active row indices.
    pub fn active_indices(&self, physical_len: usize) -> SelectionIter<'_> {
        match &self.0 {
            None => SelectionIter::Dense {
                current: 0,
                len: physical_len,
            },
            Some(indices) => SelectionIter::Sparse {
                inner: indices.iter(),
            },
        }
    }

    /// Apply a boolean mask: keep only rows where `mask[active_idx]` is true.
    ///
    /// The mask is indexed by position within the active set (not physical row).
    /// For a dense selection, `mask[i]` corresponds to physical row `i`.
    /// For a sparse selection, `mask[j]` corresponds to `indices[j]`.
    pub fn apply_bool_mask(&mut self, mask: &[bool], physical_len: usize) {
        match &self.0 {
            None => {
                let indices: Vec<u32> = (0..physical_len as u32)
                    .zip(mask.iter())
                    .filter_map(|(i, &keep)| if keep { Some(i) } else { None })
                    .collect();
                self.0 = Some(indices);
            }
            Some(existing) => {
                let indices: Vec<u32> = existing
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(&i, &keep)| if keep { Some(i) } else { None })
                    .collect();
                self.0 = Some(indices);
            }
        }
    }

    /// Apply a BooleanArray filter from `eval_vec`: TRUE keeps the row,
    /// FALSE/NULL removes it. Works like `apply_bool_mask` but reads
    /// directly from an Arrow BooleanArray without intermediate `Vec<bool>`.
    pub fn apply_bool_column(&mut self, col: &BooleanArray, physical_len: usize) {
        match &self.0 {
            None => {
                // Dense: filter all physical rows
                let indices: Vec<u32> = (0..physical_len as u32)
                    .filter(|&i| !col.is_null(i as usize) && col.value(i as usize))
                    .collect();
                self.0 = Some(indices);
            }
            Some(existing) => {
                // Sparse: zip existing active indices with corresponding array values.
                // The BooleanArray has physical_len rows, so we index by the
                // physical row index stored in the existing selection.
                let indices: Vec<u32> = existing
                    .iter()
                    .filter(|&&i| !col.is_null(i as usize) && col.value(i as usize))
                    .copied()
                    .collect();
                self.0 = Some(indices);
            }
        }
    }

    /// Keep only the first `n` active rows.
    pub fn truncate(&mut self, n: usize, physical_len: usize) {
        match &mut self.0 {
            None => {
                if n < physical_len {
                    self.0 = Some((0..n as u32).collect());
                }
            }
            Some(indices) => {
                indices.truncate(n);
            }
        }
    }

    /// Skip the first `n` active rows.
    pub fn skip(&mut self, n: usize, physical_len: usize) {
        match &mut self.0 {
            None => {
                if n > 0 {
                    let start = n.min(physical_len) as u32;
                    self.0 = Some((start..physical_len as u32).collect());
                }
            }
            Some(indices) => {
                if n >= indices.len() {
                    indices.clear();
                } else {
                    *indices = indices[n..].to_vec();
                }
            }
        }
    }

    /// Borrow the underlying indices (None if dense).
    pub fn indices(&self) -> Option<&[u32]> {
        self.0.as_deref()
    }
}

/// Iterator over active row indices in a `SelectionVector`.
pub(crate) enum SelectionIter<'a> {
    Dense { current: usize, len: usize },
    Sparse { inner: std::slice::Iter<'a, u32> },
}

impl Iterator for SelectionIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dense { current, len } => {
                if *current < *len {
                    let idx = *current;
                    *current += 1;
                    Some(idx)
                } else {
                    None
                }
            }
            Self::Sparse { inner } => inner.next().map(|&i| i as usize),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Dense { current, len } => {
                let remaining = len.saturating_sub(*current);
                (remaining, Some(remaining))
            }
            Self::Sparse { inner } => inner.size_hint(),
        }
    }
}

impl ExactSizeIterator for SelectionIter<'_> {}
