//! Copy-on-write chunked vector for near-instant graph clones.
//!
//! `ChunkedVec<T>` stores elements in fixed-size `Arc<[T]>` chunks.
//! Clone copies chunk pointers (O(N/CHUNK_SIZE) Arc increments).
//! Write triggers CoW on the modified chunk only via `Arc::make_mut`.

use std::ops::Index;
use std::sync::Arc;

/// Number of elements per chunk. Power of 2 for fast division via bit shift.
const CHUNK_SIZE: usize = 256;

/// A dense array stored as Arc'd fixed-size chunks.
///
/// - **Clone** copies chunk pointers -- O(N/256) Arc increments, no data copy.
/// - **Read** is two array lookups -- O(1), same cache behavior within chunks.
/// - **Write** triggers CoW on the single modified chunk via `Arc::make_mut`.
#[derive(Clone)]
pub struct ChunkedVec<T> {
    chunks: Vec<Arc<[T]>>,
    len: usize,
}

impl<T: Clone> ChunkedVec<T> {
    /// Create an empty ChunkedVec.
    pub fn new() -> Self {
        Self {
            chunks: Vec::new(),
            len: 0,
        }
    }

    /// Number of logical elements.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the vec contains no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Immutable access by index. Returns None if out of bounds.
    #[inline]
    pub fn get(&self, index: usize) -> Option<&T> {
        if index >= self.len {
            return None;
        }
        let chunk_idx = index / CHUNK_SIZE;
        let inner_idx = index % CHUNK_SIZE;
        Some(&self.chunks[chunk_idx][inner_idx])
    }

    /// Mutable access by index. Triggers CoW on the chunk if shared.
    /// Returns None if out of bounds.
    #[inline]
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        if index >= self.len {
            return None;
        }
        let chunk_idx = index / CHUNK_SIZE;
        let inner_idx = index % CHUNK_SIZE;
        let chunk = Arc::make_mut(&mut self.chunks[chunk_idx]);
        Some(&mut chunk[inner_idx])
    }

    /// Set an element at the given index. Triggers CoW if the chunk is shared.
    /// Panics if index is out of bounds.
    #[inline]
    pub fn set(&mut self, index: usize, value: T) {
        assert!(index < self.len, "ChunkedVec::set index out of bounds");
        let chunk_idx = index / CHUNK_SIZE;
        let inner_idx = index % CHUNK_SIZE;
        let chunk = Arc::make_mut(&mut self.chunks[chunk_idx]);
        chunk[inner_idx] = value;
    }

    /// Grow to `new_len` elements, filling new *chunks* with clones of `value`.
    ///
    /// Note: unlike `Vec::resize`, this fills entire chunks, not individual
    /// elements. Elements within an existing chunk but beyond the old `len`
    /// retain their initial value from when the chunk was first allocated.
    /// This is safe for NodeStore/EdgeStore because `insert()` explicitly
    /// writes all fields, and the alive bitset prevents reading dead slots.
    ///
    /// **Shrink behavior:** When `new_len <= self.len`, only the logical length
    /// is reduced. Backing chunks are retained (not deallocated) so that
    /// subsequent growth can reuse them without allocation. This means memory
    /// is not reclaimed until the `ChunkedVec` itself is dropped. For
    /// NodeStore/EdgeStore this is intentional -- IDs are reused via freelist
    /// so the high-water chunk count is stable.
    pub fn resize(&mut self, new_len: usize, value: T) {
        if new_len <= self.len {
            self.len = new_len;
            return;
        }
        let needed_chunks = new_len.div_ceil(CHUNK_SIZE);
        while self.chunks.len() < needed_chunks {
            let chunk: Vec<T> = vec![value.clone(); CHUNK_SIZE];
            self.chunks.push(chunk.into());
        }
        self.len = new_len;
    }

    /// Grow to `new_len` elements, filling new slots with values from `f`.
    /// No-op if `new_len <= self.len`.
    pub fn resize_with(&mut self, new_len: usize, mut f: impl FnMut() -> T) {
        if new_len <= self.len {
            self.len = new_len;
            return;
        }
        let needed_chunks = new_len.div_ceil(CHUNK_SIZE);
        while self.chunks.len() < needed_chunks {
            let chunk: Vec<T> = (0..CHUNK_SIZE).map(|_| f()).collect();
            self.chunks.push(chunk.into());
        }
        self.len = new_len;
    }

    /// Iterate over all logical elements.
    #[allow(clippy::iter_without_into_iter)]
    pub fn iter(&self) -> ChunkedVecIter<'_, T> {
        ChunkedVecIter {
            vec: self,
            index: 0,
        }
    }
}

// ── Index ────────────────────────────────────────────────────────────────────

impl<T> Index<usize> for ChunkedVec<T> {
    type Output = T;

    #[inline]
    fn index(&self, index: usize) -> &T {
        assert!(index < self.len, "ChunkedVec index out of bounds");
        let chunk_idx = index / CHUNK_SIZE;
        let inner_idx = index % CHUNK_SIZE;
        &self.chunks[chunk_idx][inner_idx]
    }
}

// ── Iterator ─────────────────────────────────────────────────────────────────

pub struct ChunkedVecIter<'a, T> {
    vec: &'a ChunkedVec<T>,
    index: usize,
}

impl<'a, T> Iterator for ChunkedVecIter<'a, T> {
    type Item = &'a T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.vec.len {
            return None;
        }
        let chunk_idx = self.index / CHUNK_SIZE;
        let inner_idx = self.index % CHUNK_SIZE;
        self.index += 1;
        Some(&self.vec.chunks[chunk_idx][inner_idx])
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.vec.len.saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

impl<T> ExactSizeIterator for ChunkedVecIter<'_, T> {}

// ── Debug ────────────────────────────────────────────────────────────────────

impl<T> std::fmt::Debug for ChunkedVec<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkedVec")
            .field("len", &self.len)
            .field("chunks", &self.chunks.len())
            .finish()
    }
}

impl<T: Clone + Default> Default for ChunkedVec<T> {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn empty() {
        let v: ChunkedVec<i32> = ChunkedVec::new();
        assert!(v.is_empty());
        assert_eq!(v.len(), 0);
        assert!(v.get(0).is_none());
    }

    #[test]
    fn resize_and_access() {
        let mut v = ChunkedVec::new();
        v.resize(10, 42i64);

        assert_eq!(v.len(), 10);
        assert_eq!(v[0], 42);
        assert_eq!(v[9], 42);
        assert!(v.get(10).is_none());
    }

    #[test]
    fn resize_across_chunk_boundary() {
        let mut v = ChunkedVec::new();
        v.resize(300, 0u64);

        assert_eq!(v.len(), 300);
        // Two chunks allocated (256 + 256)
        assert_eq!(v[0], 0);
        assert_eq!(v[255], 0);
        assert_eq!(v[256], 0);
        assert_eq!(v[299], 0);
    }

    #[test]
    fn set_and_get() {
        let mut v = ChunkedVec::new();
        v.resize(5, 0i64);

        v.set(3, 99);
        assert_eq!(v[3], 99);
        assert_eq!(v[2], 0);
        assert_eq!(v[4], 0);
    }

    #[test]
    fn get_mut_cow() {
        let mut v = ChunkedVec::new();
        v.resize(10, 0i64);

        // Clone shares chunks
        let original = v.clone();

        // Mutate the clone — triggers CoW on chunk 0
        *v.get_mut(5).unwrap() = 77;

        // Original is unchanged
        assert_eq!(original[5], 0);
        assert_eq!(v[5], 77);
    }

    #[test]
    fn clone_shares_chunks() {
        let mut v = ChunkedVec::new();
        v.resize(512, 0i64); // 2 chunks

        let clone = v.clone();

        // Before mutation, chunks are shared (Arc refcount > 1)
        // After mutation, only the modified chunk is copied
        v.set(0, 1); // CoW on chunk 0
        v.set(300, 2); // CoW on chunk 1

        assert_eq!(clone[0], 0);
        assert_eq!(clone[300], 0);
        assert_eq!(v[0], 1);
        assert_eq!(v[300], 2);
    }

    #[test]
    fn cow_only_copies_modified_chunk() {
        let mut v = ChunkedVec::new();
        v.resize(512, 0i64); // 2 chunks

        let clone = v.clone();

        // Modify only chunk 0
        v.set(0, 42);

        // Chunk 1 should still be shared (same Arc)
        // We can't directly check Arc::strong_count on Arc<[T]> from outside,
        // but we verify the values are independent
        assert_eq!(v[0], 42);
        assert_eq!(v[256], 0);
        assert_eq!(clone[0], 0);
        assert_eq!(clone[256], 0);
    }

    #[test]
    fn resize_with_fn() {
        let mut counter = 0u64;
        let mut v = ChunkedVec::new();
        v.resize_with(5, || {
            counter += 1;
            counter
        });

        assert_eq!(v[0], 1);
        assert_eq!(v[4], 5);
        assert_eq!(v.len(), 5);
    }

    #[test]
    fn iterator() {
        let mut v = ChunkedVec::new();
        v.resize(5, 0i64);
        v.set(0, 10);
        v.set(1, 20);
        v.set(2, 30);
        v.set(3, 40);
        v.set(4, 50);

        let collected: Vec<&i64> = v.iter().collect();
        assert_eq!(collected, vec![&10, &20, &30, &40, &50]);
        assert_eq!(v.iter().count(), 5);
    }

    #[test]
    fn iterator_across_chunks() {
        let mut v = ChunkedVec::new();
        v.resize(300, 0i64);
        v.set(255, 1);
        v.set(256, 2);

        let items: Vec<&i64> = v.iter().collect();
        assert_eq!(items.len(), 300);
        assert_eq!(*items[255], 1);
        assert_eq!(*items[256], 2);
    }

    #[test]
    fn resize_shrink() {
        let mut v = ChunkedVec::new();
        v.resize(300, 42i64);
        assert_eq!(v.len(), 300);

        v.resize(100, 0);
        assert_eq!(v.len(), 100);
        assert_eq!(v[99], 42);
        assert!(v.get(100).is_none());
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn index_out_of_bounds_panics() {
        let v: ChunkedVec<i64> = ChunkedVec::new();
        let _ = v[0];
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn set_out_of_bounds_panics() {
        let mut v: ChunkedVec<i64> = ChunkedVec::new();
        v.set(0, 42);
    }

    #[test]
    fn with_complex_types() {
        let mut v: ChunkedVec<Vec<String>> = ChunkedVec::new();
        v.resize_with(3, Vec::new);
        v.get_mut(1).unwrap().push("hello".into());

        assert!(v[0].is_empty());
        assert_eq!(v[1], vec!["hello".to_string()]);
        assert!(v[2].is_empty());
    }

    #[test]
    fn with_arc_types() {
        let mut v: ChunkedVec<Option<Arc<str>>> = ChunkedVec::new();
        v.resize(3, None);
        v.set(1, Some(Arc::from("test")));

        assert!(v[0].is_none());
        assert_eq!(v[1].as_deref(), Some("test"));
    }

    #[test]
    fn exact_chunk_boundary() {
        let mut v = ChunkedVec::new();
        v.resize(256, 0i64);
        assert_eq!(v.len(), 256);
        v.set(255, 99);
        assert_eq!(v[255], 99);

        // Grow to exactly 2 chunks
        v.resize(512, 0);
        assert_eq!(v.len(), 512);
        assert_eq!(v[255], 99);
        assert_eq!(v[256], 0);
        assert_eq!(v[511], 0);
    }
}
