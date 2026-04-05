//! Shared bitset helpers for column stores.
//!
//! Both `NodeStore` and `EdgeStore` use a `Vec<u64>` alive mask.
//! These helpers centralise the bit manipulation and iteration.

/// Set the bit at position `idx`.
#[inline]
pub(crate) fn bit_set(words: &mut [u64], idx: usize) {
    debug_assert!(
        idx / 64 < words.len(),
        "bit_set: word index {} out of bounds (len {})",
        idx / 64,
        words.len()
    );
    words[idx / 64] |= 1u64 << (idx % 64);
}

/// Clear the bit at position `idx`.
#[inline]
pub(crate) fn bit_clear(words: &mut [u64], idx: usize) {
    debug_assert!(
        idx / 64 < words.len(),
        "bit_clear: word index {} out of bounds (len {})",
        idx / 64,
        words.len()
    );
    words[idx / 64] &= !(1u64 << (idx % 64));
}

/// Test whether the bit at position `idx` is set.
#[inline]
pub(crate) fn bit_test(words: &[u64], idx: usize) -> bool {
    debug_assert!(
        idx / 64 < words.len(),
        "bit_test: word index {} out of bounds (len {})",
        idx / 64,
        words.len()
    );
    (words[idx / 64] >> (idx % 64)) & 1 != 0
}

/// Build a `RoaringBitmap` from a `Vec<u64>` alive mask.
/// O(N/64) where N = max slot index. Uses word-at-a-time insertion.
pub(crate) fn alive_to_roaring(words: &[u64]) -> roaring::RoaringBitmap {
    let mut bitmap = roaring::RoaringBitmap::new();
    for (word_idx, &word) in words.iter().enumerate() {
        let mut w = word;
        while w != 0 {
            let bit = w.trailing_zeros() as usize;
            bitmap.insert((word_idx * 64 + bit) as u32);
            w &= w - 1;
        }
    }
    bitmap
}

/// Iterator over set bits in a single u64 word, yielding raw indices
/// (base + bit position within the word).
pub(crate) struct BitIter {
    pub(crate) word: u64,
    pub(crate) base: u64,
}

impl Iterator for BitIter {
    type Item = u64;
    fn next(&mut self) -> Option<u64> {
        if self.word == 0 {
            return None;
        }
        let bit = u64::from(self.word.trailing_zeros());
        self.word &= self.word - 1;
        Some(self.base + bit)
    }
}
