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
