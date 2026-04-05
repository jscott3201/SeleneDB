//! Sorted label storage -- SmallVec-backed for zero heap allocation at <=3 labels.

use smallvec::SmallVec;

use crate::interner::IStr;

/// Sorted label set. Inline for ≤3 labels (covers >95% of nodes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelSet {
    labels: SmallVec<[IStr; 3]>,
}

impl LabelSet {
    /// Create an empty LabelSet.
    pub fn new() -> Self {
        Self {
            labels: SmallVec::new(),
        }
    }

    /// Create from a slice of string labels.
    pub fn from_strs(strs: &[&str]) -> Self {
        let mut labels: SmallVec<[IStr; 3]> = strs.iter().map(|s| IStr::new(s)).collect();
        labels.sort();
        labels.dedup();
        Self { labels }
    }

    /// Check if the set contains a label.
    pub fn contains(&self, label: IStr) -> bool {
        self.labels.contains(&label)
    }

    /// Check if the set contains a label by string.
    /// Uses `try_get` to avoid interning unknown strings.
    pub fn contains_str(&self, label: &str) -> bool {
        IStr::try_get(label).is_some_and(|istr| self.contains(istr))
    }

    /// Insert a label. Returns true if it was newly added.
    pub fn insert(&mut self, label: IStr) -> bool {
        if self.labels.contains(&label) {
            return false;
        }
        let pos = self.labels.partition_point(|l| *l < label);
        self.labels.insert(pos, label);
        true
    }

    /// Remove a label. Returns true if it was present.
    pub fn remove(&mut self, label: IStr) -> bool {
        if let Some(pos) = self.labels.iter().position(|l| *l == label) {
            self.labels.remove(pos);
            true
        } else {
            false
        }
    }

    /// Remove a label by string.
    /// Uses `try_get` to avoid interning unknown strings.
    pub fn remove_str(&mut self, label: &str) -> bool {
        IStr::try_get(label).is_some_and(|istr| self.remove(istr))
    }

    /// Number of labels.
    pub fn len(&self) -> usize {
        self.labels.len()
    }

    /// Is empty?
    pub fn is_empty(&self) -> bool {
        self.labels.is_empty()
    }

    /// Iterate over labels.
    pub fn iter(&self) -> impl Iterator<Item = IStr> + '_ {
        self.labels.iter().copied()
    }
}

impl Default for LabelSet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_set() {
        let s = LabelSet::new();
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn from_strs() {
        let s = LabelSet::from_strs(&["sensor", "temperature"]);
        assert_eq!(s.len(), 2);
        assert!(s.contains_str("sensor"));
        assert!(s.contains_str("temperature"));
    }

    #[test]
    fn from_strs_deduplicates() {
        let s = LabelSet::from_strs(&["sensor", "sensor", "zone"]);
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn insert_and_contains() {
        let mut s = LabelSet::new();
        assert!(s.insert(IStr::new("sensor")));
        assert!(s.contains(IStr::new("sensor")));
        assert!(!s.contains(IStr::new("actuator")));
    }

    #[test]
    fn insert_duplicate_returns_false() {
        let mut s = LabelSet::new();
        assert!(s.insert(IStr::new("sensor")));
        assert!(!s.insert(IStr::new("sensor")));
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn remove() {
        let mut s = LabelSet::from_strs(&["sensor", "zone"]);
        assert!(s.remove_str("sensor"));
        assert!(!s.contains_str("sensor"));
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn remove_nonexistent() {
        let mut s = LabelSet::new();
        assert!(!s.remove_str("nope"));
    }

    #[test]
    fn iteration() {
        let s = LabelSet::from_strs(&["b", "a", "c"]);
        let labels: Vec<IStr> = s.iter().collect();
        assert_eq!(labels.len(), 3);
        for window in labels.windows(2) {
            assert!(window[0] <= window[1]);
        }
    }

    #[test]
    fn contains_str_does_not_intern() {
        let query = "nonexistent_label_interner_test_xyz_99";
        let ls = LabelSet::new();
        assert!(!ls.contains_str(query));
        assert!(
            IStr::try_get(query).is_none(),
            "contains_str should not intern unknown labels"
        );
    }

    #[test]
    fn remove_str_does_not_intern() {
        let query = "nonexistent_label_remove_test_xyz_99";
        let mut ls = LabelSet::new();
        assert!(!ls.remove_str(query));
        assert!(
            IStr::try_get(query).is_none(),
            "remove_str should not intern unknown labels"
        );
    }
}
