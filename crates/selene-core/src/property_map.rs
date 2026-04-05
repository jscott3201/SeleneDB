//! Sorted property storage -- SmallVec-backed for zero heap allocation at <=6 properties.
//!
//! Two variants:
//! - **Standard**: sorted `SmallVec<[(IStr, Value); 6]>`, used for schema-less nodes.
//! - **Compact**: shared `Arc<[IStr]>` key array + parallel `SmallVec<[Option<Value>; 6]>`,
//!   used for schema-conformant nodes. Saves 8 bytes per property per node.

use std::sync::Arc;

use smallvec::SmallVec;

use crate::interner::IStr;
use crate::value::Value;

/// Sorted key-value property storage. Inline for ≤6 entries.
///
/// Standard variant: each node stores its own key-value pairs.
/// Compact variant: nodes of the same type share a key array (one `Arc<[IStr]>` per type).
#[derive(Debug, Clone, PartialEq)]
pub enum PropertyMap {
    /// Standard sorted key-value pairs. Used for schema-less nodes.
    Standard(SmallVec<[(IStr, Value); 6]>),
    /// Compact: shared key array + parallel values. Used for schema-conformant nodes.
    /// `None` values represent removed/absent properties.
    /// `count` tracks the number of non-None values for O(1) `len()`.
    Compact {
        keys: Arc<[IStr]>,
        values: SmallVec<[Option<Value>; 6]>,
        count: usize,
    },
}

impl PropertyMap {
    /// Create an empty PropertyMap (Standard variant).
    pub fn new() -> Self {
        Self::Standard(SmallVec::new())
    }

    /// Create a compact PropertyMap with shared keys and initial values.
    /// Keys must be sorted. Values parallel to keys (None = absent).
    pub fn compact(keys: Arc<[IStr]>, values: SmallVec<[Option<Value>; 6]>) -> Self {
        debug_assert!(
            keys.windows(2).all(|w| w[0] <= w[1]),
            "compact keys must be sorted"
        );
        debug_assert_eq!(keys.len(), values.len(), "keys and values must match");
        let count = values.iter().filter(|v| v.is_some()).count();
        Self::Compact {
            keys,
            values,
            count,
        }
    }

    /// Build a PropertyMap from a list of key-value pairs.
    ///
    /// Duplicate keys: the first occurrence wins (stable sort + dedup by key).
    pub fn from_pairs(iter: impl IntoIterator<Item = (IStr, Value)>) -> Self {
        let mut entries: SmallVec<[(IStr, Value); 6]> = iter.into_iter().collect();
        entries.sort_by_key(|(k, _)| *k);
        entries.dedup_by_key(|(k, _)| *k);
        Self::Standard(entries)
    }

    /// Get a value by key.
    pub fn get(&self, key: IStr) -> Option<&Value> {
        match self {
            Self::Standard(entries) => entries
                .binary_search_by_key(&key, |(k, _)| *k)
                .ok()
                .map(|i| &entries[i].1),
            Self::Compact { keys, values, .. } => {
                // Linear scan -- keys are typically 3-8 entries, optimal at this size
                keys.iter()
                    .position(|k| *k == key)
                    .and_then(|i| values[i].as_ref())
            }
        }
    }

    /// Get a value by string key.
    /// Uses `try_get` to avoid interning unknown strings.
    pub fn get_by_str(&self, key: &str) -> Option<&Value> {
        IStr::try_get(key).and_then(|k| self.get(k))
    }

    /// Insert or update a key-value pair. Returns the old value if replaced.
    ///
    /// For Compact: if the key is in the schema, updates the slot.
    /// If the key is NOT in the schema, promotes to Standard.
    pub fn insert(&mut self, key: IStr, value: Value) -> Option<Value> {
        match self {
            Self::Standard(entries) => match entries.binary_search_by_key(&key, |(k, _)| *k) {
                Ok(i) => {
                    let old = std::mem::replace(&mut entries[i].1, value);
                    Some(old)
                }
                Err(i) => {
                    entries.insert(i, (key, value));
                    None
                }
            },
            Self::Compact {
                keys,
                values,
                count,
            } => {
                // Check if key is in the schema
                if let Some(i) = keys.iter().position(|k| *k == key) {
                    let old = values[i].take();
                    if old.is_none() {
                        *count += 1;
                    }
                    values[i] = Some(value);
                    old
                } else {
                    // Key not in schema -- promote to Standard
                    let mut entries: SmallVec<[(IStr, Value); 6]> = SmallVec::new();
                    for (i, k) in keys.iter().enumerate() {
                        if let Some(v) = values[i].clone() {
                            entries.push((*k, v));
                        }
                    }
                    // Insert the new key in sorted position
                    match entries.binary_search_by_key(&key, |(k, _)| *k) {
                        Ok(i) => {
                            let old = std::mem::replace(&mut entries[i].1, value);
                            *self = Self::Standard(entries);
                            Some(old)
                        }
                        Err(i) => {
                            entries.insert(i, (key, value));
                            *self = Self::Standard(entries);
                            None
                        }
                    }
                }
            }
        }
    }

    /// Remove a key. Returns the old value if it existed.
    ///
    /// For Compact: sets the slot to None (does not promote to Standard).
    pub fn remove(&mut self, key: IStr) -> Option<Value> {
        match self {
            Self::Standard(entries) => match entries.binary_search_by_key(&key, |(k, _)| *k) {
                Ok(i) => Some(entries.remove(i).1),
                Err(_) => None,
            },
            Self::Compact {
                keys,
                values,
                count,
            } => {
                if let Some(i) = keys.iter().position(|k| *k == key) {
                    let old = values[i].take();
                    if old.is_some() {
                        *count -= 1;
                    }
                    old
                } else {
                    None
                }
            }
        }
    }

    /// Remove a key by string. Returns None if the key was never interned.
    pub fn remove_by_str(&mut self, key: &str) -> Option<Value> {
        IStr::try_get(key).and_then(|k| self.remove(k))
    }

    /// Number of properties (non-None values). O(1) for both variants.
    pub fn len(&self) -> usize {
        match self {
            Self::Standard(entries) => entries.len(),
            Self::Compact { count, .. } => *count,
        }
    }

    /// Is empty? O(1) for both variants.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate over all (key, value) pairs in sorted order.
    /// Compact variant skips None slots.
    #[allow(clippy::iter_without_into_iter)]
    pub fn iter(&self) -> PropertyMapIter<'_> {
        match self {
            Self::Standard(entries) => PropertyMapIter::Standard(entries.iter()),
            Self::Compact { keys, values, .. } => PropertyMapIter::Compact {
                keys: keys.iter(),
                values: values.iter(),
            },
        }
    }

    /// Check if a key exists (and has a non-None value).
    pub fn contains_key(&self, key: IStr) -> bool {
        self.get(key).is_some()
    }
}

/// Iterator over PropertyMap entries, handling both Standard and Compact variants.
pub enum PropertyMapIter<'a> {
    Standard(std::slice::Iter<'a, (IStr, Value)>),
    Compact {
        keys: std::slice::Iter<'a, IStr>,
        values: std::slice::Iter<'a, Option<Value>>,
    },
}

impl<'a> Iterator for PropertyMapIter<'a> {
    type Item = (&'a IStr, &'a Value);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Standard(iter) => iter.next().map(|(k, v)| (k, v)),
            Self::Compact { keys, values } => {
                // Skip None slots
                loop {
                    let k = keys.next()?;
                    let v = values.next()?;
                    if let Some(val) = v {
                        return Some((k, val));
                    }
                }
            }
        }
    }
}

impl Default for PropertyMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;

    #[test]
    fn empty_map() {
        let m = PropertyMap::new();
        assert!(m.is_empty());
        assert_eq!(m.len(), 0);
    }

    #[test]
    fn insert_and_get() {
        let mut m = PropertyMap::new();
        let key = IStr::new("unit");
        m.insert(key, Value::str("°F"));
        assert_eq!(m.len(), 1);
        assert_eq!(m.get(key), Some(&Value::str("°F")));
    }

    #[test]
    fn get_by_str() {
        let mut m = PropertyMap::new();
        m.insert(IStr::new("name"), Value::str("HQ"));
        assert_eq!(m.get_by_str("name"), Some(&Value::str("HQ")));
        assert_eq!(m.get_by_str("missing"), None);
    }

    #[test]
    fn insert_replaces() {
        let mut m = PropertyMap::new();
        let key = IStr::new("value");
        m.insert(key, Value::Int(1));
        let old = m.insert(key, Value::Int(2));
        assert_eq!(old, Some(Value::Int(1)));
        assert_eq!(m.get(key), Some(&Value::Int(2)));
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn remove() {
        let mut m = PropertyMap::new();
        let key = IStr::new("temp");
        m.insert(key, Value::Float(72.5));
        let removed = m.remove(key);
        assert_eq!(removed, Some(Value::Float(72.5)));
        assert!(m.is_empty());
    }

    #[test]
    fn remove_nonexistent() {
        let mut m = PropertyMap::new();
        assert_eq!(m.remove(IStr::new("nope")), None);
    }

    #[test]
    fn iteration_is_sorted() {
        let mut m = PropertyMap::new();
        m.insert(IStr::new("z_prop"), Value::Int(3));
        m.insert(IStr::new("a_prop"), Value::Int(1));
        m.insert(IStr::new("m_prop"), Value::Int(2));

        let keys: Vec<IStr> = m.iter().map(|(k, _)| *k).collect();
        for window in keys.windows(2) {
            assert!(window[0] <= window[1]);
        }
    }

    #[test]
    fn from_pairs_sorts() {
        let m = PropertyMap::from_pairs(vec![
            (IStr::new("b"), Value::Int(2)),
            (IStr::new("a"), Value::Int(1)),
        ]);
        assert_eq!(m.len(), 2);
        assert_eq!(m.get(IStr::new("a")), Some(&Value::Int(1)));
        assert_eq!(m.get(IStr::new("b")), Some(&Value::Int(2)));
    }

    #[test]
    fn from_pairs_deduplicates() {
        let m = PropertyMap::from_pairs(vec![
            (IStr::new("x"), Value::Int(1)),
            (IStr::new("x"), Value::Int(2)),
            (IStr::new("y"), Value::Int(3)),
        ]);
        assert_eq!(m.len(), 2);
        // dedup_by_key keeps the first of each run after sort
        assert!(m.get(IStr::new("x")).is_some());
        assert_eq!(m.get(IStr::new("y")), Some(&Value::Int(3)));
        // Verify binary_search works correctly
        assert!(m.contains_key(IStr::new("x")));
        assert!(m.contains_key(IStr::new("y")));
        assert!(!m.contains_key(IStr::new("z")));
    }

    #[test]
    fn contains_key() {
        let mut m = PropertyMap::new();
        m.insert(IStr::new("exists"), Value::Null);
        assert!(m.contains_key(IStr::new("exists")));
        assert!(!m.contains_key(IStr::new("nope")));
    }

    // ── Compact variant tests ────────────────────────────────────

    #[test]
    fn compact_map_get() {
        let keys: Arc<[IStr]> = Arc::from(vec![IStr::new("a"), IStr::new("b"), IStr::new("c")]);
        let values = smallvec::smallvec![
            Some(Value::Int(1)),
            Some(Value::Int(2)),
            Some(Value::Int(3))
        ];
        let m = PropertyMap::compact(keys, values);
        assert_eq!(m.get(IStr::new("a")), Some(&Value::Int(1)));
        assert_eq!(m.get(IStr::new("b")), Some(&Value::Int(2)));
        assert_eq!(m.get(IStr::new("c")), Some(&Value::Int(3)));
        assert_eq!(m.get(IStr::new("d")), None);
        assert_eq!(m.len(), 3);
    }

    #[test]
    fn compact_map_insert_existing_key() {
        let keys: Arc<[IStr]> = Arc::from(vec![IStr::new("x"), IStr::new("y")]);
        let values = smallvec::smallvec![Some(Value::Int(1)), Some(Value::Int(2))];
        let mut m = PropertyMap::compact(keys, values);
        let old = m.insert(IStr::new("x"), Value::Int(99));
        assert_eq!(old, Some(Value::Int(1)));
        assert_eq!(m.get(IStr::new("x")), Some(&Value::Int(99)));
    }

    #[test]
    fn compact_map_insert_new_key_promotes() {
        let keys: Arc<[IStr]> = Arc::from(vec![IStr::new("a")]);
        let values = smallvec::smallvec![Some(Value::Int(1))];
        let mut m = PropertyMap::compact(keys, values);
        m.insert(IStr::new("z"), Value::Int(99));
        assert_eq!(m.get(IStr::new("a")), Some(&Value::Int(1)));
        assert_eq!(m.get(IStr::new("z")), Some(&Value::Int(99)));
        assert_eq!(m.len(), 2);
        // Should now be Standard variant
        assert!(matches!(m, PropertyMap::Standard(_)));
    }

    #[test]
    fn compact_map_remove_sets_none() {
        let keys: Arc<[IStr]> = Arc::from(vec![IStr::new("a"), IStr::new("b")]);
        let values = smallvec::smallvec![Some(Value::Int(1)), Some(Value::Int(2))];
        let mut m = PropertyMap::compact(keys, values);
        let removed = m.remove(IStr::new("a"));
        assert_eq!(removed, Some(Value::Int(1)));
        assert_eq!(m.get(IStr::new("a")), None);
        assert_eq!(m.len(), 1); // len counts non-None only
    }

    #[test]
    fn compact_map_iter_skips_none() {
        let keys: Arc<[IStr]> = Arc::from(vec![IStr::new("a"), IStr::new("b"), IStr::new("c")]);
        let values = smallvec::smallvec![Some(Value::Int(1)), None, Some(Value::Int(3))];
        let m = PropertyMap::compact(keys, values);
        let pairs: Vec<_> = m.iter().collect();
        assert_eq!(pairs.len(), 2); // skips None slot for "b"
    }

    #[test]
    fn get_by_str_does_not_intern() {
        let query = "nonexistent_key_interner_test_xyz_99";
        let pm = PropertyMap::new();
        assert!(pm.get_by_str(query).is_none());
        assert!(
            IStr::try_get(query).is_none(),
            "get_by_str should not intern unknown keys"
        );
    }
}
