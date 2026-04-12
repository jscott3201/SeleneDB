//! Convenience factory functions for building test data.
//!
//! Shorter aliases for commonly-used Selene types in test code.

use selene_core::{IStr, LabelSet, PropertyMap, Value};

/// Build a `LabelSet` from string slices.
pub fn labels(names: &[&str]) -> LabelSet {
    LabelSet::from_strs(names)
}

/// Build a `PropertyMap` from `(&str, Value)` pairs.
pub fn props(pairs: &[(&str, Value)]) -> PropertyMap {
    PropertyMap::from_pairs(pairs.iter().map(|(k, v)| (IStr::new(k), v.clone())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_builds_label_set() {
        let ls = labels(&["sensor", "temperature"]);
        assert!(ls.contains(IStr::new("sensor")));
        assert!(ls.contains(IStr::new("temperature")));
        assert!(!ls.contains(IStr::new("actuator")));
    }

    #[test]
    fn labels_empty() {
        let ls = labels(&[]);
        assert_eq!(ls.len(), 0);
    }

    #[test]
    fn props_builds_property_map() {
        let pm = props(&[
            ("name", Value::str("AHU-1")),
            ("capacity", Value::Float(500.0)),
        ]);
        assert_eq!(pm.get(IStr::new("name")), Some(&Value::str("AHU-1")));
        assert_eq!(pm.get(IStr::new("capacity")), Some(&Value::Float(500.0)));
    }

    #[test]
    fn props_empty() {
        let pm = props(&[]);
        assert!(pm.is_empty());
    }
}
