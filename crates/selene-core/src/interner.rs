//! Global string interner -- all labels and property keys go through here.
//!
//! `IStr` is a Copy, Eq handle that resolves to `&str` via the global interner.
//! Equality is a single integer comparison (no string compare).

use lasso::{Spur, ThreadedRodeo};
use std::sync::OnceLock;

static GLOBAL_INTERNER: OnceLock<ThreadedRodeo> = OnceLock::new();

/// Get or initialize the global interner.
pub fn interner() -> &'static ThreadedRodeo {
    GLOBAL_INTERNER.get_or_init(ThreadedRodeo::new)
}

/// Maximum number of interned strings to prevent DoS via unique property keys.
const MAX_INTERNED_STRINGS: usize = 1_000_000;

/// Attempt to intern a string, returning None if the interner is at capacity.
/// Use for user-supplied input (HTTP property keys). Internal code uses `IStr::new()`.
pub fn try_intern(s: &str) -> Option<IStr> {
    let rodeo = interner();
    if rodeo.len() >= MAX_INTERNED_STRINGS {
        // At capacity: only return if already interned (no new allocation).
        rodeo.get(s).map(IStr)
    } else {
        Some(IStr(rodeo.get_or_intern(s)))
    }
}

/// An interned string handle. Copy, Eq via integer comparison.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct IStr(Spur);

impl IStr {
    /// Intern a string, returning a handle.
    pub fn new(s: &str) -> Self {
        Self(interner().get_or_intern(s))
    }

    /// Look up a string WITHOUT interning it.
    /// Returns None if the string has never been interned.
    /// Use this in query paths to avoid polluting the interner with arbitrary user input.
    pub fn try_get(s: &str) -> Option<Self> {
        interner().get(s).map(Self)
    }

    /// Resolve to the underlying string slice.
    pub fn as_str(self) -> &'static str {
        interner().resolve(&self.0)
    }
}

impl std::fmt::Debug for IStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IStr({:?})", self.as_str())
    }
}

impl std::fmt::Display for IStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialOrd for IStr {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Orders by interner key (insertion order), not lexicographic.
/// Use `as_str()` for lexicographic comparisons.
impl Ord for IStr {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl From<&str> for IStr {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl serde::Serialize for IStr {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_str().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for IStr {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Ok(Self::new(&s))
    }
}

impl AsRef<str> for IStr {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intern_and_resolve() {
        let a = IStr::new("sensor");
        assert_eq!(a.as_str(), "sensor");
    }

    #[test]
    fn equality_by_identity() {
        let a = IStr::new("temperature");
        let b = IStr::new("temperature");
        assert_eq!(a, b);
    }

    #[test]
    fn different_strings_not_equal() {
        let a = IStr::new("sensor");
        let b = IStr::new("actuator");
        assert_ne!(a, b);
    }

    #[test]
    fn display_and_debug() {
        let s = IStr::new("zone");
        assert_eq!(format!("{s}"), "zone");
        assert!(format!("{s:?}").contains("zone"));
    }

    #[test]
    fn ordering_is_consistent() {
        let a = IStr::new("alpha");
        let b = IStr::new("beta");
        let cmp1 = a.cmp(&b);
        let cmp2 = a.cmp(&b);
        assert_eq!(cmp1, cmp2);
    }

    #[test]
    fn try_get_existing() {
        let _ = IStr::new("try_get_test");
        assert!(IStr::try_get("try_get_test").is_some());
    }

    #[test]
    fn try_get_missing() {
        assert!(IStr::try_get("never_interned_string_xyz_42").is_none());
    }

    #[test]
    fn try_get_matches_new() {
        let a = IStr::new("try_get_match");
        let b = IStr::try_get("try_get_match").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn serde_roundtrip() {
        let original = IStr::new("building");
        let json = serde_json::to_string(&original).unwrap();
        assert_eq!(json, "\"building\"");
        let deserialized: IStr = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn from_str_impl() {
        let s: IStr = "floor".into();
        assert_eq!(s.as_str(), "floor");
    }

    #[test]
    fn hash_works() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(IStr::new("a"));
        set.insert(IStr::new("b"));
        set.insert(IStr::new("a"));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn try_intern_returns_existing() {
        let a = IStr::new("try_intern_existing");
        let b = try_intern("try_intern_existing").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn try_intern_creates_new() {
        let result = try_intern("try_intern_brand_new_string");
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_str(), "try_intern_brand_new_string");
    }
}
