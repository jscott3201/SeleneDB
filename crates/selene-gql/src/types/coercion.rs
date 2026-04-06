//! Numeric coercion and three-valued comparison per ISO GQL.
//!
//! Coercion rules (in precedence order):
//! 1. If either operand is FLOAT/DOUBLE → coerce both to Float
//! 2. If mixed INT64 + UINT64 → coerce both to Int (signed)
//!    - UInt values exceeding i64::MAX produce a runtime error
//! 3. Same types → no coercion
//!
//! Comparison rules:
//! - Any comparison with NULL → UNKNOWN
//! - NULL = NULL → UNKNOWN (not TRUE)
//! - DISTINCT/GROUP BY: NULL is NOT distinct from NULL
//! - ORDER BY: NULL is the smallest value (first in ASC, last in DESC)

use std::cmp::Ordering;

use super::error::GqlError;
use super::trilean::Trilean;
use super::value::GqlValue;

impl GqlValue {
    /// Three-valued equality per GQL spec.
    ///
    /// Returns UNKNOWN if either operand is NULL.
    /// Returns TRUE/FALSE for comparable types.
    /// Returns FALSE for incompatible types (not an error).
    pub fn gql_eq(&self, other: &GqlValue) -> Trilean {
        if self.is_null() || other.is_null() {
            return Trilean::Unknown;
        }
        match (self, other) {
            (GqlValue::Bool(a), GqlValue::Bool(b)) => Trilean::from(*a == *b),
            (GqlValue::String(a), GqlValue::String(b)) => Trilean::from(**a == **b),
            (GqlValue::Bytes(a), GqlValue::Bytes(b)) => Trilean::from(*a == *b),
            (GqlValue::Vector(a), GqlValue::Vector(b)) => Trilean::from(*a == *b),
            (GqlValue::Node(a), GqlValue::Node(b)) => Trilean::from(a == b),
            (GqlValue::Edge(a), GqlValue::Edge(b)) => Trilean::from(a == b),
            (GqlValue::ZonedDateTime(a), GqlValue::ZonedDateTime(b)) => {
                Trilean::from(a.nanos == b.nanos)
            }
            (GqlValue::Date(a), GqlValue::Date(b)) => Trilean::from(a.days == b.days),
            (GqlValue::LocalDateTime(a), GqlValue::LocalDateTime(b)) => {
                Trilean::from(a.nanos == b.nanos)
            }
            (GqlValue::LocalTime(a), GqlValue::LocalTime(b)) => Trilean::from(a.nanos == b.nanos),
            (GqlValue::ZonedTime(a), GqlValue::ZonedTime(b)) => {
                let a_utc = a.nanos as i64 - (i64::from(a.offset_seconds) * 1_000_000_000);
                let b_utc = b.nanos as i64 - (i64::from(b.offset_seconds) * 1_000_000_000);
                Trilean::from(a_utc == b_utc)
            }
            (GqlValue::Duration(a), GqlValue::Duration(b)) => Trilean::from(a.nanos == b.nanos),
            // Numeric: coerce and compare
            _ if self.is_numeric() && other.is_numeric() => {
                match coerce_and_compare_numeric(self, other) {
                    Ok(Ordering::Equal) => Trilean::True,
                    Ok(_) => Trilean::False,
                    Err(_) => Trilean::False, // overflow treated as not equal
                }
            }
            // Incompatible types
            _ => Trilean::False,
        }
    }

    /// Ordering for use in ORDER BY, min/max, and sorting.
    ///
    /// NULL is the smallest value per GQL spec.
    /// Returns Err for incomparable types (can't ORDER BY node references).
    pub fn gql_order(&self, other: &GqlValue) -> Result<Ordering, GqlError> {
        // NULL is smallest
        match (self.is_null(), other.is_null()) {
            (true, true) => return Ok(Ordering::Equal),
            (true, false) => return Ok(Ordering::Less),
            (false, true) => return Ok(Ordering::Greater),
            (false, false) => {}
        }

        match (self, other) {
            (GqlValue::Bool(a), GqlValue::Bool(b)) => {
                // FALSE < TRUE per spec
                Ok(a.cmp(b))
            }
            (GqlValue::String(a), GqlValue::String(b)) => {
                // UCS_BASIC collation (Unicode codepoint comparison)
                Ok((**a).cmp(&**b))
            }
            (GqlValue::ZonedDateTime(a), GqlValue::ZonedDateTime(b)) => Ok(a.cmp_absolute(b)),
            (GqlValue::Date(a), GqlValue::Date(b)) => Ok(a.days.cmp(&b.days)),
            (GqlValue::LocalDateTime(a), GqlValue::LocalDateTime(b)) => Ok(a.nanos.cmp(&b.nanos)),
            (GqlValue::LocalTime(a), GqlValue::LocalTime(b)) => Ok(a.nanos.cmp(&b.nanos)),
            (GqlValue::ZonedTime(a), GqlValue::ZonedTime(b)) => {
                // Normalize to UTC for comparison
                let a_utc = a.nanos as i64 - (i64::from(a.offset_seconds) * 1_000_000_000);
                let b_utc = b.nanos as i64 - (i64::from(b.offset_seconds) * 1_000_000_000);
                Ok(a_utc.cmp(&b_utc))
            }
            (GqlValue::Duration(a), GqlValue::Duration(b)) => Ok(a.nanos.cmp(&b.nanos)),
            // Cross-temporal: Date <-> ZonedDateTime (promote Date to midnight UTC)
            (GqlValue::Date(d), GqlValue::ZonedDateTime(zdt)) => {
                let d_nanos = i64::from(d.days) * 86400 * 1_000_000_000;
                Ok(d_nanos.cmp(&zdt.nanos))
            }
            (GqlValue::ZonedDateTime(zdt), GqlValue::Date(d)) => {
                let d_nanos = i64::from(d.days) * 86400 * 1_000_000_000;
                Ok(zdt.nanos.cmp(&d_nanos))
            }
            (GqlValue::Bytes(a), GqlValue::Bytes(b)) => Ok(a.cmp(b)),
            // Numeric: coerce and compare
            _ if self.is_numeric() && other.is_numeric() => coerce_and_compare_numeric(self, other),
            _ => Err(GqlError::type_error(format!(
                "cannot compare {} with {}",
                self.gql_type(),
                other.gql_type()
            ))),
        }
    }

    /// Ordering for sort that never fails. Incomparable types sort by type name.
    ///
    /// Used internally by ORDER BY (which has already validated types at plan time).
    /// When `gql_order()` returns an error (incomparable types such as INT vs STRING),
    /// falls back to comparing type-name strings for deterministic, stable ordering.
    /// Mixed-type columns cluster by type rather than error, matching the permissive
    /// ORDER BY semantics expected by callers. Fires `tracing::debug!` on fallback
    /// so operators can detect unexpected mixed-type sort keys in query logs.
    pub fn sort_order(&self, other: &GqlValue) -> Ordering {
        self.gql_order(other).unwrap_or_else(|_| {
            // Fallback: sort by type name for stability (incomparable types)
            tracing::debug!(
                left_type = %self.gql_type(),
                right_type = %other.gql_type(),
                "sort_order fallback: comparing incomparable types by type name"
            );
            self.gql_type()
                .to_string()
                .cmp(&other.gql_type().to_string())
        })
    }

    /// Distinctness check per GQL spec.
    ///
    /// Unlike equality: NULL is NOT distinct from NULL (returns false).
    /// Used by DISTINCT and GROUP BY.
    pub fn is_not_distinct(&self, other: &GqlValue) -> bool {
        match (self.is_null(), other.is_null()) {
            (true, true) => true, // NULL not distinct from NULL
            (true, false) | (false, true) => false,
            (false, false) => self.gql_eq(other).is_true(),
        }
    }

    /// Hash-like key for GROUP BY / DISTINCT.
    ///
    /// Numeric types use a unified hashing path so that values considered equal
    /// by `gql_eq` produce the same key:
    /// - All integers (Int, UInt) are widened to i128 for lossless hashing.
    /// - Floats with no fractional part that fit in i128 hash on the integer path.
    /// - Other floats hash via `f64::to_bits()` with a distinct discriminant byte.
    ///
    /// IMPORTANT: We must NOT cast everything to f64 because `i64::MAX` and
    /// `i64::MAX - 1` both round to the same f64, which would silently merge
    /// distinct groups.
    pub fn distinctness_key(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        match self {
            // -- Numeric types: unified hashing via i128 --
            GqlValue::Int(i) => {
                0u8.hash(&mut hasher); // shared numeric discriminant
                i128::from(*i).hash(&mut hasher);
            }
            GqlValue::UInt(u) => {
                0u8.hash(&mut hasher); // shared numeric discriminant
                i128::from(*u).hash(&mut hasher);
            }
            GqlValue::Float(f) => {
                let frac = f - f.trunc();
                if frac == 0.0 && *f >= i128::MIN as f64 && *f <= i128::MAX as f64 {
                    // Integer-valued float: hash on the integer path
                    0u8.hash(&mut hasher);
                    (*f as i128).hash(&mut hasher);
                } else {
                    // Non-integer float: use to_bits with a different discriminant
                    1u8.hash(&mut hasher);
                    f.to_bits().hash(&mut hasher);
                }
            }
            // -- Non-numeric types: discriminant + value --
            GqlValue::Null => {
                std::mem::discriminant(self).hash(&mut hasher);
            }
            GqlValue::Bool(b) => {
                std::mem::discriminant(self).hash(&mut hasher);
                b.hash(&mut hasher);
            }
            GqlValue::String(s) => {
                std::mem::discriminant(self).hash(&mut hasher);
                s.hash(&mut hasher);
            }
            GqlValue::ZonedDateTime(zdt) => {
                std::mem::discriminant(self).hash(&mut hasher);
                zdt.nanos.hash(&mut hasher);
            }
            GqlValue::Date(d) => {
                std::mem::discriminant(self).hash(&mut hasher);
                d.days.hash(&mut hasher);
            }
            GqlValue::LocalDateTime(dt) => {
                std::mem::discriminant(self).hash(&mut hasher);
                dt.nanos.hash(&mut hasher);
            }
            GqlValue::ZonedTime(t) => {
                std::mem::discriminant(self).hash(&mut hasher);
                // Hash UTC-normalized nanos for consistency with gql_eq (which normalizes to UTC)
                let utc_nanos = t.nanos as i64 - (i64::from(t.offset_seconds) * 1_000_000_000);
                utc_nanos.hash(&mut hasher);
            }
            GqlValue::LocalTime(t) => {
                std::mem::discriminant(self).hash(&mut hasher);
                t.nanos.hash(&mut hasher);
            }
            GqlValue::Duration(d) => {
                std::mem::discriminant(self).hash(&mut hasher);
                d.nanos.hash(&mut hasher);
            }
            GqlValue::Bytes(b) => {
                std::mem::discriminant(self).hash(&mut hasher);
                b.hash(&mut hasher);
            }
            GqlValue::Vector(v) => {
                std::mem::discriminant(self).hash(&mut hasher);
                v.len().hash(&mut hasher);
                for elem in v.iter() {
                    elem.to_bits().hash(&mut hasher);
                }
            }
            GqlValue::Record(r) => {
                std::mem::discriminant(self).hash(&mut hasher);
                r.fields.len().hash(&mut hasher);
                for (k, v) in &r.fields {
                    k.hash(&mut hasher);
                    v.distinctness_key().hash(&mut hasher);
                }
            }
            GqlValue::Node(id) => {
                std::mem::discriminant(self).hash(&mut hasher);
                id.0.hash(&mut hasher);
            }
            GqlValue::Edge(id) => {
                std::mem::discriminant(self).hash(&mut hasher);
                id.0.hash(&mut hasher);
            }
            GqlValue::List(l) => {
                std::mem::discriminant(self).hash(&mut hasher);
                l.len().hash(&mut hasher);
                for elem in l.elements.iter() {
                    elem.distinctness_key().hash(&mut hasher);
                }
            }
            GqlValue::Path(p) => {
                std::mem::discriminant(self).hash(&mut hasher);
                p.elements.len().hash(&mut hasher);
                for elem in &p.elements {
                    match elem {
                        crate::types::value::PathElement::Node(id) => id.0.hash(&mut hasher),
                        crate::types::value::PathElement::Edge(id) => id.0.hash(&mut hasher),
                    }
                }
            }
        }
        hasher.finish()
    }

    fn is_numeric(&self) -> bool {
        matches!(
            self,
            GqlValue::Int(_) | GqlValue::UInt(_) | GqlValue::Float(_)
        )
    }
}

/// Coerce two numeric values and compare.
///
/// Rules:
/// 1. If either is Float -> both to f64
/// 2. If mixed Int + UInt -> both to i64 (overflow -> error)
/// 3. Same type -> direct comparison
fn coerce_and_compare_numeric(a: &GqlValue, b: &GqlValue) -> Result<Ordering, GqlError> {
    // Rule 1: Float promotion
    if matches!(a, GqlValue::Float(_)) || matches!(b, GqlValue::Float(_)) {
        let fa = to_f64(a)?;
        let fb = to_f64(b)?;
        return Ok(fa.total_cmp(&fb));
    }

    // Rule 3: Same type
    match (a, b) {
        (GqlValue::Int(a), GqlValue::Int(b)) => Ok(a.cmp(b)),
        (GqlValue::UInt(a), GqlValue::UInt(b)) => Ok(a.cmp(b)),
        // Rule 2: Mixed Int + UInt -> coerce to signed
        (GqlValue::Int(a), GqlValue::UInt(b)) => {
            let b_signed = i64::try_from(*b)
                .map_err(|_| GqlError::type_error("UINT64 value exceeds INT64 range"))?;
            Ok(a.cmp(&b_signed))
        }
        (GqlValue::UInt(a), GqlValue::Int(b)) => {
            let a_signed = i64::try_from(*a)
                .map_err(|_| GqlError::type_error("UINT64 value exceeds INT64 range"))?;
            Ok(a_signed.cmp(b))
        }
        _ => Err(GqlError::internal(
            "coerce_and_compare_numeric called with non-numeric",
        )),
    }
}

fn to_f64(v: &GqlValue) -> Result<f64, GqlError> {
    match v {
        GqlValue::Float(f) => Ok(*f),
        GqlValue::Int(i) => Ok(*i as f64),
        GqlValue::UInt(u) => Ok(*u as f64),
        _ => Err(GqlError::type_error("expected numeric value")),
    }
}

// ── Implicit type coercion ─────────────────────────────────────────

/// Try to coerce a string value to a numeric type.
/// Returns None if the string cannot be parsed as a number.
pub(crate) fn try_coerce_to_numeric(v: &GqlValue) -> Option<GqlValue> {
    if let GqlValue::String(s) = v {
        if let Ok(i) = s.parse::<i64>() {
            return Some(GqlValue::Int(i));
        }
        if let Ok(f) = s.parse::<f64>() {
            return Some(GqlValue::Float(f));
        }
    }
    None
}

/// Coerce a boolean to an integer (true=1, false=0).
pub(crate) fn coerce_bool_to_int(v: &GqlValue) -> GqlValue {
    match v {
        GqlValue::Bool(true) => GqlValue::Int(1),
        GqlValue::Bool(false) => GqlValue::Int(0),
        other => other.clone(),
    }
}

/// Coerce a value to float if possible (for mixed-type arithmetic).
pub(crate) fn try_coerce_to_float(v: &GqlValue) -> Option<f64> {
    match v {
        GqlValue::Float(f) => Some(*f),
        GqlValue::Int(i) => Some(*i as f64),
        GqlValue::UInt(u) => Some(*u as f64),
        GqlValue::String(s) => s.parse::<f64>().ok(),
        GqlValue::Bool(true) => Some(1.0),
        GqlValue::Bool(false) => Some(0.0),
        _ => None,
    }
}

/// Try to align two values for comparison by coercing the less-specific
/// type to the more-specific one. Returns (left, right) after coercion.
pub(crate) fn coerce_for_comparison(left: &GqlValue, right: &GqlValue) -> (GqlValue, GqlValue) {
    match (left, right) {
        // String vs numeric: try parsing string
        (GqlValue::String(_), GqlValue::Int(_) | GqlValue::Float(_) | GqlValue::UInt(_)) => {
            if let Some(coerced) = try_coerce_to_numeric(left) {
                return (coerced, right.clone());
            }
            (left.clone(), right.clone())
        }
        (GqlValue::Int(_) | GqlValue::Float(_) | GqlValue::UInt(_), GqlValue::String(_)) => {
            if let Some(coerced) = try_coerce_to_numeric(right) {
                return (left.clone(), coerced);
            }
            (left.clone(), right.clone())
        }
        // Bool vs Int: coerce bool
        (GqlValue::Bool(_), GqlValue::Int(_)) => (coerce_bool_to_int(left), right.clone()),
        (GqlValue::Int(_), GqlValue::Bool(_)) => (left.clone(), coerce_bool_to_int(right)),
        _ => (left.clone(), right.clone()),
    }
}

/// Generate a type error for strict coercion mode with a CAST hint.
///
/// Used when `GqlOptions::strict_coercion` is `true` and an implicit
/// coercion would be required to proceed.
pub(crate) fn strict_type_error(left_type: &str, right_type: &str, operation: &str) -> GqlError {
    GqlError::type_error(format!(
        "Cannot {operation} {left_type} and {right_type} -- use CAST() for explicit conversion"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::value::ZonedDateTime;
    use smol_str::SmolStr;

    use selene_core::NodeId;

    // -- Equality (three-valued) --

    #[test]
    fn eq_int_int() {
        assert_eq!(GqlValue::Int(5).gql_eq(&GqlValue::Int(5)), Trilean::True);
        assert_eq!(GqlValue::Int(5).gql_eq(&GqlValue::Int(3)), Trilean::False);
    }

    #[test]
    fn eq_with_null() {
        assert_eq!(GqlValue::Int(5).gql_eq(&GqlValue::Null), Trilean::Unknown);
        assert_eq!(GqlValue::Null.gql_eq(&GqlValue::Int(5)), Trilean::Unknown);
        assert_eq!(GqlValue::Null.gql_eq(&GqlValue::Null), Trilean::Unknown);
    }

    #[test]
    fn eq_string() {
        let a = GqlValue::String(SmolStr::new("hello"));
        let b = GqlValue::String(SmolStr::new("hello"));
        let c = GqlValue::String(SmolStr::new("world"));
        assert_eq!(a.gql_eq(&b), Trilean::True);
        assert_eq!(a.gql_eq(&c), Trilean::False);
    }

    #[test]
    fn eq_bool() {
        assert_eq!(
            GqlValue::Bool(true).gql_eq(&GqlValue::Bool(true)),
            Trilean::True
        );
        assert_eq!(
            GqlValue::Bool(true).gql_eq(&GqlValue::Bool(false)),
            Trilean::False
        );
    }

    #[test]
    fn eq_node_refs() {
        assert_eq!(
            GqlValue::Node(NodeId(1)).gql_eq(&GqlValue::Node(NodeId(1))),
            Trilean::True
        );
        assert_eq!(
            GqlValue::Node(NodeId(1)).gql_eq(&GqlValue::Node(NodeId(2))),
            Trilean::False
        );
    }

    #[test]
    fn eq_incompatible_types() {
        assert_eq!(
            GqlValue::Int(42).gql_eq(&GqlValue::String(SmolStr::new("42"))),
            Trilean::False
        );
    }

    // -- Numeric coercion --

    #[test]
    fn eq_int_float_coercion() {
        // Rule 1: Float promotion
        assert_eq!(
            GqlValue::Int(42).gql_eq(&GqlValue::Float(42.0)),
            Trilean::True
        );
        assert_eq!(
            GqlValue::Int(42).gql_eq(&GqlValue::Float(42.5)),
            Trilean::False
        );
    }

    #[test]
    fn eq_int_uint_coercion() {
        // Rule 2: Mixed signed/unsigned -> signed
        assert_eq!(GqlValue::Int(42).gql_eq(&GqlValue::UInt(42)), Trilean::True);
    }

    #[test]
    fn eq_uint_overflow_is_false() {
        // UInt exceeding i64::MAX -> comparison fails -> False
        assert_eq!(
            GqlValue::Int(0).gql_eq(&GqlValue::UInt(u64::MAX)),
            Trilean::False
        );
    }

    // -- Ordering --

    #[test]
    fn order_null_is_smallest() {
        let null = GqlValue::Null;
        let five = GqlValue::Int(5);
        assert_eq!(null.gql_order(&five).unwrap(), Ordering::Less);
        assert_eq!(five.gql_order(&null).unwrap(), Ordering::Greater);
        assert_eq!(null.gql_order(&null).unwrap(), Ordering::Equal);
    }

    #[test]
    fn order_ints() {
        assert_eq!(
            GqlValue::Int(3).gql_order(&GqlValue::Int(5)).unwrap(),
            Ordering::Less
        );
    }

    #[test]
    fn order_strings_ucs_basic() {
        let a = GqlValue::String(SmolStr::new("alpha"));
        let b = GqlValue::String(SmolStr::new("beta"));
        assert_eq!(a.gql_order(&b).unwrap(), Ordering::Less);
    }

    #[test]
    fn order_booleans() {
        // FALSE < TRUE per spec
        assert_eq!(
            GqlValue::Bool(false)
                .gql_order(&GqlValue::Bool(true))
                .unwrap(),
            Ordering::Less
        );
    }

    #[test]
    fn order_zoned_datetime_chronological() {
        let earlier = GqlValue::ZonedDateTime(ZonedDateTime {
            nanos: 100,
            offset_seconds: 0,
        });
        let later = GqlValue::ZonedDateTime(ZonedDateTime {
            nanos: 200,
            offset_seconds: 3600,
        });
        assert_eq!(earlier.gql_order(&later).unwrap(), Ordering::Less);
    }

    #[test]
    fn order_incomparable_types_error() {
        let result = GqlValue::Int(1).gql_order(&GqlValue::String(SmolStr::new("x")));
        assert!(result.is_err());
    }

    #[test]
    fn order_mixed_numeric_coercion() {
        // Int vs Float -> Float comparison
        assert_eq!(
            GqlValue::Int(3).gql_order(&GqlValue::Float(3.5)).unwrap(),
            Ordering::Less
        );
    }

    // -- Distinctness --

    #[test]
    fn distinctness_null_null() {
        assert!(GqlValue::Null.is_not_distinct(&GqlValue::Null));
    }

    #[test]
    fn distinctness_null_value() {
        assert!(!GqlValue::Null.is_not_distinct(&GqlValue::Int(5)));
    }

    #[test]
    fn distinctness_same_value() {
        assert!(GqlValue::Int(5).is_not_distinct(&GqlValue::Int(5)));
    }

    #[test]
    fn distinctness_different_value() {
        assert!(!GqlValue::Int(5).is_not_distinct(&GqlValue::Int(3)));
    }

    // -- Distinctness key (for hashing in GROUP BY) --

    #[test]
    fn distinctness_key_same_values_same_hash() {
        let a = GqlValue::Int(42);
        let b = GqlValue::Int(42);
        assert_eq!(a.distinctness_key(), b.distinctness_key());
    }

    #[test]
    fn distinctness_key_float_deterministic() {
        let a = GqlValue::Float(3.15);
        let b = GqlValue::Float(3.15);
        assert_eq!(a.distinctness_key(), b.distinctness_key());
    }

    #[test]
    fn distinctness_key_mixed_numeric_types() {
        assert_eq!(
            GqlValue::Int(42).distinctness_key(),
            GqlValue::UInt(42).distinctness_key(),
            "Int(42) and UInt(42) must hash the same (gql_eq considers them equal)"
        );
        assert_eq!(
            GqlValue::Int(42).distinctness_key(),
            GqlValue::Float(42.0).distinctness_key(),
            "Int(42) and Float(42.0) must hash the same (gql_eq considers them equal)"
        );
        // Regression: large integers must NOT collide
        assert_ne!(
            GqlValue::Int(i64::MAX).distinctness_key(),
            GqlValue::Int(i64::MAX - 1).distinctness_key(),
            "i64::MAX and i64::MAX-1 must be distinct"
        );
        // Different values must differ
        assert_ne!(
            GqlValue::Int(1).distinctness_key(),
            GqlValue::Int(2).distinctness_key(),
            "Int(1) and Int(2) must be distinct"
        );
    }

    #[test]
    fn sort_order_never_panics() {
        // sort_order falls back to type name for incomparable types
        let a = GqlValue::Int(1);
        let b = GqlValue::String(SmolStr::new("x"));
        let _ = a.sort_order(&b); // should not panic
    }

    // -- Implicit coercion tests --

    #[test]
    fn coerce_string_to_int() {
        let v = GqlValue::String(SmolStr::new("42"));
        assert_eq!(try_coerce_to_numeric(&v), Some(GqlValue::Int(42)));
    }

    #[test]
    fn coerce_string_to_float() {
        let v = GqlValue::String(SmolStr::new("3.15"));
        assert_eq!(try_coerce_to_numeric(&v), Some(GqlValue::Float(3.15)));
    }

    #[test]
    fn coerce_string_non_numeric() {
        let v = GqlValue::String(SmolStr::new("hello"));
        assert_eq!(try_coerce_to_numeric(&v), None);
    }

    #[test]
    fn test_coerce_bool_to_int() {
        assert_eq!(coerce_bool_to_int(&GqlValue::Bool(true)), GqlValue::Int(1));
        assert_eq!(coerce_bool_to_int(&GqlValue::Bool(false)), GqlValue::Int(0));
    }

    // -- Coercion overflow --

    #[test]
    fn uint_overflow_comparison_error() {
        let result = coerce_and_compare_numeric(&GqlValue::UInt(u64::MAX), &GqlValue::Int(0));
        assert!(result.is_err());
    }

    // -- Strict coercion error messages --

    #[test]
    fn strict_type_error_contains_cast_hint() {
        let err = strict_type_error("STRING", "INT64", "compare");
        let msg = err.to_string();
        assert!(msg.contains("CAST"), "error should mention CAST: {msg}");
        assert!(
            msg.contains("STRING"),
            "error should mention left type: {msg}"
        );
        assert!(
            msg.contains("INT64"),
            "error should mention right type: {msg}"
        );
        assert!(
            msg.contains("compare"),
            "error should mention operation: {msg}"
        );
    }
}
