//! Three-valued logic per ISO GQL.
//!
//! GQL uses three-valued logic: TRUE, FALSE, and UNKNOWN.
//! UNKNOWN is the boolean interpretation of NULL.
//!
//! Key rules:
//! - `5 = NULL` → Unknown
//! - `NULL = NULL` → Unknown
//! - `NOT Unknown` → Unknown
//! - `TRUE AND Unknown` → Unknown
//! - `FALSE AND Unknown` → False
//! - `TRUE OR Unknown` → True
//! - `FALSE OR Unknown` → Unknown
//! - FILTER passes only TRUE rows (FALSE and UNKNOWN are filtered out)
//! - DISTINCT/GROUP BY use distinctness: NULL is NOT distinct from NULL

/// Three-valued logic result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Trilean {
    True,
    False,
    Unknown,
}

impl Trilean {
    /// Logical AND with three-valued semantics.
    ///
    /// | self    | other   | result  |
    /// |---------|---------|---------|
    /// | True    | True    | True    |
    /// | True    | False   | False   |
    /// | True    | Unknown | Unknown |
    /// | False   | True    | False   |
    /// | False   | False   | False   |
    /// | False   | Unknown | False   |
    /// | Unknown | True    | Unknown |
    /// | Unknown | False   | False   |
    /// | Unknown | Unknown | Unknown |
    pub fn and(self, other: Trilean) -> Trilean {
        match (self, other) {
            (Trilean::False, _) | (_, Trilean::False) => Trilean::False,
            (Trilean::True, Trilean::True) => Trilean::True,
            _ => Trilean::Unknown,
        }
    }

    /// Logical OR with three-valued semantics.
    ///
    /// | self    | other   | result  |
    /// |---------|---------|---------|
    /// | True    | _       | True    |
    /// | _       | True    | True    |
    /// | False   | False   | False   |
    /// | _       | _       | Unknown |
    pub fn or(self, other: Trilean) -> Trilean {
        match (self, other) {
            (Trilean::True, _) | (_, Trilean::True) => Trilean::True,
            (Trilean::False, Trilean::False) => Trilean::False,
            _ => Trilean::Unknown,
        }
    }

    /// Logical NOT with three-valued semantics.
    ///
    /// NOT True → False, NOT False → True, NOT Unknown → Unknown.
    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> Trilean {
        match self {
            Trilean::True => Trilean::False,
            Trilean::False => Trilean::True,
            Trilean::Unknown => Trilean::Unknown,
        }
    }

    /// Returns true only if the value is TRUE (not FALSE or UNKNOWN).
    /// Used by FILTER to determine which rows pass.
    pub fn is_true(self) -> bool {
        self == Trilean::True
    }

    /// Returns true if the value is UNKNOWN (NULL in boolean context).
    pub fn is_unknown(self) -> bool {
        self == Trilean::Unknown
    }
}

impl From<bool> for Trilean {
    fn from(b: bool) -> Self {
        if b { Trilean::True } else { Trilean::False }
    }
}

impl std::fmt::Display for Trilean {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Trilean::True => f.write_str("TRUE"),
            Trilean::False => f.write_str("FALSE"),
            Trilean::Unknown => f.write_str("UNKNOWN"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Trilean::*;

    // ── AND truth table (9 combinations) ──

    #[test]
    fn and_truth_table() {
        // True AND ...
        assert_eq!(True.and(True), True);
        assert_eq!(True.and(False), False);
        assert_eq!(True.and(Unknown), Unknown);
        // False AND ...
        assert_eq!(False.and(True), False);
        assert_eq!(False.and(False), False);
        assert_eq!(False.and(Unknown), False);
        // Unknown AND ...
        assert_eq!(Unknown.and(True), Unknown);
        assert_eq!(Unknown.and(False), False);
        assert_eq!(Unknown.and(Unknown), Unknown);
    }

    // ── OR truth table (9 combinations) ──

    #[test]
    fn or_truth_table() {
        // True OR ...
        assert_eq!(True.or(True), True);
        assert_eq!(True.or(False), True);
        assert_eq!(True.or(Unknown), True);
        // False OR ...
        assert_eq!(False.or(True), True);
        assert_eq!(False.or(False), False);
        assert_eq!(False.or(Unknown), Unknown);
        // Unknown OR ...
        assert_eq!(Unknown.or(True), True);
        assert_eq!(Unknown.or(False), Unknown);
        assert_eq!(Unknown.or(Unknown), Unknown);
    }

    // ── NOT truth table (3 values) ──

    #[test]
    fn not_truth_table() {
        assert_eq!(True.not(), False);
        assert_eq!(False.not(), True);
        assert_eq!(Unknown.not(), Unknown);
    }

    // ── is_true (used by FILTER) ──

    #[test]
    fn is_true_only_for_true() {
        assert!(True.is_true());
        assert!(!False.is_true());
        assert!(!Unknown.is_true());
    }

    #[test]
    fn is_unknown() {
        assert!(Unknown.is_unknown());
        assert!(!True.is_unknown());
        assert!(!False.is_unknown());
    }

    // ── Conversion ──

    #[test]
    fn from_bool() {
        assert_eq!(Trilean::from(true), True);
        assert_eq!(Trilean::from(false), False);
    }

    // ── Display ──

    #[test]
    fn display() {
        assert_eq!(format!("{True}"), "TRUE");
        assert_eq!(format!("{False}"), "FALSE");
        assert_eq!(format!("{Unknown}"), "UNKNOWN");
    }

    // ── Composition sanity checks ──

    #[test]
    fn double_negation() {
        assert_eq!(True.not().not(), True);
        assert_eq!(False.not().not(), False);
        assert_eq!(Unknown.not().not(), Unknown);
    }

    #[test]
    fn de_morgan_true_false() {
        // NOT (A AND B) == (NOT A) OR (NOT B)
        let a = True;
        let b = False;
        assert_eq!(a.and(b).not(), a.not().or(b.not()));
    }

    #[test]
    fn de_morgan_with_unknown() {
        // NOT (TRUE AND UNKNOWN) = NOT UNKNOWN = UNKNOWN
        // (NOT TRUE) OR (NOT UNKNOWN) = FALSE OR UNKNOWN = UNKNOWN
        let a = True;
        let b = Unknown;
        assert_eq!(a.and(b).not(), a.not().or(b.not()));
    }
}
