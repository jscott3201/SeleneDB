//! GQL Abstract Syntax Tree -- typed representations of parsed GQL.
//!
//! All identifiers, labels, and property keys are `IStr` (interned at parse time).
//! After AST construction, the entire engine operates on integer comparisons.

pub mod expr;
pub mod mutation;
pub mod pattern;
pub mod statement;
