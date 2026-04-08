//! Mutation AST -- INSERT, SET, REMOVE, DELETE statements.
//!
//! Mutations can optionally be preceded by a query pipeline (MATCH + FILTER)
//! to select targets, and followed by a RETURN clause to report results.

use selene_core::IStr;

use super::expr::Expr;
use super::statement::{QueryPipeline, ReturnClause};

/// A mutation pipeline: optional pattern match + mutations + optional return.
///
/// Examples:
///   INSERT (:sensor {name: 'x'})
///   MATCH (s:sensor) FILTER s.temp > 80 SET s.alert = true RETURN s.name
///   MATCH (s:sensor {name: 'old'}) DELETE s
#[derive(Debug, Clone)]
pub struct MutationPipeline {
    /// Optional MATCH + FILTER to select mutation targets.
    pub query: Option<QueryPipeline>,
    /// One or more mutation operations.
    pub mutations: Vec<MutationOp>,
    /// Optional RETURN clause to report what was mutated.
    pub returning: Option<ReturnClause>,
}

/// A single mutation operation.
#[derive(Debug, Clone)]
pub enum MutationOp {
    /// INSERT graph pattern (spec §13.2): path-based composite insertion
    InsertPattern(InsertGraphPattern),

    /// SET target.property = expr
    SetProperty {
        /// Variable name of the target node/edge.
        target: IStr,
        /// Property key to set.
        property: IStr,
        /// Value expression.
        value: Expr,
    },

    /// SET target = {key: val, ...} -- replace all properties
    SetAllProperties {
        /// Variable name of the target node/edge.
        target: IStr,
        /// Replacement properties.
        properties: Vec<(IStr, Expr)>,
    },

    /// SET target IS Label -- add label to node
    SetLabel { target: IStr, label: IStr },

    /// REMOVE target.property
    RemoveProperty {
        /// Variable name of the target node/edge.
        target: IStr,
        /// Property key to remove.
        property: IStr,
    },

    /// REMOVE target IS Label -- remove label from node
    RemoveLabel { target: IStr, label: IStr },

    /// DELETE target -- fails if node has incident edges (use DETACH DELETE for cascade).
    Delete {
        /// Variable name of the node/edge to delete.
        target: IStr,
    },

    /// DETACH DELETE target -- cascades incident edges automatically.
    DetachDelete {
        /// Variable name of the node to delete with edge cascade.
        target: IStr,
    },

    /// MERGE (pattern) ON CREATE SET ... ON MATCH SET ...
    Merge {
        var: Option<IStr>,
        labels: Vec<IStr>,
        properties: Vec<(IStr, Expr)>,
        on_create: Vec<(IStr, IStr, Expr)>, // (target_var, prop, value)
        on_match: Vec<(IStr, IStr, Expr)>,
    },
}

/// INSERT graph pattern -- creates nodes and edges in a path.
/// Spec §13.2, §16.5.
#[derive(Debug, Clone)]
pub struct InsertGraphPattern {
    pub paths: Vec<InsertPathPattern>,
}

/// A single insert path: alternating nodes and edges.
#[derive(Debug, Clone)]
pub struct InsertPathPattern {
    pub elements: Vec<InsertElement>,
}

/// An element in an insert path pattern.
#[derive(Debug, Clone)]
pub enum InsertElement {
    Node {
        var: Option<IStr>,
        labels: Vec<IStr>,
        properties: Vec<(IStr, Expr)>,
    },
    Edge {
        var: Option<IStr>,
        label: Option<IStr>,
        direction: super::pattern::EdgeDirection,
        properties: Vec<(IStr, Expr)>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::value::GqlValue;

    #[test]
    fn set_property_mutation() {
        let op = MutationOp::SetProperty {
            target: IStr::new("s"),
            property: IStr::new("alert"),
            value: Expr::Literal(GqlValue::Bool(true)),
        };
        match op {
            MutationOp::SetProperty {
                target, property, ..
            } => {
                assert_eq!(target.as_str(), "s");
                assert_eq!(property.as_str(), "alert");
            }
            _ => panic!("expected SetProperty"),
        }
    }

    #[test]
    fn delete_mutation() {
        let op = MutationOp::Delete {
            target: IStr::new("s"),
        };
        match op {
            MutationOp::Delete { target } => assert_eq!(target.as_str(), "s"),
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn mutation_pipeline_with_query() {
        let pipeline = MutationPipeline {
            query: Some(QueryPipeline { statements: vec![] }),
            mutations: vec![MutationOp::Delete {
                target: IStr::new("s"),
            }],
            returning: None,
        };
        assert!(pipeline.query.is_some());
        assert_eq!(pipeline.mutations.len(), 1);
        assert!(pipeline.returning.is_none());
    }
}
