//! Binding: the row type during GQL execution.
//!
//! A `Binding` maps variable names (IStr) to `BoundValue`s. Variables are
//! sorted by IStr for fast lookup. SmallVec<8> avoids heap allocation for
//! the common case (most GQL queries bind fewer than 8 variables).
//!
//! `BoundValue` stores NodeId/EdgeId references lazily. Properties are
//! resolved from the graph only when accessed, avoiding PropertyMap clones
//! for nodes that get filtered out.

use selene_core::{EdgeId, IStr, NodeId};
use smallvec::SmallVec;

use super::error::GqlError;
use super::value::{GqlPath, GqlValue};

/// A value bound to a variable during pattern matching or LET.
#[derive(Debug, Clone)]
pub(crate) enum BoundValue {
    /// Node reference. Properties resolved lazily from graph on access.
    Node(NodeId),
    /// Edge reference. Properties resolved lazily from graph on access.
    Edge(EdgeId),
    /// Computed scalar value from LET, aggregation, or literal.
    Scalar(GqlValue),
    /// Path value from variable-length pattern matching.
    Path(GqlPath),
    /// Group list from variable-length edge patterns.
    /// Contains all edges matched in a single path (for horizontal aggregation).
    Group(Vec<EdgeId>),
}

/// A single row during GQL execution.
///
/// Maps variable names to bound values. Variables are sorted by IStr
/// for deterministic iteration and binary-search lookup.
#[derive(Debug, Clone)]
pub(crate) struct Binding {
    vars: SmallVec<[(IStr, BoundValue); 8]>,
}

#[allow(dead_code, clippy::trivially_copy_pass_by_ref)]
impl Binding {
    /// Create an empty binding (unit table row, no variables).
    pub fn empty() -> Self {
        Self {
            vars: SmallVec::new(),
        }
    }

    /// Create a binding with a single variable.
    pub fn single(var: IStr, value: BoundValue) -> Self {
        let mut b = Self::empty();
        b.bind(var, value);
        b
    }

    /// Bind a variable to a value. If the variable already exists, it is overwritten.
    pub fn bind(&mut self, var: IStr, value: BoundValue) {
        match self.vars.binary_search_by_key(&var, |(k, _)| *k) {
            Ok(idx) => self.vars[idx].1 = value,
            Err(idx) => self.vars.insert(idx, (var, value)),
        }
    }

    /// Look up a bound value by variable name.
    pub fn get(&self, var: &IStr) -> Option<&BoundValue> {
        self.vars
            .binary_search_by_key(var, |(k, _)| *k)
            .ok()
            .map(|idx| &self.vars[idx].1)
    }

    /// Remove a variable and return its previous value, if any.
    /// Used by scoped expressions (list iteration) to restore outer scope.
    pub fn unbind(&mut self, var: &IStr) -> Option<BoundValue> {
        match self.vars.binary_search_by_key(var, |(k, _)| *k) {
            Ok(idx) => Some(self.vars.remove(idx).1),
            Err(_) => None,
        }
    }

    /// Look up a variable and extract its NodeId, or return an error.
    pub fn get_node_id(&self, var: &IStr) -> Result<NodeId, GqlError> {
        match self.get(var) {
            Some(BoundValue::Node(id)) => Ok(*id),
            Some(other) => Err(GqlError::type_error(format!(
                "variable '{}' is not a node (got {:?})",
                var,
                std::mem::discriminant(other)
            ))),
            None => Err(GqlError::internal(format!("unbound variable '{var}'"))),
        }
    }

    /// Look up a variable and extract its EdgeId, or return an error.
    pub fn get_edge_id(&self, var: &IStr) -> Result<EdgeId, GqlError> {
        match self.get(var) {
            Some(BoundValue::Edge(id)) => Ok(*id),
            Some(other) => Err(GqlError::type_error(format!(
                "variable '{}' is not an edge (got {:?})",
                var,
                std::mem::discriminant(other)
            ))),
            None => Err(GqlError::internal(format!("unbound variable '{var}'"))),
        }
    }

    /// Number of bound variables.
    pub fn len(&self) -> usize {
        self.vars.len()
    }

    /// True if no variables are bound.
    pub fn is_empty(&self) -> bool {
        self.vars.is_empty()
    }

    /// Iterate over all bound variables.
    pub fn iter(&self) -> impl Iterator<Item = (&IStr, &BoundValue)> {
        self.vars.iter().map(|(k, v)| (k, v))
    }

    /// Check if a variable is bound.
    pub fn contains(&self, var: &IStr) -> bool {
        self.vars.binary_search_by_key(var, |(k, _)| *k).is_ok()
    }

    /// Merge another binding into this one.
    /// Variables from `other` overwrite existing ones with the same name.
    pub fn merge(&mut self, other: &Binding) {
        for (var, val) in &other.vars {
            self.bind(*var, val.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_binding() {
        let b = Binding::empty();
        assert!(b.is_empty());
        assert_eq!(b.len(), 0);
    }

    #[test]
    fn single_binding() {
        let var = IStr::new("s");
        let b = Binding::single(var, BoundValue::Node(NodeId(1)));
        assert_eq!(b.len(), 1);
        assert!(b.contains(&var));
    }

    #[test]
    fn bind_and_get() {
        let mut b = Binding::empty();
        let var = IStr::new("n");
        b.bind(var, BoundValue::Node(NodeId(42)));

        match b.get(&var) {
            Some(BoundValue::Node(id)) => assert_eq!(*id, NodeId(42)),
            _ => panic!("expected Node"),
        }
    }

    #[test]
    fn bind_overwrites() {
        let mut b = Binding::empty();
        let var = IStr::new("x");
        b.bind(var, BoundValue::Scalar(GqlValue::Int(1)));
        b.bind(var, BoundValue::Scalar(GqlValue::Int(2)));

        assert_eq!(b.len(), 1);
        match b.get(&var) {
            Some(BoundValue::Scalar(GqlValue::Int(2))) => {}
            _ => panic!("expected Int(2)"),
        }
    }

    #[test]
    fn get_missing_returns_none() {
        let b = Binding::empty();
        let var = IStr::new("missing");
        assert!(b.get(&var).is_none());
    }

    #[test]
    fn get_node_id_success() {
        let var = IStr::new("n");
        let b = Binding::single(var, BoundValue::Node(NodeId(5)));
        assert_eq!(b.get_node_id(&var).unwrap(), NodeId(5));
    }

    #[test]
    fn get_node_id_wrong_type() {
        let var = IStr::new("e");
        let b = Binding::single(var, BoundValue::Edge(EdgeId(5)));
        assert!(b.get_node_id(&var).is_err());
    }

    #[test]
    fn get_node_id_unbound() {
        let b = Binding::empty();
        let var = IStr::new("unbound");
        assert!(b.get_node_id(&var).is_err());
    }

    #[test]
    fn get_edge_id_success() {
        let var = IStr::new("e");
        let b = Binding::single(var, BoundValue::Edge(EdgeId(10)));
        assert_eq!(b.get_edge_id(&var).unwrap(), EdgeId(10));
    }

    #[test]
    fn multiple_variables_sorted() {
        let mut b = Binding::empty();
        let z = IStr::new("z_var");
        let a = IStr::new("a_var");
        let m = IStr::new("m_var");

        b.bind(z, BoundValue::Node(NodeId(3)));
        b.bind(a, BoundValue::Node(NodeId(1)));
        b.bind(m, BoundValue::Node(NodeId(2)));

        assert_eq!(b.len(), 3);

        // All accessible regardless of insertion order
        assert!(b.contains(&a));
        assert!(b.contains(&m));
        assert!(b.contains(&z));
    }

    #[test]
    fn merge_bindings() {
        let mut b1 = Binding::empty();
        b1.bind(IStr::new("a"), BoundValue::Node(NodeId(1)));
        b1.bind(IStr::new("b"), BoundValue::Node(NodeId(2)));

        let mut b2 = Binding::empty();
        b2.bind(IStr::new("c"), BoundValue::Node(NodeId(3)));
        b2.bind(IStr::new("a"), BoundValue::Node(NodeId(99))); // overwrites

        b1.merge(&b2);
        assert_eq!(b1.len(), 3);
        assert_eq!(b1.get_node_id(&IStr::new("a")).unwrap(), NodeId(99));
        assert_eq!(b1.get_node_id(&IStr::new("c")).unwrap(), NodeId(3));
    }

    #[test]
    fn clone_is_independent() {
        let mut b1 = Binding::single(IStr::new("x"), BoundValue::Node(NodeId(1)));
        let b2 = b1.clone();
        b1.bind(IStr::new("x"), BoundValue::Node(NodeId(99)));

        // Clone is unaffected
        assert_eq!(b2.get_node_id(&IStr::new("x")).unwrap(), NodeId(1));
        assert_eq!(b1.get_node_id(&IStr::new("x")).unwrap(), NodeId(99));
    }

    #[test]
    fn iter_variables() {
        let mut b = Binding::empty();
        b.bind(IStr::new("a"), BoundValue::Node(NodeId(1)));
        b.bind(IStr::new("b"), BoundValue::Edge(EdgeId(2)));

        let vars: Vec<_> = b.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(vars.len(), 2);
    }

    #[test]
    fn scalar_binding() {
        let var = IStr::new("temp");
        let b = Binding::single(var, BoundValue::Scalar(GqlValue::Float(72.5)));
        match b.get(&var) {
            Some(BoundValue::Scalar(GqlValue::Float(f))) => assert_eq!(*f, 72.5),
            _ => panic!("expected Float"),
        }
    }

    #[test]
    fn group_binding() {
        let var = IStr::new("edges");
        let b = Binding::single(
            var,
            BoundValue::Group(vec![EdgeId(1), EdgeId(2), EdgeId(3)]),
        );
        match b.get(&var) {
            Some(BoundValue::Group(edges)) => assert_eq!(edges.len(), 3),
            _ => panic!("expected Group"),
        }
    }

    #[test]
    fn path_binding() {
        let var = IStr::new("p");
        let path = GqlPath::from_nodes_and_edges(&[NodeId(1), NodeId(2)], &[EdgeId(10)]);
        let b = Binding::single(var, BoundValue::Path(path));
        match b.get(&var) {
            Some(BoundValue::Path(p)) => assert_eq!(p.edge_count(), 1),
            _ => panic!("expected Path"),
        }
    }
}
