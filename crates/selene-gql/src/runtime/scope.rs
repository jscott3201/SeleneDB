//! Auth scope integration: converts Cedar scope to RoaringBitmap.
//!
//! The auth scope is a `HashSet<NodeId>` representing which nodes the
//! principal is authorized to access. This is converted to a RoaringBitmap
//! once at query start for O(1) membership checks during pattern matching.

use std::collections::HashSet;

use roaring::RoaringBitmap;
use selene_core::NodeId;

use crate::types::error::GqlError;

/// Convert a Cedar auth scope (`HashSet<NodeId>`) to a RoaringBitmap.
///
/// Called once at query start. The resulting bitmap is passed through
/// to LabelScan, Expand, and VarExpand as an implicit AND filter.
pub fn scope_to_bitmap(scope: &HashSet<NodeId>) -> Result<RoaringBitmap, GqlError> {
    scope
        .iter()
        .map(|id| {
            u32::try_from(id.0)
                .map_err(|_| GqlError::internal("NodeId exceeds RoaringBitmap u32 range"))
        })
        .collect()
}

/// Check that a target node is within the auth scope.
/// Used before mutations to enforce authorization.
pub fn check_scope(node_id: NodeId, scope: Option<&RoaringBitmap>) -> Result<(), GqlError> {
    if let Some(bitmap) = scope {
        let id = u32::try_from(node_id.0)
            .map_err(|_| GqlError::internal("NodeId exceeds u32 for scope check"))?;
        if !bitmap.contains(id) {
            return Err(GqlError::AuthDenied);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_to_bitmap_conversion() {
        let mut scope = HashSet::new();
        scope.insert(NodeId(1));
        scope.insert(NodeId(5));
        scope.insert(NodeId(10));

        let bitmap = scope_to_bitmap(&scope).unwrap();
        assert_eq!(bitmap.len(), 3);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(5));
        assert!(bitmap.contains(10));
        assert!(!bitmap.contains(2));
    }

    #[test]
    fn check_scope_in_scope() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(1);
        bitmap.insert(5);

        assert!(check_scope(NodeId(1), Some(&bitmap)).is_ok());
        assert!(check_scope(NodeId(5), Some(&bitmap)).is_ok());
    }

    #[test]
    fn check_scope_out_of_scope() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(1);

        assert!(check_scope(NodeId(99), Some(&bitmap)).is_err());
    }

    #[test]
    fn check_scope_no_scope_is_admin() {
        // No scope = admin, everything passes
        assert!(check_scope(NodeId(999), None).is_ok());
    }
}
