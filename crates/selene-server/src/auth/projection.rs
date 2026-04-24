//! Containment-to-Cedar entity projection.
//!
//! Walks the graph to build Cedar entity hierarchies and resolve principal scope.

use std::collections::HashSet;

use roaring::RoaringBitmap;
use selene_core::NodeId;
use selene_graph::SeleneGraph;

/// Walk up the containment tree from `start` to roots.
///
/// Returns the chain of node IDs from `start` to the root(s),
/// following `contains` edges in reverse (child → parent).
#[allow(dead_code)]
pub fn walk_up(graph: &SeleneGraph, start: NodeId) -> Vec<NodeId> {
    let mut chain = vec![start];
    let mut current = start;
    let mut visited = HashSet::new();
    visited.insert(current);

    loop {
        // Find incoming "contains" edges to current node
        let parent = graph.incoming(current).iter().find_map(|&edge_id| {
            let edge = graph.get_edge(edge_id)?;
            if edge.label.as_str() == "contains" && !visited.contains(&edge.source) {
                Some(edge.source)
            } else {
                None
            }
        });

        match parent {
            Some(p) => {
                chain.push(p);
                visited.insert(p);
                current = p;
            }
            None => break,
        }
    }

    chain
}

/// Walk down the containment tree from `roots`, collecting all descendants.
///
/// Returns a RoaringBitmap of all node IDs reachable via `contains` edges
/// from any of the root nodes (inclusive of roots).
pub fn resolve_scope(graph: &SeleneGraph, roots: &[NodeId]) -> RoaringBitmap {
    let mut scope = RoaringBitmap::new();
    let mut queue = std::collections::VecDeque::new();

    for &root in roots {
        if graph.contains_node(root) {
            scope.insert(root.0 as u32);
            queue.push_back(root);
        }
    }

    while let Some(current) = queue.pop_front() {
        for &edge_id in graph.outgoing(current) {
            if let Some(edge) = graph.get_edge(edge_id)
                && edge.label.as_str() == "contains"
                && scope.insert(edge.target.0 as u32)
            {
                queue.push_back(edge.target);
            }
        }
    }

    scope
}

/// Read a principal's scope roots from its `scope_root_ids` property.
///
/// Scope roots are stored as a `List<Int>` property on the principal node
/// in the vault graph. Each integer is a main-graph `NodeId` that acts as
/// the containment root for this principal's authority; the caller then
/// passes the returned `Vec<NodeId>` into [`resolve_scope`] against the
/// main graph to expand into a full access bitmap.
///
/// Prior to 1.3.0, scope roots were represented by `[:scoped_to]` edges
/// inside the main graph from the principal node to each root. That model
/// allowed any writer with `entity:create` on an in-scope node to forge a
/// principal + scoped_to edge and escalate; moving the data to a property
/// on a vault-only principal closes that escalation. See finding 11018.
///
/// Returns an empty `Vec` if the principal has no `scope_root_ids`, if the
/// node is missing, or if the property exists but is not a list of integers
/// (treated as "no scope" rather than erroring — the caller will surface
/// an empty scope as access denied at enforcement time).
pub fn scope_roots(vault_graph: &SeleneGraph, principal_id: NodeId) -> Vec<NodeId> {
    use selene_core::Value;

    let Some(node) = vault_graph.get_node(principal_id) else {
        return Vec::new();
    };
    let Some(prop) = node.property("scope_root_ids") else {
        return Vec::new();
    };
    let Value::List(items) = prop else {
        return Vec::new();
    };

    items
        .iter()
        .filter_map(|v| match v {
            Value::Int(i) if *i >= 0 => Some(NodeId(*i as u64)),
            Value::UInt(u) => Some(NodeId(*u)),
            _ => None,
        })
        .collect()
}

/// Check if `target` is within the containment subtree of any of `scope_roots`.
#[allow(dead_code)]
pub fn is_in_scope(target: NodeId, scope: &RoaringBitmap) -> bool {
    scope.contains(target.0 as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{IStr, LabelSet, PropertyMap};
    use selene_graph::{SeleneGraph, SharedGraph};

    fn test_graph() -> (SharedGraph, NodeId, NodeId, NodeId, NodeId) {
        let g = SeleneGraph::new();
        let shared = SharedGraph::new(g);
        let c = IStr::new("contains");

        let (ids, _) = shared
            .write(|m| {
                let site = m.create_node(LabelSet::from_strs(&["site"]), PropertyMap::new())?;
                let building =
                    m.create_node(LabelSet::from_strs(&["building"]), PropertyMap::new())?;
                let floor = m.create_node(LabelSet::from_strs(&["floor"]), PropertyMap::new())?;
                let zone = m.create_node(LabelSet::from_strs(&["zone"]), PropertyMap::new())?;

                m.create_edge(site, c, building, PropertyMap::new())?;
                m.create_edge(building, c, floor, PropertyMap::new())?;
                m.create_edge(floor, c, zone, PropertyMap::new())?;

                Ok((site, building, floor, zone))
            })
            .unwrap();

        (shared, ids.0, ids.1, ids.2, ids.3)
    }

    #[test]
    fn walk_up_from_leaf() {
        let (shared, site, _building, _floor, zone) = test_graph();
        shared.read(|g| {
            let chain = walk_up(g, zone);
            assert_eq!(chain.len(), 4);
            assert_eq!(chain[0], zone);
            assert_eq!(chain[3], site);
        });
    }

    #[test]
    fn walk_up_from_root() {
        let (shared, site, _, _, _) = test_graph();
        shared.read(|g| {
            let chain = walk_up(g, site);
            assert_eq!(chain.len(), 1);
            assert_eq!(chain[0], site);
        });
    }

    #[test]
    fn resolve_scope_from_building() {
        let (shared, site, building, floor, zone) = test_graph();
        shared.read(|g| {
            let scope = resolve_scope(g, &[building]);
            assert_eq!(scope.len(), 3); // building + floor + zone
            assert!(scope.contains(building.0 as u32));
            assert!(scope.contains(floor.0 as u32));
            assert!(scope.contains(zone.0 as u32));
            assert!(!scope.contains(site.0 as u32));
        });
    }

    #[test]
    fn resolve_scope_from_root() {
        let (shared, site, _, _, _) = test_graph();
        shared.read(|g| {
            let scope = resolve_scope(g, &[site]);
            assert_eq!(scope.len(), 4);
        });
    }

    #[test]
    fn resolve_scope_multiple_roots() {
        let (shared, _site, building, _, _) = test_graph();

        // Add another building under site
        let (building2, _) = shared
            .write(|m| {
                let b2 = m.create_node(LabelSet::from_strs(&["building"]), PropertyMap::new())?;
                m.create_edge(_site, IStr::new("contains"), b2, PropertyMap::new())?;
                Ok(b2)
            })
            .unwrap();

        shared.read(|g| {
            let scope = resolve_scope(g, &[building, building2]);
            // building1 + floor + zone + building2
            assert_eq!(scope.len(), 4);
            assert!(scope.contains(building2.0 as u32));
            assert!(!scope.contains(_site.0 as u32));
        });
    }

    #[test]
    fn is_in_scope_check() {
        let (shared, site, building, _, zone) = test_graph();
        shared.read(|g| {
            let scope = resolve_scope(g, &[building]);
            assert!(is_in_scope(zone, &scope));
            assert!(!is_in_scope(site, &scope));
        });
    }
}
