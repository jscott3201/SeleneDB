//! Vault audit logging — every mutation creates a tamper-resistant audit entry.
//!
//! Audit nodes live inside the encrypted vault, making them tamper-resistant
//! (an attacker without the master key cannot read or forge entries).

use selene_core::{IStr, LabelSet, PropertyMap, Value, now_nanos};
use selene_graph::SharedGraph;

/// Log an audit entry into the vault graph.
///
/// Creates an `:audit_log` node with principal, action, details, and timestamp.
/// This is called after every vault mutation.
pub fn log_audit(graph: &SharedGraph, principal_identity: &str, action: &str, details: &str) {
    let result = graph.write(|m| {
        m.create_node(
            LabelSet::from_strs(&["audit_log"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("principal"), Value::str(principal_identity)),
                (IStr::new("action"), Value::str(action)),
                (IStr::new("details"), Value::str(details)),
                (IStr::new("timestamp"), Value::Timestamp(now_nanos())),
            ]),
        )?;
        Ok(())
    });

    if let Err(e) = result {
        tracing::error!("failed to write vault audit log: {e}");
    }
}

#[cfg(test)]
mod tests {
    use selene_graph::{SeleneGraph, SharedGraph};

    use super::*;

    // 1 ─────────────────────────────────────────────────────────────────
    /// log_audit creates an :audit_log node with the correct properties.
    #[test]
    fn log_audit_creates_node() {
        let graph = SharedGraph::new(SeleneGraph::new());

        log_audit(&graph, "admin", "rotate_key", "rotated DEK");

        let snapshot = graph.load_snapshot();
        assert_eq!(snapshot.node_count(), 1);

        let node = snapshot.get_node(selene_core::NodeId(1)).unwrap();
        assert!(node.labels.contains(IStr::new("audit_log")));
        assert_eq!(
            node.properties.get(IStr::new("principal")),
            Some(&Value::str("admin"))
        );
        assert_eq!(
            node.properties.get(IStr::new("action")),
            Some(&Value::str("rotate_key"))
        );
        assert_eq!(
            node.properties.get(IStr::new("details")),
            Some(&Value::str("rotated DEK"))
        );
        // Timestamp must be present and positive.
        let ts = node.properties.get(IStr::new("timestamp"));
        assert!(
            matches!(ts, Some(Value::Timestamp(t)) if *t > 0),
            "timestamp must be a positive Timestamp"
        );
    }

    // 2 ─────────────────────────────────────────────────────────────────
    /// Multiple audit entries produce sequential nodes.
    #[test]
    fn log_audit_multiple_entries() {
        let graph = SharedGraph::new(SeleneGraph::new());

        log_audit(&graph, "admin", "create_key", "first");
        log_audit(&graph, "service", "read_secret", "second");
        log_audit(&graph, "admin", "delete_key", "third");

        let snapshot = graph.load_snapshot();
        assert_eq!(snapshot.node_count(), 3);
    }

    // 3 ─────────────────────────────────────────────────────────────────
    /// Audit log handles empty strings for principal, action, and details.
    #[test]
    fn log_audit_empty_strings() {
        let graph = SharedGraph::new(SeleneGraph::new());

        log_audit(&graph, "", "", "");

        let snapshot = graph.load_snapshot();
        assert_eq!(snapshot.node_count(), 1);
        let node = snapshot.get_node(selene_core::NodeId(1)).unwrap();
        assert_eq!(
            node.properties.get(IStr::new("principal")),
            Some(&Value::str(""))
        );
    }
}
