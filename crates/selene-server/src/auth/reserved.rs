//! Reserved labels and edge labels for the main graph.
//!
//! Security-sensitive state (principals, API keys, revoked-token deny-list,
//! JWT signing keys, vault audit log) lives in the vault graph, not the main
//! graph. Any attempt to create or modify nodes bearing these labels in the
//! main graph — or to create `scoped_to` edges there — is rejected at the
//! ops boundary, regardless of role.
//!
//! The rationale is closure: if reserved labels cannot exist in the main
//! graph at all, then there is no generic-CRUD or GQL-mutation path by which
//! a scoped writer can mint a principal, forge an API key, or wire a
//! `scoped_to` edge to escalate their own authority.

use selene_core::{IStr, LabelSet};

use crate::ops::OpError;

/// Node labels whose presence in the main graph is forbidden. These are
/// either vault-only (`principal`, `api_key`, `revoked_token`, `signing_key`,
/// `audit_log`) or otherwise security-reserved. See module doc for rationale.
pub const RESERVED_NODE_LABELS: &[&str] = &[
    "principal",
    "api_key",
    "revoked_token",
    "signing_key",
    "audit_log",
];

/// Edge labels whose presence in the main graph is forbidden. `scoped_to` is
/// auth-semantic: it previously (pre-1.3.0) attached a principal to a scope
/// root. Post-unification, scope roots are stored on the vault principal, so
/// this edge label has no legitimate main-graph use.
pub const RESERVED_EDGE_LABELS: &[&str] = &["scoped_to"];

/// Return `Err(OpError::Forbidden)` if `label` is reserved. Otherwise `Ok`.
///
/// `Forbidden` (vs `InvalidRequest`) is deliberate: reserved-label rejection
/// is an authorization outcome (the operation would have been valid for a
/// caller with the right authority, but the label itself is forbidden to
/// the main graph entirely), so it should surface as HTTP 403 and GQLSTATUS
/// `42501` rather than HTTP 400. Use when the caller passes a single label
/// by value (e.g., `create_edge`).
pub fn reject_reserved_node_label(label: &str) -> Result<(), OpError> {
    if RESERVED_NODE_LABELS.contains(&label) {
        return Err(OpError::Forbidden(format!(
            "label '{label}' is reserved — use the dedicated admin API or vault graph"
        )));
    }
    Ok(())
}

/// Return `Err(OpError::Forbidden)` if `label` is a reserved edge label.
pub fn reject_reserved_edge_label(label: &str) -> Result<(), OpError> {
    if RESERVED_EDGE_LABELS.contains(&label) {
        return Err(OpError::Forbidden(format!(
            "edge label '{label}' is reserved — use the dedicated admin API"
        )));
    }
    Ok(())
}

/// Scan a `LabelSet` for reserved labels, rejecting on first match.
pub fn reject_reserved_labels(labels: &LabelSet) -> Result<(), OpError> {
    for label in labels.iter() {
        reject_reserved_node_label(label.as_str())?;
    }
    Ok(())
}

/// Scan a slice of `IStr` labels (e.g., `add_labels` on `modify_node`).
pub fn reject_reserved_label_istrs(labels: &[IStr]) -> Result<(), OpError> {
    for label in labels {
        reject_reserved_node_label(label.as_str())?;
    }
    Ok(())
}

/// Walk a parsed `MutationPipeline` and reject if it would create, alter, or
/// wire any reserved label/edge-label in the target graph.
///
/// Covers: `InsertPattern` node labels, `InsertPattern` edge labels, and
/// `SetLabel` (which widens a node's label set — equivalent to creating one).
/// `RemoveLabel` is not rejected — removing a reserved label from a stray
/// node cleans up legacy data and poses no escalation risk.
pub fn reject_reserved_in_mutation(pipeline: &selene_gql::MutationPipeline) -> Result<(), OpError> {
    for op in &pipeline.mutations {
        match op {
            selene_gql::MutationOp::InsertPattern(pattern) => {
                for path in &pattern.paths {
                    for element in &path.elements {
                        match element {
                            selene_gql::InsertElement::Node { labels, .. } => {
                                for label in labels {
                                    reject_reserved_node_label(label.as_str())?;
                                }
                            }
                            selene_gql::InsertElement::Edge { label, .. } => {
                                if let Some(l) = label {
                                    reject_reserved_edge_label(l.as_str())?;
                                }
                            }
                        }
                    }
                }
            }
            selene_gql::MutationOp::SetLabel { label, .. } => {
                reject_reserved_node_label(label.as_str())?;
            }
            selene_gql::MutationOp::Merge { labels, .. } => {
                for label in labels {
                    reject_reserved_node_label(label.as_str())?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::LabelSet;

    #[test]
    fn reserved_node_label_set_is_rejected() {
        for name in RESERVED_NODE_LABELS {
            let labels = LabelSet::from_strs(&[*name]);
            assert!(
                reject_reserved_labels(&labels).is_err(),
                "label '{name}' should be reserved"
            );
        }
    }

    #[test]
    fn ordinary_node_label_passes() {
        let labels = LabelSet::from_strs(&["sensor", "building"]);
        assert!(reject_reserved_labels(&labels).is_ok());
    }

    #[test]
    fn reserved_node_label_among_others_is_rejected() {
        let labels = LabelSet::from_strs(&["sensor", "principal"]);
        assert!(reject_reserved_labels(&labels).is_err());
    }

    #[test]
    fn reserved_edge_label_rejected() {
        assert!(reject_reserved_edge_label("scoped_to").is_err());
        assert!(reject_reserved_edge_label("contains").is_ok());
    }

    #[test]
    fn label_istrs_reject_reserved() {
        use selene_core::IStr;
        let ok = [IStr::new("sensor"), IStr::new("zone")];
        assert!(reject_reserved_label_istrs(&ok).is_ok());
        let bad = [IStr::new("sensor"), IStr::new("api_key")];
        assert!(reject_reserved_label_istrs(&bad).is_err());
    }

    #[test]
    fn mutation_scan_rejects_principal_insert() {
        let stmt = selene_gql::parse_statement("INSERT (:principal {identity: 'x'})").unwrap();
        let selene_gql::GqlStatement::Mutate(ref pipeline) = stmt else {
            panic!("expected Mutate");
        };
        assert!(reject_reserved_in_mutation(pipeline).is_err());
    }

    #[test]
    fn mutation_scan_rejects_api_key_insert() {
        let stmt = selene_gql::parse_statement("INSERT (:api_key {token: 'x'})").unwrap();
        let selene_gql::GqlStatement::Mutate(ref pipeline) = stmt else {
            panic!("expected Mutate");
        };
        assert!(reject_reserved_in_mutation(pipeline).is_err());
    }

    #[test]
    fn mutation_scan_rejects_scoped_to_edge() {
        let stmt = selene_gql::parse_statement(
            "MATCH (a) WHERE id(a) = 1 MATCH (b) WHERE id(b) = 2 INSERT (a)-[:scoped_to]->(b)",
        )
        .unwrap();
        let selene_gql::GqlStatement::Mutate(ref pipeline) = stmt else {
            panic!("expected Mutate");
        };
        assert!(reject_reserved_in_mutation(pipeline).is_err());
    }

    #[test]
    fn mutation_scan_rejects_set_label_to_reserved() {
        let stmt =
            selene_gql::parse_statement("MATCH (n) WHERE id(n) = 1 SET n IS principal").unwrap();
        let selene_gql::GqlStatement::Mutate(ref pipeline) = stmt else {
            panic!("expected Mutate");
        };
        assert!(reject_reserved_in_mutation(pipeline).is_err());
    }

    #[test]
    fn mutation_scan_accepts_ordinary_insert() {
        let stmt = selene_gql::parse_statement("INSERT (:sensor {name: 'S1'})").unwrap();
        let selene_gql::GqlStatement::Mutate(ref pipeline) = stmt else {
            panic!("expected Mutate");
        };
        assert!(reject_reserved_in_mutation(pipeline).is_ok());
    }
}
