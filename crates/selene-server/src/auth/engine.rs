//! Cedar authorization engine.
//!
//! Loads policies from disk, evaluates authorization requests against the
//! graph's containment hierarchy using Cedar's entity model.

use std::collections::HashSet;
use std::path::Path;
use std::str::FromStr;

use cedar_policy::{
    Authorizer, Context, Decision, Entities, Entity, EntityId, EntityTypeName, EntityUid,
    PolicySet, Request, Schema,
};
use selene_core::NodeId;
use selene_graph::SeleneGraph;

use super::projection;
use super::{Role, handshake::AuthContext};

/// The Cedar authorization engine.
pub struct AuthEngine {
    policy_set: PolicySet,
    schema: Option<cedar_policy::Schema>,
    authorizer: Authorizer,
}

/// Actions that map from MsgType to Cedar action names.
pub enum Action {
    EntityRead,
    EntityCreate,
    EntityModify,
    EntityDelete,
    TsWrite,
    TsRead,
    GqlQuery,
    GqlMutate,
    ChangelogSubscribe,
    PrincipalManage,
    PolicyManage,
    FederationManage,
}

impl Action {
    pub fn cedar_name(&self) -> &'static str {
        match self {
            Self::EntityRead => "entity:read",
            Self::EntityCreate => "entity:create",
            Self::EntityModify => "entity:modify",
            Self::EntityDelete => "entity:delete",
            Self::TsWrite => "ts:write",
            Self::TsRead => "ts:read",
            Self::GqlQuery => "gql:query",
            Self::GqlMutate => "gql:mutate",
            Self::ChangelogSubscribe => "changelog:subscribe",
            Self::PrincipalManage => "principal:manage",
            Self::PolicyManage => "policy:manage",
            Self::FederationManage => "federation:manage",
        }
    }
}

impl AuthEngine {
    /// Load the auth engine from policy files on disk.
    pub fn load(policy_dir: &Path) -> anyhow::Result<Self> {
        super::policies::ensure_defaults(policy_dir)?;

        let policy_files = super::policies::load_policy_files(policy_dir)?;
        let mut policy_set = PolicySet::new();
        for (path, content) in &policy_files {
            let ps = PolicySet::from_str(content)
                .map_err(|e| anyhow::anyhow!("failed to parse {}: {e}", path.display()))?;
            for policy in ps.policies() {
                policy_set
                    .add(policy.clone())
                    .map_err(|e| anyhow::anyhow!("duplicate policy ID: {e}"))?;
            }
        }

        // Load Cedar schema for policy validation (optional — graceful if missing)
        let schema = match super::policies::load_schema(policy_dir)? {
            Some(schema_text) => match Schema::from_str(&schema_text) {
                Ok(schema) => {
                    tracing::info!("loaded Cedar schema");
                    Some(schema)
                }
                Err(e) => {
                    tracing::warn!("failed to parse Cedar schema, continuing without: {e}");
                    None
                }
            },
            None => None,
        };

        tracing::info!(
            policies = policy_set.policies().count(),
            "loaded Cedar policies"
        );

        Ok(Self {
            policy_set,
            schema,
            authorizer: Authorizer::new(),
        })
    }

    /// Create a permissive engine for dev mode (allows everything).
    pub fn dev_mode() -> Self {
        let policy_text = r"permit(principal, action, resource);";
        let policy_set = PolicySet::from_str(policy_text).expect("dev policy should parse");

        Self {
            policy_set,
            schema: None,
            authorizer: Authorizer::new(),
        }
    }

    /// Authorize an action on a specific resource node.
    ///
    /// Admin principals always pass (global scope).
    /// Scoped principals must have the resource in their scope,
    /// and their role must permit the action per Cedar policies.
    pub fn authorize(&self, auth: &AuthContext, action: Action, resource_id: NodeId) -> bool {
        if auth.role == Role::Admin {
            return true;
        }

        // Scope check (fast path via bitmap)
        if !auth.scope.contains(resource_id.0 as u32) {
            return false;
        }

        self.evaluate_cedar(auth, &action, resource_id)
    }

    /// Authorize a scopeless action (like Health).
    pub fn authorize_action(&self, auth: &AuthContext, action: Action) -> bool {
        if auth.role == Role::Admin {
            return true;
        }

        self.evaluate_cedar(auth, &action, NodeId(0))
    }

    fn evaluate_cedar(&self, auth: &AuthContext, action: &Action, resource_id: NodeId) -> bool {
        let principal_uid = make_principal_uid(auth.principal_node_id);
        let action_uid = make_action_uid(action.cedar_name());
        let resource_uid = make_node_uid(resource_id);

        let principal_entity = make_principal_entity(auth);
        let resource_entity = Entity::new_no_attrs(resource_uid.clone(), HashSet::new());

        let entities = Entities::from_entities([principal_entity, resource_entity], None)
            .unwrap_or_else(|_| Entities::empty());

        let request = match Request::new(
            principal_uid,
            action_uid,
            resource_uid,
            Context::empty(),
            self.schema.as_ref(),
        ) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("Cedar request construction failed: {e}");
                return false;
            }
        };

        let response = self
            .authorizer
            .is_authorized(&request, &self.policy_set, &entities);
        response.decision() == Decision::Allow
    }

    /// Resolve the scope for a principal from the graph.
    ///
    /// For admins, returns None (global scope).
    /// For everyone else, walks the containment tree from scoped_to targets.
    pub fn resolve_scope(
        graph: &SeleneGraph,
        principal_id: NodeId,
        role: Role,
    ) -> Option<roaring::RoaringBitmap> {
        if role == Role::Admin {
            return None;
        }

        let roots = projection::scope_roots(graph, principal_id);
        Some(projection::resolve_scope(graph, &roots))
    }
}

fn make_principal_uid(node_id: NodeId) -> EntityUid {
    EntityUid::from_type_name_and_id(
        EntityTypeName::from_str("Selene::Principal").unwrap(),
        EntityId::new(node_id.0.to_string()),
    )
}

fn make_action_uid(action_name: &str) -> EntityUid {
    EntityUid::from_type_name_and_id(
        EntityTypeName::from_str("Selene::Action").unwrap(),
        EntityId::new(action_name),
    )
}

fn make_node_uid(node_id: NodeId) -> EntityUid {
    EntityUid::from_type_name_and_id(
        EntityTypeName::from_str("Selene::Node").unwrap(),
        EntityId::new(node_id.0.to_string()),
    )
}

fn make_principal_entity(auth: &AuthContext) -> Entity {
    let uid = make_principal_uid(auth.principal_node_id);
    let attrs = [(
        "role".into(),
        cedar_policy::RestrictedExpression::new_string(auth.role.as_str().to_string()),
    )]
    .into_iter()
    .collect();
    Entity::new(uid, attrs, HashSet::new()).expect("principal entity construction should not fail")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admin_ctx() -> AuthContext {
        AuthContext {
            principal_node_id: NodeId(100),
            role: Role::Admin,
            scope: roaring::RoaringBitmap::new(),
            scope_generation: 0,
        }
    }

    fn operator_ctx() -> AuthContext {
        let mut scope = roaring::RoaringBitmap::new();
        scope.insert(1);
        scope.insert(2);
        scope.insert(3);

        AuthContext {
            principal_node_id: NodeId(200),
            role: Role::Operator,
            scope,
            scope_generation: 0,
        }
    }

    #[test]
    fn admin_always_authorized() {
        let engine = AuthEngine::dev_mode();
        let ctx = admin_ctx();
        assert!(engine.authorize(&ctx, Action::EntityDelete, NodeId(999)));
        assert!(engine.authorize(&ctx, Action::PolicyManage, NodeId(999)));
    }

    #[test]
    fn scoped_principal_in_scope() {
        let engine = AuthEngine::dev_mode();
        let ctx = operator_ctx();
        assert!(engine.authorize(&ctx, Action::EntityRead, NodeId(1)));
        assert!(engine.authorize(&ctx, Action::EntityRead, NodeId(2)));
    }

    #[test]
    fn scoped_principal_out_of_scope() {
        let engine = AuthEngine::dev_mode();
        let ctx = operator_ctx();
        assert!(!engine.authorize(&ctx, Action::EntityRead, NodeId(999)));
    }

    #[test]
    fn loaded_policies_enforce_roles() {
        let dir = tempfile::tempdir().unwrap();
        let policy_dir = dir.path().join("policies");
        let engine = AuthEngine::load(&policy_dir).unwrap();

        let ctx = operator_ctx();
        // Operator can read entities
        assert!(engine.authorize(&ctx, Action::EntityRead, NodeId(1)));
        // Operator cannot manage policies
        assert!(!engine.authorize(&ctx, Action::PolicyManage, NodeId(1)));
    }

    #[test]
    fn reader_cannot_write() {
        let dir = tempfile::tempdir().unwrap();
        let policy_dir = dir.path().join("policies");
        let engine = AuthEngine::load(&policy_dir).unwrap();

        let mut scope = roaring::RoaringBitmap::new();
        scope.insert(1);
        let ctx = AuthContext {
            principal_node_id: NodeId(300),
            role: Role::Reader,
            scope,
            scope_generation: 0,
        };

        assert!(engine.authorize(&ctx, Action::EntityRead, NodeId(1)));
        assert!(engine.authorize(&ctx, Action::GqlQuery, NodeId(1)));
        assert!(!engine.authorize(&ctx, Action::EntityCreate, NodeId(1)));
        assert!(!engine.authorize(&ctx, Action::EntityDelete, NodeId(1)));
    }

    #[test]
    fn device_can_only_write_ts() {
        let dir = tempfile::tempdir().unwrap();
        let policy_dir = dir.path().join("policies");
        let engine = AuthEngine::load(&policy_dir).unwrap();

        let mut scope = roaring::RoaringBitmap::new();
        scope.insert(42);
        let ctx = AuthContext {
            principal_node_id: NodeId(400),
            role: Role::Device,
            scope,
            scope_generation: 0,
        };

        assert!(engine.authorize(&ctx, Action::TsWrite, NodeId(42)));
        assert!(!engine.authorize(&ctx, Action::EntityRead, NodeId(42)));
        assert!(!engine.authorize(&ctx, Action::GqlQuery, NodeId(42)));
    }

    #[test]
    fn operator_can_gql_query_and_mutate() {
        let dir = tempfile::tempdir().unwrap();
        let policy_dir = dir.path().join("policies");
        let engine = AuthEngine::load(&policy_dir).unwrap();

        let ctx = operator_ctx();
        assert!(engine.authorize(&ctx, Action::GqlQuery, NodeId(1)));
        assert!(engine.authorize(&ctx, Action::GqlMutate, NodeId(1)));
    }

    #[test]
    fn reader_can_gql_query_not_mutate() {
        let dir = tempfile::tempdir().unwrap();
        let policy_dir = dir.path().join("policies");
        let engine = AuthEngine::load(&policy_dir).unwrap();

        let mut scope = roaring::RoaringBitmap::new();
        scope.insert(1);
        let ctx = AuthContext {
            principal_node_id: NodeId(300),
            role: Role::Reader,
            scope,
            scope_generation: 0,
        };

        assert!(engine.authorize(&ctx, Action::GqlQuery, NodeId(1)));
        assert!(!engine.authorize(&ctx, Action::GqlMutate, NodeId(1)));
    }

    #[test]
    fn service_can_federation_manage() {
        let dir = tempfile::tempdir().unwrap();
        let policy_dir = dir.path().join("policies");
        let engine = AuthEngine::load(&policy_dir).unwrap();

        let mut scope = roaring::RoaringBitmap::new();
        scope.insert(1);
        let ctx = AuthContext {
            principal_node_id: NodeId(500),
            role: Role::Service,
            scope,
            scope_generation: 0,
        };

        // Service role can manage federation (sync push)
        assert!(engine.authorize(&ctx, Action::FederationManage, NodeId(1)));
        // Service role can also do standard operations
        assert!(engine.authorize(&ctx, Action::EntityRead, NodeId(1)));
        assert!(engine.authorize(&ctx, Action::GqlQuery, NodeId(1)));
        // Service role cannot manage principals or policies
        assert!(!engine.authorize(&ctx, Action::PrincipalManage, NodeId(1)));
        assert!(!engine.authorize(&ctx, Action::PolicyManage, NodeId(1)));
    }

    #[test]
    fn operator_cannot_federation_manage() {
        let dir = tempfile::tempdir().unwrap();
        let policy_dir = dir.path().join("policies");
        let engine = AuthEngine::load(&policy_dir).unwrap();

        let ctx = operator_ctx();
        assert!(!engine.authorize(&ctx, Action::FederationManage, NodeId(1)));
    }
}
