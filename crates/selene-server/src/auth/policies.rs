//! Default Cedar policy generation and loading.
//!
//! Writes default `.cedar` files on first startup, loads all policies from disk.

use std::fs;
use std::path::{Path, PathBuf};

/// The default Cedar schema for Selene authorization.
pub const CEDAR_SCHEMA: &str = r#"
namespace Selene {
    entity Principal = {
        role: String,
    };

    entity Node in [Node];

    action "entity:read"         appliesTo { principal: Principal, resource: Node };
    action "entity:create"       appliesTo { principal: Principal, resource: Node };
    action "entity:modify"       appliesTo { principal: Principal, resource: Node };
    action "entity:delete"       appliesTo { principal: Principal, resource: Node };
    action "ts:write"            appliesTo { principal: Principal, resource: Node };
    action "ts:read"             appliesTo { principal: Principal, resource: Node };
    action "gql:query"           appliesTo { principal: Principal, resource: Node };
    action "gql:mutate"          appliesTo { principal: Principal, resource: Node };
    action "changelog:subscribe" appliesTo { principal: Principal, resource: Node };
    action "principal:manage"    appliesTo { principal: Principal, resource: Node };
    action "policy:manage"       appliesTo { principal: Principal, resource: Node };
    action "federation:manage"   appliesTo { principal: Principal, resource: Node };
}
"#;

/// The default Cedar policies implementing the five-role model.
pub const DEFAULT_POLICIES: &str = r#"
// Admin: full access, no scope restriction
permit(
    principal,
    action,
    resource
) when {
    principal.role == "admin"
};

// Service: scoped CRUD, GQL, TS, changelog, federation (no principal/policy management)
permit(
    principal,
    action in [
        Selene::Action::"entity:read",
        Selene::Action::"entity:create",
        Selene::Action::"entity:modify",
        Selene::Action::"entity:delete",
        Selene::Action::"ts:write",
        Selene::Action::"ts:read",
        Selene::Action::"gql:query",
        Selene::Action::"gql:mutate",
        Selene::Action::"changelog:subscribe",
        Selene::Action::"federation:manage"
    ],
    resource
) when {
    principal.role == "service"
};

// Operator: scoped CRUD, GQL, TS (no changelog, no management)
permit(
    principal,
    action in [
        Selene::Action::"entity:read",
        Selene::Action::"entity:create",
        Selene::Action::"entity:modify",
        Selene::Action::"entity:delete",
        Selene::Action::"ts:write",
        Selene::Action::"ts:read",
        Selene::Action::"gql:query",
        Selene::Action::"gql:mutate"
    ],
    resource
) when {
    principal.role == "operator"
};

// Reader: scoped read-only (TS read, GQL query, entity read)
permit(
    principal,
    action in [
        Selene::Action::"entity:read",
        Selene::Action::"ts:read",
        Selene::Action::"gql:query"
    ],
    resource
) when {
    principal.role == "reader"
};

// Device: TS write only on own entity (scope enforced by engine)
permit(
    principal,
    action == Selene::Action::"ts:write",
    resource
) when {
    principal.role == "device"
};
"#;

/// Ensure default policy files exist in the policy directory.
///
/// Creates the directory and writes `schema.cedarschema` and `default.cedar`
/// if they don't already exist.
pub fn ensure_defaults(policy_dir: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(policy_dir)?;

    let schema_path = policy_dir.join("schema.cedarschema");
    if !schema_path.exists() {
        fs::write(&schema_path, CEDAR_SCHEMA)?;
        tracing::info!(path = %schema_path.display(), "wrote default Cedar schema");
    }

    let policy_path = policy_dir.join("default.cedar");
    if !policy_path.exists() {
        fs::write(&policy_path, DEFAULT_POLICIES)?;
        tracing::info!(path = %policy_path.display(), "wrote default Cedar policies");
    }

    Ok(())
}

/// Load all `.cedar` policy files from a directory.
pub fn load_policy_files(policy_dir: &Path) -> anyhow::Result<Vec<(PathBuf, String)>> {
    let mut policies = Vec::new();

    if !policy_dir.exists() {
        return Ok(policies);
    }

    for entry in fs::read_dir(policy_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "cedar") {
            let content = fs::read_to_string(&path)?;
            policies.push((path, content));
        }
    }

    Ok(policies)
}

/// Load the Cedar schema file from the policy directory.
pub fn load_schema(policy_dir: &Path) -> anyhow::Result<Option<String>> {
    let schema_path = policy_dir.join("schema.cedarschema");
    if schema_path.exists() {
        Ok(Some(fs::read_to_string(schema_path)?))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_defaults_creates_files() {
        let dir = tempfile::tempdir().unwrap();
        let policy_dir = dir.path().join("policies");

        ensure_defaults(&policy_dir).unwrap();

        assert!(policy_dir.join("schema.cedarschema").exists());
        assert!(policy_dir.join("default.cedar").exists());
    }

    #[test]
    fn ensure_defaults_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let policy_dir = dir.path().join("policies");

        ensure_defaults(&policy_dir).unwrap();
        // Modify the file
        fs::write(policy_dir.join("default.cedar"), "custom policy").unwrap();
        // Re-run — should not overwrite
        ensure_defaults(&policy_dir).unwrap();
        let content = fs::read_to_string(policy_dir.join("default.cedar")).unwrap();
        assert_eq!(content, "custom policy");
    }

    #[test]
    fn load_policy_files_finds_cedar() {
        let dir = tempfile::tempdir().unwrap();
        let policy_dir = dir.path().join("policies");
        fs::create_dir_all(&policy_dir).unwrap();
        fs::write(policy_dir.join("a.cedar"), "policy a").unwrap();
        fs::write(policy_dir.join("b.cedar"), "policy b").unwrap();
        fs::write(policy_dir.join("readme.txt"), "not a policy").unwrap();

        let policies = load_policy_files(&policy_dir).unwrap();
        assert_eq!(policies.len(), 2);
    }
}
