#![forbid(unsafe_code)]
//! Schema packs for Selene: community-driven ontology definitions.
//!
//! # Formats
//!
//! - **Compact TOML** - `[types.label]` with `fields = { name = "string!" }` shorthand
//! - **Compact JSON** - same shorthand in JSON (better tooling support)
//! - **TTL/JSON-LD** - import ontologies directly (Brick, Haystack, 223P) *(Phase 2)*
//!
//! # Field Shorthand
//!
//! ```text
//! "string"          → optional String
//! "string!"         → required String
//! "string = '°F'"   → optional String with default "°F"
//! "float = 72.5"    → optional Float with default 72.5
//! "int = 60"        → optional Int with default 60
//! "bool = true"     → optional Bool with default true
//! ```

pub mod compact;
mod loader;

pub use compact::{load_compact_json, load_compact_toml, parse_field_spec};
pub use loader::PackError;
pub use selene_core::schema::SchemaPack;

/// Load a schema pack from a string, auto-detecting format (JSON or TOML).
pub fn load_from_str(input: &str) -> Result<SchemaPack, PackError> {
    let trimmed = input.trim_start();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        load_compact_json(input)
    } else {
        load_compact_toml(input)
    }
}

/// Built-in pack sources, embedded at compile time.
static COMMON_PACK: &str = include_str!("../packs/common.toml");

/// Load a built-in schema pack by name.
///
/// Available packs: `"common"`.
pub fn builtin(name: &str) -> Option<SchemaPack> {
    let toml = match name {
        "common" => COMMON_PACK,
        _ => return None,
    };
    load_compact_toml(toml).ok()
}

/// List the names of all available built-in packs.
pub fn available_packs() -> Vec<&'static str> {
    vec!["common"]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_common_pack() {
        let pack = builtin("common").expect("common pack should load");
        assert_eq!(pack.name, "common");
        assert!(!pack.nodes.is_empty());
        assert!(!pack.edges.is_empty());
    }

    #[test]
    fn unknown_pack_returns_none() {
        assert!(builtin("nonexistent").is_none());
    }

    #[test]
    fn available_packs_lists_all() {
        let packs = available_packs();
        assert!(packs.contains(&"common"));
    }

    #[test]
    fn all_packs_have_descriptions() {
        for name in available_packs() {
            let pack = builtin(name).unwrap();
            assert!(
                !pack.description.is_empty(),
                "{name} pack missing description"
            );
        }
    }

    #[test]
    fn common_pack_has_core_types() {
        let pack = builtin("common").unwrap();
        let labels: Vec<&str> = pack.nodes.iter().map(|n| n.label.as_ref()).collect();
        assert!(labels.contains(&"site"));
        assert!(labels.contains(&"building"));
        assert!(labels.contains(&"equipment"));
        assert!(labels.contains(&"point"));
    }

    #[test]
    fn load_from_str_json() {
        let json = r#"{"name":"test","version":"1.0","types":{"x":{"fields":{"n":"string!"}}}}"#;
        let pack = load_from_str(json).unwrap();
        assert_eq!(pack.name, "test");
    }

    #[test]
    fn load_from_str_toml() {
        let toml =
            "name = \"test\"\nversion = \"1.0\"\n\n[types.x]\nfields = { n = \"string!\" }\n";
        let pack = load_from_str(toml).unwrap();
        assert_eq!(pack.name, "test");
    }
}
