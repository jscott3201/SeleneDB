//! Materialized view definitions for incremental view maintenance.
//!
//! Stores view metadata inside `SeleneGraph`. Aggregate state lives
//! in the server layer (`ViewStateStore`), not here. Definitions
//! persist through snapshots (serialized as extra section 0x04).
//! Aggregate state lives in the server layer and rebuilds on startup.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Persistent definition of a materialized view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewDefinition {
    /// View name (stored uppercase for case-insensitive lookup).
    pub name: String,
    /// Original GQL text of the defining query (MATCH ... RETURN ...).
    pub definition_text: String,
    /// Labels referenced in the MATCH pattern (for changelog filtering).
    pub match_labels: Vec<String>,
    /// Properties referenced in WHERE predicates (for change filtering).
    pub predicate_properties: Vec<String>,
    /// Aggregate columns.
    pub aggregates: Vec<ViewAggregate>,
}

/// One aggregate column in a materialized view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewAggregate {
    /// Output column alias (from RETURN ... AS alias).
    pub alias: String,
    /// Aggregate function kind.
    pub kind: ViewAggregateKind,
    /// Source property name (e.g., "temp" in `avg(s.temp)`). None for `count(*)`.
    pub source_property: Option<String>,
}

/// The kind of aggregate function in a materialized view column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ViewAggregateKind {
    Count,
    CountStar,
    Sum,
    Avg,
    Min,
    Max,
    Collect,
    /// Unsupported aggregate: requires full recomputation on every change.
    FullRecompute,
}

/// Registry of materialized view definitions.
///
/// Lives inside `SeleneGraph`, persisted via snapshots.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ViewRegistry {
    views: HashMap<String, ViewDefinition>,
}

impl ViewRegistry {
    pub fn new() -> Self {
        Self {
            views: HashMap::new(),
        }
    }

    /// Register a new view. Returns error if name already exists.
    pub fn register(&mut self, def: ViewDefinition) -> Result<(), ViewRegistryError> {
        if self.views.contains_key(&def.name) {
            return Err(ViewRegistryError::AlreadyExists(def.name));
        }
        self.views.insert(def.name.clone(), def);
        Ok(())
    }

    /// Register or replace an existing view.
    pub fn register_or_replace(&mut self, def: ViewDefinition) {
        self.views.insert(def.name.clone(), def);
    }

    /// Remove a view by name. Returns error if not found.
    pub fn remove(&mut self, name: &str) -> Result<ViewDefinition, ViewRegistryError> {
        self.views
            .remove(name)
            .ok_or_else(|| ViewRegistryError::NotFound(name.to_string()))
    }

    /// Remove a view if it exists. Returns `None` if not found (no error).
    pub fn remove_if_exists(&mut self, name: &str) -> Option<ViewDefinition> {
        self.views.remove(name)
    }

    /// Get a view definition by name.
    pub fn get(&self, name: &str) -> Option<&ViewDefinition> {
        self.views.get(name)
    }

    /// Check if a view exists.
    pub fn contains(&self, name: &str) -> bool {
        self.views.contains_key(name)
    }

    /// List all view definitions.
    pub fn list(&self) -> Vec<&ViewDefinition> {
        self.views.values().collect()
    }

    /// Serialize all definitions for snapshot persistence.
    pub fn to_vec(&self) -> Vec<ViewDefinition> {
        self.views.values().cloned().collect()
    }

    /// Restore definitions from snapshot recovery.
    pub fn restore(&mut self, defs: Vec<ViewDefinition>) {
        self.views.clear();
        for def in defs {
            self.views.insert(def.name.clone(), def);
        }
    }

    /// Number of registered views.
    pub fn len(&self) -> usize {
        self.views.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.views.is_empty()
    }
}

/// Errors from view registry operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ViewRegistryError {
    #[error("materialized view '{0}' already exists")]
    AlreadyExists(String),
    #[error("materialized view '{0}' not found")]
    NotFound(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_view() -> ViewDefinition {
        ViewDefinition {
            name: "SENSOR_STATS".to_string(),
            definition_text: "MATCH (s:Sensor) RETURN avg(s.temp) AS avg_temp".to_string(),
            match_labels: vec!["Sensor".to_string()],
            predicate_properties: vec![],
            aggregates: vec![ViewAggregate {
                alias: "avg_temp".to_string(),
                kind: ViewAggregateKind::Avg,
                source_property: Some("temp".to_string()),
            }],
        }
    }

    fn another_view() -> ViewDefinition {
        ViewDefinition {
            name: "ZONE_COUNTS".to_string(),
            definition_text: "MATCH (z:Zone) RETURN count(*) AS total".to_string(),
            match_labels: vec!["Zone".to_string()],
            predicate_properties: vec![],
            aggregates: vec![ViewAggregate {
                alias: "total".to_string(),
                kind: ViewAggregateKind::CountStar,
                source_property: None,
            }],
        }
    }

    #[test]
    fn register_and_get() {
        let mut reg = ViewRegistry::new();
        reg.register(sample_view()).unwrap();

        let view = reg.get("SENSOR_STATS").unwrap();
        assert_eq!(view.name, "SENSOR_STATS");
        assert_eq!(view.aggregates.len(), 1);
        assert_eq!(view.aggregates[0].kind, ViewAggregateKind::Avg);
    }

    #[test]
    fn register_duplicate_fails() {
        let mut reg = ViewRegistry::new();
        reg.register(sample_view()).unwrap();

        let err = reg.register(sample_view()).unwrap_err();
        assert!(matches!(err, ViewRegistryError::AlreadyExists(_)));
    }

    #[test]
    fn register_or_replace() {
        let mut reg = ViewRegistry::new();
        reg.register(sample_view()).unwrap();

        let mut replacement = sample_view();
        replacement.definition_text = "MATCH (s:Sensor) RETURN sum(s.temp) AS sum_temp".to_string();
        reg.register_or_replace(replacement);

        let view = reg.get("SENSOR_STATS").unwrap();
        assert!(view.definition_text.contains("sum"));
    }

    #[test]
    fn remove_existing() {
        let mut reg = ViewRegistry::new();
        reg.register(sample_view()).unwrap();

        let removed = reg.remove("SENSOR_STATS").unwrap();
        assert_eq!(removed.name, "SENSOR_STATS");
        assert!(reg.is_empty());
        assert!(reg.get("SENSOR_STATS").is_none());
    }

    #[test]
    fn remove_nonexistent_fails() {
        let mut reg = ViewRegistry::new();
        let err = reg.remove("NOPE").unwrap_err();
        assert!(matches!(err, ViewRegistryError::NotFound(_)));
    }

    #[test]
    fn remove_if_exists() {
        let mut reg = ViewRegistry::new();
        assert!(reg.remove_if_exists("NOPE").is_none());

        reg.register(sample_view()).unwrap();
        let removed = reg.remove_if_exists("SENSOR_STATS");
        assert!(removed.is_some());
        assert!(reg.is_empty());
    }

    #[test]
    fn list_views() {
        let mut reg = ViewRegistry::new();
        reg.register(sample_view()).unwrap();
        reg.register(another_view()).unwrap();

        assert_eq!(reg.list().len(), 2);
        assert_eq!(reg.len(), 2);
        assert!(!reg.is_empty());
    }

    #[test]
    fn contains_check() {
        let mut reg = ViewRegistry::new();
        assert!(!reg.contains("SENSOR_STATS"));

        reg.register(sample_view()).unwrap();
        assert!(reg.contains("SENSOR_STATS"));
        assert!(!reg.contains("OTHER"));
    }

    #[test]
    fn restore_clears_existing() {
        let mut reg = ViewRegistry::new();
        reg.register(sample_view()).unwrap();
        assert_eq!(reg.len(), 1);

        let new_defs = vec![another_view()];
        reg.restore(new_defs);

        assert_eq!(reg.len(), 1);
        assert!(reg.get("SENSOR_STATS").is_none());
        assert!(reg.get("ZONE_COUNTS").is_some());
    }

    #[test]
    fn to_vec_roundtrip() {
        let mut reg = ViewRegistry::new();
        reg.register(sample_view()).unwrap();
        reg.register(another_view()).unwrap();

        let serialized = reg.to_vec();
        assert_eq!(serialized.len(), 2);

        let mut reg2 = ViewRegistry::new();
        reg2.restore(serialized);
        assert_eq!(reg2.len(), 2);
        assert!(reg2.get("SENSOR_STATS").is_some());
        assert!(reg2.get("ZONE_COUNTS").is_some());
    }

    #[test]
    fn serde_roundtrip() {
        let def = sample_view();
        let bytes = postcard::to_allocvec(&def).unwrap();
        let restored: ViewDefinition = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(restored.name, def.name);
        assert_eq!(restored.definition_text, def.definition_text);
        assert_eq!(restored.aggregates.len(), 1);
        assert_eq!(restored.aggregates[0].kind, ViewAggregateKind::Avg);
        assert_eq!(
            restored.aggregates[0].source_property,
            Some("temp".to_string())
        );
    }

    #[test]
    fn default_is_empty() {
        let reg = ViewRegistry::default();
        assert!(reg.is_empty());
        assert_eq!(reg.len(), 0);
    }
}
