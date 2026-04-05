//! Trigger registry -- stores, retrieves, and matches trigger definitions.
//!
//! The registry maintains a primary map (by name) and a secondary index
//! (by event+label) for O(1) lookup when change events fire.

use std::collections::HashMap;

use selene_core::IStr;
use selene_core::trigger::{TriggerDef, TriggerEvent};

use crate::error::GraphError;

/// In-memory registry of active triggers.
#[derive(Clone, Debug)]
pub struct TriggerRegistry {
    /// All triggers by name.
    triggers: HashMap<String, TriggerDef>,
    /// Index: (event, label) → trigger names for fast lookup.
    by_event_label: HashMap<(TriggerEvent, IStr), Vec<String>>,
}

impl TriggerRegistry {
    pub fn new() -> Self {
        Self {
            triggers: HashMap::new(),
            by_event_label: HashMap::new(),
        }
    }

    /// Register a new trigger. Returns error if name already exists.
    pub fn register(&mut self, trigger: TriggerDef) -> Result<(), GraphError> {
        let name = trigger.name.to_string();
        if self.triggers.contains_key(&name) {
            return Err(GraphError::Other(format!(
                "trigger '{name}' already exists"
            )));
        }

        let index_key = (trigger.event, IStr::new(&trigger.label));
        self.by_event_label
            .entry(index_key)
            .or_default()
            .push(name.clone());
        self.triggers.insert(name, trigger);
        Ok(())
    }

    /// Remove a trigger by name.
    pub fn remove(&mut self, name: &str) -> Result<TriggerDef, GraphError> {
        let trigger = self
            .triggers
            .remove(name)
            .ok_or_else(|| GraphError::Other(format!("trigger '{name}' not found")))?;

        let index_key = (trigger.event, IStr::new(&trigger.label));
        if let Some(names) = self.by_event_label.get_mut(&index_key) {
            names.retain(|n| n != name);
            if names.is_empty() {
                self.by_event_label.remove(&index_key);
            }
        }

        Ok(trigger)
    }

    /// Find all triggers matching an event type and label.
    pub fn matching(&self, event: TriggerEvent, label: IStr) -> Vec<&TriggerDef> {
        self.by_event_label
            .get(&(event, label))
            .map(|names| names.iter().filter_map(|n| self.triggers.get(n)).collect())
            .unwrap_or_default()
    }

    /// Returns true if the registry has no triggers.
    pub fn is_empty(&self) -> bool {
        self.triggers.is_empty()
    }

    /// List all triggers.
    pub fn list(&self) -> Vec<&TriggerDef> {
        self.triggers.values().collect()
    }

    /// Serialize all triggers for snapshot persistence.
    pub fn to_vec(&self) -> Vec<TriggerDef> {
        self.triggers.values().cloned().collect()
    }

    /// Load triggers from snapshot, replacing any existing state.
    pub fn load(&mut self, triggers: Vec<TriggerDef>) {
        self.triggers.clear();
        self.by_event_label.clear();
        for trigger in triggers {
            // Use internal insert to skip duplicate check
            let name = trigger.name.to_string();
            let index_key = (trigger.event, IStr::new(&trigger.label));
            self.by_event_label
                .entry(index_key)
                .or_default()
                .push(name.clone());
            self.triggers.insert(name, trigger);
        }
    }
}

impl Default for TriggerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn test_trigger(name: &str, event: TriggerEvent, label: &str) -> TriggerDef {
        TriggerDef {
            name: Arc::from(name),
            event,
            label: Arc::from(label),
            condition: None,
            action: "SET n.triggered = true".to_string(),
        }
    }

    #[test]
    fn register_and_find() {
        let mut reg = TriggerRegistry::new();
        reg.register(test_trigger("t1", TriggerEvent::Set, "sensor"))
            .unwrap();

        let matches = reg.matching(TriggerEvent::Set, IStr::new("sensor"));
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].name.as_ref(), "t1");
    }

    #[test]
    fn no_match_wrong_event() {
        let mut reg = TriggerRegistry::new();
        reg.register(test_trigger("t1", TriggerEvent::Set, "sensor"))
            .unwrap();

        let matches = reg.matching(TriggerEvent::Insert, IStr::new("sensor"));
        assert!(matches.is_empty());
    }

    #[test]
    fn no_match_wrong_label() {
        let mut reg = TriggerRegistry::new();
        reg.register(test_trigger("t1", TriggerEvent::Set, "sensor"))
            .unwrap();

        let matches = reg.matching(TriggerEvent::Set, IStr::new("building"));
        assert!(matches.is_empty());
    }

    #[test]
    fn duplicate_name_rejected() {
        let mut reg = TriggerRegistry::new();
        reg.register(test_trigger("t1", TriggerEvent::Set, "sensor"))
            .unwrap();
        assert!(
            reg.register(test_trigger("t1", TriggerEvent::Insert, "zone"))
                .is_err()
        );
    }

    #[test]
    fn remove_trigger() {
        let mut reg = TriggerRegistry::new();
        reg.register(test_trigger("t1", TriggerEvent::Set, "sensor"))
            .unwrap();

        let removed = reg.remove("t1").unwrap();
        assert_eq!(removed.name.as_ref(), "t1");
        assert!(
            reg.matching(TriggerEvent::Set, IStr::new("sensor"))
                .is_empty()
        );
        assert!(reg.is_empty());
    }

    #[test]
    fn remove_nonexistent_errors() {
        let mut reg = TriggerRegistry::new();
        assert!(reg.remove("nope").is_err());
    }

    #[test]
    fn multiple_triggers_same_event_label() {
        let mut reg = TriggerRegistry::new();
        reg.register(test_trigger("t1", TriggerEvent::Set, "sensor"))
            .unwrap();
        reg.register(test_trigger("t2", TriggerEvent::Set, "sensor"))
            .unwrap();

        let matches = reg.matching(TriggerEvent::Set, IStr::new("sensor"));
        assert_eq!(matches.len(), 2);
    }

    #[test]
    fn list_all() {
        let mut reg = TriggerRegistry::new();
        reg.register(test_trigger("a", TriggerEvent::Set, "s"))
            .unwrap();
        reg.register(test_trigger("b", TriggerEvent::Insert, "z"))
            .unwrap();

        assert_eq!(reg.list().len(), 2);
    }

    #[test]
    fn to_vec_and_load() {
        let mut reg = TriggerRegistry::new();
        reg.register(test_trigger("t1", TriggerEvent::Set, "sensor"))
            .unwrap();
        reg.register(test_trigger("t2", TriggerEvent::Insert, "zone"))
            .unwrap();

        let serialized = reg.to_vec();
        assert_eq!(serialized.len(), 2);

        let mut reg2 = TriggerRegistry::new();
        reg2.load(serialized);

        assert_eq!(reg2.list().len(), 2);
        assert_eq!(
            reg2.matching(TriggerEvent::Set, IStr::new("sensor")).len(),
            1
        );
    }
}
