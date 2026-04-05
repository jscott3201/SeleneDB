//! Trigger definitions -- ECA (Event-Condition-Action) triggers for reactive graph mutations.

use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Event type that activates a trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TriggerEvent {
    /// Node with matching label created.
    Insert,
    /// Property set on node with matching label.
    Set,
    /// Property removed from node with matching label.
    Remove,
    /// Node with matching label deleted (labels captured before removal).
    Delete,
}

/// Stored trigger definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerDef {
    /// Unique trigger name.
    pub name: Arc<str>,
    /// Event type that activates this trigger.
    pub event: TriggerEvent,
    /// Label filter -- trigger only fires for nodes with this label.
    pub label: Arc<str>,
    /// Optional WHEN condition (stored as GQL text, parsed at eval time).
    pub condition: Option<String>,
    /// Mutation statement(s) to execute (stored as GQL text).
    pub action: String,
}
