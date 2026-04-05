//! Sync configuration — hub-spoke topology, subscriptions, and predicate filters.

use serde::Deserialize;

/// Bidirectional sync configuration for hub-spoke topologies.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SyncConfig {
    /// Hub address to sync with (e.g., "hub.example.com:4510").
    pub upstream: String,
    /// This node's identity in the sync mesh.
    pub peer_name: String,
    /// WAL entries per SyncPush batch.
    pub batch_size: usize,
    /// Maximum delay (ms) before pushing in live mode.
    pub push_interval_ms: u64,
    /// Backoff delay (seconds) on connection failure.
    pub reconnect_delay_secs: u64,
    /// Auth identity for upstream connection.
    pub auth_identity: Option<String>,
    /// Auth credentials for upstream connection.
    pub auth_credentials: Option<String>,
    /// TLS server name override for upstream.
    pub server_name: Option<String>,
    /// Maximum allowed HLC clock skew (seconds) for incoming SyncPush
    /// entries. Entries with an HLC timestamp more than this many seconds
    /// ahead of the server's current HLC are rejected. Default: 300 (5
    /// minutes).
    pub max_hlc_skew_secs: u64,
    /// Maximum number of entries in a single SyncPush request.
    /// Requests exceeding this limit are rejected before processing.
    /// Default: 1,000.
    pub max_sync_entries: usize,
    /// Maximum number of changes per entry in a SyncPush request.
    /// Entries exceeding this limit cause the entire request to be
    /// rejected. Default: 10,000.
    pub max_changes_per_entry: usize,
    /// Subscription filters for partial graph sync.
    /// If empty, the node syncs the full graph (Phase 5B behavior).
    #[serde(default)]
    pub subscriptions: Vec<SubscriptionToml>,
    /// Maximum number of rules per subscription. Default: 50.
    #[serde(default = "default_max_subscription_rules")]
    pub max_subscription_rules: usize,
    /// Maximum number of predicates per rule. Default: 20.
    #[serde(default = "default_max_predicates_per_rule")]
    pub max_predicates_per_rule: usize,
    /// Maximum number of values in an IN predicate. Default: 1000.
    #[serde(default = "default_max_in_list_size")]
    pub max_in_list_size: usize,
}

fn default_max_subscription_rules() -> usize {
    50
}
fn default_max_predicates_per_rule() -> usize {
    20
}
fn default_max_in_list_size() -> usize {
    1000
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            upstream: String::new(),
            peer_name: String::new(),
            batch_size: 100,
            push_interval_ms: 100,
            reconnect_delay_secs: 5,
            auth_identity: None,
            auth_credentials: None,
            server_name: None,
            max_hlc_skew_secs: 300,
            max_sync_entries: 1_000,
            max_changes_per_entry: 10_000,
            subscriptions: Vec::new(),
            max_subscription_rules: default_max_subscription_rules(),
            max_predicates_per_rule: default_max_predicates_per_rule(),
            max_in_list_size: default_max_in_list_size(),
        }
    }
}

impl SyncConfig {
    /// Sync is enabled when both `upstream` and `peer_name` are non-empty.
    pub fn is_enabled(&self) -> bool {
        !self.upstream.is_empty() && !self.peer_name.is_empty()
    }
}

/// TOML-friendly predicate value (untagged enum for flexible TOML parsing).
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum TomlPredicateValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

impl TomlPredicateValue {
    pub(crate) fn to_value(&self) -> selene_core::Value {
        match self {
            Self::String(s) => selene_core::Value::String(s.as_str().into()),
            Self::Int(i) => selene_core::Value::Int(*i),
            Self::Float(f) => selene_core::Value::Float(*f),
            Self::Bool(b) => selene_core::Value::Bool(*b),
        }
    }
}

/// TOML-friendly predicate (internally tagged by `op` field).
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op")]
pub enum PredicateToml {
    #[serde(rename = "eq")]
    Eq {
        key: String,
        value: TomlPredicateValue,
    },
    #[serde(rename = "in")]
    In {
        key: String,
        values: Vec<TomlPredicateValue>,
    },
    #[serde(rename = "gt")]
    Gt {
        key: String,
        value: TomlPredicateValue,
    },
    #[serde(rename = "lt")]
    Lt {
        key: String,
        value: TomlPredicateValue,
    },
    #[serde(rename = "gte")]
    Gte {
        key: String,
        value: TomlPredicateValue,
    },
    #[serde(rename = "lte")]
    Lte {
        key: String,
        value: TomlPredicateValue,
    },
    #[serde(rename = "is_not_null")]
    IsNotNull { key: String },
}

/// TOML subscription rule.
#[derive(Debug, Clone, Deserialize)]
pub struct SubscriptionRuleToml {
    pub labels: Vec<String>,
    #[serde(default)]
    pub predicates: Vec<PredicateToml>,
}

/// TOML subscription definition.
#[derive(Debug, Clone, Deserialize)]
pub struct SubscriptionToml {
    pub name: String,
    #[serde(default = "default_direction")]
    pub direction: String,
    #[serde(default)]
    pub rules: Vec<SubscriptionRuleToml>,
}

fn default_direction() -> String {
    "bidirectional".to_string()
}

impl SubscriptionToml {
    /// Convert TOML config representation to wire-format SubscriptionConfig.
    pub fn to_wire_config(&self) -> selene_wire::dto::sync::SubscriptionConfig {
        use selene_wire::dto::sync::*;

        let direction = match self.direction.as_str() {
            "push_only" => SyncDirectionConfig::PushOnly,
            "pull_only" => SyncDirectionConfig::PullOnly,
            _ => SyncDirectionConfig::Bidirectional,
        };

        let rules = self
            .rules
            .iter()
            .map(|r| SubscriptionRuleConfig {
                labels: r.labels.clone(),
                predicates: r
                    .predicates
                    .iter()
                    .map(|p| match p {
                        PredicateToml::Eq { key, value } => PropertyPredicateConfig::Eq {
                            key: key.clone(),
                            value: value.to_value(),
                        },
                        PredicateToml::In { key, values } => PropertyPredicateConfig::In {
                            key: key.clone(),
                            values: values.iter().map(|v| v.to_value()).collect(),
                        },
                        PredicateToml::Gt { key, value } => PropertyPredicateConfig::Gt {
                            key: key.clone(),
                            value: value.to_value(),
                        },
                        PredicateToml::Lt { key, value } => PropertyPredicateConfig::Lt {
                            key: key.clone(),
                            value: value.to_value(),
                        },
                        PredicateToml::Gte { key, value } => PropertyPredicateConfig::Gte {
                            key: key.clone(),
                            value: value.to_value(),
                        },
                        PredicateToml::Lte { key, value } => PropertyPredicateConfig::Lte {
                            key: key.clone(),
                            value: value.to_value(),
                        },
                        PredicateToml::IsNotNull { key } => {
                            PropertyPredicateConfig::IsNotNull { key: key.clone() }
                        }
                    })
                    .collect(),
            })
            .collect();

        selene_wire::dto::sync::SubscriptionConfig {
            name: self.name.clone(),
            rules,
            direction,
        }
    }
}

#[cfg(test)]
mod subscription_config_tests {
    use super::*;

    #[test]
    fn parse_subscription_from_toml() {
        let toml_str = r#"
            upstream = "hub:4510"
            peer_name = "edge-hq"

            [[subscriptions]]
            name = "building-hq"
            direction = "bidirectional"

            [[subscriptions.rules]]
            labels = ["Sensor", "Actuator"]
            predicates = [
                { op = "eq", key = "building", value = "HQ" },
            ]

            [[subscriptions.rules]]
            labels = ["Equipment"]
            predicates = [
                { op = "in", key = "floor", values = [1, 2, 3] },
            ]
        "#;

        let sync: SyncConfig = toml::from_str(toml_str).unwrap();

        assert_eq!(sync.subscriptions.len(), 1);
        let sub = &sync.subscriptions[0];
        assert_eq!(sub.name, "building-hq");
        assert_eq!(sub.rules.len(), 2);
        assert_eq!(sub.rules[0].labels, vec!["Sensor", "Actuator"]);
        assert_eq!(sub.rules[0].predicates.len(), 1);
        assert!(
            matches!(&sub.rules[0].predicates[0], PredicateToml::Eq { key, .. } if key == "building")
        );

        // Test conversion to wire config
        let wire = sub.to_wire_config();
        assert_eq!(wire.rules.len(), 2);
        assert_eq!(wire.rules[0].labels, vec!["Sensor", "Actuator"]);
    }

    #[test]
    fn empty_subscriptions_default() {
        let toml_str = r#"
            upstream = "hub:4510"
            peer_name = "edge-1"
        "#;

        let sync: SyncConfig = toml::from_str(toml_str).unwrap();
        assert!(sync.subscriptions.is_empty());
    }
}
