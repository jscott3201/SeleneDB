//! Property version store — archives old property values for point-in-time queries.
//!
//! Each property overwrite pushes the old value into a per-(node, key) version chain.
//! Chains are sorted newest-first (most recent superseded_at at index 0).
//!
//! Designed for **low-frequency structural properties** (equipment name, zone assignment,
//! configuration values). For high-frequency numeric data, use the time-series system
//! (hot tier + Parquet).

use std::collections::HashMap;

use selene_core::IStr;
use selene_core::entity::NodeId;
use selene_core::value::Value;

/// A single archived property version.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PropertyVersion {
    /// When this value was superseded (timestamp of the SET that replaced it).
    pub superseded_at: i64,
    /// The old value that was replaced.
    pub value: Value,
}

/// Serializable form of a version chain entry (for snapshot persistence).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SerializableVersionEntry {
    pub node_id: u64,
    pub key: String,
    pub versions: Vec<PropertyVersion>,
}

/// Per-node property version history.
pub struct VersionStore {
    /// (node_id, property_key) → version chain (newest first)
    versions: HashMap<(u64, IStr), Vec<PropertyVersion>>,
    /// Retention policy: maximum age in nanoseconds.
    retention_nanos: i64,
}

impl VersionStore {
    /// Create a new empty version store with the given retention policy.
    pub fn new(retention_days: u32) -> Self {
        Self {
            versions: HashMap::new(),
            retention_nanos: i64::from(retention_days) * 86_400 * 1_000_000_000,
        }
    }

    /// Archive an old property value (newest-first insertion).
    pub fn archive(&mut self, node_id: NodeId, key: IStr, old_value: Value, superseded_at: i64) {
        self.versions.entry((node_id.0, key)).or_default().insert(
            0,
            PropertyVersion {
                superseded_at,
                value: old_value,
            },
        );
    }

    /// Get the property value at a specific point in time.
    ///
    /// If timestamp >= current_updated_at, returns current_value.
    /// Otherwise walks the version chain (newest-first) to find the
    /// value that was active at the given timestamp.
    ///
    /// A version with `superseded_at = T` was active UNTIL time T.
    /// For a query at time Q, we want the last version where `superseded_at > Q`
    /// (it hadn't been replaced yet at time Q).
    pub fn value_at(
        &self,
        node_id: NodeId,
        key: &str,
        timestamp: i64,
        current_value: Option<&Value>,
        current_updated_at: i64,
    ) -> Option<Value> {
        // If querying current or future time, return current value
        if timestamp >= current_updated_at {
            return current_value.cloned();
        }

        // Search version chain (sorted newest first)
        let ikey = IStr::new(key);
        let chain = self.versions.get(&(node_id.0, ikey))?;

        // Walk newest→oldest. The last version where superseded_at > timestamp
        // is the value that was active at query time.
        let mut result = None;
        for version in chain {
            if version.superseded_at > timestamp {
                result = Some(version.value.clone());
            } else {
                break;
            }
        }
        result
    }

    /// Get all versions of a property, optionally filtered by time range.
    pub fn property_history(
        &self,
        node_id: NodeId,
        key: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Vec<&PropertyVersion> {
        let ikey = IStr::new(key);
        let Some(chain) = self.versions.get(&(node_id.0, ikey)) else {
            return vec![];
        };

        chain
            .iter()
            .filter(|v| {
                if let Some(start) = start_time
                    && v.superseded_at < start
                {
                    return false;
                }
                if let Some(end) = end_time
                    && v.superseded_at > end
                {
                    return false;
                }
                true
            })
            .collect()
    }

    /// Prune versions older than the retention window.
    /// Returns the number of versions pruned.
    pub fn prune(&mut self, cutoff: i64) -> usize {
        let mut pruned = 0;
        for chain in self.versions.values_mut() {
            let before = chain.len();
            chain.retain(|v| v.superseded_at >= cutoff);
            pruned += before - chain.len();
        }
        self.versions.retain(|_, chain| !chain.is_empty());
        pruned
    }

    /// Total number of archived versions (for metrics/diagnostics).
    pub fn version_count(&self) -> usize {
        self.versions.values().map(|c| c.len()).sum()
    }

    /// Number of distinct (node, property) pairs with version history.
    pub fn chain_count(&self) -> usize {
        self.versions.len()
    }

    /// Retention policy in nanoseconds.
    pub fn retention_nanos(&self) -> i64 {
        self.retention_nanos
    }

    /// Convert to serializable form for snapshot persistence.
    pub fn to_serializable(&self) -> Vec<SerializableVersionEntry> {
        self.versions
            .iter()
            .map(|((node_id, key), versions)| SerializableVersionEntry {
                node_id: *node_id,
                key: key.to_string(),
                versions: versions.clone(),
            })
            .collect()
    }

    /// Restore from serialized form (snapshot recovery).
    pub fn from_serializable(entries: Vec<SerializableVersionEntry>, retention_days: u32) -> Self {
        let mut versions = HashMap::new();
        for entry in entries {
            let key = IStr::new(&entry.key);
            versions.insert((entry.node_id, key), entry.versions);
        }
        Self {
            versions,
            retention_nanos: i64::from(retention_days) * 86_400 * 1_000_000_000,
        }
    }
}

// ── Service wrapper ──────────────────────────────────────────────────

use parking_lot::RwLock;
use std::sync::Arc;

/// VersionStore as a registered service in the ServiceRegistry.
pub struct VersionStoreService {
    pub store: Arc<RwLock<VersionStore>>,
}

impl VersionStoreService {
    pub fn new(store: Arc<RwLock<VersionStore>>) -> Self {
        Self { store }
    }
}

impl crate::service_registry::Service for VersionStoreService {
    fn name(&self) -> &'static str {
        "temporal"
    }

    fn health(&self) -> crate::service_registry::ServiceHealth {
        crate::service_registry::ServiceHealth::Healthy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smol_str::SmolStr;

    #[test]
    fn archive_and_value_at() {
        let mut vs = VersionStore::new(90);

        // Set temp = 20.0 at time 1000
        // (no archive yet — this is the first value)

        // Set temp = 25.0 at time 2000, archiving old value 20.0
        vs.archive(NodeId(1), IStr::new("temp"), Value::Float(20.0), 2000);

        // Set temp = 30.0 at time 3000, archiving old value 25.0
        vs.archive(NodeId(1), IStr::new("temp"), Value::Float(25.0), 3000);

        // Current value is 30.0, updated_at = 3000

        // Query at time 3500 → current (30.0)
        let v = vs.value_at(NodeId(1), "temp", 3500, Some(&Value::Float(30.0)), 3000);
        assert_eq!(v, Some(Value::Float(30.0)));

        // Query at time 2500 → was 25.0 (superseded at 3000, but active between 2000-3000)
        let v = vs.value_at(NodeId(1), "temp", 2500, Some(&Value::Float(30.0)), 3000);
        assert_eq!(v, Some(Value::Float(25.0)));

        // Query at time 1500 → was 20.0 (superseded at 2000, active before 2000)
        let v = vs.value_at(NodeId(1), "temp", 1500, Some(&Value::Float(30.0)), 3000);
        assert_eq!(v, Some(Value::Float(20.0)));

        // Query at time 500 → returns oldest known value (20.0).
        // The version store can't distinguish "before property existed" from
        // "oldest known value" since it doesn't track creation time.
        let v = vs.value_at(NodeId(1), "temp", 500, Some(&Value::Float(30.0)), 3000);
        assert_eq!(v, Some(Value::Float(20.0)));
    }

    #[test]
    fn property_history_returns_all_versions() {
        let mut vs = VersionStore::new(90);
        vs.archive(
            NodeId(1),
            IStr::new("name"),
            Value::String(SmolStr::new("A")),
            1000,
        );
        vs.archive(
            NodeId(1),
            IStr::new("name"),
            Value::String(SmolStr::new("B")),
            2000,
        );
        vs.archive(
            NodeId(1),
            IStr::new("name"),
            Value::String(SmolStr::new("C")),
            3000,
        );

        let history = vs.property_history(NodeId(1), "name", None, None);
        assert_eq!(history.len(), 3);
        // Newest first
        assert_eq!(history[0].superseded_at, 3000);
        assert_eq!(history[2].superseded_at, 1000);
    }

    #[test]
    fn property_history_time_range() {
        let mut vs = VersionStore::new(90);
        vs.archive(NodeId(1), IStr::new("temp"), Value::Float(20.0), 1000);
        vs.archive(NodeId(1), IStr::new("temp"), Value::Float(25.0), 2000);
        vs.archive(NodeId(1), IStr::new("temp"), Value::Float(30.0), 3000);

        let history = vs.property_history(NodeId(1), "temp", Some(1500), Some(2500));
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].superseded_at, 2000);
    }

    #[test]
    fn prune_removes_old_versions() {
        let mut vs = VersionStore::new(90);
        vs.archive(NodeId(1), IStr::new("temp"), Value::Float(20.0), 1000);
        vs.archive(NodeId(1), IStr::new("temp"), Value::Float(25.0), 2000);
        vs.archive(NodeId(1), IStr::new("temp"), Value::Float(30.0), 3000);

        let pruned = vs.prune(2500);
        assert_eq!(pruned, 2); // 1000 and 2000 pruned
        assert_eq!(vs.version_count(), 1);
    }

    #[test]
    fn serialization_round_trip() {
        let mut vs = VersionStore::new(90);
        vs.archive(NodeId(1), IStr::new("temp"), Value::Float(20.0), 1000);
        vs.archive(
            NodeId(2),
            IStr::new("name"),
            Value::String(SmolStr::new("HQ")),
            2000,
        );

        let serialized = vs.to_serializable();
        let restored = VersionStore::from_serializable(serialized, 90);

        assert_eq!(restored.version_count(), 2);
        // Query at time before superseded_at returns the archived value
        let v = restored.value_at(NodeId(1), "temp", 500, Some(&Value::Float(99.0)), 2000);
        assert_eq!(v, Some(Value::Float(20.0)));
    }

    #[test]
    fn empty_store_returns_none() {
        let vs = VersionStore::new(90);
        let v = vs.value_at(NodeId(1), "temp", 1000, Some(&Value::Float(20.0)), 2000);
        assert_eq!(v, None);
        assert_eq!(vs.version_count(), 0);
        assert_eq!(vs.chain_count(), 0);
    }
}
