//! Schema validation -- optional write-time type enforcement for nodes and edges.
//!
//! [`SchemaValidator`] holds a registry of [`NodeSchema`] and [`EdgeSchema`]
//! definitions and validates graph entities against those definitions.
//! If no schema is registered for a label, that label is silently ignored.
//!
//! Validation returns a list of [`ValidationIssue`]s. An empty list means the
//! entity is valid (or unconstrained). The caller decides what to do with
//! issues based on the configured [`ValidationMode`].

use std::collections::HashMap;

use selene_core::schema::{EdgeSchema, NodeSchema, ValidationMode};
use selene_core::{IStr, Value};

pub use crate::schema_compat::{ChangeSeverity, CompatibilityError, SchemaChange};
use crate::schema_compat::{check_edge_compatibility, check_node_compatibility};

/// Maximum parent chain depth for label inheritance.
/// Prevents infinite loops from cyclic schemas.
const MAX_INHERITANCE_DEPTH: usize = 32;

// ── ValidationIssue ──────────────────────────────────────────────────────────

/// A single schema violation found during validation.
///
/// The caller uses the validator's [`ValidationMode`] to decide whether an
/// issue should be logged as a warning or cause a write to be rejected.
///
/// `label` — the node or edge label the issue is attributable to, used by
/// [`SchemaValidator::effective_mode_for_label`] to resolve per-type mode
/// overrides. `None` means the issue is schema-agnostic (e.g., unique or
/// composite-key checks that span multiple labels).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationIssue {
    pub message: String,
    pub label: Option<IStr>,
}

impl ValidationIssue {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            label: None,
        }
    }

    /// Attach a label to this issue so per-type validation mode can be
    /// resolved at enforcement time.
    #[must_use]
    pub fn with_label(mut self, label: impl Into<IStr>) -> Self {
        self.label = Some(label.into());
        self
    }
}

impl std::fmt::Display for ValidationIssue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

// ── SchemaValidator ───────────────────────────────────────────────────────────

/// Registry of node/edge schemas with configurable validation mode.
///
/// Supports:
/// - Per-schema validation mode override (falls back to global default)
/// - Schema inheritance (parent chain resolved at registration time)
/// - Edge endpoint label validation (source/target label constraints)
/// - Default value collection for node creation
/// - Schema serialization for snapshot persistence
///
/// # Example
/// ```rust,ignore
/// let mut v = SchemaValidator::new(ValidationMode::Strict);
/// v.register_node_schema(my_schema);
/// let issues = v.validate_node(&node);
/// assert!(issues.is_empty());
/// ```
#[derive(Clone)]
pub struct SchemaValidator {
    pub(crate) node_schemas: HashMap<IStr, NodeSchema>,
    pub(crate) edge_schemas: HashMap<IStr, EdgeSchema>,
    mode: ValidationMode,
}

impl SchemaValidator {
    /// Create a new, empty validator with the given validation mode.
    pub fn new(mode: ValidationMode) -> Self {
        Self {
            node_schemas: HashMap::new(),
            edge_schemas: HashMap::new(),
            mode,
        }
    }

    /// The global validation mode.
    pub fn mode(&self) -> ValidationMode {
        self.mode
    }

    /// Partition issues into `(strict, warn)` buckets using per-issue effective
    /// mode. An issue's label (if any) is consulted first; unlabeled issues or
    /// labels without an override fall back to the global mode.
    ///
    /// Returns a pair where:
    /// - `strict` contains issues that must reject the transaction
    /// - `warn` contains issues that should only be logged
    pub fn partition_issues_by_mode(
        &self,
        issues: Vec<ValidationIssue>,
    ) -> (Vec<ValidationIssue>, Vec<ValidationIssue>) {
        let mut strict = Vec::new();
        let mut warn = Vec::new();
        for issue in issues {
            let mode = self.effective_mode_for_label(issue.label.as_ref().map(|s| s.as_str()));
            match mode {
                ValidationMode::Strict => strict.push(issue),
                ValidationMode::Warn => warn.push(issue),
            }
        }
        (strict, warn)
    }

    /// Resolve the effective validation mode for a given label.
    ///
    /// Per-type override (set via `CREATE NODE TYPE ... STRICT/WARN` or
    /// `validation_mode` in a schema pack) wins over the global default.
    /// `None` label or unknown label falls back to global mode — this covers
    /// cross-label checks like composite keys and structural violations that
    /// aren't attributable to a single type.
    pub fn effective_mode_for_label(&self, label: Option<&str>) -> ValidationMode {
        let Some(lbl) = label else {
            return self.mode;
        };
        let key = IStr::new(lbl);
        if let Some(node_schema) = self.node_schemas.get(&key)
            && let Some(mode) = node_schema.validation_mode
        {
            return mode;
        }
        if let Some(edge_schema) = self.edge_schemas.get(&key)
            && let Some(mode) = edge_schema.validation_mode
        {
            return mode;
        }
        self.mode
    }

    /// Register a [`NodeSchema`], keyed by `schema.label`.
    ///
    /// If the schema has a `parent`, properties from the parent schema are
    /// inherited (prepended to this schema's properties). Parent must already
    /// be registered. If parent is missing, the schema is registered as-is
    /// (no error -- parent may be registered later).
    ///
    /// When replacing an existing schema, runs a compatibility check. If the
    /// user did not bump the version, the version is auto-bumped based on
    /// the severity of changes. If the user provided an explicit bump that is
    /// insufficient, returns `Err(CompatibilityError)`.
    ///
    /// Returns `Ok(true)` if this replaced an existing schema with the same label.
    pub fn register_node_schema(
        &mut self,
        mut schema: NodeSchema,
    ) -> Result<bool, CompatibilityError> {
        // Resolve inheritance: prepend parent properties
        if let Some(ref parent_label) = schema.parent
            && let Some(parent) = self.node_schemas.get(&IStr::new(parent_label))
        {
            let mut merged = parent.properties.clone();
            // Child properties override parent properties with the same name
            let child_names: std::collections::HashSet<&str> =
                schema.properties.iter().map(|p| p.name.as_ref()).collect();
            merged.retain(|p| !child_names.contains(p.name.as_ref()));
            merged.append(&mut schema.properties);
            schema.properties = merged;

            // Inherit valid_edge_labels if child has none
            if schema.valid_edge_labels.is_empty() {
                schema.valid_edge_labels = parent.valid_edge_labels.clone();
            }
        }

        // Compatibility check if replacing an existing schema
        let key = IStr::new(&schema.label);
        if let Some(existing) = self.node_schemas.get(&key) {
            let auto_version = check_node_compatibility(existing, &schema)?;
            if schema.version == existing.version {
                schema.version = auto_version;
            }
        }

        let replaced = self.node_schemas.insert(key, schema).is_some();
        Ok(replaced)
    }

    /// Register a [`NodeSchema`] only if the label doesn't already exist.
    ///
    /// Returns `Ok(true)` if the schema was registered, `Ok(false)` if it already existed.
    /// Because this only inserts new schemas (never replaces), compatibility
    /// checking is not needed, but the `Result` is propagated from
    /// `register_node_schema` for API consistency.
    pub fn register_node_schema_if_new(
        &mut self,
        schema: NodeSchema,
    ) -> Result<bool, CompatibilityError> {
        if self.node_schemas.contains_key(&IStr::new(&schema.label)) {
            return Ok(false);
        }
        self.register_node_schema(schema)?;
        Ok(true)
    }

    /// Register an [`EdgeSchema`], keyed by `schema.label`.
    ///
    /// When replacing an existing schema, runs a compatibility check. If the
    /// user did not bump the version, the version is auto-bumped. If the user
    /// provided an insufficient explicit bump, returns `Err(CompatibilityError)`.
    ///
    /// Returns `Ok(true)` if this replaced an existing schema.
    pub fn register_edge_schema(
        &mut self,
        mut schema: EdgeSchema,
    ) -> Result<bool, CompatibilityError> {
        // Compatibility check if replacing an existing schema
        let key = IStr::new(&schema.label);
        if let Some(existing) = self.edge_schemas.get(&key) {
            let auto_version = check_edge_compatibility(existing, &schema)?;
            if schema.version == existing.version {
                schema.version = auto_version;
            }
        }

        let replaced = self.edge_schemas.insert(key, schema).is_some();
        Ok(replaced)
    }

    /// Register an [`EdgeSchema`] only if the label doesn't already exist.
    ///
    /// Returns `Ok(true)` if registered, `Ok(false)` if already existed.
    pub fn register_edge_schema_if_new(
        &mut self,
        schema: EdgeSchema,
    ) -> Result<bool, CompatibilityError> {
        if self.edge_schemas.contains_key(&IStr::new(&schema.label)) {
            return Ok(false);
        }
        self.register_edge_schema(schema)?;
        Ok(true)
    }

    /// Remove a node schema by label. Returns the removed schema if it existed.
    pub fn unregister_node_schema(&mut self, label: &str) -> Option<NodeSchema> {
        self.node_schemas.remove(&IStr::new(label))
    }

    /// Remove an edge schema by label. Returns the removed schema if it existed.
    pub fn unregister_edge_schema(&mut self, label: &str) -> Option<EdgeSchema> {
        self.edge_schemas.remove(&IStr::new(label))
    }

    /// Look up a node schema by label.
    pub fn node_schema(&self, label: &str) -> Option<&NodeSchema> {
        self.node_schemas.get(&IStr::new(label))
    }

    /// Look up an edge schema by label.
    pub fn edge_schema(&self, label: &str) -> Option<&EdgeSchema> {
        self.edge_schemas.get(&IStr::new(label))
    }

    /// Iterate over all registered node schemas.
    pub fn all_node_schemas(&self) -> impl Iterator<Item = &NodeSchema> {
        self.node_schemas.values()
    }

    /// Iterate over all registered edge schemas.
    pub fn all_edge_schemas(&self) -> impl Iterator<Item = &EdgeSchema> {
        self.edge_schemas.values()
    }

    /// Number of registered node schemas.
    pub fn node_schema_count(&self) -> usize {
        self.node_schemas.len()
    }

    /// Number of registered edge schemas.
    pub fn edge_schema_count(&self) -> usize {
        self.edge_schemas.len()
    }

    /// Get the default value for a single property key from schemas matching the labels.
    ///
    /// Checks each label's schema for a PropertyDef with a matching name
    /// and a `default` value. Returns the first match found.
    pub fn property_default(
        &self,
        labels: &selene_core::LabelSet,
        key: selene_core::IStr,
    ) -> Option<selene_core::Value> {
        let key_str = key.as_str();
        for label in labels.iter() {
            if let Some(schema) = self.node_schemas.get(&label) {
                for prop in &schema.properties {
                    if prop.name.as_ref() == key_str {
                        return prop.default.clone();
                    }
                }
            }
        }
        None
    }

    /// Collect default values for a node based on its labels.
    ///
    /// For each matching schema, any property with a `default` value that is
    /// not already present in `existing_props` is included in the result.
    /// When multiple labels define defaults for the same property (common with
    /// label inheritance), the last label in iteration order wins. LabelSet
    /// iterates in sorted order, so child schemas that sort after ancestors
    /// correctly override inherited defaults.
    pub fn collect_defaults(
        &self,
        labels: &selene_core::LabelSet,
        existing_props: &selene_core::PropertyMap,
    ) -> Vec<(selene_core::IStr, Value)> {
        let mut defaults: Vec<(selene_core::IStr, Value)> = Vec::new();
        for label in labels.iter() {
            if let Some(schema) = self.node_schemas.get(&label) {
                for prop in &schema.properties {
                    if let Some(ref default) = prop.default
                        && !existing_props.contains_key(selene_core::IStr::new(prop.name.as_ref()))
                    {
                        let key = selene_core::IStr::new(prop.name.as_ref());
                        if let Some(existing) = defaults.iter_mut().find(|(k, _)| *k == key) {
                            existing.1 = default.clone();
                        } else {
                            defaults.push((key, default.clone()));
                        }
                    }
                }
            }
        }
        defaults
    }

    /// Walk the parent chain and return all labels (leaf + ancestors).
    /// Returns just the input label if no schema or no parent exists.
    /// Depth-capped at `MAX_INHERITANCE_DEPTH` to guard against cycles.
    pub fn resolve_label_chain(&self, label: &str) -> Vec<IStr> {
        let mut chain = vec![IStr::new(label)];
        let mut current = IStr::new(label);
        for _ in 0..MAX_INHERITANCE_DEPTH {
            if let Some(parent) = self
                .node_schemas
                .get(&current)
                .and_then(|s| s.parent.as_ref())
            {
                let parent_istr = IStr::new(parent);
                chain.push(parent_istr);
                current = parent_istr;
            } else {
                break;
            }
        }
        chain
    }

    /// Check if registering a schema with the given label and parent would
    /// create an inheritance cycle. Returns `true` if a cycle would result.
    ///
    /// Also returns `true` if the parent chain exceeds `MAX_INHERITANCE_DEPTH`
    /// without terminating -- a chain that deep is almost certainly a
    /// configuration error and should be treated as cyclic.
    pub fn has_inheritance_cycle(&self, label: &str, parent: &str) -> bool {
        let label_istr = IStr::new(label);
        let mut current = IStr::new(parent);
        for _ in 0..MAX_INHERITANCE_DEPTH {
            if current == label_istr {
                return true;
            }
            if let Some(p) = self
                .node_schemas
                .get(&current)
                .and_then(|s| s.parent.as_ref())
            {
                current = IStr::new(p);
            } else {
                return false;
            }
        }
        // Depth exceeded without terminating -- treat as cyclic
        true
    }

    /// Export all schemas for serialization (used by snapshot persistence).
    pub fn export(&self) -> (Vec<NodeSchema>, Vec<EdgeSchema>) {
        (
            self.node_schemas.values().cloned().collect(),
            self.edge_schemas.values().cloned().collect(),
        )
    }

    /// Import schemas (used during snapshot recovery).
    pub fn import(&mut self, node_schemas: Vec<NodeSchema>, edge_schemas: Vec<EdgeSchema>) {
        for schema in node_schemas {
            let key = IStr::new(&schema.label);
            self.node_schemas.insert(key, schema);
        }
        for schema in edge_schemas {
            let key = IStr::new(&schema.label);
            self.edge_schemas.insert(key, schema);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use selene_core::{
        Value,
        schema::{NodeSchema, PropertyDef, ValidationMode, ValueType},
    };

    use super::*;

    fn node_schema(label: &str, props: Vec<PropertyDef>) -> NodeSchema {
        NodeSchema {
            label: Arc::from(label),
            parent: None,
            properties: props,
            valid_edge_labels: vec![],
            description: String::new(),
            annotations: HashMap::new(),
            version: Default::default(),
            validation_mode: None,
            key_properties: vec![],
        }
    }

    #[test]
    fn schema_lookup() {
        use selene_core::schema::EdgeSchema;
        let mut v = SchemaValidator::new(ValidationMode::Warn);
        v.register_node_schema(node_schema("device", vec![]))
            .unwrap();
        v.register_edge_schema(EdgeSchema {
            label: Arc::from("powers"),
            properties: vec![],
            description: String::new(),
            source_labels: vec![],
            target_labels: vec![],
            annotations: HashMap::new(),
            version: Default::default(),
            validation_mode: None,
            max_out_degree: None,
            max_in_degree: None,
            min_out_degree: None,
            min_in_degree: None,
        })
        .unwrap();

        assert!(v.node_schema("device").is_some());
        assert!(v.node_schema("missing").is_none());
        assert!(v.edge_schema("powers").is_some());
        assert!(v.edge_schema("missing").is_none());
    }

    #[test]
    fn resolve_label_chain_no_schema() {
        let v = SchemaValidator::new(ValidationMode::Strict);
        let chain = v.resolve_label_chain("unknown_label");
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].as_str(), "unknown_label");
    }

    #[test]
    fn resolve_label_chain_no_parent() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        v.register_node_schema(node_schema("sensor", vec![]))
            .unwrap();
        let chain = v.resolve_label_chain("sensor");
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].as_str(), "sensor");
    }

    #[test]
    fn resolve_label_chain_three_levels() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        v.register_node_schema(node_schema("point", vec![]))
            .unwrap();

        let mut sensor = node_schema("sensor", vec![]);
        sensor.parent = Some(Arc::from("point"));
        v.register_node_schema(sensor).unwrap();

        let mut temp = node_schema("temperature_sensor", vec![]);
        temp.parent = Some(Arc::from("sensor"));
        v.register_node_schema(temp).unwrap();

        let chain = v.resolve_label_chain("temperature_sensor");
        let names: Vec<&str> = chain.iter().map(|i| i.as_str()).collect();
        assert_eq!(names, vec!["temperature_sensor", "sensor", "point"]);
    }

    #[test]
    fn resolve_label_chain_four_levels() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        v.register_node_schema(node_schema("point", vec![]))
            .unwrap();

        let mut sensor = node_schema("sensor", vec![]);
        sensor.parent = Some(Arc::from("point"));
        v.register_node_schema(sensor).unwrap();

        let mut temp = node_schema("temperature_sensor", vec![]);
        temp.parent = Some(Arc::from("sensor"));
        v.register_node_schema(temp).unwrap();

        let mut supply = node_schema("supply_air_temp_sensor", vec![]);
        supply.parent = Some(Arc::from("temperature_sensor"));
        v.register_node_schema(supply).unwrap();

        let chain = v.resolve_label_chain("supply_air_temp_sensor");
        assert_eq!(chain.len(), 4);
        let names: Vec<&str> = chain.iter().map(|i| i.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "supply_air_temp_sensor",
                "temperature_sensor",
                "sensor",
                "point"
            ]
        );
    }

    #[test]
    fn has_inheritance_cycle_direct() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        let mut a = node_schema("a", vec![]);
        a.parent = Some(Arc::from("b"));
        v.register_node_schema(a).unwrap();
        assert!(v.has_inheritance_cycle("b", "a"));
    }

    #[test]
    fn has_inheritance_cycle_none() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        v.register_node_schema(node_schema("point", vec![]))
            .unwrap();

        let mut sensor = node_schema("sensor", vec![]);
        sensor.parent = Some(Arc::from("point"));
        v.register_node_schema(sensor).unwrap();

        assert!(!v.has_inheritance_cycle("temp_sensor", "sensor"));
    }

    #[test]
    fn has_inheritance_cycle_self_reference() {
        let v = SchemaValidator::new(ValidationMode::Strict);
        assert!(v.has_inheritance_cycle("x", "x"));
    }

    #[test]
    fn has_inheritance_cycle_depth_exceeded_returns_true() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        for i in 0..34 {
            let mut s = node_schema(&format!("s{i}"), vec![]);
            if i > 0 {
                s.parent = Some(Arc::from(format!("s{}", i - 1).as_str()));
            }
            v.register_node_schema(s).unwrap();
        }
        assert!(v.has_inheritance_cycle("s34", "s33"));
    }

    #[test]
    fn collect_defaults_deduplicates_inherited_properties() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        let mut status_point = PropertyDef::simple("status", ValueType::String, false);
        status_point.default = Some(Value::str("point_default"));
        let mut status_sensor = PropertyDef::simple("status", ValueType::String, false);
        status_sensor.default = Some(Value::str("sensor_default"));

        v.register_node_schema(NodeSchema {
            label: Arc::from("point"),
            parent: None,
            properties: vec![status_point],
            valid_edge_labels: vec![],
            description: String::new(),
            annotations: std::collections::HashMap::new(),
            version: Default::default(),
            validation_mode: None,
            key_properties: vec![],
        })
        .unwrap();
        v.register_node_schema(NodeSchema {
            label: Arc::from("sensor"),
            parent: None,
            properties: vec![status_sensor],
            valid_edge_labels: vec![],
            description: String::new(),
            annotations: std::collections::HashMap::new(),
            version: Default::default(),
            validation_mode: None,
            key_properties: vec![],
        })
        .unwrap();

        let labels = selene_core::LabelSet::from_strs(&["point", "sensor"]);
        let existing = selene_core::PropertyMap::new();
        let defaults = v.collect_defaults(&labels, &existing);
        assert_eq!(defaults.len(), 1);
        assert_eq!(defaults[0].0.as_str(), "status");
        let val = &defaults[0].1;
        assert!(
            *val == Value::str("point_default") || *val == Value::str("sensor_default"),
            "unexpected default value: {val:?}"
        );
    }

    #[test]
    fn property_default_returns_schema_default() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        let mut prop = PropertyDef::simple("version", ValueType::String, false);
        prop.default = Some(Value::str("1.0.0"));
        v.register_node_schema(node_schema("firmware", vec![prop]))
            .unwrap();

        let labels = selene_core::LabelSet::from_strs(&["firmware"]);
        let key = selene_core::IStr::new("version");
        let result = v.property_default(&labels, key);
        assert_eq!(result, Some(Value::str("1.0.0")));
    }

    #[test]
    fn property_default_returns_none_no_schema() {
        let v = SchemaValidator::new(ValidationMode::Strict);
        let labels = selene_core::LabelSet::from_strs(&["unregistered"]);
        let key = selene_core::IStr::new("version");
        assert!(v.property_default(&labels, key).is_none());
    }

    #[test]
    fn property_default_returns_none_no_default() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        let prop = PropertyDef::simple("version", ValueType::String, false);
        v.register_node_schema(node_schema("firmware", vec![prop]))
            .unwrap();

        let labels = selene_core::LabelSet::from_strs(&["firmware"]);
        let key = selene_core::IStr::new("version");
        assert!(v.property_default(&labels, key).is_none());
    }
}
