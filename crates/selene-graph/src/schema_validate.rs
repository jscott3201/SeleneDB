//! Schema validation helpers and `SchemaValidator` validation methods.
//!
//! Extracted from `schema.rs` to isolate the validation logic (type
//! checking, range/length/enum/regex constraints, edge endpoint
//! validation) from the schema registry CRUD.

use std::sync::Arc;

use selene_core::schema::{PropertyDef, ValueType};
use selene_core::{Edge, IStr, Node, Value};

use crate::schema::{SchemaValidator, ValidationIssue};

// ── Validation impl block ────────────────────────────────────────────────────

impl SchemaValidator {
    /// Validate a node against all registered schemas whose labels appear on it.
    pub fn validate_node(&self, node: &Node) -> Vec<ValidationIssue> {
        let mut issues = Vec::new();

        for label in node.labels.iter() {
            if let Some(schema) = self.node_schemas.get(&label) {
                check_properties(&schema.properties, &node.properties, &mut issues);
            }
        }

        issues
    }

    /// Validate an edge against its registered schema (if any).
    ///
    /// Checks both property definitions and source/target label constraints.
    pub fn validate_edge(&self, edge: &Edge) -> Vec<ValidationIssue> {
        let mut issues = Vec::new();

        if let Some(schema) = self.edge_schemas.get(&edge.label) {
            check_properties(&schema.properties, &edge.properties, &mut issues);
        }

        issues
    }

    /// Validate edge endpoint labels against the edge schema's constraints.
    ///
    /// This is separate from `validate_edge` because it needs access to the
    /// source and target nodes (not just the edge itself).
    pub fn validate_edge_endpoints(
        &self,
        label: &str,
        source_labels: &selene_core::LabelSet,
        target_labels: &selene_core::LabelSet,
    ) -> Vec<ValidationIssue> {
        let mut issues = Vec::new();

        if let Some(schema) = self.edge_schemas.get(&IStr::new(label)) {
            if !schema.source_labels.is_empty()
                && !schema
                    .source_labels
                    .iter()
                    .any(|l| source_labels.contains_str(l))
            {
                issues.push(ValidationIssue::new(format!(
                    "edge '{}' source must have one of labels {:?}, got {:?}",
                    label,
                    schema
                        .source_labels
                        .iter()
                        .map(|l| &**l)
                        .collect::<Vec<_>>(),
                    source_labels.iter().map(|l| l.as_str()).collect::<Vec<_>>(),
                )));
            }
            if !schema.target_labels.is_empty()
                && !schema
                    .target_labels
                    .iter()
                    .any(|l| target_labels.contains_str(l))
            {
                issues.push(ValidationIssue::new(format!(
                    "edge '{}' target must have one of labels {:?}, got {:?}",
                    label,
                    schema
                        .target_labels
                        .iter()
                        .map(|l| &**l)
                        .collect::<Vec<_>>(),
                    target_labels.iter().map(|l| l.as_str()).collect::<Vec<_>>(),
                )));
            }
        }

        issues
    }

    /// Check if a property is marked immutable for the given label.
    pub fn is_immutable(&self, label: &str, prop_name: &str) -> bool {
        self.node_schemas.get(&IStr::new(label)).is_some_and(|s| {
            s.properties
                .iter()
                .any(|p| p.name.as_ref() == prop_name && p.immutable)
        })
    }

    /// Check if a property is marked unique for the given label.
    pub fn is_unique(&self, label: &str, prop_name: &str) -> bool {
        self.node_schemas.get(&IStr::new(label)).is_some_and(|s| {
            s.properties
                .iter()
                .any(|p| p.name.as_ref() == prop_name && p.unique)
        })
    }

    /// Check if a node schema has key_properties defined.
    pub fn key_properties(&self, label: &str) -> Option<&[Arc<str>]> {
        self.node_schemas
            .get(&IStr::new(label))
            .filter(|s| !s.key_properties.is_empty())
            .map(|s| s.key_properties.as_slice())
    }

    /// Check edge cardinality constraints.
    /// Returns violations for max_out_degree and max_in_degree.
    pub fn check_edge_cardinality(
        &self,
        edge_label: &str,
        source_out_count: usize,
        target_in_count: usize,
    ) -> Vec<ValidationIssue> {
        let mut issues = Vec::new();
        if let Some(schema) = self.edge_schemas.get(&IStr::new(edge_label)) {
            if let Some(max_out) = schema.max_out_degree
                && source_out_count > max_out as usize
            {
                issues.push(ValidationIssue::new(format!(
                        "edge '{edge_label}' source exceeds max_out_degree {max_out} (has {source_out_count})"
                    )));
            }
            if let Some(max_in) = schema.max_in_degree
                && target_in_count > max_in as usize
            {
                issues.push(ValidationIssue::new(format!(
                        "edge '{edge_label}' target exceeds max_in_degree {max_in} (has {target_in_count})"
                    )));
            }
        }
        issues
    }
}

// ── Free validation helpers ──────────────────────────────────────────────────

/// Check a set of [`PropertyDef`]s against an actual property map, pushing
/// any violations into `issues`.
pub(crate) fn check_properties(
    defs: &[PropertyDef],
    actual: &selene_core::PropertyMap,
    issues: &mut Vec<ValidationIssue>,
) {
    for def in defs {
        match actual.get_by_str(def.name.as_ref()) {
            None => {
                if def.required {
                    issues.push(ValidationIssue::new(format!(
                        "required property '{}' is missing",
                        def.name
                    )));
                }
                // Optional and absent -- valid.
            }
            Some(value) => {
                // Null is always acceptable regardless of declared type.
                if value.is_null() {
                    continue;
                }
                if !type_matches(&def.value_type, value) {
                    issues.push(ValidationIssue::new(format!(
                        "property '{}' expected type {:?} but got {}",
                        def.name,
                        def.value_type,
                        value.type_name(),
                    )));
                }
                // Range constraints (numeric)
                check_range(def, value, issues);
                // Length constraints (string)
                check_length(def, value, issues);
                // Enum constraint
                check_allowed_values(def, value, issues);
                // Regex pattern constraint
                check_pattern(def, value, issues);
            }
        }
    }
}

/// Check numeric range constraints (min/max).
fn check_range(def: &PropertyDef, value: &Value, issues: &mut Vec<ValidationIssue>) {
    let num = match value {
        Value::Int(i) => Some(*i as f64),
        Value::UInt(u) => Some(*u as f64),
        Value::Float(f) => Some(*f),
        _ => None,
    };
    if let Some(v) = num {
        if let Some(min) = def.min
            && v < min
        {
            issues.push(ValidationIssue::new(format!(
                "property '{}' value {} is below minimum {}",
                def.name, v, min
            )));
        }
        if let Some(max) = def.max
            && v > max
        {
            issues.push(ValidationIssue::new(format!(
                "property '{}' value {} exceeds maximum {}",
                def.name, v, max
            )));
        }
    }
}

/// Check string length constraints (min_length/max_length).
fn check_length(def: &PropertyDef, value: &Value, issues: &mut Vec<ValidationIssue>) {
    let s = match value {
        Value::String(s) => s.as_str(),
        Value::InternedStr(s) => s.as_str(),
        _ => return,
    };
    let len = s.len();
    if let Some(min) = def.min_length
        && len < min
    {
        issues.push(ValidationIssue::new(format!(
            "property '{}' length {} is below minimum {}",
            def.name, len, min
        )));
    }
    if let Some(max) = def.max_length
        && len > max
    {
        issues.push(ValidationIssue::new(format!(
            "property '{}' length {} exceeds maximum {}",
            def.name, len, max
        )));
    }
}

/// Check allowed values (enum constraint).
/// Handles cross-variant string matching (String vs InternedStr).
fn check_allowed_values(def: &PropertyDef, value: &Value, issues: &mut Vec<ValidationIssue>) {
    if def.allowed_values.is_empty() {
        return;
    }
    // Derived PartialEq treats String and InternedStr as distinct variants,
    // so we need an explicit cross-variant string check.
    let found = def.allowed_values.iter().any(|allowed| {
        if allowed == value {
            return true;
        }
        // Cross-variant: compare string content regardless of storage form
        match (allowed.as_str(), value.as_str()) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        }
    });
    if !found {
        issues.push(ValidationIssue::new(format!(
            "property '{}' value not in allowed set",
            def.name
        )));
    }
}

/// Check regex pattern constraint (cached compilation via thread-local).
fn check_pattern(def: &PropertyDef, value: &Value, issues: &mut Vec<ValidationIssue>) {
    use std::cell::RefCell;
    use std::collections::HashMap;

    thread_local! {
        static REGEX_CACHE: RefCell<HashMap<String, Result<regex::Regex, String>>> =
            RefCell::new(HashMap::new());
    }

    if let Some(ref pattern) = def.pattern {
        let s = match value {
            Value::String(s) => s.as_str(),
            Value::InternedStr(s) => s.as_str(),
            _ => return,
        };
        REGEX_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            let entry = cache
                .entry(pattern.clone())
                .or_insert_with(|| regex::Regex::new(pattern).map_err(|e| e.to_string()));
            match entry {
                Ok(re) => {
                    if !re.is_match(s) {
                        issues.push(ValidationIssue::new(format!(
                            "property '{}' value does not match pattern '{}'",
                            def.name, pattern
                        )));
                    }
                }
                Err(e) => {
                    issues.push(ValidationIssue::new(format!(
                        "property '{}' has invalid regex pattern '{}': {}",
                        def.name, pattern, e
                    )));
                }
            }
        });
    }
}

/// Return `true` if `value` is compatible with the declared `ValueType`.
pub(crate) fn type_matches(expected: &ValueType, value: &Value) -> bool {
    match expected {
        ValueType::Any => true,
        ValueType::Bool => matches!(value, Value::Bool(_)),
        ValueType::Int => matches!(value, Value::Int(_)),
        ValueType::UInt => matches!(value, Value::UInt(_)),
        ValueType::Float => matches!(value, Value::Float(_)),
        ValueType::String => matches!(value, Value::String(_) | Value::InternedStr(_)),
        ValueType::ZonedDateTime => matches!(value, Value::Timestamp(_)),
        ValueType::Date => matches!(value, Value::Date(_)),
        ValueType::LocalDateTime => matches!(value, Value::LocalDateTime(_)),
        ValueType::Duration => matches!(value, Value::Duration(_)),
        ValueType::Bytes => matches!(value, Value::Bytes(_)),
        ValueType::List => matches!(value, Value::List(_)),
        ValueType::Vector => matches!(value, Value::Vector(_)),
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use selene_core::{
        Edge, Node, Value,
        entity::{EdgeId, NodeId},
        schema::{EdgeSchema, NodeSchema, PropertyDef, ValidationMode, ValueType},
    };

    use crate::schema::SchemaValidator;

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

    fn edge_schema(label: &str, props: Vec<PropertyDef>) -> EdgeSchema {
        EdgeSchema {
            label: Arc::from(label),
            properties: props,
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
        }
    }

    fn prop_def(name: &str, vt: ValueType, required: bool) -> PropertyDef {
        PropertyDef::simple(name, vt, required)
    }

    fn make_node(labels: &[&str], props: &[(&str, Value)]) -> Node {
        let label_set = selene_core::LabelSet::from_strs(labels);
        let prop_map = selene_core::PropertyMap::from_pairs(
            props
                .iter()
                .map(|(k, v)| (selene_core::IStr::new(k), v.clone())),
        );
        Node::new(NodeId(1), label_set, prop_map)
    }

    fn make_edge(label: &str, props: &[(&str, Value)]) -> Edge {
        let prop_map = selene_core::PropertyMap::from_pairs(
            props
                .iter()
                .map(|(k, v)| (selene_core::IStr::new(k), v.clone())),
        );
        Edge::new(
            EdgeId(1),
            NodeId(1),
            NodeId(2),
            selene_core::IStr::new(label),
            prop_map,
        )
    }

    #[test]
    fn no_schema_means_no_issues() {
        let v = SchemaValidator::new(ValidationMode::Warn);
        let node = make_node(&["sensor"], &[]);
        assert!(v.validate_node(&node).is_empty());
    }

    #[test]
    fn valid_node_passes() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        v.register_node_schema(node_schema(
            "sensor",
            vec![prop_def("unit", ValueType::String, true)],
        ))
        .unwrap();

        let node = make_node(&["sensor"], &[("unit", Value::str("C"))]);
        assert!(v.validate_node(&node).is_empty());
    }

    #[test]
    fn missing_required_property() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        v.register_node_schema(node_schema(
            "sensor",
            vec![prop_def("unit", ValueType::String, true)],
        ))
        .unwrap();

        let node = make_node(&["sensor"], &[]);
        let issues = v.validate_node(&node);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].message.contains("unit"));
        assert!(issues[0].message.contains("missing"));
    }

    #[test]
    fn wrong_property_type() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        v.register_node_schema(node_schema(
            "sensor",
            vec![prop_def("value", ValueType::Float, false)],
        ))
        .unwrap();

        let node = make_node(&["sensor"], &[("value", Value::str("bad"))]);
        let issues = v.validate_node(&node);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].message.contains("value"));
    }

    #[test]
    fn null_matches_any_type() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        v.register_node_schema(node_schema(
            "sensor",
            vec![prop_def("reading", ValueType::Float, false)],
        ))
        .unwrap();

        let node = make_node(&["sensor"], &[("reading", Value::Null)]);
        assert!(v.validate_node(&node).is_empty());
    }

    #[test]
    fn any_type_matches_everything() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        v.register_node_schema(node_schema(
            "flexible",
            vec![
                prop_def("a", ValueType::Any, false),
                prop_def("b", ValueType::Any, false),
                prop_def("c", ValueType::Any, false),
            ],
        ))
        .unwrap();

        let node = make_node(
            &["flexible"],
            &[
                ("a", Value::Bool(true)),
                ("b", Value::Int(42)),
                ("c", Value::Bytes(Arc::from(vec![0u8]))),
            ],
        );
        assert!(v.validate_node(&node).is_empty());
    }

    #[test]
    fn unregistered_label_ignored() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        v.register_node_schema(node_schema(
            "known",
            vec![prop_def("x", ValueType::Int, true)],
        ))
        .unwrap();

        let node = make_node(&["unknown"], &[]);
        assert!(v.validate_node(&node).is_empty());
    }

    #[test]
    fn multi_label_validates_all_matching_schemas() {
        let mut v = SchemaValidator::new(ValidationMode::Warn);
        v.register_node_schema(node_schema(
            "sensor",
            vec![prop_def("unit", ValueType::String, true)],
        ))
        .unwrap();
        v.register_node_schema(node_schema(
            "temperature",
            vec![prop_def("scale", ValueType::String, true)],
        ))
        .unwrap();

        let node = make_node(&["sensor", "temperature"], &[]);
        let issues = v.validate_node(&node);
        assert_eq!(issues.len(), 2);
    }

    #[test]
    fn edge_validation() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        v.register_edge_schema(edge_schema(
            "feeds",
            vec![prop_def("medium", ValueType::String, true)],
        ))
        .unwrap();

        let edge = make_edge("feeds", &[]);
        let issues = v.validate_edge(&edge);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].message.contains("medium"));
    }

    #[test]
    fn optional_property_not_present_is_ok() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        v.register_node_schema(node_schema(
            "device",
            vec![
                prop_def("name", ValueType::String, true),
                prop_def("description", ValueType::String, false),
            ],
        ))
        .unwrap();

        let node = make_node(&["device"], &[("name", Value::str("AHU-1"))]);
        assert!(v.validate_node(&node).is_empty());
    }

    #[test]
    fn range_constraint_rejects_below_min() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        let mut prop = PropertyDef::simple("temp", ValueType::Float, false);
        prop.min = Some(0.0);
        prop.max = Some(100.0);
        v.register_node_schema(node_schema("sensor", vec![prop]))
            .unwrap();

        let node = make_node(&["sensor"], &[("temp", Value::Float(-5.0))]);
        let issues = v.validate_node(&node);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].message.contains("below minimum"));
    }

    #[test]
    fn range_constraint_rejects_above_max() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        let mut prop = PropertyDef::simple("temp", ValueType::Float, false);
        prop.min = Some(0.0);
        prop.max = Some(100.0);
        v.register_node_schema(node_schema("sensor", vec![prop]))
            .unwrap();

        let node = make_node(&["sensor"], &[("temp", Value::Float(150.0))]);
        let issues = v.validate_node(&node);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].message.contains("exceeds maximum"));
    }

    #[test]
    fn range_constraint_accepts_valid() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        let mut prop = PropertyDef::simple("temp", ValueType::Float, false);
        prop.min = Some(0.0);
        prop.max = Some(100.0);
        v.register_node_schema(node_schema("sensor", vec![prop]))
            .unwrap();

        let node = make_node(&["sensor"], &[("temp", Value::Float(72.5))]);
        assert!(v.validate_node(&node).is_empty());
    }

    #[test]
    fn length_constraint_rejects_too_short() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        let mut prop = PropertyDef::simple("name", ValueType::String, false);
        prop.min_length = Some(3);
        v.register_node_schema(node_schema("item", vec![prop]))
            .unwrap();

        let node = make_node(&["item"], &[("name", Value::str("ab"))]);
        let issues = v.validate_node(&node);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].message.contains("below minimum"));
    }

    #[test]
    fn length_constraint_rejects_too_long() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        let mut prop = PropertyDef::simple("code", ValueType::String, false);
        prop.max_length = Some(5);
        v.register_node_schema(node_schema("item", vec![prop]))
            .unwrap();

        let node = make_node(&["item"], &[("code", Value::str("toolong"))]);
        let issues = v.validate_node(&node);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].message.contains("exceeds maximum"));
    }

    #[test]
    fn allowed_values_rejects_invalid() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        let mut prop = PropertyDef::simple("status", ValueType::String, false);
        prop.allowed_values = vec![
            Value::str("active"),
            Value::str("inactive"),
            Value::str("maintenance"),
        ];
        v.register_node_schema(node_schema("device", vec![prop]))
            .unwrap();

        let node = make_node(&["device"], &[("status", Value::str("broken"))]);
        let issues = v.validate_node(&node);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].message.contains("not in allowed set"));
    }

    #[test]
    fn allowed_values_accepts_valid() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        let mut prop = PropertyDef::simple("status", ValueType::String, false);
        prop.allowed_values = vec![Value::str("active"), Value::str("inactive")];
        v.register_node_schema(node_schema("device", vec![prop]))
            .unwrap();

        let node = make_node(&["device"], &[("status", Value::str("active"))]);
        assert!(v.validate_node(&node).is_empty());
    }

    #[test]
    fn regex_pattern_rejects_invalid() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        let mut prop = PropertyDef::simple("serial", ValueType::String, false);
        prop.pattern = Some("^[A-Z]{2}\\d{4}$".into());
        v.register_node_schema(node_schema("device", vec![prop]))
            .unwrap();

        let node = make_node(&["device"], &[("serial", Value::str("invalid"))]);
        let issues = v.validate_node(&node);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].message.contains("does not match pattern"));
    }

    #[test]
    fn regex_pattern_accepts_valid() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        let mut prop = PropertyDef::simple("serial", ValueType::String, false);
        prop.pattern = Some("^[A-Z]{2}\\d{4}$".into());
        v.register_node_schema(node_schema("device", vec![prop]))
            .unwrap();

        let node = make_node(&["device"], &[("serial", Value::str("AB1234"))]);
        assert!(v.validate_node(&node).is_empty());
    }

    #[test]
    fn immutable_check() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        let mut prop = PropertyDef::simple("serial", ValueType::String, false);
        prop.immutable = true;
        v.register_node_schema(node_schema("device", vec![prop]))
            .unwrap();

        assert!(v.is_immutable("device", "serial"));
        assert!(!v.is_immutable("device", "name"));
        assert!(!v.is_immutable("unknown", "serial"));
    }

    #[test]
    fn unique_check() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        let mut prop = PropertyDef::simple("email", ValueType::String, false);
        prop.unique = true;
        v.register_node_schema(node_schema("user", vec![prop]))
            .unwrap();

        assert!(v.is_unique("user", "email"));
        assert!(!v.is_unique("user", "name"));
    }

    #[test]
    fn edge_cardinality_max_out() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        let mut schema = edge_schema("contains", vec![]);
        schema.max_out_degree = Some(2);
        v.register_edge_schema(schema).unwrap();

        assert!(v.check_edge_cardinality("contains", 2, 0).is_empty());
        let issues = v.check_edge_cardinality("contains", 3, 0);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].message.contains("max_out_degree"));
    }

    #[test]
    fn edge_cardinality_max_in() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        let mut schema = edge_schema("feeds", vec![]);
        schema.max_in_degree = Some(1);
        v.register_edge_schema(schema).unwrap();

        assert!(v.check_edge_cardinality("feeds", 0, 1).is_empty());
        let issues = v.check_edge_cardinality("feeds", 0, 2);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].message.contains("max_in_degree"));
    }
}
