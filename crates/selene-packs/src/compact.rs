//! Compact schema format parser.
//!
//! Parses the concise field shorthand used in both compact TOML and JSON packs:
//!
//! ```text
//! "string"          → optional String
//! "string!"         → required String
//! "string = '°F'"   → optional String with default "°F"
//! "float = 72.5"    → optional Float with default 72.5
//! "int = 60"        → optional Int with default 60
//! "bool = true"     → optional Bool with default true
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use selene_core::Value;
use selene_core::schema::{EdgeSchema, NodeSchema, PropertyDef, SchemaPack, ValueType};

use crate::loader::{PackError, parse_validation_mode, parse_value_type};

/// A compact pack format (shared between JSON and TOML compact loaders).
#[derive(Debug, serde::Deserialize)]
pub struct CompactPack {
    /// Pack name.
    pub name: String,
    /// Pack version.
    pub version: String,
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
    /// Base ontology this pack extends.
    #[serde(default)]
    pub ontology: Option<String>,
    /// Extends another pack (informational).
    #[serde(default)]
    pub extends: Option<String>,
    /// Node type definitions keyed by label.
    #[serde(default)]
    pub types: HashMap<String, CompactNodeType>,
    /// Edge/relationship definitions keyed by label.
    #[serde(default)]
    pub relationships: HashMap<String, CompactEdgeType>,
}

/// A compact node type definition.
#[derive(Debug, serde::Deserialize)]
pub struct CompactNodeType {
    /// Parent type to inherit from.
    #[serde(default)]
    pub extends: Option<String>,
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
    /// Ontology mapping (e.g., "brick:Temperature_Sensor").
    #[serde(default)]
    pub ontology: Option<String>,
    /// Validation mode override.
    #[serde(default)]
    pub validation: Option<String>,
    /// Valid edge labels for this type.
    #[serde(default)]
    pub edges: Vec<String>,
    /// Field definitions using shorthand syntax.
    /// Key is field name, value is type spec like "string!", "float = 72.5".
    #[serde(default)]
    pub fields: HashMap<String, String>,
}

/// A compact edge type definition.
#[derive(Debug, serde::Deserialize)]
pub struct CompactEdgeType {
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
    /// Valid source node labels.
    #[serde(default)]
    pub source: Vec<String>,
    /// Valid target node labels.
    #[serde(default)]
    pub target: Vec<String>,
    /// Field definitions using shorthand syntax.
    #[serde(default)]
    pub fields: HashMap<String, String>,
}

/// Parse a compact JSON string into a SchemaPack.
pub fn load_compact_json(json: &str) -> Result<SchemaPack, PackError> {
    let raw: CompactPack =
        serde_json::from_str(json).map_err(|e| PackError::Json(e.to_string()))?;
    convert_compact(raw)
}

/// Parse a compact TOML string into a SchemaPack.
pub fn load_compact_toml(toml_str: &str) -> Result<SchemaPack, PackError> {
    let raw: CompactPack = toml::from_str(toml_str)?;
    convert_compact(raw)
}

/// Convert a CompactPack to a SchemaPack.
fn convert_compact(raw: CompactPack) -> Result<SchemaPack, PackError> {
    let mut nodes = Vec::with_capacity(raw.types.len());
    for (label, type_def) in raw.types {
        nodes.push(convert_compact_node(&label, type_def)?);
    }

    let mut edges = Vec::with_capacity(raw.relationships.len());
    for (label, edge_def) in raw.relationships {
        edges.push(convert_compact_edge(&label, edge_def)?);
    }

    Ok(SchemaPack {
        name: raw.name,
        version: raw.version,
        description: raw.description,
        ontology: raw.ontology,
        nodes,
        edges,
    })
}

fn convert_compact_node(label: &str, def: CompactNodeType) -> Result<NodeSchema, PackError> {
    let mut properties = Vec::with_capacity(def.fields.len());
    for (name, spec) in &def.fields {
        properties.push(parse_field_spec(name, spec)?);
    }

    properties.sort_by(|a, b| a.name.cmp(&b.name));

    let mut annotations = std::collections::HashMap::new();
    if let Some(ontology) = def.ontology {
        annotations.insert(Arc::from("ontology"), selene_core::Value::str(&ontology));
    }

    Ok(NodeSchema {
        label: Arc::from(label),
        parent: def.extends.map(|p| Arc::from(p.as_str())),
        properties,
        valid_edge_labels: def
            .edges
            .into_iter()
            .map(|s| Arc::from(s.as_str()))
            .collect(),
        description: def.description,
        annotations,
        version: Default::default(),
        validation_mode: def
            .validation
            .map(|v| parse_validation_mode(&v))
            .transpose()?,
        key_properties: vec![],
    })
}

fn convert_compact_edge(label: &str, def: CompactEdgeType) -> Result<EdgeSchema, PackError> {
    let mut properties = Vec::with_capacity(def.fields.len());
    for (name, spec) in &def.fields {
        properties.push(parse_field_spec(name, spec)?);
    }
    properties.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(EdgeSchema {
        label: Arc::from(label),
        properties,
        description: def.description,
        source_labels: def
            .source
            .into_iter()
            .map(|s| Arc::from(s.as_str()))
            .collect(),
        target_labels: def
            .target
            .into_iter()
            .map(|s| Arc::from(s.as_str()))
            .collect(),
        annotations: std::collections::HashMap::new(),
        version: Default::default(),
        validation_mode: None,
        max_out_degree: None,
        max_in_degree: None,
        min_out_degree: None,
        min_in_degree: None,
    })
}

/// Parse a field shorthand spec into a PropertyDef.
///
/// Grammar:
/// ```text
/// spec     := type_name ['!'] [' = ' default]
/// type     := string | int | float | bool | timestamp | bytes | list | any
/// required := '!' suffix means required
/// default  := quoted_string | number | true | false | null
/// ```
///
/// Examples:
/// - `"string"` → optional String, no default
/// - `"string!"` → required String
/// - `"string = '°F'"` → optional String, default "°F"
/// - `"float = 72.5"` → optional Float, default 72.5
/// - `"bool = true"` → optional Bool, default true
pub fn parse_field_spec(name: &str, spec: &str) -> Result<PropertyDef, PackError> {
    let spec = spec.trim();

    let (type_part, default_part) = if let Some(idx) = spec.find(" = ") {
        (&spec[..idx], Some(spec[idx + 3..].trim()))
    } else if let Some(idx) = spec.find('=') {
        (&spec[..idx], Some(spec[idx + 1..].trim()))
    } else {
        (spec, None)
    };

    let type_part = type_part.trim();

    let (type_name, required) = if let Some(stripped) = type_part.strip_suffix('!') {
        (stripped, true)
    } else {
        (type_part, false)
    };

    let value_type = parse_value_type(type_name)?;

    let default = default_part
        .map(|d| parse_default_value(d, &value_type, name))
        .transpose()?;

    Ok(PropertyDef {
        name: Arc::from(name),
        value_type,
        required,
        default,
        description: String::new(),
        indexed: false,
        unique: false,
        min: None,
        max: None,
        min_length: None,
        max_length: None,
        allowed_values: vec![],
        pattern: None,
        immutable: false,
        searchable: false,
        dictionary: false,
        fill: None,
        expected_interval_nanos: None,
        encoding: selene_core::ValueEncoding::Gorilla,
    })
}

/// Parse a default value string into a Value.
fn parse_default_value(s: &str, vt: &ValueType, prop_name: &str) -> Result<Value, PackError> {
    let s = s.trim();

    if s == "null" || s == "none" {
        return Ok(Value::Null);
    }

    if s.len() >= 2
        && ((s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')))
    {
        let inner = &s[1..s.len() - 1];
        return Ok(Value::str(inner));
    }

    if s == "true" {
        return Ok(Value::Bool(true));
    }
    if s == "false" {
        return Ok(Value::Bool(false));
    }

    if let Ok(i) = s.parse::<i64>() {
        return match vt {
            ValueType::Float => Ok(Value::Float(i as f64)),
            ValueType::ZonedDateTime => Ok(Value::Timestamp(i)),
            _ => Ok(Value::Int(i)),
        };
    }
    if let Ok(f) = s.parse::<f64>() {
        return Ok(Value::Float(f));
    }

    if matches!(vt, ValueType::String | ValueType::Any) {
        return Ok(Value::str(s));
    }

    Err(PackError::InvalidDefault {
        name: prop_name.into(),
        reason: format!("cannot parse '{s}' as {vt:?}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_field_optional_string() {
        let def = parse_field_spec("name", "string").unwrap();
        assert_eq!(&*def.name, "name");
        assert_eq!(def.value_type, ValueType::String);
        assert!(!def.required);
        assert_eq!(def.default, None);
    }

    #[test]
    fn parse_field_required_string() {
        let def = parse_field_spec("name", "string!").unwrap();
        assert!(def.required);
        assert_eq!(def.value_type, ValueType::String);
    }

    #[test]
    fn parse_field_string_with_default() {
        let def = parse_field_spec("unit", "string = '°F'").unwrap();
        assert!(!def.required);
        assert_eq!(def.default, Some(Value::str("°F")));
    }

    #[test]
    fn parse_field_float_with_default() {
        let def = parse_field_spec("setpoint", "float = 72.5").unwrap();
        assert_eq!(def.value_type, ValueType::Float);
        assert_eq!(def.default, Some(Value::Float(72.5)));
    }

    #[test]
    fn parse_field_int_with_default() {
        let def = parse_field_spec("interval", "int = 60").unwrap();
        assert_eq!(def.value_type, ValueType::Int);
        assert_eq!(def.default, Some(Value::Int(60)));
    }

    #[test]
    fn parse_field_bool_with_default() {
        let def = parse_field_spec("active", "bool = true").unwrap();
        assert_eq!(def.value_type, ValueType::Bool);
        assert_eq!(def.default, Some(Value::Bool(true)));
    }

    #[test]
    fn parse_field_int_coerced_to_float() {
        let def = parse_field_spec("temp", "float = 72").unwrap();
        assert_eq!(def.default, Some(Value::Float(72.0)));
    }

    #[test]
    fn compact_json_full() {
        let json = r#"{
            "name": "test",
            "version": "1.0",
            "description": "Test pack",
            "types": {
                "sensor": {
                    "extends": "equipment",
                    "description": "A sensor device",
                    "ontology": "brick:Sensor",
                    "fields": {
                        "name": "string!",
                        "unit": "string = '°F'",
                        "value": "float"
                    },
                    "edges": ["isPointOf", "hasLocation"]
                }
            },
            "relationships": {
                "monitors": {
                    "description": "Equipment monitors a zone",
                    "source": ["equipment"],
                    "target": ["zone"]
                }
            }
        }"#;

        let pack = load_compact_json(json).unwrap();
        assert_eq!(pack.name, "test");
        assert_eq!(pack.nodes.len(), 1);
        assert_eq!(pack.edges.len(), 1);

        let node = &pack.nodes[0];
        assert_eq!(&*node.label, "sensor");
        assert_eq!(node.parent.as_deref(), Some("equipment"));
        assert_eq!(node.properties.len(), 3);
        assert_eq!(node.valid_edge_labels.len(), 2);

        // Check that "name" is required
        let name_prop = node.properties.iter().find(|p| &*p.name == "name").unwrap();
        assert!(name_prop.required);

        // Check that "unit" has default
        let unit_prop = node.properties.iter().find(|p| &*p.name == "unit").unwrap();
        assert_eq!(unit_prop.default, Some(Value::str("°F")));

        let edge = &pack.edges[0];
        assert_eq!(&*edge.label, "monitors");
    }

    #[test]
    fn compact_toml_format() {
        let toml = r#"
name = "test"
version = "1.0"

[types.sensor]
extends = "equipment"
description = "A sensor"
fields = { name = "string!", unit = "string = '°F'", value = "float" }
"#;
        let pack = load_compact_toml(toml).unwrap();
        assert_eq!(pack.name, "test");
        assert_eq!(pack.nodes.len(), 1);
        assert_eq!(&*pack.nodes[0].label, "sensor");
    }

    #[test]
    fn parse_field_double_quoted_default() {
        let def = parse_field_spec("name", r#"string = "hello""#).unwrap();
        assert_eq!(def.default, Some(Value::str("hello")));
    }

    #[test]
    fn parse_field_invalid_type() {
        assert!(parse_field_spec("x", "invalid_type").is_err());
    }
}
