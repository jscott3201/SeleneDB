//! Schema registry -- optional type definitions for nodes and edges.

use std::collections::HashMap;
use std::sync::Arc;

use crate::value::Value;

/// Value type for schema property definitions.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ValueType {
    Bool,
    Int,
    UInt,
    Float,
    String,
    ZonedDateTime,
    Date,
    LocalDateTime,
    Duration,
    Bytes,
    List,
    Vector,
    Any,
}

impl std::str::FromStr for ValueType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Normalize whitespace for multi-word types (ZONED  DATETIME, etc.)
        let normalized: String = s.split_whitespace().collect::<Vec<_>>().join(" ");
        let upper = normalized.to_uppercase();
        match upper.as_str() {
            "BOOL" | "BOOLEAN" => Ok(Self::Bool),
            "INT" | "INT8" | "INT16" | "INT32" | "INT64" | "INTEGER" | "BIGINT" | "SMALLINT" => {
                Ok(Self::Int)
            }
            "UINT" | "UINT8" | "UINT16" | "UINT32" | "UINT64" => Ok(Self::UInt),
            "FLOAT" | "FLOAT16" | "FLOAT32" | "FLOAT64" | "DOUBLE" | "DOUBLE PRECISION"
            | "REAL" => Ok(Self::Float),
            "STRING" | "VARCHAR" | "TEXT" => Ok(Self::String),
            "BYTES" | "BYTEA" => Ok(Self::Bytes),
            "DATE" => Ok(Self::Date),
            "DURATION" => Ok(Self::Duration),
            "ZONED DATETIME" | "TIMESTAMP" | "DATETIME" => Ok(Self::ZonedDateTime),
            "LOCAL DATETIME" => Ok(Self::LocalDateTime),
            "VECTOR" => Ok(Self::Vector),
            "LIST" => Ok(Self::List),
            "ANY" => Ok(Self::Any),
            "SIGNED INTEGER" | "SIGNED INT" => Ok(Self::Int),
            other => Err(format!("unknown value type: {other}")),
        }
    }
}

/// Schema validation mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ValidationMode {
    /// Accept data, log warnings on schema mismatch.
    #[default]
    Warn,
    /// Reject writes that violate schema.
    Strict,
}

/// Strategy for filling gaps between time-series samples.
/// Used as a per-property default in schemas and as a query-time override.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FillStrategy {
    /// Last observation carried forward: hold the most recent value.
    Locf,
    /// Linear interpolation between the two bracketing samples.
    Linear,
}

/// Encoding strategy for time-series value compression in the hot tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum ValueEncoding {
    /// XOR compression with leading/trailing zero optimization (Gorilla paper).
    /// Best for continuous analog sensors (temperature, pressure, humidity).
    #[default]
    Gorilla,
    /// Run-length encoding. Best for binary/digital sensors (occupancy, door contacts)
    /// where values hold steady for long periods.
    Rle,
    /// Codebook encoding. Best for discrete/enum sensors (HVAC mode, fan speed)
    /// with a small set of repeating float values. Falls back to Gorilla if >256
    /// distinct values in a block.
    Dictionary,
}

/// Semantic version for schema evolution tracking.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct SchemaVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl SchemaVersion {
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }
    pub fn bump_major(&self) -> Self {
        Self {
            major: self.major + 1,
            minor: 0,
            patch: 0,
        }
    }
    pub fn bump_minor(&self) -> Self {
        Self {
            major: self.major,
            minor: self.minor + 1,
            patch: 0,
        }
    }
    pub fn bump_patch(&self) -> Self {
        Self {
            major: self.major,
            minor: self.minor,
            patch: self.patch + 1,
        }
    }
}

impl Default for SchemaVersion {
    fn default() -> Self {
        Self {
            major: 1,
            minor: 0,
            patch: 0,
        }
    }
}

impl std::fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl std::str::FromStr for SchemaVersion {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('.').collect();
        match parts.len() {
            3 => Ok(Self {
                major: parts[0]
                    .parse()
                    .map_err(|e| format!("invalid major: {e}"))?,
                minor: parts[1]
                    .parse()
                    .map_err(|e| format!("invalid minor: {e}"))?,
                patch: parts[2]
                    .parse()
                    .map_err(|e| format!("invalid patch: {e}"))?,
            }),
            2 => Ok(Self {
                major: parts[0]
                    .parse()
                    .map_err(|e| format!("invalid major: {e}"))?,
                minor: parts[1]
                    .parse()
                    .map_err(|e| format!("invalid minor: {e}"))?,
                patch: 0,
            }),
            1 => Ok(Self {
                major: parts[0]
                    .parse()
                    .map_err(|e| format!("invalid major: {e}"))?,
                minor: 0,
                patch: 0,
            }),
            _ => Err(format!(
                "invalid version format: '{s}', expected 'major.minor.patch'"
            )),
        }
    }
}

/// Defines the expected shape of nodes with a given label.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeSchema {
    pub label: Arc<str>,
    /// Parent schema label for inheritance. If set, this schema inherits
    /// all property definitions from the parent (resolved at registration time).
    pub parent: Option<Arc<str>>,
    pub properties: Vec<PropertyDef>,
    #[serde(default)]
    pub valid_edge_labels: Vec<Arc<str>>,
    #[serde(default)]
    pub description: String,
    /// Application-defined annotations on this type definition.
    /// Selene stores and persists these but never interprets them.
    #[serde(default)]
    pub annotations: HashMap<Arc<str>, Value>,
    /// Schema version (semver). Compatibility checked on replace.
    #[serde(default)]
    pub version: SchemaVersion,
    /// Override the global validation mode for this schema.
    /// `None` means use the global default.
    #[serde(default)]
    pub validation_mode: Option<ValidationMode>,
    /// Composite node key: these properties together must be unique across
    /// all nodes with this label. Empty = no composite key constraint.
    #[serde(default)]
    pub key_properties: Vec<Arc<str>>,
}

/// Defines the expected shape of edges with a given label.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EdgeSchema {
    pub label: Arc<str>,
    pub properties: Vec<PropertyDef>,
    #[serde(default)]
    pub description: String,
    /// If non-empty, source node must carry at least one of these labels.
    #[serde(default)]
    pub source_labels: Vec<Arc<str>>,
    /// If non-empty, target node must carry at least one of these labels.
    #[serde(default)]
    pub target_labels: Vec<Arc<str>>,
    /// Application-defined annotations on this type definition.
    /// Selene stores and persists these but never interprets them.
    #[serde(default)]
    pub annotations: HashMap<Arc<str>, Value>,
    /// Schema version (semver). Compatibility checked on replace.
    #[serde(default)]
    pub version: SchemaVersion,
    /// Override the global validation mode for this schema.
    #[serde(default)]
    pub validation_mode: Option<ValidationMode>,
    /// Maximum outgoing edges of this type per source node (enforced on create).
    #[serde(default)]
    pub max_out_degree: Option<u32>,
    /// Maximum incoming edges of this type per target node (enforced on create).
    #[serde(default)]
    pub max_in_degree: Option<u32>,
    /// Minimum outgoing edges of this type per source node.
    /// Stored and displayed by `graph.schemaInfo` but not yet validated at write time.
    #[serde(default)]
    pub min_out_degree: Option<u32>,
    /// Minimum incoming edges of this type per target node.
    /// Stored and displayed by `graph.schemaInfo` but not yet validated at write time.
    #[serde(default)]
    pub min_in_degree: Option<u32>,
}

/// A property definition within a schema.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct PropertyDef {
    pub name: Arc<str>,
    pub value_type: ValueType,
    pub required: bool,
    #[serde(default)]
    pub default: Option<Value>,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub indexed: bool,
    /// No two nodes with the same label may share this property value.
    #[serde(default)]
    pub unique: bool,
    /// Numeric minimum value (inclusive).
    #[serde(default)]
    pub min: Option<f64>,
    /// Numeric maximum value (inclusive).
    #[serde(default)]
    pub max: Option<f64>,
    /// String minimum length.
    #[serde(default)]
    pub min_length: Option<usize>,
    /// String maximum length.
    #[serde(default)]
    pub max_length: Option<usize>,
    /// Allowed values (enum constraint). Empty = no restriction.
    #[serde(default)]
    pub allowed_values: Vec<Value>,
    /// Regex pattern for string validation. Compiled and cached on registration.
    #[serde(default)]
    pub pattern: Option<String>,
    /// Property cannot be changed after initial creation.
    #[serde(default)]
    pub immutable: bool,
    /// Full-text search indexing. When true, string values are indexed via tantivy.
    #[serde(default)]
    pub searchable: bool,
    /// Dictionary-encode string values via IStr interning.
    /// Reduces memory for recurring enum-like string values.
    #[serde(default)]
    pub dictionary: bool,
    /// Time-series gap-filling strategy. When set, TS query procedures use
    /// this as the default fill behavior for this property.
    #[serde(default)]
    pub fill: Option<FillStrategy>,
    /// Expected reporting interval in nanoseconds. Used by ts.gaps for
    /// default gap threshold (3x this value).
    #[serde(default)]
    pub expected_interval_nanos: Option<i64>,
    /// Time-series value encoding strategy for the hot tier.
    /// Defaults to Gorilla (XOR compression).
    #[serde(default)]
    pub encoding: ValueEncoding,
}

impl PropertyDef {
    /// Create a simple property definition with only name, type, and required.
    /// All constraint fields default to off/None.
    pub fn simple(name: &str, value_type: ValueType, required: bool) -> Self {
        Self {
            name: Arc::from(name),
            value_type,
            required,
            default: None,
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
            encoding: ValueEncoding::Gorilla,
        }
    }

    /// Start a fluent builder for a property definition.
    pub fn builder(name: &str, value_type: ValueType) -> PropertyDefBuilder {
        PropertyDefBuilder {
            def: PropertyDef::simple(name, value_type, false),
        }
    }
}

/// Fluent builder for [`PropertyDef`].
pub struct PropertyDefBuilder {
    def: PropertyDef,
}

impl PropertyDefBuilder {
    pub fn required(mut self, required: bool) -> Self {
        self.def.required = required;
        self
    }
    pub fn default_value(mut self, value: Value) -> Self {
        self.def.default = Some(value);
        self
    }
    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.def.description = desc.into();
        self
    }
    pub fn indexed(mut self) -> Self {
        self.def.indexed = true;
        self
    }
    pub fn unique(mut self) -> Self {
        self.def.unique = true;
        self
    }
    pub fn searchable(mut self) -> Self {
        self.def.searchable = true;
        self
    }
    pub fn dictionary(mut self) -> Self {
        self.def.dictionary = true;
        self
    }
    pub fn immutable(mut self) -> Self {
        self.def.immutable = true;
        self
    }
    pub fn min(mut self, min: f64) -> Self {
        self.def.min = Some(min);
        self
    }
    pub fn max(mut self, max: f64) -> Self {
        self.def.max = Some(max);
        self
    }
    pub fn min_length(mut self, len: usize) -> Self {
        self.def.min_length = Some(len);
        self
    }
    pub fn max_length(mut self, len: usize) -> Self {
        self.def.max_length = Some(len);
        self
    }
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.def.pattern = Some(pattern.into());
        self
    }
    pub fn allowed_values(mut self, values: Vec<Value>) -> Self {
        self.def.allowed_values = values;
        self
    }
    pub fn encoding(mut self, encoding: ValueEncoding) -> Self {
        self.def.encoding = encoding;
        self
    }
    pub fn build(self) -> PropertyDef {
        self.def
    }
}

impl NodeSchema {
    /// Start a fluent builder for a node schema.
    pub fn builder(label: &str) -> NodeSchemaBuilder {
        NodeSchemaBuilder {
            schema: NodeSchema {
                label: Arc::from(label),
                parent: None,
                properties: Vec::new(),
                valid_edge_labels: Vec::new(),
                description: String::new(),
                annotations: HashMap::new(),
                version: SchemaVersion::default(),
                validation_mode: None,
                key_properties: Vec::new(),
            },
        }
    }
}

/// Fluent builder for [`NodeSchema`].
pub struct NodeSchemaBuilder {
    schema: NodeSchema,
}

impl NodeSchemaBuilder {
    pub fn parent(mut self, parent: &str) -> Self {
        self.schema.parent = Some(Arc::from(parent));
        self
    }
    pub fn property(mut self, prop: PropertyDef) -> Self {
        self.schema.properties.push(prop);
        self
    }
    pub fn valid_edge(mut self, label: &str) -> Self {
        self.schema.valid_edge_labels.push(Arc::from(label));
        self
    }
    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.schema.description = desc.into();
        self
    }
    pub fn key_property(mut self, name: &str) -> Self {
        self.schema.key_properties.push(Arc::from(name));
        self
    }
    pub fn version(mut self, version: SchemaVersion) -> Self {
        self.schema.version = version;
        self
    }
    pub fn validation_mode(mut self, mode: ValidationMode) -> Self {
        self.schema.validation_mode = Some(mode);
        self
    }
    pub fn build(self) -> NodeSchema {
        self.schema
    }
}

impl EdgeSchema {
    /// Start a fluent builder for an edge schema.
    pub fn builder(label: &str) -> EdgeSchemaBuilder {
        EdgeSchemaBuilder {
            schema: EdgeSchema {
                label: Arc::from(label),
                properties: Vec::new(),
                description: String::new(),
                source_labels: Vec::new(),
                target_labels: Vec::new(),
                annotations: HashMap::new(),
                version: SchemaVersion::default(),
                validation_mode: None,
                max_out_degree: None,
                max_in_degree: None,
                min_out_degree: None,
                min_in_degree: None,
            },
        }
    }
}

/// Fluent builder for [`EdgeSchema`].
pub struct EdgeSchemaBuilder {
    schema: EdgeSchema,
}

impl EdgeSchemaBuilder {
    pub fn property(mut self, prop: PropertyDef) -> Self {
        self.schema.properties.push(prop);
        self
    }
    pub fn source_label(mut self, label: &str) -> Self {
        self.schema.source_labels.push(Arc::from(label));
        self
    }
    pub fn target_label(mut self, label: &str) -> Self {
        self.schema.target_labels.push(Arc::from(label));
        self
    }
    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.schema.description = desc.into();
        self
    }
    pub fn max_out_degree(mut self, n: u32) -> Self {
        self.schema.max_out_degree = Some(n);
        self
    }
    pub fn max_in_degree(mut self, n: u32) -> Self {
        self.schema.max_in_degree = Some(n);
        self
    }
    pub fn min_out_degree(mut self, n: u32) -> Self {
        self.schema.min_out_degree = Some(n);
        self
    }
    pub fn min_in_degree(mut self, n: u32) -> Self {
        self.schema.min_in_degree = Some(n);
        self
    }
    pub fn version(mut self, version: SchemaVersion) -> Self {
        self.schema.version = version;
        self
    }
    pub fn build(self) -> EdgeSchema {
        self.schema
    }
}

/// A collection of schemas loaded from a pack file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SchemaPack {
    /// Pack name (e.g., "brick", "haystack", "common").
    pub name: String,
    /// Pack version (e.g., "1.3.0").
    pub version: String,
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
    /// Ontology URI (e.g., `"https://brickschema.org/schema/Brick"`).
    #[serde(default)]
    pub ontology: Option<String>,
    /// Node schema definitions.
    #[serde(default)]
    pub nodes: Vec<NodeSchema>,
    /// Edge schema definitions.
    #[serde(default)]
    pub edges: Vec<EdgeSchema>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_schema_creation() {
        let mut prop = PropertyDef::simple("unit", ValueType::String, false);
        prop.default = Some(Value::str("°F"));
        prop.description = "Unit of measurement".into();
        let schema = NodeSchema {
            label: Arc::from("temperature_sensor"),
            parent: Some(Arc::from("sensor")),
            properties: vec![prop],
            valid_edge_labels: vec![Arc::from("isPointOf"), Arc::from("hasLocation")],
            description: "A sensor that measures temperature".into(),
            annotations: HashMap::from([(Arc::from("brick"), Value::str("Temperature_Sensor"))]),
            version: SchemaVersion::new(1, 3, 0),
            validation_mode: Some(ValidationMode::Strict),
            key_properties: vec![],
        };
        assert_eq!(&*schema.label, "temperature_sensor");
        assert_eq!(schema.parent.as_deref(), Some("sensor"));
        assert_eq!(schema.properties.len(), 1);
        assert_eq!(schema.version, SchemaVersion::new(1, 3, 0));
    }

    #[test]
    fn edge_schema_creation() {
        let schema = EdgeSchema {
            label: Arc::from("feeds"),
            properties: vec![PropertyDef::simple("medium", ValueType::String, false)],
            description: "Equipment feeds relationship".into(),
            source_labels: vec![Arc::from("equipment")],
            target_labels: vec![Arc::from("equipment")],
            annotations: HashMap::new(),
            version: SchemaVersion::default(),
            validation_mode: None,
            max_out_degree: None,
            max_in_degree: None,
            min_out_degree: None,
            min_in_degree: None,
        };
        assert_eq!(&*schema.label, "feeds");
    }

    #[test]
    fn validation_mode_default() {
        assert_eq!(ValidationMode::default(), ValidationMode::Warn);
    }

    #[test]
    fn property_def_builder() {
        let prop = PropertyDef::builder("temperature", ValueType::Float)
            .required(true)
            .default_value(Value::Float(72.0))
            .description("Current temperature reading")
            .indexed()
            .unique()
            .searchable()
            .immutable()
            .min(0.0)
            .max(200.0)
            .build();

        assert_eq!(&*prop.name, "temperature");
        assert_eq!(prop.value_type, ValueType::Float);
        assert!(prop.required);
        assert_eq!(prop.default, Some(Value::Float(72.0)));
        assert_eq!(prop.description, "Current temperature reading");
        assert!(prop.indexed);
        assert!(prop.unique);
        assert!(prop.searchable);
        assert!(prop.immutable);
        assert_eq!(prop.min, Some(0.0));
        assert_eq!(prop.max, Some(200.0));
    }

    #[test]
    fn property_def_builder_dictionary() {
        let prop = PropertyDef::builder("status", ValueType::String)
            .dictionary()
            .build();
        assert!(prop.dictionary);

        // Default is false
        let prop2 = PropertyDef::simple("name", ValueType::String, false);
        assert!(!prop2.dictionary);
    }

    #[test]
    fn node_schema_builder() {
        let schema = NodeSchema::builder("temperature_sensor")
            .parent("sensor")
            .property(
                PropertyDef::builder("unit", ValueType::String)
                    .required(true)
                    .build(),
            )
            .property(PropertyDef::simple("value", ValueType::Float, false))
            .valid_edge("isPointOf")
            .valid_edge("hasLocation")
            .description("A sensor that measures temperature")
            .key_property("external_id")
            .validation_mode(ValidationMode::Strict)
            .build();

        assert_eq!(&*schema.label, "temperature_sensor");
        assert_eq!(schema.parent.as_deref(), Some("sensor"));
        assert_eq!(schema.properties.len(), 2);
        assert!(schema.properties[0].required);
        assert_eq!(schema.valid_edge_labels.len(), 2);
        assert_eq!(&*schema.valid_edge_labels[0], "isPointOf");
        assert_eq!(schema.description, "A sensor that measures temperature");
        assert_eq!(schema.key_properties.len(), 1);
        assert_eq!(&*schema.key_properties[0], "external_id");
        assert_eq!(schema.validation_mode, Some(ValidationMode::Strict));
    }

    #[test]
    fn edge_schema_builder() {
        let schema = EdgeSchema::builder("feeds")
            .property(PropertyDef::simple("medium", ValueType::String, false))
            .source_label("equipment")
            .target_label("equipment")
            .description("Equipment feeds relationship")
            .max_out_degree(5)
            .max_in_degree(10)
            .min_out_degree(1)
            .min_in_degree(0)
            .build();

        assert_eq!(&*schema.label, "feeds");
        assert_eq!(schema.properties.len(), 1);
        assert_eq!(schema.source_labels.len(), 1);
        assert_eq!(&*schema.source_labels[0], "equipment");
        assert_eq!(schema.target_labels.len(), 1);
        assert_eq!(schema.description, "Equipment feeds relationship");
        assert_eq!(schema.max_out_degree, Some(5));
        assert_eq!(schema.max_in_degree, Some(10));
        assert_eq!(schema.min_out_degree, Some(1));
        assert_eq!(schema.min_in_degree, Some(0));
    }

    #[test]
    fn schema_pack_creation() {
        let pack = SchemaPack {
            name: "brick".into(),
            version: "1.3.0".into(),
            description: "Brick Schema for smart buildings".into(),
            ontology: Some("https://brickschema.org/schema/Brick".into()),
            nodes: vec![],
            edges: vec![],
        };
        assert_eq!(pack.name, "brick");
    }

    #[test]
    fn schema_version_parse_display() {
        let v: SchemaVersion = "2.1.3".parse().unwrap();
        assert_eq!(v, SchemaVersion::new(2, 1, 3));
        assert_eq!(v.to_string(), "2.1.3");
        let v2: SchemaVersion = "3.0".parse().unwrap();
        assert_eq!(v2, SchemaVersion::new(3, 0, 0));
        assert!("abc".parse::<SchemaVersion>().is_err());
    }

    #[test]
    fn schema_version_ordering() {
        let v1 = SchemaVersion::new(1, 0, 0);
        let v2 = SchemaVersion::new(1, 1, 0);
        let v3 = SchemaVersion::new(2, 0, 0);
        assert!(v1 < v2);
        assert!(v2 < v3);
    }

    #[test]
    fn schema_version_bump() {
        let v = SchemaVersion::new(1, 2, 3);
        assert_eq!(v.bump_major(), SchemaVersion::new(2, 0, 0));
        assert_eq!(v.bump_minor(), SchemaVersion::new(1, 3, 0));
        assert_eq!(v.bump_patch(), SchemaVersion::new(1, 2, 4));
    }

    #[test]
    fn schema_version_default() {
        assert_eq!(SchemaVersion::default(), SchemaVersion::new(1, 0, 0));
    }
}
