# Schema Guide

## Overview

Schemas define the expected shape of nodes and edges in the property graph. They are
optional -- Selene accepts any data without schemas registered. When a schema is present,
it validates properties on write and can either warn on mismatches (the default) or reject
them outright.

Schemas serve several purposes:

- **Validation** -- enforce property types, required fields, value ranges, and patterns
- **Defaults** -- auto-populate properties on node creation
- **Documentation** -- describe the data model with descriptions and annotations
- **Dictionary encoding** -- intern recurring string values for memory savings
- **Constraints** -- enforce uniqueness, immutability, composite keys, and edge cardinality

## Node Schemas

A `NodeSchema` is keyed by its `label` field. When a node carries that label, the schema's
property definitions are checked on every write (INSERT, SET, SET ALL, HTTP create/modify).

### NodeSchema Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `label` | string | (required) | The label this schema applies to |
| `parent` | string | none | Parent schema label for inheritance |
| `properties` | PropertyDef[] | [] | Property definitions (see below) |
| `valid_edge_labels` | string[] | [] | Allowed outgoing edge labels from nodes with this label |
| `description` | string | "" | Human-readable description |
| `annotations` | map | {} | Application-defined metadata. Selene stores and persists these but never interprets them. |
| `version` | semver | 1.0.0 | Schema version for evolution tracking |
| `validation_mode` | enum | (global default) | Override validation mode for this schema: `Warn` or `Strict` |
| `key_properties` | string[] | [] | Composite node key. These properties together must be unique across all nodes with this label. |

### HTTP Example

Schemas are registered via the HTTP API or MCP `create_schema` tool (there is no GQL procedure for schema creation).

```bash
curl -X POST http://localhost:8080/schemas/nodes \
  -H 'Content-Type: application/json' \
  -d '{
    "label": "sensor",
    "description": "A sensor that produces readings",
    "properties": [
      {"name": "name", "type": "STRING", "required": true},
      {"name": "unit", "type": "STRING", "default": "°F"},
      {"name": "status", "type": "STRING", "dictionary": true}
    ],
    "valid_edge_labels": ["isPointOf", "hasLocation"]
  }'
```

## Edge Schemas

An `EdgeSchema` validates edges with a matching label. In addition to property validation,
edge schemas can constrain which node labels are allowed at the source and target endpoints,
and enforce cardinality limits.

### EdgeSchema Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `label` | string | (required) | The edge label this schema applies to |
| `properties` | PropertyDef[] | [] | Property definitions |
| `description` | string | "" | Human-readable description |
| `source_labels` | string[] | [] | If non-empty, source node must carry at least one of these labels |
| `target_labels` | string[] | [] | If non-empty, target node must carry at least one of these labels |
| `annotations` | map | {} | Application-defined metadata |
| `version` | semver | 1.0.0 | Schema version |
| `validation_mode` | enum | (global default) | Override validation mode |
| `max_out_degree` | integer | none | Maximum outgoing edges of this type per source node |
| `max_in_degree` | integer | none | Maximum incoming edges of this type per target node |
| `min_out_degree` | integer | none | Minimum outgoing edges (stored, not yet enforced on write) |
| `min_in_degree` | integer | none | Minimum incoming edges (stored, not yet enforced on write) |

## Property Definitions

Each property is defined with a `PropertyDef` that controls its type, constraints, and
behavior during validation.

### PropertyDef Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | (required) | Property name |
| `value_type` | ValueType | (required) | Expected type (see value types below) |
| `required` | bool | false | Reject writes that omit this property (in Strict mode) |
| `default` | Value | none | Default value applied on node creation when the property is absent |
| `description` | string | "" | Human-readable description |
| `indexed` | bool | false | Create a TypedIndex for fast lookups on this property |
| `unique` | bool | false | No two nodes with the same label may share this property value |
| `immutable` | bool | false | Property cannot be changed after initial creation |
| `dictionary` | bool | false | Dictionary-encode string values via IStr interning |
| `searchable` | bool | false | Index string values for full-text search (requires `search` feature) |
| `min` | float | none | Numeric minimum value (inclusive) |
| `max` | float | none | Numeric maximum value (inclusive) |
| `min_length` | integer | none | Minimum string length |
| `max_length` | integer | none | Maximum string length |
| `allowed_values` | Value[] | [] | Allowed values (enum constraint). Empty means no restriction. |
| `pattern` | string | none | Regex pattern for string validation. Compiled and cached on first use. |

### Value Types

| ValueType | GQL Aliases | Description |
|-----------|-------------|-------------|
| `Bool` | BOOL, BOOLEAN | Boolean true/false |
| `Int` | INT, INT8..INT64, INTEGER, BIGINT, SMALLINT, SIGNED INTEGER | Signed 64-bit integer |
| `UInt` | UINT, UINT8..UINT64 | Unsigned 64-bit integer |
| `Float` | FLOAT, FLOAT16..FLOAT64, DOUBLE, DOUBLE PRECISION, REAL | 64-bit floating point |
| `String` | STRING, VARCHAR, TEXT | UTF-8 string |
| `ZonedDateTime` | ZONED DATETIME, TIMESTAMP, DATETIME | Timestamp with timezone |
| `Date` | DATE | Calendar date |
| `LocalDateTime` | LOCAL DATETIME | Date and time without timezone |
| `Duration` | DURATION | Time duration |
| `Bytes` | BYTES, BYTEA | Binary data |
| `List` | LIST | Ordered list of values |
| `Vector` | VECTOR | Numeric vector (for embeddings) |
| `Any` | ANY | Accept any value type |

## Validation Modes

The validation mode determines what happens when a write violates a schema. The mode
can be set globally on the `SchemaValidator` and overridden per schema.

| Mode | Behavior |
|------|----------|
| `Warn` (default) | Accept the data and log warnings for each schema mismatch. This is the default for all schemas unless overridden. |
| `Strict` | Reject writes that violate the schema. The write fails with a `SchemaViolation` error listing all issues. |

Validation checks include:
- Required properties present
- Property values match declared types (null is always acceptable)
- Numeric values within min/max range
- String values within length constraints
- Values in the allowed_values set
- String values match regex pattern
- Edge endpoints match source_labels/target_labels constraints
- Edge cardinality within max_out_degree/max_in_degree limits

## Dictionary Encoding

Setting `dictionary: true` on a `PropertyDef` enables dictionary encoding for that property.
When a string value is written to a dictionary-encoded property, it is automatically promoted
from `Value::String` to `Value::InternedStr(IStr)`. Interned strings are stored once in a
global string table and referenced by handle, so repeated values (common with enum-like
properties such as `status`, `zone_type`, or `unit`) share storage.

Measured savings are approximately 83% for typical building data where properties have
a small set of recurring values.

Dictionary encoding is applied on all write paths: GQL INSERT, GQL SET, GQL SET ALL,
HTTP node/edge create, and HTTP node/edge modify. The encoding applies to both node
and edge schemas.

Example:

```toml
[types.sensor]
fields = { status = "string" }
# Without dictionary: each node stores its own "active" / "fault" / "offline" string
# With dictionary: all nodes share a single interned copy per unique value
```

To enable via the HTTP API or MCP `create_schema` tool, set `dictionary: true` on the
property definition. In schema packs, there is currently no shorthand for dictionary
encoding -- use the full JSON/HTTP API.

## Schema Inheritance

A node schema can declare a `parent` (called `extends` in schema packs) to inherit
property definitions from another schema. The parent must be registered before the
child, or the child is registered as-is and inheritance is not resolved.

Inheritance rules:
- All parent properties are prepended to the child's property list
- Child properties with the same name as a parent property override the parent
- If the child declares no `valid_edge_labels`, it inherits the parent's
- Inheritance chains are depth-capped at 32 to guard against cycles
- Cycle detection is available via `has_inheritance_cycle(label, parent)` before registration

The full label chain (child through all ancestors) is resolvable via
`resolve_label_chain(label)`.

Example:

Register the parent first, then the child via the HTTP API:

```bash
# Register parent
curl -X POST http://localhost:8080/schemas/nodes \
  -H 'Content-Type: application/json' \
  -d '{
    "label": "equipment",
    "properties": [
      {"name": "name", "type": "STRING", "required": true},
      {"name": "manufacturer", "type": "STRING"},
      {"name": "model", "type": "STRING"}
    ]
  }'

# Child inherits name, manufacturer, model and adds setpoint
curl -X POST http://localhost:8080/schemas/nodes \
  -H 'Content-Type: application/json' \
  -d '{
    "label": "thermostat",
    "parent": "equipment",
    "properties": [
      {"name": "setpoint", "type": "FLOAT", "default": 72.0}
    ]
  }'
```

## Schema Evolution

Every schema carries a `SchemaVersion` with semver semantics (major.minor.patch,
defaulting to 1.0.0). When a schema is replaced with a new definition, Selene
classifies each change by severity and enforces version bump rules.

### Change Classification

| Severity | Changes |
|----------|---------|
| **Major** | Remove a property. Change a property's type. Make an optional property required without providing a default. Tighten range constraints (add or narrow min/max). |
| **Minor** | Add an optional property. Make a required property optional. Add a required property with a default value. |
| **Patch** | Change the description or annotations only. |

### Version Bump Rules

When replacing a schema:

- **No explicit bump** (new version equals old version): Selene auto-bumps the version
  based on the highest severity change detected.
- **Explicit bump provided**: Selene checks that the bump is sufficient. For example,
  a minor bump is rejected if the changes include a major-severity change. The
  replacement fails with a `CompatibilityError` listing all detected changes.
- **Import path** (snapshot recovery): Bypasses compatibility checking entirely.

Example: if a schema at version 1.2.0 has a property removed (major change) and no
explicit version is provided, the schema is auto-bumped to 2.0.0.

## Schema Packs

Schema packs are collections of node and edge schemas defined in a single TOML or JSON
file. They provide a compact format for loading domain-specific data models.

### Pack Format

Packs support a field shorthand syntax for concise property definitions:

| Shorthand | Result |
|-----------|--------|
| `"string"` | Optional String, no default |
| `"string!"` | Required String |
| `"string = '°F'"` | Optional String with default "°F" |
| `"float = 72.5"` | Optional Float with default 72.5 |
| `"int = 60"` | Optional Int with default 60 |
| `"bool = true"` | Optional Bool with default true |
| `"any"` | Optional Any type |

Node types are defined under `[types.<label>]` with `fields`, `description`, `extends`,
`edges`, and `validation` keys. Edge types go under `[relationships.<label>]` with
`description`, `source`, `target`, and `fields`.

### Built-in Packs

Selene ships with a `common` pack embedded at compile time. Load it via
`selene_packs::builtin("common")` in Rust or the `import_schema_pack` MCP tool.

### Example: common.toml

```toml
# Common schema pack -- shared building/IoT entity types.

name = "common"
version = "1.0.0"
description = "Common entity types for buildings, spaces, equipment, and points"

# -- Spatial hierarchy --

[types.site]
description = "A physical location (campus, property)"
fields = { name = "string!", address = "string", latitude = "float", longitude = "float" }

[types.building]
description = "A building within a site"
fields = { name = "string!", area_sqft = "float", year_built = "int" }

[types.floor]
description = "A floor or level within a building"
fields = { name = "string!", level = "int" }

[types.zone]
description = "A thermal or functional zone"
fields = { name = "string!", zone_type = "string" }

[types.space]
description = "A physical room or area"
fields = { name = "string!", area_sqft = "float", occupancy_max = "int" }

# -- Equipment & Points --

[types.equipment]
description = "A mechanical, electrical, or control device"
fields = { name = "string!", manufacturer = "string", model = "string", serial_number = "string" }

[types.point]
description = "A sensor, setpoint, command, or status value"
fields = { name = "string!", unit = "string", current_value = "any", point_type = "string" }

# -- Relationships --

[relationships.contains]
description = "Spatial or logical containment (site > building > floor > zone > space)"

[relationships.feeds]
description = "Equipment feeds another equipment or zone"
source = ["equipment"]
fields = { medium = "string" }

[relationships.isPointOf]
description = "A point belongs to a piece of equipment"
source = ["point"]
target = ["equipment"]

[relationships.hasLocation]
description = "An entity is physically located in a space"
target = ["space", "zone", "floor", "building", "site"]

[relationships.monitors]
description = "A point or system monitors an entity"
```

### Loading a Custom Pack

Via the HTTP API:

```bash
curl -X POST http://localhost:8080/schema/pack \
  -H 'Content-Type: application/toml' \
  --data-binary @my-building-pack.toml
```

Via the MCP `import_schema_pack` tool, pass the file contents as the `toml` parameter.
The format (JSON or TOML) is auto-detected from the first non-whitespace character.

## GQL Introspection

Several built-in procedures expose schema information at runtime:

```gql
-- Get a specific schema by label
CALL graph.schema('sensor')
  YIELD property, valueType, required, unique, indexed, immutable, constraints

-- List all schema constraints across all registered schemas
CALL graph.constraints()
  YIELD label, constraintType, properties, description

-- Discover the actual shape of data in the graph (scans all nodes)
CALL graph.discoverSchema()
  YIELD label, property, inferredType, nullRate, uniqueRate, sampleSize
```

These procedures work regardless of whether schemas are registered. `discoverSchema`
samples the live graph data and reports what it finds, which is useful for exploring
an untyped graph before defining schemas.
