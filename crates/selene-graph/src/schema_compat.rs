//! Schema compatibility checking for schema evolution.
//!
//! Detects breaking changes, minor additions, and patch-level modifications
//! when a schema is replaced. Auto-bumps the version when the user does not
//! provide an explicit version, or validates that an explicit version bump
//! meets the severity of detected changes.

use std::collections::HashMap;
use std::sync::Arc;

use selene_core::Value;
use selene_core::schema::{EdgeSchema, NodeSchema, PropertyDef, SchemaVersion};

// ── Types ────────────────────────────────────────────────────────────────────

/// Severity of a schema change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ChangeSeverity {
    Patch,
    Minor,
    Major,
}

/// A single detected schema change.
#[derive(Debug, Clone)]
pub struct SchemaChange {
    pub description: String,
    pub severity: ChangeSeverity,
}

/// Result of schema compatibility checking.
#[derive(Debug)]
pub struct CompatibilityError {
    pub required_severity: ChangeSeverity,
    pub provided_bump: Option<ChangeSeverity>,
    pub changes: Vec<SchemaChange>,
}

impl std::fmt::Display for CompatibilityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "schema version bump required: {:?}, changes: ",
            self.required_severity
        )?;
        for (i, c) in self.changes.iter().enumerate() {
            if i > 0 {
                write!(f, "; ")?;
            }
            write!(f, "{} ({:?})", c.description, c.severity)?;
        }
        Ok(())
    }
}

impl std::error::Error for CompatibilityError {}

// ── Compatibility functions ──────────────────────────────────────────────────

/// Compare old and new property lists, classifying each change by severity.
///
/// Returns `Ok(auto_version)` with the auto-bumped version when the user did
/// not explicitly bump the version (old.version == new.version). When the user
/// provided an explicit version bump, validates that the bump is sufficient
/// for the severity of changes detected.
#[allow(clippy::too_many_arguments)]
pub(crate) fn check_property_compatibility(
    old_props: &[PropertyDef],
    new_props: &[PropertyDef],
    old_version: &SchemaVersion,
    new_version: &SchemaVersion,
    old_description: &str,
    new_description: &str,
    old_annotations: &HashMap<Arc<str>, Value>,
    new_annotations: &HashMap<Arc<str>, Value>,
) -> Result<SchemaVersion, CompatibilityError> {
    let mut changes = Vec::new();

    // Check removed properties (major)
    for old_prop in old_props {
        if !new_props.iter().any(|p| p.name == old_prop.name) {
            changes.push(SchemaChange {
                description: format!("removed property '{}'", old_prop.name),
                severity: ChangeSeverity::Major,
            });
        }
    }

    for new_prop in new_props {
        if let Some(old_prop) = old_props.iter().find(|p| p.name == new_prop.name) {
            // Type changed (major)
            if old_prop.value_type != new_prop.value_type {
                changes.push(SchemaChange {
                    description: format!(
                        "changed type of '{}' from {:?} to {:?}",
                        new_prop.name, old_prop.value_type, new_prop.value_type
                    ),
                    severity: ChangeSeverity::Major,
                });
            }
            // Required added (major) -- was optional, now required without default
            if !old_prop.required && new_prop.required && new_prop.default.is_none() {
                changes.push(SchemaChange {
                    description: format!(
                        "property '{}' made required without default",
                        new_prop.name
                    ),
                    severity: ChangeSeverity::Major,
                });
            }
            // Required relaxed (minor)
            if old_prop.required && !new_prop.required {
                changes.push(SchemaChange {
                    description: format!("property '{}' made optional", new_prop.name),
                    severity: ChangeSeverity::Minor,
                });
            }
            // Constraint tightened: range narrowed (major)
            let max_tightened = match (old_prop.max, new_prop.max) {
                (None, Some(_)) => true,
                (Some(old_max), Some(new_max)) => new_max < old_max,
                _ => false,
            };
            let min_tightened = match (old_prop.min, new_prop.min) {
                (None, Some(_)) => true,
                (Some(old_min), Some(new_min)) => new_min > old_min,
                _ => false,
            };
            if max_tightened || min_tightened {
                changes.push(SchemaChange {
                    description: format!("tightened range constraint on '{}'", new_prop.name),
                    severity: ChangeSeverity::Major,
                });
            }
        } else {
            // New property
            if new_prop.required && new_prop.default.is_none() {
                changes.push(SchemaChange {
                    description: format!(
                        "added required property '{}' without default",
                        new_prop.name
                    ),
                    severity: ChangeSeverity::Major,
                });
            } else {
                changes.push(SchemaChange {
                    description: format!("added optional property '{}'", new_prop.name),
                    severity: ChangeSeverity::Minor,
                });
            }
        }
    }

    // Description/annotation-only changes
    if changes.is_empty()
        && (old_description != new_description || old_annotations != new_annotations)
    {
        changes.push(SchemaChange {
            description: "description or annotations changed".into(),
            severity: ChangeSeverity::Patch,
        });
    }

    // Determine required severity
    let required = changes
        .iter()
        .map(|c| c.severity)
        .max()
        .unwrap_or(ChangeSeverity::Patch);

    // Check version bump
    if new_version == old_version {
        // Auto-bump: return the auto-bumped version
        let auto = match required {
            ChangeSeverity::Major => old_version.bump_major(),
            ChangeSeverity::Minor => old_version.bump_minor(),
            ChangeSeverity::Patch => old_version.bump_patch(),
        };
        return Ok(auto);
    }

    // User provided a version -- validate it is sufficient
    let provided_bump = if new_version.major > old_version.major {
        Some(ChangeSeverity::Major)
    } else if new_version.minor > old_version.minor {
        Some(ChangeSeverity::Minor)
    } else if new_version.patch > old_version.patch {
        Some(ChangeSeverity::Patch)
    } else if new_version < old_version {
        return Err(CompatibilityError {
            required_severity: required,
            provided_bump: None,
            changes: vec![SchemaChange {
                description: format!("version downgrade: {old_version} -> {new_version}"),
                severity: ChangeSeverity::Major,
            }],
        });
    } else {
        Some(ChangeSeverity::Patch)
    };

    if let Some(bump) = provided_bump
        && bump < required
    {
        return Err(CompatibilityError {
            required_severity: required,
            provided_bump: Some(bump),
            changes,
        });
    }

    Ok(new_version.clone())
}

/// Check compatibility between old and new node schemas.
///
/// Returns the auto-bumped version when versions are equal (auto-bump mode),
/// or validates the explicit version bump is sufficient.
pub(crate) fn check_node_compatibility(
    old: &NodeSchema,
    new: &NodeSchema,
) -> Result<SchemaVersion, CompatibilityError> {
    check_property_compatibility(
        &old.properties,
        &new.properties,
        &old.version,
        &new.version,
        &old.description,
        &new.description,
        &old.annotations,
        &new.annotations,
    )
}

/// Check compatibility between old and new edge schemas.
///
/// Returns the auto-bumped version when versions are equal (auto-bump mode),
/// or validates the explicit version bump is sufficient.
pub(crate) fn check_edge_compatibility(
    old: &EdgeSchema,
    new: &EdgeSchema,
) -> Result<SchemaVersion, CompatibilityError> {
    check_property_compatibility(
        &old.properties,
        &new.properties,
        &old.version,
        &new.version,
        &old.description,
        &new.description,
        &old.annotations,
        &new.annotations,
    )
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use selene_core::schema::{
        EdgeSchema, NodeSchema, PropertyDef, SchemaVersion, ValidationMode, ValueType,
    };

    use super::*;
    use crate::schema::SchemaValidator;

    #[test]
    fn compat_add_optional_property_is_minor() {
        let old = NodeSchema::builder("Sensor")
            .property(PropertyDef::simple("temp", ValueType::Float, false))
            .build();
        let new = NodeSchema::builder("Sensor")
            .property(PropertyDef::simple("temp", ValueType::Float, false))
            .property(PropertyDef::simple("unit", ValueType::String, false))
            .build();
        let result = check_node_compatibility(&old, &new);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SchemaVersion::new(1, 1, 0));
    }

    #[test]
    fn compat_remove_property_is_major() {
        let old = NodeSchema::builder("Sensor")
            .property(PropertyDef::simple("temp", ValueType::Float, false))
            .property(PropertyDef::simple("unit", ValueType::String, false))
            .build();
        let new = NodeSchema::builder("Sensor")
            .property(PropertyDef::simple("temp", ValueType::Float, false))
            .build();
        let result = check_node_compatibility(&old, &new);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SchemaVersion::new(2, 0, 0));
    }

    #[test]
    fn compat_type_change_is_major() {
        let old = NodeSchema::builder("Sensor")
            .property(PropertyDef::simple("temp", ValueType::Float, false))
            .build();
        let new = NodeSchema::builder("Sensor")
            .property(PropertyDef::simple("temp", ValueType::String, false))
            .build();
        let result = check_node_compatibility(&old, &new);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SchemaVersion::new(2, 0, 0));
    }

    #[test]
    fn compat_insufficient_explicit_bump_rejected() {
        let old = NodeSchema::builder("Sensor")
            .property(PropertyDef::simple("temp", ValueType::Float, false))
            .build();
        let new = NodeSchema::builder("Sensor")
            .version(SchemaVersion::new(1, 1, 0))
            .build();
        let result = check_node_compatibility(&old, &new);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.required_severity, ChangeSeverity::Major);
    }

    #[test]
    fn compat_no_changes_no_bump() {
        let old = NodeSchema::builder("Sensor")
            .property(PropertyDef::simple("temp", ValueType::Float, false))
            .build();
        let new = old.clone();
        let result = check_node_compatibility(&old, &new);
        assert!(result.is_ok());
    }

    #[test]
    fn compat_required_without_default_is_major() {
        let old = NodeSchema::builder("Sensor")
            .property(PropertyDef::simple("temp", ValueType::Float, false))
            .build();
        let new = NodeSchema::builder("Sensor")
            .property(PropertyDef::simple("temp", ValueType::Float, false))
            .property(PropertyDef::simple("unit", ValueType::String, true))
            .build();
        let result = check_node_compatibility(&old, &new);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SchemaVersion::new(2, 0, 0));
    }

    #[test]
    fn compat_tightened_range_is_major() {
        let mut old_prop = PropertyDef::simple("temp", ValueType::Float, false);
        old_prop.max = Some(100.0);
        let old = NodeSchema::builder("Sensor").property(old_prop).build();

        let mut new_prop = PropertyDef::simple("temp", ValueType::Float, false);
        new_prop.max = Some(50.0);
        let new = NodeSchema::builder("Sensor").property(new_prop).build();

        let result = check_node_compatibility(&old, &new);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SchemaVersion::new(2, 0, 0));
    }

    #[test]
    fn compat_relaxed_required_is_minor() {
        let old = NodeSchema::builder("Sensor")
            .property(PropertyDef::simple("temp", ValueType::Float, true))
            .build();
        let new = NodeSchema::builder("Sensor")
            .property(PropertyDef::simple("temp", ValueType::Float, false))
            .build();
        let result = check_node_compatibility(&old, &new);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SchemaVersion::new(1, 1, 0));
    }

    #[test]
    fn compat_sufficient_explicit_bump_accepted() {
        let old = NodeSchema::builder("Sensor")
            .property(PropertyDef::simple("temp", ValueType::Float, false))
            .build();
        let new = NodeSchema::builder("Sensor")
            .version(SchemaVersion::new(2, 0, 0))
            .build();
        let result = check_node_compatibility(&old, &new);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SchemaVersion::new(2, 0, 0));
    }

    #[test]
    fn compat_edge_schema_add_property() {
        let old = EdgeSchema::builder("feeds")
            .property(PropertyDef::simple("medium", ValueType::String, false))
            .build();
        let new = EdgeSchema::builder("feeds")
            .property(PropertyDef::simple("medium", ValueType::String, false))
            .property(PropertyDef::simple("rate", ValueType::Float, false))
            .build();
        let result = check_edge_compatibility(&old, &new);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SchemaVersion::new(1, 1, 0));
    }

    #[test]
    fn compat_auto_bump_on_register_replace() {
        let mut v = SchemaValidator::new(ValidationMode::Strict);
        v.register_node_schema(
            NodeSchema::builder("Sensor")
                .property(PropertyDef::simple("temp", ValueType::Float, false))
                .build(),
        )
        .unwrap();

        v.register_node_schema(
            NodeSchema::builder("Sensor")
                .property(PropertyDef::simple("temp", ValueType::Float, false))
                .property(PropertyDef::simple("unit", ValueType::String, false))
                .build(),
        )
        .unwrap();

        let schema = v.node_schema("Sensor").unwrap();
        assert_eq!(schema.version, SchemaVersion::new(1, 1, 0));
    }

    #[test]
    fn compat_version_downgrade_rejected() {
        let old = NodeSchema::builder("Sensor")
            .version(SchemaVersion::new(2, 0, 0))
            .property(PropertyDef::simple("temp", ValueType::Float, false))
            .build();
        let new = NodeSchema::builder("Sensor")
            .version(SchemaVersion::new(1, 0, 0))
            .property(PropertyDef::simple("temp", ValueType::Float, false))
            .build();
        let result = check_node_compatibility(&old, &new);
        assert!(result.is_err());
    }
}
