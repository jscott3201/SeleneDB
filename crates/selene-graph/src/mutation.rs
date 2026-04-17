//! TrackedMutation -- eager writes with automatic rollback on drop.
//!
//! [`TrackedMutation`] applies changes to the graph immediately so that
//! subsequent operations within the same mutation session see the effects.
//! Each mutation is recorded into a forward changelog ([`Change`]) and a
//! reverse undo log (`RollbackEntry`).
//!
//! - On [`commit()`](TrackedMutation::commit): schema validation runs,
//!   changes are appended to the graph's changelog, and the forward changes
//!   are returned to the caller.
//! - On [`drop()`](TrackedMutation::drop) without commit: rollback entries
//!   are replayed in reverse order, restoring the graph to its pre-mutation
//!   state.

use std::collections::HashSet;

use selene_core::changeset::Change;
use selene_core::schema::ValidationMode;
use selene_core::{Edge, EdgeId, IStr, LabelSet, Node, NodeId, PropertyMap, Value};

use crate::error::GraphError;
use crate::graph::SeleneGraph;

use selene_core::now_nanos;

// ── RollbackEntry ────────────────────────────────────────────────────────────

/// An undo-log entry.  Replayed in reverse order to restore the graph.
enum RollbackEntry {
    /// Remove a node that was created during this mutation.
    RemoveNode(NodeId),
    /// Re-insert a node that was deleted during this mutation.
    RestoreNode(Node),
    /// Remove an edge that was created during this mutation.
    RemoveEdge(EdgeId),
    /// Re-insert an edge that was deleted during this mutation.
    RestoreEdge(Edge),
    /// Restore (or remove) a property on a node, including version/timestamp.
    SetNodeProperty {
        id: NodeId,
        key: IStr,
        old: Option<Value>,
        old_version: u64,
        old_updated_at: i64,
    },
    /// Restore (or remove) a property on an edge.
    SetEdgeProperty {
        id: EdgeId,
        key: IStr,
        old: Option<Value>,
    },
    /// Restore (or remove) a label on a node.
    SetLabel {
        id: NodeId,
        label: IStr,
        was_present: bool,
    },
}

// ── TrackedMutation ──────────────────────────────────────────────────────────

/// A mutation session that eagerly applies changes and records undo entries.
///
/// Created via [`SeleneGraph::mutate()`].  All changes are visible
/// immediately within the session.  Call [`commit()`](Self::commit) to
/// finalise, or simply drop the value to roll back.
pub struct TrackedMutation<'g> {
    pub(crate) graph: &'g mut SeleneGraph,
    changes: Vec<Change>,
    rollback: Vec<RollbackEntry>,
    committed: bool,
}

impl<'g> TrackedMutation<'g> {
    /// Create a new mutation session over the given graph.
    pub(crate) fn new(graph: &'g mut SeleneGraph) -> Self {
        Self {
            graph,
            changes: Vec::new(),
            rollback: Vec::new(),
            committed: false,
        }
    }

    /// Read-only access to the current graph state (including pending mutations).
    /// Mutations are applied eagerly, so this reflects all changes made so far.
    pub fn graph(&self) -> &SeleneGraph {
        self.graph
    }

    // ── Node operations ──────────────────────────────────────────────────

    /// Create a node with the given labels and properties.
    ///
    /// The node is inserted into the graph immediately.  Returns the
    /// allocated [`NodeId`].
    pub fn create_node(
        &mut self,
        mut labels: LabelSet,
        props: PropertyMap,
    ) -> Result<NodeId, GraphError> {
        // Resolve label inheritance: add ancestor labels from schema parent chains.
        let mut ancestors = Vec::new();
        for label in labels.iter() {
            for ancestor in self.graph.schema.resolve_label_chain(label.as_str()) {
                if !labels.contains(ancestor) && !ancestors.contains(&ancestor) {
                    ancestors.push(ancestor);
                }
            }
        }
        for ancestor in ancestors {
            labels.insert(ancestor);
        }

        // Apply schema defaults for missing properties (after inheritance
        // so collect_defaults sees the full label set including ancestors).
        let mut props = props;
        let defaults = self.graph.schema.collect_defaults(&labels, &props);
        for (key, value) in defaults {
            props.insert(key, value);
        }

        // Promote dictionary-flagged string properties to InternedStr.
        Self::apply_node_dictionary_encoding(&self.graph.schema, &labels, &mut props);

        let id = self.graph.allocate_node_id()?;

        // Emit change events FIRST (IStr is Copy, so this costs nothing).
        // Then move labels/props into Node::new without cloning.
        self.changes.push(Change::NodeCreated { node_id: id });
        // Emit label and property changes so CDC replicas can reconstruct the node
        for label in labels.iter() {
            self.changes.push(Change::LabelAdded { node_id: id, label });
        }
        for (key, value) in props.iter() {
            self.changes.push(Change::PropertySet {
                node_id: id,
                key: *key,
                value: value.clone(),
                old_value: None,
            });
        }

        let node = Node::new(id, labels, props);
        self.graph.insert_node_raw(node);
        self.rollback.push(RollbackEntry::RemoveNode(id));

        Ok(id)
    }

    /// Delete a node and **cascade** all incident edges.
    ///
    /// Returns [`GraphError::NodeNotFound`] if the node does not exist.
    pub fn delete_node(&mut self, id: NodeId) -> Result<(), GraphError> {
        if !self.graph.contains_node(id) {
            return Err(GraphError::NodeNotFound(id));
        }

        // 1. Collect all incident edge IDs (clone to avoid borrow conflict).
        let outgoing: Vec<EdgeId> = self.graph.outgoing(id).to_vec();
        let incoming: Vec<EdgeId> = self.graph.incoming(id).to_vec();

        // 2. Remove each incident edge, recording rollback + change.
        let mut seen = HashSet::new();
        for eid in outgoing.into_iter().chain(incoming) {
            if !seen.insert(eid) {
                continue; // self-loop already handled
            }
            if let Some(edge) = self.graph.remove_edge_raw(eid) {
                self.changes.push(Change::EdgeDeleted {
                    edge_id: eid,
                    source: edge.source,
                    target: edge.target,
                    label: edge.label,
                });
                self.rollback.push(RollbackEntry::RestoreEdge(edge));
            }
        }

        // 3. Remove the node itself.
        let node = self
            .graph
            .remove_node_raw(id)
            .expect("node existence already checked");

        // Capture labels before moving node into rollback (for trigger evaluation + changelog).
        // IStr is Copy, so this is zero-allocation.
        let labels: Vec<IStr> = node.labels.iter().collect();

        self.changes.push(Change::NodeDeleted {
            node_id: id,
            labels,
        });
        self.rollback.push(RollbackEntry::RestoreNode(node));

        Ok(())
    }

    // ── Node property operations ─────────────────────────────────────────

    /// Set (create or overwrite) a property on a node.
    ///
    /// Also bumps the node's `version` and `updated_at` fields.
    pub fn set_property(&mut self, id: NodeId, key: IStr, value: Value) -> Result<(), GraphError> {
        let store = self.graph.node_store_mut();
        let props = store
            .properties_mut(id)
            .ok_or(GraphError::NodeNotFound(id))?;
        let old = props.insert(key, value.clone());

        let (old_version, old_updated_at) = store
            .bump_version(id, now_nanos())
            .expect("node confirmed to exist above");
        store.invalidate_json_cache(id);

        // Update property indexes if any (label, key) pair is indexed
        let node_labels: Vec<IStr> = self
            .graph
            .get_node(id)
            .map(|n| n.labels.iter().collect())
            .unwrap_or_default();
        for &label in &node_labels {
            let index_key = (label, key);
            if let Some(idx) = self.graph.property_index.get_mut(&index_key) {
                let idx = std::sync::Arc::make_mut(idx);
                // Remove old value entry
                if let Some(old_val) = &old {
                    idx.remove(old_val, id);
                }
                // Add new value entry
                idx.insert(&value, id);
            }
        }

        // Update composite indexes if any contain this property key.
        // Collect all needed property values up front (owned) to avoid
        // holding an immutable borrow on self.graph while mutating indexes.
        if let Some(node) = self.graph.get_node(id) {
            let labels: Vec<IStr> = node.labels.iter().collect();
            #[allow(clippy::type_complexity)]
            let mut composite_ops: Vec<(
                IStr,
                Vec<IStr>,
                Option<Vec<Value>>,
                Option<Vec<Value>>,
            )> = Vec::new();
            for &label in &labels {
                let matching_keys: Vec<Vec<IStr>> = self
                    .graph
                    .composite_indexes
                    .keys()
                    .filter(|(l, props)| *l == label && props.contains(&key))
                    .map(|(_, props)| props.clone())
                    .collect();
                for props in matching_keys {
                    // Build owned old-value vector
                    let old_vals = old.as_ref().and_then(|old_val| {
                        let vals: Vec<Value> = props
                            .iter()
                            .filter_map(|k| {
                                if *k == key {
                                    Some(old_val.clone())
                                } else {
                                    node.properties.get(*k).cloned()
                                }
                            })
                            .collect();
                        if vals.len() == props.len() {
                            Some(vals)
                        } else {
                            None
                        }
                    });
                    // Build owned new-value vector
                    let new_vals = {
                        let vals: Vec<Value> = props
                            .iter()
                            .filter_map(|k| {
                                if *k == key {
                                    Some(value.clone())
                                } else {
                                    node.properties.get(*k).cloned()
                                }
                            })
                            .collect();
                        if vals.len() == props.len() {
                            Some(vals)
                        } else {
                            None
                        }
                    };
                    composite_ops.push((label, props, old_vals, new_vals));
                }
            }
            // Now apply mutations without holding the node borrow
            for (label, props, old_vals, new_vals) in &composite_ops {
                let idx_key = (*label, props.clone());
                if let Some(old_vals) = old_vals {
                    let refs: Vec<&Value> = old_vals.iter().collect();
                    if let Some(cidx) = self.graph.composite_indexes.get_mut(&idx_key) {
                        std::sync::Arc::make_mut(cidx).remove(&refs, id);
                    }
                }
                if let Some(new_vals) = new_vals {
                    let refs: Vec<&Value> = new_vals.iter().collect();
                    if let Some(cidx) = self.graph.composite_indexes.get_mut(&idx_key) {
                        std::sync::Arc::make_mut(cidx).insert(&refs, id);
                    }
                }
            }
        }

        self.changes.push(Change::PropertySet {
            node_id: id,
            key,
            value,
            old_value: old.clone(),
        });
        self.rollback.push(RollbackEntry::SetNodeProperty {
            id,
            key,
            old,
            old_version,
            old_updated_at,
        });

        Ok(())
    }

    /// Remove a property from a node.
    ///
    /// Returns the old value if the property existed, or `None` if it was
    /// already absent (not an error).  Bumps `version` and `updated_at`
    /// when a value is actually removed.
    pub fn remove_property(&mut self, id: NodeId, key: &str) -> Result<Option<Value>, GraphError> {
        let ikey = IStr::new(key);
        let store = self.graph.node_store_mut();
        let props = store
            .properties_mut(id)
            .ok_or(GraphError::NodeNotFound(id))?;
        let old = props.remove(ikey);

        if old.is_some() {
            let (old_version, old_updated_at) = store
                .bump_version(id, now_nanos())
                .expect("node confirmed to exist above");
            store.invalidate_json_cache(id);

            // Remove old value from property indexes
            if let Some(old_val) = &old {
                let node_labels: Vec<IStr> = self
                    .graph
                    .get_node(id)
                    .map(|n| n.labels.iter().collect())
                    .unwrap_or_default();
                for &label in &node_labels {
                    let index_key = (label, ikey);
                    if let Some(idx) = self.graph.property_index.get_mut(&index_key) {
                        std::sync::Arc::make_mut(idx).remove(old_val, id);
                    }
                }

                // Remove from composite indexes if any contain this key.
                // Collect owned values up front to avoid borrow conflict.
                let mut composite_removals: Vec<(IStr, Vec<IStr>, Vec<Value>)> = Vec::new();
                if let Some(node) = self.graph.get_node(id) {
                    for &label in &node_labels {
                        let matching_keys: Vec<Vec<IStr>> = self
                            .graph
                            .composite_indexes
                            .keys()
                            .filter(|(l, props)| *l == label && props.contains(&ikey))
                            .map(|(_, props)| props.clone())
                            .collect();
                        for props in matching_keys {
                            let vals: Vec<Value> = props
                                .iter()
                                .filter_map(|k| {
                                    if *k == ikey {
                                        Some(old_val.clone())
                                    } else {
                                        node.properties.get(*k).cloned()
                                    }
                                })
                                .collect();
                            if vals.len() == props.len() {
                                composite_removals.push((label, props, vals));
                            }
                        }
                    }
                }
                for (label, props, vals) in &composite_removals {
                    let idx_key = (*label, props.clone());
                    let refs: Vec<&Value> = vals.iter().collect();
                    if let Some(cidx) = self.graph.composite_indexes.get_mut(&idx_key) {
                        std::sync::Arc::make_mut(cidx).remove(&refs, id);
                    }
                }
            }

            self.changes.push(Change::PropertyRemoved {
                node_id: id,
                key: ikey,
                old_value: old.clone(),
            });
            self.rollback.push(RollbackEntry::SetNodeProperty {
                id,
                key: ikey,
                old: old.clone(),
                old_version,
                old_updated_at,
            });
        }

        Ok(old)
    }

    // ── Label operations ─────────────────────────────────────────────────

    /// Add a label to a node.
    ///
    /// Resolves ancestor labels from schema inheritance and applies schema
    /// defaults for properties not already present on the node. No-op (but
    /// not an error) if the label is already present.
    pub fn add_label(&mut self, id: NodeId, label: IStr) -> Result<(), GraphError> {
        if !self.graph.contains_node(id) {
            return Err(GraphError::NodeNotFound(id));
        }

        // Resolve label + ancestors from schema inheritance.
        let mut to_add = Vec::new();
        for ancestor in self.graph.schema.resolve_label_chain(label.as_str()) {
            let already = self
                .graph
                .get_node(id)
                .is_some_and(|n| n.labels.contains(ancestor));
            if !already {
                to_add.push(ancestor);
            }
        }

        if to_add.is_empty() {
            return Ok(());
        }

        for lbl in &to_add {
            self.graph.add_label_raw(id, *lbl);
            self.changes.push(Change::LabelAdded {
                node_id: id,
                label: *lbl,
            });
            self.rollback.push(RollbackEntry::SetLabel {
                id,
                label: *lbl,
                was_present: false,
            });
        }

        // Apply schema defaults for properties not already present.
        // Must read labels AFTER adding them so collect_defaults sees the
        // full label set including newly-added ancestors.
        if let Some(node_ref) = self.graph.get_node(id) {
            let labels = node_ref.labels.clone();
            let props = node_ref.properties.clone();
            let defaults = self.graph.schema.collect_defaults(&labels, &props);
            for (key, value) in defaults {
                // Only set if the property is truly missing -- don't overwrite.
                if props.get(key).is_none() {
                    // Use set_property path for proper change tracking.
                    self.set_property(id, key, value)?;
                }
            }
        }

        Ok(())
    }

    /// Remove a label from a node.
    ///
    /// In Strict mode, rejects removal of labels that are inherited from
    /// another label still present on the node (e.g., cannot remove `:sensor`
    /// while `:temperature_sensor` is present if sensor is its ancestor).
    /// No-op (but not an error) if the label is not present.
    pub fn remove_label(&mut self, id: NodeId, label: &str) -> Result<(), GraphError> {
        if !self.graph.contains_node(id) {
            return Err(GraphError::NodeNotFound(id));
        }

        let ilabel = IStr::new(label);
        let was_present = self
            .graph
            .get_node(id)
            .is_some_and(|n| n.labels.contains(ilabel));

        if !was_present {
            return Ok(());
        }

        // Check if this label is an ancestor inherited from another label
        // still on the node. If so, removing it would break the inheritance
        // invariant (MATCH (p:point) must return all point subtypes).
        if let Some(node_ref) = self.graph.get_node(id) {
            for other_label in node_ref.labels.iter() {
                if other_label == ilabel {
                    continue;
                }
                // Check if `label` appears in other_label's parent chain.
                let chain = self.graph.schema.resolve_label_chain(other_label.as_str());
                if chain.contains(&ilabel) {
                    // Strict wins if either the label being removed or the
                    // label depending on it declares Strict.
                    // Label-removal inheritance check: both sides are node
                    // labels (edge labels can't inherit), so resolve both
                    // against the node schema registry explicitly.
                    let mode_removed = self.graph.schema.effective_mode_for_node_label(label);
                    let mode_owner = self
                        .graph
                        .schema
                        .effective_mode_for_node_label(other_label.as_str());
                    let strict = matches!(mode_removed, ValidationMode::Strict)
                        || matches!(mode_owner, ValidationMode::Strict);
                    if strict {
                        return Err(GraphError::SchemaViolation(format!(
                            "cannot remove inherited label '{label}' -- required by label '{other_label}'"
                        )));
                    }
                    tracing::warn!(
                        label,
                        required_by = other_label.as_str(),
                        "removing inherited label"
                    );
                }
            }
        }

        self.graph.remove_label_raw(id, ilabel);

        self.changes.push(Change::LabelRemoved {
            node_id: id,
            label: ilabel,
        });
        self.rollback.push(RollbackEntry::SetLabel {
            id,
            label: ilabel,
            was_present,
        });

        Ok(())
    }

    // ── Edge operations ──────────────────────────────────────────────────

    /// Create an edge between two existing nodes.
    ///
    /// Returns [`GraphError::NodeNotFound`] if either the source or target
    /// node does not exist.
    pub fn create_edge(
        &mut self,
        source: NodeId,
        label: IStr,
        target: NodeId,
        props: PropertyMap,
    ) -> Result<EdgeId, GraphError> {
        if !self.graph.contains_node(source) {
            return Err(GraphError::NodeNotFound(source));
        }
        if !self.graph.contains_node(target) {
            return Err(GraphError::NodeNotFound(target));
        }

        // Validate edge endpoint labels (eager, immediate feedback).
        let source_labels = self
            .graph
            .get_node(source)
            .map(|n| n.labels.clone())
            .unwrap_or_default();
        let target_labels = self
            .graph
            .get_node(target)
            .map(|n| n.labels.clone())
            .unwrap_or_default();
        let issues = self.graph.schema.validate_edge_endpoints(
            label.as_str(),
            &source_labels,
            &target_labels,
        );
        if !issues.is_empty() {
            let (strict, warn) = self.graph.schema.partition_issues_by_mode(issues);
            for issue in &warn {
                tracing::warn!(issue = %issue, "edge endpoint warning");
            }
            if !strict.is_empty() {
                let msg = strict
                    .iter()
                    .map(|i| i.message.as_str())
                    .collect::<Vec<_>>()
                    .join("; ");
                return Err(GraphError::SchemaViolation(msg));
            }
        }

        // Cardinality check: count existing edges of this type.
        let current_out = self
            .graph
            .outgoing(source)
            .iter()
            .filter(|&&eid| self.graph.get_edge(eid).is_some_and(|e| e.label == label))
            .count();
        let current_in = self
            .graph
            .incoming(target)
            .iter()
            .filter(|&&eid| self.graph.get_edge(eid).is_some_and(|e| e.label == label))
            .count();
        let card_issues = self.graph.schema.check_edge_cardinality(
            label.as_str(),
            current_out + 1,
            current_in + 1,
        );
        if !card_issues.is_empty() {
            let (strict, warn) = self.graph.schema.partition_issues_by_mode(card_issues);
            for issue in &warn {
                tracing::warn!(issue = %issue, "edge cardinality warning");
            }
            if !strict.is_empty() {
                let msg = strict
                    .iter()
                    .map(|i| i.message.as_str())
                    .collect::<Vec<_>>()
                    .join("; ");
                return Err(GraphError::SchemaViolation(msg));
            }
        }

        // Promote dictionary-flagged string properties to InternedStr.
        let mut props = props;
        Self::apply_edge_dictionary_encoding(&self.graph.schema, label, &mut props);

        let id = self.graph.allocate_edge_id()?;
        let edge = Edge::new(id, source, target, label, props.clone());
        self.graph.insert_edge_raw(edge);

        self.changes.push(Change::EdgeCreated {
            edge_id: id,
            source,
            target,
            label,
        });
        // Emit property changes so CDC replicas can reconstruct edge properties
        for (key, value) in props.iter() {
            self.changes.push(Change::EdgePropertySet {
                edge_id: id,
                source,
                target,
                key: *key,
                value: value.clone(),
                old_value: None,
            });
        }
        self.rollback.push(RollbackEntry::RemoveEdge(id));

        Ok(id)
    }

    /// Delete an edge.
    ///
    /// Returns [`GraphError::EdgeNotFound`] if the edge does not exist.
    pub fn delete_edge(&mut self, id: EdgeId) -> Result<(), GraphError> {
        let edge = self
            .graph
            .remove_edge_raw(id)
            .ok_or(GraphError::EdgeNotFound(id))?;

        self.changes.push(Change::EdgeDeleted {
            edge_id: id,
            source: edge.source,
            target: edge.target,
            label: edge.label,
        });
        self.rollback.push(RollbackEntry::RestoreEdge(edge));

        Ok(())
    }

    /// Set (create or overwrite) a property on an edge.
    pub fn set_edge_property(
        &mut self,
        id: EdgeId,
        key: IStr,
        value: Value,
    ) -> Result<(), GraphError> {
        // Get source/target before mutating (for the change record)
        let (source, target) = {
            let edge = self
                .graph
                .get_edge(id)
                .ok_or(GraphError::EdgeNotFound(id))?;
            (edge.source, edge.target)
        };

        let store = self.graph.edge_store_mut();
        let props = store
            .properties_mut(id)
            .ok_or(GraphError::EdgeNotFound(id))?;
        let old = props.insert(key, value.clone());

        self.changes.push(Change::EdgePropertySet {
            edge_id: id,
            source,
            target,
            key,
            value,
            old_value: old.clone(),
        });
        self.rollback
            .push(RollbackEntry::SetEdgeProperty { id, key, old });

        Ok(())
    }

    /// Remove a property from an edge.
    pub fn remove_edge_property(
        &mut self,
        id: EdgeId,
        key: &str,
    ) -> Result<Option<Value>, GraphError> {
        let ikey = IStr::new(key);

        // Read source/target before mutating (for the change record)
        let (source, target) = {
            let edge = self
                .graph
                .get_edge(id)
                .ok_or(GraphError::EdgeNotFound(id))?;
            (edge.source, edge.target)
        };

        let store = self.graph.edge_store_mut();
        let props = store
            .properties_mut(id)
            .ok_or(GraphError::EdgeNotFound(id))?;
        let old = props.remove(ikey);

        if old.is_some() {
            self.changes.push(Change::EdgePropertyRemoved {
                edge_id: id,
                source,
                target,
                key: ikey,
                old_value: old.clone(),
            });
            self.rollback.push(RollbackEntry::SetEdgeProperty {
                id,
                key: ikey,
                old: old.clone(),
            });
        }
        Ok(old)
    }

    // ── Commit / rollback ────────────────────────────────────────────────

    /// Commit the mutation: validate schemas, append to changelog, return
    /// the forward change list.
    ///
    /// If the schema mode is [`ValidationMode::Strict`] and any issues are
    /// found, an error is returned and the [`Drop`] impl will handle
    /// rollback automatically.
    pub fn commit(mut self, hlc_timestamp: u64) -> Result<Vec<Change>, GraphError> {
        // Collect IDs of nodes/edges that were created or modified.
        let mut node_ids: HashSet<NodeId> = HashSet::new();
        let mut edge_ids: HashSet<EdgeId> = HashSet::new();
        let mut created_node_ids: HashSet<NodeId> = HashSet::new();

        for change in &self.changes {
            match change {
                Change::NodeCreated { node_id } => {
                    node_ids.insert(*node_id);
                    created_node_ids.insert(*node_id);
                }
                Change::PropertySet { node_id, .. }
                | Change::PropertyRemoved { node_id, .. }
                | Change::LabelAdded { node_id, .. }
                | Change::LabelRemoved { node_id, .. } => {
                    node_ids.insert(*node_id);
                }
                Change::EdgeCreated { edge_id, .. }
                | Change::EdgePropertySet { edge_id, .. }
                | Change::EdgePropertyRemoved { edge_id, .. } => {
                    edge_ids.insert(*edge_id);
                }
                // Deleted nodes/edges don't need validation.
                Change::NodeDeleted { .. } | Change::EdgeDeleted { .. } => {}
            }
        }

        // Remove IDs of nodes/edges that were subsequently deleted -- no
        // point validating something that no longer exists.
        for change in &self.changes {
            match change {
                Change::NodeDeleted { node_id, .. } => {
                    node_ids.remove(node_id);
                    created_node_ids.remove(node_id);
                }
                Change::EdgeDeleted { edge_id, .. } => {
                    edge_ids.remove(edge_id);
                }
                _ => {}
            }
        }

        // Validate surviving nodes (construct owned Node for validator).
        let mut all_issues = Vec::new();
        for nid in &node_ids {
            if let Some(node_ref) = self.graph.get_node(*nid) {
                let owned = node_ref.to_owned_node();
                let issues = self.graph.schema.validate_node(&owned);
                all_issues.extend(issues);
            }
        }

        // Validate surviving edges (construct owned Edge for validator).
        for eid in &edge_ids {
            if let Some(edge_ref) = self.graph.get_edge(*eid) {
                let owned = edge_ref.to_owned_edge();
                let issues = self.graph.schema.validate_edge(&owned);
                all_issues.extend(issues);
            }
        }

        // Validate minimum edge degree constraints for newly created nodes.
        // At commit time all edges in the transaction are visible.
        for nid in &created_node_ids {
            if let Some(node_ref) = self.graph.get_node(*nid) {
                let labels: Vec<selene_core::IStr> = node_ref.labels.iter().collect();
                // Count outgoing edges by label
                let mut out_by_label = std::collections::HashMap::new();
                for eid in self.graph.outgoing(*nid) {
                    if let Some(edge) = self.graph.get_edge(*eid) {
                        *out_by_label.entry(edge.label).or_insert(0usize) += 1;
                    }
                }
                // Count incoming edges by label
                let mut in_by_label = std::collections::HashMap::new();
                for eid in self.graph.incoming(*nid) {
                    if let Some(edge) = self.graph.get_edge(*eid) {
                        *in_by_label.entry(edge.label).or_insert(0usize) += 1;
                    }
                }
                let issues =
                    self.graph
                        .schema
                        .check_min_edge_degrees(&labels, &out_by_label, &in_by_label);
                all_issues.extend(issues);
            }
        }

        validate_immutability(self.graph, &self.changes, &mut all_issues);
        validate_unique_constraints(self.graph, &node_ids, &mut all_issues);
        validate_composite_keys(self.graph, &node_ids, &mut all_issues);

        if !all_issues.is_empty() {
            let (strict, warn) = self.graph.schema.partition_issues_by_mode(all_issues);
            for issue in &warn {
                tracing::warn!(issue = %issue, "schema validation warning");
            }
            if !strict.is_empty() {
                let msg = strict
                    .iter()
                    .map(|i| i.message.as_str())
                    .collect::<Vec<_>>()
                    .join("; ");
                // Do NOT call rollback explicitly -- Drop will handle it.
                return Err(GraphError::SchemaViolation(msg));
            }
        }

        // Move changes into changelog, then clone from the stored entry
        // (avoids cloning the full Vec before the append).
        let changes = std::mem::take(&mut self.changes);
        self.graph
            .changelog
            .append(changes, selene_core::entity::now_nanos(), hlc_timestamp);
        let returned = self
            .graph
            .changelog
            .last_changes()
            .cloned()
            .unwrap_or_default();

        // Bump generation for cache invalidation.
        self.graph.bump_generation();

        self.committed = true;
        self.rollback.clear();

        Ok(returned)
    }

    /// Replay rollback entries in reverse order to undo all mutations.
    fn rollback(&mut self) {
        for entry in self.rollback.drain(..).rev() {
            match entry {
                RollbackEntry::RemoveNode(id) => {
                    self.graph.remove_node_raw(id);
                }
                RollbackEntry::RestoreNode(node) => {
                    self.graph.insert_node_raw(node);
                }
                RollbackEntry::RemoveEdge(id) => {
                    self.graph.remove_edge_raw(id);
                }
                RollbackEntry::RestoreEdge(edge) => {
                    self.graph.insert_edge_raw(edge);
                }
                RollbackEntry::SetNodeProperty {
                    id,
                    key,
                    old,
                    old_version,
                    old_updated_at,
                } => {
                    // Read the current (forward-path) value before restoring,
                    // so we can reverse the property index updates.
                    let current_value = self
                        .graph
                        .node_store()
                        .properties(id)
                        .and_then(|props| props.get(key))
                        .cloned();

                    let node_labels: Vec<IStr> = self
                        .graph
                        .get_node(id)
                        .map(|n| n.labels.iter().collect())
                        .unwrap_or_default();

                    // Reverse property index: remove current value, restore old value
                    for &label in &node_labels {
                        let index_key = (label, key);
                        if let Some(idx) = self.graph.property_index.get_mut(&index_key) {
                            let idx = std::sync::Arc::make_mut(idx);
                            if let Some(cur) = &current_value {
                                idx.remove(cur, id);
                            }
                            if let Some(old_val) = &old {
                                idx.insert(old_val, id);
                            }
                        }
                    }

                    // Reverse composite indexes: remove forward entry, restore old entry.
                    // Must run before the property is restored so other property values
                    // read from the node reflect the current (forward) state.
                    if let Some(node) = self.graph.get_node(id) {
                        #[allow(clippy::type_complexity)]
                        let mut composite_ops: Vec<(
                            IStr,
                            Vec<IStr>,
                            Option<Vec<Value>>,
                            Option<Vec<Value>>,
                        )> = Vec::new();
                        for &label in &node_labels {
                            let matching_keys: Vec<Vec<IStr>> = self
                                .graph
                                .composite_indexes
                                .keys()
                                .filter(|(l, props)| *l == label && props.contains(&key))
                                .map(|(_, props)| props.clone())
                                .collect();
                            for props in matching_keys {
                                // Forward composite key (to remove)
                                let fwd_vals = current_value.as_ref().and_then(|cur_val| {
                                    let vals: Vec<Value> = props
                                        .iter()
                                        .filter_map(|k| {
                                            if *k == key {
                                                Some(cur_val.clone())
                                            } else {
                                                node.properties.get(*k).cloned()
                                            }
                                        })
                                        .collect();
                                    if vals.len() == props.len() {
                                        Some(vals)
                                    } else {
                                        None
                                    }
                                });
                                // Old composite key (to restore)
                                let old_vals = old.as_ref().and_then(|old_val| {
                                    let vals: Vec<Value> = props
                                        .iter()
                                        .filter_map(|k| {
                                            if *k == key {
                                                Some(old_val.clone())
                                            } else {
                                                node.properties.get(*k).cloned()
                                            }
                                        })
                                        .collect();
                                    if vals.len() == props.len() {
                                        Some(vals)
                                    } else {
                                        None
                                    }
                                });
                                composite_ops.push((label, props, fwd_vals, old_vals));
                            }
                        }
                        // Apply without holding the node borrow
                        for (label, props, fwd_vals, old_vals) in &composite_ops {
                            let idx_key = (*label, props.clone());
                            if let Some(fwd) = fwd_vals {
                                let refs: Vec<&Value> = fwd.iter().collect();
                                if let Some(cidx) = self.graph.composite_indexes.get_mut(&idx_key) {
                                    std::sync::Arc::make_mut(cidx).remove(&refs, id);
                                }
                            }
                            if let Some(old_v) = old_vals {
                                let refs: Vec<&Value> = old_v.iter().collect();
                                if let Some(cidx) = self.graph.composite_indexes.get_mut(&idx_key) {
                                    std::sync::Arc::make_mut(cidx).insert(&refs, id);
                                }
                            }
                        }
                    }

                    let store = self.graph.node_store_mut();
                    if let Some(props) = store.properties_mut(id) {
                        match old {
                            Some(v) => {
                                props.insert(key, v);
                            }
                            None => {
                                props.remove(key);
                            }
                        }
                        store.set_version(id, old_version, old_updated_at);
                        store.invalidate_json_cache(id);
                    }
                }
                RollbackEntry::SetEdgeProperty { id, key, old } => {
                    if let Some(props) = self.graph.edge_store_mut().properties_mut(id) {
                        match old {
                            Some(v) => {
                                props.insert(key, v);
                            }
                            None => {
                                props.remove(key);
                            }
                        }
                    }
                }
                RollbackEntry::SetLabel {
                    id,
                    label,
                    was_present,
                } => {
                    if was_present {
                        self.graph.add_label_raw(id, label);
                    } else {
                        self.graph.remove_label_raw(id, label);
                    }
                }
            }
        }
    }

    // ── Dictionary encoding helpers ─────────────────────────────────────

    /// Promote `Value::String` to `Value::InternedStr` for properties whose
    /// schema has `dictionary: true`. Operates in-place on the `PropertyMap`.
    fn apply_node_dictionary_encoding(
        schema: &crate::schema::SchemaValidator,
        labels: &LabelSet,
        props: &mut PropertyMap,
    ) {
        let promotions: Vec<(IStr, Value)> = props
            .iter()
            .filter_map(|(key, value)| {
                if let Value::String(s) = value {
                    for label in labels.iter() {
                        if let Some(ns) = schema.node_schema(label.as_str())
                            && let Some(pd) =
                                ns.properties.iter().find(|p| *p.name == *key.as_str())
                            && pd.dictionary
                        {
                            return Some((*key, Value::InternedStr(IStr::new(s.as_str()))));
                        }
                    }
                }
                None
            })
            .collect();
        for (key, value) in promotions {
            props.insert(key, value);
        }
    }

    /// Promote dictionary-flagged string properties on an edge.
    fn apply_edge_dictionary_encoding(
        schema: &crate::schema::SchemaValidator,
        label: IStr,
        props: &mut PropertyMap,
    ) {
        let promotions: Vec<(IStr, Value)> = props
            .iter()
            .filter_map(|(key, value)| {
                if let Value::String(s) = value
                    && let Some(es) = schema.edge_schema(label.as_str())
                    && let Some(pd) = es.properties.iter().find(|p| *p.name == *key.as_str())
                    && pd.dictionary
                {
                    return Some((*key, Value::InternedStr(IStr::new(s.as_str()))));
                }
                None
            })
            .collect();
        for (key, value) in promotions {
            props.insert(key, value);
        }
    }
}

impl Drop for TrackedMutation<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.rollback();
        }
    }
}

// ── Commit validation helpers ────────────────────────────────────────────────

/// Reject PropertySet and PropertyRemoved on pre-existing nodes for properties
/// marked `immutable: true`. Newly created nodes are exempt (defaults CAN set
/// immutable properties on creation).
fn validate_immutability(
    graph: &SeleneGraph,
    changes: &[Change],
    issues: &mut Vec<crate::schema::ValidationIssue>,
) {
    let created_in_tx: HashSet<NodeId> = changes
        .iter()
        .filter_map(|c| {
            if let Change::NodeCreated { node_id } = c {
                Some(*node_id)
            } else {
                None
            }
        })
        .collect();

    for change in changes {
        let (node_id, key) = match change {
            Change::PropertySet { node_id, key, .. } => (node_id, key),
            Change::PropertyRemoved { node_id, key, .. } => (node_id, key),
            _ => continue,
        };
        if !created_in_tx.contains(node_id)
            && let Some(node_ref) = graph.get_node(*node_id)
        {
            for label in node_ref.labels.iter() {
                if graph.schema.is_immutable(label.as_str(), key.as_str()) {
                    issues.push(
                        crate::schema::ValidationIssue::new(format!(
                            "property '{key}' is immutable on label '{label}'"
                        ))
                        .with_node_label(label.as_str()),
                    );
                }
            }
        }
    }
}

/// Check unique property constraints. Skip Null values (SQL semantics:
/// multiple NULLs allowed). Deduplicate symmetric violations (only report
/// when other_id > nid). Use TypedIndex O(1) lookup when available,
/// falling back to label bitmap scan.
fn validate_unique_constraints(
    graph: &SeleneGraph,
    node_ids: &HashSet<NodeId>,
    issues: &mut Vec<crate::schema::ValidationIssue>,
) {
    for nid in node_ids {
        if let Some(node_ref) = graph.get_node(*nid) {
            for label in node_ref.labels.iter() {
                if let Some(schema) = graph.schema.node_schema(label.as_str()) {
                    for prop_def in &schema.properties {
                        if !prop_def.unique {
                            continue;
                        }
                        let prop_key = IStr::new(prop_def.name.as_ref());
                        let Some(val) = node_ref.properties.get_by_str(prop_def.name.as_ref())
                        else {
                            continue;
                        };
                        if val.is_null() {
                            continue;
                        }

                        // Fast path: use TypedIndex for O(1) lookup when available.
                        if let Some(index_hits) = graph.property_index_lookup(label, prop_key, val)
                        {
                            for &other_id in index_hits {
                                if other_id == *nid {
                                    continue;
                                }
                                if node_ids.contains(&other_id) && other_id.0 < nid.0 {
                                    continue;
                                }
                                issues.push(
                                    crate::schema::ValidationIssue::new(format!(
                                        "unique violation: '{}'='{}' already exists on node {}",
                                        prop_def.name, val, other_id.0
                                    ))
                                    .with_node_label(label.as_str()),
                                );
                            }
                            continue;
                        }

                        // Fallback: scan label bitmap for unindexed properties.
                        if let Some(bitmap) = graph.label_bitmap(label.as_str()) {
                            for other_raw in bitmap {
                                let other_id = NodeId(u64::from(other_raw));
                                if other_id == *nid {
                                    continue;
                                }
                                if node_ids.contains(&other_id) && other_id.0 < nid.0 {
                                    continue;
                                }
                                if let Some(other_ref) = graph.get_node(other_id)
                                    && let Some(other_val) =
                                        other_ref.properties.get_by_str(prop_def.name.as_ref())
                                    && val == other_val
                                {
                                    issues.push(
                                        crate::schema::ValidationIssue::new(format!(
                                            "unique violation: '{}'='{}' already exists on node {}",
                                            prop_def.name, val, other_id.0
                                        ))
                                        .with_node_label(label.as_str()),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Check composite key constraints. For each schema with non-empty
/// key_properties, compute a composite key tuple and check for duplicates
/// across all nodes with that label. NULL values in any key property make
/// the tuple non-comparable (SQL semantics).
///
/// Uses the composite index for O(1) lookups when available. Falls back
/// to bitmap scan when no composite index exists for the label+keys pair.
fn validate_composite_keys(
    graph: &SeleneGraph,
    node_ids: &HashSet<NodeId>,
    issues: &mut Vec<crate::schema::ValidationIssue>,
) {
    for nid in node_ids {
        if let Some(node_ref) = graph.get_node(*nid) {
            for label in node_ref.labels.iter() {
                if let Some(schema) = graph.schema.node_schema(label.as_str()) {
                    if schema.key_properties.is_empty() {
                        continue;
                    }
                    // Build composite key for this node
                    let key_values: Vec<Option<&Value>> = schema
                        .key_properties
                        .iter()
                        .map(|kp| node_ref.properties.get_by_str(kp.as_ref()))
                        .collect();

                    // Skip if any key property is NULL (SQL: NULL != NULL)
                    if key_values.iter().any(|v| v.is_none_or(|v| v.is_null())) {
                        continue;
                    }

                    // Unwrap the Option layer (we know all are Some from the check above)
                    let values: Vec<&Value> = key_values.iter().map(|v| v.unwrap()).collect();

                    // Build IStr keys for composite index lookup
                    let key_props: Vec<IStr> = schema
                        .key_properties
                        .iter()
                        .map(|kp| IStr::new(kp.as_ref()))
                        .collect();

                    // Fast path: use composite index for O(1) duplicate check
                    if let Some(ids) = graph.composite_index_lookup(label, &key_props, &values) {
                        if let Some(&other_id) = ids.iter().find(|&&oid| oid != *nid) {
                            // Skip if the other node is also in node_ids and has a lower
                            // ID (it will report the violation from its own iteration)
                            if !(node_ids.contains(&other_id) && other_id.0 < nid.0) {
                                let key_desc = format_key_desc(&schema.key_properties, &key_values);
                                issues.push(
                                    crate::schema::ValidationIssue::new(format!(
                                        "composite key violation on :{}: ({key_desc}) already exists on node {}",
                                        label, other_id.0
                                    ))
                                    .with_node_label(label.as_str()),
                                );
                            }
                        }
                        continue;
                    }

                    // Slow path: scan label bitmap (no composite index for this label+keys)
                    if let Some(bitmap) = graph.label_bitmap(label.as_str()) {
                        for other_raw in bitmap {
                            let other_id = NodeId(u64::from(other_raw));
                            if other_id == *nid {
                                continue;
                            }
                            if node_ids.contains(&other_id) && other_id.0 < nid.0 {
                                continue;
                            }
                            if let Some(other_ref) = graph.get_node(other_id) {
                                let other_key: Vec<Option<&Value>> = schema
                                    .key_properties
                                    .iter()
                                    .map(|kp| other_ref.properties.get_by_str(kp.as_ref()))
                                    .collect();
                                if key_values == other_key {
                                    let key_desc =
                                        format_key_desc(&schema.key_properties, &key_values);
                                    issues.push(
                                        crate::schema::ValidationIssue::new(format!(
                                            "composite key violation on :{}: ({key_desc}) already exists on node {}",
                                            label, other_id.0
                                        ))
                                        .with_node_label(label.as_str()),
                                    );
                                    break; // One violation per node is enough
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Format a human-readable composite key description for error messages.
fn format_key_desc(
    key_properties: &[std::sync::Arc<str>],
    key_values: &[Option<&Value>],
) -> String {
    key_properties
        .iter()
        .zip(key_values)
        .map(|(k, v)| format!("{}={}", k, v.map(|x| x.to_string()).unwrap_or_default()))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
#[path = "mutation_tests.rs"]
mod tests;
