//! Full-text search index — tantivy-backed inverted indexes for searchable properties.
//!
//! Each (label, property) pair marked `searchable: true` in the schema gets its own
//! tantivy index. Updates arrive via the changelog subscriber (background task).
//! Writers are persistent (held in Mutex), commits batched on a timer.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use selene_core::{IStr, NodeId};
use selene_graph::SeleneGraph;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, STORED, Schema, TEXT};
use tantivy::{Index, IndexWriter, TantivyDocument, Term, doc};

/// Per-(label, property) search index entry with persistent writer and reader.
struct SearchEntry {
    index: Index,
    writer: Mutex<IndexWriter>,
    reader: tantivy::IndexReader,
    node_id_field: Field,
    text_field: Field,
}

/// Full-text search index manager.
///
/// Holds one tantivy index per searchable (label, property) pair.
/// Writers are persistent and reused. Commits are batched via `commit_all()`.
/// New indexes can be added at runtime via `add_searchable()` when schemas
/// are registered via DDL.
pub struct SearchIndex {
    index_dir: PathBuf,
    entries: parking_lot::RwLock<HashMap<(IStr, IStr), SearchEntry>>,
}

impl SearchIndex {
    /// Create or open indexes for all searchable schema properties.
    pub fn open_or_create(
        index_dir: &Path,
        schema: &selene_graph::SchemaValidator,
    ) -> Result<Self, anyhow::Error> {
        std::fs::create_dir_all(index_dir)?;
        let mut entries = HashMap::new();

        for node_schema in schema.all_node_schemas() {
            let label = IStr::new(node_schema.label.as_ref());
            for prop_def in &node_schema.properties {
                if !prop_def.searchable {
                    continue;
                }
                let prop_key = IStr::new(prop_def.name.as_ref());
                let dir_name = format!("{}_{}", label.as_str(), prop_key.as_str());
                let dir_path = index_dir.join(&dir_name);
                std::fs::create_dir_all(&dir_path)?;

                // Build tantivy schema
                let mut schema_builder = Schema::builder();
                let node_id_field = schema_builder.add_u64_field("node_id", STORED);
                let text_field = schema_builder.add_text_field("text", TEXT | STORED);
                let tantivy_schema = schema_builder.build();

                // Open or create index
                let index = if dir_path.join("meta.json").exists() {
                    Index::open_in_dir(&dir_path)?
                } else {
                    Index::create_in_dir(&dir_path, tantivy_schema)?
                };

                // Persistent writer (15 MB heap) + auto-reloading reader
                let writer = index.writer(15_000_000)?;
                let reader = index
                    .reader_builder()
                    .reload_policy(tantivy::ReloadPolicy::OnCommitWithDelay)
                    .try_into()?;
                let entry = SearchEntry {
                    index,
                    writer: Mutex::new(writer),
                    reader,
                    node_id_field,
                    text_field,
                };

                tracing::debug!(
                    label = label.as_str(),
                    property = prop_key.as_str(),
                    "search index opened"
                );
                entries.insert((label, prop_key), entry);
            }
        }

        if !entries.is_empty() {
            tracing::info!(indexes = entries.len(), "search indexes initialized");
        }

        Ok(Self {
            index_dir: index_dir.to_path_buf(),
            entries: parking_lot::RwLock::new(entries),
        })
    }

    /// Dynamically add a search index for a (label, property) pair.
    ///
    /// Called when a schema with `searchable` properties is registered via DDL
    /// after the server has started. If the index already exists, this is a no-op.
    pub fn add_searchable(&self, label: IStr, property: IStr) -> Result<(), anyhow::Error> {
        {
            let guard = self.entries.read();
            if guard.contains_key(&(label, property)) {
                return Ok(());
            }
        }

        let dir_name = format!("{}_{}", label.as_str(), property.as_str());
        let dir_path = self.index_dir.join(&dir_name);
        std::fs::create_dir_all(&dir_path)?;

        let mut schema_builder = Schema::builder();
        let node_id_field = schema_builder.add_u64_field("node_id", STORED);
        let text_field = schema_builder.add_text_field("text", TEXT | STORED);
        let tantivy_schema = schema_builder.build();

        let index = if dir_path.join("meta.json").exists() {
            Index::open_in_dir(&dir_path)?
        } else {
            Index::create_in_dir(&dir_path, tantivy_schema)?
        };

        let writer = index.writer(15_000_000)?;
        let reader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        let entry = SearchEntry {
            index,
            writer: Mutex::new(writer),
            reader,
            node_id_field,
            text_field,
        };

        tracing::info!(
            label = label.as_str(),
            property = property.as_str(),
            "search index added dynamically"
        );
        self.entries.write().insert((label, property), entry);
        Ok(())
    }

    /// Returns true if there are any search indexes configured.
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Index a property value for a node.
    pub fn index_property(&self, node_id: NodeId, label: IStr, key: IStr, text: &str) {
        if let Some(entry) = self.entries.read().get(&(label, key)) {
            let writer = entry.writer.lock();
            // Delete old document for this node_id (idempotent)
            writer.delete_term(Term::from_field_u64(entry.node_id_field, node_id.0));
            // Add new document
            if let Err(e) = writer.add_document(doc!(
                entry.node_id_field => node_id.0,
                entry.text_field => text,
            )) {
                tracing::warn!(
                    node_id = node_id.0,
                    error = %e,
                    "failed to index document"
                );
            }
        }
    }

    /// Remove a node from all indexes.
    pub fn remove_node(&self, node_id: NodeId) {
        for entry in self.entries.read().values() {
            let writer = entry.writer.lock();
            writer.delete_term(Term::from_field_u64(entry.node_id_field, node_id.0));
        }
    }

    /// Commit all pending writes across all indexes.
    pub fn commit_all(&self) {
        for ((label, prop), entry) in self.entries.read().iter() {
            let mut writer = entry.writer.lock();
            if let Err(e) = writer.commit() {
                tracing::warn!(
                    label = label.as_str(),
                    property = prop.as_str(),
                    error = %e,
                    "search index commit failed"
                );
            }
        }
    }

    /// Search for nodes matching a text query. Returns (node_id, BM25 score).
    pub fn search(
        &self,
        label: &str,
        property: &str,
        query_text: &str,
        limit: usize,
    ) -> Result<Vec<(NodeId, f32)>, anyhow::Error> {
        let guard = self.entries.read();
        let entry = guard
            .get(&(IStr::new(label), IStr::new(property)))
            .ok_or_else(|| anyhow::anyhow!("no search index for {label}.{property}"))?;

        let searcher = entry.reader.searcher();
        let query_parser = QueryParser::for_index(&entry.index, vec![entry.text_field]);
        let query = query_parser.parse_query(query_text)?;
        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit).order_by_score())?;

        let mut results = Vec::with_capacity(top_docs.len());
        for (score, doc_addr) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_addr)?;
            if let Some(val) = doc.get_first(entry.node_id_field) {
                let owned: tantivy::schema::OwnedValue = val.into();
                if let tantivy::schema::OwnedValue::U64(nid) = owned {
                    results.push((NodeId(nid), score));
                }
            }
        }
        Ok(results)
    }

    /// Rebuild all indexes from current graph state. Called once on startup.
    pub fn rebuild_from_graph(&self, graph: &SeleneGraph) -> Result<(), anyhow::Error> {
        for ((label, prop_key), entry) in self.entries.read().iter() {
            let mut writer = entry.writer.lock();
            writer.delete_all_documents()?;

            let mut count = 0usize;
            for node_id in graph.nodes_by_label(label.as_str()) {
                if let Some(node) = graph.get_node(node_id)
                    && let Some(text) = node.properties.get(*prop_key).and_then(|v| v.as_str())
                {
                    if let Err(e) = writer.add_document(doc!(
                        entry.node_id_field => node_id.0,
                        entry.text_field => text,
                    )) {
                        tracing::warn!(
                            node_id = node_id.0,
                            error = %e,
                            "failed to index document during rebuild"
                        );
                        continue;
                    }
                    count += 1;
                }
            }
            writer.commit()?;
            tracing::info!(
                label = label.as_str(),
                property = prop_key.as_str(),
                documents = count,
                "search index rebuilt"
            );
        }
        Ok(())
    }
}

/// SearchProvider implementation bridging GQL procedures to tantivy indexes.
impl selene_gql::runtime::procedures::search::SearchProvider for SearchIndex {
    fn search(
        &self,
        label: &str,
        property: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<(NodeId, f32)>, String> {
        SearchIndex::search(self, label, property, query, limit).map_err(|e| e.to_string())
    }

    fn search_all_properties(
        &self,
        label: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<(NodeId, f32)>, String> {
        let label_key = IStr::new(label);
        let mut all_results: Vec<(NodeId, f32)> = Vec::new();
        for (l, prop) in self.entries.read().keys() {
            if *l == label_key
                && let Ok(results) = SearchIndex::search(self, label, prop.as_str(), query, limit)
            {
                all_results.extend(results);
            }
        }
        // Sort by score descending and deduplicate by node_id (keep best score)
        all_results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        let mut seen = std::collections::HashSet::new();
        all_results.retain(|(nid, _)| seen.insert(nid.0));
        all_results.truncate(limit);
        Ok(all_results)
    }
}

/// Initialize the search index and register as the SearchProvider for GQL procedures.
pub fn init_search_provider(index: Arc<SearchIndex>) {
    selene_gql::runtime::procedures::search::set_search_provider(index);
}

// ── Service wrapper ──────────────────────────────────────────────────

/// SearchIndex as a registered service in the ServiceRegistry.
pub struct SearchIndexService {
    pub index: Arc<SearchIndex>,
}

impl SearchIndexService {
    pub fn new(index: Arc<SearchIndex>) -> Self {
        Self { index }
    }
}

impl crate::service_registry::Service for SearchIndexService {
    fn name(&self) -> &'static str {
        "search"
    }

    fn health(&self) -> crate::service_registry::ServiceHealth {
        crate::service_registry::ServiceHealth::Healthy
    }
}
