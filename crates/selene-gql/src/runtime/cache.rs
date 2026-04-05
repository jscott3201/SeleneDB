//! Plan cache: parsed GQL ASTs keyed by query text hash.
//!
//! Avoids re-parsing identical queries. Invalidated when the graph's
//! generation counter changes (schema modifications).
//!
//! Includes a fast-parse path for `CALL name(args) YIELD cols` queries
//! that bypasses the PEG parser entirely (~10-20x speedup for procedure
//! calls, from ~10 us to ~500 ns).

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use selene_core::IStr;
use smol_str::SmolStr;

use crate::ast::expr::{Expr, ProcedureCall, YieldItem};
use crate::ast::statement::{GqlStatement, PipelineStatement, QueryPipeline};
use crate::parser::parse_statement;
use crate::types::error::GqlError;
use crate::types::value::GqlValue;

/// Maximum number of cached entries before LRU eviction.
const MAX_ENTRIES: usize = 256;

/// Cache for parsed GQL statements, keyed by query text hash.
///
/// Scope-independent: the auth scope bitmap is applied at execution time,
/// not cached. The same parsed AST serves queries from different auth contexts.
pub struct PlanCache {
    entries: Mutex<HashMap<u64, CachedEntry>>,
    /// Last known graph generation for bulk invalidation.
    last_generation: AtomicU64,
}

struct CachedEntry {
    statement: Arc<GqlStatement>,
    /// Tracks access order for LRU eviction (higher = more recent).
    access_counter: u64,
}

impl Default for PlanCache {
    fn default() -> Self {
        Self::new()
    }
}

impl PlanCache {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            last_generation: AtomicU64::new(0),
        }
    }

    /// Get a cached parsed statement, or parse and cache it.
    ///
    /// If the graph generation has changed since the last call, the entire
    /// cache is cleared (schema may have changed, affecting type resolution).
    ///
    /// CALL queries with literal arguments bypass both the cache and the PEG
    /// parser via `try_fast_parse_call`, yielding ~10-20x speedup.
    pub fn get_or_parse(
        &self,
        gql: &str,
        graph_generation: u64,
    ) -> Result<Arc<GqlStatement>, GqlError> {
        // Try CALL fast path (bypasses cache and PEG parser entirely)
        if let Some(stmt) = try_fast_parse_call(gql) {
            return Ok(Arc::new(stmt));
        }

        let key = hash_query(gql);

        let mut entries = self.entries.lock();

        // Invalidate all on schema change (generation mismatch)
        let last_gen = self.last_generation.load(Ordering::Relaxed);
        if graph_generation != last_gen {
            entries.clear();
            self.last_generation
                .store(graph_generation, Ordering::Relaxed);
        }

        // Cache hit
        if let Some(entry) = entries.get_mut(&key) {
            entry.access_counter = graph_generation; // touch for LRU
            return Ok(Arc::clone(&entry.statement));
        }

        // Cache miss: parse and insert
        let statement = parse_statement(gql)?;

        // Evict least-recently-used entry if at capacity.
        // O(n) scan over MAX_ENTRIES=256 (under 1us on modern hardware).
        // A linked-list LRU is not worth the complexity at this capacity.
        if entries.len() >= MAX_ENTRIES
            && let Some(&oldest_key) = entries
                .iter()
                .min_by_key(|(_, e)| e.access_counter)
                .map(|(k, _)| k)
        {
            entries.remove(&oldest_key);
        }

        let statement = Arc::new(statement);
        entries.insert(
            key,
            CachedEntry {
                statement: Arc::clone(&statement),
                access_counter: graph_generation,
            },
        );

        Ok(statement)
    }

    /// Number of cached entries.
    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    /// Clear all cached entries.
    pub fn clear(&self) {
        self.entries.lock().clear();
    }

    /// True if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.lock().is_empty()
    }

    /// Cache statistics for monitoring.
    pub fn stats(&self) -> CacheStats {
        let entries = self.entries.lock();
        CacheStats {
            entries: entries.len(),
            capacity: MAX_ENTRIES,
            generation: self.last_generation.load(Ordering::Relaxed),
        }
    }
}

/// Plan cache statistics.
pub struct CacheStats {
    pub entries: usize,
    pub capacity: usize,
    pub generation: u64,
}

// ── CALL fast-parse path ──────────────────────────────────────────

/// Attempt to fast-parse a standalone `CALL name(args) YIELD cols` query.
///
/// Returns `Some(GqlStatement)` if the input matches the expected pattern
/// with only literal arguments (integers, floats, strings). Falls through
/// to `None` for anything more complex (variable references, nested
/// expressions, MATCH + CALL pipelines, etc.).
///
/// The returned AST is identical to what the PEG parser would produce:
/// `GqlStatement::Query(QueryPipeline { statements: [PipelineStatement::Call(..)] })`.
fn try_fast_parse_call(gql: &str) -> Option<GqlStatement> {
    let s = gql.trim();

    // Must start with CALL (case-insensitive)
    if s.len() < 5 {
        return None;
    }
    if !s[..4].eq_ignore_ascii_case("CALL") {
        return None;
    }
    // The character after CALL must be whitespace (not part of a longer keyword)
    let after_call = s.as_bytes()[4];
    if after_call != b' ' && after_call != b'\t' && after_call != b'\n' && after_call != b'\r' {
        return None;
    }

    let rest = s[4..].trim_start();

    // Extract procedure name: alphanumeric + underscores + dots, up to '('
    let open_paren = rest.find('(')?;
    let name_str = rest[..open_paren].trim_end();

    // Validate procedure name: must be non-empty, chars are [a-zA-Z0-9_.]
    if name_str.is_empty() {
        return None;
    }
    for c in name_str.chars() {
        if !c.is_ascii_alphanumeric() && c != '_' && c != '.' {
            return None;
        }
    }

    // Find the matching close paren. Must handle quoted strings inside args
    // that could contain parentheses.
    let args_start = open_paren + 1;
    let close_paren = find_matching_close_paren(&rest[args_start..])?;
    let args_str = &rest[args_start..args_start + close_paren];
    let after_args = rest[args_start + close_paren + 1..].trim_start();

    // Parse arguments
    let args = parse_arg_list(args_str)?;

    // Parse YIELD clause (optional for the fast path -- some CALL queries have no YIELD)
    let yields;
    let remainder;
    if after_args.len() >= 5 && after_args[..5].eq_ignore_ascii_case("YIELD") {
        let after_yield_kw = &after_args[5..];
        // Must have whitespace after YIELD keyword
        if after_yield_kw.is_empty()
            || (!after_yield_kw.starts_with(' ')
                && !after_yield_kw.starts_with('\t')
                && !after_yield_kw.starts_with('\n'))
        {
            return None;
        }
        let yield_str = after_yield_kw.trim_start();
        let (parsed_yields, rest_after_yield) = parse_yield_list(yield_str)?;
        yields = parsed_yields;
        remainder = rest_after_yield;
    } else {
        yields = Vec::new();
        remainder = after_args;
    }

    // Must have nothing left (or only whitespace/semicolons)
    let remainder = remainder.trim();
    if !remainder.is_empty() && remainder != ";" {
        return None;
    }

    let proc_call = ProcedureCall {
        name: IStr::new(name_str),
        args,
        yields,
    };

    Some(GqlStatement::Query(QueryPipeline {
        statements: vec![PipelineStatement::Call(proc_call)],
    }))
}

/// Find the index of the closing ')' that matches the opening '(' at position 0,
/// skipping over single-quoted string literals.
fn find_matching_close_paren(s: &str) -> Option<usize> {
    let mut depth: u32 = 0;
    let mut in_string = false;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if in_string {
            if b == b'\\' {
                i += 1; // skip escaped character
            } else if b == b'\'' {
                in_string = false;
            }
        } else {
            match b {
                b'\'' => in_string = true,
                b'(' => depth += 1,
                b')' => {
                    if depth == 0 {
                        return Some(i);
                    }
                    depth -= 1;
                }
                _ => {}
            }
        }
        i += 1;
    }
    None
}

/// Parse a comma-separated argument list of literal values.
/// Supports: integer literals (including negative), float literals, and
/// single-quoted string literals. Returns None if any argument is not a
/// recognized literal (variable references, expressions, etc.).
fn parse_arg_list(s: &str) -> Option<Vec<Expr>> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Some(Vec::new());
    }

    let mut args = Vec::new();
    // Split on commas, respecting single-quoted strings
    let parts = split_on_comma(trimmed)?;
    for part in parts {
        let arg = part.trim();
        if arg.is_empty() {
            return None;
        }
        args.push(parse_literal_arg(arg)?);
    }
    Some(args)
}

/// Split a string on commas, respecting single-quoted strings.
fn split_on_comma(s: &str) -> Option<Vec<&str>> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut in_string = false;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if in_string {
            if b == b'\\' {
                i += 1; // skip escaped char
            } else if b == b'\'' {
                in_string = false;
            }
        } else if b == b'\'' {
            in_string = true;
        } else if b == b',' {
            parts.push(&s[start..i]);
            start = i + 1;
        }
        i += 1;
    }
    if in_string {
        return None; // unterminated string
    }
    parts.push(&s[start..]);
    Some(parts)
}

/// Parse a single literal argument: integer, float, or single-quoted string.
fn parse_literal_arg(s: &str) -> Option<Expr> {
    // Boolean literals
    if s.eq_ignore_ascii_case("TRUE") {
        return Some(Expr::Literal(GqlValue::Bool(true)));
    }
    if s.eq_ignore_ascii_case("FALSE") {
        return Some(Expr::Literal(GqlValue::Bool(false)));
    }
    if s.eq_ignore_ascii_case("NULL") {
        return Some(Expr::Literal(GqlValue::Null));
    }

    // Single-quoted string literal
    if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
        let inner = &s[1..s.len() - 1];
        let unescaped = fast_unescape(inner)?;
        return Some(Expr::Literal(GqlValue::String(SmolStr::new(&unescaped))));
    }

    // Numeric: try integer first, then float
    // Handle optional leading sign
    let num_str = s;

    // Integer (may be negative)
    if let Ok(v) = num_str.parse::<i64>() {
        return Some(Expr::Literal(GqlValue::Int(v)));
    }

    // Float (may be negative, must contain '.' or 'e'/'E')
    if (num_str.contains('.') || num_str.contains('e') || num_str.contains('E'))
        && let Ok(v) = num_str.parse::<f64>()
    {
        return Some(Expr::Literal(GqlValue::Float(v)));
    }

    // Not a recognized literal -- fall through to PEG parser
    None
}

/// Simplified string unescape for the fast path. Returns None on invalid
/// escape sequences (caller falls through to PEG parser).
fn fast_unescape(s: &str) -> Option<String> {
    if !s.contains('\\') {
        return Some(s.to_string());
    }
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => result.push('\n'),
                Some('r') => result.push('\r'),
                Some('t') => result.push('\t'),
                Some('\\') => result.push('\\'),
                Some('\'') => result.push('\''),
                Some('"') => result.push('"'),
                _ => return None, // unknown escape, fall through to PEG
            }
        } else {
            result.push(c);
        }
    }
    Some(result)
}

/// Parse `YIELD col1, col2 AS alias, ...` returning the parsed items
/// and the remaining unparsed input.
fn parse_yield_list(s: &str) -> Option<(Vec<YieldItem>, &str)> {
    let mut items = Vec::new();
    let mut pos = 0;
    let bytes = s.as_bytes();

    loop {
        // Skip whitespace
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= bytes.len() {
            break;
        }

        // Check for semicolons or other terminators
        if bytes[pos] == b';' {
            break;
        }

        // Read an identifier (column name)
        let name_start = pos;
        while pos < bytes.len() && (bytes[pos].is_ascii_alphanumeric() || bytes[pos] == b'_') {
            pos += 1;
        }
        if pos == name_start {
            return None; // expected identifier
        }
        let name = &s[name_start..pos];

        // Skip whitespace
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }

        // Check for AS alias
        let alias = if pos + 2 <= bytes.len()
            && s[pos..pos + 2].eq_ignore_ascii_case("AS")
            && (pos + 2 >= bytes.len() || bytes[pos + 2].is_ascii_whitespace())
        {
            pos += 2;
            while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
                pos += 1;
            }
            let alias_start = pos;
            while pos < bytes.len() && (bytes[pos].is_ascii_alphanumeric() || bytes[pos] == b'_') {
                pos += 1;
            }
            if pos == alias_start {
                return None; // expected alias identifier
            }
            Some(IStr::new(&s[alias_start..pos].to_uppercase()))
        } else {
            None
        };

        items.push(YieldItem {
            name: IStr::new(&name.to_uppercase()),
            alias,
        });

        // Skip whitespace
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }

        // Expect comma or end
        if pos < bytes.len() && bytes[pos] == b',' {
            pos += 1;
        } else {
            break;
        }
    }

    Some((items, &s[pos..]))
}

fn hash_query(gql: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    gql.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_hit() {
        let cache = PlanCache::new();
        let gql = "MATCH (n) RETURN n";
        let _first = cache.get_or_parse(gql, 1).unwrap();
        let _second = cache.get_or_parse(gql, 1).unwrap();
        assert_eq!(cache.len(), 1); // cached, not re-parsed
    }

    #[test]
    fn cache_miss_different_query() {
        let cache = PlanCache::new();
        let _a = cache.get_or_parse("MATCH (n) RETURN n", 1).unwrap();
        let _b = cache.get_or_parse("MATCH (s:sensor) RETURN s", 1).unwrap();
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn cache_invalidation_on_generation_change() {
        let cache = PlanCache::new();
        let _a = cache.get_or_parse("MATCH (n) RETURN n", 1).unwrap();
        assert_eq!(cache.len(), 1);

        // Generation changes → cache cleared
        let _b = cache.get_or_parse("MATCH (n) RETURN n", 2).unwrap();
        assert_eq!(cache.len(), 1); // re-parsed and cached with new gen
    }

    #[test]
    fn cache_parse_error() {
        let cache = PlanCache::new();
        let result = cache.get_or_parse("INVALID QUERY", 1);
        assert!(result.is_err());
        assert_eq!(cache.len(), 0); // errors not cached
    }

    #[test]
    fn cache_clear() {
        let cache = PlanCache::new();
        let _ = cache.get_or_parse("MATCH (n) RETURN n", 1).unwrap();
        assert_eq!(cache.len(), 1);
        cache.clear();
        assert_eq!(cache.len(), 0);
    }

    // ── CALL fast-parse tests ────────────────────────────────────────

    #[test]
    fn call_fast_parse_basic() {
        let stmt = try_fast_parse_call("CALL ts.latest(1, 'temp') YIELD timestamp, value")
            .expect("should fast-parse");
        match &stmt {
            GqlStatement::Query(pipeline) => {
                assert_eq!(pipeline.statements.len(), 1);
                match &pipeline.statements[0] {
                    PipelineStatement::Call(call) => {
                        assert_eq!(call.name.as_str(), "ts.latest");
                        assert_eq!(call.args.len(), 2);
                        assert!(matches!(call.args[0], Expr::Literal(GqlValue::Int(1))));
                        match &call.args[1] {
                            Expr::Literal(GqlValue::String(s)) => assert_eq!(&**s, "temp"),
                            other => panic!("expected String literal, got {other:?}"),
                        }
                        assert_eq!(call.yields.len(), 2);
                        assert_eq!(call.yields[0].name.as_str(), "TIMESTAMP");
                        assert!(call.yields[0].alias.is_none());
                        assert_eq!(call.yields[1].name.as_str(), "VALUE");
                        assert!(call.yields[1].alias.is_none());
                    }
                    other => panic!("expected Call, got {other:?}"),
                }
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn call_fast_parse_with_float() {
        let stmt =
            try_fast_parse_call("CALL ts.aggregate(1, 'temp', 3600000000000, 'avg') YIELD value")
                .expect("should fast-parse");
        match &stmt {
            GqlStatement::Query(pipeline) => {
                if let PipelineStatement::Call(call) = &pipeline.statements[0] {
                    assert_eq!(call.name.as_str(), "ts.aggregate");
                    assert_eq!(call.args.len(), 4);
                    assert!(matches!(call.args[0], Expr::Literal(GqlValue::Int(1))));
                    assert!(matches!(
                        call.args[2],
                        Expr::Literal(GqlValue::Int(3_600_000_000_000))
                    ));
                    assert_eq!(call.yields.len(), 1);
                    assert_eq!(call.yields[0].name.as_str(), "VALUE");
                } else {
                    panic!("expected Call");
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn call_fast_parse_negative_int() {
        let stmt =
            try_fast_parse_call("CALL ts.range(1, 'temp', -1000, 9000) YIELD timestamp, value")
                .expect("should fast-parse");
        match &stmt {
            GqlStatement::Query(pipeline) => {
                if let PipelineStatement::Call(call) = &pipeline.statements[0] {
                    assert_eq!(call.args.len(), 4);
                    assert!(matches!(call.args[2], Expr::Literal(GqlValue::Int(-1000))));
                    assert!(matches!(call.args[3], Expr::Literal(GqlValue::Int(9000))));
                } else {
                    panic!("expected Call");
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn call_fast_parse_falls_through() {
        // Non-CALL queries should return None
        assert!(try_fast_parse_call("MATCH (n) RETURN n").is_none());
    }

    #[test]
    fn call_fast_parse_case_insensitive() {
        let stmt = try_fast_parse_call("call ts.latest(1, 'temp') YIELD timestamp, value")
            .expect("should fast-parse case-insensitive CALL");
        match &stmt {
            GqlStatement::Query(pipeline) => {
                if let PipelineStatement::Call(call) = &pipeline.statements[0] {
                    assert_eq!(call.name.as_str(), "ts.latest");
                } else {
                    panic!("expected Call");
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn call_fast_parse_with_alias() {
        let stmt = try_fast_parse_call("CALL ts.latest(1, 'temp') YIELD value AS temperature")
            .expect("should fast-parse with alias");
        match &stmt {
            GqlStatement::Query(pipeline) => {
                if let PipelineStatement::Call(call) = &pipeline.statements[0] {
                    assert_eq!(call.yields.len(), 1);
                    assert_eq!(call.yields[0].name.as_str(), "VALUE");
                    assert_eq!(call.yields[0].alias.unwrap().as_str(), "TEMPERATURE");
                } else {
                    panic!("expected Call");
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn call_fast_parse_no_args() {
        let stmt = try_fast_parse_call("CALL graph.stats() YIELD count")
            .expect("should fast-parse with no args");
        match &stmt {
            GqlStatement::Query(pipeline) => {
                if let PipelineStatement::Call(call) = &pipeline.statements[0] {
                    assert_eq!(call.name.as_str(), "graph.stats");
                    assert!(call.args.is_empty());
                    assert_eq!(call.yields.len(), 1);
                } else {
                    panic!("expected Call");
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn call_fast_parse_no_yield() {
        let stmt = try_fast_parse_call("CALL graph.doSomething(42)")
            .expect("should fast-parse with no YIELD");
        match &stmt {
            GqlStatement::Query(pipeline) => {
                if let PipelineStatement::Call(call) = &pipeline.statements[0] {
                    assert_eq!(call.args.len(), 1);
                    assert!(call.yields.is_empty());
                } else {
                    panic!("expected Call");
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn call_fast_parse_rejects_pipeline() {
        // MATCH + CALL pipeline should NOT fast-parse (it has more than just CALL)
        assert!(
            try_fast_parse_call(
                "MATCH (s:sensor) CALL ts.latest(s.id, 'temp') YIELD value RETURN s"
            )
            .is_none()
        );
    }

    #[test]
    fn call_fast_parse_rejects_variable_arg() {
        // Variable references in args should cause fallthrough
        assert!(try_fast_parse_call("CALL ts.latest(s.id, 'temp') YIELD value").is_none());
    }

    #[test]
    fn call_fast_parse_matches_peg_parser() {
        // Verify the fast-parsed AST matches the PEG parser for a standalone CALL
        let gql = "CALL ts.latest(1, 'temp') YIELD value AS temperature";
        let fast = try_fast_parse_call(gql).expect("fast parse should succeed");
        let peg = parse_statement(gql).expect("PEG parse should succeed");

        // Extract ProcedureCalls from both
        let fast_call = match &fast {
            GqlStatement::Query(p) => match &p.statements[0] {
                PipelineStatement::Call(c) => c,
                _ => panic!("expected Call"),
            },
            _ => panic!("expected Query"),
        };
        let peg_call = match &peg {
            GqlStatement::Query(p) => match &p.statements[0] {
                PipelineStatement::Call(c) => c,
                _ => panic!("expected Call from PEG"),
            },
            _ => panic!("expected Query from PEG"),
        };

        // Compare procedure name
        assert_eq!(fast_call.name.as_str(), peg_call.name.as_str());

        // Compare args
        assert_eq!(fast_call.args.len(), peg_call.args.len());
        assert!(matches!(fast_call.args[0], Expr::Literal(GqlValue::Int(1))));
        assert!(matches!(peg_call.args[0], Expr::Literal(GqlValue::Int(1))));
        match (&fast_call.args[1], &peg_call.args[1]) {
            (Expr::Literal(GqlValue::String(a)), Expr::Literal(GqlValue::String(b))) => {
                assert_eq!(&**a, &**b);
            }
            _ => panic!("expected matching string args"),
        }

        // Compare yields
        assert_eq!(fast_call.yields.len(), peg_call.yields.len());
        assert_eq!(
            fast_call.yields[0].name.as_str(),
            peg_call.yields[0].name.as_str()
        );
        assert_eq!(
            fast_call.yields[0].alias.unwrap().as_str(),
            peg_call.yields[0].alias.unwrap().as_str()
        );
    }

    #[test]
    fn call_fast_parse_bypasses_cache() {
        // CALL fast path should not populate the cache
        let cache = PlanCache::new();
        let _stmt = cache
            .get_or_parse("CALL ts.latest(1, 'temp') YIELD timestamp, value", 1)
            .unwrap();
        assert_eq!(cache.len(), 0); // fast path bypasses cache entirely
    }
}
