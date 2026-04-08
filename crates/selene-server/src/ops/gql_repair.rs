//! GQL query repair: fuzzy-match labels and properties against the schema.
//!
//! When a GQL query fails, this module extracts candidate identifiers from
//! the error message and fuzzy-matches them against known schema labels
//! and property names using Levenshtein distance.

use selene_graph::SeleneGraph;

/// A repair suggestion for a failed GQL query.
#[derive(serde::Serialize)]
pub struct RepairSuggestion {
    /// What was wrong.
    pub issue: String,
    /// Suggested correction.
    pub suggestion: String,
    /// Confidence score (0.0 to 1.0). Higher = more confident.
    pub confidence: f64,
}

/// Analyze a GQL error message and suggest repairs based on schema knowledge.
pub fn suggest_repairs(
    error_message: &str,
    query: &str,
    graph: &SeleneGraph,
) -> Vec<RepairSuggestion> {
    let mut suggestions = Vec::new();

    // Collect known labels (node + edge) from schema
    let mut known_labels: Vec<String> = graph
        .node_label_counts()
        .keys()
        .map(|l| l.to_string())
        .collect();
    known_labels.extend(graph.edge_label_counts().keys().map(|l| l.to_string()));

    let known_properties: Vec<String> = graph
        .all_node_ids()
        .take(200) // sample up to 200 nodes for property names
        .filter_map(|nid| graph.get_node(nid))
        .flat_map(|n| n.properties.iter().map(|(k, _)| k.to_string()))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    // Extract words from the query that might be labels (after : in patterns)
    // or properties (after . in expressions)
    let query_labels = extract_pattern_labels(query);
    let query_properties = extract_dot_properties(query);

    // Fuzzy-match query labels against schema labels
    for ql in &query_labels {
        if known_labels.iter().any(|kl| kl == ql) {
            continue; // exact match, no repair needed
        }
        if let Some((best, dist)) = closest_match(ql, &known_labels) {
            let confidence = 1.0 - (dist as f64 / ql.len().max(best.len()) as f64);
            if confidence >= 0.6 {
                suggestions.push(RepairSuggestion {
                    issue: format!("unknown label '{ql}'"),
                    suggestion: format!("did you mean '{best}'?"),
                    confidence,
                });
            }
        }
    }

    // Fuzzy-match query properties against known properties
    for qp in &query_properties {
        if known_properties.iter().any(|kp| kp == qp) {
            continue;
        }
        if let Some((best, dist)) = closest_match(qp, &known_properties) {
            let confidence = 1.0 - (dist as f64 / qp.len().max(best.len()) as f64);
            if confidence >= 0.6 {
                suggestions.push(RepairSuggestion {
                    issue: format!("unknown property '{qp}'"),
                    suggestion: format!("did you mean '{best}'?"),
                    confidence,
                });
            }
        }
    }

    // Check for common error patterns in the error message
    let msg_lower = error_message.to_lowercase();
    if msg_lower.contains("expected")
        && !msg_lower.contains("return")
        && !query.to_uppercase().contains("RETURN")
        && query.to_uppercase().contains("MATCH")
    {
        suggestions.push(RepairSuggestion {
            issue: "query may be missing RETURN clause".into(),
            suggestion: "MATCH queries require a RETURN clause".into(),
            confidence: 0.9,
        });
    }

    suggestions.sort_by(|a, b| {
        b.confidence
            .partial_cmp(&a.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    suggestions
}

/// Extract label names from GQL patterns like `(:Label)`, `(n:Label)`, or `[:Label]`.
fn extract_pattern_labels(query: &str) -> Vec<String> {
    let mut labels = Vec::new();
    let chars: Vec<char> = query.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == ':' && i > 0 {
            // Label context: after ( or [ or a variable name
            let prev = chars[i - 1];
            if prev == '(' || prev == '[' || prev.is_alphanumeric() || prev == '_' {
                // Read the label name
                let start = i + 1;
                let mut end = start;
                while end < chars.len() && (chars[end].is_alphanumeric() || chars[end] == '_') {
                    end += 1;
                }
                if end > start {
                    labels.push(chars[start..end].iter().collect());
                }
            }
        }
        i += 1;
    }
    labels
}

/// Extract property names from dot notation like `n.property`.
fn extract_dot_properties(query: &str) -> Vec<String> {
    let mut props = Vec::new();
    let chars: Vec<char> = query.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '.' && i > 0 && chars[i - 1].is_alphanumeric() {
            let start = i + 1;
            let mut end = start;
            while end < chars.len() && (chars[end].is_alphanumeric() || chars[end] == '_') {
                end += 1;
            }
            if end > start {
                let prop: String = chars[start..end].iter().collect();
                // Skip common non-property dot-access patterns
                if prop != "id" {
                    props.push(prop);
                }
            }
        }
        i += 1;
    }
    props
}

/// Find the closest string match using Levenshtein distance.
fn closest_match<'a>(target: &str, candidates: &'a [String]) -> Option<(&'a str, usize)> {
    candidates
        .iter()
        .map(|c| (c.as_str(), levenshtein(target, c)))
        .filter(|(_, d)| *d <= target.len() / 2 + 1) // reject if too different
        .min_by_key(|(_, d)| *d)
}

/// Levenshtein edit distance between two strings.
fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let (m, n) = (a.len(), b.len());

    if m == 0 {
        return n;
    }
    if n == 0 {
        return m;
    }

    let mut prev: Vec<usize> = (0..=n).collect();
    let mut curr = vec![0; n + 1];

    for i in 1..=m {
        curr[0] = i;
        for j in 1..=n {
            let cost = usize::from(!a[i - 1].to_lowercase().eq(b[j - 1].to_lowercase()));
            curr[j] = (prev[j] + 1).min(curr[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[n]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levenshtein_basic() {
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("", "abc"), 3);
        assert_eq!(levenshtein("abc", "abc"), 0);
        assert_eq!(levenshtein("equipment", "equpment"), 1);
        assert_eq!(levenshtein("temperature", "temparature"), 1);
    }

    #[test]
    fn extract_labels_from_gql() {
        let labels = extract_pattern_labels("MATCH (n:sensor) WHERE n.temp > 72 RETURN n");
        assert_eq!(labels, vec!["sensor"]);

        let labels = extract_pattern_labels("MATCH (a:building)-[:contains]->(b:floor)");
        assert!(labels.contains(&"building".to_string()));
        assert!(labels.contains(&"floor".to_string()));
        assert!(labels.contains(&"contains".to_string()));
    }

    #[test]
    fn extract_properties_from_gql() {
        let props = extract_dot_properties("FILTER n.temparature > 72 AND n.status = 'ok'");
        assert!(props.contains(&"temparature".to_string()));
        assert!(props.contains(&"status".to_string()));
    }

    #[test]
    fn case_insensitive_levenshtein() {
        assert_eq!(levenshtein("Sensor", "sensor"), 0);
        assert_eq!(levenshtein("Equipment", "equipment"), 0);
    }
}
