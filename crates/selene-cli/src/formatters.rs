//! Result formatters for CLI output: table, JSON, CSV.
//!
//! Format GQL query results for terminal display. The table formatter
//! auto-detects terminal width and truncates wide columns.

use std::fmt::Write;

/// Format result rows for display.
pub trait ResultFormatter {
    fn format(&self, columns: &[String], rows: &[Vec<String>]) -> String;
}

/// Aligned table with column headers, auto-truncation, and row limit.
pub struct TableFormatter {
    width: usize,
    row_limit: usize,
}

impl TableFormatter {
    pub fn new(width: usize, row_limit: usize) -> Self {
        Self { width, row_limit }
    }
}

impl ResultFormatter for TableFormatter {
    fn format(&self, columns: &[String], rows: &[Vec<String>]) -> String {
        if columns.is_empty() {
            return String::new();
        }

        let display_rows = if self.row_limit > 0 && rows.len() > self.row_limit {
            &rows[..self.row_limit]
        } else {
            rows
        };

        // Column widths: capped at fair share of terminal width
        let num_cols = columns.len();
        let separators = (num_cols + 1) * 3; // " | " between columns + edges
        let available = self.width.saturating_sub(separators);
        let max_col_width = (available / num_cols).max(8);

        let mut col_widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
        for row in display_rows {
            for (i, cell) in row.iter().enumerate() {
                if i < col_widths.len() {
                    col_widths[i] = col_widths[i].max(cell.len());
                }
            }
        }
        // Cap each column at max width
        for w in &mut col_widths {
            *w = (*w).min(max_col_width);
        }

        let mut output = String::new();

        // Render header, separator, and rows
        let header: Vec<String> = columns
            .iter()
            .enumerate()
            .map(|(i, c)| truncate_pad(c, col_widths[i]))
            .collect();
        let _ = writeln!(output, " {} ", header.join(" | "));

        let sep: Vec<String> = col_widths.iter().map(|w| "-".repeat(*w)).collect();
        let _ = writeln!(output, "-{}-", sep.join("-+-"));
        for row in display_rows {
            let cells: Vec<String> = row
                .iter()
                .enumerate()
                .map(|(i, cell)| {
                    let w = col_widths.get(i).copied().unwrap_or(8);
                    truncate_pad(cell, w)
                })
                .collect();
            let _ = writeln!(output, " {} ", cells.join(" | "));
        }

        if self.row_limit > 0 && rows.len() > self.row_limit {
            let _ = writeln!(output, "... and {} more rows", rows.len() - self.row_limit);
        }

        output
    }
}

/// Newline-delimited JSON (NDJSON), one JSON object per row.
pub struct JsonFormatter;

impl ResultFormatter for JsonFormatter {
    fn format(&self, columns: &[String], rows: &[Vec<String>]) -> String {
        let mut output = String::new();
        for row in rows {
            let mut map = serde_json::Map::new();
            for (i, col) in columns.iter().enumerate() {
                let val = row.get(i).map_or("", |s| s.as_str());
                let json_val = serde_json::from_str(val)
                    .unwrap_or_else(|_| serde_json::Value::String(val.to_string()));
                map.insert(col.clone(), json_val);
            }
            output.push_str(&serde_json::to_string(&map).unwrap_or_default());
            output.push('\n');
        }
        output
    }
}

/// Standard CSV with header row.
pub struct CsvFormatter;

fn escape_csv_field(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

impl ResultFormatter for CsvFormatter {
    fn format(&self, columns: &[String], rows: &[Vec<String>]) -> String {
        let mut output = String::new();
        let headers: Vec<String> = columns.iter().map(|c| escape_csv_field(c)).collect();
        output.push_str(&headers.join(","));
        output.push('\n');
        for row in rows {
            let escaped: Vec<String> = row.iter().map(|cell| escape_csv_field(cell)).collect();
            output.push_str(&escaped.join(","));
            output.push('\n');
        }
        output
    }
}

/// Parse the JSON data payload from a GQL response into columns and rows.
///
/// The data is a JSON array of arrays: `[["col1","col2",...], [val1,val2,...], ...]`
/// where the first row is column names and subsequent rows are data.
pub fn parse_gql_json_data(json_data: &str) -> (Vec<String>, Vec<Vec<String>>) {
    let trimmed = json_data.trim();
    if trimmed.is_empty() || trimmed == "[]" {
        return (Vec::new(), Vec::new());
    }

    let parsed: Result<Vec<Vec<serde_json::Value>>, _> = serde_json::from_str(trimmed);
    match parsed {
        Ok(table) if !table.is_empty() => {
            // First row is column names
            let columns: Vec<String> = table[0]
                .iter()
                .map(|v| match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .collect();

            let rows: Vec<Vec<String>> = table[1..]
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            serde_json::Value::Null => "null".to_string(),
                            other => other.to_string(),
                        })
                        .collect()
                })
                .collect();

            (columns, rows)
        }
        _ => (Vec::new(), Vec::new()),
    }
}

fn truncate_pad(s: &str, width: usize) -> String {
    let display_len = s.chars().count();
    if display_len <= width {
        format!("{s:<width$}")
    } else {
        if width <= 3 {
            return ".".repeat(width);
        }
        let truncated: String = s.chars().take(width.saturating_sub(3)).collect();
        format!("{truncated}...")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_formats_simple_rows() {
        let columns = vec!["name".into(), "value".into()];
        let rows = vec![
            vec!["sensor1".into(), "72.5".into()],
            vec!["sensor2".into(), "68.0".into()],
        ];
        let output = TableFormatter::new(120, 100).format(&columns, &rows);
        assert!(output.contains("name"), "should contain header 'name'");
        assert!(output.contains("sensor1"), "should contain data 'sensor1'");
        assert!(output.contains("72.5"), "should contain data '72.5'");
        assert!(output.contains("---"), "should contain separator");
    }

    #[test]
    fn table_respects_row_limit() {
        let columns = vec!["id".into()];
        let rows: Vec<Vec<String>> = (0..200).map(|i| vec![i.to_string()]).collect();
        let output = TableFormatter::new(80, 5).format(&columns, &rows);
        assert!(output.contains("... and 195 more rows"));
    }

    #[test]
    fn table_empty_columns() {
        let output = TableFormatter::new(80, 100).format(&[], &[]);
        assert!(output.is_empty());
    }

    #[test]
    fn json_formats_as_ndjson() {
        let columns = vec!["name".into(), "val".into()];
        let rows = vec![vec!["test".into(), "42".into()]];
        let output = JsonFormatter.format(&columns, &rows);
        assert!(output.contains("\"name\""));
        assert!(output.contains("\"test\""));
        // 42 should parse as number, not string
        assert!(output.contains(":42"));
    }

    #[test]
    fn csv_escapes_commas() {
        let columns = vec!["name".into()];
        let rows = vec![vec!["hello, world".into()]];
        let output = CsvFormatter.format(&columns, &rows);
        assert!(output.contains("\"hello, world\""));
    }

    #[test]
    fn parse_gql_json_data_basic() {
        let data = r#"[["id","name"],[1,"sensor"],[2,"pump"]]"#;
        let (cols, rows) = parse_gql_json_data(data);
        assert_eq!(cols, vec!["id", "name"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec!["1", "sensor"]);
        assert_eq!(rows[1], vec!["2", "pump"]);
    }

    #[test]
    fn parse_gql_json_data_empty() {
        let (cols, rows) = parse_gql_json_data("[]");
        assert!(cols.is_empty());
        assert!(rows.is_empty());
    }

    #[test]
    fn parse_gql_json_data_whitespace() {
        let (cols, rows) = parse_gql_json_data("  ");
        assert!(cols.is_empty());
        assert!(rows.is_empty());
    }

    #[test]
    fn truncate_pad_ascii() {
        assert_eq!(truncate_pad("hello", 10), "hello     ");
        assert_eq!(truncate_pad("hello world!", 8), "hello...");
    }

    #[test]
    fn truncate_pad_utf8_no_panic() {
        // Multi-byte chars: should not panic on truncation
        let s = "temperature\u{00B0}F\u{00B0}C\u{00B0}K";
        let result = truncate_pad(s, 10);
        assert!(result.ends_with("..."));
        // Should not exceed width in chars
        assert!(result.chars().count() <= 10);
    }

    #[test]
    fn truncate_pad_short_width() {
        assert_eq!(truncate_pad("hello", 3), "...");
        assert_eq!(truncate_pad("hello", 2), "..");
    }
}
