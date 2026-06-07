//! Print sink output parser for capturing and verifying pipeline output.
//!
//! The print sink outputs JSON rows to tracing logs with format:
//! `{row_count} -> (sample_every={n}) [{row_kind}] {json}`
//!
//! This module parses that output to enable assertions on the pipeline's output data.

use regex::Regex;
use serde_json::Value;

/// Parsed output from a print sink
#[derive(Debug, Clone)]
pub struct PrintSinkOutput {
    rows: Vec<PrintSinkRow>,
}

/// A single row from the print sink output
#[derive(Debug, Clone)]
pub struct PrintSinkRow {
    /// Row kind: "Insert", "Delete", or "Update"
    pub row_kind: String,
    /// Parsed JSON data
    pub data: Value,
}

impl PrintSinkOutput {
    /// Parse print sink output from captured stderr/stdout
    ///
    /// Format: "{row_count} -> (sample_every={n}) [{row_kind}] {json}"
    pub fn parse(output: &str) -> Self {
        let re =
            Regex::new(r"\d+ -> \(sample_every=\d+\) \[(Insert|Delete|Update)\] (.+)").unwrap();

        let rows = output
            .lines()
            .filter_map(|line| {
                re.captures(line).and_then(|caps| {
                    let row_kind = caps.get(1)?.as_str().to_string();
                    let json_str = caps.get(2)?.as_str();
                    let data = serde_json::from_str(json_str).ok()?;
                    Some(PrintSinkRow { row_kind, data })
                })
            })
            .collect();

        Self { rows }
    }

    /// Check if a column exists in any row
    pub fn has_column(&self, name: &str) -> bool {
        self.rows.iter().any(|r| r.data.get(name).is_some())
    }

    /// Count how many rows have a given column
    pub fn column_count(&self, name: &str) -> usize {
        self.rows
            .iter()
            .filter(|r| r.data.get(name).is_some())
            .count()
    }

    /// Get all values for a column across all rows
    pub fn column_values(&self, name: &str) -> Vec<&Value> {
        self.rows.iter().filter_map(|r| r.data.get(name)).collect()
    }

    /// Get all rows
    pub fn rows(&self) -> &[PrintSinkRow] {
        &self.rows
    }

    /// Get the number of rows
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Check if there are no rows
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get column names from the first row
    pub fn column_names(&self) -> Vec<String> {
        self.rows
            .first()
            .and_then(|r| r.data.as_object())
            .map(|obj| obj.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Get a specific row by index
    pub fn get(&self, index: usize) -> Option<&PrintSinkRow> {
        self.rows.get(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_print_sink_output() {
        let output = r#"
2025-01-08T10:00:00.000Z  INFO streamling: 1 -> (sample_every=1) [Insert] {"id":1,"data":"hello","_gs_op":"c"}
2025-01-08T10:00:00.001Z  INFO streamling: 2 -> (sample_every=1) [Insert] {"id":2,"data":"world","_gs_op":"c"}
2025-01-08T10:00:00.002Z  INFO streamling: 3 -> (sample_every=1) [Delete] {"id":1,"data":"hello","_gs_op":"d"}
"#;

        let parsed = PrintSinkOutput::parse(output);

        assert_eq!(parsed.len(), 3);
        assert!(parsed.has_column("id"));
        assert!(parsed.has_column("data"));
        assert!(parsed.has_column("_gs_op"));
        assert!(!parsed.has_column("nonexistent"));

        assert_eq!(parsed.column_count("id"), 3);
        assert_eq!(parsed.column_count("_gs_op"), 3);

        let column_names = parsed.column_names();
        assert!(column_names.contains(&"id".to_string()));
        assert!(column_names.contains(&"data".to_string()));
        assert!(column_names.contains(&"_gs_op".to_string()));

        // Check row kinds
        assert_eq!(parsed.rows()[0].row_kind, "Insert");
        assert_eq!(parsed.rows()[1].row_kind, "Insert");
        assert_eq!(parsed.rows()[2].row_kind, "Delete");
    }

    #[test]
    fn test_parse_empty_output() {
        let output = "Some other log line\nAnother line without print sink format";
        let parsed = PrintSinkOutput::parse(output);
        assert!(parsed.is_empty());
    }
}
