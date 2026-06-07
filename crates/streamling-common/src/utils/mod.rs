pub mod arrow;
pub mod batch;
pub mod dedup;
pub mod metrics;

/// Parse a comma-delimited primary key string into a vector of column names.
/// Handles whitespace trimming and filters out empty strings.
pub fn parse_primary_key_columns(primary_key: &str) -> Vec<&str> {
    primary_key
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect()
}
