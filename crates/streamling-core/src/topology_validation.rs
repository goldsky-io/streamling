//! Validation for pipeline topology.
//!
//! Pre-deserialization: catches orphan sources/transforms (nodes with no downstream
//! consumers) which would cause scan-sharing to wait forever and deadlock the pipeline.
//!
//! Post-deserialization: validates configuration constraints (e.g. job_mode requires
//! hybrid sources).

use crate::sql_parse::extract_table_references_from_sql;
use crate::streamling_user_err;
use crate::topology::{FileSourceMode, PipelineTopology, Source};
use std::collections::HashMap;

fn strip_sql_quotes(name: &str) -> &str {
    name.strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(name)
}

/// Validates that all sources and transforms have at least one consumer.
/// Runs on the raw config before preprocessing.
///
/// Returns an error listing any orphan nodes (sources or transforms with no consumers).
pub fn validate_no_orphan_nodes(config: &str) -> crate::error::Result<()> {
    let value: serde_yaml::Value =
        serde_yaml::from_str(config).map_err(|e| streamling_user_err!("invalid YAML: {}", e))?;

    let root = if let Some(def) = value.get("definition") {
        if def.is_mapping() { def } else { &value }
    } else {
        &value
    };

    let sources: Vec<String> = root
        .get("sources")
        .and_then(|v| v.as_mapping())
        .map(|m| {
            m.keys()
                .filter_map(|k| k.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let transforms: Vec<(String, serde_yaml::Value)> = root
        .get("transforms")
        .and_then(|v| v.as_mapping())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| k.as_str().map(String::from).map(|name| (name, v.clone())))
                .collect()
        })
        .unwrap_or_default();

    let sinks: Vec<String> = root
        .get("sinks")
        .and_then(|v| v.as_mapping())
        .map(|m| {
            m.iter()
                .filter_map(|(_, v)| v.get("from").and_then(|f| f.as_str().map(String::from)))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let mut node_consumers: HashMap<String, usize> = HashMap::new();
    for name in &sources {
        node_consumers.insert(name.to_lowercase(), 0);
    }
    for (name, transform_val) in &transforms {
        // Skip dynamic_table transforms — they are consumed via dynamic_table_check()
        // UDF calls in SQL WHERE clauses, not via FROM clauses or `from:` fields.
        // They have dedicated validation in validate_dynamic_table_usage().
        let is_dynamic_table = transform_val
            .as_mapping()
            .and_then(|m| m.get("type"))
            .and_then(|v| v.as_str())
            == Some("dynamic_table");
        if !is_dynamic_table {
            node_consumers.insert(name.to_lowercase(), 0);
        }
    }

    for (_, transform_val) in &transforms {
        let mapping = match transform_val.as_mapping() {
            Some(m) => m,
            None => continue,
        };
        if let Some(sql) = mapping.get("sql").and_then(|v| v.as_str()) {
            match extract_table_references_from_sql(sql) {
                Ok(table_names) => {
                    for name in table_names {
                        let key = strip_sql_quotes(&name).to_lowercase();
                        if let Some(count) = node_consumers.get_mut(&key) {
                            *count += 1;
                        }
                    }
                }
                Err(_) => {
                    // SQL parser can't handle this query (recursive CTEs, JOINs, etc.).
                    // Skip validation entirely — false positives would block valid pipelines.
                    return Ok(());
                }
            }
        } else if let Some(from) = mapping.get("from").and_then(|v| v.as_str())
            && let Some(count) = node_consumers.get_mut(&from.to_lowercase())
        {
            *count += 1;
        }
    }

    for from in &sinks {
        if let Some(count) = node_consumers.get_mut(&from.to_lowercase()) {
            *count += 1;
        }
    }

    let mut orphans: Vec<_> = node_consumers
        .into_iter()
        .filter(|(_, count)| *count == 0)
        .map(|(name, _)| name)
        .collect();

    if orphans.is_empty() {
        Ok(())
    } else {
        orphans.sort();
        Err(streamling_user_err!(
            "Source(s) and/or transform(s) have no consumers: {}. Remove them or connect them to a transform or sink.",
            orphans.join(", ")
        ))
    }
}

/// Whether a source self-terminates and so can run under `job_mode`. Hybrid
/// sources terminate after all bounded phases complete; file sources terminate
/// only in `Bounded` mode (a `Continuous` file source is unbounded and never
/// completes).
fn source_supports_job_mode(source: &Source) -> bool {
    match source {
        Source::hybrid(_) => true,
        Source::file(file) => matches!(file.mode, FileSourceMode::Bounded),
        _ => false,
    }
}

/// Validates that job_mode is only enabled when every source supports it.
///
/// Job mode requires bounded sources that terminate on their own: hybrid sources
/// (they terminate after all bounded phases complete) and file sources in bounded
/// mode (they read their files once and complete). If any source is neither, we
/// fail early with a clear message listing the unsupported sources.
pub fn validate_job_mode(job_mode: bool, topology: &PipelineTopology) -> crate::error::Result<()> {
    if !job_mode {
        return Ok(());
    }

    let mut unsupported: Vec<&String> = topology
        .sources
        .iter()
        .filter(|(_, s)| !source_supports_job_mode(s))
        .map(|(name, _)| name)
        .collect();

    if !unsupported.is_empty() {
        unsupported.sort();
        return Err(streamling_user_err!(
            "job_mode is enabled but the following source(s) do not support it: {}. \
             Job mode is only supported for pipelines where every source is a hybrid or \
             bounded file source.",
            unsupported
                .iter()
                .map(|s| format!("'{s}'"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unused_source_returns_error() {
        let config = r#"
sources:
  instructions:
    type: dataset
    dataset_name: solana.instructions
    version: "1.0.0"
  transactions_with_instructions:
    type: dataset
    dataset_name: solana.transactions_with_instructions
    version: "1.0.0"
transforms:
  decoded:
    type: sql
    primary_key: id
    sql: "SELECT * FROM transactions_with_instructions"
sinks:
  out:
    type: print
    from: decoded
"#;
        let result = validate_no_orphan_nodes(config);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("instructions"));
        assert!(err.to_string().contains("no consumers"));
    }

    #[test]
    fn unused_transform_returns_error() {
        let config = r#"
sources:
  src:
    type: kafka
    topic: test
transforms:
  used:
    type: sql
    primary_key: id
    sql: "SELECT * FROM src"
  unused:
    type: sql
    primary_key: id
    sql: "SELECT * FROM src"
sinks:
  out:
    type: print
    from: used
"#;
        let result = validate_no_orphan_nodes(config);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("unused"));
    }

    #[test]
    fn valid_topology_ok() {
        let config = r#"
sources:
  src:
    type: kafka
    topic: test
transforms:
  tx:
    type: sql
    primary_key: id
    sql: "SELECT * FROM src"
sinks:
  out:
    type: print
    from: tx
"#;
        assert!(validate_no_orphan_nodes(config).is_ok());
    }

    #[test]
    fn empty_transforms_ok() {
        let config = r#"
sources:
  src:
    type: kafka
    topic: test
transforms: {}
sinks:
  out:
    type: print
    from: src
"#;
        assert!(validate_no_orphan_nodes(config).is_ok());
    }

    #[test]
    fn multiple_orphans_lists_all() {
        let config = r#"
sources:
  a:
    type: kafka
    topic: t1
  b:
    type: kafka
    topic: t2
transforms:
  tx:
    type: sql
    primary_key: id
    sql: "SELECT * FROM a"
sinks:
  out:
    type: print
    from: tx
"#;
        let result = validate_no_orphan_nodes(config);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("b"));
    }

    #[test]
    fn definition_wrapper_uses_nested_structure() {
        let config = r#"
definition:
  name: test
  sources:
    src:
      type: kafka
      topic: test
  transforms: {}
  sinks:
    out:
      type: print
      from: src
"#;
        assert!(validate_no_orphan_nodes(config).is_ok());
    }

    #[test]
    fn definition_wrapper_orphan_detected() {
        let config = r#"
definition:
  sources:
    unused:
      type: kafka
      topic: test
  transforms: {}
  sinks: {}
"#;
        let result = validate_no_orphan_nodes(config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unused"));
    }

    #[test]
    fn case_insensitive_sql_reference_matches_yaml_key() {
        let config = r#"
sources:
  src:
    type: kafka
    topic: test
transforms:
  tx:
    type: sql
    primary_key: id
    sql: "SELECT * FROM Src"
sinks:
  out:
    type: print
    from: tx
"#;
        assert!(
            validate_no_orphan_nodes(config).is_ok(),
            "unquoted SQL identifiers should match YAML keys case-insensitively"
        );
    }

    #[test]
    fn quoted_sql_identifier_matches_yaml_key() {
        let config = r#"
sources:
  my_table:
    type: kafka
    topic: test
transforms:
  tx:
    type: sql
    primary_key: id
    sql: "SELECT * FROM \"my_table\""
sinks:
  out:
    type: print
    from: tx
"#;
        assert!(
            validate_no_orphan_nodes(config).is_ok(),
            "quoted SQL identifiers should match YAML keys after stripping quotes"
        );
    }

    #[test]
    fn unparseable_sql_skips_validation() {
        let config = r#"
sources:
  src:
    type: kafka
    topic: test
transforms:
  joined:
    type: sql
    primary_key: id
    sql: "SELECT * FROM src JOIN src AS s2 ON src.id = s2.id"
sinks:
  out:
    type: print
    from: joined
"#;
        assert!(
            validate_no_orphan_nodes(config).is_ok(),
            "should skip validation when SQL parsing fails, not flag src as orphan"
        );
    }

    #[test]
    fn dynamic_table_not_flagged_as_orphan() {
        let config = r#"
sources:
  token_transfers:
    type: dataset
    dataset_name: solana.token_transfers
    version: "1.0.0"
transforms:
  nest_mint_addresses:
    type: dynamic_table
    backend_type: InMemory
    backend_entity_name: nest_mint_addresses
    sql: "SELECT token_mint_address FROM token_transfers"
  mint_burn:
    type: sql
    primary_key: id
    sql: "SELECT * FROM token_transfers WHERE dynamic_table_check('nest_mint_addresses', token_mint_address)"
sinks:
  out:
    type: print
    from: mint_burn
"#;
        assert!(
            validate_no_orphan_nodes(config).is_ok(),
            "dynamic_table consumed via dynamic_table_check() should not be flagged as orphan"
        );
    }

    #[test]
    fn multiple_dynamic_tables_not_flagged() {
        let config = r#"
sources:
  user_balances:
    type: dataset
    dataset_name: polymarket.user_balances
transforms:
  tracked_wallets:
    type: dynamic_table
    backend_type: Postgres
    backend_entity_name: tracked_wallets
  tracked_condition_ids:
    type: dynamic_table
    backend_type: Postgres
    backend_entity_name: tracked_condition_ids
  filtered:
    type: sql
    primary_key: eventkey
    sql: "SELECT * FROM user_balances WHERE dynamic_table_check('tracked_wallets', lower(user)) AND dynamic_table_check('tracked_condition_ids', condition_id)"
sinks:
  out:
    type: blackhole
    from: filtered
"#;
        assert!(
            validate_no_orphan_nodes(config).is_ok(),
            "multiple dynamic tables consumed via dynamic_table_check() should not be flagged"
        );
    }

    #[test]
    fn dynamic_table_coexists_with_real_orphan() {
        let config = r#"
sources:
  src:
    type: kafka
    topic: test
  unused_src:
    type: kafka
    topic: unused
transforms:
  tracked:
    type: dynamic_table
    backend_type: Postgres
    backend_entity_name: tracked
  tx:
    type: sql
    primary_key: id
    sql: "SELECT * FROM src WHERE dynamic_table_check('tracked', id)"
sinks:
  out:
    type: print
    from: tx
"#;
        let result = validate_no_orphan_nodes(config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unused_src"),
            "should flag the real orphan source"
        );
        assert!(
            !err.contains("tracked"),
            "should NOT flag the dynamic table"
        );
    }

    #[test]
    fn dynamic_table_without_sql_not_flagged() {
        let config = r#"
sources:
  src:
    type: kafka
    topic: test
transforms:
  watched_addresses:
    type: dynamic_table
    backend_type: Postgres
    backend_entity_name: watched_addresses
  tx:
    type: sql
    primary_key: id
    sql: "SELECT * FROM src WHERE dynamic_table_check('watched_addresses', address)"
sinks:
  out:
    type: print
    from: tx
"#;
        assert!(
            validate_no_orphan_nodes(config).is_ok(),
            "dynamic_table without sql field should not be flagged as orphan"
        );
    }

    #[test]
    fn dynamic_table_still_counts_as_consumer_of_source() {
        let config = r#"
sources:
  token_transfers:
    type: dataset
    dataset_name: solana.token_transfers
    version: "1.0.0"
transforms:
  nest_mint_addresses:
    type: dynamic_table
    backend_type: InMemory
    backend_entity_name: nest_mint_addresses
    sql: "SELECT token_mint_address FROM token_transfers"
sinks:
  out:
    type: print
    from: nest_mint_addresses
"#;
        assert!(
            validate_no_orphan_nodes(config).is_ok(),
            "source consumed only by a dynamic table's SQL should not be flagged as orphan"
        );
    }

    #[test]
    fn recursive_cte_skips_validation() {
        let config = r#"
sources:
  src:
    type: kafka
    topic: test
transforms:
  recursive:
    type: sql
    primary_key: id
    sql: "WITH RECURSIVE cte AS (SELECT * FROM src UNION ALL SELECT * FROM cte) SELECT * FROM cte"
sinks:
  out:
    type: print
    from: recursive
"#;
        assert!(
            validate_no_orphan_nodes(config).is_ok(),
            "should skip validation when SQL uses recursive CTEs"
        );
    }

    #[test]
    fn job_mode_without_hybrid_source_returns_error() {
        let config = r#"
sources:
  src:
    type: kafka
    topic: test
transforms: {}
sinks:
  out:
    type: print
    from: src
"#;
        let topology = PipelineTopology::load_from_string(config).unwrap();
        let result = validate_job_mode(true, &topology);
        assert!(
            result.is_err(),
            "job_mode without hybrid source should fail"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("job_mode"),
            "error should mention job_mode: {err}"
        );
        assert!(
            err.contains("'src'"),
            "error should name the unsupported source: {err}"
        );
    }

    #[test]
    fn job_mode_with_hybrid_source_ok() {
        let config = r#"
sources:
  src:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: test_table
    unbounded_source:
      source_type: kafka
      topic: test_topic
    primary_key: id
transforms: {}
sinks:
  out:
    type: print
    from: src
"#;
        let topology = PipelineTopology::load_from_string(config).unwrap();
        assert!(
            validate_job_mode(true, &topology).is_ok(),
            "job_mode with all hybrid sources should succeed"
        );
    }

    #[test]
    fn non_job_mode_without_hybrid_source_ok() {
        let config = r#"
sources:
  src:
    type: kafka
    topic: test
transforms: {}
sinks:
  out:
    type: print
    from: src
"#;
        let topology = PipelineTopology::load_from_string(config).unwrap();
        assert!(
            validate_job_mode(false, &topology).is_ok(),
            "non-job_mode without hybrid source should succeed"
        );
    }

    #[test]
    fn job_mode_mixed_sources_rejects_non_hybrid() {
        let config = r#"
sources:
  kafka_src:
    type: kafka
    topic: test
  hybrid_src:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: test_table
    unbounded_source:
      source_type: kafka
      topic: test_topic
    primary_key: id
transforms: {}
sinks:
  out1:
    type: print
    from: kafka_src
  out2:
    type: print
    from: hybrid_src
"#;
        let topology = PipelineTopology::load_from_string(config).unwrap();
        let result = validate_job_mode(true, &topology);
        assert!(
            result.is_err(),
            "job_mode should fail when any source is not hybrid"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("'kafka_src'"),
            "error should name the unsupported source: {err}"
        );
        assert!(
            !err.contains("'hybrid_src'"),
            "error should not name the hybrid source: {err}"
        );
    }

    #[test]
    fn job_mode_with_file_source_ok() {
        let config = r#"
sources:
  src:
    type: file
    path: /tmp/events
    format: parquet
    primary_key: id
    mode:
      type: bounded
transforms: {}
sinks:
  out:
    type: print
    from: src
"#;
        let topology = PipelineTopology::load_from_string(config).unwrap();
        assert!(
            validate_job_mode(true, &topology).is_ok(),
            "job_mode with a bounded file source should succeed"
        );
    }

    #[test]
    fn job_mode_with_continuous_file_source_rejected() {
        let config = r#"
sources:
  src:
    type: file
    path: /tmp/events
    format: parquet
    mode:
      type: continuous
      poll_interval: 5s
transforms: {}
sinks:
  out:
    type: print
    from: src
"#;
        let topology = PipelineTopology::load_from_string(config).unwrap();
        let result = validate_job_mode(true, &topology);
        assert!(
            result.is_err(),
            "job_mode with a continuous file source should fail"
        );
        let err = result.unwrap_err().to_string();
        assert!(err.contains("'src'"), "error should name the source: {err}");
    }

    #[test]
    fn job_mode_mixed_hybrid_and_file_ok() {
        let config = r#"
sources:
  file_src:
    type: file
    path: /tmp/events
    format: json
    mode:
      type: bounded
  hybrid_src:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: test_table
    unbounded_source:
      source_type: kafka
      topic: test_topic
    primary_key: id
transforms: {}
sinks:
  out1:
    type: print
    from: file_src
  out2:
    type: print
    from: hybrid_src
"#;
        let topology = PipelineTopology::load_from_string(config).unwrap();
        assert!(
            validate_job_mode(true, &topology).is_ok(),
            "job_mode with hybrid + file sources should succeed"
        );
    }

    #[test]
    fn job_mode_multiple_hybrid_sources_ok() {
        let config = r#"
sources:
  src1:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: table1
    unbounded_source:
      source_type: kafka
      topic: topic1
    primary_key: id
  src2:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: table2
    unbounded_source:
      source_type: kafka
      topic: topic2
    primary_key: id
transforms: {}
sinks:
  out1:
    type: print
    from: src1
  out2:
    type: print
    from: src2
"#;
        let topology = PipelineTopology::load_from_string(config).unwrap();
        assert!(
            validate_job_mode(true, &topology).is_ok(),
            "job_mode with all hybrid sources should succeed"
        );
    }
}
