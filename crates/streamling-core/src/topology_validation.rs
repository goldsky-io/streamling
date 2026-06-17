//! Validation for pipeline topology.
//!
//! Pre-deserialization: catches orphan sources/transforms (nodes with no downstream
//! consumers) which would cause scan-sharing to wait forever and deadlock the pipeline.
//!
//! Post-deserialization: validates configuration constraints (e.g. job_mode requires
//! hybrid sources).

use crate::sql_parse::extract_table_references_from_sql;
use crate::streamling_user_err;
use crate::topology::{PipelineTopology, Source};
use std::collections::HashSet;

fn strip_sql_quotes(name: &str) -> &str {
    name.strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(name)
}

/// Returns the set of lowercased node names consumed by at least one transform
/// (via SQL table refs or `from`) or sink (`from`).
///
/// Returns `Err(())` if a SQL transform could not be parsed, signalling that
/// consumer analysis is unreliable. Callers decide how to handle this:
/// - `validate_no_orphan_nodes` skips validation entirely (preserves existing behavior).
/// - `find_terminal_nodes` returns all candidate nodes (conservative / safe for preview).
fn collect_consumed_nodes(
    transforms: &[(String, serde_yaml::Value)],
    sinks: Option<&serde_yaml::Mapping>,
) -> Result<HashSet<String>, ()> {
    let mut consumed: HashSet<String> = HashSet::new();

    for (_, transform_val) in transforms {
        let mapping = match transform_val.as_mapping() {
            Some(m) => m,
            None => continue,
        };
        if let Some(sql) = mapping.get("sql").and_then(|v| v.as_str()) {
            match extract_table_references_from_sql(sql) {
                Ok(table_names) => {
                    for name in table_names {
                        consumed.insert(strip_sql_quotes(&name).to_lowercase());
                    }
                }
                Err(_) => return Err(()),
            }
        } else if let Some(from) = mapping.get("from").and_then(|v| v.as_str()) {
            consumed.insert(from.to_lowercase());
        }
    }

    if let Some(m) = sinks {
        for (_, v) in m {
            if let Some(from) = v.get("from").and_then(|f| f.as_str()) {
                consumed.insert(from.to_lowercase());
            }
        }
    }

    Ok(consumed)
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

    // Candidate nodes: sources + non-dynamic-table transforms (lowercased for comparison).
    let mut candidates: HashSet<String> = HashSet::new();
    for name in &sources {
        candidates.insert(name.to_lowercase());
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
            candidates.insert(name.to_lowercase());
        }
    }

    let sinks_mapping = root.get("sinks").and_then(|v| v.as_mapping());
    let consumed = match collect_consumed_nodes(&transforms, sinks_mapping) {
        Ok(c) => c,
        Err(()) => {
            // SQL parser can't handle this query (recursive CTEs, JOINs, etc.).
            // Skip validation entirely — false positives would block valid pipelines.
            return Ok(());
        }
    };

    let mut orphans: Vec<String> = candidates
        .into_iter()
        .filter(|name| !consumed.contains(name))
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

/// Returns the names (original case) of sources/transforms that have no
/// downstream consumer. Used by the preview rewriter to attach blackhole sinks
/// when the submitted config has no sinks.
///
/// If a SQL transform cannot be parsed for table references, analysis is
/// unreliable, so every candidate node is returned (attaching a blackhole to
/// each is always valid and keeps data flowing through every block).
pub fn find_terminal_nodes(config: &str) -> crate::error::Result<Vec<String>> {
    let value: serde_yaml::Value =
        serde_yaml::from_str(config).map_err(|e| streamling_user_err!("invalid YAML: {}", e))?;

    let root = if let Some(def) = value.get("definition") {
        if def.is_mapping() { def } else { &value }
    } else {
        &value
    };

    // Candidate nodes: all sources + non-dynamic-table transforms, original case.
    let mut candidates: Vec<String> = Vec::new();
    if let Some(m) = root.get("sources").and_then(|v| v.as_mapping()) {
        for k in m.keys() {
            if let Some(name) = k.as_str() {
                candidates.push(name.to_string());
            }
        }
    }
    let transforms: Vec<(String, serde_yaml::Value)> = root
        .get("transforms")
        .and_then(|v| v.as_mapping())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| k.as_str().map(|s| (s.to_string(), v.clone())))
                .collect()
        })
        .unwrap_or_default();
    for (name, transform_val) in &transforms {
        let is_dynamic_table = transform_val
            .as_mapping()
            .and_then(|m| m.get("type"))
            .and_then(|v| v.as_str())
            == Some("dynamic_table");
        if !is_dynamic_table {
            candidates.push(name.clone());
        }
    }

    let sinks_mapping = root.get("sinks").and_then(|v| v.as_mapping());
    let consumed = match collect_consumed_nodes(&transforms, sinks_mapping) {
        Ok(c) => c,
        Err(()) => {
            // Unanalyzable: be conservative, treat every node as terminal.
            return Ok(candidates);
        }
    };

    Ok(candidates
        .into_iter()
        .filter(|name| !consumed.contains(&name.to_lowercase()))
        .collect())
}

/// Validates that job_mode is only enabled when every source supports it.
///
/// Job mode requires bounded sources that terminate on their own: hybrid sources
/// (they terminate after all bounded phases complete) and file sources (they read
/// their files once and complete). If any source is neither, we fail early with a
/// clear message listing the unsupported sources.
pub fn validate_job_mode(job_mode: bool, topology: &PipelineTopology) -> crate::error::Result<()> {
    if !job_mode {
        return Ok(());
    }

    let mut unsupported: Vec<&String> = topology
        .sources
        .iter()
        .filter(|(_, s)| !matches!(s, Source::hybrid(_) | Source::file(_)))
        .map(|(name, _)| name)
        .collect();

    if !unsupported.is_empty() {
        unsupported.sort();
        return Err(streamling_user_err!(
            "job_mode is enabled but the following source(s) do not support it: {}. \
             Job mode is only supported for pipelines where every source is a hybrid or \
             file (bounded) source.",
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
transforms: {}
sinks:
  out:
    type: print
    from: src
"#;
        let topology = PipelineTopology::load_from_string(config).unwrap();
        assert!(
            validate_job_mode(true, &topology).is_ok(),
            "job_mode with a file source should succeed"
        );
    }

    #[test]
    fn job_mode_mixed_hybrid_and_file_ok() {
        let config = r#"
sources:
  file_src:
    type: file
    path: /tmp/events
    format: json
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

#[cfg(test)]
mod terminal_node_tests {
    use super::find_terminal_nodes;

    #[test]
    fn single_sink_terminal_is_the_unconsumed_transform() {
        let yaml = r#"
sources:
  src:
    type: kafka
    topic: t
    primary_key: id
transforms:
  filt:
    type: sql
    sql: select * from src
    primary_key: id
"#;
        // `src` is consumed by `filt`; `filt` is consumed by nothing -> terminal.
        let mut terminals = find_terminal_nodes(yaml).unwrap();
        terminals.sort();
        assert_eq!(terminals, vec!["filt".to_string()]);
    }

    #[test]
    fn multiple_independent_terminals_both_returned() {
        let yaml = r#"
sources:
  a:
    type: kafka
    topic: t1
    primary_key: id
  b:
    type: kafka
    topic: t2
    primary_key: id
transforms: {}
"#;
        let mut terminals = find_terminal_nodes(yaml).unwrap();
        terminals.sort();
        assert_eq!(terminals, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn node_consumed_by_sink_is_not_terminal() {
        let yaml = r#"
sources:
  src:
    type: kafka
    topic: t
    primary_key: id
transforms: {}
sinks:
  out:
    type: print
    from: src
"#;
        let terminals = find_terminal_nodes(yaml).unwrap();
        assert!(terminals.is_empty());
    }

    #[test]
    fn unparseable_sql_returns_all_candidates() {
        // A JOIN causes extract_table_references_from_sql to return Err, so
        // find_terminal_nodes must conservatively return ALL candidate nodes.
        let yaml = r#"
sources:
  a:
    type: kafka
    topic: t1
    primary_key: id
  b:
    type: kafka
    topic: t2
    primary_key: id
transforms:
  joined:
    type: sql
    primary_key: id
    sql: "SELECT a.id FROM a JOIN b ON a.id = b.id"
"#;
        let mut terminals = find_terminal_nodes(yaml).unwrap();
        terminals.sort();
        // All three candidates (a, b, joined) should be returned when SQL is unanalyzable.
        assert_eq!(
            terminals,
            vec!["a".to_string(), "b".to_string(), "joined".to_string()]
        );
    }
}
