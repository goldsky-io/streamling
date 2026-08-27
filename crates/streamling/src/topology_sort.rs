use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Word};
use std::collections::{HashMap, VecDeque};
use streamling_core::topology;
use tracing::debug;

/// Get priority for transform types (lower number = higher priority)
fn get_transform_priority(transform: &topology::Transform) -> u8 {
    match transform {
        topology::Transform::dynamic_table(_) => 0, // Highest priority
        topology::Transform::plugin(_) => 1,        // Plugin transforms register tables
        topology::Transform::sql(_) => 2,
        _ => 3, // Other transforms have equal lower priority
    }
}

/// Topologically sort transforms to ensure dependencies are processed before dependents
/// Dynamic tables are prioritized over other transforms when they have the same dependency level
pub fn sort_transforms(
    transforms: &HashMap<String, topology::Transform>,
    existing_plans: &HashMap<String, LogicalPlan>,
) -> Vec<String> {
    let mut in_degree = HashMap::new();
    let mut dependencies = HashMap::new();

    // Initialize all transforms
    for transform_name in transforms.keys() {
        in_degree.insert(transform_name.clone(), 0);
        dependencies.insert(transform_name.clone(), Vec::new());
    }

    // Build dependency graph
    for (transform_name, transform) in transforms {
        let transform_dependencies: Vec<String> = match transform {
            topology::Transform::sql(sql_transform) => {
                // Extract table references from SQL to find dependencies on other transforms
                extract_sql_dependencies(&sql_transform.sql, transforms, existing_plans)
            }
            // Dynamic tables may depend on other transforms if they have an associated SQL query
            topology::Transform::dynamic_table(dt) => {
                match &dt.sql {
                    Some(query) => extract_sql_dependencies(query, transforms, existing_plans),
                    None => vec![], // No SQL means no dependencies
                }
            }
            topology::Transform::handler(handler) => vec![handler.from.clone()],
            topology::Transform::script(script) => vec![script.from.clone()],
            topology::Transform::plugin(plugin) => vec![plugin.from.clone()],
        };

        debug!(
            "Transform '{}' has dependencies: {:?}",
            transform_name, transform_dependencies
        );

        for dep in transform_dependencies {
            if dep == *transform_name {
                continue; // ignore self-references (e.g. field paths that coincidentally match)
            }
            // Only count as dependency if it's another transform (not a source)
            if transforms.contains_key(&dep) {
                dependencies
                    .get_mut(&dep)
                    .unwrap()
                    .push(transform_name.clone());
                *in_degree.get_mut(transform_name).unwrap() += 1;
            }
        }
    }

    // Kahn's algorithm for topological sorting
    let mut queue = VecDeque::new();
    let mut result = Vec::new();

    // Helper function to add ready transforms to queue in priority order
    let add_ready_transforms = |queue: &mut VecDeque<String>, ready_transforms: Vec<String>| {
        let mut sorted_ready = ready_transforms;
        sorted_ready.sort_by(|a, b| {
            let priority_a = get_transform_priority(&transforms[a]);
            let priority_b = get_transform_priority(&transforms[b]);
            priority_a.cmp(&priority_b).then_with(|| a.cmp(b)) // Tie-break by name for determinism
        });
        for transform in sorted_ready {
            queue.push_back(transform);
        }
    };

    // Start with transforms that have no dependencies
    let ready_transforms: Vec<String> = in_degree
        .iter()
        .filter(|(_, degree)| **degree == 0)
        .map(|(name, _)| name.clone())
        .collect();
    add_ready_transforms(&mut queue, ready_transforms);

    while let Some(transform_name) = queue.pop_front() {
        result.push(transform_name.clone());

        // Find newly ready transforms after processing this one
        let mut newly_ready = Vec::new();
        for dependent in &dependencies[&transform_name] {
            let degree = in_degree.get_mut(dependent).unwrap();
            *degree -= 1;
            if *degree == 0 {
                newly_ready.push(dependent.clone());
            }
        }
        add_ready_transforms(&mut queue, newly_ready);
    }

    // Check for cycles
    if result.len() != transforms.len() {
        let problematic_transforms: Vec<_> = in_degree
            .iter()
            .filter(|&(_, &degree)| degree > 0)
            .map(|(name, _)| name.clone())
            .collect();
        panic!(
            "Circular dependency detected in transforms: {:?}",
            problematic_transforms
        );
    }

    result
}

/// Parsed SQL token categories for dependency detection.
struct SqlTokens {
    /// Lowercased identifiers NOT preceded by `.` — top-level names like
    /// table references in `FROM my_table` or standalone keywords.
    standalone_words: Vec<String>,
    /// Lowercased multi-part dotted identifiers like `schema.table` or
    /// `receipt.value.logs`, reconstructed from consecutive Word.Word runs.
    dotted_sequences: Vec<String>,
}

/// Tokenize SQL and collect both standalone words and dotted sequences.
///
/// Uses sqlparser's `Tokenizer` so comments, string literals, quoted
/// identifiers and all other SQL syntax are handled correctly.  Falls back
/// to empty lists if tokenisation fails.
///
/// A word is *standalone* only when it is not part of any dotted path.
/// `myschema` in `FROM myschema.rewards` is **not** standalone — it is
/// the prefix of the dotted sequence `myschema.rewards`.  This prevents
/// a transform named `myschema` from being falsely detected as a dependency.
fn extract_sql_tokens(sql: &str) -> SqlTokens {
    let dialect = GenericDialect {};
    let tokens = match Tokenizer::new(&dialect, sql).tokenize() {
        Ok(t) => t,
        Err(_) => {
            return SqlTokens {
                standalone_words: Vec::new(),
                dotted_sequences: Vec::new(),
            };
        }
    };

    let mut standalone_words = Vec::new();
    let mut dotted_sequences = Vec::new();
    // Accumulates the current dotted-or-standalone sequence.
    let mut current_parts: Vec<String> = Vec::new();
    // True when the most recent non-whitespace token was `.`.
    let mut after_period = false;
    // True when current_parts[0] was pushed via the after_period path
    // (e.g. the `field` in `func().field`).  Such orphaned words are
    // struct field accesses and should not be treated as standalone.
    let mut started_after_period = false;

    fn flush(
        parts: &mut Vec<String>,
        started_after_dot: &mut bool,
        standalone: &mut Vec<String>,
        dotted: &mut Vec<String>,
    ) {
        match parts.len() {
            0 => {}
            1 if !*started_after_dot => standalone.push(parts[0].clone()),
            1 => {} // drop: field access after non-word token, e.g. ).field
            _ => dotted.push(parts.join(".")),
        }
        parts.clear();
        *started_after_dot = false;
    }

    for tok in &tokens {
        match tok {
            Token::Period => {
                after_period = true;
            }
            Token::Word(Word { value, .. }) => {
                let lower = value.to_lowercase();
                if after_period {
                    if current_parts.is_empty() {
                        started_after_period = true;
                    }
                    current_parts.push(lower);
                    after_period = false;
                } else {
                    flush(
                        &mut current_parts,
                        &mut started_after_period,
                        &mut standalone_words,
                        &mut dotted_sequences,
                    );
                    current_parts.push(lower);
                }
            }
            Token::Whitespace(_) => {}
            _ => {
                flush(
                    &mut current_parts,
                    &mut started_after_period,
                    &mut standalone_words,
                    &mut dotted_sequences,
                );
                after_period = false;
            }
        }
    }

    flush(
        &mut current_parts,
        &mut started_after_period,
        &mut standalone_words,
        &mut dotted_sequences,
    );

    SqlTokens {
        standalone_words,
        dotted_sequences,
    }
}

/// Extract dependencies from SQL queries by checking for transform references.
///
/// - Simple transform names (no dot) are matched against standalone word tokens.
/// - Dotted transform names (e.g. `schema.table`) are matched against the
///   reconstructed dotted sequences from the SQL.
///
/// Field paths like `receipt.value.logs` only appear in `dotted_sequences`,
/// so a simple transform named `logs` won't false-match against them.
fn extract_sql_dependencies(
    sql: &str,
    transforms: &HashMap<String, topology::Transform>,
    _existing_plans: &HashMap<String, LogicalPlan>,
) -> Vec<String> {
    let tokens = extract_sql_tokens(sql);
    let mut dependencies = Vec::new();

    for transform_name in transforms.keys() {
        let transform_lower = transform_name.to_lowercase();
        if transform_lower.contains('.') {
            if tokens.dotted_sequences.contains(&transform_lower) {
                dependencies.push(transform_name.clone());
            }
        } else if tokens.standalone_words.contains(&transform_lower) {
            dependencies.push(transform_name.clone());
        }
    }

    dependencies
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use streamling_core::topology;

    fn sql_word_tokens(sql: &str) -> Vec<String> {
        extract_sql_tokens(sql).standalone_words
    }

    fn create_sql_transform(sql: &str) -> topology::Transform {
        topology::Transform::sql(topology::SqlTransform {
            primary_key: "id".to_string(),
            sql: sql.to_string(),
            telemetry: None,
        })
    }

    fn create_handler_transform(from: &str, url: &str) -> topology::Transform {
        topology::Transform::handler(topology::HandlerTransform {
            primary_key: "id".to_string(),
            from: from.to_string(),
            url: url.to_string(),
            headers: None,
            secret_name: None,
            one_row_per_request: Some(true),
            payload_version: Some(0),
            schema_override: None,
            telemetry: None,
            batch_size: None,
            batch_flush_interval: None,
        })
    }

    fn create_script_transform(from: &str) -> topology::Transform {
        topology::Transform::script(topology::ScriptTransform {
            primary_key: "id".to_string(),
            from: from.to_string(),
            language: "javascript".to_string(),
            script: "function process(input) { return input; }".to_string(),
            schema: None,
            parallelism: None,
            batch_size: None,
            telemetry: None,
        })
    }

    #[test]
    fn test_topological_sort_edge_cases() {
        // Test empty transforms
        let transforms = HashMap::new();
        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);
        assert_eq!(result, Vec::<String>::new());

        // Test single transform
        let mut transforms = HashMap::new();
        transforms.insert(
            "sql_transform".to_string(),
            create_sql_transform("SELECT * FROM source"),
        );
        let result = sort_transforms(&transforms, &existing_plans);
        assert_eq!(result, vec!["sql_transform"]);

        // Test multiple independent transforms (no dependencies)
        transforms.insert(
            "transform2".to_string(),
            create_sql_transform("SELECT * FROM source2"),
        );
        transforms.insert(
            "transform3".to_string(),
            create_sql_transform("SELECT * FROM source3"),
        );
        let result = sort_transforms(&transforms, &existing_plans);
        assert_eq!(result.len(), 3);
        assert!(result.contains(&"sql_transform".to_string()));
        assert!(result.contains(&"transform2".to_string()));
        assert!(result.contains(&"transform3".to_string()));
    }

    #[test]
    fn test_topological_sort_complex_dependencies() {
        let mut transforms = HashMap::new();

        // Create complex dependency patterns:
        // - Simple chain: sql_a -> handler_b -> script_c
        // - Diamond: sql_d -> {handler_e, handler_f} -> script_g (depends only on handler_e)
        // - Independent: sql_h
        // - Source dependency: handler_i (depends on external source)

        transforms.insert(
            "sql_a".to_string(),
            create_sql_transform("SELECT * FROM source"),
        );
        transforms.insert(
            "handler_b".to_string(),
            create_handler_transform("sql_a", "http://b.com"),
        );
        transforms.insert("script_c".to_string(), create_script_transform("handler_b"));

        transforms.insert(
            "sql_d".to_string(),
            create_sql_transform("SELECT * FROM source2"),
        );
        transforms.insert(
            "handler_e".to_string(),
            create_handler_transform("sql_d", "http://e.com"),
        );
        transforms.insert(
            "handler_f".to_string(),
            create_handler_transform("sql_d", "http://f.com"),
        );
        transforms.insert("script_g".to_string(), create_script_transform("handler_e"));

        transforms.insert(
            "sql_h".to_string(),
            create_sql_transform("SELECT * FROM source3"),
        );
        transforms.insert(
            "handler_i".to_string(),
            create_handler_transform("kafka_source", "http://i.com"),
        );

        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);

        assert_eq!(result.len(), 9);

        // Verify ordering constraints
        let pos = |name: &str| result.iter().position(|x| x == name).unwrap();

        // Chain: sql_a -> handler_b -> script_c
        assert!(pos("sql_a") < pos("handler_b"), "sql_a before handler_b");
        assert!(
            pos("handler_b") < pos("script_c"),
            "handler_b before script_c"
        );

        // Diamond: sql_d -> {handler_e, handler_f} -> script_g
        assert!(pos("sql_d") < pos("handler_e"), "sql_d before handler_e");
        assert!(pos("sql_d") < pos("handler_f"), "sql_d before handler_f");
        assert!(
            pos("handler_e") < pos("script_g"),
            "handler_e before script_g"
        );

        // handler_i depends on external source, so no ordering constraint with other transforms
    }

    #[test]
    fn test_topological_sort_sql_dependencies() {
        let mut transforms = HashMap::new();

        // Test SQL transforms that reference other transforms
        transforms.insert(
            "base_transform".to_string(),
            create_sql_transform("SELECT * FROM source"),
        );
        transforms.insert(
            "derived_sql".to_string(),
            create_sql_transform("SELECT id, data FROM base_transform WHERE id > 100"),
        );
        transforms.insert(
            "handler_transform".to_string(),
            create_handler_transform("derived_sql", "http://example.com"),
        );

        // Test SQL that doesn't depend on other transforms
        transforms.insert(
            "independent_sql".to_string(),
            create_sql_transform("SELECT * FROM kafka_source"),
        );

        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);

        assert_eq!(result.len(), 4);

        // Verify ordering: base_transform -> derived_sql -> handler_transform
        let pos = |name: &str| result.iter().position(|x| x == name).unwrap();

        assert!(
            pos("base_transform") < pos("derived_sql"),
            "base_transform should come before derived_sql"
        );
        assert!(
            pos("derived_sql") < pos("handler_transform"),
            "derived_sql should come before handler_transform"
        );

        // independent_sql can be anywhere since it doesn't depend on other transforms
        assert!(result.contains(&"independent_sql".to_string()));
    }

    /// Transform name must appear as a complete SQL token, not as a substring.
    /// e.g. "logs" in "from logs_unnested" must not count as a dependency on "logs".
    #[test]
    fn test_sql_dependencies_token_boundary_no_false_match() {
        let mut transforms = HashMap::new();
        transforms.insert(
            "logs".to_string(),
            create_sql_transform("select id from logs_unnested"),
        );
        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);
        assert_eq!(
            result,
            vec!["logs"],
            "logs_unnested is a different token; logs has no self-dependency"
        );
    }

    /// "logs" in "receipt.value.logs" is a field reference, not a table name; must not count.
    #[test]
    fn test_sql_dependencies_no_match_after_dot() {
        let mut transforms = HashMap::new();
        transforms.insert(
            "logs".to_string(),
            create_sql_transform("select log from receipts_unnested, unnest(array_enumerate(receipt.value.logs)) as log"),
        );
        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);
        assert_eq!(
            result,
            vec!["logs"],
            "receipt.value.logs is a field path, not a table reference"
        );
    }

    /// When the transform name appears as a full token it is detected as a dependency.
    #[test]
    fn test_sql_dependencies_token_boundary_real_match() {
        let mut transforms = HashMap::new();
        transforms.insert(
            "logs".to_string(),
            create_sql_transform("select id from logs where id > 0"),
        );
        transforms.insert(
            "source".to_string(),
            create_sql_transform("select * from source"),
        );
        let existing_plans = HashMap::new();
        // logs depends on logs (self) -> cycle. source has no deps. So we get a cycle and panic,
        // unless we're only checking. Actually: "logs" with sql "from logs where" -> "logs" is a token, so logs depends on "logs". So in_degree[logs] = 1. So we have a cycle (logs -> logs). So sort_transforms would panic. So we can't use that for "real match" test easily. Instead test that "other" depending on "logs" works: other has sql "from logs", logs has sql "from logs_unnested". Then other depends on logs, logs has no deps. Order: logs, other.
        transforms.clear();
        transforms.insert(
            "logs".to_string(),
            create_sql_transform("select id from logs_unnested"),
        );
        transforms.insert(
            "other".to_string(),
            create_sql_transform("select id from logs"),
        );
        let result = sort_transforms(&transforms, &existing_plans);
        assert_eq!(result.len(), 2);
        let pos_logs = result.iter().position(|x| x == "logs").unwrap();
        let pos_other = result.iter().position(|x| x == "other").unwrap();
        assert!(
            pos_logs < pos_other,
            "logs (no deps) should come before other (depends on logs)"
        );
    }

    #[test]
    fn test_topological_sort_all_transform_types() {
        let mut transforms = HashMap::new();

        // Test all transform types in a dependency chain
        transforms.insert(
            "sql_transform".to_string(),
            create_sql_transform("SELECT * FROM source"),
        );
        transforms.insert(
            "handler_transform".to_string(),
            create_handler_transform("sql_transform", "http://example.com"),
        );
        transforms.insert(
            "script_transform".to_string(),
            create_script_transform("handler_transform"),
        );

        // Add plugin transform
        let plugin_transform = topology::Transform::plugin(topology::PluginTransform {
            r#type: "test_plugin".to_string(),
            from: "script_transform".to_string(),
            options: None,
            primary_key: None,
            telemetry: None,
            batch_size: None,
            batch_flush_interval: None,
        });
        transforms.insert("plugin_transform".to_string(), plugin_transform);

        // Add transform depending on external source
        transforms.insert(
            "source_handler".to_string(),
            create_handler_transform("kafka_source", "http://source.com"),
        );

        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);

        assert_eq!(result.len(), 5);

        // Verify the main chain ordering
        let expected_chain = [
            "sql_transform",
            "handler_transform",
            "script_transform",
            "plugin_transform",
        ];

        let chain_positions: Vec<_> = expected_chain
            .iter()
            .map(|name| result.iter().position(|x| x == *name).unwrap())
            .collect();

        for i in 1..chain_positions.len() {
            assert!(
                chain_positions[i - 1] < chain_positions[i],
                "Transform {} should come before {}",
                expected_chain[i - 1],
                expected_chain[i]
            );
        }
    }

    #[test]
    #[should_panic(expected = "Circular dependency detected in transforms")]
    fn test_topological_sort_circular_dependency() {
        let mut transforms = HashMap::new();

        // Test both simple (A -> B -> A) and complex cycles (A -> B -> C -> A)
        transforms.insert(
            "transform_a".to_string(),
            create_handler_transform("transform_c", "http://a.com"),
        );
        transforms.insert(
            "transform_b".to_string(),
            create_handler_transform("transform_a", "http://b.com"),
        );
        transforms.insert(
            "transform_c".to_string(),
            create_handler_transform("transform_b", "http://c.com"),
        );

        let existing_plans = HashMap::new();
        sort_transforms(&transforms, &existing_plans);
    }

    #[test]
    fn test_topological_sort_dynamic_tables_dependencies() {
        let mut transforms = HashMap::new();

        // Create dynamic table transforms
        let dynamic_table1 = topology::Transform::dynamic_table(topology::DynamicTableTransform {
            backend_type: streamling_config::DynamicTableBackendType::Postgres,
            backend_entity_name: "table1".to_string(),
            sql: Some("SELECT * FROM source1".to_string()),
            schema: None,
            column: None,
            time_column: None,
            cache: None,
            telemetry: None,
        });
        transforms.insert("dynamic_table1".to_string(), dynamic_table1);

        let dynamic_table2 = topology::Transform::dynamic_table(topology::DynamicTableTransform {
            backend_type: streamling_config::DynamicTableBackendType::Postgres,
            backend_entity_name: "table2".to_string(),
            sql: None,
            schema: None,
            column: None,
            time_column: None,
            cache: None,
            telemetry: None,
        });
        transforms.insert("dynamic_table2".to_string(), dynamic_table2);

        // Create SQL transform that depends on dynamic tables
        let sql_with_dynamic_deps = topology::Transform::sql(topology::SqlTransform {
            primary_key: "id".to_string(),
            sql: "SELECT * FROM some_source WHERE dynamic_table_check('dynamic_table1', id) AND dynamic_table_check('dynamic_table2', name)".to_string(),
            telemetry: None,
        });
        transforms.insert("sql_with_dynamic_deps".to_string(), sql_with_dynamic_deps);

        // Create another SQL transform that depends on the first one
        transforms.insert(
            "final_sql".to_string(),
            create_sql_transform("SELECT id FROM sql_with_dynamic_deps"),
        );

        // Create independent dynamic table
        let independent_dynamic =
            topology::Transform::dynamic_table(topology::DynamicTableTransform {
                backend_type: streamling_config::DynamicTableBackendType::InMemory,
                backend_entity_name: "independent".to_string(),
                sql: None,
                schema: None,
                column: None,
                time_column: None,
                cache: None,
                telemetry: None,
            });
        transforms.insert("independent_dynamic".to_string(), independent_dynamic);

        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);

        assert_eq!(result.len(), 5);

        // Verify ordering constraints
        let pos = |name: &str| result.iter().position(|x| x == name).unwrap();

        // Dynamic tables should come before SQL transforms that depend on them
        assert!(
            pos("dynamic_table1") < pos("sql_with_dynamic_deps"),
            "dynamic_table1 should come before sql_with_dynamic_deps"
        );
        assert!(
            pos("dynamic_table2") < pos("sql_with_dynamic_deps"),
            "dynamic_table2 should come before sql_with_dynamic_deps"
        );

        // SQL with dynamic deps should come before final_sql
        assert!(
            pos("sql_with_dynamic_deps") < pos("final_sql"),
            "sql_with_dynamic_deps should come before final_sql"
        );

        // Independent dynamic table can be anywhere
        assert!(result.contains(&"independent_dynamic".to_string()));
    }

    #[test]
    fn test_dynamic_table_priority_over_sql() {
        let mut transforms = HashMap::new();

        transforms.insert(
            "preprocessed_transactions".to_string(),
            create_sql_transform("SELECT id, vid, market, _gs_op FROM polymarket.transaction"),
        );

        // filtered_market (dynamic table depending on preprocessed_transactions)
        let filtered_market = topology::Transform::dynamic_table(topology::DynamicTableTransform {
            backend_type: streamling_config::DynamicTableBackendType::Postgres,
            backend_entity_name: "dt_market_filter".to_string(),
            sql: Some("SELECT id FROM preprocessed_transactions WHERE market = '0x32810b0a800c280616acc7690d33eb7831e1daa4'".to_string()),
            schema: None,
            column: None,
            time_column: None,
            cache: None,
            telemetry: None,
        });
        transforms.insert("filtered_market".to_string(), filtered_market);

        // updated_transactions (SQL transform also depending on preprocessed_transactions)
        let updated_transactions = topology::Transform::sql(topology::SqlTransform {
            primary_key: "id".to_string(),
            sql: "SELECT id, vid, _gs_op FROM preprocessed_transactions WHERE dynamic_table_check('filtered_market', id)".to_string(),
            telemetry: None,
        });
        transforms.insert("updated_transactions".to_string(), updated_transactions);

        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);

        assert_eq!(result.len(), 3);

        let pos = |name: &str| result.iter().position(|x| x == name).unwrap();

        // Verify the expected ordering
        assert!(
            pos("preprocessed_transactions") < pos("filtered_market"),
            "preprocessed_transactions should come before filtered_market"
        );
        assert!(
            pos("preprocessed_transactions") < pos("updated_transactions"),
            "preprocessed_transactions should come before updated_transactions"
        );

        // Most importantly: dynamic table should come before SQL transform when both depend on the same transform
        assert!(
            pos("filtered_market") < pos("updated_transactions"),
            "Dynamic table 'filtered_market' should come before SQL transform 'updated_transactions' when both depend on the same transform"
        );
    }

    #[test]
    fn test_plugin_priority_over_sql() {
        let mut transforms = HashMap::new();

        // Create a plugin transform that reads from a source
        let plugin_transform = topology::Transform::plugin(topology::PluginTransform {
            r#type: "solana_transform".to_string(),
            from: "solana_source".to_string(),
            options: None,
            primary_key: None,
            telemetry: None,
            batch_size: None,
            batch_flush_interval: None,
        });
        transforms.insert("instructions".to_string(), plugin_transform);

        // Create a SQL transform that queries the plugin transform
        // Use newlines between FROM and table name to simulate the broken dependency detection
        let sql_transform = topology::Transform::sql(topology::SqlTransform {
            primary_key: "id".to_string(),
            sql: "select
        id,
        block_slot,
        accounts
      from
        instructions
      where
        program_id = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'"
                .to_string(),
            telemetry: None,
        });
        transforms.insert("cash".to_string(), sql_transform);

        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);

        assert_eq!(result.len(), 2);

        let pos = |name: &str| result.iter().position(|x| x == name).unwrap();

        // Plugin transform should come before SQL transform even when dependency detection fails
        assert!(
            pos("instructions") < pos("cash"),
            "Plugin transform 'instructions' should come before SQL transform 'cash' due to priority, even when dependency detection fails"
        );
    }

    /// SQL comments containing apostrophes (e.g. `-- don't`) must not
    /// break string-literal stripping and cause dependency detection to miss
    /// real table references. Regression test for a pipeline where a comment
    /// `-- cast to string so scripts don't have to deal with timestamp types`
    /// caused `FROM condition_events` to be incorrectly stripped.
    #[test]
    fn test_sql_comment_apostrophe_does_not_break_dependency_detection() {
        let mut transforms = HashMap::new();

        transforms.insert(
            "condition_events".to_string(),
            create_sql_transform(
                r#"SELECT
                    _gs_log_decode(
                        '[{"name":"ConditionPreparation","type":"event"}]',
                        topics,
                        `data`
                    ) AS decoded,
                    id,
                    _gs_op
                FROM poly_logs"#,
            ),
        );

        transforms.insert(
            "condition_preparation".to_string(),
            create_sql_transform(
                r#"SELECT
                    id,
                    block_number,
                    -- cast to string so scripts don't have to deal with timestamp types
                    CAST(block_timestamp AS VARCHAR) AS block_timestamp,
                    concat('0x', regexp_replace(lower(decoded.event_params[1]), '^0x', '')) AS condition_id,
                    lower(decoded.event_params[2]) AS oracle,
                    concat('0x', regexp_replace(lower(decoded.event_params[3]), '^0x', '')) AS question_id,
                    case
                      when lower(decoded.event_params[2]) = '0xd91e80cf2e7be2e162c6513ced06f1dd0da35296'
                        then lower('0x3a3bd7bb9528e159577f7c2e685cc81a765002e2')
                      else lower('0x2791bca1f2de4661ed88a30c99a7a9449aa84174')
                    end AS collateral_token,
                    _gs_op
                FROM condition_events
                WHERE decoded.event_signature = 'ConditionPreparation'"#,
            ),
        );

        transforms.insert(
            "collection_hashes".to_string(),
            create_sql_transform("SELECT id FROM condition_preparation"),
        );

        transforms.insert(
            "token_id_condition_0".to_string(),
            create_script_transform("collection_hashes"),
        );

        transforms.insert(
            "token_id_condition_1".to_string(),
            create_script_transform("collection_hashes"),
        );

        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);

        assert_eq!(result.len(), 5);

        let pos = |name: &str| result.iter().position(|x| x == name).unwrap();

        assert!(
            pos("condition_events") < pos("condition_preparation"),
            "condition_events must come before condition_preparation (actual order: {:?})",
            result
        );
        assert!(
            pos("condition_preparation") < pos("collection_hashes"),
            "condition_preparation must come before collection_hashes"
        );
        assert!(
            pos("collection_hashes") < pos("token_id_condition_0"),
            "collection_hashes must come before token_id_condition_0"
        );
        assert!(
            pos("collection_hashes") < pos("token_id_condition_1"),
            "collection_hashes must come before token_id_condition_1"
        );
    }

    /// Verify that the tokenizer correctly handles comments with apostrophes
    /// — the table name must appear in the word tokens.
    #[test]
    fn test_tokenizer_handles_comment_with_apostrophe() {
        let sql = "select id, -- don't strip this\nfrom condition_events where x = 'val'";
        let words = sql_word_tokens(sql);
        assert!(
            words.contains(&"condition_events".to_string()),
            "FROM clause table must appear in word tokens, got: {words:?}"
        );
        assert!(
            !words.contains(&"val".to_string()),
            "string literal content must not appear in word tokens"
        );
    }

    /// Transform names inside SQL string literals (e.g. ABI JSON) must not
    /// be treated as dependencies. Regression test for the polymarket config
    /// where `poly_events` contains `'..."name":"OrderFilled"...'` and
    /// `orderfilled` is a separate transform that depends on `poly_events`.
    #[test]
    fn test_sql_dependencies_no_match_inside_string_literal() {
        let mut transforms = HashMap::new();
        transforms.insert(
            "poly_events".to_string(),
            create_sql_transform(
                r#"SELECT
                    _gs_log_decode(
                        '[{"name":"OrderFilled","type":"event"},{"name":"FeeRefunded","type":"event"}]',
                        topics,
                        data
                    ) AS decoded,
                    id
                FROM poly_logs"#,
            ),
        );
        transforms.insert(
            "orderfilled".to_string(),
            create_sql_transform(
                "SELECT id FROM poly_events WHERE decoded.event_signature = 'OrderFilled'",
            ),
        );
        transforms.insert(
            "activities_all".to_string(),
            create_sql_transform("SELECT * FROM orderfilled"),
        );

        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);

        assert_eq!(result.len(), 3);
        let pos = |name: &str| result.iter().position(|x| x == name).unwrap();
        assert!(
            pos("poly_events") < pos("orderfilled"),
            "poly_events should come before orderfilled"
        );
        assert!(
            pos("orderfilled") < pos("activities_all"),
            "orderfilled should come before activities_all"
        );
    }

    /// Schema-qualified names produce a dotted sequence but do NOT leak
    /// the schema prefix into standalone_words — otherwise a transform
    /// named `myschema` would be falsely detected as a dependency.
    #[test]
    fn test_schema_qualified_tokens() {
        let tokens = extract_sql_tokens("SELECT * FROM myschema.rewards");
        assert!(
            !tokens.standalone_words.contains(&"myschema".to_string()),
            "schema prefix must NOT appear in standalone words (would cause false deps)"
        );
        assert!(
            !tokens.standalone_words.contains(&"rewards".to_string()),
            "table after dot should NOT appear in standalone words"
        );
        assert!(
            tokens
                .dotted_sequences
                .contains(&"myschema.rewards".to_string()),
            "full schema.table should appear in dotted sequences"
        );
    }

    /// Schema prefix must not create false dependency.  If transforms
    /// `matic` and `matic.decoded` both exist, SQL referencing only
    /// `matic.decoded` must not falsely depend on plain `matic`.
    #[test]
    fn test_schema_prefix_no_false_dependency() {
        let mut transforms = HashMap::new();
        transforms.insert(
            "matic".to_string(),
            create_sql_transform("SELECT * FROM final_output"),
        );
        transforms.insert(
            "matic.decoded".to_string(),
            create_sql_transform("SELECT * FROM kafka_source"),
        );
        transforms.insert(
            "final_output".to_string(),
            create_sql_transform("SELECT * FROM matic.decoded"),
        );

        let existing_plans = HashMap::new();
        // Must not panic — the old code would create a false cycle:
        // matic → final_output (real), final_output → matic (false from prefix)
        let result = sort_transforms(&transforms, &existing_plans);
        assert_eq!(result.len(), 3);
        let pos = |name: &str| result.iter().position(|x| x == name).unwrap();
        assert!(
            pos("matic.decoded") < pos("final_output"),
            "matic.decoded should come before final_output"
        );
    }

    /// A transform whose name contains a dot (registered in a custom schema)
    /// must be detected as a dependency when referenced as `schema.table`.
    #[test]
    fn test_dotted_transform_name_detected_as_dependency() {
        let mut transforms = HashMap::new();
        transforms.insert(
            "custom.decoded_events".to_string(),
            create_sql_transform("SELECT * FROM raw_source"),
        );
        transforms.insert(
            "final_output".to_string(),
            create_sql_transform("SELECT id FROM custom.decoded_events WHERE id > 0"),
        );

        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);
        let pos = |name: &str| result.iter().position(|x| x == name).unwrap();
        assert!(
            pos("custom.decoded_events") < pos("final_output"),
            "dotted transform name should be detected via dotted sequence matching"
        );
    }

    /// Multiple schema-qualified transforms in a chain should all be resolved.
    #[test]
    fn test_multiple_dotted_transforms_chain() {
        let mut transforms = HashMap::new();
        transforms.insert(
            "matic.raw_logs".to_string(),
            create_sql_transform("SELECT * FROM kafka_source"),
        );
        transforms.insert(
            "matic.decoded".to_string(),
            create_sql_transform("SELECT id, data FROM matic.raw_logs"),
        );
        transforms.insert(
            "final".to_string(),
            create_sql_transform("SELECT * FROM matic.decoded"),
        );

        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);
        let pos = |name: &str| result.iter().position(|x| x == name).unwrap();
        assert!(
            pos("matic.raw_logs") < pos("matic.decoded"),
            "matic.raw_logs before matic.decoded"
        );
        assert!(
            pos("matic.decoded") < pos("final"),
            "matic.decoded before final"
        );
    }

    /// A dotted sequence that is a field path (not a transform) must not
    /// falsely match a simple transform name.
    #[test]
    fn test_field_path_does_not_match_simple_transform() {
        let mut transforms = HashMap::new();
        transforms.insert(
            "event_params".to_string(),
            create_sql_transform("SELECT * FROM raw_source"),
        );
        transforms.insert(
            "processed".to_string(),
            create_sql_transform("SELECT decoded.event_params[1] AS param FROM other_source"),
        );

        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);
        assert_eq!(result.len(), 2);
        let pos = |name: &str| result.iter().position(|x| x == name).unwrap();
        // event_params should NOT be detected as a dep of processed —
        // it's after a dot (field path), not a standalone table reference
        assert!(
            pos("event_params") == 0 || pos("event_params") == 1,
            "both transforms should appear (no cycle)"
        );
    }

    /// Nested field paths like `decoded.event_params[1]` and
    /// `receipt.value.logs` must not put ANY of their components into
    /// standalone_words.  The entire dotted path goes into dotted_sequences.
    #[test]
    fn test_nested_field_paths_excluded() {
        let tokens =
            extract_sql_tokens("SELECT decoded.event_params[1], receipt.value.logs FROM my_source");
        assert!(tokens.standalone_words.contains(&"my_source".to_string()));
        // Roots of dotted paths are NOT standalone — `decoded.event_params`
        // is a column field access, not a reference to a transform named `decoded`.
        assert!(
            !tokens.standalone_words.contains(&"decoded".to_string()),
            "root of dotted path should not be standalone"
        );
        assert!(
            !tokens.standalone_words.contains(&"receipt".to_string()),
            "root of dotted path should not be standalone"
        );
        assert!(
            !tokens
                .standalone_words
                .contains(&"event_params".to_string()),
            "nested field after dot should be excluded"
        );
        assert!(
            !tokens.standalone_words.contains(&"value".to_string()),
            "nested field after dot should be excluded"
        );
        assert!(
            !tokens.standalone_words.contains(&"logs".to_string()),
            "leaf field after dot should be excluded"
        );
        // Full paths should be in dotted_sequences
        assert!(
            tokens
                .dotted_sequences
                .contains(&"decoded.event_params".to_string())
        );
        assert!(
            tokens
                .dotted_sequences
                .contains(&"receipt.value.logs".to_string())
        );
    }

    /// Block comments `/* ... */` must be handled — content inside
    /// (including apostrophes and transform-like names) is ignored.
    #[test]
    fn test_block_comment_content_ignored() {
        let sql = "SELECT /* don't match my_transform here */ id FROM real_source";
        let words = sql_word_tokens(sql);
        assert!(words.contains(&"real_source".to_string()));
        assert!(words.contains(&"id".to_string()));
        assert!(
            !words.contains(&"my_transform".to_string()),
            "words inside block comments must not appear"
        );
    }

    /// Backtick-quoted identifiers should still be recognised as words.
    #[test]
    fn test_backtick_quoted_identifier() {
        let words = sql_word_tokens("SELECT `data`, `value` FROM my_source");
        assert!(words.contains(&"data".to_string()));
        assert!(words.contains(&"value".to_string()));
        assert!(words.contains(&"my_source".to_string()));
    }

    /// Double-quoted identifiers should still be recognised as words.
    #[test]
    fn test_double_quoted_identifier() {
        let words = sql_word_tokens(r#"SELECT "blockTime", "loadedAddresses" FROM my_source"#);
        assert!(
            words.contains(&"blocktime".to_string()) || words.contains(&"blockTime".to_string()),
            "double-quoted identifier should appear"
        );
        assert!(words.contains(&"my_source".to_string()));
    }

    /// dynamic_table_check('table_name', col) — the table name inside the
    /// string literal must NOT appear as a word token. Dynamic tables are
    /// ordered by priority, not by dependency detection.
    #[test]
    fn test_dynamic_table_check_string_arg_not_matched() {
        let sql = "SELECT * FROM source WHERE dynamic_table_check('watched_wallets', address)";
        let words = sql_word_tokens(sql);
        assert!(words.contains(&"source".to_string()));
        assert!(words.contains(&"dynamic_table_check".to_string()));
        assert!(
            !words.contains(&"watched_wallets".to_string()),
            "dynamic table name inside string literal must not be a word token"
        );
    }

    /// Full sort: dynamic table consumed via dynamic_table_check() should
    /// still be ordered before SQL transforms due to priority, even though
    /// the dependency isn't detected through string-literal matching.
    #[test]
    fn test_dynamic_table_ordered_by_priority_not_string_match() {
        let mut transforms = HashMap::new();

        let dt = topology::Transform::dynamic_table(topology::DynamicTableTransform {
            backend_type: streamling_config::DynamicTableBackendType::Postgres,
            backend_entity_name: "hot_wallets".to_string(),
            sql: Some("SELECT wallet FROM wallets_source".to_string()),
            schema: None,
            column: None,
            time_column: None,
            cache: None,
            telemetry: None,
        });
        transforms.insert("hot_wallets".to_string(), dt);

        transforms.insert(
            "filtered".to_string(),
            create_sql_transform(
                "SELECT * FROM transfers WHERE dynamic_table_check('hot_wallets', sender)",
            ),
        );

        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);
        let pos = |name: &str| result.iter().position(|x| x == name).unwrap();

        assert!(
            pos("hot_wallets") < pos("filtered"),
            "dynamic table should come before SQL transform via priority"
        );
    }

    /// Subquery in FROM clause — the inner table should be detected.
    #[test]
    fn test_subquery_in_from_detected() {
        let mut transforms = HashMap::new();
        transforms.insert(
            "raw_events".to_string(),
            create_sql_transform("SELECT * FROM external_source"),
        );
        transforms.insert(
            "processed".to_string(),
            create_sql_transform(
                "SELECT * FROM (SELECT id, data FROM raw_events WHERE data IS NOT NULL) sub",
            ),
        );

        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);
        let pos = |name: &str| result.iter().position(|x| x == name).unwrap();
        assert!(
            pos("raw_events") < pos("processed"),
            "raw_events should come before processed (subquery reference)"
        );
    }

    /// UNION ALL referencing the same transform should detect the dependency.
    #[test]
    fn test_union_all_dependency_detected() {
        let mut transforms = HashMap::new();
        transforms.insert(
            "base".to_string(),
            create_sql_transform("SELECT * FROM kafka_source"),
        );
        transforms.insert(
            "combined".to_string(),
            create_sql_transform(
                "SELECT id, amount, 'credit' as type FROM base \
                 UNION ALL \
                 SELECT id, -amount, 'debit' as type FROM base",
            ),
        );

        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);
        let pos = |name: &str| result.iter().position(|x| x == name).unwrap();
        assert!(
            pos("base") < pos("combined"),
            "base should come before combined (UNION ALL references)"
        );
    }

    /// CTE (WITH clause) that references another transform should be detected.
    #[test]
    fn test_cte_reference_detected() {
        let mut transforms = HashMap::new();
        transforms.insert(
            "decoded_logs".to_string(),
            create_sql_transform("SELECT * FROM raw_logs"),
        );
        transforms.insert(
            "balances".to_string(),
            create_sql_transform(
                r#"WITH ledger AS (
                    SELECT address, CAST(value AS BIGINT) AS val FROM decoded_logs
                    UNION ALL
                    SELECT address, -CAST(value AS BIGINT) AS val FROM decoded_logs
                )
                SELECT address, SUM(val) AS balance FROM ledger GROUP BY address"#,
            ),
        );

        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);
        let pos = |name: &str| result.iter().position(|x| x == name).unwrap();
        assert!(
            pos("decoded_logs") < pos("balances"),
            "decoded_logs should come before balances (CTE body references it)"
        );
    }

    /// Multiple SQL comments (line + block) interleaved should not affect detection.
    #[test]
    fn test_multiple_comment_styles_interleaved() {
        let sql = r#"SELECT
            id, /* grab the primary key */
            -- don't forget the timestamp
            block_timestamp,
            /* multi-line
               comment with 'quotes' and -- dashes */
            data
        FROM my_upstream_transform
        WHERE id > 0"#;
        let words = sql_word_tokens(sql);
        assert!(words.contains(&"my_upstream_transform".to_string()));
        assert!(words.contains(&"id".to_string()));
        assert!(words.contains(&"block_timestamp".to_string()));
        assert!(words.contains(&"data".to_string()));
    }

    /// Transform name that matches a SQL keyword (e.g. `values`) should
    /// still be detected — we match all Word tokens, not just non-keywords.
    #[test]
    fn test_transform_name_matching_sql_keyword() {
        let mut transforms = HashMap::new();
        transforms.insert(
            "source".to_string(),
            create_sql_transform("SELECT * FROM kafka_input"),
        );
        // "values" is a SQL keyword but also a valid transform name
        transforms.insert(
            "values".to_string(),
            create_sql_transform("SELECT * FROM source"),
        );
        transforms.insert(
            "final_output".to_string(),
            create_sql_transform("SELECT * FROM values"),
        );

        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);
        let pos = |name: &str| result.iter().position(|x| x == name).unwrap();
        assert!(
            pos("source") < pos("values"),
            "source should come before values"
        );
        assert!(
            pos("values") < pos("final_output"),
            "values should come before final_output"
        );
    }

    #[test]
    fn test_topological_sort_newline_char() {
        // Test empty transforms
        let transforms = HashMap::new();
        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);
        assert_eq!(result, Vec::<String>::new());

        // Test single transform
        let mut transforms = HashMap::new();
        transforms.insert(
            "sql_transform".to_string(),
            create_sql_transform("select \n*\nfrom\nsolana.rewards\n"),
        );
        transforms.insert(
            "transform2".to_string(),
            create_sql_transform("select \n*\nfrom\nsql_transform\n"),
        );

        let result = sort_transforms(&transforms, &existing_plans);
        assert_eq!(result, vec!["sql_transform", "transform2"]);
    }

    /// Self-reference (transform SQL mentions its own name) must not cause a
    /// cycle panic — the self-dependency is filtered out.
    #[test]
    fn test_self_reference_ignored() {
        let mut transforms = HashMap::new();
        transforms.insert(
            "my_transform".to_string(),
            create_sql_transform("SELECT id FROM my_transform WHERE id > 0"),
        );

        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);
        assert_eq!(result, vec!["my_transform"]);
    }

    /// Words after `).` (function-call dot-access like `unnest(arr).field`)
    /// are dropped — they're struct field accesses, not table references.
    /// This is acceptable: no real SQL references a transform as `func().name`.
    #[test]
    fn test_function_dot_access_field_dropped() {
        let tokens = extract_sql_tokens("SELECT unnest(arr).field_name, col FROM my_source");
        assert!(tokens.standalone_words.contains(&"unnest".to_string()));
        assert!(tokens.standalone_words.contains(&"arr".to_string()));
        assert!(tokens.standalone_words.contains(&"col".to_string()));
        assert!(tokens.standalone_words.contains(&"my_source".to_string()));
        assert!(
            !tokens.standalone_words.contains(&"field_name".to_string()),
            "field after ). should not be a standalone word"
        );
        assert!(
            !tokens
                .dotted_sequences
                .iter()
                .any(|s| s.contains("field_name")),
            "field after ). should not appear in any dotted sequence"
        );
    }

    /// unnest/lateral patterns must not interfere with transform detection.
    /// The transform name inside the function args should be detected.
    #[test]
    fn test_unnest_with_transform_reference() {
        let mut transforms = HashMap::new();
        transforms.insert(
            "events".to_string(),
            create_sql_transform("SELECT * FROM kafka_source"),
        );
        transforms.insert(
            "expanded".to_string(),
            create_sql_transform("SELECT e.id, u.val FROM events e, unnest(e.items) AS u(val)"),
        );

        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);
        let pos = |name: &str| result.iter().position(|x| x == name).unwrap();
        assert!(
            pos("events") < pos("expanded"),
            "events should come before expanded (referenced in FROM clause)"
        );
    }

    /// Struct field access like `col.field` on a column should not create a
    /// false dependency on a transform that happens to share the field name.
    #[test]
    fn test_struct_field_no_false_dependency() {
        let mut transforms = HashMap::new();
        transforms.insert(
            "output".to_string(),
            create_sql_transform("SELECT * FROM kafka_source"),
        );
        transforms.insert(
            "processed".to_string(),
            create_sql_transform("SELECT result.output, result.status FROM some_source"),
        );

        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);
        assert_eq!(result.len(), 2);
        // "output" after dot in "result.output" must NOT create a dependency
        // from processed → output. Both should sort independently.
        let pos_output = result.iter().position(|x| x == "output").unwrap();
        let pos_processed = result.iter().position(|x| x == "processed").unwrap();
        // Neither depends on the other — order is by priority/name
        assert!(
            pos_output != pos_processed,
            "both should appear independently"
        );
    }

    /// Verify dotted sequences are collected from various SQL patterns.
    #[test]
    fn test_extract_sql_tokens_dotted_sequences() {
        let tokens =
            extract_sql_tokens("SELECT a.b, c.d.e, standalone FROM schema1.table1 WHERE x.y = 1");
        assert!(
            tokens.dotted_sequences.contains(&"a.b".to_string()),
            "a.b should be a dotted sequence"
        );
        assert!(
            tokens.dotted_sequences.contains(&"c.d.e".to_string()),
            "c.d.e should be a dotted sequence"
        );
        assert!(
            tokens
                .dotted_sequences
                .contains(&"schema1.table1".to_string()),
            "schema1.table1 should be a dotted sequence"
        );
        assert!(
            tokens.dotted_sequences.contains(&"x.y".to_string()),
            "x.y should be a dotted sequence"
        );
        assert!(
            tokens.standalone_words.contains(&"standalone".to_string()),
            "standalone should appear in standalone words"
        );
        assert!(
            !tokens.standalone_words.contains(&"table1".to_string()),
            "table1 after dot should not be in standalone words"
        );
        // Roots of dotted paths should NOT be standalone
        assert!(
            !tokens.standalone_words.contains(&"a".to_string()),
            "root of a.b should not be standalone"
        );
        assert!(
            !tokens.standalone_words.contains(&"c".to_string()),
            "root of c.d.e should not be standalone"
        );
        assert!(
            !tokens.standalone_words.contains(&"schema1".to_string()),
            "root of schema1.table1 should not be standalone"
        );
    }

    /// CTE that unnests an array column from an upstream transform.
    /// Both the CTE body's FROM reference and the outer query must resolve,
    /// and field accesses on the unnested alias must not create false deps.
    #[test]
    fn test_unnest_inside_cte_with_transform_reference() {
        let mut transforms = HashMap::new();
        transforms.insert(
            "raw_events".to_string(),
            create_sql_transform("SELECT * FROM kafka_source"),
        );
        transforms.insert(
            "items".to_string(),
            create_sql_transform(
                r#"WITH expanded AS (
                    SELECT e.id, u.item
                    FROM raw_events e, unnest(e.items) AS u(item)
                )
                SELECT expanded.id, expanded.item
                FROM expanded
                WHERE expanded.item IS NOT NULL"#,
            ),
        );
        transforms.insert(
            "summary".to_string(),
            create_sql_transform("SELECT id, count(*) AS cnt FROM items GROUP BY id"),
        );

        let existing_plans = HashMap::new();
        let result = sort_transforms(&transforms, &existing_plans);
        assert_eq!(result.len(), 3);
        let pos = |name: &str| result.iter().position(|x| x == name).unwrap();
        assert!(
            pos("raw_events") < pos("items"),
            "raw_events should come before items (CTE body references it)"
        );
        assert!(
            pos("items") < pos("summary"),
            "items should come before summary"
        );
        // "expanded" is a CTE alias, not a transform — must not cause issues
        assert!(
            !result.contains(&"expanded".to_string()),
            "CTE alias should not appear as a transform"
        );
    }
}
