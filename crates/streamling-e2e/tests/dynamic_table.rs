//! PostgreSQL dynamic-table cache end-to-end tests.

use serde::Serialize;
use std::time::{Duration, Instant};
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

#[derive(Debug, Clone, Serialize)]
struct TestRecord {
    id: String,
    data: String,
}

const TEST_SCHEMA: &str = r#"{
    "type": "record",
    "name": "DynamicTableTestRecord",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "data", "type": "string"}
    ]
}"#;
const CACHE_WRITE_LOCK_QUERY: &str = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))";

fn record(id: &str) -> TestRecord {
    TestRecord {
        id: id.to_string(),
        data: format!("data_for_{id}"),
    }
}

#[derive(Debug, Clone, Serialize)]
struct ArrayMembershipRecord {
    id: String,
    accounts: Vec<String>,
}

const ARRAY_MEMBERSHIP_SCHEMA: &str = r#"{
    "type": "record",
    "name": "ArrayMembershipRecord",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "accounts", "type": {"type": "array", "items": "string"}}
    ]
}"#;

fn pipeline(
    ctx: &TestContext,
    backing_table: &str,
    output_table: &str,
    drain_source: bool,
) -> String {
    let drain = drain_source.then_some(
        r#"
  source_drain:
    type: blackhole
    from: input
"#,
    );

    format!(
        r#"
sources:
  input:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms:
  membership:
    type: dynamic_table
    backend_type: Postgres
    backend_entity_name: {backing_table}
    schema: public
    column: value
    time_column: updated_at
  matched:
    type: sql
    sql: "SELECT id, data FROM input WHERE dynamic_table_check('membership', id)"
    primary_key: id

sinks:
  matched_output:
    type: postgres
    from: matched
    table: {output_table}
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
    batch_flush_interval: 100ms
{drain}
"#,
        topic = ctx.kafka_topic,
        drain = drain.unwrap_or_default(),
    )
}

fn append_pipeline(ctx: &TestContext, backing_table: &str, cached: bool) -> String {
    let time_column = cached.then_some("    time_column: updated_at\n");

    format!(
        r#"
sources:
  input:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms:
  membership_writer:
    type: dynamic_table
    backend_type: Postgres
    backend_entity_name: {backing_table}
    schema: public
    column: value
{time_column}    sql: "SELECT id FROM input"

sinks:
  source_drain:
    type: blackhole
    from: input
"#,
        topic = ctx.kafka_topic,
        time_column = time_column.unwrap_or_default(),
    )
}

fn cached_postgres_opts(ctx: &TestContext, record_limit: u64, timeout: Duration) -> PipelineOpts {
    PipelineOpts::new()
        .record_limit(record_limit)
        .timeout(timeout)
        .env(
            "STREAMLING__DYNAMIC_TABLE_BACKEND__POSTGRES__HOST",
            &ctx.postgres.host,
        )
        .env(
            "STREAMLING__DYNAMIC_TABLE_BACKEND__POSTGRES__PORT",
            ctx.postgres.port.to_string(),
        )
        .env(
            "STREAMLING__DYNAMIC_TABLE_BACKEND__POSTGRES__DB",
            &ctx.pg_database,
        )
        .env(
            "STREAMLING__DYNAMIC_TABLE_BACKEND__POSTGRES__USER",
            &ctx.postgres.user,
        )
        .env(
            "STREAMLING__DYNAMIC_TABLE_BACKEND__POSTGRES__PASSWORD",
            &ctx.postgres.password,
        )
        .env(
            "STREAMLING__DYNAMIC_TABLE_BACKEND__POSTGRES__SSLMODE",
            "disable",
        )
        .env(
            "STREAMLING__DYNAMIC_TABLE_BACKEND__POSTGRES__CACHE_ENABLED",
            "true",
        )
        .env("STREAMLING__RECORD_BATCH_SIZE", "1")
}

async fn wait_for_count(ctx: &TestContext, table: &str, expected: i64) {
    let query = format!("SELECT COUNT(*) FROM public.{table}");
    wait_for_query_count(ctx, &query, expected, table).await;
}

async fn wait_for_query_count(ctx: &TestContext, query: &str, expected: i64, description: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut last_count = None;

    while Instant::now() < deadline {
        if let Ok(count) = ctx.postgres.count(query).await {
            last_count = Some(count);
            if count >= expected {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    panic!("timed out waiting for {description} to reach {expected}; last count: {last_count:?}");
}

async fn output_ids(ctx: &TestContext, table: &str) -> Vec<String> {
    let query = format!("SELECT id FROM public.{table} ORDER BY id");
    ctx.postgres
        .query::<(String,)>(&query)
        .await
        .expect("failed to read dynamic-table output")
        .into_iter()
        .map(|row| row.0)
        .collect()
}

async fn lock_cached_table(transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>, table: &str) {
    sqlx::query(CACHE_WRITE_LOCK_QUERY)
        .bind(format!("public.{table}"))
        .execute(&mut **transaction)
        .await
        .expect("failed to acquire dynamic-table writer lock");
}

#[tokio::test]
async fn test_postgres_dynamic_table_cache_refreshes_with_serialized_writers() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("failed to create test context");
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("failed to register schema");
    ctx.postgres
        .execute(
            r#"
            CREATE TABLE public.cached_members (
                value TEXT PRIMARY KEY,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CLOCK_TIMESTAMP()
            )
            "#,
        )
        .await
        .expect("failed to create dynamic table");

    // Start this transaction first, then make it commit second. Assigning NOW() here would
    // leave MAX(updated_at) unchanged and make the second insert invisible to the cache.
    let mut older_transaction = ctx
        .postgres
        .pool()
        .begin()
        .await
        .expect("failed to begin older transaction");
    sqlx::query_scalar::<_, String>("SELECT transaction_timestamp()::TEXT")
        .fetch_one(&mut *older_transaction)
        .await
        .expect("failed to establish the older transaction timestamp");

    let mut first_writer = ctx
        .postgres
        .pool()
        .begin()
        .await
        .expect("failed to begin first writer");
    lock_cached_table(&mut first_writer, "cached_members").await;
    sqlx::query(
        "INSERT INTO public.cached_members (value, updated_at) \
         VALUES ('commits_first', clock_timestamp())",
    )
    .execute(&mut *first_writer)
    .await
    .expect("failed to insert first committed row");

    let lock_available =
        sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))")
            .bind("public.cached_members")
            .fetch_one(&mut *older_transaction)
            .await
            .expect("failed to check the writer lock");
    assert!(!lock_available, "concurrent writers must be serialized");
    first_writer
        .commit()
        .await
        .expect("failed to commit first writer");

    ctx.kafka
        .produce_avro_records(&[record("commits_first")])
        .await
        .expect("failed to produce first record");

    let pipeline = pipeline(&ctx, "cached_members", "out_of_order_output", true);
    let pipeline_fut = ctx.run_pipeline_with_opts(
        &pipeline,
        cached_postgres_opts(&ctx, 2, Duration::from_secs(45)),
    );
    let mutation_fut = async {
        wait_for_count(&ctx, "out_of_order_output", 1).await;
        lock_cached_table(&mut older_transaction, "cached_members").await;
        sqlx::query(
            "INSERT INTO public.cached_members (value, updated_at) \
             VALUES ('commits_last', clock_timestamp())",
        )
        .execute(&mut *older_transaction)
        .await
        .expect("failed to insert in older transaction");
        older_transaction
            .commit()
            .await
            .expect("failed to commit older transaction");
        ctx.kafka
            .produce_avro_records(&[record("commits_last")])
            .await
            .expect("failed to produce late-commit record");
    };

    let (status, ()) = tokio::join!(pipeline_fut, mutation_fut);
    let status = status.expect("pipeline failed");
    assert!(status.success(), "pipeline should exit successfully");
    assert_eq!(
        output_ids(&ctx, "out_of_order_output").await,
        ["commits_first", "commits_last"]
    );
}

#[tokio::test]
async fn test_postgres_dynamic_table_cache_missing_time_column_fails_fast() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("failed to create test context");
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("failed to register schema");
    ctx.postgres
        .execute("CREATE TABLE public.cached_members (value TEXT PRIMARY KEY)")
        .await
        .expect("failed to create dynamic table");
    ctx.kafka
        .produce_avro_records(&[record("member")])
        .await
        .expect("failed to produce record");

    let output = ctx
        .run_pipeline_raw(
            &pipeline(&ctx, "cached_members", "missing_column_output", false),
            cached_postgres_opts(&ctx, 1, Duration::from_secs(10)),
        )
        .await
        .expect("invalid cache columns must fail instead of timing out");

    assert!(!output.status.success(), "invalid cache column should fail");
    let logs = format!("{}\n{}", output.stdout, output.stderr);
    assert!(
        logs.contains("updated_at") && logs.contains("cache"),
        "error should identify the invalid cache column:\n{logs}"
    );
}

#[tokio::test]
async fn test_postgres_dynamic_table_cache_non_orderable_time_column_fails_fast() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("failed to create test context");
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("failed to register schema");
    ctx.postgres
        .execute(
            "CREATE TABLE public.cached_members (\
             value TEXT PRIMARY KEY, updated_at BOOLEAN NOT NULL DEFAULT FALSE)",
        )
        .await
        .expect("failed to create dynamic table");
    ctx.kafka
        .produce_avro_records(&[record("member")])
        .await
        .expect("failed to produce record");

    let output = ctx
        .run_pipeline_raw(
            &pipeline(&ctx, "cached_members", "invalid_column_output", false),
            cached_postgres_opts(&ctx, 1, Duration::from_secs(10)),
        )
        .await
        .expect("invalid cache columns must fail instead of timing out");

    assert!(!output.status.success(), "invalid cache column should fail");
    let logs = format!("{}\n{}", output.stdout, output.stderr);
    assert!(
        logs.contains("updated_at") && logs.contains("cache"),
        "error should identify the invalid cache column:\n{logs}"
    );
}

#[tokio::test]
async fn test_postgres_dynamic_table_cache_appends_to_legacy_table_after_writer_lock() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("failed to create test context");
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("failed to register schema");
    ctx.postgres
        .execute(
            r#"
            CREATE TABLE public.cached_members (
                value TEXT PRIMARY KEY,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
            "#,
        )
        .await
        .expect("failed to create legacy dynamic table");
    ctx.postgres
        .execute(
            "CREATE TABLE public.lock_release_marker (\
             released_at TIMESTAMP WITH TIME ZONE NOT NULL)",
        )
        .await
        .expect("failed to create lock release marker");
    ctx.postgres
        .execute("INSERT INTO public.cached_members (value) VALUES ('already_deployed')")
        .await
        .expect("failed to seed legacy dynamic table");

    let mut lock_holder = ctx
        .postgres
        .pool()
        .begin()
        .await
        .expect("failed to begin lock-holder transaction");
    lock_cached_table(&mut lock_holder, "cached_members").await;

    ctx.kafka
        .produce_avro_records(&[record("new_member")])
        .await
        .expect("failed to produce dynamic-table value");

    let pipeline = append_pipeline(&ctx, "cached_members", true);
    let pipeline_fut = ctx.run_pipeline_with_opts(
        &pipeline,
        cached_postgres_opts(&ctx, 1, Duration::from_secs(45)),
    );
    let release_lock_fut = async {
        wait_for_query_count(
            &ctx,
            r#"
            SELECT COUNT(*)
            FROM pg_locks
            WHERE locktype = 'advisory'
              AND database = (
                  SELECT oid FROM pg_database WHERE datname = current_database()
              )
              AND NOT granted
            "#,
            1,
            "a cached writer waiting for the legacy table lock",
        )
        .await;
        sqlx::query("INSERT INTO public.lock_release_marker VALUES (clock_timestamp())")
            .execute(&mut *lock_holder)
            .await
            .expect("failed to record lock release time");
        sqlx::query("SELECT pg_sleep(0.02)")
            .execute(&mut *lock_holder)
            .await
            .expect("failed to separate lock and write timestamps");
        lock_holder
            .commit()
            .await
            .expect("failed to release dynamic-table writer lock");
    };

    let (status, ()) = tokio::join!(pipeline_fut, release_lock_fut);
    let status = status.expect("pipeline failed");
    assert!(status.success(), "pipeline should exit successfully");

    let rows = ctx
        .postgres
        .query::<(String, bool)>(
            r#"
            SELECT value, updated_at > released_at
            FROM public.cached_members
            CROSS JOIN public.lock_release_marker
            WHERE value = 'new_member'
            "#,
        )
        .await
        .expect("failed to verify the legacy-table write timestamp");
    assert_eq!(rows, [("new_member".to_string(), true)]);

    assert_eq!(
        ctx.postgres
            .query::<(String,)>("SELECT value FROM public.cached_members ORDER BY value")
            .await
            .expect("failed to verify legacy table values"),
        [
            ("already_deployed".to_string(),),
            ("new_member".to_string(),)
        ]
    );
    let schema = ctx
        .postgres
        .query::<(String, Option<String>)>(
            r#"
            SELECT is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'cached_members'
              AND column_name = 'updated_at'
            "#,
        )
        .await
        .expect("failed to inspect legacy table schema");
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].0, "YES");
    assert!(
        schema[0]
            .1
            .as_deref()
            .is_some_and(|default| default.eq_ignore_ascii_case("now()")),
        "legacy default should remain unchanged: {schema:?}"
    );
}

#[tokio::test]
async fn test_postgres_dynamic_table_cache_flag_preserves_uncached_legacy_table() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("failed to create test context");
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("failed to register schema");
    ctx.postgres
        .execute("CREATE TABLE public.legacy_members (value TEXT PRIMARY KEY)")
        .await
        .expect("failed to create legacy dynamic table");
    ctx.kafka
        .produce_avro_records(&[record("legacy_member")])
        .await
        .expect("failed to produce dynamic-table value");

    let status = ctx
        .run_pipeline_with_opts(
            &append_pipeline(&ctx, "legacy_members", false),
            cached_postgres_opts(&ctx, 1, Duration::from_secs(30)),
        )
        .await
        .expect("pipeline failed");
    assert!(status.success(), "pipeline should exit successfully");
    assert_eq!(
        ctx.postgres
            .query::<(String,)>("SELECT value FROM public.legacy_members")
            .await
            .expect("failed to read uncached legacy table"),
        [("legacy_member".to_string(),)]
    );
}

#[tokio::test]
async fn test_postgres_dynamic_table_cache_loads_and_refreshes_after_append() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("failed to create test context");
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("failed to register schema");
    ctx.postgres
        .execute(
            r#"
            CREATE TABLE public.cached_members (
                value TEXT PRIMARY KEY,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
            "#,
        )
        .await
        .expect("failed to create dynamic table");
    let mut seed_transaction = ctx
        .postgres
        .pool()
        .begin()
        .await
        .expect("failed to begin seed transaction");
    lock_cached_table(&mut seed_transaction, "cached_members").await;
    // Cross a cache-load page boundary during initial cache population.
    sqlx::query(
        "INSERT INTO public.cached_members (value, updated_at) \
         SELECT value, clock_timestamp() \
         FROM ( \
             SELECT 'seed_' || value::TEXT AS value \
             FROM generate_series(1, 1000) AS seed(value) \
             UNION ALL SELECT 'initial' \
         ) seeded",
    )
    .execute(&mut *seed_transaction)
    .await
    .expect("failed to seed dynamic table");
    seed_transaction
        .commit()
        .await
        .expect("failed to commit seed transaction");
    ctx.kafka
        .produce_avro_records(&[record("initial")])
        .await
        .expect("failed to produce initial record");

    let pipeline = pipeline(&ctx, "cached_members", "cache_refresh_output", true);
    let pipeline_fut = ctx.run_pipeline_with_opts(
        &pipeline,
        cached_postgres_opts(&ctx, 3, Duration::from_secs(45)),
    );
    let append_fut = async {
        wait_for_count(&ctx, "cache_refresh_output", 1).await;
        let mut transaction = ctx
            .postgres
            .pool()
            .begin()
            .await
            .expect("failed to begin append transaction");
        lock_cached_table(&mut transaction, "cached_members").await;
        sqlx::query(
            "INSERT INTO public.cached_members (value, updated_at) \
             VALUES ('appended_member', clock_timestamp())",
        )
        .execute(&mut *transaction)
        .await
        .expect("failed to append dynamic table value and its time column");
        transaction
            .commit()
            .await
            .expect("failed to commit dynamic-table append");
        ctx.kafka
            .produce_avro_records(&[record("appended_member"), record("not_a_member")])
            .await
            .expect("failed to produce refreshed records");
    };

    let (status, ()) = tokio::join!(pipeline_fut, append_fut);
    let status = status.expect("pipeline failed");
    assert!(status.success(), "pipeline should exit successfully");
    assert_eq!(
        output_ids(&ctx, "cache_refresh_output").await,
        ["appended_member", "initial"]
    );
}

#[tokio::test]
async fn test_postgres_dynamic_table_creates_time_column_index() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("failed to create test context");
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("failed to register schema");
    ctx.kafka
        .produce_avro_records(&[record("indexed_member")])
        .await
        .expect("failed to produce dynamic-table value");

    let status = ctx
        .run_pipeline_with_opts(
            &append_pipeline(&ctx, "indexed_members", true),
            cached_postgres_opts(&ctx, 1, Duration::from_secs(30)),
        )
        .await
        .expect("pipeline failed");
    assert!(status.success(), "pipeline should exit successfully");

    let index_count = ctx
        .postgres
        .query::<(i64,)>(
            r#"
            SELECT COUNT(*)::bigint FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'indexed_members'
              AND indexdef LIKE '%updated_at%'
            "#,
        )
        .await
        .expect("failed to query pg_indexes");
    assert_eq!(
        index_count,
        [(1,)],
        "fresh Streamling-created table should index the time column"
    );
}

/// Verify that deduplication in the uncached `contains()` path produces identical
/// results for duplicate values. This exercises the exact code path the dedup
/// changes: with MAX_BATCH_SIZE=3 and 6 total values (3 unique), without dedup
/// the query would split into 2 batches; with dedup it fits in 1. If dedup
/// dropped or mis-mapped a value, a duplicate of an existing value would be
/// incorrectly filtered out.
#[tokio::test]
async fn test_uncached_dedup_identical_results_for_duplicate_values() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("failed to create test context");
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("failed to register schema");

    // Pre-seed the backing table so the positive filter has known membership.
    ctx.postgres
        .execute("CREATE TABLE public.dedup_membership (value TEXT PRIMARY KEY)")
        .await
        .expect("failed to create backing table");
    ctx.postgres
        .execute("INSERT INTO public.dedup_membership (value) VALUES ('group_a'), ('group_c')")
        .await
        .expect("failed to seed backing table");

    // Produce records with duplicate values in the column being checked.
    // group_a ×3, group_b ×2, group_c ×1 → 6 values, 3 unique.
    // With MAX_BATCH_SIZE=3 the dedup collapses 6 values into 3, turning a
    // 2-batch query into a 1-batch query.
    ctx.kafka
        .produce_avro_records(&[
            TestRecord {
                id: "r1".into(),
                data: "group_a".into(),
            },
            TestRecord {
                id: "r2".into(),
                data: "group_b".into(),
            },
            TestRecord {
                id: "r3".into(),
                data: "group_a".into(),
            },
            TestRecord {
                id: "r4".into(),
                data: "group_a".into(),
            },
            TestRecord {
                id: "r5".into(),
                data: "group_c".into(),
            },
            TestRecord {
                id: "r6".into(),
                data: "group_b".into(),
            },
        ])
        .await
        .expect("failed to produce records");

    // source_drain (blackhole) consumes all 6 source records directly, triggering
    // shutdown via num_records_before_stop. Without it, only 4 records reach the
    // postgres sink (2 filtered out by dynamic_table_check) and the pipeline hangs.
    let pipeline_yaml = format!(
        r#"
sources:
  input:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms:
  membership:
    type: dynamic_table
    backend_type: Postgres
    backend_entity_name: dedup_membership
    schema: public
    column: value
  matched:
    type: sql
    sql: "SELECT id, data FROM input WHERE dynamic_table_check('membership', data)"
    primary_key: id

sinks:
  matched_output:
    type: postgres
    from: matched
    table: dedup_output
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
    batch_flush_interval: 100ms
  source_drain:
    type: blackhole
    from: input
"#,
        topic = ctx.kafka_topic,
    );

    let opts = PipelineOpts::new()
        .record_limit(6)
        .timeout(Duration::from_secs(60))
        .env(
            "STREAMLING__DYNAMIC_TABLE_BACKEND__POSTGRES__HOST",
            &ctx.postgres.host,
        )
        .env(
            "STREAMLING__DYNAMIC_TABLE_BACKEND__POSTGRES__PORT",
            ctx.postgres.port.to_string(),
        )
        .env(
            "STREAMLING__DYNAMIC_TABLE_BACKEND__POSTGRES__DB",
            &ctx.pg_database,
        )
        .env(
            "STREAMLING__DYNAMIC_TABLE_BACKEND__POSTGRES__USER",
            &ctx.postgres.user,
        )
        .env(
            "STREAMLING__DYNAMIC_TABLE_BACKEND__POSTGRES__PASSWORD",
            &ctx.postgres.password,
        )
        .env(
            "STREAMLING__DYNAMIC_TABLE_BACKEND__POSTGRES__SSLMODE",
            "disable",
        )
        // Force a small batch size so 6 values would split into 2 queries
        // without dedup, but 3 unique values fit in 1 query with dedup.
        .env("STREAMLING__DYNAMIC_TABLE_BACKEND__MAX_BATCH_SIZE", "3");

    let status = ctx
        .run_pipeline_with_opts(&pipeline_yaml, opts)
        .await
        .expect("pipeline failed");
    assert!(status.success(), "pipeline should exit successfully");

    // group_a (×3) and group_c (×1) exist in the table → 4 records pass.
    // group_b (×2) does NOT exist → 2 records filtered.
    // If dedup dropped a duplicate, a group_a record would be missing.
    let output = output_ids(&ctx, "dedup_output").await;
    assert_eq!(
        output,
        vec!["r1", "r3", "r4", "r5"],
        "all duplicates of existing values must pass identically"
    );
}

/// Any-match `text[]` overload end-to-end: a source `accounts` array column is
/// checked against a dynamic table, and a row passes iff ANY element is a
/// member. Also covers the empty-array-is-false case.
#[tokio::test]
async fn test_dynamic_table_check_text_array_any_match() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("failed to create test context");
    ctx.kafka
        .register_schema(ARRAY_MEMBERSHIP_SCHEMA)
        .await
        .expect("failed to register schema");

    ctx.postgres
        .execute("CREATE TABLE public.array_membership (value TEXT PRIMARY KEY)")
        .await
        .expect("failed to create backing table");
    ctx.postgres
        .execute("INSERT INTO public.array_membership (value) VALUES ('allowed_1'), ('allowed_2')")
        .await
        .expect("failed to seed backing table");

    ctx.kafka
        .produce_avro_records(&[
            // any-match: a later element is a member
            ArrayMembershipRecord {
                id: "r1".into(),
                accounts: vec!["nobody".into(), "allowed_1".into()],
            },
            // no element is a member
            ArrayMembershipRecord {
                id: "r2".into(),
                accounts: vec!["nobody".into(), "stranger".into()],
            },
            // empty array → false
            ArrayMembershipRecord {
                id: "r3".into(),
                accounts: vec![],
            },
            // single element is a member
            ArrayMembershipRecord {
                id: "r4".into(),
                accounts: vec!["allowed_2".into()],
            },
        ])
        .await
        .expect("failed to produce records");

    // source_drain (blackhole) consumes all 4 source records directly, triggering
    // shutdown via num_records_before_stop. Without it the 2 filtered rows never
    // reach the sink and the pipeline hangs.
    let pipeline_yaml = format!(
        r#"
sources:
  input:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms:
  membership:
    type: dynamic_table
    backend_type: Postgres
    backend_entity_name: array_membership
    schema: public
    column: value
  matched:
    type: sql
    sql: "SELECT id FROM input WHERE dynamic_table_check('membership', accounts)"
    primary_key: id

sinks:
  matched_output:
    type: postgres
    from: matched
    table: array_membership_output
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
    batch_flush_interval: 100ms
  source_drain:
    type: blackhole
    from: input
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml,
            cached_postgres_opts(&ctx, 4, Duration::from_secs(60)),
        )
        .await
        .expect("pipeline failed");
    assert!(status.success(), "pipeline should exit successfully");

    // Only r1 (allowed_1) and r4 (allowed_2) have any member element; r2 misses
    // and r3 is empty.
    let output = output_ids(&ctx, "array_membership_output").await;
    assert_eq!(
        output,
        vec!["r1", "r4"],
        "only rows with any member element should pass"
    );
}
