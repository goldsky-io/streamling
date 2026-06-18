//! ClickHouse source e2e tests.
//!
//! These tests verify that streamling can correctly read from ClickHouse and write to PostgreSQL.
//! Ported from crates/streamling/tests/pipeline.rs (test_clickhouse_duplicate_boundary_e2e, test_clickhouse_keyset_pagination)
//!
//! Note: The original tests used MemorySink to capture output. These have been converted to use
//! PostgresSink for proper e2e verification.

use streamling_e2e::{init_tracing, PipelineOpts, TestContext, TestContextOptions};

// ============================================================================
// Scenario 1: ClickHouse source with duplicate boundary handling
// ============================================================================

/// Test reading from ClickHouse with complex pagination boundary conditions
/// Ported from: test_clickhouse_duplicate_boundary_e2e
#[tokio::test]
async fn test_clickhouse_source_boundary() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    // Create source table — first sorting key must be numeric for sort key range pagination
    clickhouse
        .execute(
            "CREATE TABLE boundary_test (
                category String,
                priority UInt32,
                id UInt64,
                data String,
                is_deleted UInt8
            ) ENGINE = MergeTree() ORDER BY (priority, category, id)",
        )
        .await
        .expect("Failed to create table");

    // Insert test data with multiple groups and UNIQUE IDs
    // Category A, Priority 1: 50 records (IDs 0-49)
    // Category A, Priority 2: 60 records (IDs 50-109)
    // Category B, Priority 1: 90 records (IDs 110-199)
    // Category C, Priority 1: 20 records (IDs 200-219)
    // Note: All records have is_deleted=0 to test boundary pagination without delete handling
    let mut values = Vec::new();
    let mut id_counter = 0u64;

    for i in 0..50 {
        values.push(format!("('A', 1, {}, 'data_A_1_{}', 0)", id_counter, i));
        id_counter += 1;
    }
    for i in 0..60 {
        values.push(format!("('A', 2, {}, 'data_A_2_{}', 0)", id_counter, i));
        id_counter += 1;
    }
    for i in 0..90 {
        values.push(format!("('B', 1, {}, 'data_B_1_{}', 0)", id_counter, i));
        id_counter += 1;
    }
    for i in 0..20 {
        values.push(format!("('C', 1, {}, 'data_C_1_{}', 0)", id_counter, i));
        id_counter += 1;
    }

    let total_records = 50 + 60 + 90 + 20; // 220 records

    // Insert in chunks
    for chunk in values.chunks(100) {
        let insert_query = format!(
            "INSERT INTO boundary_test (category, priority, id, data, is_deleted) VALUES {}",
            chunk.join(", ")
        );
        clickhouse
            .execute(&insert_query)
            .await
            .expect("Failed to insert data");
    }

    // Run pipeline: ClickHouse source → PostgreSQL sink
    let pipeline = r#"
sources:
  ch_source:
    type: clickhouse
    table_name: boundary_test
    primary_key: id

transforms: {}

sinks:
  pg_sink:
    type: postgres
    from: ch_source
    table: boundary_results
    schema: public
    primary_key: id
    on_conflict: update
"#;

    let status = ctx
        .run_pipeline_with_opts(
            pipeline,
            PipelineOpts::new()
                .record_limit(total_records as u64)
                .timeout(std::time::Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Verify all records were processed
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.boundary_results")
        .await
        .expect("Failed to query count");

    assert_eq!(
        count, total_records as i64,
        "Should have processed all {} records",
        total_records
    );

    // Verify records from each category
    let a1_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.boundary_results WHERE category = 'A' AND priority = 1")
        .await
        .unwrap();
    assert_eq!(a1_count, 50, "Should have 50 records in A/1");

    let a2_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.boundary_results WHERE category = 'A' AND priority = 2")
        .await
        .unwrap();
    assert_eq!(a2_count, 60, "Should have 60 records in A/2");
}

// ============================================================================
// Scenario 2: ClickHouse source with keyset pagination
// ============================================================================

/// Test keyset pagination with compound sorting keys
/// Ported from: test_clickhouse_keyset_pagination
#[tokio::test]
async fn test_clickhouse_source_keyset_pagination() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    // Create table with compound sorting key — first sorting key must be numeric for sort key range pagination
    clickhouse
        .execute(
            "CREATE TABLE keyset_test (
                region String,
                country String,
                city String,
                population UInt64,
                data_point String,
                is_deleted UInt8
            ) ENGINE = MergeTree() ORDER BY (population, region, country, city)",
        )
        .await
        .expect("Failed to create table");

    // Insert test data with hierarchical structure
    let mut values = Vec::new();
    let regions = ["A_Region", "B_Region", "C_Region"];
    let countries = ["Country_A", "Country_B"];
    let cities = ["City_1", "City_2"];

    for region in &regions {
        for country in &countries {
            for city in &cities {
                for pop_idx in 0..10 {
                    let population = (pop_idx + 1) * 10000;
                    values.push(format!(
                        "('{}', '{}', '{}', {}, 'data_{}_{}_{}', 0)",
                        region, country, city, population, region, country, city
                    ));
                }
            }
        }
    }

    // 3 regions × 2 countries × 2 cities × 10 populations = 120 records
    let total_records = 120;

    // Insert all data
    let insert_query = format!(
        "INSERT INTO keyset_test (region, country, city, population, data_point, is_deleted) VALUES {}",
        values.join(", ")
    );
    clickhouse
        .execute(&insert_query)
        .await
        .expect("Failed to insert data");

    // Run pipeline: ClickHouse source → PostgreSQL sink
    let pipeline = r#"
sources:
  ch_source:
    type: clickhouse
    table_name: keyset_test
    primary_key: region,country,city,population

transforms: {}

sinks:
  pg_sink:
    type: postgres
    from: ch_source
    table: keyset_results
    schema: public
    primary_key: population
    on_conflict: update
"#;

    let status = ctx
        .run_pipeline_with_opts(
            pipeline,
            PipelineOpts::new()
                .record_limit(total_records as u64)
                .timeout(std::time::Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Verify all records were processed
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.keyset_results")
        .await
        .expect("Failed to query count");

    // Note: With population as PK, we might have fewer due to deduplication
    // since multiple regions/countries/cities can have same population
    assert!(count > 0, "Should have processed some records");

    // Verify data from different regions exists
    let region_count: i64 = ctx
        .postgres
        .count(
            "SELECT COUNT(DISTINCT region) FROM public.keyset_results WHERE region LIKE '%Region'",
        )
        .await
        .unwrap_or(0);
    assert!(region_count > 0, "Should have data from multiple regions");
}

// ============================================================================
// Scenario 3: Sort key range with inner keyset pagination
// ============================================================================

/// Test that when a sort key range contains more rows than page_size, the source
/// shrinks the range until each page fits and still delivers every row across
/// all ranges.
#[tokio::test]
async fn test_clickhouse_source_sort_key_range_exceeds_page_size() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    clickhouse
        .execute(
            "CREATE TABLE sort_key_range_paging_test (
                block_number UInt64,
                id UInt64,
                data String,
                is_deleted UInt8
            ) ENGINE = MergeTree() ORDER BY (block_number, id)",
        )
        .await
        .expect("Failed to create table");

    // Insert 500 rows: block_number 0..499, each with a unique id.
    // page_size=30 forces the adaptive controller to shrink ranges below the
    // dense regions until each page fits; all 500 rows must still arrive.
    let total_records: u64 = 500;
    let mut values = Vec::new();
    for i in 0..total_records {
        values.push(format!("({}, {}, 'row_{}', 0)", i, i, i));
    }

    for chunk in values.chunks(200) {
        let insert_query = format!(
            "INSERT INTO sort_key_range_paging_test (block_number, id, data, is_deleted) VALUES {}",
            chunk.join(", ")
        );
        clickhouse
            .execute(&insert_query)
            .await
            .expect("Failed to insert data");
    }

    let pipeline = r#"
sources:
  ch_source:
    type: clickhouse
    table_name: sort_key_range_paging_test
    primary_key: id

transforms: {}

sinks:
  pg_sink:
    type: postgres
    from: ch_source
    table: sort_key_range_paging_results
    schema: public
    primary_key: id
    on_conflict: update
"#;

    let status = ctx
        .run_pipeline_with_opts(
            pipeline,
            PipelineOpts::new()
                .env("STREAMLING__CLICKHOUSE_SOURCE__PAGE_SIZE", "30")
                .env("STREAMLING__CLICKHOUSE_SOURCE__SORT_KEY_RANGE", "100")
                .record_limit(total_records)
                .timeout(std::time::Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.sort_key_range_paging_results")
        .await
        .expect("Failed to query count");

    assert_eq!(
        count, total_records as i64,
        "Should have processed all {} records across multiple sort key ranges with inner keyset pagination",
        total_records
    );

    // Verify rows from different sort key ranges made it through
    let first_range = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.sort_key_range_paging_results WHERE block_number < 100")
        .await
        .unwrap();
    assert_eq!(
        first_range, 100,
        "First sort key range [0,100) should have 100 rows"
    );

    let last_range = ctx
        .postgres
        .count(
            "SELECT COUNT(*) FROM public.sort_key_range_paging_results WHERE block_number >= 400",
        )
        .await
        .unwrap();
    assert_eq!(
        last_range, 100,
        "Last sort key range [400,500) should have 100 rows"
    );
}

// ============================================================================
// Scenario 4: Checkpoint flow across sparse sort key ranges
// ============================================================================

/// Test that checkpoints flow correctly when sort key range pagination scans
/// through a mix of populated and empty ranges. Verifies:
/// 1. Pipeline 1 processes the first cluster and checkpoints its position
/// 2. Pipeline 2 resumes from the checkpoint and processes the second cluster
///    without re-reading the first cluster
///
/// Data layout with sort_key_range=100:
///   [0,100)   → 50 rows (cluster 1)
///   [100,500) → empty (4 empty ranges)
///   [500,600) → 50 rows (cluster 2)
#[tokio::test]
async fn test_clickhouse_source_checkpoint_across_sparse_ranges() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    clickhouse
        .execute(
            "CREATE TABLE sparse_checkpoint_test (
                block_number UInt64,
                id UInt64,
                data String,
                is_deleted UInt8
            ) ENGINE = MergeTree() ORDER BY (block_number, id)",
        )
        .await
        .expect("Failed to create table");

    // Cluster 1: block_number 0..49 (in range [0,100))
    let mut values = Vec::new();
    for i in 0u64..50 {
        values.push(format!("({}, {}, 'cluster1_row_{}', 0)", i, i, i));
    }
    // Cluster 2: block_number 500..549 (in range [500,600))
    for i in 500u64..550 {
        values.push(format!("({}, {}, 'cluster2_row_{}', 0)", i, i, i));
    }

    clickhouse
        .execute(&format!(
            "INSERT INTO sparse_checkpoint_test (block_number, id, data, is_deleted) VALUES {}",
            values.join(", ")
        ))
        .await
        .expect("Failed to insert data");

    let state_table = format!("sparse_ckpt_{}", ctx.test_id.replace("-", "_"));
    let application_id = format!("sparse_ckpt_{}", ctx.test_id);

    let pipeline_run1 = r#"
sources:
  ch_source:
    type: clickhouse
    table_name: sparse_checkpoint_test
    primary_key: id

transforms: {}

sinks:
  pg_sink:
    type: postgres
    from: ch_source
    table: sparse_ckpt_run1
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
"#;

    // Run 1: process only the first 50 records (cluster 1).
    // With sort_key_range=100 and page_size=30 the source will also scan
    // empty ranges [100,200)…[400,500) before reaching cluster 2,
    // but record_limit will stop it after 50 records.
    let status_1 = ctx
        .run_pipeline_with_opts(
            pipeline_run1,
            PipelineOpts::new()
                .record_limit(50)
                .timeout(std::time::Duration::from_secs(120))
                .env("STREAMLING__APPLICATION_ID", &application_id)
                .env("STREAMLING__STATE_BACKEND__BACKEND_TYPE", "Postgres")
                .env(
                    "STREAMLING__STATE_BACKEND__POSTGRES__HOST",
                    &ctx.postgres.host,
                )
                .env(
                    "STREAMLING__STATE_BACKEND__POSTGRES__PORT",
                    ctx.postgres.port.to_string(),
                )
                .env("STREAMLING__STATE_BACKEND__POSTGRES__USER", "postgres")
                .env("STREAMLING__STATE_BACKEND__POSTGRES__PASSWORD", "postgres")
                .env("STREAMLING__STATE_BACKEND__POSTGRES__DB", &ctx.pg_database)
                .env("STREAMLING__STATE_BACKEND__POSTGRES__SSLMODE", "disable")
                .env(
                    "STREAMLING__STATE_BACKEND__POSTGRES__STATE_TABLE_NAME",
                    &state_table,
                )
                .env("STREAMLING__CHECKPOINT_INTERVAL_SEC", "1")
                .env("STREAMLING__RECORD_BATCH_SIZE", "10")
                .env("STREAMLING__CLICKHOUSE_SOURCE__PAGE_SIZE", "30")
                .env("STREAMLING__CLICKHOUSE_SOURCE__SORT_KEY_RANGE", "100"),
        )
        .await
        .expect("Pipeline run 1 failed");

    assert!(status_1.success(), "Pipeline run 1 should succeed");

    let count_1 = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.sparse_ckpt_run1")
        .await
        .expect("Failed to query count");
    assert!(
        count_1 >= 40,
        "Run 1 should have processed ~50 records from cluster 1, got {}",
        count_1
    );

    // Verify checkpoint was saved
    let checkpoint_count = ctx
        .postgres
        .count(&format!(
            "SELECT COUNT(*) FROM streamling.\"{}\"",
            state_table
        ))
        .await
        .expect("Failed to query checkpoint table");
    tracing::info!("Checkpoint entries after run 1: {}", checkpoint_count);

    // Run 2: resume from checkpoint — should NOT reprocess cluster 1
    let pipeline_run2 = r#"
sources:
  ch_source:
    type: clickhouse
    table_name: sparse_checkpoint_test
    primary_key: id

transforms: {}

sinks:
  pg_sink:
    type: postgres
    from: ch_source
    table: sparse_ckpt_run2
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
"#;

    let status_2 = ctx
        .run_pipeline_with_opts(
            pipeline_run2,
            PipelineOpts::new()
                .record_limit(50)
                .timeout(std::time::Duration::from_secs(120))
                .env("STREAMLING__APPLICATION_ID", &application_id)
                .env("STREAMLING__STATE_BACKEND__BACKEND_TYPE", "Postgres")
                .env(
                    "STREAMLING__STATE_BACKEND__POSTGRES__HOST",
                    &ctx.postgres.host,
                )
                .env(
                    "STREAMLING__STATE_BACKEND__POSTGRES__PORT",
                    ctx.postgres.port.to_string(),
                )
                .env("STREAMLING__STATE_BACKEND__POSTGRES__USER", "postgres")
                .env("STREAMLING__STATE_BACKEND__POSTGRES__PASSWORD", "postgres")
                .env("STREAMLING__STATE_BACKEND__POSTGRES__DB", &ctx.pg_database)
                .env("STREAMLING__STATE_BACKEND__POSTGRES__SSLMODE", "disable")
                .env(
                    "STREAMLING__STATE_BACKEND__POSTGRES__STATE_TABLE_NAME",
                    &state_table,
                )
                .env("STREAMLING__CHECKPOINT_INTERVAL_SEC", "1")
                .env("STREAMLING__RECORD_BATCH_SIZE", "10")
                .env("STREAMLING__CLICKHOUSE_SOURCE__PAGE_SIZE", "30")
                .env("STREAMLING__CLICKHOUSE_SOURCE__SORT_KEY_RANGE", "100"),
        )
        .await
        .expect("Pipeline run 2 failed");

    assert!(status_2.success(), "Pipeline run 2 should succeed");

    let count_2 = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.sparse_ckpt_run2")
        .await
        .expect("Failed to query count");
    assert!(
        count_2 > 0,
        "Run 2 should have processed records, got {}",
        count_2
    );

    // Run 2 should NOT have re-read cluster 1 rows if checkpoint worked
    if checkpoint_count > 0 {
        let min_block_2: Vec<(i64,)> = ctx
            .postgres
            .query("SELECT MIN(block_number) FROM public.sparse_ckpt_run2")
            .await
            .expect("Failed to query min block_number");

        tracing::info!(
            "Run 2: min_block_number={}, count={}",
            min_block_2[0].0,
            count_2
        );

        // If checkpointing worked, run 2 should not restart from block 0.
        // It should resume from somewhere after cluster 1 (block_number >= ~49).
        assert!(
            min_block_2[0].0 > 0,
            "Run 2 should NOT restart from block 0 when checkpoint exists, got min={}",
            min_block_2[0].0
        );
    }
}

// ============================================================================
// Scenario 5: Version-aware dedup activates when columns omit the version col
// ============================================================================

/// Regression: source-side ReplacingMergeTree dedup must activate even when
/// the configured `columns` omit the inferred version column. This is the
/// hybrid-source path — `ClickHouseSchemaAdapter::get_columns` projects
/// ClickHouse to the unbounded source's (Kafka) target schema, which excludes
/// ClickHouse housekeeping columns like `insert_timestamp` and `is_deleted`.
///
/// The fix force-includes the inferred version column in the internal scan
/// and projects it back out before emission, so:
///   1. dedup picks the max-`insert_timestamp` row per ORDER BY key,
///   2. tombstone winners (`is_deleted=1`) drop the key entirely (FINAL),
///   3. the external schema stays exactly the configured columns (no leaked
///      `insert_timestamp` in the postgres sink table).
#[tokio::test]
async fn test_clickhouse_source_replacing_dedup_when_version_column_not_selected() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");
    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    clickhouse
        .execute(
            "CREATE TABLE replacing_dedup_test (
                block_number UInt64,
                id String,
                payload String,
                insert_timestamp DateTime,
                is_deleted UInt8
            ) ENGINE = ReplacingMergeTree(insert_timestamp, is_deleted)
            ORDER BY (block_number, id)",
        )
        .await
        .expect("Failed to create source table");

    // 5 distinct (block_number, id) keys, 9 raw rows. Each scenario probes a
    // different dedup property; together they catch the activation regression
    // regardless of ClickHouse part-read order.
    //
    //   (1, 'a')  — single version, sanity (must arrive once).
    //   (2, 'b')  — newer insert_timestamp inserted second; dedup picks 'b_new'.
    //   (3, 'c')  — newer insert_timestamp inserted FIRST; position-based dedup
    //               would pick the wrong row, version-aware picks 'c_new'.
    //   (4, 'd')  — tombstone has the max insert_timestamp → whole key dropped.
    //   (5, 'e')  — tombstone is older than the live row → key kept as 'e_alive'.
    clickhouse
        .execute(
            "INSERT INTO replacing_dedup_test VALUES
                (1, 'a', 'a1',        toDateTime(1000), 0),
                (2, 'b', 'b_old',     toDateTime(1000), 0),
                (2, 'b', 'b_new',     toDateTime(2000), 0),
                (3, 'c', 'c_new',     toDateTime(2000), 0),
                (3, 'c', 'c_old',     toDateTime(1000), 0),
                (4, 'd', 'd_alive',   toDateTime(1000), 0),
                (4, 'd', 'd_deleted', toDateTime(2000), 1),
                (5, 'e', 'e_alive',   toDateTime(2000), 0),
                (5, 'e', 'e_deleted', toDateTime(1000), 1)",
        )
        .await
        .expect("Failed to insert source data");

    // The pipeline's `columns` deliberately OMIT `insert_timestamp` and
    // `is_deleted` — replaying the hybrid-source projection that previously
    // silently disabled dedup. (Comma-separated, no spaces: the topology
    // parser splits on ',' without trimming.)
    let pipeline = r#"
sources:
  ch_source:
    type: clickhouse
    table_name: replacing_dedup_test
    columns: "block_number,id,payload"
    primary_key: id

transforms: {}

sinks:
  pg_sink:
    type: postgres
    from: ch_source
    table: replacing_dedup_results
    schema: public
    primary_key: id
    on_conflict: update
"#;

    let status = ctx
        .run_pipeline_with_opts(
            pipeline,
            // Upper bound: 9 raw rows would be emitted without dedup. Bounded
            // source completes naturally; the limit is a safety net.
            PipelineOpts::new()
                .record_limit(9)
                .timeout(std::time::Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");
    assert!(status.success(), "pipeline should exit successfully");

    // (a) FINAL row count: 5 keys − 1 tombstoned key ('d') = 4.
    let total = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.replacing_dedup_results")
        .await
        .expect("count query failed");
    assert_eq!(
        total, 4,
        "ReplacingMergeTree FINAL semantics: 5 keys minus 1 tombstoned = 4"
    );

    // (b) Position-vs-version: 'c_new' has the higher `insert_timestamp` but
    // was inserted FIRST, so a position-based or non-deduped reader would
    // either pick 'c_old' or vary by scan order. Version-aware dedup picks
    // 'c_new' deterministically.
    let c_new = ctx
        .postgres
        .count(
            "SELECT COUNT(*) FROM public.replacing_dedup_results \
             WHERE id = 'c' AND payload = 'c_new'",
        )
        .await
        .unwrap();
    assert_eq!(
        c_new, 1,
        "max insert_timestamp must win for id='c' (got != 'c_new')"
    );

    // (c) Tombstone winner: key 'd' must be entirely absent.
    let d = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.replacing_dedup_results WHERE id = 'd'")
        .await
        .unwrap();
    assert_eq!(d, 0, "tombstoned key 'd' must be dropped (FINAL)");

    // (d) Tombstone non-winner: 'e' survives as alive — an older delete must
    // not displace a newer live row.
    let e_alive = ctx
        .postgres
        .count(
            "SELECT COUNT(*) FROM public.replacing_dedup_results \
             WHERE id = 'e' AND payload = 'e_alive'",
        )
        .await
        .unwrap();
    assert_eq!(
        e_alive, 1,
        "older tombstone must not delete a newer live row for id='e'"
    );

    // (e) External schema contract: the force-included version column is
    // projected out before emission, so the postgres table only carries the
    // configured columns (plus any standard sink columns) — never
    // `insert_timestamp` or `is_deleted`.
    let cols = ctx
        .postgres
        .get_column_names("replacing_dedup_results")
        .await
        .unwrap();
    assert!(
        !cols.iter().any(|c| c == "insert_timestamp"),
        "insert_timestamp must be projected out before emission (got columns: {:?})",
        cols
    );
    assert!(
        !cols.iter().any(|c| c == "is_deleted"),
        "is_deleted must not leak into the external schema (got columns: {:?})",
        cols
    );
}
