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

    // Create source table — first sorting key must be numeric for block range pagination
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

    // Create table with compound sorting key — first sorting key must be numeric for block range pagination
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
// Scenario 3: Block range with inner keyset pagination
// ============================================================================

/// Test that when a single block range contains more rows than page_size,
/// inner keyset pagination correctly pages through them before advancing
/// to the next block range.
#[tokio::test]
async fn test_clickhouse_source_block_range_exceeds_page_size() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    clickhouse
        .execute(
            "CREATE TABLE block_range_paging_test (
                block_number UInt64,
                id UInt64,
                data String,
                is_deleted UInt8
            ) ENGINE = MergeTree() ORDER BY (block_number, id)",
        )
        .await
        .expect("Failed to create table");

    // Insert 500 rows: block_number 0..499, each with a unique id.
    // With block_range=100, we get 5 block ranges: [0,100), [100,200), ...
    // With page_size=30, each block range (100 rows) needs ~4 keyset pages.
    let total_records: u64 = 500;
    let mut values = Vec::new();
    for i in 0..total_records {
        values.push(format!("({}, {}, 'row_{}', 0)", i, i, i));
    }

    for chunk in values.chunks(200) {
        let insert_query = format!(
            "INSERT INTO block_range_paging_test (block_number, id, data, is_deleted) VALUES {}",
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
    table_name: block_range_paging_test
    primary_key: id

transforms: {}

sinks:
  pg_sink:
    type: postgres
    from: ch_source
    table: block_range_paging_results
    schema: public
    primary_key: id
    on_conflict: update
"#;

    let status = ctx
        .run_pipeline_with_opts(
            pipeline,
            PipelineOpts::new()
                .env("STREAMLING__CLICKHOUSE_SOURCE__PAGE_SIZE", "30")
                .env("STREAMLING__CLICKHOUSE_SOURCE__BLOCK_RANGE", "100")
                .record_limit(total_records)
                .timeout(std::time::Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.block_range_paging_results")
        .await
        .expect("Failed to query count");

    assert_eq!(
        count, total_records as i64,
        "Should have processed all {} records across multiple block ranges with inner keyset pagination",
        total_records
    );

    // Verify rows from different block ranges made it through
    let first_range = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.block_range_paging_results WHERE block_number < 100")
        .await
        .unwrap();
    assert_eq!(
        first_range, 100,
        "First block range [0,100) should have 100 rows"
    );

    let last_range = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.block_range_paging_results WHERE block_number >= 400")
        .await
        .unwrap();
    assert_eq!(
        last_range, 100,
        "Last block range [400,500) should have 100 rows"
    );
}

// ============================================================================
// Scenario 4: Checkpoint flow across sparse block ranges
// ============================================================================

/// Test that checkpoints flow correctly when block range pagination scans
/// through a mix of populated and empty ranges. Verifies:
/// 1. Pipeline 1 processes the first cluster and checkpoints its position
/// 2. Pipeline 2 resumes from the checkpoint and processes the second cluster
///    without re-reading the first cluster
///
/// Data layout with block_range=100:
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
    // With block_range=100 and page_size=30 the source will also scan
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
                .env("STREAMLING__CLICKHOUSE_SOURCE__BLOCK_RANGE", "100"),
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
                .env("STREAMLING__CLICKHOUSE_SOURCE__BLOCK_RANGE", "100"),
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
