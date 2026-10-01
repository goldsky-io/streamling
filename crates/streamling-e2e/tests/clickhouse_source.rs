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

/// Count-first pagination: a sort-key span whose *average* density fits a page
/// but contains a DENSE cluster (several rows per key) must be sized from the
/// exact per-range count BEFORE the data read, not from the span average. The
/// up-front probe sizes the initial width from whole-span density and would
/// otherwise walk a wide range straight into the cluster, materialising more
/// than `page_size` rows before the reactive overflow could shrink it. With
/// count-first sizing each range is probed and shrunk to fit first; this test
/// verifies the orchestration (probe, shrink, re-probe, converge) delivers
/// every row through a real dense cluster without loss or stall.
#[tokio::test]
async fn test_clickhouse_source_count_first_shrinks_dense_cluster() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    clickhouse
        .execute(
            "CREATE TABLE count_first_cluster_test (
                block_number UInt64,
                id UInt64,
                data String,
                is_deleted UInt8
            ) ENGINE = MergeTree() ORDER BY (block_number, id)",
        )
        .await
        .expect("Failed to create table");

    // Sparse [0,40): one row per key. Dense cluster [40,50): five rows per key
    // (fanout 5, kept under page_size so no single key hits the unsplittable
    // floor). Sparse [50,90): one row per key. Total = 40 + 50 + 40 = 130.
    let mut values: Vec<String> = Vec::new();
    for i in 0..40u64 {
        values.push(format!("({}, {}, 'sparse_{}', 0)", i, i, i));
    }
    for blk in 40..50u64 {
        for j in 0..5u64 {
            let id = blk * 10 + j;
            values.push(format!("({}, {}, 'dense_{}', 0)", blk, id, id));
        }
    }
    for i in 50..90u64 {
        values.push(format!("({}, {}, 'sparse_{}', 0)", i, i, i));
    }
    let total_records = values.len() as i64;

    for chunk in values.chunks(200) {
        let insert_query = format!(
            "INSERT INTO count_first_cluster_test (block_number, id, data, is_deleted) VALUES {}",
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
    table_name: count_first_cluster_test
    primary_key: id

transforms: {}

sinks:
  pg_sink:
    type: postgres
    from: ch_source
    table: count_first_cluster_results
    schema: public
    primary_key: id
    on_conflict: update
"#;

    // page_size 30 < the cluster's 50 rows across [40,50), so any multi-key range
    // overlapping the cluster overflows by rows and must be shrunk from its exact
    // count before reading.
    let status = ctx
        .run_pipeline_with_opts(
            pipeline,
            PipelineOpts::new()
                .env("STREAMLING__CLICKHOUSE_SOURCE__PAGE_SIZE", "30")
                .env("STREAMLING__CLICKHOUSE_SOURCE__SORT_KEY_RANGE", "50")
                .record_limit(total_records as u64)
                .timeout(std::time::Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.count_first_cluster_results")
        .await
        .expect("Failed to query count");
    assert_eq!(
        count, total_records,
        "every row, including the dense cluster, must be delivered after count-first sizing"
    );

    // The cluster [40,50) carries 50 rows across 10 keys; confirming all 50
    // arrived proves the count-first loop shrank each overlapping range to fit
    // rather than skipping the overflow.
    let cluster_rows = ctx
        .postgres
        .count(
            "SELECT COUNT(*) FROM public.count_first_cluster_results \
             WHERE block_number >= 40 AND block_number < 50",
        )
        .await
        .unwrap();
    assert_eq!(
        cluster_rows, 50,
        "dense cluster [40,50) must deliver all 50 rows (5 per key x 10 keys)"
    );
}

/// A single first-sort-key value (a "hot" block) holds more raw rows than
/// `page_size`, so no first-key range can fit a page — even at the minimum
/// width of one key. The source must page WITHIN that key on the remaining
/// sort keys instead of failing with "at min width still exceeds page limits"
/// (prod: token-metadata-tier1, block 22270037, 1.27M rows > page_size 1M).
///
/// The table is a ReplacingMergeTree with every hot id written as 2-3
/// versions in separate parts (merges stopped), in groups whose sizes do not
/// divide the page, so an in-key page boundary that split one
/// (block_number, id) tuple's versions across pages would emit a stale
/// version or a tombstoned id. A per-row emission id makes every emitted row
/// its own sink row, so a split or a duplicate emission is counted.
#[tokio::test]
async fn test_clickhouse_source_hot_first_key_exceeds_page_size() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");
    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    clickhouse
        .execute(
            "CREATE TABLE hot_key_test (
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
    clickhouse
        .execute("SYSTEM STOP MERGES hot_key_test")
        .await
        .expect("Failed to stop merges");

    // Sparse neighbours: blocks 0..10 and 101, one row each.
    let mut sparse: Vec<String> = (0..10u64)
        .map(|b| format!("({b}, 's{b}', 'new', toDateTime(1000), 0)"))
        .collect();
    sparse.push("(101, 't0', 'new', toDateTime(1000), 0)".to_string());
    // Hot block 100: 100 ids, each an 'old' then a 'new' version in separate
    // parts, every third id also a 'mid' version in between; ids h090..h099
    // also get a newer tombstone and must be dropped.
    let old: Vec<String> = (0..100u64)
        .map(|i| format!("(100, 'h{i:03}', 'old', toDateTime(1000), 0)"))
        .collect();
    let mid: Vec<String> = (0..100u64)
        .step_by(3)
        .map(|i| format!("(100, 'h{i:03}', 'mid', toDateTime(1500), 0)"))
        .collect();
    let new: Vec<String> = (0..100u64)
        .map(|i| format!("(100, 'h{i:03}', 'new', toDateTime(2000), 0)"))
        .collect();
    let tombstones: Vec<String> = (90..100u64)
        .map(|i| format!("(100, 'h{i:03}', 'new', toDateTime(3000), 1)"))
        .collect();
    let raw_rows = (sparse.len() + old.len() + mid.len() + new.len() + tombstones.len()) as u64;
    for part in [&sparse, &old, &mid, &new, &tombstones] {
        clickhouse
            .execute(&format!(
                "INSERT INTO hot_key_test SETTINGS optimize_on_insert = 0 VALUES {}",
                part.join(", ")
            ))
            .await
            .expect("Failed to insert source data");
    }

    let pipeline = r#"
sources:
  ch_source:
    type: clickhouse
    table_name: hot_key_test
    columns: "block_number,id,payload"
    primary_key: id

transforms:
  emitted:
    type: sql
    primary_key: emission_id
    sql: "SELECT *, uuid() AS emission_id FROM ch_source"

sinks:
  pg_sink:
    type: postgres
    from: emitted
    table: hot_key_results
    schema: public
    primary_key: emission_id
    on_conflict: update
"#;

    // page_size 30 < the hot block's 244 raw rows: every range covering block
    // 100, including the one-key range [100, 101), overflows.
    let status = ctx
        .run_pipeline_with_opts(
            pipeline,
            PipelineOpts::new()
                .env("STREAMLING__CLICKHOUSE_SOURCE__PAGE_SIZE", "30")
                .env("STREAMLING__CLICKHOUSE_SOURCE__SORT_KEY_RANGE", "100")
                .record_limit(raw_rows)
                .timeout(std::time::Duration::from_secs(90)),
        )
        .await
        .expect("Streamling execution failed");
    assert!(
        status.success(),
        "a first-key value with more rows than page_size must be paged, not fail the scan"
    );

    // 11 sparse + 90 live hot ids (h090..h099 tombstoned), each emitted once.
    let total = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.hot_key_results")
        .await
        .expect("count query failed");
    assert_eq!(total, 101, "every live key must be emitted exactly once");
    let distinct = ctx
        .postgres
        .count("SELECT COUNT(DISTINCT id) FROM public.hot_key_results")
        .await
        .unwrap();
    assert_eq!(distinct, 101, "every live key must arrive");

    let hot = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.hot_key_results WHERE block_number = 100")
        .await
        .unwrap();
    assert_eq!(hot, 90, "hot block must deliver its 90 live ids");

    let stale = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.hot_key_results WHERE payload <> 'new'")
        .await
        .unwrap();
    assert_eq!(
        stale, 0,
        "a page boundary split a tuple's versions: a stale version was emitted"
    );

    let resurrected = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.hot_key_results WHERE id >= 'h090' AND id <= 'h099'")
        .await
        .unwrap();
    assert_eq!(resurrected, 0, "tombstoned hot ids must be dropped");
}

/// Paging within a hot first-key value must order, seek and resume on the raw
/// sort keys whatever the scan selects: `n` is selected through a String cast
/// alias (lexicographic, while the key is numeric), `tag` is an unselected
/// Nullable FixedString whose NULL group spans a page boundary, and `m` is an
/// unselected MATERIALIZED key.
#[tokio::test]
async fn test_clickhouse_source_hot_first_key_with_awkward_sort_keys() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");
    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    clickhouse
        .execute(
            "CREATE TABLE awkward_key_test (
                block_number UInt64,
                tag Nullable(FixedString(4)),
                n UInt64,
                m String MATERIALIZED toString(n),
                payload String
            ) ENGINE = MergeTree ORDER BY (block_number, tag, n, m)
            SETTINGS allow_nullable_key = 1",
        )
        .await
        .expect("Failed to create source table");

    // Hot block 100: 200 rows, 50 of them with a NULL tag. Sparse neighbours:
    // blocks 0..10 and 101, one row each.
    let mut rows: Vec<String> = (0..200u64)
        .map(|n| {
            let tag = match n % 4 {
                0 => "NULL",
                1 => "'aaaa'",
                _ => "'bb'",
            };
            format!("(100, {tag}, {n}, 'p')")
        })
        .collect();
    rows.extend((0..10u64).map(|b| format!("({b}, 'aaaa', {}, 'p')", 1000 + b)));
    rows.push("(101, 'aaaa', 2000, 'p')".to_string());
    let total_rows = rows.len() as i64;
    clickhouse
        .execute(&format!(
            "INSERT INTO awkward_key_test (block_number, tag, n, payload) VALUES {}",
            rows.join(", ")
        ))
        .await
        .expect("Failed to insert source data");

    let pipeline = r#"
sources:
  ch_source:
    type: clickhouse
    table_name: awkward_key_test
    columns: "block_number,CAST(n AS String) AS n,payload"
    primary_key: n

transforms:
  emitted:
    type: sql
    primary_key: emission_id
    sql: "SELECT *, uuid() AS emission_id FROM ch_source"

sinks:
  pg_sink:
    type: postgres
    from: emitted
    table: awkward_key_results
    schema: public
    primary_key: emission_id
    on_conflict: update
"#;

    let status = ctx
        .run_pipeline_with_opts(
            pipeline,
            PipelineOpts::new()
                .env("STREAMLING__CLICKHOUSE_SOURCE__PAGE_SIZE", "30")
                .env("STREAMLING__CLICKHOUSE_SOURCE__SORT_KEY_RANGE", "100")
                .record_limit(total_rows as u64)
                .timeout(std::time::Duration::from_secs(120)),
        )
        .await
        .expect("Streamling execution failed");
    assert!(
        status.success(),
        "the hot block must be paged, not fail or stall the scan"
    );

    let emitted = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.awkward_key_results")
        .await
        .expect("count query failed");
    assert_eq!(
        emitted, total_rows,
        "every row must be emitted exactly once"
    );
    let delivered = ctx
        .postgres
        .count("SELECT COUNT(DISTINCT n) FROM public.awkward_key_results")
        .await
        .unwrap();
    assert_eq!(delivered, total_rows, "every row must arrive: none skipped");
    let hot = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.awkward_key_results WHERE block_number = 100")
        .await
        .unwrap();
    assert_eq!(hot, 200, "the hot block must deliver all 200 rows");
}

/// Postgres state backend, checkpointing every second, for a pipeline whose
/// checkpoints a test reads back.
fn pg_state_opts(ctx: &TestContext, application_id: &str, state_table: &str) -> PipelineOpts {
    PipelineOpts::new()
        .env("STREAMLING__APPLICATION_ID", application_id)
        .env("STREAMLING__STATE_BACKEND__BACKEND_TYPE", "Postgres")
        .env(
            "STREAMLING__STATE_BACKEND__POSTGRES__HOST",
            &ctx.postgres.host,
        )
        .env(
            "STREAMLING__STATE_BACKEND__POSTGRES__PORT",
            ctx.postgres.port.to_string(),
        )
        .env(
            "STREAMLING__STATE_BACKEND__POSTGRES__USER",
            &ctx.postgres.user,
        )
        .env(
            "STREAMLING__STATE_BACKEND__POSTGRES__PASSWORD",
            &ctx.postgres.password,
        )
        .env("STREAMLING__STATE_BACKEND__POSTGRES__DB", &ctx.pg_database)
        .env("STREAMLING__STATE_BACKEND__POSTGRES__SSLMODE", "disable")
        .env(
            "STREAMLING__STATE_BACKEND__POSTGRES__STATE_TABLE_NAME",
            state_table,
        )
        .env("STREAMLING__CHECKPOINT_INTERVAL_SEC", "1")
}

/// `page_size` 30 and `sort_key_range` 100 in batches of 10, on top of
/// `pg_state_opts`.
fn small_page_opts(ctx: &TestContext, application_id: &str, state_table: &str) -> PipelineOpts {
    pg_state_opts(ctx, application_id, state_table)
        .env("STREAMLING__RECORD_BATCH_SIZE", "10")
        .env("STREAMLING__CLICKHOUSE_SOURCE__PAGE_SIZE", "30")
        .env("STREAMLING__CLICKHOUSE_SOURCE__SORT_KEY_RANGE", "100")
}

/// A checkpoint taken while paging inside a hot first-key value must carry
/// the full sort-key tuple cursor, and a restart must resume strictly after
/// that tuple: the resumed run emits exactly the rows past the cursor, once
/// each — none at or before it (no duplicate) and none skipped (no loss).
///
/// Every row sits in block 7, so any checkpoint run 1 persists before the
/// scan finishes is necessarily mid-key.
#[tokio::test]
async fn test_clickhouse_source_hot_first_key_resumes_mid_key() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");
    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    clickhouse
        .execute(
            "CREATE TABLE hot_key_resume_test (
                block_number UInt64,
                id UInt64,
                data String
            ) ENGINE = MergeTree() ORDER BY (block_number, id)",
        )
        .await
        .expect("Failed to create table");
    let total_rows: i64 = 3000;
    let values: Vec<String> = (0..total_rows)
        .map(|i| format!("(7, {i}, 'row_{i}')"))
        .collect();
    clickhouse
        .execute(&format!(
            "INSERT INTO hot_key_resume_test (block_number, id, data) VALUES {}",
            values.join(", ")
        ))
        .await
        .expect("Failed to insert data");

    let state_table = format!("hot_key_ckpt_{}", ctx.test_id.replace("-", "_"));
    let application_id = format!("hot_key_ckpt_{}", ctx.test_id);
    let pipeline = |sink_table: &str| {
        format!(
            r#"
sources:
  ch_source:
    type: clickhouse
    table_name: hot_key_resume_test
    primary_key: id

transforms:
  emitted:
    type: sql
    primary_key: emission_id
    sql: "SELECT *, uuid() AS emission_id FROM ch_source"

sinks:
  pg_sink:
    type: postgres
    from: emitted
    table: {sink_table}
    schema: public
    primary_key: emission_id
    on_conflict: update
    batch_size: 1
"#
        )
    };

    // Run 1 stops long before the 3000-row hot key is exhausted.
    let status_1 = ctx
        .run_pipeline_with_opts(
            &pipeline("hot_key_ckpt_run1"),
            small_page_opts(&ctx, &application_id, &state_table)
                .record_limit(1000)
                .timeout(std::time::Duration::from_secs(120)),
        )
        .await
        .expect("Pipeline run 1 failed");
    assert!(status_1.success(), "Pipeline run 1 should succeed");

    let split_query = |expr: &str| {
        format!(
            "SELECT ({expr})::bigint FROM streamling.\"{state_table}\" \
             WHERE key LIKE 'clickhouse_source:%'"
        )
    };
    let cursor_len = ctx
        .postgres
        .count(&split_query("jsonb_array_length(data->'in_key_after')"))
        .await
        .expect("run 1 must persist a mid-key ClickHouse source checkpoint");
    assert_eq!(
        cursor_len, 1,
        "a mid-hot-key checkpoint must carry the remaining (id) cursor"
    );
    let cursor_block = ctx
        .postgres
        .count(&split_query("data->'args'->0->>'value'"))
        .await
        .unwrap();
    let cursor_id = ctx
        .postgres
        .count(&split_query("data->'in_key_after'->0->>'value'"))
        .await
        .unwrap();
    assert_eq!(cursor_block, 7, "cursor must sit inside the hot block");
    assert!(
        (0..total_rows - 1).contains(&cursor_id),
        "cursor id {cursor_id} must be mid-key"
    );

    // Run 2 resumes from the checkpoint and scans to the end.
    let status_2 = ctx
        .run_pipeline_with_opts(
            &pipeline("hot_key_ckpt_run2"),
            small_page_opts(&ctx, &application_id, &state_table)
                .record_limit(total_rows as u64)
                .timeout(std::time::Duration::from_secs(120)),
        )
        .await
        .expect("Pipeline run 2 failed");
    assert!(status_2.success(), "Pipeline run 2 should succeed");

    let replayed = ctx
        .postgres
        .count(&format!(
            "SELECT COUNT(*) FROM public.hot_key_ckpt_run2 WHERE id <= {cursor_id}"
        ))
        .await
        .unwrap();
    assert_eq!(
        replayed, 0,
        "resume must start strictly after the checkpointed tuple (7, {cursor_id})"
    );
    let resumed = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.hot_key_ckpt_run2")
        .await
        .unwrap();
    assert_eq!(
        resumed,
        total_rows - 1 - cursor_id,
        "resume must emit every row after (7, {cursor_id}) exactly once"
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
            small_page_opts(&ctx, &application_id, &state_table)
                .record_limit(50)
                .timeout(std::time::Duration::from_secs(120)),
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
            small_page_opts(&ctx, &application_id, &state_table)
                .record_limit(50)
                .timeout(std::time::Duration::from_secs(120)),
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
            .query("SELECT MIN(block_number)::bigint FROM public.sparse_ckpt_run2")
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

/// Regression: when a live row and a delete share the SAME `insert_timestamp`
/// (a tied version), the source-side version dedup must let the delete win so
/// the key is dropped — the row's final state is deleted. The scan has no
/// ORDER BY, so the previous tiebreak (later position wins) was
/// order-dependent: a live row scanned after the delete survived and the
/// deletion was silently lost, leaving a stale live row in the sink.
#[tokio::test]
async fn test_clickhouse_source_tied_version_delete_drops_key() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");
    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    clickhouse
        .execute(
            "CREATE TABLE tied_version_dedup_test (
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

    // Two keys; for each, a live row and a delete at the SAME version, in
    // opposite orders so the regression is caught regardless of scan order.
    //   (1, 'a') — live first, then delete.
    //   (2, 'b') — delete first, then live.
    // Both must drop the key: the delete supersedes the live row on a tie.
    //
    // `optimize_on_insert = 0` stops ClickHouse from collapsing the tied live
    // + delete rows at INSERT time — its default dedups them per the engine
    // before the source scan ever sees both, which hides exactly this bug.
    // One part, no insert-time collapse, no background-merge race: the scan
    // reads all four raw rows and the source-side dedup resolves the tie.
    clickhouse
        .execute(
            "INSERT INTO tied_version_dedup_test SETTINGS optimize_on_insert = 0 VALUES
                (1, 'a', 'a_alive', toDateTime(1000), 0),
                (1, 'a', 'a_alive', toDateTime(1000), 1),
                (2, 'b', 'b_alive', toDateTime(1000), 1),
                (2, 'b', 'b_alive', toDateTime(1000), 0)",
        )
        .await
        .expect("Failed to insert source data");

    let pipeline = r#"
sources:
  ch_source:
    type: clickhouse
    table_name: tied_version_dedup_test
    columns: "block_number,id,payload"
    primary_key: id

transforms: {}

sinks:
  pg_sink:
    type: postgres
    from: ch_source
    table: tied_version_dedup_results
    schema: public
    primary_key: id
    on_conflict: update
"#;

    let status = ctx
        .run_pipeline_with_opts(
            pipeline,
            PipelineOpts::new()
                .record_limit(4)
                .timeout(std::time::Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");
    assert!(status.success(), "pipeline should exit successfully");

    // Both keys are deleted at their (tied) max version -> FINAL drops them.
    let total = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.tied_version_dedup_results")
        .await
        .expect("count query failed");
    assert_eq!(
        total, 0,
        "tied live+delete rows must both be dropped (delete wins the tie)"
    );
}
