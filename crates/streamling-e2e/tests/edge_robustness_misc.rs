//! Adversarial e2e tests probing pipeline ROBUSTNESS across fan-out, filters,
//! SQL type coercion, arithmetic edge cases, and passthrough transforms.
//!
//! Every test produces real FLAT avro input from a `#[derive(Serialize)]` struct
//! matching a `const SCHEMA`, registers the schema, produces records, runs a
//! Kafka -> (transform?) -> sink pipeline, and asserts on what landed in Postgres
//! (or, for the arithmetic edge cases, on the raw process output).
//!
//! Some assertions are intentionally adversarial: a failure here is a *finding*
//! about the SQL/transform/sink path, not a flaky test.
//!
//! PITFALL guarded against (caused 60s hangs in earlier drafts): every `sql`
//! transform output column ALIAS exactly matches BOTH the CREATE TABLE column
//! name AND the verification SELECT, so the Postgres sink never retries a failing
//! INSERT forever. Column types are generous (BIGINT, DOUBLE PRECISION, TEXT).

use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

/// Pipeline options copied verbatim from the verified decimal/avro templates:
/// a generous timeout and empty plugin config so the binary runs without plugins.
fn base_opts() -> PipelineOpts {
    PipelineOpts::new()
        .timeout(std::time::Duration::from_secs(60))
        .env("STREAMLING__PLUGIN__PATH", "")
        .env("STREAMLING__PLUGIN__PREPROCESSOR_IDS", "")
        .env("STREAMLING__PLUGIN__SIDE_OUTPUT_IDS", "")
}

// ---------------------------------------------------------------------------
// Shared FLAT avro record types
// ---------------------------------------------------------------------------

/// id + a single long `val`.
#[derive(Debug, Clone, Serialize)]
struct ValRec {
    id: i64,
    val: i64,
}

const VAL_SCHEMA: &str = r#"{
    "type": "record",
    "name": "ValRec",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "val", "type": "long"}
    ]
}"#;

/// id + a nullable long `opt_col`.
#[derive(Debug, Clone, Serialize)]
struct OptRec {
    id: i64,
    opt_col: Option<i64>,
}

const OPT_SCHEMA: &str = r#"{
    "type": "record",
    "name": "OptRec",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "opt_col", "type": ["null", "long"], "default": null}
    ]
}"#;

/// id + a long `i_col` and double `d_col` (for coercion tests).
#[derive(Debug, Clone, Serialize)]
struct MixRec {
    id: i64,
    i_col: i64,
    d_col: f64,
}

const MIX_SCHEMA: &str = r#"{
    "type": "record",
    "name": "MixRec",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "i_col", "type": "long"},
        {"name": "d_col", "type": "double"}
    ]
}"#;

/// id + a long `big_col` (for overflow tests).
#[derive(Debug, Clone, Serialize)]
struct BigRec {
    id: i64,
    big_col: i64,
}

const BIG_SCHEMA: &str = r#"{
    "type": "record",
    "name": "BigRec",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "big_col", "type": "long"}
    ]
}"#;

/// id + a string `data` (for rename / passthrough tests).
#[derive(Debug, Clone, Serialize)]
struct DataRec {
    id: i64,
    data: String,
}

const DATA_SCHEMA: &str = r#"{
    "type": "record",
    "name": "DataRec",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "data", "type": "string"}
    ]
}"#;

// ---------------------------------------------------------------------------
// Shared FromRow result types
// ---------------------------------------------------------------------------

#[derive(Debug, FromRow, Deserialize)]
struct IdVal {
    #[allow(dead_code)]
    id: i64,
    val: i64,
}

#[derive(Debug, FromRow, Deserialize)]
struct IdOpt {
    #[allow(dead_code)]
    id: i64,
    opt_col: Option<i64>,
}

#[derive(Debug, FromRow, Deserialize)]
struct IdDouble {
    #[allow(dead_code)]
    id: i64,
    total: f64,
}

#[derive(Debug, FromRow, Deserialize)]
struct IdLabel {
    #[allow(dead_code)]
    id: i64,
    label: String,
}

#[derive(Debug, FromRow, Deserialize)]
struct IdRenamed {
    #[allow(dead_code)]
    id: i64,
    renamed: String,
}

// ===========================================================================
// Scenario 1: fan-out source -> postgres sink AND blackhole sink
// ===========================================================================

#[tokio::test]
async fn fanout_postgres_and_blackhole() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE fan_pg_blackhole (id BIGINT PRIMARY KEY, val BIGINT NOT NULL)")
        .await
        .expect("create table");

    let recs: Vec<ValRec> = (1..=20).map(|i| ValRec { id: i, val: i * 10 }).collect();
    ctx.kafka.register_schema(VAL_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    let yaml = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms: {{}}
sinks:
  pg_out:
    type: postgres
    from: src
    table: fan_pg_blackhole
    schema: public
    primary_key: id
    on_conflict: update
  bh_out:
    type: blackhole
    from: src
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(recs.len() as u64))
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.fan_pg_blackhole")
        .await
        .expect("count");
    assert_eq!(
        count, 20,
        "postgres branch of the fan-out must have all 20 rows"
    );
}

// ===========================================================================
// Scenario 2: fan-out source -> TWO postgres tables (different `table:`)
// ===========================================================================

#[tokio::test]
async fn fanout_two_postgres_tables() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE fan_two_a (id BIGINT PRIMARY KEY, val BIGINT NOT NULL)")
        .await
        .expect("create table a");
    ctx.postgres
        .execute("CREATE TABLE fan_two_b (id BIGINT PRIMARY KEY, val BIGINT NOT NULL)")
        .await
        .expect("create table b");

    let recs: Vec<ValRec> = (1..=15)
        .map(|i| ValRec {
            id: i,
            val: i + 100,
        })
        .collect();
    ctx.kafka.register_schema(VAL_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    let yaml = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms: {{}}
sinks:
  pg_a:
    type: postgres
    from: src
    table: fan_two_a
    schema: public
    primary_key: id
    on_conflict: update
  pg_b:
    type: postgres
    from: src
    table: fan_two_b
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(recs.len() as u64))
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let count_a = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.fan_two_a")
        .await
        .expect("count a");
    let count_b = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.fan_two_b")
        .await
        .expect("count b");
    assert_eq!(count_a, 15, "table A must be fully populated");
    assert_eq!(count_b, 15, "table B must be fully populated");
}

// ===========================================================================
// Scenario 3: fan-out one branch sql transform, other branch passthrough
// ===========================================================================

#[tokio::test]
async fn fanout_transform_and_passthrough() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    // Passthrough branch: same `val`.
    ctx.postgres
        .execute("CREATE TABLE fan_passthrough (id BIGINT PRIMARY KEY, val BIGINT NOT NULL)")
        .await
        .expect("create passthrough table");
    // Transformed branch: doubled `val`.
    ctx.postgres
        .execute("CREATE TABLE fan_doubled (id BIGINT PRIMARY KEY, val BIGINT NOT NULL)")
        .await
        .expect("create doubled table");

    let recs: Vec<ValRec> = (1..=10).map(|i| ValRec { id: i, val: i }).collect();
    ctx.kafka.register_schema(VAL_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    // Alias `val` matches both the CREATE TABLE column and the SELECT below.
    let yaml = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms:
  doubler:
    type: sql
    sql: "SELECT id, val * 2 AS val FROM src"
    primary_key: id
sinks:
  pass_out:
    type: postgres
    from: src
    table: fan_passthrough
    schema: public
    primary_key: id
    on_conflict: update
  doubled_out:
    type: postgres
    from: doubler
    table: fan_doubled
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(recs.len() as u64))
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let pass: Vec<IdVal> = ctx
        .postgres
        .query("SELECT id, val FROM public.fan_passthrough ORDER BY id")
        .await
        .expect("query passthrough");
    let doubled: Vec<IdVal> = ctx
        .postgres
        .query("SELECT id, val FROM public.fan_doubled ORDER BY id")
        .await
        .expect("query doubled");

    assert_eq!(pass.len(), 10);
    assert_eq!(doubled.len(), 10);
    assert_eq!(pass[0].val, 1, "passthrough branch keeps val unchanged");
    assert_eq!(doubled[0].val, 2, "transform branch doubles val");
    assert_eq!(pass[9].val, 10);
    assert_eq!(doubled[9].val, 20);
}

// ===========================================================================
// Scenario 4: filter WHERE val > 100 over mixed values -> only matching rows
// ===========================================================================

#[tokio::test]
async fn filter_gt_100_mixed() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE filter_gt (id BIGINT PRIMARY KEY, val BIGINT NOT NULL)")
        .await
        .expect("create table");

    // vals: 50, 100, 150, 200, ... ; exactly the ones > 100 should pass.
    let recs: Vec<ValRec> = (1..=10).map(|i| ValRec { id: i, val: i * 50 }).collect();
    // > 100 means val in {150,200,250,...,500} -> ids 3..=10 => 8 rows.
    ctx.kafka.register_schema(VAL_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    let yaml = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
    filter: "val > 100"
transforms: {{}}
sinks:
  pg_out:
    type: postgres
    from: src
    table: filter_gt
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(8))
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let rows: Vec<IdVal> = ctx
        .postgres
        .query("SELECT id, val FROM public.filter_gt ORDER BY val")
        .await
        .expect("query");
    assert_eq!(rows.len(), 8, "only val > 100 should pass (8 rows)");
    assert!(
        rows.iter().all(|r| r.val > 100),
        "no row <= 100 may slip through"
    );
    assert_eq!(rows[0].val, 150, "smallest passing value is 150");
}

// ===========================================================================
// Scenario 5: filter matching NOTHING (WHERE val < 0, all-positive) -> 0 rows
// ===========================================================================

#[tokio::test]
async fn filter_matches_nothing_clean_exit() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE filter_none (id BIGINT PRIMARY KEY, val BIGINT NOT NULL)")
        .await
        .expect("create table");

    let recs: Vec<ValRec> = (1..=10).map(|i| ValRec { id: i, val: i }).collect();
    ctx.kafka.register_schema(VAL_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    // Filter excludes everything. record_limit counts records that PASS the
    // filter; since none pass, the run is driven to completion by consuming all
    // source records. Use raw + a tight check so we never hang the suite.
    let yaml = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
    filter: "val < 0"
transforms: {{}}
sinks:
  pg_out:
    type: postgres
    from: src
    table: filter_none
    schema: public
    primary_key: id
    on_conflict: update
  bh_out:
    type: blackhole
    from: src
"#,
        topic = ctx.kafka_topic,
    );

    // All records are filtered out, so the (unbounded) Kafka source never
    // reaches record_limit and runs until the timeout — expected here. Bound it
    // short and tolerate the timeout; the invariant is that NO rows leak and
    // nothing panics.
    let opts = base_opts()
        .record_limit(recs.len() as u64)
        .timeout(std::time::Duration::from_secs(15));
    if let Ok(out) = ctx.run_pipeline_raw(&yaml, opts).await {
        assert!(
            !out.stderr.contains("panicked"),
            "no panic on all-filtered-out input: {}",
            out.stderr
        );
    }

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.filter_none")
        .await
        .expect("count");
    assert_eq!(count, 0, "filter matching nothing must produce 0 rows");
}

// ===========================================================================
// Scenario 6: filter NULL comparison (opt_col IS NOT NULL) -> only non-null
// ===========================================================================

#[tokio::test]
async fn filter_is_not_null() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE filter_notnull (id BIGINT PRIMARY KEY, opt_col BIGINT)")
        .await
        .expect("create table");

    // Even ids have a value, odd ids are NULL. ids 2,4,6,8,10 => 5 non-null.
    let recs: Vec<OptRec> = (1..=10)
        .map(|i| OptRec {
            id: i,
            opt_col: if i % 2 == 0 { Some(i * 7) } else { None },
        })
        .collect();
    ctx.kafka.register_schema(OPT_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    let yaml = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
    filter: "opt_col IS NOT NULL"
transforms: {{}}
sinks:
  pg_out:
    type: postgres
    from: src
    table: filter_notnull
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(5))
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let rows: Vec<IdOpt> = ctx
        .postgres
        .query("SELECT id, opt_col FROM public.filter_notnull ORDER BY id")
        .await
        .expect("query");
    assert_eq!(rows.len(), 5, "only non-null rows should pass");
    assert!(
        rows.iter().all(|r| r.opt_col.is_some()),
        "no NULL row may pass IS NOT NULL"
    );
}

// ===========================================================================
// Scenario 7: SQL int + double coercion: SELECT id, i_col + d_col AS total
// ===========================================================================

#[tokio::test]
async fn sql_int_plus_double_coercion() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute(
            "CREATE TABLE coerce_total (id BIGINT PRIMARY KEY, total DOUBLE PRECISION NOT NULL)",
        )
        .await
        .expect("create table");

    let recs = vec![
        MixRec {
            id: 1,
            i_col: 10,
            d_col: 0.5,
        },
        MixRec {
            id: 2,
            i_col: 100,
            d_col: 2.25,
        },
        MixRec {
            id: 3,
            i_col: -5,
            d_col: 1.5,
        },
    ];
    ctx.kafka.register_schema(MIX_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    // Alias `total` matches CREATE TABLE column and the SELECT below.
    let yaml = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms:
  coerce:
    type: sql
    sql: "SELECT id, i_col + d_col AS total FROM src"
    primary_key: id
sinks:
  pg_out:
    type: postgres
    from: coerce
    table: coerce_total
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(recs.len() as u64))
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let rows: Vec<IdDouble> = ctx
        .postgres
        .query("SELECT id, total FROM public.coerce_total ORDER BY id")
        .await
        .expect("query");
    assert_eq!(rows.len(), 3);
    assert!(
        (rows[0].total - 10.5).abs() < 1e-9,
        "10 + 0.5 = 10.5, got {}",
        rows[0].total
    );
    assert!(
        (rows[1].total - 102.25).abs() < 1e-9,
        "100 + 2.25 = 102.25, got {}",
        rows[1].total
    );
    assert!(
        (rows[2].total - (-3.5)).abs() < 1e-9,
        "-5 + 1.5 = -3.5, got {}",
        rows[2].total
    );
}

// ===========================================================================
// Scenario 8: SQL CAST + concat: SELECT id, CAST(i_col AS VARCHAR) || '!' AS label
// ===========================================================================

#[tokio::test]
async fn sql_cast_int_to_varchar_concat() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE cast_label (id BIGINT PRIMARY KEY, label TEXT NOT NULL)")
        .await
        .expect("create table");

    let recs = vec![
        MixRec {
            id: 1,
            i_col: 42,
            d_col: 0.0,
        },
        MixRec {
            id: 2,
            i_col: 0,
            d_col: 0.0,
        },
        MixRec {
            id: 3,
            i_col: -7,
            d_col: 0.0,
        },
    ];
    ctx.kafka.register_schema(MIX_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    // Alias `label` matches CREATE TABLE column and the SELECT below.
    let yaml = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms:
  labeler:
    type: sql
    sql: "SELECT id, CAST(i_col AS VARCHAR) || '!' AS label FROM src"
    primary_key: id
sinks:
  pg_out:
    type: postgres
    from: labeler
    table: cast_label
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(recs.len() as u64))
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let rows: Vec<IdLabel> = ctx
        .postgres
        .query("SELECT id, label FROM public.cast_label ORDER BY id")
        .await
        .expect("query");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].label, "42!");
    assert_eq!(rows[1].label, "0!");
    assert_eq!(rows[2].label, "-7!");
}

// ===========================================================================
// Scenario 9: integer overflow: SELECT id, big_col * big_col AS prod
//   big_col near i64::MAX. Behavior may be error OR wrap; assert NO panic.
// ===========================================================================

#[tokio::test]
async fn sql_integer_overflow_no_panic() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    // NUMERIC(40,0) is generous enough to hold a non-overflowed product if the
    // engine widens; if it errors instead, we never read the table.
    ctx.postgres
        .execute("CREATE TABLE overflow_prod (id BIGINT PRIMARY KEY, prod NUMERIC(40,0) NOT NULL)")
        .await
        .expect("create table");

    let recs = vec![
        BigRec {
            id: 1,
            big_col: i64::MAX,
        },
        BigRec {
            id: 2,
            big_col: i64::MAX / 2,
        },
        BigRec {
            id: 3,
            big_col: 3_037_000_500,
        }, // ~sqrt(i64::MAX); square overflows i64
    ];
    ctx.kafka.register_schema(BIG_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    // Alias `prod` matches CREATE TABLE column.
    let yaml = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms:
  squarer:
    type: sql
    sql: "SELECT id, big_col * big_col AS prod FROM src"
    primary_key: id
sinks:
  pg_out:
    type: postgres
    from: squarer
    table: overflow_prod
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );

    let out = ctx
        .run_pipeline_raw(&yaml, base_opts().record_limit(recs.len() as u64))
        .await
        .expect("pipeline run");

    // The contract under test: i64*i64 overflow must NEVER panic the process.
    // It may error cleanly (overflow detected) or wrap/widen — both acceptable.
    assert!(
        !out.stderr.contains("panicked"),
        "i64 overflow must not panic: {}",
        out.stderr
    );
}

// ===========================================================================
// Scenario 10: divide by zero: SELECT id, i_col / 0 AS q
//   Expect a clean error (!success), assert NO panic.
// ===========================================================================

#[tokio::test]
async fn sql_divide_by_zero_clean_error() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE div_zero (id BIGINT PRIMARY KEY, q BIGINT NOT NULL)")
        .await
        .expect("create table");

    let recs = vec![
        MixRec {
            id: 1,
            i_col: 10,
            d_col: 0.0,
        },
        MixRec {
            id: 2,
            i_col: 20,
            d_col: 0.0,
        },
    ];
    ctx.kafka.register_schema(MIX_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    // Alias `q` matches CREATE TABLE column.
    let yaml = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms:
  divider:
    type: sql
    sql: "SELECT id, i_col / 0 AS q FROM src"
    primary_key: id
sinks:
  pg_out:
    type: postgres
    from: divider
    table: div_zero
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );

    let out = ctx
        .run_pipeline_raw(&yaml, base_opts().record_limit(recs.len() as u64))
        .await
        .expect("pipeline run");

    assert!(
        !out.stderr.contains("panicked"),
        "divide-by-zero must not panic: {}",
        out.stderr
    );
    assert!(
        !out.status.success(),
        "divide-by-zero should surface as a clean pipeline error, not silent success"
    );
}

// ===========================================================================
// Scenario 11: SELECT * passthrough transform -> all columns preserved
// ===========================================================================

#[tokio::test]
async fn sql_select_star_passthrough() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute(
            "CREATE TABLE star_pass (id BIGINT PRIMARY KEY, i_col BIGINT NOT NULL, d_col DOUBLE PRECISION NOT NULL)",
        )
        .await
        .expect("create table");

    let recs = vec![
        MixRec {
            id: 1,
            i_col: 11,
            d_col: 1.5,
        },
        MixRec {
            id: 2,
            i_col: 22,
            d_col: 2.5,
        },
        MixRec {
            id: 3,
            i_col: 33,
            d_col: 3.5,
        },
    ];
    ctx.kafka.register_schema(MIX_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    let yaml = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms:
  passthrough:
    type: sql
    sql: "SELECT * FROM src"
    primary_key: id
sinks:
  pg_out:
    type: postgres
    from: passthrough
    table: star_pass
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(recs.len() as u64))
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    #[derive(Debug, FromRow, Deserialize)]
    struct StarRow {
        #[allow(dead_code)]
        id: i64,
        i_col: i64,
        d_col: f64,
    }

    let rows: Vec<StarRow> = ctx
        .postgres
        .query("SELECT id, i_col, d_col FROM public.star_pass ORDER BY id")
        .await
        .expect("query");
    assert_eq!(rows.len(), 3, "SELECT * must preserve every row");
    assert_eq!(rows[0].i_col, 11);
    assert!((rows[0].d_col - 1.5).abs() < 1e-9);
    assert_eq!(rows[2].i_col, 33);
    assert!(
        (rows[2].d_col - 3.5).abs() < 1e-9,
        "all columns must be preserved"
    );
}

// ===========================================================================
// Scenario 12: column rename: SELECT id, data AS renamed FROM src
// ===========================================================================

#[tokio::test]
async fn sql_column_rename() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE rename_tbl (id BIGINT PRIMARY KEY, renamed TEXT NOT NULL)")
        .await
        .expect("create table");

    let recs: Vec<DataRec> = (1..=5)
        .map(|i| DataRec {
            id: i,
            data: format!("payload_{}", i),
        })
        .collect();
    ctx.kafka.register_schema(DATA_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    // Alias `renamed` matches CREATE TABLE column and the SELECT below.
    let yaml = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms:
  renamer:
    type: sql
    sql: "SELECT id, data AS renamed FROM src"
    primary_key: id
sinks:
  pg_out:
    type: postgres
    from: renamer
    table: rename_tbl
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(recs.len() as u64))
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let rows: Vec<IdRenamed> = ctx
        .postgres
        .query("SELECT id, renamed FROM public.rename_tbl ORDER BY id")
        .await
        .expect("query");
    assert_eq!(rows.len(), 5);
    assert_eq!(
        rows[0].renamed, "payload_1",
        "renamed column must hold original `data`"
    );
    assert_eq!(rows[4].renamed, "payload_5");
}
