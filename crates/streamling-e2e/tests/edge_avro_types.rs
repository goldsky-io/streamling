//! Adversarial e2e tests for NON-decimal primitive avro types through the
//! Kafka(Avro) -> Arrow decode -> Postgres sink pipeline.
//!
//! Where `edge_decimal_boundaries.rs` probes decimal precision/scale routing,
//! this file probes the *other* primitives: long/int boundary values, f64
//! special values, booleans, and a battery of nasty strings (unicode, emoji,
//! SQL-injection-shaped, control chars, very long, normalization edges).
//!
//! Every test produces real avro input from a FLAT `#[derive(Serialize)]`
//! struct whose fields match a `const SCHEMA` avro record, registers the
//! schema, produces records, runs a kafka->postgres pipeline (no transforms),
//! and asserts on what landed in Postgres. Some assertions are intentionally
//! adversarial: a failure is a *finding* about the decode/sink path, not a
//! flaky test.

use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

/// Pipeline options copied from the decimal-boundary template: a generous
/// timeout and empty plugin config so the binary runs without external plugins.
fn base_opts() -> PipelineOpts {
    PipelineOpts::new()
        .timeout(std::time::Duration::from_secs(60))
        .env("STREAMLING__PLUGIN__PATH", "")
        .env("STREAMLING__PLUGIN__PREPROCESSOR_IDS", "")
        .env("STREAMLING__PLUGIN__SIDE_OUTPUT_IDS", "")
}

/// Build a kafka -> postgres pipeline YAML for a given topic + target table.
/// No transforms; primary_key `id` on both source and sink; upsert on conflict.
fn pipeline_yaml(topic: &str, table: &str) -> String {
    format!(
        r#"
sources:
  src_in:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms: {{}}
sinks:
  pg_out:
    type: postgres
    from: src_in
    table: {table}
    schema: public
    primary_key: id
    on_conflict: update
"#
    )
}

// ---------------------------------------------------------------------------
// Scenario 1: i64 (avro long) boundary values
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
struct LongRec {
    id: i64,
    val: i64,
}

const LONG_SCHEMA: &str = r#"{
    "type": "record",
    "name": "LongRec",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "val", "type": "long"}
    ]
}"#;

#[derive(Debug, FromRow, Deserialize)]
struct IdValLong {
    #[allow(dead_code)]
    id: i64,
    val: i64,
}

#[tokio::test]
async fn long_boundaries_max_min_zero_neg() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE long_bounds (id BIGINT PRIMARY KEY, val BIGINT NOT NULL)")
        .await
        .expect("create table");

    let recs = vec![
        LongRec {
            id: 1,
            val: i64::MAX,
        },
        LongRec {
            id: 2,
            val: i64::MIN,
        },
        LongRec { id: 3, val: 0 },
        LongRec { id: 4, val: -1 },
    ];

    ctx.kafka.register_schema(LONG_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "long_bounds"),
            base_opts().record_limit(recs.len() as u64),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let rows: Vec<IdValLong> = ctx
        .postgres
        .query("SELECT id, val FROM public.long_bounds ORDER BY id")
        .await
        .expect("query");

    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].val, i64::MAX, "i64::MAX must round-trip exactly");
    assert_eq!(rows[1].val, i64::MIN, "i64::MIN must round-trip exactly");
    assert_eq!(rows[2].val, 0);
    assert_eq!(rows[3].val, -1);
}

// ---------------------------------------------------------------------------
// Scenario 2: i32 (avro int) boundary values
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
struct IntRec {
    id: i64,
    val: i32,
}

const INT_SCHEMA: &str = r#"{
    "type": "record",
    "name": "IntRec",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "val", "type": "int"}
    ]
}"#;

#[derive(Debug, FromRow, Deserialize)]
struct IdValInt {
    #[allow(dead_code)]
    id: i64,
    val: i32,
}

#[tokio::test]
async fn int_boundaries_max_min() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE int_bounds (id BIGINT PRIMARY KEY, val INTEGER NOT NULL)")
        .await
        .expect("create table");

    let recs = vec![
        IntRec {
            id: 1,
            val: i32::MAX,
        },
        IntRec {
            id: 2,
            val: i32::MIN,
        },
        IntRec { id: 3, val: 0 },
    ];

    ctx.kafka.register_schema(INT_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "int_bounds"),
            base_opts().record_limit(recs.len() as u64),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let rows: Vec<IdValInt> = ctx
        .postgres
        .query("SELECT id, val FROM public.int_bounds ORDER BY id")
        .await
        .expect("query");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].val, i32::MAX, "i32::MAX must round-trip exactly");
    assert_eq!(rows[1].val, i32::MIN, "i32::MIN must round-trip exactly");
    assert_eq!(rows[2].val, 0);
}

// ---------------------------------------------------------------------------
// f64 (avro double) records reused for scenarios 3 and 4
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
struct DoubleRec {
    id: i64,
    val: f64,
}

const DOUBLE_SCHEMA: &str = r#"{
    "type": "record",
    "name": "DoubleRec",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "val", "type": "double"}
    ]
}"#;

#[derive(Debug, FromRow, Deserialize)]
struct IdValDouble {
    #[allow(dead_code)]
    id: i64,
    val: f64,
}

#[derive(Debug, FromRow, Deserialize)]
struct IdText {
    #[allow(dead_code)]
    id: i64,
    t: String,
}

// ---------------------------------------------------------------------------
// Scenario 3: f64 special / extreme magnitude values
// ---------------------------------------------------------------------------

#[tokio::test]
async fn double_special_magnitudes() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE dbl_special (id BIGINT PRIMARY KEY, val DOUBLE PRECISION NOT NULL)")
        .await
        .expect("create table");

    let recs = vec![
        DoubleRec { id: 1, val: 1e308 },  // near f64::MAX
        DoubleRec { id: 2, val: 1e-308 }, // near smallest normal
        DoubleRec { id: 3, val: -0.0 },   // negative zero
        DoubleRec { id: 4, val: 0.0 },    // positive zero
    ];

    ctx.kafka.register_schema(DOUBLE_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "dbl_special"),
            base_opts().record_limit(recs.len() as u64),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let rows: Vec<IdValDouble> = ctx
        .postgres
        .query("SELECT id, val FROM public.dbl_special ORDER BY id")
        .await
        .expect("query");

    assert_eq!(rows.len(), 4);
    // Large/small magnitudes: compare with relative tolerance.
    assert!(
        (rows[0].val - 1e308).abs() <= 1e308 * 1e-12,
        "1e308 lost: {}",
        rows[0].val
    );
    assert!(
        (rows[1].val - 1e-308).abs() <= 1e-308 * 1e-6,
        "1e-308 lost: {}",
        rows[1].val
    );
    // -0.0 == 0.0 numerically; both must read back as zero.
    assert_eq!(
        rows[2].val, 0.0,
        "negative zero should compare equal to zero"
    );
    assert_eq!(rows[3].val, 0.0);
}

// ---------------------------------------------------------------------------
// Scenario 4: f64 with many significant decimals (round-trip fidelity)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn double_many_decimals_roundtrip() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE dbl_pi (id BIGINT PRIMARY KEY, val DOUBLE PRECISION NOT NULL)")
        .await
        .expect("create table");

    let pi = std::f64::consts::PI;
    let e = std::f64::consts::E;
    let weird = 0.1 + 0.2; // classic 0.30000000000000004
    let recs = vec![
        DoubleRec { id: 1, val: pi },
        DoubleRec { id: 2, val: e },
        DoubleRec { id: 3, val: weird },
        DoubleRec {
            id: 4,
            val: 123_456_789.123_456_79,
        },
    ];

    ctx.kafka.register_schema(DOUBLE_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "dbl_pi"),
            base_opts().record_limit(recs.len() as u64),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let rows: Vec<IdValDouble> = ctx
        .postgres
        .query("SELECT id, val FROM public.dbl_pi ORDER BY id")
        .await
        .expect("query");

    assert_eq!(rows.len(), 4);
    // IEEE-754 double should survive bit-for-bit through avro double + PG double.
    assert_eq!(rows[0].val, pi, "pi must round-trip bit-exact");
    assert_eq!(rows[1].val, e, "e must round-trip bit-exact");
    assert_eq!(rows[2].val, weird, "0.1+0.2 must round-trip bit-exact");
    assert_eq!(rows[3].val, 123_456_789.123_456_79);
}

// ---------------------------------------------------------------------------
// Scenario 5: boolean true/false round-trip
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
struct BoolRec {
    id: i64,
    flag: bool,
}

const BOOL_SCHEMA: &str = r#"{
    "type": "record",
    "name": "BoolRec",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "flag", "type": "boolean"}
    ]
}"#;

#[derive(Debug, FromRow, Deserialize)]
struct IdFlag {
    #[allow(dead_code)]
    id: i64,
    flag: bool,
}

#[tokio::test]
async fn boolean_true_false_roundtrip() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE bool_rt (id BIGINT PRIMARY KEY, flag BOOLEAN NOT NULL)")
        .await
        .expect("create table");

    let recs = vec![
        BoolRec { id: 1, flag: true },
        BoolRec { id: 2, flag: false },
    ];

    ctx.kafka.register_schema(BOOL_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "bool_rt"),
            base_opts().record_limit(recs.len() as u64),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let rows: Vec<IdFlag> = ctx
        .postgres
        .query("SELECT id, flag FROM public.bool_rt ORDER BY id")
        .await
        .expect("query");

    assert_eq!(rows.len(), 2);
    assert!(rows[0].flag, "id=1 should be true");
    assert!(!rows[1].flag, "id=2 should be false");
}

// ---------------------------------------------------------------------------
// String records reused for several scenarios (NOT NULL string)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
struct StrRec {
    id: i64,
    s: String,
}

const STR_SCHEMA: &str = r#"{
    "type": "record",
    "name": "StrRec",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "s", "type": "string"}
    ]
}"#;

#[derive(Debug, FromRow, Deserialize)]
struct IdStr {
    #[allow(dead_code)]
    id: i64,
    s: String,
}

// ---------------------------------------------------------------------------
// Scenario 6: unicode string — emoji + CJK + accents
// ---------------------------------------------------------------------------

#[tokio::test]
async fn string_unicode_emoji_cjk_accents() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE str_unicode (id BIGINT PRIMARY KEY, s TEXT NOT NULL)")
        .await
        .expect("create table");

    let s1 = "héllo 世界 🚀".to_string();
    let s2 = "Ωμέγα café ✅ 日本語 𝔘𝔫𝔦𝔠𝔬𝔡𝔢".to_string();
    let recs = vec![
        StrRec {
            id: 1,
            s: s1.clone(),
        },
        StrRec {
            id: 2,
            s: s2.clone(),
        },
    ];

    ctx.kafka.register_schema(STR_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "str_unicode"),
            base_opts().record_limit(recs.len() as u64),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let rows: Vec<IdStr> = ctx
        .postgres
        .query("SELECT id, s FROM public.str_unicode ORDER BY id")
        .await
        .expect("query");

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].s, s1,
        "emoji+CJK+accents must round-trip byte-exact"
    );
    assert_eq!(rows[1].s, s2);
}

// ---------------------------------------------------------------------------
// Scenario 7: empty string vs NULL (nullable string) — distinction preserved
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
struct OptStrRec {
    id: i64,
    s: Option<String>,
}

const OPT_STR_SCHEMA: &str = r#"{
    "type": "record",
    "name": "OptStrRec",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "s", "type": ["null", "string"], "default": null}
    ]
}"#;

#[derive(Debug, FromRow, Deserialize)]
struct IdOptStr {
    #[allow(dead_code)]
    id: i64,
    s: Option<String>,
}

#[tokio::test]
async fn string_empty_vs_null_distinction() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE str_empty_null (id BIGINT PRIMARY KEY, s TEXT)")
        .await
        .expect("create table");

    let recs = vec![
        OptStrRec {
            id: 1,
            s: Some(String::new()),
        }, // empty string
        OptStrRec { id: 2, s: None }, // SQL NULL
        OptStrRec {
            id: 3,
            s: Some("x".to_string()),
        },
    ];

    ctx.kafka.register_schema(OPT_STR_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "str_empty_null"),
            base_opts().record_limit(recs.len() as u64),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let rows: Vec<IdOptStr> = ctx
        .postgres
        .query("SELECT id, s FROM public.str_empty_null ORDER BY id")
        .await
        .expect("query");

    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0].s,
        Some(String::new()),
        "empty string must stay empty string, not become NULL"
    );
    assert_eq!(
        rows[1].s, None,
        "absent value must stay SQL NULL, not become ''"
    );
    assert_eq!(rows[2].s, Some("x".to_string()));
}

// ---------------------------------------------------------------------------
// Scenario 8: very long string (10_000 chars) — length preserved
// ---------------------------------------------------------------------------

#[tokio::test]
async fn string_very_long_10k() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE str_long (id BIGINT PRIMARY KEY, s TEXT NOT NULL)")
        .await
        .expect("create table");

    let big = "a".repeat(10_000);
    let recs = vec![StrRec {
        id: 1,
        s: big.clone(),
    }];

    ctx.kafka.register_schema(STR_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "str_long"),
            base_opts().record_limit(recs.len() as u64),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let rows: Vec<IdStr> = ctx
        .postgres
        .query("SELECT id, s FROM public.str_long ORDER BY id")
        .await
        .expect("query");

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].s.len(),
        10_000,
        "10k-char string must not be truncated"
    );
    assert_eq!(rows[0].s, big);
}

// ---------------------------------------------------------------------------
// Scenario 9: SQL-significant characters (data-as-injection must be parameterized)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn string_sql_injection_shaped() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE str_inject (id BIGINT PRIMARY KEY, s TEXT NOT NULL)")
        .await
        .expect("create table");

    // Values that would break naive string-concatenated SQL.
    let cases = [
        "Robert'); DROP TABLE str_inject;--".to_string(),
        "O'Brien said \"hello\"".to_string(),
        r"back\slash and ; semicolon".to_string(),
        "percent %s and %d format".to_string(),
        "100% done -- not a comment".to_string(),
        "$$dollar quoting$$ and $1 bind".to_string(),
    ];
    let recs: Vec<StrRec> = cases
        .iter()
        .enumerate()
        .map(|(i, s)| StrRec {
            id: i as i64 + 1,
            s: s.clone(),
        })
        .collect();

    ctx.kafka.register_schema(STR_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "str_inject"),
            base_opts().record_limit(recs.len() as u64),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    // The table must still exist (DROP TABLE injection did NOT execute) and hold all rows.
    let rows: Vec<IdStr> = ctx
        .postgres
        .query("SELECT id, s FROM public.str_inject ORDER BY id")
        .await
        .expect("query (table must still exist)");

    assert_eq!(
        rows.len(),
        cases.len(),
        "all injection-shaped rows must land safely"
    );
    for (i, expected) in cases.iter().enumerate() {
        assert_eq!(
            &rows[i].s,
            expected,
            "value {} must be stored verbatim",
            i + 1
        );
    }
}

// ---------------------------------------------------------------------------
// Scenario 10: control characters — newlines, tabs, literal "\0" escape text
// ---------------------------------------------------------------------------

#[tokio::test]
async fn string_control_chars_and_escapes() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE str_ctrl (id BIGINT PRIMARY KEY, s TEXT NOT NULL)")
        .await
        .expect("create table");

    let cases = [
        "line1\nline2\nline3".to_string(),
        "col1\tcol2\tcol3".to_string(),
        // Literal backslash-zero TEXT (not an actual NUL byte; PG TEXT rejects \0).
        r"literal \0 escape sequence".to_string(),
        "carriage\r\nreturn\tand mix".to_string(),
        // Vertical tab + form feed + bell (real control chars, but not NUL).
        "ctrl\u{0B}\u{0C}\u{07}chars".to_string(),
    ];
    let recs: Vec<StrRec> = cases
        .iter()
        .enumerate()
        .map(|(i, s)| StrRec {
            id: i as i64 + 1,
            s: s.clone(),
        })
        .collect();

    ctx.kafka.register_schema(STR_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "str_ctrl"),
            base_opts().record_limit(recs.len() as u64),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let rows: Vec<IdStr> = ctx
        .postgres
        .query("SELECT id, s FROM public.str_ctrl ORDER BY id")
        .await
        .expect("query");

    assert_eq!(rows.len(), cases.len());
    for (i, expected) in cases.iter().enumerate() {
        assert_eq!(
            &rows[i].s,
            expected,
            "control-char value {} must be byte-exact",
            i + 1
        );
    }
}

// ---------------------------------------------------------------------------
// Scenario 11: wide record — ~25 columns of mixed types — all land
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
struct WideRec {
    id: i64,
    c_long_1: i64,
    c_long_2: i64,
    c_int_1: i32,
    c_int_2: i32,
    c_dbl_1: f64,
    c_dbl_2: f64,
    c_bool_1: bool,
    c_bool_2: bool,
    c_str_1: String,
    c_str_2: String,
    c_str_3: String,
    c_opt_long: Option<i64>,
    c_opt_int: Option<i32>,
    c_opt_dbl: Option<f64>,
    c_opt_bool: Option<bool>,
    c_opt_str: Option<String>,
    c_long_3: i64,
    c_int_3: i32,
    c_dbl_3: f64,
    c_bool_3: bool,
    c_str_4: String,
    c_long_4: i64,
    c_int_4: i32,
    c_str_5: String,
}

const WIDE_SCHEMA: &str = r#"{
    "type": "record",
    "name": "WideRec",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "c_long_1", "type": "long"},
        {"name": "c_long_2", "type": "long"},
        {"name": "c_int_1", "type": "int"},
        {"name": "c_int_2", "type": "int"},
        {"name": "c_dbl_1", "type": "double"},
        {"name": "c_dbl_2", "type": "double"},
        {"name": "c_bool_1", "type": "boolean"},
        {"name": "c_bool_2", "type": "boolean"},
        {"name": "c_str_1", "type": "string"},
        {"name": "c_str_2", "type": "string"},
        {"name": "c_str_3", "type": "string"},
        {"name": "c_opt_long", "type": ["null", "long"], "default": null},
        {"name": "c_opt_int", "type": ["null", "int"], "default": null},
        {"name": "c_opt_dbl", "type": ["null", "double"], "default": null},
        {"name": "c_opt_bool", "type": ["null", "boolean"], "default": null},
        {"name": "c_opt_str", "type": ["null", "string"], "default": null},
        {"name": "c_long_3", "type": "long"},
        {"name": "c_int_3", "type": "int"},
        {"name": "c_dbl_3", "type": "double"},
        {"name": "c_bool_3", "type": "boolean"},
        {"name": "c_str_4", "type": "string"},
        {"name": "c_long_4", "type": "long"},
        {"name": "c_int_4", "type": "int"},
        {"name": "c_str_5", "type": "string"}
    ]
}"#;

#[derive(Debug, FromRow, Deserialize)]
#[allow(dead_code)]
struct WideRow {
    id: i64,
    c_long_1: i64,
    c_int_1: i32,
    c_dbl_1: f64,
    c_bool_1: bool,
    c_str_1: String,
    c_opt_long: Option<i64>,
    c_opt_str: Option<String>,
    c_str_5: String,
}

#[tokio::test]
async fn wide_record_mixed_types() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute(
            "CREATE TABLE wide_rec (
                id BIGINT PRIMARY KEY,
                c_long_1 BIGINT NOT NULL,
                c_long_2 BIGINT NOT NULL,
                c_int_1 INTEGER NOT NULL,
                c_int_2 INTEGER NOT NULL,
                c_dbl_1 DOUBLE PRECISION NOT NULL,
                c_dbl_2 DOUBLE PRECISION NOT NULL,
                c_bool_1 BOOLEAN NOT NULL,
                c_bool_2 BOOLEAN NOT NULL,
                c_str_1 TEXT NOT NULL,
                c_str_2 TEXT NOT NULL,
                c_str_3 TEXT NOT NULL,
                c_opt_long BIGINT,
                c_opt_int INTEGER,
                c_opt_dbl DOUBLE PRECISION,
                c_opt_bool BOOLEAN,
                c_opt_str TEXT,
                c_long_3 BIGINT NOT NULL,
                c_int_3 INTEGER NOT NULL,
                c_dbl_3 DOUBLE PRECISION NOT NULL,
                c_bool_3 BOOLEAN NOT NULL,
                c_str_4 TEXT NOT NULL,
                c_long_4 BIGINT NOT NULL,
                c_int_4 INTEGER NOT NULL,
                c_str_5 TEXT NOT NULL
            )",
        )
        .await
        .expect("create table");

    let recs = vec![WideRec {
        id: 1,
        c_long_1: i64::MAX,
        c_long_2: -42,
        c_int_1: i32::MIN,
        c_int_2: 7,
        c_dbl_1: std::f64::consts::PI,
        c_dbl_2: -1e100,
        c_bool_1: true,
        c_bool_2: false,
        c_str_1: "first".to_string(),
        c_str_2: "héllo 世界".to_string(),
        c_str_3: String::new(),
        c_opt_long: Some(999),
        c_opt_int: None,
        c_opt_dbl: Some(2.5),
        c_opt_bool: None,
        c_opt_str: Some("present".to_string()),
        c_long_3: 0,
        c_int_3: -1,
        c_dbl_3: 0.0,
        c_bool_3: true,
        c_str_4: "🚀".to_string(),
        c_long_4: i64::MIN,
        c_int_4: i32::MAX,
        c_str_5: "last".to_string(),
    }];

    ctx.kafka.register_schema(WIDE_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "wide_rec"),
            base_opts().record_limit(recs.len() as u64),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let rows: Vec<WideRow> = ctx
        .postgres
        .query(
            "SELECT id, c_long_1, c_int_1, c_dbl_1, c_bool_1, c_str_1, \
             c_opt_long, c_opt_str, c_str_5 FROM public.wide_rec ORDER BY id",
        )
        .await
        .expect("query");

    assert_eq!(rows.len(), 1, "wide record must produce exactly one row");
    let r = &rows[0];
    assert_eq!(r.c_long_1, i64::MAX);
    assert_eq!(r.c_int_1, i32::MIN);
    assert_eq!(r.c_dbl_1, std::f64::consts::PI);
    assert!(r.c_bool_1);
    assert_eq!(r.c_str_1, "first");
    assert_eq!(r.c_opt_long, Some(999));
    assert_eq!(r.c_opt_str, Some("present".to_string()));
    assert_eq!(r.c_str_5, "last", "the 25th column must also land");
}

// ---------------------------------------------------------------------------
// Scenario 12: Option fields across long/string/double — nulls preserved
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
struct OptMixRec {
    id: i64,
    n: Option<i64>,
    s: Option<String>,
    d: Option<f64>,
}

const OPT_MIX_SCHEMA: &str = r#"{
    "type": "record",
    "name": "OptMixRec",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "n", "type": ["null", "long"], "default": null},
        {"name": "s", "type": ["null", "string"], "default": null},
        {"name": "d", "type": ["null", "double"], "default": null}
    ]
}"#;

#[derive(Debug, FromRow, Deserialize)]
struct OptMixRow {
    #[allow(dead_code)]
    id: i64,
    n: Option<i64>,
    s: Option<String>,
    d: Option<f64>,
}

#[tokio::test]
async fn option_fields_nulls_not_defaulted() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute(
            "CREATE TABLE opt_mix (id BIGINT PRIMARY KEY, n BIGINT, s TEXT, d DOUBLE PRECISION)",
        )
        .await
        .expect("create table");

    // Half null, half present. Crucially id=2 has present-but-zero-ish values
    // and id=1/3 have NULLs — the sink must NOT collapse NULL -> 0 / "".
    let recs = vec![
        OptMixRec {
            id: 1,
            n: None,
            s: None,
            d: None,
        },
        OptMixRec {
            id: 2,
            n: Some(0),
            s: Some(String::new()),
            d: Some(0.0),
        },
        OptMixRec {
            id: 3,
            n: None,
            s: Some("only string".to_string()),
            d: None,
        },
        OptMixRec {
            id: 4,
            n: Some(123),
            s: None,
            d: Some(4.5),
        },
    ];

    ctx.kafka.register_schema(OPT_MIX_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "opt_mix"),
            base_opts().record_limit(recs.len() as u64),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let rows: Vec<OptMixRow> = ctx
        .postgres
        .query("SELECT id, n, s, d FROM public.opt_mix ORDER BY id")
        .await
        .expect("query");

    assert_eq!(rows.len(), 4);

    // id=1: all NULL — must stay NULL, never 0 / "".
    assert_eq!(rows[0].n, None, "NULL long must not become 0");
    assert_eq!(rows[0].s, None, "NULL string must not become ''");
    assert_eq!(rows[0].d, None, "NULL double must not become 0.0");

    // id=2: present zero values — must be Some(0), distinct from NULL above.
    assert_eq!(
        rows[1].n,
        Some(0),
        "present 0 must survive (not confused with NULL)"
    );
    assert_eq!(rows[1].s, Some(String::new()));
    assert_eq!(rows[1].d, Some(0.0));

    // id=3: only string present.
    assert_eq!(rows[2].n, None);
    assert_eq!(rows[2].s, Some("only string".to_string()));
    assert_eq!(rows[2].d, None);

    // id=4: long + double present, string NULL.
    assert_eq!(rows[3].n, Some(123));
    assert_eq!(rows[3].s, None);
    assert_eq!(rows[3].d, Some(4.5));
}

// ---------------------------------------------------------------------------
// Scenario 13: unicode normalization edge — combining marks + ZWJ — bytes preserved
// ---------------------------------------------------------------------------

#[tokio::test]
async fn string_unicode_normalization_edges() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE str_norm (id BIGINT PRIMARY KEY, s TEXT NOT NULL)")
        .await
        .expect("create table");

    // "é" as base 'e' + COMBINING ACUTE ACCENT (NFD) — must NOT be silently
    // normalized to the single precomposed codepoint (NFC).
    let nfd = "e\u{0301}".to_string(); // e + combining acute
    let nfc = "\u{00E9}".to_string(); // precomposed é
                                      // Family emoji via ZERO WIDTH JOINER sequence.
    let zwj = "\u{1F468}\u{200D}\u{1F469}\u{200D}\u{1F467}\u{200D}\u{1F466}".to_string();
    // Standalone zero-width joiner + zero-width space, surrounded by text.
    let zw = "a\u{200D}b\u{200B}c".to_string();

    let recs = vec![
        StrRec {
            id: 1,
            s: nfd.clone(),
        },
        StrRec {
            id: 2,
            s: nfc.clone(),
        },
        StrRec {
            id: 3,
            s: zwj.clone(),
        },
        StrRec {
            id: 4,
            s: zw.clone(),
        },
    ];

    ctx.kafka.register_schema(STR_SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "str_norm"),
            base_opts().record_limit(recs.len() as u64),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    // Read raw bytes via octet_length too, to detect any normalization that
    // changes the byte count.
    let rows: Vec<IdStr> = ctx
        .postgres
        .query("SELECT id, s FROM public.str_norm ORDER BY id")
        .await
        .expect("query");
    let byte_lens: Vec<IdText> = ctx
        .postgres
        .query("SELECT id, octet_length(s)::text AS t FROM public.str_norm ORDER BY id")
        .await
        .expect("query lengths");

    assert_eq!(rows.len(), 4);
    assert_eq!(
        rows[0].s, nfd,
        "NFD (decomposed) form must be preserved, not normalized to NFC"
    );
    assert_eq!(rows[1].s, nfc, "NFC (precomposed) form must be preserved");
    assert_ne!(
        rows[0].s, rows[1].s,
        "NFD and NFC must remain distinct (no silent normalization)"
    );
    assert_eq!(rows[2].s, zwj, "ZWJ emoji sequence must survive byte-exact");
    assert_eq!(
        rows[3].s, zw,
        "zero-width joiner/space must survive byte-exact"
    );

    // NFD "é" is 3 bytes (e + 2-byte combining mark); NFC "é" is 2 bytes.
    assert_eq!(byte_lens[0].t, nfd.len().to_string());
    assert_eq!(byte_lens[1].t, nfc.len().to_string());
}
