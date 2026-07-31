//! Adversarial e2e tests: general value / throughput edges through the
//! Kafka(Avro) -> (optional SQL) -> Postgres pipeline.
//!
//! Probes large batches, duplicate/unicode primary keys, integer & float
//! extremes, string transforms, and filter edges — looking for runtime errors
//! or silent failures. Failures here are findings.

use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

fn base_opts() -> PipelineOpts {
    PipelineOpts::new()
        .timeout(std::time::Duration::from_secs(60))
        .env("STREAMLING__PLUGIN__PATH", "")
        .env("STREAMLING__PLUGIN__PREPROCESSOR_IDS", "")
        .env("STREAMLING__PLUGIN__SIDE_OUTPUT_IDS", "")
}

// ---- record shapes + matching avro schemas ----

#[derive(Debug, Clone, Serialize)]
struct LongRec {
    id: i64,
    val: i64,
}
const LONG_SCHEMA: &str = r#"{"type":"record","name":"LongRec","fields":[
    {"name":"id","type":"long"},{"name":"val","type":"long"}]}"#;

#[derive(Debug, Clone, Serialize)]
struct StrRec {
    id: String,
    data: String,
}
const STR_SCHEMA: &str = r#"{"type":"record","name":"StrRec","fields":[
    {"name":"id","type":"string"},{"name":"data","type":"string"}]}"#;

#[derive(Debug, Clone, Serialize)]
struct DblRec {
    id: i64,
    v: f64,
}
const DBL_SCHEMA: &str = r#"{"type":"record","name":"DblRec","fields":[
    {"name":"id","type":"long"},{"name":"v","type":"double"}]}"#;

#[derive(Debug, Clone, Serialize)]
struct MixRec {
    id: i64,
    i_col: i64,
    d_col: f64,
}
const MIX_SCHEMA: &str = r#"{"type":"record","name":"MixRec","fields":[
    {"name":"id","type":"long"},{"name":"i_col","type":"long"},{"name":"d_col","type":"double"}]}"#;

#[derive(Debug, FromRow, Deserialize)]
struct StrRow {
    #[allow(dead_code)]
    id: String,
    data: String,
}

#[derive(Debug, FromRow)]
struct LongValRow {
    #[allow(dead_code)]
    id: i64,
    val: i64,
}

#[derive(Debug, FromRow)]
struct DblRow {
    #[allow(dead_code)]
    id: i64,
    v: f64,
}

#[derive(Debug, FromRow)]
struct TotalRow {
    #[allow(dead_code)]
    id: i64,
    total: f64,
}

// ---------------------------------------------------------------------------

/// 1. Large batch: 1000 records must all land (batch-boundary / throughput).
#[tokio::test]
async fn large_batch_1000_records() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.postgres
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, val BIGINT NOT NULL)")
        .await
        .unwrap();
    ctx.kafka.register_schema(LONG_SCHEMA).await.unwrap();
    let recs: Vec<LongRec> = (0..1000).map(|i| LongRec { id: i, val: i * 2 }).collect();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();

    let yaml = pg_passthrough(&ctx.kafka_topic, "t");
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(1000))
        .await
        .unwrap();
    assert!(status.success());
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.t")
        .await
        .unwrap();
    assert_eq!(count, 1000, "all 1000 records should land");
}

/// 2. A single record.
#[tokio::test]
async fn single_record() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.postgres
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, val BIGINT NOT NULL)")
        .await
        .unwrap();
    ctx.kafka.register_schema(LONG_SCHEMA).await.unwrap();
    ctx.kafka
        .produce_avro_records(&[LongRec { id: 42, val: 7 }])
        .await
        .unwrap();
    let yaml = pg_passthrough(&ctx.kafka_topic, "t");
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(1))
        .await
        .unwrap();
    assert!(status.success());
    assert_eq!(
        ctx.postgres
            .count("SELECT COUNT(*) FROM public.t")
            .await
            .unwrap(),
        1
    );
}

/// 3. Duplicate primary keys within one batch + on_conflict update -> last wins.
#[tokio::test]
async fn duplicate_pk_in_batch_last_wins() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.postgres
        .execute("CREATE TABLE t (id TEXT PRIMARY KEY, data TEXT NOT NULL)")
        .await
        .unwrap();
    ctx.kafka.register_schema(STR_SCHEMA).await.unwrap();
    let recs: Vec<StrRec> = (0..5)
        .map(|i| StrRec {
            id: "k".into(),
            data: format!("v{i}"),
        })
        .collect();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();
    let yaml = pg_passthrough_str(&ctx.kafka_topic, "t");
    // 5 records share one primary key -> in-pipeline dedup collapses them to a
    // single sink write, so record_limit(5) may never be reached. Bound the
    // timeout and assert the final table state (which is well-defined either way:
    // dedup keeps the last, and on_conflict update would also leave the last).
    let opts = base_opts()
        .record_limit(5)
        .timeout(std::time::Duration::from_secs(15));
    let _ = ctx.run_pipeline_raw(&yaml, opts).await;
    assert_eq!(
        ctx.postgres
            .count("SELECT COUNT(*) FROM public.t")
            .await
            .unwrap(),
        1
    );
    let rows: Vec<StrRow> = ctx
        .postgres
        .query("SELECT id, data FROM public.t")
        .await
        .unwrap();
    assert_eq!(rows[0].data, "v4", "last duplicate should win");
}

/// 4. i64::MAX / i64::MIN pass straight through unchanged.
#[tokio::test]
async fn long_extremes_passthrough() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.postgres
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, val BIGINT NOT NULL)")
        .await
        .unwrap();
    ctx.kafka.register_schema(LONG_SCHEMA).await.unwrap();
    ctx.kafka
        .produce_avro_records(&[
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
        ])
        .await
        .unwrap();
    let yaml = pg_passthrough(&ctx.kafka_topic, "t");
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(4))
        .await
        .unwrap();
    assert!(status.success());
    let rows: Vec<LongValRow> = ctx
        .postgres
        .query("SELECT id, val FROM public.t ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows[0].val, i64::MAX);
    assert_eq!(rows[1].val, i64::MIN);
}

/// 5. Float extremes (avro double): MAX, smallest positive, signed zeros, large negative.
#[tokio::test]
async fn double_extremes() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.postgres
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v DOUBLE PRECISION NOT NULL)")
        .await
        .unwrap();
    ctx.kafka.register_schema(DBL_SCHEMA).await.unwrap();
    ctx.kafka
        .produce_avro_records(&[
            DblRec { id: 1, v: f64::MAX },
            DblRec {
                id: 2,
                v: f64::MIN_POSITIVE,
            },
            DblRec { id: 3, v: 0.0 },
            DblRec { id: 4, v: -0.0 },
            DblRec { id: 5, v: -1.0e308 },
        ])
        .await
        .unwrap();
    let yaml = pg_passthrough_dbl(&ctx.kafka_topic, "t");
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(5))
        .await
        .unwrap();
    assert!(status.success());
    let rows: Vec<DblRow> = ctx
        .postgres
        .query("SELECT id, v FROM public.t ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows[0].v, f64::MAX, "f64::MAX must round-trip");
    assert_eq!(rows[4].v, -1.0e308);
}

/// 6. String primary keys that are empty / whitespace must stay distinct.
#[tokio::test]
async fn string_pk_empty_and_whitespace() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.postgres
        .execute("CREATE TABLE t (id TEXT PRIMARY KEY, data TEXT NOT NULL)")
        .await
        .unwrap();
    ctx.kafka.register_schema(STR_SCHEMA).await.unwrap();
    ctx.kafka
        .produce_avro_records(&[
            StrRec {
                id: "".into(),
                data: "empty".into(),
            },
            StrRec {
                id: " ".into(),
                data: "space".into(),
            },
            StrRec {
                id: "\t".into(),
                data: "tab".into(),
            },
        ])
        .await
        .unwrap();
    let yaml = pg_passthrough_str(&ctx.kafka_topic, "t");
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(3))
        .await
        .unwrap();
    assert!(status.success());
    assert_eq!(
        ctx.postgres
            .count("SELECT COUNT(*) FROM public.t")
            .await
            .unwrap(),
        3,
        "empty/space/tab pks must be distinct"
    );
}

/// 7. Unicode primary keys must stay distinct and intact.
#[tokio::test]
async fn unicode_primary_keys() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.postgres
        .execute("CREATE TABLE t (id TEXT PRIMARY KEY, data TEXT NOT NULL)")
        .await
        .unwrap();
    ctx.kafka.register_schema(STR_SCHEMA).await.unwrap();
    ctx.kafka
        .produce_avro_records(&[
            StrRec {
                id: "café".into(),
                data: "a".into(),
            },
            StrRec {
                id: "日本".into(),
                data: "b".into(),
            },
            StrRec {
                id: "🚀".into(),
                data: "c".into(),
            },
        ])
        .await
        .unwrap();
    let yaml = pg_passthrough_str(&ctx.kafka_topic, "t");
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(3))
        .await
        .unwrap();
    assert!(status.success());
    assert_eq!(
        ctx.postgres
            .count("SELECT COUNT(*) FROM public.t")
            .await
            .unwrap(),
        3
    );
    let count_rocket = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.t WHERE id = '🚀'")
        .await
        .unwrap();
    assert_eq!(count_rocket, 1, "emoji pk must round-trip exactly");
}

/// 8. SQL string concat (alias == table column `data`), unicode-safe.
#[tokio::test]
async fn sql_concat_suffix() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.postgres
        .execute("CREATE TABLE t (id TEXT PRIMARY KEY, data TEXT NOT NULL)")
        .await
        .unwrap();
    ctx.kafka.register_schema(STR_SCHEMA).await.unwrap();
    ctx.kafka
        .produce_avro_records(&[StrRec {
            id: "1".into(),
            data: "héllo🚀".into(),
        }])
        .await
        .unwrap();
    let yaml = format!(
        r#"
sources:
  s:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms:
  tr:
    type: sql
    sql: "SELECT id, data || '_x' AS data FROM s"
    primary_key: id
sinks:
  out:
    type: postgres
    from: tr
    table: t
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(1))
        .await
        .unwrap();
    assert!(status.success());
    let rows: Vec<StrRow> = ctx
        .postgres
        .query("SELECT id, data FROM public.t")
        .await
        .unwrap();
    assert_eq!(rows[0].data, "héllo🚀_x");
}

/// 9. SQL UPPER over mixed-case + unicode (alias == table column `data`).
#[tokio::test]
async fn sql_upper_mixed_case() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.postgres
        .execute("CREATE TABLE t (id TEXT PRIMARY KEY, data TEXT NOT NULL)")
        .await
        .unwrap();
    ctx.kafka.register_schema(STR_SCHEMA).await.unwrap();
    ctx.kafka
        .produce_avro_records(&[StrRec {
            id: "1".into(),
            data: "AbCdé".into(),
        }])
        .await
        .unwrap();
    let yaml = format!(
        r#"
sources:
  s:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms:
  tr:
    type: sql
    sql: "SELECT id, UPPER(data) AS data FROM s"
    primary_key: id
sinks:
  out:
    type: postgres
    from: tr
    table: t
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(1))
        .await
        .unwrap();
    assert!(status.success());
    let rows: Vec<StrRow> = ctx
        .postgres
        .query("SELECT id, data FROM public.t")
        .await
        .unwrap();
    assert!(rows[0].data.starts_with("ABCD"), "got {}", rows[0].data);
}

/// 10. A filter matching nothing must not leak rows (bounded short timeout to
/// avoid a never-reached record_limit hanging the pipeline — a known risk).
#[tokio::test]
async fn filter_matches_nothing() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.postgres
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, val BIGINT NOT NULL)")
        .await
        .unwrap();
    ctx.kafka.register_schema(LONG_SCHEMA).await.unwrap();
    let recs: Vec<LongRec> = (1..=5).map(|i| LongRec { id: i, val: i }).collect();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();
    let yaml = format!(
        r#"
sources:
  s:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms:
  tr:
    type: sql
    sql: "SELECT id, val FROM s WHERE val < 0"
    primary_key: id
sinks:
  out:
    type: postgres
    from: tr
    table: t
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );
    // May time out (no record ever reaches record_limit); we don't care about the
    // exit, only that no rows leaked and it didn't panic.
    let opts = base_opts()
        .record_limit(5)
        .timeout(std::time::Duration::from_secs(20));
    let out = ctx.run_pipeline_raw(&yaml, opts).await;
    if let Ok(o) = &out {
        assert!(!o.stderr.contains("panicked"), "pipeline panicked");
    }
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.t")
        .await
        .unwrap();
    assert_eq!(count, 0, "filter that matches nothing must leak no rows");
}

/// 11. An always-true filter passes every row.
#[tokio::test]
async fn filter_always_true() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.postgres
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, val BIGINT NOT NULL)")
        .await
        .unwrap();
    ctx.kafka.register_schema(LONG_SCHEMA).await.unwrap();
    let recs: Vec<LongRec> = (1..=6).map(|i| LongRec { id: i, val: i }).collect();
    ctx.kafka.produce_avro_records(&recs).await.unwrap();
    let yaml = format!(
        r#"
sources:
  s:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms:
  tr:
    type: sql
    sql: "SELECT id, val FROM s WHERE val >= 0"
    primary_key: id
sinks:
  out:
    type: postgres
    from: tr
    table: t
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(6))
        .await
        .unwrap();
    assert!(status.success());
    assert_eq!(
        ctx.postgres
            .count("SELECT COUNT(*) FROM public.t")
            .await
            .unwrap(),
        6
    );
}

/// 12. int + double coercion in SQL -> float result (table column `total`).
#[tokio::test]
async fn sql_int_plus_double_coercion() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.postgres
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, total DOUBLE PRECISION NOT NULL)")
        .await
        .unwrap();
    ctx.kafka.register_schema(MIX_SCHEMA).await.unwrap();
    ctx.kafka
        .produce_avro_records(&[
            MixRec {
                id: 1,
                i_col: 10,
                d_col: 0.5,
            },
            MixRec {
                id: 2,
                i_col: -3,
                d_col: 2.25,
            },
        ])
        .await
        .unwrap();
    let yaml = format!(
        r#"
sources:
  s:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms:
  tr:
    type: sql
    sql: "SELECT id, i_col + d_col AS total FROM s"
    primary_key: id
sinks:
  out:
    type: postgres
    from: tr
    table: t
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(2))
        .await
        .unwrap();
    assert!(status.success());
    let rows: Vec<TotalRow> = ctx
        .postgres
        .query("SELECT id, total FROM public.t ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows[0].total, 10.5);
    assert_eq!(rows[1].total, -0.75);
}

// ---- pipeline YAML helpers (passthrough; alias == table column) ----

fn pg_passthrough(topic: &str, table: &str) -> String {
    format!(
        r#"
sources:
  s:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms: {{}}
sinks:
  out:
    type: postgres
    from: s
    table: {table}
    schema: public
    primary_key: id
    on_conflict: update
"#
    )
}

fn pg_passthrough_str(topic: &str, table: &str) -> String {
    pg_passthrough(topic, table)
}

fn pg_passthrough_dbl(topic: &str, table: &str) -> String {
    pg_passthrough(topic, table)
}
