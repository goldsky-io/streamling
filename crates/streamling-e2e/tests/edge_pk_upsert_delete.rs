//! Adversarial e2e tests probing primary-key / upsert / delete (debezium op)
//! semantics through the Kafka(Avro) -> Postgres pipeline.
//!
//! These exercise the interaction between the debezium `dbz.op` header
//! ("c"=insert, "u"=update, "d"=delete), the sink's `on_conflict` mode
//! (update / nothing), and primary-key dedup. Some of these are expected to
//! surface findings rather than pass — see the per-test documentation.
//!
//! NOTE: `record_limit` counts ALL produced records, including deletes.

use serde::Serialize;
use sqlx::FromRow;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

// ============================================================================
// Record types + schemas
// ============================================================================

/// Flat record with an integer primary key.
#[derive(Debug, Clone, Serialize)]
struct IntPkRecord {
    id: i64,
    name: String,
    score: i32,
    active: bool,
}

const INT_PK_SCHEMA: &str = r#"{
    "type": "record",
    "name": "IntPkRecord",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": "string"},
        {"name": "score", "type": "int"},
        {"name": "active", "type": "boolean"}
    ]
}"#;

/// Flat record with a string primary key.
#[derive(Debug, Clone, Serialize)]
struct StrPkRecord {
    id: String,
    payload: String,
}

const STR_PK_SCHEMA: &str = r#"{
    "type": "record",
    "name": "StrPkRecord",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "payload", "type": "string"}
    ]
}"#;

// ============================================================================
// FromRow types for assertions
// ============================================================================

#[derive(Debug, FromRow)]
struct IntRow {
    id: i64,
    name: String,
    score: i32,
    active: bool,
}

#[derive(Debug, FromRow)]
struct StrRow {
    id: String,
    payload: String,
}

// ============================================================================
// Shared helpers
// ============================================================================

fn base_opts() -> PipelineOpts {
    PipelineOpts::new()
        .timeout(std::time::Duration::from_secs(60))
        .env("STREAMLING__PLUGIN__PATH", "")
        .env("STREAMLING__PLUGIN__PREPROCESSOR_IDS", "")
        .env("STREAMLING__PLUGIN__SIDE_OUTPUT_IDS", "")
}

/// Build a kafka -> postgres pipeline YAML for the int-PK table.
fn int_pipeline(ctx: &TestContext, table: &str, on_conflict: &str) -> String {
    format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: kafka_source
    table: {table}
    schema: public
    primary_key: id
    on_conflict: {on_conflict}
    batch_size: 1
    batch_flush_interval: 100ms
"#,
        topic = ctx.kafka_topic,
    )
}

/// Build a kafka -> postgres pipeline YAML for the string-PK table.
fn str_pipeline(ctx: &TestContext, table: &str, on_conflict: &str) -> String {
    format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: kafka_source
    table: {table}
    schema: public
    primary_key: id
    on_conflict: {on_conflict}
    batch_size: 1
    batch_flush_interval: 100ms
"#,
        topic = ctx.kafka_topic,
    )
}

fn rec(id: i64, name: &str, score: i32, active: bool) -> IntPkRecord {
    IntPkRecord {
        id,
        name: name.to_string(),
        score,
        active,
    }
}

// ============================================================================
// Scenario 1: insert then update (op "u") same id -> updated data, count == 1
// ============================================================================

#[tokio::test]
async fn pk_insert_then_update_same_id() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.kafka.register_schema(INT_PK_SCHEMA).await.unwrap();
    ctx.postgres
        .execute(
            "CREATE TABLE pk_insert_update (id BIGINT PRIMARY KEY, name TEXT, score INT, active BOOLEAN)",
        )
        .await
        .unwrap();

    // c: id=1 name=alpha score=10
    ctx.kafka
        .produce_avro_records(&[rec(1, "alpha", 10, true)])
        .await
        .unwrap();
    // u: id=1 name=beta score=20
    ctx.kafka
        .produce_avro_records_with_op(&[rec(1, "beta", 20, false)], "u")
        .await
        .unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &int_pipeline(&ctx, "pk_insert_update", "update"),
            base_opts().record_limit(2),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.pk_insert_update")
        .await
        .unwrap();
    assert_eq!(count, 1, "update of existing id should not add a row");

    let rows: Vec<IntRow> = ctx
        .postgres
        .query("SELECT id, name, score, active FROM public.pk_insert_update WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "beta");
    assert_eq!(rows[0].score, 20);
    assert!(!rows[0].active);
}

// ============================================================================
// Scenario 2: insert then delete (op "d") same id -> row gone, count == 0
// ============================================================================

#[tokio::test]
async fn pk_insert_then_delete_same_id() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.kafka.register_schema(INT_PK_SCHEMA).await.unwrap();
    ctx.postgres
        .execute(
            "CREATE TABLE pk_insert_delete (id BIGINT PRIMARY KEY, name TEXT, score INT, active BOOLEAN)",
        )
        .await
        .unwrap();

    ctx.kafka
        .produce_avro_records(&[rec(1, "alpha", 10, true)])
        .await
        .unwrap();
    ctx.kafka
        .produce_avro_records_with_op(&[rec(1, "", 0, false)], "d")
        .await
        .unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &int_pipeline(&ctx, "pk_insert_delete", "update"),
            base_opts().record_limit(2),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.pk_insert_delete")
        .await
        .unwrap();
    assert_eq!(count, 0, "delete should remove the inserted row");
}

// ============================================================================
// Scenario 3: delete (op "d") a non-existent id -> no error, count == 0
// ============================================================================

#[tokio::test]
async fn pk_delete_nonexistent_id() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.kafka.register_schema(INT_PK_SCHEMA).await.unwrap();
    ctx.postgres
        .execute(
            "CREATE TABLE pk_delete_missing (id BIGINT PRIMARY KEY, name TEXT, score INT, active BOOLEAN)",
        )
        .await
        .unwrap();

    // Delete for an id that was never inserted.
    ctx.kafka
        .produce_avro_records_with_op(&[rec(99, "", 0, false)], "d")
        .await
        .unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &int_pipeline(&ctx, "pk_delete_missing", "update"),
            base_opts().record_limit(1),
        )
        .await
        .expect("pipeline run");
    assert!(
        status.success(),
        "deleting a non-existent id should be a no-op, not an error"
    );

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.pk_delete_missing")
        .await
        .unwrap();
    assert_eq!(count, 0, "table should remain empty");
}

// ============================================================================
// Scenario 4: on_conflict=update, insert id=1 twice (different data) -> last wins
// ============================================================================

#[tokio::test]
async fn pk_on_conflict_update_last_wins() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.kafka.register_schema(INT_PK_SCHEMA).await.unwrap();
    ctx.postgres
        .execute(
            "CREATE TABLE pk_conflict_update (id BIGINT PRIMARY KEY, name TEXT, score INT, active BOOLEAN)",
        )
        .await
        .unwrap();

    // Two inserts (op "c") with the same id but different data.
    ctx.kafka
        .produce_avro_records(&[rec(1, "first", 1, true)])
        .await
        .unwrap();
    ctx.kafka
        .produce_avro_records(&[rec(1, "second", 2, false)])
        .await
        .unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &int_pipeline(&ctx, "pk_conflict_update", "update"),
            base_opts().record_limit(2),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.pk_conflict_update")
        .await
        .unwrap();
    assert_eq!(count, 1);

    let rows: Vec<IntRow> = ctx
        .postgres
        .query("SELECT id, name, score, active FROM public.pk_conflict_update WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(
        rows[0].name, "second",
        "on_conflict=update: last write wins"
    );
    assert_eq!(rows[0].score, 2);
}

// ============================================================================
// Scenario 5: on_conflict=nothing, insert id=1 twice -> FIRST wins
// ============================================================================

#[tokio::test]
async fn pk_on_conflict_nothing_first_wins() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.kafka.register_schema(INT_PK_SCHEMA).await.unwrap();
    ctx.postgres
        .execute(
            "CREATE TABLE pk_conflict_nothing (id BIGINT PRIMARY KEY, name TEXT, score INT, active BOOLEAN)",
        )
        .await
        .unwrap();

    ctx.kafka
        .produce_avro_records(&[rec(1, "first", 1, true)])
        .await
        .unwrap();
    ctx.kafka
        .produce_avro_records(&[rec(1, "second", 2, false)])
        .await
        .unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &int_pipeline(&ctx, "pk_conflict_nothing", "nothing"),
            base_opts().record_limit(2),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.pk_conflict_nothing")
        .await
        .unwrap();
    assert_eq!(count, 1);

    let rows: Vec<IntRow> = ctx
        .postgres
        .query("SELECT id, name, score, active FROM public.pk_conflict_nothing WHERE id = 1")
        .await
        .unwrap();
    // Contrast with scenario 4: with on_conflict=nothing the FIRST insert is kept.
    assert_eq!(
        rows[0].name, "first",
        "on_conflict=nothing: first write wins (vs last-wins under update)"
    );
    assert_eq!(rows[0].score, 1);
}

// ============================================================================
// Scenario 6: insert / delete / insert same id -> re-inserted row present
// ============================================================================

#[tokio::test]
async fn pk_insert_delete_reinsert() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.kafka.register_schema(INT_PK_SCHEMA).await.unwrap();
    ctx.postgres
        .execute(
            "CREATE TABLE pk_reinsert (id BIGINT PRIMARY KEY, name TEXT, score INT, active BOOLEAN)",
        )
        .await
        .unwrap();

    ctx.kafka
        .produce_avro_records(&[rec(1, "original", 10, true)])
        .await
        .unwrap();
    ctx.kafka
        .produce_avro_records_with_op(&[rec(1, "", 0, false)], "d")
        .await
        .unwrap();
    ctx.kafka
        .produce_avro_records(&[rec(1, "reborn", 30, true)])
        .await
        .unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &int_pipeline(&ctx, "pk_reinsert", "update"),
            base_opts().record_limit(3),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.pk_reinsert")
        .await
        .unwrap();
    assert_eq!(
        count, 1,
        "re-insert after delete should leave exactly one row"
    );

    let rows: Vec<IntRow> = ctx
        .postgres
        .query("SELECT id, name, score, active FROM public.pk_reinsert WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(rows[0].name, "reborn");
    assert_eq!(rows[0].score, 30);
}

// ============================================================================
// Scenario 7: duplicate pk within a single batch (10 records id=1) -> count==1, last value
// ============================================================================

#[tokio::test]
async fn pk_duplicate_within_single_batch() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.kafka.register_schema(INT_PK_SCHEMA).await.unwrap();
    ctx.postgres
        .execute(
            "CREATE TABLE pk_dup_batch (id BIGINT PRIMARY KEY, name TEXT, score INT, active BOOLEAN)",
        )
        .await
        .unwrap();

    // 10 records all id=1 with increasing score. Produced in one call.
    let records: Vec<IntPkRecord> = (0..10)
        .map(|i| rec(1, &format!("v{}", i), i, i % 2 == 0))
        .collect();
    ctx.kafka.produce_avro_records(&records).await.unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &int_pipeline(&ctx, "pk_dup_batch", "update"),
            base_opts().record_limit(10),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.pk_dup_batch")
        .await
        .unwrap();
    assert_eq!(
        count, 1,
        "10 records with same pk should collapse to one row"
    );

    let rows: Vec<IntRow> = ctx
        .postgres
        .query("SELECT id, name, score, active FROM public.pk_dup_batch WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(rows[0].name, "v9", "the last record (score=9) should win");
    assert_eq!(rows[0].score, 9);
}

// ============================================================================
// Scenario 8: string primary key with special chars (quotes, unicode, spaces)
// ============================================================================

#[tokio::test]
async fn pk_string_special_chars_insert_update() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.kafka.register_schema(STR_PK_SCHEMA).await.unwrap();
    ctx.postgres
        .execute("CREATE TABLE pk_str_special (id TEXT PRIMARY KEY, payload TEXT)")
        .await
        .unwrap();

    // A pk loaded with quotes, unicode, and spaces.
    let weird_id = r#"o'reilly "café" 日本語 spaced"#;

    ctx.kafka
        .produce_avro_records(&[StrPkRecord {
            id: weird_id.to_string(),
            payload: "initial".to_string(),
        }])
        .await
        .unwrap();
    ctx.kafka
        .produce_avro_records_with_op(
            &[StrPkRecord {
                id: weird_id.to_string(),
                payload: "updated".to_string(),
            }],
            "u",
        )
        .await
        .unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &str_pipeline(&ctx, "pk_str_special", "update"),
            base_opts().record_limit(2),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.pk_str_special")
        .await
        .unwrap();
    assert_eq!(
        count, 1,
        "special-char string pk should still dedup to one row"
    );

    let rows: Vec<StrRow> = ctx
        .postgres
        .query("SELECT id, payload FROM public.pk_str_special")
        .await
        .unwrap();
    assert_eq!(rows[0].id, weird_id);
    assert_eq!(
        rows[0].payload, "updated",
        "the correct row should be updated"
    );
}

// ============================================================================
// Scenario 9: multiple distinct ids with interleaved updates -> 3 rows updated
// ============================================================================

#[tokio::test]
async fn pk_multiple_ids_interleaved_updates() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.kafka.register_schema(INT_PK_SCHEMA).await.unwrap();
    ctx.postgres
        .execute(
            "CREATE TABLE pk_interleaved (id BIGINT PRIMARY KEY, name TEXT, score INT, active BOOLEAN)",
        )
        .await
        .unwrap();

    // Interleave: insert 1, insert 2, insert 3, update 1, update 2, update 3.
    ctx.kafka
        .produce_avro_records(&[rec(1, "a1", 1, true)])
        .await
        .unwrap();
    ctx.kafka
        .produce_avro_records(&[rec(2, "a2", 2, true)])
        .await
        .unwrap();
    ctx.kafka
        .produce_avro_records(&[rec(3, "a3", 3, true)])
        .await
        .unwrap();
    ctx.kafka
        .produce_avro_records_with_op(&[rec(1, "b1", 11, false)], "u")
        .await
        .unwrap();
    ctx.kafka
        .produce_avro_records_with_op(&[rec(2, "b2", 12, false)], "u")
        .await
        .unwrap();
    ctx.kafka
        .produce_avro_records_with_op(&[rec(3, "b3", 13, false)], "u")
        .await
        .unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &int_pipeline(&ctx, "pk_interleaved", "update"),
            base_opts().record_limit(6),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.pk_interleaved")
        .await
        .unwrap();
    assert_eq!(count, 3, "three distinct ids -> three rows");

    let rows: Vec<IntRow> = ctx
        .postgres
        .query("SELECT id, name, score, active FROM public.pk_interleaved ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    for (idx, row) in rows.iter().enumerate() {
        let id = (idx + 1) as i64;
        assert_eq!(row.id, id);
        assert_eq!(
            row.name,
            format!("b{}", id),
            "each id should hold its update"
        );
        assert_eq!(row.score, 10 + id as i32);
        assert!(!row.active);
    }
}

// ============================================================================
// Scenario 10: delete then re-insert with DIFFERENT data -> re-inserted data present
// ============================================================================

#[tokio::test]
async fn pk_delete_then_reinsert_different_data() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.kafka.register_schema(INT_PK_SCHEMA).await.unwrap();
    ctx.postgres
        .execute(
            "CREATE TABLE pk_del_reinsert (id BIGINT PRIMARY KEY, name TEXT, score INT, active BOOLEAN)",
        )
        .await
        .unwrap();

    // Seed, delete, then re-insert with entirely different field values.
    ctx.kafka
        .produce_avro_records(&[rec(7, "old", 100, true)])
        .await
        .unwrap();
    ctx.kafka
        .produce_avro_records_with_op(&[rec(7, "", 0, false)], "d")
        .await
        .unwrap();
    ctx.kafka
        .produce_avro_records(&[rec(7, "fresh", 200, false)])
        .await
        .unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &int_pipeline(&ctx, "pk_del_reinsert", "update"),
            base_opts().record_limit(3),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.pk_del_reinsert")
        .await
        .unwrap();
    assert_eq!(count, 1);

    let rows: Vec<IntRow> = ctx
        .postgres
        .query("SELECT id, name, score, active FROM public.pk_del_reinsert WHERE id = 7")
        .await
        .unwrap();
    assert_eq!(rows[0].name, "fresh", "re-inserted data should be present");
    assert_eq!(rows[0].score, 200);
}

// ============================================================================
// Scenario 11: update (op "u" only) an id that was never inserted, on_conflict=update
//
// EXPECTATION (documented finding): debezium "u" is treated as an upsert by the
// postgres sink (it routes through the same INSERT ... ON CONFLICT DO UPDATE
// path as "c"). So an update to a never-seen id is expected to APPEAR as a new
// row. If the sink instead treated "u" as update-only (no insert), the row
// would be absent and this assertion would surface that as a finding.
// ============================================================================

#[tokio::test]
async fn pk_update_only_never_inserted_upserts() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.kafka.register_schema(INT_PK_SCHEMA).await.unwrap();
    ctx.postgres
        .execute(
            "CREATE TABLE pk_update_only (id BIGINT PRIMARY KEY, name TEXT, score INT, active BOOLEAN)",
        )
        .await
        .unwrap();

    // No prior insert for id=5 — go straight to an "u".
    ctx.kafka
        .produce_avro_records_with_op(&[rec(5, "phantom", 55, true)], "u")
        .await
        .unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &int_pipeline(&ctx, "pk_update_only", "update"),
            base_opts().record_limit(1),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.pk_update_only")
        .await
        .unwrap();
    // Expected: upsert semantics -> the row appears.
    assert_eq!(
        count, 1,
        "op=u on a never-inserted id is expected to upsert (insert) the row"
    );

    let rows: Vec<IntRow> = ctx
        .postgres
        .query("SELECT id, name, score, active FROM public.pk_update_only WHERE id = 5")
        .await
        .unwrap();
    assert_eq!(rows[0].name, "phantom");
    assert_eq!(rows[0].score, 55);
}

// ============================================================================
// Scenario 12: mixed batch — 5 inserts, 2 updates, 1 delete across overlapping ids
//
// Sequence (8 records total):
//   c id=1 name=i1 score=1
//   c id=2 name=i2 score=2
//   c id=3 name=i3 score=3
//   c id=4 name=i4 score=4
//   c id=5 name=i5 score=5
//   u id=2 name=u2 score=20    (updates 2)
//   u id=3 name=u3 score=30    (updates 3)
//   d id=4                     (deletes 4)
// Final expected state: ids {1,2,3,5} present (4 rows), 2 & 3 updated, 4 gone.
// ============================================================================

#[tokio::test]
async fn pk_mixed_batch_inserts_updates_delete() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.kafka.register_schema(INT_PK_SCHEMA).await.unwrap();
    ctx.postgres
        .execute(
            "CREATE TABLE pk_mixed (id BIGINT PRIMARY KEY, name TEXT, score INT, active BOOLEAN)",
        )
        .await
        .unwrap();

    // 5 inserts
    ctx.kafka
        .produce_avro_records(&[
            rec(1, "i1", 1, true),
            rec(2, "i2", 2, true),
            rec(3, "i3", 3, true),
            rec(4, "i4", 4, true),
            rec(5, "i5", 5, true),
        ])
        .await
        .unwrap();
    // 2 updates
    ctx.kafka
        .produce_avro_records_with_op(&[rec(2, "u2", 20, false), rec(3, "u3", 30, false)], "u")
        .await
        .unwrap();
    // 1 delete
    ctx.kafka
        .produce_avro_records_with_op(&[rec(4, "", 0, false)], "d")
        .await
        .unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &int_pipeline(&ctx, "pk_mixed", "update"),
            base_opts().record_limit(8),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.pk_mixed")
        .await
        .unwrap();
    assert_eq!(count, 4, "ids 1,2,3,5 remain (4 deleted)");

    let rows: Vec<IntRow> = ctx
        .postgres
        .query("SELECT id, name, score, active FROM public.pk_mixed ORDER BY id")
        .await
        .unwrap();
    let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
    assert_eq!(ids, vec![1, 2, 3, 5], "id=4 must be gone");

    // id=1 untouched insert
    assert_eq!(rows[0].name, "i1");
    assert_eq!(rows[0].score, 1);
    // id=2 updated
    assert_eq!(rows[1].name, "u2");
    assert_eq!(rows[1].score, 20);
    // id=3 updated
    assert_eq!(rows[2].name, "u3");
    assert_eq!(rows[2].score, 30);
    // id=5 untouched insert
    assert_eq!(rows[3].name, "i5");
    assert_eq!(rows[3].score, 5);
}
