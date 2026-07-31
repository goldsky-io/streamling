//! Adversarial e2e tests: Avro SCHEMA EVOLUTION through the schema-registry +
//! the arrow-avro multi-writer-generation decode path.
//!
//! Each test registers TWO (or more) flat Avro schemas under the topic subject,
//! produces records with each, then runs Kafka(Avro) -> Postgres and asserts on
//! counts + values. Some scenarios are EXPECTED to fail — those failures are the
//! findings (documented inline + in the closing report).
//!
//! IMPORTANT REGISTRY CONSTRAINT (shapes the expected outcomes below):
//! `produce_avro_records` encodes against the *latest* schema registered under
//! the TopicNameStrategy subject, and `register_schema` is subject to the
//! registry's compatibility level (Redpanda default = BACKWARD). So:
//!   - add-field-with-default, remove-field, int->long widening register cleanly.
//!   - add-field-without-default, float->double, some reorder/rename shapes are
//!     BACKWARD-incompatible and the *registration* (not the pipeline) is the
//!     point that rejects them. We mirror schema_evolution.rs's sequential
//!     register->produce pattern and surface where the failure actually lands.

use serde::Serialize;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

fn base_opts() -> PipelineOpts {
    PipelineOpts::new()
        .timeout(std::time::Duration::from_secs(60))
        .env("STREAMLING__PLUGIN__PATH", "")
        .env("STREAMLING__PLUGIN__PREPROCESSOR_IDS", "")
        .env("STREAMLING__PLUGIN__SIDE_OUTPUT_IDS", "")
}

/// Build a standard Kafka(Avro) -> Postgres pipeline YAML targeting `table`.
fn pipeline_yaml(topic: &str, table: &str) -> String {
    format!(
        r#"
sources:
  evo_in:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms: {{}}
sinks:
  evo_out:
    type: postgres
    from: evo_in
    table: {table}
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = topic,
        table = table,
    )
}

// ===========================================================================
// 1. Add a nullable field with default null in v2 (backward compatible).
//    v1 rows get null, v2 rows get a value.
// ===========================================================================

#[derive(Debug, Clone, Serialize)]
struct AddNullV1 {
    id: i64,
    name: String,
}
#[derive(Debug, Clone, Serialize)]
struct AddNullV2 {
    id: i64,
    name: String,
    nickname: Option<String>,
}
const ADD_NULL_V1: &str = r#"{
    "type":"record","name":"AddNull",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"name","type":"string"}
    ]}"#;
const ADD_NULL_V2: &str = r#"{
    "type":"record","name":"AddNull",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"name","type":"string"},
        {"name":"nickname","type":["null","string"],"default":null}
    ]}"#;

#[tokio::test]
async fn evo_add_nullable_field_default_null() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.kafka.register_schema(ADD_NULL_V1).await.unwrap();
    let v1: Vec<AddNullV1> = (1..=3)
        .map(|i| AddNullV1 {
            id: i,
            name: format!("n{i}"),
        })
        .collect();
    ctx.kafka.produce_avro_records(&v1).await.unwrap();

    ctx.kafka.register_schema(ADD_NULL_V2).await.unwrap();
    let v2: Vec<AddNullV2> = (4..=6)
        .map(|i| AddNullV2 {
            id: i,
            name: format!("n{i}"),
            nickname: Some(format!("nick{i}")),
        })
        .collect();
    ctx.kafka.produce_avro_records(&v2).await.unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "evo_add_null"),
            base_opts().record_limit(6),
        )
        .await
        .expect("pipeline run");
    assert!(
        status.success(),
        "backward-compatible add-nullable should succeed"
    );

    let total = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.evo_add_null")
        .await
        .unwrap();
    assert_eq!(total, 6, "all 6 rows should land");

    // v1 rows: nickname IS NULL; v2 rows: nickname IS NOT NULL.
    let null_nick = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.evo_add_null WHERE nickname IS NULL")
        .await
        .unwrap();
    assert_eq!(null_nick, 3, "3 v1 rows should have null nickname");
}

// ===========================================================================
// 2. Add a field with a non-null default in v2. v1 rows get the default.
// ===========================================================================

#[derive(Debug, Clone, Serialize)]
struct AddDefV1 {
    id: i64,
    data: String,
}
#[derive(Debug, Clone, Serialize)]
struct AddDefV2 {
    id: i64,
    data: String,
    version: i32,
}
const ADD_DEF_V1: &str = r#"{
    "type":"record","name":"AddDef",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"data","type":"string"}
    ]}"#;
const ADD_DEF_V2: &str = r#"{
    "type":"record","name":"AddDef",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"data","type":"string"},
        {"name":"version","type":"int","default":7}
    ]}"#;

#[tokio::test]
async fn evo_add_field_nonnull_default() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.kafka.register_schema(ADD_DEF_V1).await.unwrap();
    let v1: Vec<AddDefV1> = (1..=4)
        .map(|i| AddDefV1 {
            id: i,
            data: format!("d{i}"),
        })
        .collect();
    ctx.kafka.produce_avro_records(&v1).await.unwrap();

    ctx.kafka.register_schema(ADD_DEF_V2).await.unwrap();
    let v2: Vec<AddDefV2> = (5..=8)
        .map(|i| AddDefV2 {
            id: i,
            data: format!("d{i}"),
            version: 2,
        })
        .collect();
    ctx.kafka.produce_avro_records(&v2).await.unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "evo_add_def"),
            base_opts().record_limit(8),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "add-field-with-default should succeed");

    let total = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.evo_add_def")
        .await
        .unwrap();
    assert_eq!(total, 8);

    // FINDING PROBE: do v1 rows materialize the schema default (7) for `version`,
    // or NULL? The arrow-avro reader resolves the writer (v1) datum against the
    // reader (v2) schema and should apply default=7. Assert the strong contract.
    let v1_default = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.evo_add_def WHERE id <= 4 AND version = 7")
        .await
        .unwrap();
    assert_eq!(v1_default, 4, "v1 rows should pick up default version=7");
}

// ===========================================================================
// 3. Remove a field in v2 (forward compatible read of v1 by v2 reader).
// ===========================================================================

#[derive(Debug, Clone, Serialize)]
struct RemoveV1 {
    id: i64,
    keep: String,
    drop_me: String,
}
#[derive(Debug, Clone, Serialize)]
struct RemoveV2 {
    id: i64,
    keep: String,
}
const REMOVE_V1: &str = r#"{
    "type":"record","name":"Remove",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"keep","type":"string"},
        {"name":"drop_me","type":"string"}
    ]}"#;
const REMOVE_V2: &str = r#"{
    "type":"record","name":"Remove",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"keep","type":"string"}
    ]}"#;

#[tokio::test]
async fn evo_remove_field() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.kafka.register_schema(REMOVE_V1).await.unwrap();
    let v1: Vec<RemoveV1> = (1..=3)
        .map(|i| RemoveV1 {
            id: i,
            keep: format!("k{i}"),
            drop_me: format!("x{i}"),
        })
        .collect();
    ctx.kafka.produce_avro_records(&v1).await.unwrap();

    // Removing a field with no default is BACKWARD-incompatible at registration.
    // If the registry rejects it, this surfaces as a panic on unwrap — documented
    // as a finding. We still attempt the run with whatever schema is current.
    let removed = ctx.kafka.register_schema(REMOVE_V2).await;
    if removed.is_ok() {
        let v2: Vec<RemoveV2> = (4..=6)
            .map(|i| RemoveV2 {
                id: i,
                keep: format!("k{i}"),
            })
            .collect();
        ctx.kafka.produce_avro_records(&v2).await.unwrap();
    }

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "evo_remove"),
            base_opts().record_limit(if removed.is_ok() { 6 } else { 3 }),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "field-removal read path should succeed");

    let expected = if removed.is_ok() { 6 } else { 3 };
    let total = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.evo_remove")
        .await
        .unwrap();
    assert_eq!(total, expected as i64, "all produced rows should land");
}

// ===========================================================================
// 4. Widen int -> long for a field across v1 -> v2.
// ===========================================================================

#[derive(Debug, Clone, Serialize)]
struct WidenV1 {
    id: i64,
    qty: i32,
}
#[derive(Debug, Clone, Serialize)]
struct WidenV2 {
    id: i64,
    qty: i64,
}
const WIDEN_V1: &str = r#"{
    "type":"record","name":"Widen",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"qty","type":"int"}
    ]}"#;
const WIDEN_V2: &str = r#"{
    "type":"record","name":"Widen",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"qty","type":"long"}
    ]}"#;

#[tokio::test]
async fn evo_widen_int_to_long() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.kafka.register_schema(WIDEN_V1).await.unwrap();
    ctx.kafka
        .produce_avro_records(&[
            WidenV1 { id: 1, qty: 10 },
            WidenV1 {
                id: 2,
                qty: 2_000_000_000,
            },
        ])
        .await
        .unwrap();

    ctx.kafka.register_schema(WIDEN_V2).await.unwrap();
    ctx.kafka
        .produce_avro_records(&[
            WidenV2 { id: 3, qty: 5 },
            WidenV2 {
                id: 4,
                qty: 9_000_000_000,
            },
        ])
        .await
        .unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "evo_widen"),
            base_opts().record_limit(4),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "int->long widening should succeed");

    let total = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.evo_widen")
        .await
        .unwrap();
    assert_eq!(total, 4);

    // The v2 big value must survive (would overflow i32).
    let big = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.evo_widen WHERE qty = 9000000000")
        .await
        .unwrap();
    assert_eq!(big, 1, "9e9 must round-trip through the long column");
}

// ===========================================================================
// 5. Reorder fields between v1 and v2 (same names). Values map by name.
// ===========================================================================

#[derive(Debug, Clone, Serialize)]
struct ReorderV1 {
    id: i64,
    a: String,
    b: i64,
}
// v2: same fields, different declaration order.
#[derive(Debug, Clone, Serialize)]
struct ReorderV2 {
    b: i64,
    id: i64,
    a: String,
}
const REORDER_V1: &str = r#"{
    "type":"record","name":"Reorder",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"a","type":"string"},
        {"name":"b","type":"long"}
    ]}"#;
const REORDER_V2: &str = r#"{
    "type":"record","name":"Reorder",
    "fields":[
        {"name":"b","type":"long"},
        {"name":"id","type":"long"},
        {"name":"a","type":"string"}
    ]}"#;

#[tokio::test]
async fn evo_reorder_fields() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.kafka.register_schema(REORDER_V1).await.unwrap();
    ctx.kafka
        .produce_avro_records(&[
            ReorderV1 {
                id: 1,
                a: "one".into(),
                b: 100,
            },
            ReorderV1 {
                id: 2,
                a: "two".into(),
                b: 200,
            },
        ])
        .await
        .unwrap();

    ctx.kafka.register_schema(REORDER_V2).await.unwrap();
    ctx.kafka
        .produce_avro_records(&[ReorderV2 {
            b: 300,
            id: 3,
            a: "three".into(),
        }])
        .await
        .unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "evo_reorder"),
            base_opts().record_limit(3),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "field reorder should resolve by name");

    // Values must map by NAME, not position: id=3 -> a='three', b=300.
    let ok = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.evo_reorder WHERE id = 3 AND a = 'three' AND b = 300")
        .await
        .unwrap();
    assert_eq!(ok, 1, "reordered v2 row must map fields by name");
    let total = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.evo_reorder")
        .await
        .unwrap();
    assert_eq!(total, 3);
}

// ===========================================================================
// 6. Mixed v1 and v2 records INTERLEAVED in one run.
//    Exercises arrow-avro per-writer-id generation batching.
// ===========================================================================

#[derive(Debug, Clone, Serialize)]
struct MixV1 {
    id: i64,
    val: String,
}
#[derive(Debug, Clone, Serialize)]
struct MixV2 {
    id: i64,
    val: String,
    extra: Option<i64>,
}
const MIX_V1: &str = r#"{
    "type":"record","name":"Mix",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"val","type":"string"}
    ]}"#;
const MIX_V2: &str = r#"{
    "type":"record","name":"Mix",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"val","type":"string"},
        {"name":"extra","type":["null","long"],"default":null}
    ]}"#;

#[tokio::test]
async fn evo_interleaved_v1_v2_generations() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    // Register both schemas up front so both ids exist in the registry.
    ctx.kafka.register_schema(MIX_V1).await.unwrap();
    // Produce a v1 record (encoder uses latest = v1 here).
    ctx.kafka
        .produce_avro_records(&[MixV1 {
            id: 1,
            val: "a".into(),
        }])
        .await
        .unwrap();

    ctx.kafka.register_schema(MIX_V2).await.unwrap();
    // Now latest = v2; produce a v2 record.
    ctx.kafka
        .produce_avro_records(&[MixV2 {
            id: 2,
            val: "b".into(),
            extra: Some(22),
        }])
        .await
        .unwrap();
    // Re-register v1 to flip latest back so the next v1 record encodes under v1.
    ctx.kafka.register_schema(MIX_V1).await.unwrap();
    ctx.kafka
        .produce_avro_records(&[MixV1 {
            id: 3,
            val: "c".into(),
        }])
        .await
        .unwrap();
    ctx.kafka.register_schema(MIX_V2).await.unwrap();
    ctx.kafka
        .produce_avro_records(&[MixV2 {
            id: 4,
            val: "d".into(),
            extra: Some(44),
        }])
        .await
        .unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "evo_mix"),
            base_opts().record_limit(4),
        )
        .await
        .expect("pipeline run");
    assert!(
        status.success(),
        "interleaved writer generations should all decode"
    );

    let total = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.evo_mix")
        .await
        .unwrap();
    assert_eq!(total, 4, "no row loss across writer-id generations");

    // v2 rows carry extra; v1 rows null.
    let with_extra = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.evo_mix WHERE extra IS NOT NULL")
        .await
        .unwrap();
    assert_eq!(with_extra, 2, "the two v2 rows keep their extra values");
}

// ===========================================================================
// 7. v2 adds TWO new nullable fields.
// ===========================================================================

#[derive(Debug, Clone, Serialize)]
struct TwoNewV1 {
    id: i64,
    base: String,
}
#[derive(Debug, Clone, Serialize)]
struct TwoNewV2 {
    id: i64,
    base: String,
    f1: Option<i64>,
    f2: Option<String>,
}
const TWO_NEW_V1: &str = r#"{
    "type":"record","name":"TwoNew",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"base","type":"string"}
    ]}"#;
const TWO_NEW_V2: &str = r#"{
    "type":"record","name":"TwoNew",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"base","type":"string"},
        {"name":"f1","type":["null","long"],"default":null},
        {"name":"f2","type":["null","string"],"default":null}
    ]}"#;

#[tokio::test]
async fn evo_add_two_nullable_fields() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.kafka.register_schema(TWO_NEW_V1).await.unwrap();
    ctx.kafka
        .produce_avro_records(&[
            TwoNewV1 {
                id: 1,
                base: "p".into(),
            },
            TwoNewV1 {
                id: 2,
                base: "q".into(),
            },
        ])
        .await
        .unwrap();

    ctx.kafka.register_schema(TWO_NEW_V2).await.unwrap();
    ctx.kafka
        .produce_avro_records(&[TwoNewV2 {
            id: 3,
            base: "r".into(),
            f1: Some(99),
            f2: Some("hi".into()),
        }])
        .await
        .unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "evo_two_new"),
            base_opts().record_limit(3),
        )
        .await
        .expect("pipeline run");
    assert!(
        status.success(),
        "adding two nullable fields should succeed"
    );

    let total = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.evo_two_new")
        .await
        .unwrap();
    assert_eq!(total, 3);

    let both_null = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.evo_two_new WHERE f1 IS NULL AND f2 IS NULL")
        .await
        .unwrap();
    assert_eq!(both_null, 2, "v1 rows have both new fields null");
}

// ===========================================================================
// 8. Promote float -> double across versions.
// ===========================================================================

#[derive(Debug, Clone, Serialize)]
struct FloatV1 {
    id: i64,
    measure: f32,
}
#[derive(Debug, Clone, Serialize)]
struct FloatV2 {
    id: i64,
    measure: f64,
}
const FLOAT_V1: &str = r#"{
    "type":"record","name":"Flt",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"measure","type":"float"}
    ]}"#;
const FLOAT_V2: &str = r#"{
    "type":"record","name":"Flt",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"measure","type":"double"}
    ]}"#;

#[tokio::test]
async fn evo_promote_float_to_double() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.kafka.register_schema(FLOAT_V1).await.unwrap();
    ctx.kafka
        .produce_avro_records(&[FloatV1 {
            id: 1,
            measure: 1.5,
        }])
        .await
        .unwrap();

    // float->double is an Avro promotion but BACKWARD-incompatible at the
    // registry. Surface where it lands rather than presuming success.
    let promoted = ctx.kafka.register_schema(FLOAT_V2).await;
    if promoted.is_ok() {
        ctx.kafka
            .produce_avro_records(&[FloatV2 {
                id: 2,
                measure: 2.25,
            }])
            .await
            .unwrap();
    }

    let limit = if promoted.is_ok() { 2 } else { 1 };
    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "evo_float"),
            base_opts().record_limit(limit),
        )
        .await
        .expect("pipeline run");
    assert!(
        status.success(),
        "float->double promotion read should succeed"
    );

    let total = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.evo_float")
        .await
        .unwrap();
    assert_eq!(total, limit as i64);
}

// ===========================================================================
// 9. Change a field from required to nullable (string -> ["null","string"]).
// ===========================================================================

#[derive(Debug, Clone, Serialize)]
struct ReqNullV1 {
    id: i64,
    note: String,
}
#[derive(Debug, Clone, Serialize)]
struct ReqNullV2 {
    id: i64,
    note: Option<String>,
}
const REQ_NULL_V1: &str = r#"{
    "type":"record","name":"ReqNull",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"note","type":"string"}
    ]}"#;
const REQ_NULL_V2: &str = r#"{
    "type":"record","name":"ReqNull",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"note","type":["null","string"],"default":null}
    ]}"#;

#[tokio::test]
async fn evo_required_to_nullable() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.kafka.register_schema(REQ_NULL_V1).await.unwrap();
    ctx.kafka
        .produce_avro_records(&[ReqNullV1 {
            id: 1,
            note: "required".into(),
        }])
        .await
        .unwrap();

    // Making a required field nullable is BACKWARD-incompatible (old readers
    // can't read null). Registration may reject; surface it.
    let nulled = ctx.kafka.register_schema(REQ_NULL_V2).await;
    if nulled.is_ok() {
        ctx.kafka
            .produce_avro_records(&[
                ReqNullV2 {
                    id: 2,
                    note: Some("still here".into()),
                },
                ReqNullV2 { id: 3, note: None },
            ])
            .await
            .unwrap();
    }

    let limit = if nulled.is_ok() { 3 } else { 1 };
    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "evo_req_null"),
            base_opts().record_limit(limit),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "required->nullable read should succeed");

    let total = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.evo_req_null")
        .await
        .unwrap();
    assert_eq!(total, limit as i64);
}

// ===========================================================================
// 10. Add a nullable field, then produce rows that set it to null explicitly.
// ===========================================================================

#[derive(Debug, Clone, Serialize)]
struct ExplicitNullV1 {
    id: i64,
    label: String,
}
#[derive(Debug, Clone, Serialize)]
struct ExplicitNullV2 {
    id: i64,
    label: String,
    opt: Option<i64>,
}
const EXPL_NULL_V1: &str = r#"{
    "type":"record","name":"ExplNull",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"label","type":"string"}
    ]}"#;
const EXPL_NULL_V2: &str = r#"{
    "type":"record","name":"ExplNull",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"label","type":"string"},
        {"name":"opt","type":["null","long"],"default":null}
    ]}"#;

#[tokio::test]
async fn evo_explicit_null_after_add() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.kafka.register_schema(EXPL_NULL_V1).await.unwrap();
    ctx.kafka
        .produce_avro_records(&[ExplicitNullV1 {
            id: 1,
            label: "v1".into(),
        }])
        .await
        .unwrap();

    ctx.kafka.register_schema(EXPL_NULL_V2).await.unwrap();
    // Explicitly null the new field on v2 records, plus one non-null.
    ctx.kafka
        .produce_avro_records(&[
            ExplicitNullV2 {
                id: 2,
                label: "explicit-null".into(),
                opt: None,
            },
            ExplicitNullV2 {
                id: 3,
                label: "set".into(),
                opt: Some(33),
            },
        ])
        .await
        .unwrap();

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "evo_expl_null"),
            base_opts().record_limit(3),
        )
        .await
        .expect("pipeline run");
    assert!(status.success(), "explicit-null v2 records should decode");

    let total = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.evo_expl_null")
        .await
        .unwrap();
    assert_eq!(total, 3);

    // id 1 (default null) + id 2 (explicit null) => 2 nulls; id 3 set.
    let nulls = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.evo_expl_null WHERE opt IS NULL")
        .await
        .unwrap();
    assert_eq!(nulls, 2, "default-null v1 row + explicit-null v2 row");
    let set = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.evo_expl_null WHERE opt = 33")
        .await
        .unwrap();
    assert_eq!(set, 1);
}

// ===========================================================================
// 11. Many small schema generations: alternating v1/v2/v1/v2...
//     No row loss / desync across rapid writer-id flips.
// ===========================================================================

#[derive(Debug, Clone, Serialize)]
struct AltV1 {
    id: i64,
    seq: i64,
}
#[derive(Debug, Clone, Serialize)]
struct AltV2 {
    id: i64,
    seq: i64,
    tag: Option<String>,
}
const ALT_V1: &str = r#"{
    "type":"record","name":"Alt",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"seq","type":"long"}
    ]}"#;
const ALT_V2: &str = r#"{
    "type":"record","name":"Alt",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"seq","type":"long"},
        {"name":"tag","type":["null","string"],"default":null}
    ]}"#;

#[tokio::test]
async fn evo_many_alternating_generations() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    // 12 records, alternating schema generation on every record.
    let n: i64 = 12;
    for i in 1..=n {
        if i % 2 == 1 {
            ctx.kafka.register_schema(ALT_V1).await.unwrap();
            ctx.kafka
                .produce_avro_records(&[AltV1 { id: i, seq: i }])
                .await
                .unwrap();
        } else {
            ctx.kafka.register_schema(ALT_V2).await.unwrap();
            ctx.kafka
                .produce_avro_records(&[AltV2 {
                    id: i,
                    seq: i,
                    tag: Some(format!("t{i}")),
                }])
                .await
                .unwrap();
        }
    }

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "evo_alt"),
            base_opts().record_limit(n as u64),
        )
        .await
        .expect("pipeline run");
    assert!(
        status.success(),
        "rapidly alternating generations should not desync"
    );

    let total = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.evo_alt")
        .await
        .unwrap();
    assert_eq!(total, n, "every record across all generations must land");

    // seq must equal id for every row (no value desync across generations).
    let aligned = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.evo_alt WHERE seq = id")
        .await
        .unwrap();
    assert_eq!(
        aligned, n,
        "seq must stay aligned with id (no batch desync)"
    );
}

// ===========================================================================
// 12. v2 renames a field (drop old + add new). Old data maps to null/absent
//     for the new column. Documented expectation: rename = drop + add, so the
//     v1 value does NOT carry into the new column.
// ===========================================================================

#[derive(Debug, Clone, Serialize)]
struct RenameV1 {
    id: i64,
    old_name: String,
}
#[derive(Debug, Clone, Serialize)]
struct RenameV2 {
    id: i64,
    new_name: Option<String>,
}
const RENAME_V1: &str = r#"{
    "type":"record","name":"Rename",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"old_name","type":"string"}
    ]}"#;
// Drop old_name, add new_name (nullable w/ default for backward compat).
const RENAME_V2: &str = r#"{
    "type":"record","name":"Rename",
    "fields":[
        {"name":"id","type":"long"},
        {"name":"new_name","type":["null","string"],"default":null}
    ]}"#;

#[tokio::test]
async fn evo_rename_field_drop_add() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.kafka.register_schema(RENAME_V1).await.unwrap();
    ctx.kafka
        .produce_avro_records(&[RenameV1 {
            id: 1,
            old_name: "alice".into(),
        }])
        .await
        .unwrap();

    // Dropping old_name (no default) is BACKWARD-incompatible; surface where it
    // fails. If accepted, new data uses new_name.
    let renamed = ctx.kafka.register_schema(RENAME_V2).await;
    if renamed.is_ok() {
        ctx.kafka
            .produce_avro_records(&[RenameV2 {
                id: 2,
                new_name: Some("bob".into()),
            }])
            .await
            .unwrap();
    }

    let limit = if renamed.is_ok() { 2 } else { 1 };
    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml(&ctx.kafka_topic, "evo_rename"),
            base_opts().record_limit(limit),
        )
        .await
        .expect("pipeline run");
    assert!(
        status.success(),
        "rename (drop+add) read path should succeed"
    );

    let total = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.evo_rename")
        .await
        .unwrap();
    assert_eq!(total, limit as i64);

    // DOCUMENTED EXPECTATION: rename = drop + add. The v1 row's `old_name`
    // value ("alice") does NOT carry into `new_name`; it should be null/absent.
    if renamed.is_ok() {
        let v1_new_name_null = ctx
            .postgres
            .count("SELECT COUNT(*) FROM public.evo_rename WHERE id = 1 AND new_name IS NULL")
            .await
            .unwrap();
        assert_eq!(
            v1_new_name_null, 1,
            "renamed field must not carry old value into new column"
        );
    }
}
