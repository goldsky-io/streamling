//! Fresh black-box numeric correctness probes for PR37, using the real binary.
use apache_avro::{types::Value, Decimal};
use num_bigint::BigInt;
use serde::Deserialize;
use sqlx::FromRow;
use std::{fs, time::Duration};
use streamling_e2e::{init_tracing, PipelineOpts, TestContext, TestContextOptions};

fn opts() -> PipelineOpts {
    PipelineOpts::new()
        .timeout(Duration::from_secs(45))
        .env("STREAMLING__PLUGIN__PATH", "")
        .env("STREAMLING__PLUGIN__PREPROCESSOR_IDS", "")
        .env("STREAMLING__PLUGIN__SIDE_OUTPUT_IDS", "")
        .env("STREAMLING__RECORD_BATCH_SIZE", "1")
        .env("RUST_LOG", "info")
}

fn decimal(coefficient: i64) -> Value {
    Value::Decimal(Decimal::from(
        BigInt::from(coefficient).to_signed_bytes_be(),
    ))
}

#[derive(Debug, Deserialize, FromRow)]
struct BoolRow {
    id: i64,
    result: Option<bool>,
}

async fn kafka_predicate(sql: &str, precision: u32, expected: Option<bool>) {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.postgres
        .execute("CREATE TABLE result (id BIGINT PRIMARY KEY, result BOOLEAN)")
        .await
        .unwrap();
    let schema = format!(
        r#"{{"type":"record","name":"V3","fields":[
      {{"name":"id","type":"long"}},
      {{"name":"a","type":{{"type":"bytes","logicalType":"decimal","precision":{precision},"scale":0}}}},
      {{"name":"b","type":{{"type":"bytes","logicalType":"decimal","precision":{precision},"scale":2}}}},
      {{"name":"c","type":{{"type":"bytes","logicalType":"decimal","precision":{precision},"scale":0}}}},
      {{"name":"xs","type":{{"type":"array","items":{{"type":"bytes","logicalType":"decimal","precision":{precision},"scale":0}}}}}}
    ]}}"#
    );
    ctx.kafka
        .produce_avro_value(
            &schema,
            Value::Record(vec![
                ("id".into(), Value::Long(1)),
                ("a".into(), decimal(255)),
                ("b".into(), decimal(25600)),
                ("c".into(), decimal(256)),
                ("xs".into(), Value::Array(vec![decimal(255), decimal(256)])),
            ]),
        )
        .await
        .unwrap();
    let yaml = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms:
  evaluated:
    type: sql
    primary_key: id
    sql: >-
      {sql}
sinks:
  out:
    type: postgres
    from: evaluated
    table: result
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
"#,
        topic = ctx.kafka_topic
    );
    let outcome = ctx.run_pipeline_raw(&yaml, opts().record_limit(1)).await;
    if let Ok(ref out) = outcome {
        eprintln!(
            "V3 KAFKA PIPELINE precision={precision} sql={sql} status={:?}",
            out.status
        );
        if !out.status.success() {
            eprintln!("V3 PIPELINE ERROR {}", out.stderr);
        }
    }
    assert!(
        outcome.is_ok(),
        "pipeline failed before numeric verification: {outcome:?}"
    );
    let rows: Vec<BoolRow> = ctx
        .postgres
        .query("SELECT id,result FROM result ORDER BY id")
        .await
        .unwrap();
    eprintln!("V3 KAFKA ROWS precision={precision} sql={sql} rows={rows:?}");
    assert!(
        outcome.as_ref().unwrap().status.success(),
        "pipeline unsuccessful: {outcome:?}"
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 1);
    assert_eq!(
        rows[0].result, expected,
        "precision={precision}, query={sql}"
    );
}

const DIRECT: &str = "SELECT id, a<c AS result FROM src";
const DERIVED: &str = "SELECT id, v<c AS result,_gs_op FROM (SELECT id,CASE WHEN id=1 THEN a ELSE c END AS v,c,_gs_op FROM src) q";
const NULL_EXTREME: &str = "SELECT id,greatest(a,c,NULL)=c AS result FROM src";
const NULL_BETWEEN: &str = "SELECT id,a BETWEEN NULL AND c AS result FROM src";
const LIST_MIN: &str = "SELECT id,array_min(xs)=a AS result FROM src";
const MIXED_SCALE: &str = "SELECT id,(CASE WHEN id=1 THEN a ELSE b END)=to_decimal_arb_from_string('2.55',100,2) AS result FROM src";

#[tokio::test]
async fn v3_e2e_decimal_direct_control() {
    kafka_predicate(DIRECT, 100, Some(true)).await;
}
#[tokio::test]
async fn v3_e2e_native_derived_control() {
    kafka_predicate(DERIVED, 30, Some(true)).await;
}
#[tokio::test]
async fn v3_e2e_decimal_derived_comparison() {
    kafka_predicate(DERIVED, 100, Some(true)).await;
}
#[tokio::test]
async fn v3_e2e_native_null_extreme_control() {
    kafka_predicate(NULL_EXTREME, 30, Some(true)).await;
}
#[tokio::test]
async fn v3_e2e_decimal_null_extreme() {
    kafka_predicate(NULL_EXTREME, 100, Some(true)).await;
}
#[tokio::test]
async fn v3_e2e_native_null_between_control() {
    kafka_predicate(NULL_BETWEEN, 30, None).await;
}
#[tokio::test]
async fn v3_e2e_decimal_null_between() {
    kafka_predicate(NULL_BETWEEN, 100, None).await;
}
#[tokio::test]
async fn v3_e2e_native_list_min_control() {
    kafka_predicate(LIST_MIN, 30, Some(true)).await;
}
#[tokio::test]
async fn v3_e2e_decimal_list_min() {
    kafka_predicate(LIST_MIN, 100, Some(true)).await;
}
#[tokio::test]
async fn v3_e2e_decimal_mixed_scale_false_equality() {
    kafka_predicate(MIXED_SCALE, 100, Some(false)).await;
}

async fn bounded_aggregate(precision: u32, operation: &str, expected: &str, boolean: bool) {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    let input = ctx.temp_dir.path().join("input");
    fs::create_dir(&input).unwrap();
    fs::write(input.join("rows.csv"), "id,amount\n1,1\n2,1\n3,3\n4,\n").unwrap();
    let cast = if precision > 76 {
        format!("to_decimal_arb_from_int(amount,{precision},0)")
    } else {
        format!("CAST(amount AS DECIMAL({precision},0))")
    };
    let subquery = format!("(SELECT {operation}(DISTINCT a) FROM t HAVING COUNT(*)>0)");
    let text_result = if boolean {
        let scale = u32::from(operation == "AVG");
        let rhs = if precision > 76 {
            format!("to_decimal_arb_from_string('{expected}',{precision},{scale})")
        } else {
            format!("CAST('{expected}' AS DECIMAL({precision},{scale}))")
        };
        format!("{subquery}={rhs}")
    } else {
        format!("CAST({subquery} AS VARCHAR)")
    };
    let yaml = format!(
        r#"
sources:
  raw:
    type: file
    path: {path}/
    format: csv
    primary_key: id
    mode:
      type: bounded
transforms:
  t:
    type: sql
    primary_key: id
    sql: "SELECT id,{cast} AS a FROM raw"
  evaluated:
    type: sql
    primary_key: id
    sql: "SELECT id,{text_result} AS result FROM t WHERE id=1"
sinks:
  out:
    type: print
    from: evaluated
    sample_every: 1
"#,
        path = input.display()
    );
    let output = ctx.run_pipeline_with_capture(&yaml, opts()).await;
    eprintln!("V3 BOUNDED AGGREGATE precision={precision} op={operation} output={output:?}");
    let output = output.unwrap();
    let actual = output.column_values("result");
    assert_eq!(actual.len(), 1);
    if boolean {
        assert_eq!(actual[0].as_bool(), Some(true));
    } else {
        assert_eq!(actual[0].as_str(), Some(expected));
    }
}

#[tokio::test]
async fn v3_e2e_bounded_native_distinct_sum_control() {
    bounded_aggregate(30, "SUM", "4", false).await;
}
#[tokio::test]
async fn v3_e2e_bounded_decimal_distinct_sum() {
    bounded_aggregate(100, "SUM", "4", false).await;
}
#[tokio::test]
async fn v3_e2e_bounded_decimal_distinct_avg() {
    bounded_aggregate(100, "AVG", "2.0", false).await;
}

#[tokio::test]
async fn v3_e2e_bounded_native_distinct_boolean_control() {
    bounded_aggregate(30, "SUM", "4", true).await;
    bounded_aggregate(30, "AVG", "2", true).await;
}
#[tokio::test]
async fn v3_e2e_bounded_decimal_distinct_sum_boolean() {
    bounded_aggregate(100, "SUM", "4", true).await;
}
#[tokio::test]
async fn v3_e2e_bounded_decimal_distinct_avg_boolean() {
    bounded_aggregate(100, "AVG", "2", true).await;
}

#[derive(Debug, Deserialize, FromRow)]
struct TextRow {
    id: i64,
    value: String,
}

async fn nested_script(script: bool) {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.postgres
        .execute("CREATE TABLE nested_result(id BIGINT PRIMARY KEY, nested JSONB)")
        .await
        .unwrap();
    let schema = r#"{"type":"record","name":"V3Nested","fields":[
      {"name":"id","type":"long"},
      {"name":"nested","type":{"type":"record","name":"N","fields":[
        {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}}
      ]}}
    ]}"#;
    for (id, coefficient) in [(1, 1000), (2, -256)] {
        ctx.kafka
            .produce_avro_value(
                schema,
                Value::Record(vec![
                    ("id".into(), Value::Long(id)),
                    (
                        "nested".into(),
                        Value::Record(vec![("amount".into(), decimal(coefficient))]),
                    ),
                ]),
            )
            .await
            .unwrap();
    }
    let transforms = if script {
        r#"
transforms:
  identity:
    type: script
    from: src
    language: javascript
    primary_key: id
    batch_size: 1
    script: |
      function(input) { return {id: input.id, nested: input.nested}; }
"#
    } else {
        "transforms: {}"
    };
    let from = if script { "identity" } else { "src" };
    let output_topic = ctx.create_kafka_topic("nested_out").await.unwrap();
    let yaml = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
{transforms}
sinks:
  out:
    type: kafka
    from: {from}
    topic: {out_topic}
    topic_partitions: 1
    data_format: avro
"#,
        topic = ctx.kafka_topic,
        out_topic = output_topic.topic
    );
    let out = ctx
        .run_pipeline_raw(&yaml, opts().record_limit(2))
        .await
        .unwrap();
    eprintln!(
        "V3 NESTED SCRIPT first-hop script={script} status={:?}",
        out.status
    );
    if !out.status.success() {
        eprintln!("{}", out.stderr);
    }
    assert!(out.status.success());
    let readback = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms: {{}}
sinks:
  out:
    type: postgres
    from: src
    table: nested_result
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
"#,
        topic = output_topic.topic
    );
    let out = ctx
        .run_pipeline_raw(&readback, opts().record_limit(2))
        .await
        .unwrap();
    eprintln!("V3 NESTED SCRIPT script={script} status={:?}", out.status);
    if !out.status.success() {
        eprintln!("{}", out.stderr);
    }
    let rows: Vec<TextRow> = ctx
        .postgres
        .query("SELECT id,nested->>'amount' AS value FROM nested_result ORDER BY id")
        .await
        .unwrap();
    eprintln!("V3 NESTED SCRIPT script={script} rows={rows:?}");
    assert!(out.status.success());
    assert_eq!(
        rows.iter()
            .map(|r| (r.id, r.value.as_str()))
            .collect::<Vec<_>>(),
        vec![(1, "1000"), (2, "-256")]
    );
}

#[tokio::test]
async fn v3_e2e_nested_decimal_direct_postgres_control() {
    nested_script(false).await;
}
#[tokio::test]
async fn v3_e2e_nested_decimal_identity_script_postgres() {
    nested_script(true).await;
}

#[derive(Debug, Deserialize, clickhouse::Row)]
struct ChTextRow {
    id: i64,
    value: String,
}

async fn bounded_union(b_scale: u32) {
    init_tracing();
    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .unwrap();
    let input = ctx.temp_dir.path().join("input");
    fs::create_dir(&input).unwrap();
    fs::write(input.join("rows.csv"), "id\n1\n").unwrap();
    let yaml = format!(
        r#"
sources:
  raw:
    type: file
    path: {path}/
    format: csv
    primary_key: id
    mode:
      type: bounded
transforms:
  t:
    type: sql
    primary_key: id
    sql: "SELECT id,to_decimal_arb_from_int(id,100,0) AS a,to_decimal_arb_from_int(id,100,{b_scale}) AS b,_gs_op FROM raw"
  combined:
    type: sql
    primary_key: id
    sql: "SELECT id,a AS value,_gs_op FROM t UNION ALL SELECT id+1,b AS value,_gs_op FROM t"
sinks:
  out:
    type: clickhouse
    from: combined
    table: union_result
    primary_key: id
    batch_size: 1
"#,
        path = input.display()
    );
    let out = ctx
        .run_pipeline_raw(
            &yaml,
            opts().env(
                "STREAMLING__CLICKHOUSE_SINK__COLUMNS",
                r#"[{"name":"value","coerce_to":"string"}]"#,
            ),
        )
        .await
        .unwrap();
    eprintln!("V3 FULL UNION scale={b_scale} status={:?}", out.status);
    if !out.status.success() {
        eprintln!("{}", out.stderr);
    }
    assert!(out.status.success());
    let rows: Vec<ChTextRow> = ctx.clickhouse.as_ref().unwrap().query("SELECT assumeNotNull(id) AS id,assumeNotNull(value) AS value FROM union_result ORDER BY id").await.unwrap();
    eprintln!("V3 FULL UNION scale={b_scale} rows={rows:?}");
    assert_eq!(rows.len(), 2);
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.id, i as i64 + 1);
        assert_eq!(
            if row.value.contains('.') {
                row.value.trim_end_matches('0').trim_end_matches('.')
            } else {
                row.value.as_str()
            },
            "1",
            "a and b both represent exact integer 1"
        );
    }
}

#[tokio::test]
async fn v3_e2e_uniform_union_clickhouse_control() {
    bounded_union(0).await;
}
#[tokio::test]
async fn v3_e2e_mixed_union_clickhouse() {
    bounded_union(2).await;
}

async fn bounded_wide_literal(quoted: bool) {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    let input = ctx.temp_dir.path().join("input");
    fs::create_dir(&input).unwrap();
    fs::write(input.join("rows.csv"), "id\n1\n").unwrap();
    ctx.postgres
        .execute("CREATE TABLE literal_result(id BIGINT PRIMARY KEY,value NUMERIC(77,0))")
        .await
        .unwrap();
    let literal = if quoted {
        "'18446744073709551617'"
    } else {
        "18446744073709551617"
    };
    let yaml = format!(
        r#"
sources:
  raw:
    type: file
    path: {path}/
    format: csv
    primary_key: id
    mode:
      type: bounded
transforms:
  converted:
    type: sql
    primary_key: id
    sql: "SELECT id,CAST({literal} AS DECIMAL(77,0)) AS value FROM raw"
sinks:
  out:
    type: postgres
    from: converted
    table: literal_result
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
"#,
        path = input.display()
    );
    let output = ctx.run_pipeline_raw(&yaml, opts()).await.unwrap();
    eprintln!("V3 WIDE LITERAL quoted={quoted} status={:?}", output.status);
    if !output.status.success() {
        eprintln!("{}", output.stderr);
    }
    let rows: Vec<TextRow> = ctx
        .postgres
        .query("SELECT id,value::text AS value FROM literal_result ORDER BY id")
        .await
        .unwrap();
    eprintln!("V3 WIDE LITERAL quoted={quoted} rows={rows:?}");
    assert!(output.status.success());
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].value, "18446744073709551617");
}

#[tokio::test]
async fn v3_e2e_quoted_wide_literal_control() {
    bounded_wide_literal(true).await;
}
#[tokio::test]
async fn v3_e2e_unquoted_wide_literal_precision() {
    bounded_wide_literal(false).await;
}

async fn bounded_not_in(native: bool, fractional_rhs: bool) {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    let input = ctx.temp_dir.path().join("input");
    fs::create_dir(&input).unwrap();
    fs::write(
        input.join("rows.csv"),
        "id,raw_a,raw_b\n1,255,256\n2,256,255\n3,-1,0\n4,,1\n",
    )
    .unwrap();
    let a = if native {
        "CAST(raw_a AS DECIMAL(30,0))"
    } else {
        "to_decimal_arb_from_int(raw_a,100,0)"
    };
    let b = if native {
        "CAST(raw_b AS DECIMAL(30,2))"
    } else {
        "to_decimal_arb_from_int(raw_b,100,2)"
    };
    let rhs = if fractional_rhs {
        if native {
            "CAST('2.55' AS DECIMAL(30,2))"
        } else {
            "to_decimal_arb_from_string('2.55',100,2)"
        }
    } else {
        "b"
    };
    let yaml = format!(
        r#"
sources:
  raw:
    type: file
    path: {path}/
    format: csv
    primary_key: id
    mode:
      type: bounded
transforms:
  t:
    type: sql
    primary_key: id
    sql: "SELECT id,{a} AS a,{b} AS b,_gs_op FROM raw"
  evaluated:
    type: sql
    primary_key: id
    sql: "SELECT id,_gs_op FROM t WHERE a NOT IN(SELECT {rhs} FROM t)"
sinks:
  out:
    type: print
    from: evaluated
    sample_every: 1
"#,
        path = input.display()
    );
    let output = ctx.run_pipeline_with_capture(&yaml, opts()).await.unwrap();
    let mut ids = output
        .column_values("id")
        .into_iter()
        .map(|v| v.as_i64().unwrap())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    eprintln!("V3 FULL NOT IN native={native} fractional_rhs={fractional_rhs} ids={ids:?}");
    let expected = if fractional_rhs {
        vec![1, 2, 3]
    } else {
        vec![3]
    };
    assert_eq!(ids, expected);
}

#[tokio::test]
async fn v3_e2e_bounded_native_not_in_controls() {
    bounded_not_in(true, false).await;
    bounded_not_in(true, true).await;
}
#[tokio::test]
async fn v3_e2e_bounded_decimal_not_in_false_misses() {
    bounded_not_in(false, false).await;
}
#[tokio::test]
async fn v3_e2e_bounded_decimal_not_in_false_collision() {
    bounded_not_in(false, true).await;
}

async fn kafka_unnest(precision: u32) {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    let schema = format!(
        r#"{{"type":"record","name":"V3Unnest","fields":[
      {{"name":"id","type":"long"}},
      {{"name":"xs","type":{{"type":"array","items":{{"type":"bytes","logicalType":"decimal","precision":{precision},"scale":2}}}}}}
    ]}}"#
    );
    for (id, coefficient) in [
        (1, 123),
        (2, -456),
        (3, 16),
        (4, 18),
        (5, 32),
        (6, 100),
        (7, 256),
    ] {
        ctx.kafka
            .produce_avro_value(
                &schema,
                Value::Record(vec![
                    ("id".into(), Value::Long(id)),
                    ("xs".into(), Value::Array(vec![decimal(coefficient)])),
                ]),
            )
            .await
            .unwrap();
    }
    let yaml = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms:
  expanded:
    type: sql
    primary_key: id
    sql: "SELECT id,UNNEST(xs) AS value,_gs_op FROM src"
sinks:
  out:
    type: print
    from: expanded
    sample_every: 1
"#,
        topic = ctx.kafka_topic
    );
    let output = ctx
        .run_pipeline_with_capture(&yaml, opts().record_limit(7))
        .await
        .unwrap();
    let mut rows = output
        .rows()
        .iter()
        .map(|r| {
            let value = &r.data["value"];
            (
                r.data["id"].as_i64().unwrap(),
                value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string()),
            )
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    eprintln!("V3 FULL UNNEST precision={precision} rows={rows:?}");
    let expected = ["1.23", "-4.56", "0.16", "0.18", "0.32", "1.0", "2.56"]
        .iter()
        .enumerate()
        .map(|(i, s)| (i as i64 + 1, (*s).to_string()))
        .collect::<Vec<_>>();
    // Native JSON renders1.00 as1.0, while numeric strings may preserve scale.
    let normalize = |v: &str| {
        if v.contains('.') {
            v.trim_end_matches('0').trim_end_matches('.').to_owned()
        } else {
            v.to_owned()
        }
    };
    assert_eq!(
        rows.iter()
            .map(|(i, s)| (*i, normalize(s)))
            .collect::<Vec<_>>(),
        expected
            .iter()
            .map(|(i, s)| (*i, normalize(s)))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn v3_e2e_native_unnest_control() {
    kafka_unnest(30).await;
}
#[tokio::test]
async fn v3_e2e_decimal_unnest_json() {
    kafka_unnest(100).await;
}

async fn bounded_aggregate_parent(native: bool, cross_scale: bool) {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    let input = ctx.temp_dir.path().join("input");
    fs::create_dir(&input).unwrap();
    fs::write(input.join("rows.csv"), "id,amount\n1,255\n2,256\n").unwrap();
    let a = if native {
        "CAST(amount AS DECIMAL(30,0))"
    } else {
        "to_decimal_arb_from_int(amount,100,0)"
    };
    let b = if native {
        "CAST(amount AS DECIMAL(30,2))"
    } else {
        "to_decimal_arb_from_int(amount,100,2)"
    };
    let predicate = if cross_scale {
        "MIN(a)=MIN(b)"
    } else {
        "MIN(a)<MAX(a)"
    };
    let yaml = format!(
        r#"
sources:
  raw:
    type: file
    path: {path}/
    format: csv
    primary_key: id
    mode:
      type: bounded
transforms:
  t:
    type: sql
    primary_key: id
    sql: "SELECT id,{a} AS a,{b} AS b FROM raw"
  evaluated:
    type: sql
    primary_key: id
    sql: "SELECT id,(SELECT {predicate} FROM t) AS result FROM t WHERE id=1"
sinks:
  out:
    type: print
    from: evaluated
    sample_every: 1
"#,
        path = input.display()
    );
    let output = ctx.run_pipeline_with_capture(&yaml, opts()).await.unwrap();
    let actual = output.column_values("result");
    eprintln!("V3 FULL AGGREGATE PARENT native={native} predicate={predicate} actual={actual:?}");
    assert_eq!(actual.len(), 1);
    assert_eq!(actual[0].as_bool(), Some(true));
}

#[tokio::test]
async fn v3_e2e_bounded_native_aggregate_parent_controls() {
    bounded_aggregate_parent(true, false).await;
    bounded_aggregate_parent(true, true).await;
}
#[tokio::test]
async fn v3_e2e_bounded_decimal_aggregate_parent_order() {
    bounded_aggregate_parent(false, false).await;
}
#[tokio::test]
async fn v3_e2e_bounded_decimal_aggregate_parent_cross_scale() {
    bounded_aggregate_parent(false, true).await;
}
