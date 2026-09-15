//! Exact SQL literal-token checks around integer and floating-point boundaries.
use std::{fs, time::Duration};
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

#[derive(Debug, sqlx::FromRow)]
struct LiteralRow {
    values: Vec<String>,
}

async fn literals(quoted: bool) {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    let mut expected = vec![
        "0",
        "1",
        "9007199254740991",
        "9007199254740992",
        "9007199254740993",
        "9223372036854775807",
        "9223372036854775809",
        "18446744073709551615",
        "18446744073709551616",
        "18446744073709551617",
        "36893488147419103232",
        "100000000000000000001",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<Vec<_>>();
    expected.push(format!("1{}1", "0".repeat(75)));
    let columns = (0..expected.len())
        .map(|i| format!("v{i} NUMERIC(77,0)"))
        .collect::<Vec<_>>()
        .join(",");
    ctx.postgres
        .execute(&format!(
            "CREATE TABLE results(id BIGINT PRIMARY KEY,{columns})"
        ))
        .await
        .unwrap();
    let expressions = expected
        .iter()
        .enumerate()
        .map(|(i, value)| {
            let literal = if quoted {
                format!("'{value}'")
            } else {
                value.clone()
            };
            format!("CAST({literal} AS DECIMAL(77,0)) AS v{i}")
        })
        .collect::<Vec<_>>()
        .join(",");
    let input = ctx.temp_dir.path().join("input");
    fs::create_dir(&input).unwrap();
    fs::write(input.join("input.csv"), "id\n1\n").unwrap();
    let yaml = format!(
        r#"
sources:
  src:
    type: file
    path: {path}/
    format: csv
    primary_key: id
    mode:
      type: bounded
transforms:
  casted:
    type: sql
    primary_key: id
    sql: "SELECT id,{expressions} FROM src"
sinks:
  out:
    type: postgres
    from: casted
    table: results
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
"#,
        path = input.display()
    );
    let opts = PipelineOpts::new()
        .timeout(Duration::from_secs(45))
        .env("RUST_LOG", "info")
        .env("STREAMLING__PLUGIN__PATH", "")
        .env("STREAMLING__PLUGIN__PREPROCESSOR_IDS", "")
        .env("STREAMLING__PLUGIN__SIDE_OUTPUT_IDS", "")
        .env("STREAMLING__RECORD_BATCH_SIZE", "1");
    let out = ctx.run_pipeline_raw(&yaml, opts).await.unwrap();
    if !out.status.success() {
        eprintln!("{}", out.stderr);
    }
    assert!(out.status.success());
    let fields = (0..expected.len())
        .map(|i| format!("v{i}::text"))
        .collect::<Vec<_>>()
        .join(",");
    let rows: Vec<LiteralRow> = ctx
        .postgres
        .query(&format!("SELECT ARRAY[{fields}] AS values FROM results"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values.len(), expected.len());
    let mismatches = expected
        .iter()
        .zip(&rows[0].values)
        .filter(|(a, b)| a != b)
        .collect::<Vec<_>>();
    eprintln!(
        "V3 LITERAL MATRIX quoted={quoted} checked={} mismatches={mismatches:?}",
        expected.len()
    );
    assert!(
        mismatches.is_empty(),
        "numeric literal tokens must preserve their exact decimal value"
    );
}

#[tokio::test]
async fn v3_quoted_literal_boundary_control() {
    literals(true).await;
}
#[tokio::test]
async fn v3_unquoted_literal_boundaries_preserve_exact_integer() {
    literals(false).await;
}
