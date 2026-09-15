//! Full-binary Parquet schema-metadata and scale preservation tests.
//! Fixtures are checked in; Python/pyarrow is not required to run this target.
//! The arbitrary-decimal assertions cover new-type integration, not a claimed
//! formerly-supported Parquet extension contract: legacy metadata was skipped too.
use std::{fs, time::Duration};
use streamling_e2e::{init_tracing, PipelineOpts, PrintSinkOutput, TestContext};

fn fixture(name: &str) -> &'static [u8] {
    match name {
        "arb_s0_id1" => include_bytes!("fixtures/deep_v3/arb_s0_id1.parquet"),
        "arb_s0_id2" => include_bytes!("fixtures/deep_v3/arb_s0_id2.parquet"),
        "arb_s2_id1" => include_bytes!("fixtures/deep_v3/arb_s2_id1.parquet"),
        "arb_s2_id2" => include_bytes!("fixtures/deep_v3/arb_s2_id2.parquet"),
        "native_s0_id1" => include_bytes!("fixtures/deep_v3/native_s0_id1.parquet"),
        "native_s0_id2" => include_bytes!("fixtures/deep_v3/native_s0_id2.parquet"),
        "native_s2_id1" => include_bytes!("fixtures/deep_v3/native_s2_id1.parquet"),
        "native_s2_id2" => include_bytes!("fixtures/deep_v3/native_s2_id2.parquet"),
        "binary_s0_id1" => include_bytes!("fixtures/deep_v3/binary_s0_id1.parquet"),
        "binary_s0_id2" => include_bytes!("fixtures/deep_v3/binary_s0_id2.parquet"),
        "binary_s2_id2" => include_bytes!("fixtures/deep_v3/binary_s2_id2.parquet"),
        _ => panic!("unknown fixture {name}"),
    }
}
fn canonical(s: &str) -> &str {
    if s.contains('.') {
        s.trim_end_matches('0').trim_end_matches('.')
    } else {
        s
    }
}

async fn exercise(kind: &str, files: &[(&str, &str)], mixed: bool, filter: bool) {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    let dir = ctx.temp_dir.path().join("input");
    fs::create_dir(&dir).unwrap();
    for (source, destination) in files {
        fs::write(dir.join(destination), fixture(source)).unwrap();
    }
    let transforms = if filter {
        let rhs = if kind == "arb" {
            "to_decimal_arb_from_string('1',100,0)"
        } else {
            "CAST(1 AS DECIMAL(30,0))"
        };
        format!(
            "\n  selected:\n    type: sql\n    primary_key: id\n    sql: \"SELECT id,amount,_gs_op FROM source WHERE amount={rhs}\"\n"
        )
    } else {
        " {}\n".into()
    };
    let from = if filter { "selected" } else { "source" };
    let pipeline = format!(
        r#"
sources:
  source:
    type: file
    path: {path}/
    format: parquet
    primary_key: id
    mode:
      type: bounded
transforms:{transforms}
sinks:
  out:
    type: print
    from: {from}
    sample_every: 1
"#,
        path = dir.display()
    );
    let opts = PipelineOpts::new()
        .timeout(Duration::from_secs(45))
        .env("RUST_LOG", "info")
        .env("STREAMLING__PLUGIN__PATH", "")
        .env("STREAMLING__PLUGIN__PREPROCESSOR_IDS", "")
        .env("STREAMLING__PLUGIN__SIDE_OUTPUT_IDS", "")
        .env("STREAMLING__RECORD_BATCH_SIZE", "1");
    let out = ctx.run_pipeline_raw(&pipeline, opts).await.unwrap();
    let parsed = PrintSinkOutput::parse(&out.stderr);
    let rows: Vec<(i64, String)> = parsed
        .rows()
        .iter()
        .map(|row| {
            let value = &row.data["amount"];
            (
                row.data["id"].as_i64().unwrap(),
                value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string()),
            )
        })
        .collect();
    eprintln!(
        "V3 PARQUET kind={kind} files={files:?} mixed={mixed} filter={filter} status={:?} stored={rows:?}",
        out.status
    );
    if !out.status.success() {
        eprintln!("V3 PARQUET REJECTION {}", out.stderr);
        assert!(
            kind == "arb" || mixed,
            "native single/same-scale and ordinary binary controls must succeed"
        );
        assert!(
            rows.is_empty(),
            "a rejected source must not leave partial stored values"
        );
        let error = out.stderr.to_lowercase();
        assert!(
            error.contains("unsupported decimal_arb")
                || error.contains("decimal_arb is not supported")
                || (mixed && error.contains("schema error: fail to merge schema field 'amount'"))
                || (mixed && error.contains("schema error: fail to merge field 'amount' due to conflicting metadata data value for key arrow:extension:metadata")),
            "only an explicit source rejection is an acceptable alternative to preserving numeric values"
        );
        return;
    }
    assert_eq!(
        rows.len(),
        files.len(),
        "every numeric-one input must reach the sink, including through equality filtering"
    );
    let mut expected_ids = files
        .iter()
        .map(|(name, _)| name.rsplit_once("_id").unwrap().1.parse::<i64>().unwrap())
        .collect::<Vec<_>>();
    let mut actual_ids = rows.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    expected_ids.sort_unstable();
    actual_ids.sort_unstable();
    assert_eq!(
        actual_ids, expected_ids,
        "preserve each input row exactly once"
    );
    for (id, value) in rows {
        if kind == "binary" {
            let (name, _) = files
                .iter()
                .find(|(name, _)| name.ends_with(&format!("id{id}")))
                .unwrap();
            let expected = if name.contains("_s2_") {
                "0064"
            } else {
                "0001"
            };
            assert_eq!(value, expected, "ordinary binary bytes must remain bytes");
            continue;
        }
        assert_eq!(
            canonical(&value),
            "1",
            "each file stores numeric1 with its own declared scale"
        );
    }
}

#[tokio::test]
async fn v3_parquet_arb_single_scale0() {
    exercise("arb", &[("arb_s0_id1", "a.parquet")], false, false).await;
}
#[tokio::test]
async fn v3_parquet_arb_single_scale2() {
    exercise("arb", &[("arb_s2_id2", "a.parquet")], false, false).await;
}
#[tokio::test]
async fn v3_parquet_arb_same_scale_control() {
    exercise(
        "arb",
        &[("arb_s0_id1", "a.parquet"), ("arb_s0_id2", "b.parquet")],
        false,
        false,
    )
    .await;
}
#[tokio::test]
async fn v3_parquet_arb_mixed_scales() {
    exercise(
        "arb",
        &[("arb_s0_id1", "a.parquet"), ("arb_s2_id2", "b.parquet")],
        true,
        false,
    )
    .await;
}
#[tokio::test]
async fn v3_parquet_arb_mixed_scales_reversed_order() {
    exercise(
        "arb",
        &[("arb_s0_id1", "b.parquet"), ("arb_s2_id2", "a.parquet")],
        true,
        false,
    )
    .await;
}
#[tokio::test]
async fn v3_parquet_native_single_scale2_control() {
    exercise("native", &[("native_s2_id2", "a.parquet")], false, false).await;
}
#[tokio::test]
async fn v3_parquet_native_same_scale_control() {
    exercise(
        "native",
        &[
            ("native_s0_id1", "a.parquet"),
            ("native_s0_id2", "b.parquet"),
        ],
        false,
        false,
    )
    .await;
}
#[tokio::test]
async fn v3_parquet_native_mixed_scales_control() {
    exercise(
        "native",
        &[
            ("native_s0_id1", "a.parquet"),
            ("native_s2_id2", "b.parquet"),
        ],
        true,
        false,
    )
    .await;
}
#[tokio::test]
async fn v3_parquet_arb_same_scale_filter_control() {
    exercise(
        "arb",
        &[("arb_s0_id1", "a.parquet"), ("arb_s0_id2", "b.parquet")],
        false,
        true,
    )
    .await;
}
#[tokio::test]
async fn v3_parquet_arb_mixed_scale_filter() {
    exercise(
        "arb",
        &[("arb_s0_id1", "a.parquet"), ("arb_s2_id2", "b.parquet")],
        true,
        true,
    )
    .await;
}

#[tokio::test]
async fn v3_parquet_plain_binary_control() {
    exercise(
        "binary",
        &[
            ("binary_s0_id1", "a.parquet"),
            ("binary_s2_id2", "b.parquet"),
        ],
        false,
        false,
    )
    .await;
}
#[tokio::test]
async fn v3_parquet_native_same_scale_filter_control() {
    exercise(
        "native",
        &[
            ("native_s0_id1", "a.parquet"),
            ("native_s0_id2", "b.parquet"),
        ],
        false,
        true,
    )
    .await;
}
