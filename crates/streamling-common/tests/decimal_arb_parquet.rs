//! Controls for the Parquet metadata options underlying the full-binary fixture tests.
//! No production change: explicit format settings are exercised only inside tests.
use arrow::array::RecordBatch;
use datafusion::{
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    prelude::{SessionConfig, SessionContext},
};
use std::{fs, path::PathBuf, sync::Arc};
use streamling_common::formats::{FromArrowConverter, json::FromArrowToJsonConverter};

struct TempInput(PathBuf);
impl TempInput {
    fn new(mixed: bool) -> Self {
        let path =
            std::env::temp_dir().join(format!("streamling-v3-parquet-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        fs::write(
            path.join("a.parquet"),
            include_bytes!("../../streamling-e2e/tests/fixtures/decimal_arb/arb_s0_id1.parquet"),
        )
        .unwrap();
        if mixed {
            fs::write(
                path.join("b.parquet"),
                include_bytes!(
                    "../../streamling-e2e/tests/fixtures/decimal_arb/arb_s2_id2.parquet"
                ),
            )
            .unwrap();
        }
        Self(path)
    }
}
impl Drop for TempInput {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

async fn scan(
    mixed: bool,
    explicit: bool,
    session_options: bool,
) -> datafusion::error::Result<Vec<RecordBatch>> {
    let dir = TempInput::new(mixed);
    let mut config = SessionConfig::new();
    if session_options {
        config = config
            .set_bool("datafusion.execution.parquet.skip_metadata", false)
            .set_bool(
                "datafusion.execution.parquet.schema_force_view_types",
                false,
            );
    }
    let ctx = SessionContext::new_with_config(config);
    let format = if explicit {
        ParquetFormat::default()
            .with_skip_metadata(false)
            .with_force_view_types(false)
    } else {
        ParquetFormat::default()
    };
    let config = ListingTableConfig::new(ListingTableUrl::parse(format!("{}/", dir.0.display()))?)
        .with_listing_options(ListingOptions::new(Arc::new(format)).with_file_extension(".parquet"))
        .infer_schema(&ctx.state())
        .await?;
    let table = Arc::new(ListingTable::try_new(config)?);
    let batches = ctx.read_table(table)?.collect().await?;
    for batch in &batches {
        eprintln!(
            "explicit={explicit} session_options={session_options} schema={:?}",
            batch.schema()
        );
    }
    Ok(batches)
}
fn amounts(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            FromArrowToJsonConverter::new()
                .convert_from_batch(b)
                .unwrap()
        })
        .map(|r| {
            let row: serde_json::Value = serde_json::from_slice(&r).unwrap();
            row["amount"].as_str().unwrap().to_owned()
        })
        .collect()
}
#[tokio::test]
async fn explicit_parquet_metadata_and_storage_flags_preserve_numeric_value() {
    let b = scan(false, true, false).await.unwrap();
    assert_eq!(amounts(&b), ["1"]);
}
#[tokio::test]
async fn explicit_parquet_metadata_flags_reject_conflicting_scales() {
    let error = scan(true, true, false).await.unwrap_err().to_string();
    eprintln!("explicit-format mixed-scale rejection: {error}");
    assert!(
        error.contains("conflicting metadata") || error.contains("conflicting values"),
        "{error}"
    );
}
#[tokio::test]
#[ignore = "diagnostic: the file source's pre-existing explicit ParquetFormat defaults do not inherit session options"]
async fn session_flags_do_not_override_explicit_default_format_diagnostic() {
    assert_eq!(amounts(&scan(false, false, false).await.unwrap()), ["0001"]);
    assert_eq!(amounts(&scan(false, false, true).await.unwrap()), ["0001"]);
}
