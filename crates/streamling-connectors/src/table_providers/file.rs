//! File source backed by DataFusion's `ListingTable`.
//!
//! Reads files from a local path or remote object store (`s3://`, `gs://`) in a
//! configured format, inferring the schema (and Hive-style partition columns) at
//! startup. Because file reads are append-only, a `_gs_op = 'i'` change-operation
//! column is synthesized when the data does not already provide one.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Column, ScalarValue};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::{TableProvider, ViewTable, provider_as_source};
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, lit};

use streamling_core::data::{COLUMN_NAME_OP, RowKind};
use streamling_core::error::Result;
use streamling_core::session::SessionManager;
use streamling_core::topology::FileSourceFormat;
use streamling_core::{streamling_user_bail, streamling_user_err};

/// Builds the runtime provider for a `file` source backed by DataFusion's
/// `ListingTable`. The schema is inferred from the files at startup. Because file
/// reads are append-only, a constant `_gs_op = 'i'` column is appended (via a
/// `ViewTable`) when the inferred schema lacks it, so the source composes with
/// sinks that require the change-operation column. Files that already carry
/// `_gs_op` pass through unwrapped, but the column must be a non-nullable
/// Utf8 column to match what downstream consumers expect.
///
/// Supported path schemes: local paths, `s3://`, and `gs://`. Remote credentials,
/// region, and endpoint are read from the environment by each cloud builder's
/// `from_env()` (e.g. `AWS_*`, `GOOGLE_*`). Hive-style partition columns in the
/// path (e.g. `…/dt=2024-01-01/`) are inferred and added to the schema.
pub async fn build_file_source_provider(
    reference_name: &str,
    path: &str,
    format: FileSourceFormat,
    session_manager: &SessionManager,
) -> Result<Arc<dyn TableProvider>> {
    let table_url = ListingTableUrl::parse(path)?;

    // Local paths use the default object store; remote schemes need one
    // registered. Each cloud builder's `from_env()` folds in credentials/region/
    // endpoint from the environment (it lowercases keys before parsing, which the
    // generic `parse_url` path does not — so `from_env` is required for `AWS_*`).
    match table_url.scheme() {
        "file" => {}
        "s3" | "s3a" => {
            let store = object_store::aws::AmazonS3Builder::from_env()
                .with_url(table_url.as_str())
                .build()
                .map_err(|e| {
                    streamling_user_err!(
                        "failed to create S3 object store for path '{}': {}",
                        path,
                        e
                    )
                })?;
            session_manager
                .session_context()
                .register_object_store(table_url.object_store().as_ref(), Arc::new(store));
        }
        "gs" => {
            let store = object_store::gcp::GoogleCloudStorageBuilder::from_env()
                .with_url(table_url.as_str())
                .build()
                .map_err(|e| {
                    streamling_user_err!(
                        "failed to create GCS object store for path '{}': {}",
                        path,
                        e
                    )
                })?;
            session_manager
                .session_context()
                .register_object_store(table_url.object_store().as_ref(), Arc::new(store));
        }
        other => {
            streamling_user_bail!(
                "file source path '{}' uses unsupported scheme '{}'. \
                 Supported schemes: local paths, s3://, gs://",
                path,
                other
            );
        }
    }

    let file_format: Arc<dyn FileFormat> = match format {
        FileSourceFormat::Parquet => Arc::new(ParquetFormat::default()),
        FileSourceFormat::Csv => Arc::new(CsvFormat::default()),
        FileSourceFormat::Json => Arc::new(JsonFormat::default()),
        FileSourceFormat::Avro => Arc::new(AvroFormat),
    };

    let state = session_manager.session_state();
    let listing_options = ListingOptions::new(file_format);
    let config = ListingTableConfig::new(table_url)
        .with_listing_options(listing_options)
        // Surface Hive-style partition columns (e.g. `dt=2024-01-01/`) in the
        // schema, matching DataFusion's dynamic-file behavior. A no-op for flat
        // directories.
        .infer_partitions_from_path(&state)
        .await?
        .infer_schema(&state)
        .await?;
    let listing_table = Arc::new(ListingTable::try_new(config)?);

    let schema = listing_table.schema();

    // Schema inference over a path that matches no files yields an empty schema
    // and a silent zero-row source; fail fast instead.
    if schema.fields().is_empty() {
        streamling_user_bail!(
            "file source '{}': no files matching format {:?} found at path '{}'. \
             Check the path and that the files use the format's extension.",
            reference_name,
            format,
            path
        );
    }

    if let Ok(op_field) = schema.field_with_name(COLUMN_NAME_OP) {
        // Downstream consumers (RowKind::extract_row_kinds_from_batch) require a
        // non-nullable Utf8 op column; reject anything else rather than panicking
        // later in a sink.
        if op_field.data_type() != &DataType::Utf8 || op_field.is_nullable() {
            streamling_user_bail!(
                "file source '{}': column '{}' must be a non-nullable Utf8 column, \
                 found type {:?} (nullable: {})",
                reference_name,
                COLUMN_NAME_OP,
                op_field.data_type(),
                op_field.is_nullable()
            );
        }
        return Ok(listing_table);
    }

    // Reference each column by its exact name. `col(name)` would parse the name as a
    // SQL identifier and lowercase it (so `fieldName` -> `fieldname`), breaking case-sensitive
    // columns; `Column::new_unqualified` stores the name verbatim.
    let mut projection: Vec<Expr> = schema
        .fields()
        .iter()
        .map(|f| Expr::Column(Column::new_unqualified(f.name())))
        .collect();
    projection.push(lit(ScalarValue::Utf8(Some(RowKind::Insert.to_str()))).alias(COLUMN_NAME_OP));
    let plan = LogicalPlanBuilder::scan(
        reference_name,
        provider_as_source(listing_table as Arc<dyn TableProvider>),
        None,
    )?
    .project(projection)?
    .build()?;

    Ok(Arc::new(ViewTable::new(plan, None)))
}
