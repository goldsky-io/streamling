//! Utility functions for RecordBatch operations

use crate::error::{Result, ResultExt};
use arrow_schema::Schema;
use datafusion::arrow::array::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

/// Enriches a RecordBatch with metadata from another batch or metadata map.
/// Creates a new RecordBatch with the same data but with enriched schema metadata.
///
/// # Arguments
/// * `batch` - The batch to enrich
/// * `metadata` - The metadata to add to the batch schema
///
/// # Returns
/// A new RecordBatch with the enriched schema metadata
pub fn enrich_batch_with_metadata(
    batch: RecordBatch,
    metadata: HashMap<String, String>,
) -> Result<RecordBatch> {
    let schema = batch.schema();
    let enriched_schema = Arc::new(Schema::new_with_metadata(schema.fields().clone(), metadata));
    RecordBatch::try_new(enriched_schema, batch.columns().to_vec())
        .streamling_context("failed to create RecordBatch with enriched metadata")
}
