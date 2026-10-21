use crate::error::ResultExt;
use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::Result;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

/// Where a side output on a source runs relative to the source's `filter`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SideOutputPlacement {
    /// Sees every row the source reads.
    BeforeSourceFilter,
    /// Sees only the rows the source's filter keeps.
    AfterSourceFilter,
}

#[async_trait]
pub trait SourceSideOutput: Send + Sync + Debug {
    /// Returns schema metadata entries for the node to add to `batch` before
    /// it yields it.
    async fn process(&self, batch: &RecordBatch) -> Result<HashMap<String, String>>;

    /// Ignored on a transform, which has no filter of its own.
    fn placement(&self) -> SideOutputPlacement {
        SideOutputPlacement::BeforeSourceFilter
    }
}

pub trait SupportsSideOutputs: Send + Sync {
    fn add_side_output(&self, side_output: Arc<dyn SourceSideOutput>);

    /// The name of the node's `SideOutputExec`.
    fn reference_name(&self) -> &str;
}

/// `batch` with `entries` added to its schema metadata.
pub fn with_metadata(
    batch: RecordBatch,
    entries: HashMap<String, String>,
) -> crate::error::Result<RecordBatch> {
    let mut metadata = batch.schema().metadata().clone();
    metadata.extend(entries);
    let schema = Arc::new(Schema::new_with_metadata(
        batch.schema().fields().clone(),
        metadata,
    ));
    RecordBatch::try_new_with_options(
        schema,
        batch.columns().to_vec(),
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .streamling_context("failed to add the side outputs' metadata to the batch")
}
