use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
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
    async fn process(&self, batch: &RecordBatch) -> Result<()>;

    /// Ignored on a transform, which has no filter of its own.
    fn placement(&self) -> SideOutputPlacement {
        SideOutputPlacement::BeforeSourceFilter
    }
}

pub trait SupportsSideOutputs: Send + Sync {
    fn add_side_output(&self, side_output: Arc<dyn SourceSideOutput>);
}
