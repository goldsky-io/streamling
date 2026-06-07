use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use std::fmt::Debug;
use std::sync::Arc;

#[async_trait]
pub trait SourceSideOutput: Send + Sync + Debug {
    async fn process(&self, batch: &RecordBatch) -> Result<()>;
}

pub trait SupportsSideOutputs: Send + Sync {
    fn add_side_output(&self, side_output: Arc<dyn SourceSideOutput>);
}
