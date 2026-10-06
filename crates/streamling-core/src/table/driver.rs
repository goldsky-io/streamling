//! Polls a table's plan so the table is populated, and its checkpoint markers
//! acked, even when nothing reads it.

use crate::checkpoints::checkpoint_management::{
    CheckpointControl, extract_checkpoint_messages, now_ms, process_checkpoint_acks,
};
use crate::session::SessionManager;
use crate::telemetry::recorder::get_metrics_recorder;
use datafusion::error::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{displayable, execute_stream};
use futures::StreamExt;
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, info};

/// Plans and polls the table's `plan`, acking its markers as `table_name`.
/// This mirrors a sink future but streams instead of collecting, because the
/// output is unbounded and discarded.
pub async fn run(
    session_manager: SessionManager,
    plan: LogicalPlan,
    table_name: String,
    metric_metadata_id: String,
    checkpoint_control: CheckpointControl,
) -> Result<()> {
    let df = session_manager.new_df(plan);
    let task_ctx = Arc::new(df.task_ctx());
    let stream = df.create_physical_plan().await.and_then(|physical_plan| {
        info!(
            "Table [{}] physical plan:\n{}",
            table_name,
            displayable(physical_plan.as_ref()).indent(true)
        );
        execute_stream(physical_plan, task_ctx)
    });
    match stream {
        Ok(stream) => {
            drive(
                stream,
                &table_name,
                &metric_metadata_id,
                &checkpoint_control,
            )
            .await
        }
        Err(err) => {
            error!("Table driver [{}] failed to start: {}", table_name, err);
            Err(err)
        }
    }
}

async fn drive(
    mut stream: SendableRecordBatchStream,
    table_name: &str,
    metric_metadata_id: &str,
    checkpoint_control: &CheckpointControl,
) -> Result<()> {
    let metrics_recorder = get_metrics_recorder();
    let result = loop {
        match stream.next().await {
            Some(Ok(batch)) => process_checkpoint_acks(
                extract_checkpoint_messages(batch.schema().metadata()),
                now_ms(),
                Instant::now(),
                &metrics_recorder,
                metric_metadata_id,
                table_name,
            ),
            Some(Err(err)) => break Err(err),
            None => break Ok(()),
        }
    };
    match &result {
        // The table will never ack again, so the coordinator stops waiting on it.
        Ok(()) => checkpoint_control.sink_completed(table_name),
        // Same rationale as a failed sink: staying registered keeps the epochs
        // it touched from finalizing as if their data were durable.
        Err(err) => error!(
            "Table driver [{}] completed with error: {}",
            table_name, err
        ),
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoints::channels::{subscribe_with_id, unsubscribe};
    use crate::checkpoints::checkpoint_management::{
        CHECKPOINT_COORDINATOR_CHANNEL, CheckpointCoordinator, CheckpointEpoch, CheckpointMessage,
        enrich_batch_metadata_with_checkpoints,
    };
    use arrow::array::RecordBatch;
    use arrow::datatypes::Schema;
    use datafusion::error::DataFusionError;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use serial_test::serial;
    use std::collections::HashMap;

    const COORDINATOR_TIMEOUT_SEC: u64 = 300;
    const NEVER_SEC: u64 = 3600;

    fn marker_batch(epoch: u64) -> RecordBatch {
        let mut metadata = HashMap::new();
        enrich_batch_metadata_with_checkpoints(
            &mut metadata,
            &[CheckpointMessage::Marker {
                epoch: CheckpointEpoch(epoch),
                created_at_ms: 0,
            }],
        );
        RecordBatch::new_empty(Arc::new(Schema::empty().with_metadata(metadata)))
    }

    fn stream(batches: Vec<Result<RecordBatch>>) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            futures::stream::iter(batches),
        ))
    }

    // `#[serial]`: acks and the coordinator use process-wide checkpoint channels.
    #[tokio::test]
    #[serial]
    async fn acks_every_marker_under_the_table_name() {
        const TABLE: &str = "driver_acks_table";
        let (acks, subscriber_id) = subscribe_with_id(CHECKPOINT_COORDINATOR_CHANNEL);

        drive(
            stream(vec![Ok(marker_batch(1)), Ok(marker_batch(2))]),
            TABLE,
            "app::driver_acks_table",
            &CheckpointCoordinator::new().control(),
        )
        .await
        .unwrap();
        unsubscribe(CHECKPOINT_COORDINATOR_CHANNEL, subscriber_id);

        let acked: Vec<u64> = acks
            .try_iter()
            .filter_map(|message| match message {
                CheckpointMessage::Ack { epoch, sink_id } if sink_id == TABLE => Some(epoch.0),
                _ => None,
            })
            .collect();
        assert_eq!(acked, [1, 2]);
    }

    #[tokio::test]
    #[serial]
    async fn completes_the_table_when_its_stream_ends() {
        const TABLE: &str = "driver_done_table";
        let mut coordinator = CheckpointCoordinator::with_timeout(COORDINATOR_TIMEOUT_SEC);
        coordinator.start(NEVER_SEC, vec![TABLE.to_string()]);
        let control = coordinator.control();
        control.begin_terminal_checkpoint();

        drive(stream(vec![]), TABLE, "app::driver_done_table", &control)
            .await
            .unwrap();

        assert!(
            control.is_terminal_finalized(),
            "the table was the only ack participant"
        );
        coordinator.stop().await;
    }

    #[tokio::test]
    #[serial]
    async fn keeps_the_table_registered_when_its_stream_fails() {
        const TABLE: &str = "driver_failed_table";
        let mut coordinator = CheckpointCoordinator::with_timeout(COORDINATOR_TIMEOUT_SEC);
        coordinator.start(NEVER_SEC, vec![TABLE.to_string()]);
        let control = coordinator.control();
        control.begin_terminal_checkpoint();

        let result = drive(
            stream(vec![Err(DataFusionError::Execution("boom".to_string()))]),
            TABLE,
            "app::driver_failed_table",
            &control,
        )
        .await;

        assert!(result.is_err());
        assert!(
            !control.is_terminal_finalized(),
            "a failed table must keep the epochs it touched from finalizing"
        );
        coordinator.stop().await;
    }
}
