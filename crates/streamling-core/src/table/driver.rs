//! Polls a table's plan so the table is populated, and its checkpoint markers
//! acked, even when nothing reads it.

use crate::checkpoints::checkpoint_management::{
    CheckpointControl, extract_checkpoint_messages, last_marker_epoch, now_ms,
    process_checkpoint_acks, register_sink_streams, send_checkpoint_ack, sink_stream_done,
};
use crate::session::SessionManager;
use crate::shutdown::ComponentScope;
use crate::telemetry::recorder::get_metrics_recorder;
use datafusion::common::internal_datafusion_err;
use datafusion::error::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{ExecutionPlanProperties, displayable};
use futures::StreamExt;
use futures::future::try_join_all;
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, info};

/// Plans the table's `plan` and polls each of its partitions in a task of
/// `scope`, acking its markers as `table_name`. This mirrors a sink future
/// but streams instead of collecting, because the output is unbounded and
/// discarded.
pub async fn run(
    session_manager: SessionManager,
    plan: LogicalPlan,
    table_name: String,
    metric_metadata_id: String,
    checkpoint_control: CheckpointControl,
    scope: Arc<ComponentScope>,
) -> Result<()> {
    let df = session_manager.new_df(plan);
    let task_ctx = Arc::new(df.task_ctx());
    let streams = df.create_physical_plan().await.and_then(|physical_plan| {
        info!(
            "Table [{}] physical plan:\n{}",
            table_name,
            displayable(physical_plan.as_ref()).indent(true)
        );
        (0..physical_plan.output_partitioning().partition_count())
            .map(|partition| physical_plan.execute(partition, Arc::clone(&task_ctx)))
            .collect::<Result<Vec<_>>>()
    });
    match streams {
        Ok(streams) => {
            drive(
                streams,
                &table_name,
                &metric_metadata_id,
                &checkpoint_control,
                &scope,
            )
            .await
        }
        Err(err) => {
            error!("Table driver [{}] failed to start: {}", table_name, err);
            Err(err)
        }
    }
}

/// Polls each stream in its own task, so an upstream that runs inline in the
/// table's plan runs its partitions in parallel. As with a sink's partitions,
/// the streams share an ack gate: an epoch is acked once every stream
/// delivered its marker or ended.
async fn drive(
    streams: Vec<SendableRecordBatchStream>,
    table_name: &str,
    metric_metadata_id: &str,
    checkpoint_control: &CheckpointControl,
    scope: &ComponentScope,
) -> Result<()> {
    register_sink_streams(table_name, streams.len());
    let tasks: Vec<_> = streams
        .into_iter()
        .map(|stream| {
            scope.spawn(drive_partition(
                stream,
                table_name.to_string(),
                metric_metadata_id.to_string(),
            ))
        })
        .collect();
    // After the first failure the other tasks run until shutdown ends their
    // streams. The failed stream never reports to the gate, so no later epoch
    // is acked.
    let result = try_join_all(tasks.into_iter().map(|task| async move {
        task.await
            .map_err(|err| internal_datafusion_err!("table partition task failed: {err}"))?
    }))
    .await
    .map(|_| ());
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

async fn drive_partition(
    mut stream: SendableRecordBatchStream,
    table_name: String,
    metric_metadata_id: String,
) -> Result<()> {
    let metrics_recorder = get_metrics_recorder();
    let mut last_flushed = None;
    while let Some(batch) = stream.next().await {
        let messages = extract_checkpoint_messages(batch?.schema().metadata());
        last_flushed = last_flushed.max(last_marker_epoch(&messages));
        process_checkpoint_acks(
            messages,
            now_ms(),
            Instant::now(),
            &metrics_recorder,
            &metric_metadata_id,
            &table_name,
        );
    }
    // An ended stream never delivers another marker, so the gate counts its
    // share of every epoch after `last_flushed` as delivered. A failed stream returns
    // above, so the epochs it touched aren't acked.
    for epoch in sink_stream_done(&table_name, last_flushed) {
        send_checkpoint_ack(epoch, &table_name);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoints::channels::{subscribe_with_id, unsubscribe};
    use crate::checkpoints::checkpoint_management::{
        CHECKPOINT_COORDINATOR_CHANNEL, CheckpointCoordinator, CheckpointEpoch, CheckpointMessage,
        enrich_batch_metadata_with_checkpoints,
    };
    use crate::dynamic_table::DynamicTableRegistry;
    use crate::shutdown::ComponentScope;
    use arrow::array::RecordBatch;
    use arrow::datatypes::Schema;
    use datafusion::datasource::MemTable;
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

    fn acked(messages: impl Iterator<Item = CheckpointMessage>, table: &str) -> Vec<u64> {
        messages
            .filter_map(|message| match message {
                CheckpointMessage::Ack { epoch, sink_id } if sink_id == table => Some(epoch.0),
                _ => None,
            })
            .collect()
    }

    // `#[serial]`: acks and the coordinator use process-wide checkpoint channels.
    #[tokio::test]
    #[serial]
    async fn acks_every_marker_under_the_table_name() {
        const TABLE: &str = "driver_acks_table";
        let (acks, subscriber_id) = subscribe_with_id(CHECKPOINT_COORDINATOR_CHANNEL);

        drive(
            vec![stream(vec![Ok(marker_batch(1)), Ok(marker_batch(2))])],
            TABLE,
            "app::driver_acks_table",
            &CheckpointCoordinator::new().control(),
            &ComponentScope::detached("test"),
        )
        .await
        .unwrap();
        unsubscribe(CHECKPOINT_COORDINATOR_CHANNEL, subscriber_id);

        assert_eq!(acked(acks.try_iter(), TABLE), [1, 2]);
    }

    #[tokio::test]
    #[serial]
    async fn completes_the_table_when_its_stream_ends() {
        const TABLE: &str = "driver_done_table";
        let mut coordinator = CheckpointCoordinator::with_timeout(COORDINATOR_TIMEOUT_SEC);
        coordinator.start(NEVER_SEC, vec![TABLE.to_string()]);
        let control = coordinator.control();
        control.begin_terminal_checkpoint();

        drive(
            vec![stream(vec![])],
            TABLE,
            "app::driver_done_table",
            &control,
            &ComponentScope::detached("test"),
        )
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
            vec![stream(vec![Err(DataFusionError::Execution(
                "boom".to_string(),
            ))])],
            TABLE,
            "app::driver_failed_table",
            &control,
            &ComponentScope::detached("test"),
        )
        .await;

        assert!(result.is_err());
        assert!(
            !control.is_terminal_finalized(),
            "a failed table must keep the epochs it touched from finalizing"
        );
        coordinator.stop().await;
    }

    /// Each partition carries its own copy of every marker.
    #[tokio::test]
    #[serial]
    async fn acks_an_epoch_once_across_partitions() {
        const TABLE: &str = "driver_partitions_table";
        let batch = marker_batch(1);
        let source =
            MemTable::try_new(batch.schema(), vec![vec![batch.clone()], vec![batch]]).unwrap();
        let session_manager = SessionManager::new(100, 10, DynamicTableRegistry::new(), 2).unwrap();
        let ctx = session_manager.session_context();
        ctx.register_table("src", Arc::new(source)).unwrap();
        let plan = ctx.table("src").await.unwrap().into_unoptimized_plan();
        let (acks, subscriber_id) = subscribe_with_id(CHECKPOINT_COORDINATOR_CHANNEL);

        run(
            session_manager,
            plan,
            TABLE.to_string(),
            "app::driver_partitions_table".to_string(),
            CheckpointCoordinator::new().control(),
            ComponentScope::detached("test"),
        )
        .await
        .unwrap();
        unsubscribe(CHECKPOINT_COORDINATOR_CHANNEL, subscriber_id);

        assert_eq!(acked(acks.try_iter(), TABLE), [1]);
    }

    #[tokio::test]
    #[serial]
    async fn an_ended_partition_releases_the_epochs_it_never_reached() {
        const TABLE: &str = "driver_ended_partition_table";
        let (acks, subscriber_id) = subscribe_with_id(CHECKPOINT_COORDINATOR_CHANNEL);

        drive(
            vec![stream(vec![Ok(marker_batch(1))]), stream(vec![])],
            TABLE,
            "app::driver_ended_partition_table",
            &CheckpointCoordinator::new().control(),
            &ComponentScope::detached("test"),
        )
        .await
        .unwrap();
        unsubscribe(CHECKPOINT_COORDINATOR_CHANNEL, subscriber_id);

        assert_eq!(acked(acks.try_iter(), TABLE), [1]);
    }
}
