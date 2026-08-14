//! An operator to write to multiple sinks in a single logical plan.
//! This allows Streamling to implement scan sharing: multiple sinks can reuse the same source.
//! By default, DataFusion creates a separate sub-plan for each sink, which is not efficient.

pub(crate) mod broadcast_stream;

use crate::operators::broadcast::broadcast_stream::BroadcastStream;
use crate::operators::filter::StreamingFilterExec;
use crate::operators::projection::StreamingProjectionExec;
use crate::operators::rebatch::RebatchConfig;
use crate::operators::unnest::StreamingUnnestExec;
use crate::session::{DEFAULT_CATALOG_NAME, SessionManager, get_streamling_config};
use crate::utils::batch_accumulator::AsyncBatchAccumulator;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchemaRef, Statistics, internal_err};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::unnest::UnnestExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    execute_input_stream,
    execution_plan::{Boundedness, EmissionType},
};

use crate::checkpoints::checkpoint_management::{
    register_sink_streams, send_checkpoint_ack, sink_stream_done,
};
use crate::operators::parallel_sink::ParallelSinkExec;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::debug;

/// Leaf `ExecutionPlan` that yields a pre-existing `SendableRecordBatchStream`.
///
/// Used to pipe broadcast data through per-sink transformation plans so that
/// sink-specific projections (e.g. `_gs_op` → `is_deleted`) are applied before
/// reaching the `DataSink`.
struct StreamSourcePlan {
    schema: SchemaRef,
    stream: std::sync::Mutex<Option<SendableRecordBatchStream>>,
    cache: Arc<PlanProperties>,
}

impl StreamSourcePlan {
    fn new(schema: SchemaRef, stream: SendableRecordBatchStream) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );
        Self {
            schema,
            stream: std::sync::Mutex::new(Some(stream)),
            cache: Arc::new(cache),
        }
    }
}

impl Debug for StreamSourcePlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StreamSourcePlan")
    }
}

impl DisplayAs for StreamSourcePlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "StreamSourcePlan: partitions={}",
            self.properties().output_partitioning().partition_count()
        )
    }
}

impl ExecutionPlan for StreamSourcePlan {
    fn name(&self) -> &'static str {
        "StreamSourcePlan"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.stream.lock().unwrap().take().ok_or_else(|| {
            datafusion::common::DataFusionError::from(crate::streamling_err!(
                "StreamSourcePlan: stream already consumed (execute called more than once)"
            ))
        })
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema)))
    }
}

/// Per-sink entry attached to a `MultiSinkLogicalNode`. Each sink carries its
/// own `RebatchConfig` so that the fan-out planner can inject an independent
/// `RebatchExec` between the broadcast and each sink's `write_all`, avoiding
/// the first-wins asymmetry that would otherwise apply when only the shared
/// upstream plan could be rebatched.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Hash)]
pub struct MultiSinkEntry {
    /// Sink reference name — used both to look up the `TableProvider` from
    /// the session catalog at physical-planning time and as the log identity
    /// for the per-sink `RebatchExec`.
    pub name: String,
    pub rebatch_config: RebatchConfig,
}

#[derive(PartialEq, Eq, PartialOrd, Hash)]
pub struct MultiSinkLogicalNode {
    pub input: LogicalPlan,
    pub sinks: Vec<MultiSinkEntry>,
}

impl MultiSinkLogicalNode {
    pub fn new(input: LogicalPlan, sinks: Vec<MultiSinkEntry>) -> Self {
        Self { input, sinks }
    }
}

impl Debug for MultiSinkLogicalNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for MultiSinkLogicalNode {
    fn name(&self) -> &str {
        "MultiSink"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MultiSink: sinks={}",
            self.sinks
                .iter()
                .map(|s| s.name.as_str())
                .collect::<Vec<&str>>()
                .join(", ")
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        let sinks = self.sinks.clone();

        Ok(Self {
            input: inputs.swap_remove(0),
            sinks,
        })
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }
}

/// Rewrite stock DataFusion operators in a sink's transform chain (the nodes
/// `insert_into` adds between the shared broadcast input and the `DataSink`,
/// e.g. a ClickHouse sink's `ProjectionExec`) into their metadata-preserving
/// `Streaming*` counterparts.
///
/// The main plan tree gets this rewrite from the
/// `Streaming*RewritePhysicalOptimizerRule`s, but the per-sink plans built
/// here are stored in `MultiSinkExec::sinks`, which is not exposed through
/// `children()`, so the physical optimizer never traverses them. Left as-is,
/// a stock `ProjectionExec` rebuilds every batch with its plan-time schema,
/// dropping the checkpoint-marker metadata — the sink then keeps writing but
/// never acks an epoch, and checkpointing stalls forever on "missing sinks".
///
/// Traversal stops at `shared_input`: that subtree belongs to the main plan
/// tree (optimized separately) and is replaced by the broadcast stream at
/// execute time anyway. Not descending into it also keeps the
/// `Arc::ptr_eq(dse.input(), &input_physical)` transform detection intact for
/// sinks whose `insert_into` adds no nodes.
fn rewrite_sink_transform_chain(
    plan: Arc<dyn ExecutionPlan>,
    shared_input: &Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_down(|node| {
        if Arc::ptr_eq(&node, shared_input) {
            return Ok(Transformed::new(node, false, TreeNodeRecursion::Jump));
        }
        if let Some(projection) = node.downcast_ref::<ProjectionExec>() {
            let streaming = StreamingProjectionExec::from_original(projection.clone())?;
            return Ok(Transformed::yes(Arc::new(streaming)));
        }
        if let Some(filter) = node.downcast_ref::<FilterExec>() {
            let streaming = StreamingFilterExec::from_original(filter.clone())?;
            return Ok(Transformed::yes(Arc::new(streaming)));
        }
        if let Some(unnest) = node.downcast_ref::<UnnestExec>() {
            let streaming = StreamingUnnestExec::from_original(unnest.clone())?;
            return Ok(Transformed::yes(Arc::new(streaming)));
        }
        Ok(Transformed::no(node))
    })
    .data()
}

pub struct MultiSinkExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for MultiSinkExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(
            if let Some(multi_sink_node) = node.as_any().downcast_ref::<MultiSinkLogicalNode>() {
                let input_physical = planner
                    .create_physical_plan(&multi_sink_node.input, session_state)
                    .await
                    .unwrap();

                let catalog = session_state
                    .catalog_list()
                    .catalog(DEFAULT_CATALOG_NAME)
                    .unwrap();

                let mut sinks: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
                let mut sink_has_transforms: Vec<bool> = Vec::new();
                let mut sink_names: Vec<String> = Vec::new();
                let mut sink_rebatch_configs: Vec<RebatchConfig> = Vec::new();
                for entry in &multi_sink_node.sinks {
                    let (schema_name, table_name) =
                        SessionManager::extract_schema_and_table_names(entry.name.as_str());
                    let schema = catalog.schema(schema_name).unwrap();
                    let table = schema.table(table_name).await?.unwrap();

                    let sink_plan = table
                        .insert_into(session_state, input_physical.clone(), InsertOp::Append)
                        .await?;
                    // Checkpoint markers must survive the sink's transform
                    // chain; the physical optimizer can't reach it here.
                    let sink_plan = rewrite_sink_transform_chain(sink_plan, &input_physical)?;

                    let has_transforms = sink_plan
                        .downcast_ref::<ParallelSinkExec>()
                        .map(|sink_exec| !Arc::ptr_eq(sink_exec.input(), &input_physical))
                        .unwrap_or(false);
                    sink_has_transforms.push(has_transforms);
                    sinks.push(sink_plan);
                    sink_names.push(entry.name.clone());
                    sink_rebatch_configs.push(entry.rebatch_config.clone());
                }

                let internal_buffer_size =
                    get_streamling_config(session_state)?.internal_buffer_size;

                let exec = Arc::new(MultiSinkExec::new(
                    input_physical,
                    sinks,
                    sink_has_transforms,
                    sink_names,
                    sink_rebatch_configs,
                    internal_buffer_size,
                ));
                Some(exec)
            } else {
                None
            },
        )
    }
}

struct MultiSinkExec {
    input: Arc<dyn ExecutionPlan>,
    sinks: Vec<Arc<dyn ExecutionPlan>>,
    /// Pre-computed at plan time via `Arc::ptr_eq` — before the physical
    /// optimizer runs — because `with_new_children` later replaces `self.input`
    /// while sinks keep stale references, making a runtime ptr_eq unreliable.
    sink_has_transforms: Vec<bool>,
    sink_names: Vec<String>,
    /// Per-sink rebatching applied at `execute` time between the broadcast
    /// consumer and the sink's write path. Parallel to `sink_names`.
    sink_rebatch_configs: Vec<RebatchConfig>,
    cache: Arc<PlanProperties>,
    internal_buffer_size: usize,
}

impl MultiSinkExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        sinks: Vec<Arc<dyn ExecutionPlan>>,
        sink_has_transforms: Vec<bool>,
        sink_names: Vec<String>,
        sink_rebatch_configs: Vec<RebatchConfig>,
        internal_buffer_size: usize,
    ) -> Self {
        let cache = Self::compute_properties(input.schema());
        Self {
            input,
            sinks,
            sink_has_transforms,
            sink_names,
            sink_rebatch_configs,
            cache: Arc::new(cache),
            internal_buffer_size,
        }
    }

    /// Render one sink's identity plus its rebatch status for `DisplayAs` /
    /// `Debug`. Sinks without rebatching show just the name; active
    /// rebatchers include size / interval so the plan dump is self-describing.
    fn format_sink(name: &str, config: &RebatchConfig) -> String {
        if config.is_noop() {
            name.to_string()
        } else {
            let size = config
                .batch_size
                .map(|n| n.to_string())
                .unwrap_or_else(|| "∞".to_string());
            let interval = config
                .batch_flush_interval
                .map(|d| format!("{:?}", d))
                .unwrap_or_else(|| "-".to_string());
            format!(
                "{} [rebatch: batch_size={}, flush={}]",
                name, size, interval
            )
        }
    }

    fn format_sinks(&self) -> String {
        self.sink_names
            .iter()
            .zip(self.sink_rebatch_configs.iter())
            .map(|(name, cfg)| Self::format_sink(name, cfg))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        )
    }
}

impl Debug for MultiSinkExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MultiSinkExec {{ sinks=[{}] }}", self.format_sinks())
    }
}

impl DisplayAs for MultiSinkExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "MultiSinkExec: partitions={}, sinks=[{}]",
                    self.input.output_partitioning().partition_count(),
                    self.format_sinks()
                )
            }
        }
    }
}

impl ExecutionPlan for MultiSinkExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // Partition-transparent: every input partition gets its own broadcast
        // and its own write stream per sink. Requiring `SinglePartition` here
        // would make the optimizer coalesce a parallel source back to one stream
        // before it ever reached the fan-out.
        vec![Distribution::UnspecifiedDistribution]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MultiSinkExec::new(
            children[0].clone(),
            self.sinks.clone(),
            self.sink_has_transforms.clone(),
            self.sink_names.clone(),
            self.sink_rebatch_configs.clone(),
            self.internal_buffer_size,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Like `ParallelSinkExec`, this node reports a single output partition
        // and fans over its input partitions internally: it is always the plan
        // root, so its own width buys nothing, while the fan-out below it is
        // where the parallelism has to live.
        if partition != 0 {
            return internal_err!("MultiSinkExec can only be called on partition 0!");
        }

        let input_partitions = self.input.output_partitioning().partition_count();
        let total_sinks = self.sinks.len();
        // Each sink is written by one stream per input partition. `MultiSinkExec`
        // calls `write_all` directly rather than going through
        // `ParallelSinkExec`, so the ack gate has to be registered here or a
        // sink would ack an epoch on the first partition that flushed it.
        for sink_name in &self.sink_names {
            register_sink_streams(sink_name, input_partitions);
        }

        // Handles for the per-(sink, partition) writer tasks. The output stream
        // below only ends after every one of these is joined: the tasks keep
        // flushing their rebatched tails after the broadcast input ends, and
        // completing the plan while they are still writing lets the pipeline
        // start teardown (or exit) mid-flush — dropping the tail of every sink
        // that didn't win the race (observed as ClickHouse receiving 0 rows in
        // the multi-sink e2e test once shutdown began cancelling in-flight
        // writes).
        let mut sink_handles: Vec<tokio::task::JoinHandle<Result<()>>> =
            Vec::with_capacity(total_sinks * input_partitions);

        let mut output_consumers = Vec::with_capacity(input_partitions);
        for input_partition in 0..input_partitions {
            let data = execute_input_stream(
                Arc::clone(&self.input),
                Arc::clone(&self.schema()),
                input_partition,
                Arc::clone(&context),
            )?;

            let broadcast_stream = Arc::new(BroadcastStream::new(
                self.schema(),
                self.internal_buffer_size,
            ));

            // Scoped to this partition: its broadcast stops once every sink has
            // finished *this* partition's stream, independent of the others.
            let completed_sinks = Arc::new(Mutex::new(0));

            for (i, sink) in self.sinks.iter().enumerate() {
                let sink = sink.clone();
                let has_transforms = self.sink_has_transforms[i];
                let sink_name = self.sink_names.get(i).cloned().unwrap_or_default();
                let rebatch_config = self.sink_rebatch_configs[i].clone();
                let broadcast_consumer: SendableRecordBatchStream =
                    Box::pin(broadcast_stream.add_consumer());
                let task_context = context.clone();
                let broadcast_schema = self.schema();

                let broadcast_stream = broadcast_stream.clone();
                let completed_sinks = completed_sinks.clone();
                // Stagger flushes across every (sink, partition) stream, not just
                // across sinks, so N×M writers don't all fire at the same instant.
                let stagger_index = input_partition * total_sinks + i;
                let stagger_total = total_sinks * input_partitions;

                let handle = tokio::spawn(async move {
                    let data_sink_exec = sink
                        .downcast_ref::<ParallelSinkExec>()
                        .expect("MultiSinkExec: sink must be a ParallelSinkExec");
                    let data_sink = data_sink_exec.sink();

                    // Per-sink rebatching: run the broadcast consumer through an
                    // `AsyncBatchAccumulator` before it reaches the sink. Placing
                    // this upstream of the sink's own transformation chain
                    // mirrors the single-sink case where `RebatchExec` sits above
                    // any projection the sink provider attaches.
                    let sink_input_stream: SendableRecordBatchStream = if rebatch_config.is_noop() {
                        broadcast_consumer
                    } else {
                        let accumulator = AsyncBatchAccumulator::new(
                            rebatch_config
                                .batch_size
                                .map(|s| s as usize)
                                .unwrap_or(usize::MAX),
                            rebatch_config.batch_flush_interval,
                        )
                        .with_name(sink_name.clone())
                        .with_flush_stagger(stagger_index, stagger_total);
                        let rebatched = accumulator.process_stream(broadcast_consumer);
                        Box::pin(RecordBatchStreamAdapter::new(
                            broadcast_schema.clone(),
                            rebatched,
                        ))
                    };

                    let input_stream = if !has_transforms {
                        sink_input_stream
                    } else {
                        // Pipe broadcast data through the sink's transformation plan
                        // (e.g. a ProjectionExec for column mapping). The stream
                        // source is single-partition — this chain handles one
                        // broadcast partition — so it is executed at partition 0.
                        let sink_input = data_sink_exec.input();
                        let stream_source =
                            Arc::new(StreamSourcePlan::new(broadcast_schema, sink_input_stream));
                        let modified_input = Arc::clone(sink_input)
                            .with_new_children(vec![stream_source])
                            .expect("Failed to replace sink input children with broadcast stream");
                        modified_input
                            .execute(0, Arc::clone(&task_context))
                            .expect("Failed to execute sink input plan over broadcast data")
                    };

                    let write_result = data_sink.write_all(input_stream, &task_context).await;

                    // Release this stream's share of the ack gate and count it
                    // done on BOTH paths (success and failure): the stream will
                    // never report another epoch either way, and the broadcast's
                    // all-streams-completed stop must not stall on a failed sink.
                    // The epochs freed by a FAILED write are deliberately not
                    // acked — acking them would let the source commit offsets for
                    // rows that never landed.
                    let freed_epochs = sink_stream_done(&sink_name);
                    if write_result.is_ok() {
                        for epoch in freed_epochs {
                            send_checkpoint_ack(epoch, &sink_name);
                        }
                    }
                    {
                        let mut count = completed_sinks.lock().await;
                        *count += 1;
                        if *count >= total_sinks {
                            broadcast_stream.stop();
                        }
                    }

                    match write_result {
                        Ok(_) => {
                            debug!(
                                "MultiSinkExec [{}]: Sink completed partition {}",
                                sink_name, input_partition
                            );
                            Ok(())
                        }
                        Err(e) => {
                            // Surface the failure as a plan error via the joined
                            // handle below — previously this panicked inside a
                            // detached task whose JoinHandle nobody awaited, so a
                            // failed sink silently dropped out of the fan-out
                            // while the pipeline kept running.
                            tracing::warn!(
                                "MultiSinkExec [{}]: Sink write_all failed: {}",
                                sink_name,
                                e
                            );
                            Err(datafusion::error::DataFusionError::Execution(format!(
                                "MultiSinkExec [{}]: Sink write_all failed: {}",
                                sink_name, e
                            )))
                        }
                    }
                });
                sink_handles.push(handle);
            }

            output_consumers.push(broadcast_stream.add_consumer());

            // Start broadcasting after all consumers (sinks + output) are registered
            broadcast_stream.start(data);
        }

        // Gate the plan's completion on every sink writer finishing: forward
        // the output consumers, then join the writer tasks and surface any
        // failure as a stream error. `collect()` on this plan therefore
        // resolves only once all sinks have durably drained — which is what
        // the run loop's teardown ordering (and the shutdown drain guarantee)
        // relies on. Note this means the plan's wall-clock now includes sink
        // write latency (and any residual retry backoff), by design.
        // ASSUMPTION: downstream drains this stream to completion before
        // dropping it (DataFusion's collect() does). Dropping it early skips
        // the handle-join below and aborts in-flight sink writers mid-flush.
        let schema = self.schema();
        let output = async_stream::stream! {
            let mut out = futures::stream::select_all(output_consumers);
            while let Some(item) = futures::StreamExt::next(&mut out).await {
                yield item;
            }
            // Join ALL writers before yielding any failure: consumers like
            // `collect()` stop pulling on the first error and drop this
            // stream, and dropping it before the remaining handles are joined
            // would abort the sibling writers mid-flush — the exact tail loss
            // this gating exists to prevent. So: drain everyone first, then
            // report the first failure.
            let mut first_error: Option<datafusion::error::DataFusionError> = None;
            for handle in sink_handles {
                let failure = match handle.await {
                    Ok(Ok(())) => None,
                    Ok(Err(e)) => Some(e),
                    Err(join_err) => Some(datafusion::error::DataFusionError::Execution(
                        format!("MultiSinkExec: sink writer task panicked: {}", join_err),
                    )),
                };
                if let Some(e) = failure {
                    tracing::warn!("MultiSinkExec: sink writer failed: {}", e);
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
            if let Some(e) = first_error {
                yield Err(e);
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::catalog::TableProvider;
    use datafusion::datasource::MemTable;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::prelude::SessionContext;

    fn scan_plan() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        let state = ctx.state();
        futures::executor::block_on(mem_table.scan(&state, None, &[], None)).unwrap()
    }

    fn identity_projection(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let col: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(Column::new("id", 0));
        Arc::new(ProjectionExec::try_new(vec![(col, "id".to_string())], input).unwrap())
    }

    /// Collects every row it is handed, across all the concurrent `write_all`
    /// calls `MultiSinkExec` makes (one per input partition).
    #[derive(Debug)]
    struct CollectingSink {
        schema: SchemaRef,
        ids: std::sync::Mutex<Vec<i64>>,
        finished_streams: std::sync::atomic::AtomicUsize,
    }

    impl CollectingSink {
        fn new(schema: SchemaRef) -> Self {
            Self {
                schema,
                ids: std::sync::Mutex::new(Vec::new()),
                finished_streams: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn sorted_ids(&self) -> Vec<i64> {
            let mut ids = self.ids.lock().unwrap().clone();
            ids.sort();
            ids
        }
    }

    #[async_trait]
    impl datafusion::datasource::sink::DataSink for CollectingSink {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }

        async fn write_all(
            &self,
            mut data: SendableRecordBatchStream,
            _context: &Arc<TaskContext>,
        ) -> Result<u64> {
            let mut count = 0;
            while let Some(batch) = futures::StreamExt::next(&mut data).await.transpose()? {
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let mut ids = self.ids.lock().unwrap();
                for i in 0..batch.num_rows() {
                    ids.push(column.value(i));
                    count += 1;
                }
            }
            self.finished_streams
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(count)
        }
    }

    impl DisplayAs for CollectingSink {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "CollectingSink")
        }
    }

    fn two_partition_input() -> (Arc<dyn ExecutionPlan>, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch_for = |ids: Vec<i64>| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids))]).unwrap()
        };
        let input = datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
            &[vec![batch_for(vec![1, 2, 3])], vec![batch_for(vec![4, 5])]],
            schema.clone(),
            None,
        )
        .unwrap();
        (input, schema)
    }

    fn multi_sink_over(
        input: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        names: &[&str],
    ) -> (MultiSinkExec, Vec<Arc<CollectingSink>>) {
        let collectors: Vec<Arc<CollectingSink>> = names
            .iter()
            .map(|_| Arc::new(CollectingSink::new(schema.clone())))
            .collect();
        let sinks: Vec<Arc<dyn ExecutionPlan>> = collectors
            .iter()
            .zip(names)
            .map(|(collector, name)| {
                Arc::new(ParallelSinkExec::new(
                    Arc::clone(&input),
                    collector.clone(),
                    name.to_string(),
                )) as Arc<dyn ExecutionPlan>
            })
            .collect();
        let exec = MultiSinkExec::new(
            input,
            sinks,
            names.iter().map(|_| false).collect(),
            names.iter().map(|n| n.to_string()).collect(),
            names.iter().map(|_| RebatchConfig::default()).collect(),
            10,
        );
        (exec, collectors)
    }

    /// `MultiSinkExec` used to demand `SinglePartition`, so the optimizer
    /// coalesced a parallel source before it ever reached the fan-out. It must
    /// now take the input at its own width.
    #[test]
    fn multi_partition_input_is_not_coalesced_before_the_fan_out() {
        use crate::operators::coalesce::StreamingCoalesceExec;
        use crate::optimizer::EnforceSinglePartitionPhysicalOptimizerRule;
        use datafusion::config::ConfigOptions;
        use datafusion::physical_optimizer::PhysicalOptimizerRule;

        let (input, schema) = two_partition_input();
        let (exec, _) = multi_sink_over(input, schema, &["sink_a", "sink_b"]);

        let optimized = EnforceSinglePartitionPhysicalOptimizerRule::new()
            .optimize(Arc::new(exec), &ConfigOptions::default())
            .unwrap();

        assert!(
            optimized.children()[0]
                .downcast_ref::<StreamingCoalesceExec>()
                .is_none(),
            "the fan-out input must keep its partitions, got {}",
            optimized.children()[0].name()
        );
        assert_eq!(
            optimized.children()[0]
                .output_partitioning()
                .partition_count(),
            2
        );
    }

    /// The property the whole fan-out rests on: every sink sees every row of
    /// every input partition. A partition dropped here is silent data loss.
    #[tokio::test]
    async fn every_sink_receives_every_input_partition() {
        let (input, schema) = two_partition_input();
        let (exec, collectors) =
            multi_sink_over(input, schema, &["fanout_sink_a", "fanout_sink_b"]);

        let ctx = datafusion::prelude::SessionContext::new();
        let mut stream = exec.execute(0, ctx.task_ctx()).unwrap();
        while (futures::StreamExt::next(&mut stream).await).is_some() {}

        // The output stream ends when the broadcasts drain, which can happen a
        // beat before the sink tasks record their last rows.
        for collector in &collectors {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            while collector
                .finished_streams
                .load(std::sync::atomic::Ordering::SeqCst)
                < 2
            {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "sink did not finish both partition streams"
                );
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }

        for (index, collector) in collectors.iter().enumerate() {
            assert_eq!(
                collector.sorted_ids(),
                vec![1, 2, 3, 4, 5],
                "sink {index} must receive both input partitions"
            );
        }
    }

    /// `MultiSinkExec` calls `write_all` directly instead of going through
    /// `ParallelSinkExec`, so it owns the ack-gate registration: without it a
    /// sink would ack an epoch on the first partition that flushed it, while the
    /// other partition still had pre-marker rows in flight.
    #[tokio::test]
    async fn fan_out_registers_an_ack_gate_per_sink() {
        use crate::checkpoints::checkpoint_management::{CheckpointEpoch, report_marker_at_sink};

        let (input, schema) = two_partition_input();
        let (exec, _) = multi_sink_over(input, schema, &["gated_fanout_sink"]);

        let ctx = datafusion::prelude::SessionContext::new();
        let _stream = exec.execute(0, ctx.task_ctx()).unwrap();

        assert!(
            !report_marker_at_sink("gated_fanout_sink", CheckpointEpoch(1)),
            "the first of two partition streams must not ack"
        );
        assert!(
            report_marker_at_sink("gated_fanout_sink", CheckpointEpoch(1)),
            "the second releases the ack"
        );
    }

    /// A `ProjectionExec` added by a sink's `insert_into` must be rewritten to
    /// the metadata-preserving `StreamingProjectionExec`, while the shared
    /// broadcast input below it stays untouched (same `Arc`).
    #[test]
    fn rewrite_replaces_sink_projection_and_keeps_shared_input() {
        let shared = scan_plan();
        let plan = identity_projection(shared.clone());

        let rewritten = rewrite_sink_transform_chain(plan, &shared).unwrap();

        assert!(
            rewritten
                .downcast_ref::<StreamingProjectionExec>()
                .is_some(),
            "expected StreamingProjectionExec at the root, got {}",
            rewritten.name()
        );
        let children = rewritten.children();
        assert_eq!(children.len(), 1);
        assert!(
            Arc::ptr_eq(children[0], &shared),
            "shared input must be forwarded untouched"
        );
    }

    /// A sink plan that adds no nodes (e.g. a webhook sink) must come back as
    /// the very same plan, preserving the `Arc::ptr_eq` transform detection.
    #[test]
    fn rewrite_is_identity_when_sink_adds_no_nodes() {
        let shared = scan_plan();

        let rewritten = rewrite_sink_transform_chain(shared.clone(), &shared).unwrap();

        assert!(
            Arc::ptr_eq(&rewritten, &shared),
            "plan without sink-side transforms must be returned as-is"
        );
    }

    /// Operators inside the shared input subtree belong to the main plan tree
    /// (the physical optimizer handles them there); the rewrite must not
    /// descend into it.
    #[test]
    fn rewrite_does_not_descend_into_shared_input() {
        let shared = identity_projection(scan_plan());
        let plan = identity_projection(shared.clone());

        let rewritten = rewrite_sink_transform_chain(plan, &shared).unwrap();

        assert!(
            rewritten
                .downcast_ref::<StreamingProjectionExec>()
                .is_some(),
            "sink-side projection must still be rewritten"
        );
        let children = rewritten.children();
        assert!(
            Arc::ptr_eq(children[0], &shared),
            "shared input subtree must not be rebuilt"
        );
        assert!(
            children[0].downcast_ref::<ProjectionExec>().is_some(),
            "nodes inside the shared input must keep their original type"
        );
    }
}
