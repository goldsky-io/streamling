//! An operator to write to multiple sinks in a single logical plan.
//! This allows Streamling to implement scan sharing: multiple sinks can reuse the same source.
//! By default, DataFusion creates a separate sub-plan for each sink, which is not efficient.

pub(crate) mod broadcast_stream;

use crate::operators::broadcast::broadcast_stream::BroadcastStream;
use crate::operators::rebatch::RebatchConfig;
use crate::session::{DEFAULT_CATALOG_NAME, SessionManager, get_streamling_config};
use crate::utils::batch_accumulator::AsyncBatchAccumulator;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::common::{DFSchemaRef, Statistics, internal_err};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, execute_input_stream,
    execution_plan::{Boundedness, EmissionType},
};

use datafusion::datasource::sink::DataSinkExec;
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
        write!(f, "StreamSourcePlan")
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

                    let has_transforms = sink_plan
                        .downcast_ref::<DataSinkExec>()
                        .map(|dse| !Arc::ptr_eq(dse.input(), &input_physical))
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
                write!(f, "MultiSinkExec: sinks=[{}]", self.format_sinks())
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
        vec![Distribution::SinglePartition]
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
        // same as DataSinkExec
        if partition != 0 {
            return internal_err!("MultiSinkExec can only be called on partition 0!");
        }

        let data = execute_input_stream(
            Arc::clone(&self.input),
            Arc::clone(&self.schema()),
            0,
            Arc::clone(&context),
        )?;

        let broadcast_stream = Arc::new(BroadcastStream::new(
            self.schema(),
            self.internal_buffer_size,
        ));

        let total_sinks = self.sinks.len();
        let completed_sinks = Arc::new(Mutex::new(0));
        // Handles for the per-sink writer tasks. The output stream below only
        // ends after every one of these is joined: the tasks keep flushing
        // their rebatched tails after the broadcast input ends, and completing
        // the plan while they are still writing lets the pipeline start
        // teardown (or exit) mid-flush — dropping the tail of every sink that
        // didn't win the race (observed as ClickHouse receiving 0 rows in the
        // multi-sink e2e test once shutdown began cancelling in-flight writes).
        let mut sink_handles: Vec<tokio::task::JoinHandle<Result<()>>> =
            Vec::with_capacity(total_sinks);

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

            let handle = tokio::spawn(async move {
                let data_sink_exec = sink
                    .downcast_ref::<DataSinkExec>()
                    .expect("MultiSinkExec: sink must be a DataSinkExec");
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
                    .with_name(sink_name.clone());
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
                    // (e.g. a ProjectionExec for column mapping).
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

                // Count this sink as done on BOTH paths (success and failure)
                // so the broadcast's all-sinks-completed stop can never stall
                // on a failed sink.
                {
                    let mut count = completed_sinks.lock().await;
                    *count += 1;
                    if *count >= total_sinks {
                        broadcast_stream.stop();
                    }
                }

                match write_result {
                    Ok(_) => {
                        debug!("MultiSinkExec [{}]: Sink completed", sink_name);
                        Ok(())
                    }
                    Err(e) => {
                        // Surface the failure as a plan error via the joined
                        // handle below — previously this panicked inside a
                        // detached task whose JoinHandle nobody awaited, so a
                        // failed sink silently dropped out of the fan-out while
                        // the pipeline kept running.
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

        let output_consumer = broadcast_stream.add_consumer();

        // Start broadcasting after all consumers (sinks + output) are registered
        broadcast_stream.start(data);

        // Gate the plan's completion on every sink writer finishing: forward
        // the output consumer, then join the writer tasks and surface any
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
            let mut out = output_consumer;
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
