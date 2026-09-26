//! A marker-aware exchange: N input partitions in, M output partitions out.
//!
//! Stock `RepartitionExec` cannot be used for two reasons: it rebuilds batches
//! with a metadata-less schema (dropping the checkpoint markers that ride on
//! schema metadata), and it has no notion of aligning those markers when several
//! input streams merge into one output.
//!
//! Rows are placed either by key hash or round-robin (see [`Exchange`]). Hash
//! placement is a correctness mechanism, not an optimization: per-key ordering
//! and `WrappingDataSink`'s keep-last primary-key dedup are only correct when all
//! rows of a key share one stream. Round-robin is therefore never inferred — only
//! a caller that knows its sink has no such requirement may ask for it.
//!
//! Markers are broadcast to every output under both modes: each output feeds a
//! write stream, and the sink's ack gate waits for all of them.

use arrow::array::UInt32Array;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::common::hash_utils::create_hashes;
use datafusion::common::{DFSchemaRef, Statistics, internal_datafusion_err, internal_err};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    Distribution, EquivalenceProperties, Partitioning, PhysicalExprRef,
};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    execute_input_stream,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use futures::StreamExt;
use parking_lot::Mutex;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use streamling_common::utils::arrow::safe_take_record_batch;
use tokio::sync::mpsc;
use tracing::debug;

use crate::checkpoints::checkpoint_management::{
    MarkerAligner, extract_checkpoint_messages, strip_checkpoint_messages,
};
use crate::operators::coalesce::StreamingCoalesceExec;
use crate::operators::{marker_only_batch, schema_with_messages};
use crate::session::get_streamling_config;
use crate::streamling_user_err;

/// Does a plan with these `properties` already satisfy
/// `Distribution::HashPartitioned(hash_exprs)` — is every row of a given key
/// already guaranteed to be on one stream?
///
/// True in exactly two cases:
/// - the plan has a single partition, which trivially holds every row of every
///   key; or
/// - it declares `Partitioning::Hash` on equivalent expressions. Equivalence is
///   DataFusion's, so a key that survived a projection or a rename still matches.
pub fn satisfies_hash_distribution(
    properties: &PlanProperties,
    hash_exprs: &[PhysicalExprRef],
) -> bool {
    properties
        .output_partitioning()
        .satisfaction(
            &Distribution::HashPartitioned(hash_exprs.to_vec()),
            properties.equivalence_properties(),
            false,
        )
        .is_satisfied()
}

/// How rows are placed across the exchange's output streams.
///
/// An enum rather than "hash exprs, empty means round-robin": the two modes have
/// different correctness requirements, and a missing primary key on a keyed sink
/// must stay an error rather than silently degrading to round-robin.
#[derive(Debug, Clone)]
pub enum Exchange {
    /// All rows of a key land on one stream. Required whenever the sink's
    /// correctness depends on per-key ordering (upserts, keep-last dedup).
    Hash(Vec<PhysicalExprRef>),
    /// Whole batches are dealt out in turn. Only valid for sinks with no
    /// ordering or dedup requirement, which is why it is never inferred — the
    /// caller has to say so.
    RoundRobin,
}

type BatchResult = Result<RecordBatch>;

/// The receivers feeding one output partition — one per input partition.
type OutputReceivers = Vec<mpsc::Receiver<BatchResult>>;

/// Redistributes its input onto `output_partitions` output streams, by key hash
/// or round-robin (see [`Exchange`]).
///
/// # Execution contract
///
/// The N input readers are shared by all M outputs, so they are started once, by
/// whichever `execute` call comes first. That has two consequences callers must
/// respect:
///
/// - **Every output partition must be executed.** An output that is never
///   consumed leaves its channels full, which blocks the shared readers and
///   stalls the whole exchange. `collect` and `ParallelSinkExec` both execute
///   every partition.
/// - **Each output partition may be executed only once**; a second call is an
///   internal error rather than a silently empty stream.
#[derive(Debug)]
pub struct StreamingRepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    exchange: Exchange,
    channel_buffer_size: usize,
    /// Per-output receiver sets, populated by the first `execute` call and taken
    /// one-shot by each output.
    outputs: Mutex<Option<Vec<Option<OutputReceivers>>>>,
    cache: Arc<PlanProperties>,
}

impl StreamingRepartitionExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        exchange: Exchange,
        output_partitions: usize,
        channel_buffer_size: usize,
    ) -> Self {
        let cache = Self::compute_properties(&input, &exchange, output_partitions);
        Self {
            input,
            exchange,
            channel_buffer_size,
            outputs: Mutex::new(None),
            cache: Arc::new(cache),
        }
    }

    pub fn exchange(&self) -> &Exchange {
        &self.exchange
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        exchange: &Exchange,
        output_partitions: usize,
    ) -> PlanProperties {
        PlanProperties::new(
            // Rows are redistributed across streams, so nothing the input claimed
            // about ordering survives; only the schema does.
            EquivalenceProperties::new(input.schema()),
            // The only operator in streamling allowed to declare `Hash`: it is the
            // one that physically enforces key-to-partition placement.
            match exchange {
                Exchange::Hash(exprs) => Partitioning::Hash(exprs.to_vec(), output_partitions),
                Exchange::RoundRobin => Partitioning::RoundRobinBatch(output_partitions),
            },
            input.pipeline_behavior(),
            input.boundedness(),
        )
    }

    /// Starts the input readers and builds the per-(input, output) channel grid.
    fn start(&self, context: &Arc<TaskContext>) -> Result<()> {
        let input_partitions = self.input.output_partitioning().partition_count();
        let output_partitions = self.properties().output_partitioning().partition_count();
        let schema = self.input.schema();

        // `senders[i][j]` carries input partition i's share of output j. Per-(i,j)
        // FIFO is the entire ordering guarantee: key k from input i always reaches
        // output h(k) in input order, and a marker follows the data it was emitted
        // behind. Bounded channels make a slow output backpressure its readers
        // rather than buffer without limit.
        let mut receivers: Vec<Vec<mpsc::Receiver<BatchResult>>> =
            (0..output_partitions).map(|_| Vec::new()).collect();
        let mut senders: Vec<Vec<mpsc::Sender<BatchResult>>> =
            (0..input_partitions).map(|_| Vec::new()).collect();
        for sender_row in senders.iter_mut() {
            for receiver_column in receivers.iter_mut() {
                let (tx, rx) = mpsc::channel(self.channel_buffer_size.max(1));
                sender_row.push(tx);
                receiver_column.push(rx);
            }
        }

        for (input_partition, txs) in senders.into_iter().enumerate() {
            let stream = execute_input_stream(
                Arc::clone(&self.input),
                Arc::clone(&schema),
                input_partition,
                Arc::clone(context),
            )?;
            let exchange = self.exchange.clone();
            let schema = Arc::clone(&schema);
            // Sanctioned: the router lives exactly as long as its input
            // stream — shutdown forces source streams to end, the router
            // drains what it read, drops its senders, and downstream sees
            // end-of-stream; a dropped receiver ends it from the other side.
            // Threading a ComponentScope through RepartitionExec is tracked
            // follow-up alongside the drain-under-parallelism validation.
            #[allow(clippy::disallowed_methods)]
            tokio::spawn(route_input_partition(
                stream,
                txs,
                exchange,
                schema,
                input_partition,
            ));
        }

        *self.outputs.lock() = Some(receivers.into_iter().map(Some).collect());
        Ok(())
    }
}

/// Reads one input partition and fans its batches out to the M output channels.
///
/// Dropping `txs` on return closes the receivers, which is how each output learns
/// this input is done — no sentinel message is needed.
async fn route_input_partition(
    mut stream: SendableRecordBatchStream,
    txs: Vec<mpsc::Sender<BatchResult>>,
    exchange: Exchange,
    schema: SchemaRef,
    input_partition: usize,
) {
    let output_partitions = txs.len();
    let mut hashes: Vec<u64> = Vec::new();
    let mut indices: Vec<Vec<u32>> = Vec::new();
    let mut next_output = 0;

    while let Some(batch) = stream.next().await {
        let batch = match batch {
            Ok(batch) => batch,
            Err(e) => {
                debug!(
                    "StreamingRepartitionExec [input {}]: error from input stream, stream will terminate: {}",
                    input_partition, e
                );
                broadcast_error(&txs, e).await;
                return;
            }
        };

        let messages = extract_checkpoint_messages(batch.schema().metadata());

        // A zero-row batch carries no keys to hash — it exists only to carry
        // markers (or a heartbeat), so every output gets a copy.
        if batch.num_rows() == 0 {
            for tx in &txs {
                if tx.send(Ok(batch.clone())).await.is_err() {
                    return;
                }
            }
            continue;
        }

        // Rows go to one output (round-robin) or are split across all of them
        // (hash); markers are re-attached to *every* output below either way,
        // because each output feeds a write stream that has to see the epoch.
        let split = match &exchange {
            Exchange::Hash(exprs) => {
                match split_batch_by_hash(
                    &batch,
                    exprs,
                    output_partitions,
                    &mut hashes,
                    &mut indices,
                ) {
                    Ok(split) => split,
                    Err(e) => {
                        broadcast_error(&txs, e).await;
                        return;
                    }
                }
            }
            Exchange::RoundRobin => {
                let chosen = next_output;
                next_output = (next_output + 1) % output_partitions;
                (0..output_partitions)
                    .map(|i| (i == chosen).then(|| batch.clone()))
                    .collect()
            }
        };

        for (tx, sub_batch) in txs.iter().zip(split) {
            // Markers are re-attached explicitly rather than inherited from the
            // take: `safe_take_record_batch`'s overflow fallback rebuilds the
            // schema and can drop metadata. Every output receives exactly one
            // copy of the epoch, which the per-output aligner then collapses.
            let out = match (sub_batch, messages.is_empty()) {
                (Some(b), true) => b,
                (None, true) => continue,
                (Some(b), false) => {
                    let schema = schema_with_messages(&b.schema(), &messages);
                    RecordBatch::try_new(schema, b.columns().to_vec()).unwrap_or(b)
                }
                (None, false) => marker_only_batch(&schema, &messages),
            };
            if tx.send(Ok(out)).await.is_err() {
                return;
            }
        }
    }
}

/// Every output must learn the exchange failed, not just the one a failing row
/// would have hashed to.
async fn broadcast_error(txs: &[mpsc::Sender<BatchResult>], error: DataFusionError) {
    let message = error.to_string();
    for tx in txs {
        let _ = tx
            .send(Err(DataFusionError::Execution(message.clone())))
            .await;
    }
}

/// Splits `batch` into one sub-batch per output partition, `None` where an output
/// received no rows.
///
/// `hashes` and `indices` are scratch buffers owned by the caller, reused across
/// batches so a per-batch shuffle costs no allocations of its own.
fn split_batch_by_hash(
    batch: &RecordBatch,
    hash_exprs: &[PhysicalExprRef],
    output_partitions: usize,
    hashes: &mut Vec<u64>,
    indices: &mut Vec<Vec<u32>>,
) -> Result<Vec<Option<RecordBatch>>> {
    let arrays = hash_exprs
        .iter()
        .map(|expr| expr.evaluate(batch)?.into_array(batch.num_rows()))
        .collect::<Result<Vec<_>>>()?;

    hashes.clear();
    hashes.resize(batch.num_rows(), 0);
    // The same fixed seed DataFusion's own `RepartitionExec` uses. All hash
    // exchanges in a process must agree, or `Partitioning::satisfy` could elide
    // an exchange whose placement does not actually match.
    create_hashes(&arrays, REPARTITION_RANDOM_STATE.random_state(), hashes)?;

    indices.resize_with(output_partitions, Vec::new);
    for rows in indices.iter_mut() {
        rows.clear();
    }
    for (row, hash) in hashes.iter().enumerate() {
        indices[(*hash % output_partitions as u64) as usize].push(row as u32);
    }

    // One gather for the whole batch rather than one per output: the rows are
    // permuted so each output's share is contiguous, and the outputs then take
    // zero-copy slices of it. Per-output takes move the same bytes but pay a
    // kernel dispatch, an allocation and an overflow guard per column *per
    // output*, which dominates at the small batch sizes this operator sees (it
    // sits below the rebatcher).
    //
    // The permutation is a bijection of `0..num_rows`, so the gathered batch
    // holds exactly the input's column payloads: it cannot overflow i32 offsets
    // unless the input already had.
    let permutation = UInt32Array::from_iter_values(indices.iter().flatten().copied());
    let grouped = safe_take_record_batch(batch, &permutation)?;

    let mut offset = 0;
    Ok(indices
        .iter()
        .map(|rows| {
            let slice = (!rows.is_empty()).then(|| grouped.slice(offset, rows.len()));
            offset += rows.len();
            slice
        })
        .collect())
}

impl DisplayAs for StreamingRepartitionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "StreamingRepartitionExec: partitions={}, ",
                    self.properties().output_partitioning().partition_count()
                )?;
                match &self.exchange {
                    Exchange::Hash(exprs) => write!(
                        f,
                        "keys=[{}]",
                        exprs
                            .iter()
                            .map(|expr| expr.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    Exchange::RoundRobin => write!(f, "round_robin"),
                }
            }
        }
    }
}

impl ExecutionPlan for StreamingRepartitionExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StreamingRepartitionExec::new(
            Arc::clone(&children[0]),
            self.exchange.clone(),
            self.properties().output_partitioning().partition_count(),
            self.channel_buffer_size,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let output_partitions = self.properties().output_partitioning().partition_count();
        if partition >= output_partitions {
            return internal_err!(
                "StreamingRepartitionExec has {output_partitions} output partitions, got request for partition {partition}"
            );
        }

        let started = self.outputs.lock().is_some();
        if !started {
            self.start(&context)?;
        }

        let receivers = {
            let mut outputs = self.outputs.lock();
            let slots = outputs
                .as_mut()
                .ok_or_else(|| internal_datafusion_err!("repartition outputs were not started"))?;
            slots[partition].take().ok_or_else(|| {
                internal_datafusion_err!(
                    "StreamingRepartitionExec output partition {partition} was already executed"
                )
            })?
        };

        let output_schema = self.schema();
        let mut builder =
            RecordBatchReceiverStreamBuilder::new(output_schema.clone(), self.channel_buffer_size);

        // One aligner per output: this output is a merge point for N input
        // readers, so it must not forward an epoch until all of them delivered it.
        let aligner = Arc::new(Mutex::new(MarkerAligner::new(receivers.len())));
        for mut receiver in receivers {
            let tx = builder.tx();
            let aligner = Arc::clone(&aligner);
            let output_schema = Arc::clone(&output_schema);
            builder.spawn(async move {
                while let Some(batch) = receiver.recv().await {
                    let batch = match batch {
                        Ok(batch) => batch,
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                            return Ok(());
                        }
                    };
                    let messages = extract_checkpoint_messages(batch.schema().metadata());
                    if messages.is_empty() {
                        if tx.send(Ok(batch)).await.is_err() {
                            return Ok(());
                        }
                        continue;
                    }
                    if batch.num_rows() > 0
                        && tx
                            .send(Ok(strip_checkpoint_messages(&batch)))
                            .await
                            .is_err()
                    {
                        return Ok(());
                    }
                    let released = aligner.lock().observe(messages);
                    if !released.is_empty()
                        && tx
                            .send(Ok(marker_only_batch(&output_schema, &released)))
                            .await
                            .is_err()
                    {
                        return Ok(());
                    }
                }
                // The sender was dropped: this input reader is finished.
                let released = aligner.lock().input_done();
                if !released.is_empty() {
                    let _ = tx
                        .send(Ok(marker_only_batch(&output_schema, &released)))
                        .await;
                }
                Ok(())
            });
        }

        Ok(builder.build())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.input.metrics()
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema())))
    }
}

// ============================================================================
// Logical plan: RepartitionNode
// ============================================================================

/// Requests that its input be hash-partitioned by `key_columns`.
///
/// Planned into a [`StreamingRepartitionExec`], or elided when the input already
/// places rows that way.
/// The logical counterpart of [`Exchange`], before column names are resolved
/// against a physical schema.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Debug)]
pub enum Placement {
    /// All rows of these columns' key land on one stream. An empty column list
    /// is an error at planning time.
    ByKey(Vec<String>),
    /// Any stream will do. Only for sinks with no ordering or dedup requirement.
    RoundRobin,
    /// The sink cannot write from more than one stream. Always lowers to a
    /// coalesce, ignoring any declared parallelism.
    Single,
}

#[derive(PartialEq, Eq, PartialOrd, Hash, Clone)]
pub struct RepartitionNode {
    pub input: LogicalPlan,
    pub placement: Placement,
    /// Desired output width. `None` keeps the input's width, which makes the
    /// node a pure placement request.
    pub target_parallelism: Option<usize>,
    pub name: String,
}

impl RepartitionNode {
    pub fn new(
        input: LogicalPlan,
        placement: Placement,
        target_parallelism: Option<usize>,
        name: String,
    ) -> Self {
        Self {
            input,
            placement,
            target_parallelism,
            name,
        }
    }
}

impl Debug for RepartitionNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for RepartitionNode {
    fn name(&self) -> &str {
        "RepartitionNode"
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
        match &self.placement {
            Placement::ByKey(columns) => write!(f, "Repartition keys=[{}]", columns.join(", "))?,
            Placement::RoundRobin => write!(f, "Repartition round_robin")?,
            Placement::Single => write!(f, "Repartition single")?,
        }
        if let Some(target) = self.target_parallelism {
            write!(f, " parallelism={target}")?;
        }
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        Ok(Self {
            input: inputs.swap_remove(0),
            placement: self.placement.clone(),
            target_parallelism: self.target_parallelism,
            name: self.name.clone(),
        })
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }
}

pub struct RepartitionExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for RepartitionExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(repartition_node) = node.as_any().downcast_ref::<RepartitionNode>() else {
            return Ok(None);
        };
        let input = planner
            .create_physical_plan(&repartition_node.input, session_state)
            .await?;
        let buffer_size = get_streamling_config(session_state)?.internal_buffer_size;
        Ok(Some(plan_repartition(
            input,
            &repartition_node.placement,
            repartition_node.target_parallelism,
            &repartition_node.name,
            buffer_size,
        )?))
    }
}

/// Lowers a repartition request onto `input`, eliding it when the input already
/// satisfies the placement.
fn plan_repartition(
    input: Arc<dyn ExecutionPlan>,
    placement: &Placement,
    target_parallelism: Option<usize>,
    name: &str,
    buffer_size: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_partitions = input.output_partitioning().partition_count();
    let target = match placement {
        Placement::Single => 1,
        _ => target_parallelism.unwrap_or(input_partitions).max(1),
    };

    // A single output stream needs no exchange at all: one stream trivially
    // holds every key, in order. Reuse the coalesce, which also works keyless.
    if target == 1 {
        return Ok(if input_partitions > 1 {
            Arc::new(StreamingCoalesceExec::new(input, buffer_size))
        } else {
            input
        });
    }

    let exchange = match placement {
        Placement::Single => {
            return internal_err!("'{name}': a single-stream placement cannot reach an exchange");
        }
        Placement::RoundRobin => {
            // Any placement satisfies a sink with no ordering requirement, so an
            // input that is already the right width needs no exchange.
            if input_partitions == target {
                return Ok(input);
            }
            Exchange::RoundRobin
        }
        Placement::ByKey(columns) if columns.is_empty() => {
            return Err(streamling_user_err!(
                "'{name}': parallelism {target} requires a primary key to partition by, \
                 but none is configured or inherited from upstream"
            )
            .into());
        }
        Placement::ByKey(columns) => {
            let schema = input.schema();
            let hash_exprs = columns
                .iter()
                .map(|column| {
                    let index = schema.index_of(column).map_err(|_| {
                        streamling_user_err!(
                            "'{name}': cannot partition by primary key column '{column}', \
                             which is not in the input schema"
                        )
                    })?;
                    Ok(Arc::new(Column::new(column, index)) as PhysicalExprRef)
                })
                .collect::<Result<Vec<_>>>()?;

            if input_partitions == target
                && satisfies_hash_distribution(input.properties(), &hash_exprs)
            {
                return Ok(input);
            }
            Exchange::Hash(hash_exprs)
        }
    };

    Ok(Arc::new(StreamingRepartitionExec::new(
        input,
        exchange,
        target,
        buffer_size,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoints::checkpoint_management::{CheckpointEpoch, CheckpointMessage};
    use crate::operators::coalesce::test_util::MarkerSourceExec;
    use arrow::array::Int32Array;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::prelude::SessionContext;

    fn marker(epoch: u64) -> CheckpointMessage {
        CheckpointMessage::Marker {
            epoch: CheckpointEpoch(epoch),
            created_at_ms: 0,
        }
    }

    fn id_key() -> Vec<PhysicalExprRef> {
        vec![Arc::new(Column::new("id", 0))]
    }

    fn id_hash() -> Exchange {
        Exchange::Hash(id_key())
    }

    fn by_id() -> Placement {
        Placement::ByKey(vec!["id".to_string()])
    }

    /// Drains every output partition concurrently — the operator's contract
    /// requires it, since the input readers are shared.
    async fn drain_all(exec: &StreamingRepartitionExec, outputs: usize) -> Vec<(Vec<i32>, usize)> {
        let ctx = SessionContext::new();
        let streams: Vec<_> = (0..outputs)
            .map(|p| exec.execute(p, ctx.task_ctx()).unwrap())
            .collect();
        let mut results = Vec::new();
        for mut stream in streams {
            let (mut ids, mut markers) = (Vec::new(), 0);
            while let Some(batch) = stream.next().await {
                let batch = batch.unwrap();
                markers += extract_checkpoint_messages(batch.schema().metadata()).len();
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                ids.extend((0..batch.num_rows()).map(|i| column.value(i)));
            }
            results.push((ids, markers));
        }
        results
    }

    /// The exchange must disappear when the input already places rows that way,
    /// or every keyed sink would re-shuffle input a transform already shuffled.
    #[test]
    fn plan_elides_when_the_input_already_places_rows_by_the_key() {
        let source = Arc::new(MarkerSourceExec::new(vec![vec![], vec![]]));
        let exchange: Arc<dyn ExecutionPlan> =
            Arc::new(StreamingRepartitionExec::new(source, id_hash(), 2, 10));

        let planned =
            plan_repartition(Arc::clone(&exchange), &by_id(), Some(2), "sink", 10).unwrap();
        assert!(
            Arc::ptr_eq(&planned, &exchange),
            "a second exchange on the same key and width must be elided"
        );
    }

    #[test]
    fn plan_inserts_an_exchange_when_the_width_changes() {
        let source = Arc::new(MarkerSourceExec::new(vec![vec![], vec![]]));
        let planned = plan_repartition(source, &by_id(), Some(4), "sink", 10).unwrap();
        assert_eq!(planned.output_partitioning().partition_count(), 4);
        assert!(planned.downcast_ref::<StreamingRepartitionExec>().is_some());
    }

    /// One write stream holds every key by construction, so no hashing is needed
    /// — and this path must work for keyless sinks too.
    #[test]
    fn plan_lowers_a_single_stream_target_to_a_coalesce() {
        let source = Arc::new(MarkerSourceExec::new(vec![vec![], vec![]]));
        let planned =
            plan_repartition(source, &Placement::RoundRobin, Some(1), "sink", 10).unwrap();
        assert_eq!(planned.output_partitioning().partition_count(), 1);
        assert!(planned.downcast_ref::<StreamingCoalesceExec>().is_some());
    }

    #[test]
    fn plan_is_a_noop_for_a_single_partition_input() {
        let source: Arc<dyn ExecutionPlan> = Arc::new(MarkerSourceExec::new(vec![vec![]]));
        let planned = plan_repartition(Arc::clone(&source), &by_id(), None, "sink", 10).unwrap();
        assert!(
            Arc::ptr_eq(&planned, &source),
            "a single-partition input already satisfies any key placement"
        );
    }

    /// A sink that cannot write from more than one stream (webhook, plugin)
    /// collapses whatever width its input has, and is left alone when its input
    /// is already single-stream.
    #[test]
    fn single_coalesces_a_multi_partition_input() {
        let wide = Arc::new(MarkerSourceExec::new(vec![vec![], vec![]]));
        let planned = plan_repartition(wide, &Placement::Single, None, "sink", 10).unwrap();
        assert_eq!(planned.output_partitioning().partition_count(), 1);
        assert!(planned.downcast_ref::<StreamingCoalesceExec>().is_some());

        let narrow: Arc<dyn ExecutionPlan> = Arc::new(MarkerSourceExec::new(vec![vec![]]));
        let planned =
            plan_repartition(Arc::clone(&narrow), &Placement::Single, None, "sink", 10).unwrap();
        assert!(
            Arc::ptr_eq(&planned, &narrow),
            "there is nothing to coalesce when the input is already one stream"
        );
    }

    /// `Single` is the sink saying it *cannot* be parallelized, so it outranks a
    /// requested width rather than negotiating with it.
    #[test]
    fn single_ignores_a_declared_parallelism() {
        let source = Arc::new(MarkerSourceExec::new(vec![vec![], vec![]]));
        let planned = plan_repartition(source, &Placement::Single, Some(4), "sink", 10).unwrap();

        assert_eq!(planned.output_partitioning().partition_count(), 1);
        assert!(planned.downcast_ref::<StreamingCoalesceExec>().is_some());
    }

    /// Round-robin is for sinks that neither dedupe nor depend on ordering
    /// (print, blackhole): batches are dealt out in turn, so a keyless sink can
    /// still be widened without inventing a primary key.
    #[tokio::test]
    async fn round_robin_deals_batches_across_the_outputs() {
        let source = Arc::new(MarkerSourceExec::new(vec![vec![
            MarkerSourceExec::batch(vec![1], &[]),
            MarkerSourceExec::batch(vec![2], &[]),
            MarkerSourceExec::batch(vec![3], &[]),
            MarkerSourceExec::batch(vec![4], &[]),
        ]]));
        let exec = StreamingRepartitionExec::new(source, Exchange::RoundRobin, 2, 10);

        let outputs = drain_all(&exec, 2).await;

        assert_eq!(
            outputs[0].0,
            vec![1, 3],
            "batches alternate between outputs"
        );
        assert_eq!(outputs[1].0, vec![2, 4]);
    }

    /// Rows go to one output, but a marker has to reach *every* output: each one
    /// feeds a write stream, and the sink's ack gate waits for all of them.
    #[tokio::test]
    async fn round_robin_still_broadcasts_markers_to_every_output() {
        let source = Arc::new(MarkerSourceExec::new(vec![vec![MarkerSourceExec::batch(
            vec![1, 2],
            &[marker(1)],
        )]]));
        let exec = StreamingRepartitionExec::new(source, Exchange::RoundRobin, 3, 10);

        let outputs = drain_all(&exec, 3).await;

        for (index, (_, markers)) in outputs.iter().enumerate() {
            assert_eq!(
                *markers, 1,
                "output {index} must see epoch 1 even though it got no rows"
            );
        }
        let rows: usize = outputs.iter().map(|(ids, _)| ids.len()).sum();
        assert_eq!(rows, 2, "the rows themselves go to exactly one output");
    }

    /// An input that is already the right width needs no exchange at all when
    /// any placement will do.
    #[test]
    fn round_robin_is_elided_at_the_requested_width() {
        let source: Arc<dyn ExecutionPlan> = Arc::new(MarkerSourceExec::new(vec![vec![], vec![]]));
        let planned = plan_repartition(
            Arc::clone(&source),
            &Placement::RoundRobin,
            Some(2),
            "sink",
            10,
        )
        .unwrap();

        assert!(
            Arc::ptr_eq(&planned, &source),
            "a keyless sink at its input's width needs no exchange"
        );
    }

    /// Widening a keyless sink must not require a primary key.
    #[test]
    fn round_robin_widens_without_a_key() {
        let source = Arc::new(MarkerSourceExec::new(vec![vec![]]));
        let planned =
            plan_repartition(source, &Placement::RoundRobin, Some(4), "sink", 10).unwrap();

        assert_eq!(planned.output_partitioning().partition_count(), 4);
        assert!(matches!(
            planned
                .downcast_ref::<StreamingRepartitionExec>()
                .map(StreamingRepartitionExec::exchange),
            Some(Exchange::RoundRobin)
        ));
    }

    /// A sink that *does* depend on per-key ordering must not silently degrade
    /// to round-robin when its primary key is missing.
    #[test]
    fn plan_rejects_a_keyless_widening() {
        let source = Arc::new(MarkerSourceExec::new(vec![vec![]]));
        let error = plan_repartition(source, &Placement::ByKey(Vec::new()), Some(4), "sink", 10)
            .expect_err("parallelism > 1 without a primary key must fail planning");
        assert!(
            error.to_string().contains("primary key"),
            "unexpected: {error}"
        );
    }

    #[test]
    fn plan_rejects_a_key_column_that_is_not_in_the_schema() {
        let source = Arc::new(MarkerSourceExec::new(vec![vec![], vec![]]));
        let error = plan_repartition(
            source,
            &Placement::ByKey(vec!["missing".to_string()]),
            Some(2),
            "sink",
            10,
        )
        .expect_err("an unresolvable primary key column must fail planning");
        assert!(error.to_string().contains("missing"), "unexpected: {error}");
    }

    /// The correctness property everything else rests on: all rows of a key land
    /// on one output, and no row is lost or duplicated.
    #[tokio::test]
    async fn every_row_of_a_key_lands_on_one_output() {
        let source = Arc::new(MarkerSourceExec::new(vec![
            vec![MarkerSourceExec::batch(vec![1, 2, 3, 4, 5], &[])],
            vec![MarkerSourceExec::batch(vec![1, 2, 3, 4, 5], &[])],
        ]));
        let exec = StreamingRepartitionExec::new(source, id_hash(), 3, 10);

        let outputs = drain_all(&exec, 3).await;

        let mut all: Vec<i32> = outputs.iter().flat_map(|(ids, _)| ids.clone()).collect();
        all.sort();
        assert_eq!(
            all,
            vec![1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
            "every input row must appear exactly once across the outputs"
        );

        for key in 1..=5 {
            let holders = outputs.iter().filter(|(ids, _)| ids.contains(&key)).count();
            assert_eq!(holders, 1, "key {key} must live on exactly one output");
        }
    }

    /// Rows reach an output as a slice of one permuted batch, so a mistake in
    /// the permutation's offsets would reorder them. Per-key ordering is what
    /// `WrappingDataSink`'s keep-last dedup and CDC upserts rest on.
    #[tokio::test]
    async fn hash_keeps_rows_in_input_order_within_an_output() {
        let source = Arc::new(MarkerSourceExec::new(vec![vec![MarkerSourceExec::batch(
            (1..=64).collect(),
            &[],
        )]]));
        let exec = StreamingRepartitionExec::new(source, id_hash(), 4, 10);

        let outputs = drain_all(&exec, 4).await;

        for (index, (ids, _)) in outputs.iter().enumerate() {
            assert!(
                ids.windows(2).all(|pair| pair[0] < pair[1]),
                "output {index} reordered its rows: {ids:?}"
            );
        }
        assert_eq!(
            outputs.iter().map(|(ids, _)| ids.len()).sum::<usize>(),
            64,
            "every row must still be accounted for"
        );
    }

    /// N inputs × M outputs: each output is a merge point, so each must emit the
    /// epoch exactly once — not once per input.
    #[tokio::test]
    async fn each_output_emits_one_marker_per_epoch() {
        let source = Arc::new(MarkerSourceExec::new(vec![
            vec![MarkerSourceExec::batch(vec![1, 2, 3], &[marker(1)])],
            vec![MarkerSourceExec::batch(vec![4, 5, 6], &[marker(1)])],
        ]));
        let exec = StreamingRepartitionExec::new(source, id_hash(), 2, 10);

        for (index, (_, markers)) in drain_all(&exec, 2).await.into_iter().enumerate() {
            assert_eq!(
                markers, 1,
                "output {index} must forward exactly one copy of epoch 1"
            );
        }
    }

    /// A sink's keyed distribution requirement must be satisfied by this
    /// operator, so the planner can elide a second exchange above it.
    #[test]
    fn output_satisfies_the_hash_distribution_it_enforces() {
        let source = Arc::new(MarkerSourceExec::new(vec![vec![]]));
        let exec = StreamingRepartitionExec::new(source, id_hash(), 4, 10);
        assert!(satisfies_hash_distribution(exec.properties(), &id_key()));
        assert_eq!(exec.properties().output_partitioning().partition_count(), 4);
    }

    /// The readers are shared and taken one-shot; a second execute would
    /// otherwise return an empty stream and silently lose that partition's rows.
    #[tokio::test]
    async fn executing_an_output_twice_is_an_error() {
        let source = Arc::new(MarkerSourceExec::new(vec![vec![MarkerSourceExec::batch(
            vec![1],
            &[],
        )]]));
        let exec = StreamingRepartitionExec::new(source, id_hash(), 1, 10);
        let ctx = SessionContext::new();
        exec.execute(0, ctx.task_ctx()).unwrap();
        assert!(exec.execute(0, ctx.task_ctx()).is_err());
    }
}
