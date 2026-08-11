//! Scan sharing allows multiple sub-pipelines to reuse the same source scan.
//! This is useful for many sources. E.g., in case Kafka sources where we want to read from a topic
//! only once even if multiple downstream transforms/sinks consume from it.

use crate::operators::broadcast::broadcast_stream::{BroadcastConsumer, BroadcastStream};
use arrow_schema::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::StreamExt;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tracing::debug;

/// Shared expected consumer counts (accessible by both sources and transforms)
static EXPECTED_CONSUMERS: Lazy<Mutex<HashMap<String, usize>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Registry that tracks shared sources and coordinates scan sharing across multiple consumers
#[derive(Clone, Debug)]
pub struct SharedSourceRegistry {
    pub(crate) sources: Arc<RwLock<HashMap<String, Arc<SharedSourceHandle>>>>,
}

impl SharedSourceRegistry {
    pub fn new() -> Self {
        Self {
            sources: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Pre-register expected consumer count during topology analysis (before scans).
    pub fn set_expected_consumers(&self, name: String, count: usize) {
        let mut expected = EXPECTED_CONSUMERS.lock().unwrap();
        expected.insert(name.clone(), count);
        debug!(
            "Pre-registered expected consumers for '{}': {}",
            name, count
        );
    }

    /// Get pre-registered expected consumer count (shared via static storage).
    pub(crate) fn get_expected_consumers(name: &str) -> Option<usize> {
        let expected = EXPECTED_CONSUMERS.lock().unwrap();
        let count = expected.get(name).copied();
        debug!("Getting expected consumers for '{}': {:?}", name, count);
        count
    }
}

impl Default for SharedSourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Broadcast state for one partition of the shared base plan.
#[derive(Default)]
struct PartitionBroadcast {
    stream: Option<Arc<BroadcastStream>>,
    started: bool,
    registered_consumers: usize,
}

/// Handle for a shared source that manages broadcasting to multiple consumers.
///
/// The base plan keeps its own width: partition j of the shared source is
/// broadcast to partition j of every consumer, so a parallel source stays
/// parallel through scan sharing instead of collapsing to one stream.
pub struct SharedSourceHandle {
    schema: SchemaRef,
    base_exec: Arc<dyn ExecutionPlan>,
    /// One independent broadcast per base-plan partition.
    partitions: Mutex<Vec<PartitionBroadcast>>,
    channel_capacity: usize,
    /// Number of consumer plans. Each executes every partition exactly once, so
    /// a partition's broadcast starts once this many consumers registered *for
    /// that partition*.
    expected_consumers: AtomicUsize,
}

impl Debug for SharedSourceHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedSourceHandle")
            .field("schema", &self.schema)
            .field("channel_capacity", &self.channel_capacity)
            .field("expected", &self.expected_consumers.load(Ordering::SeqCst))
            .field("partitions", &self.partition_count())
            .finish()
    }
}

impl SharedSourceHandle {
    pub fn new(
        schema: SchemaRef,
        base_exec: Arc<dyn ExecutionPlan>,
        channel_capacity: usize,
        expected_consumers: usize,
    ) -> Self {
        let base_partitions = base_exec.output_partitioning().partition_count().max(1);
        Self {
            schema,
            base_exec,
            partitions: Mutex::new(
                (0..base_partitions)
                    .map(|_| PartitionBroadcast::default())
                    .collect(),
            ),
            channel_capacity,
            expected_consumers: AtomicUsize::new(expected_consumers),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn base_exec(&self) -> Arc<dyn ExecutionPlan> {
        self.base_exec.clone()
    }

    pub fn partition_count(&self) -> usize {
        self.partitions.lock().unwrap().len()
    }

    #[cfg(test)]
    fn partition_started(&self, partition: usize) -> bool {
        self.partitions.lock().unwrap()[partition].started
    }

    /// Register a consumer on `partition` and hand back its receiving end.
    ///
    /// The partition's broadcast starts as soon as every expected consumer has
    /// registered on it — batches sent before a consumer registers would be lost,
    /// so the base plan must not be polled until the last one arrives.
    pub fn add_consumer(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<BroadcastConsumer> {
        let mut partitions = self.partitions.lock().unwrap();
        let partition_count = partitions.len();
        let state = partitions.get_mut(partition).ok_or_else(|| {
            datafusion::common::DataFusionError::from(crate::streamling_err!(
                "shared source has {partition_count} partition(s), got request for partition {partition}"
            ))
        })?;

        let broadcast = state
            .stream
            .get_or_insert_with(|| {
                Arc::new(BroadcastStream::new(
                    self.schema.clone(),
                    self.channel_capacity,
                ))
            })
            .clone();
        let consumer = broadcast.add_consumer();

        state.registered_consumers += 1;
        let expected = self.expected_consumers.load(Ordering::SeqCst);
        if !state.started && state.registered_consumers >= expected {
            state.started = true;
            debug!(
                "All {} consumers registered on shared-source partition {}, starting broadcast",
                expected, partition
            );
            let source_stream = self.base_exec.execute(partition, context)?;
            broadcast.start(source_stream);
        } else {
            debug!(
                "Shared-source partition {}: {}/{} consumers registered",
                partition, state.registered_consumers, expected
            );
        }

        Ok(consumer)
    }
}

/// Execution plan that broadcasts data from a source to multiple consumers.
///
/// A shared source broadcasts full rows so that consumers with different column
/// projections can share one scan. Each consumer applies its own `projection`
/// (column indices into the shared source's full schema) to the broadcast batches,
/// keeping the physical schema aligned with the logical plan — which DataFusion 54
/// strictly enforces for extension nodes. Projecting inside this operator (rather
/// than wrapping it in a separate projection node) preserves the broadcast
/// consumer-registration timing and the per-batch schema metadata that carries
/// streamling's checkpoint signals.
#[derive(Debug)]
pub struct BroadcastingExec {
    handle: Arc<SharedSourceHandle>,
    /// Column indices into the shared source's full schema, or `None` for all columns.
    projection: Option<Arc<[usize]>>,
    /// Output schema after applying `projection` (full schema when `None`).
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl BroadcastingExec {
    pub fn new(handle: Arc<SharedSourceHandle>, projection: Option<Vec<usize>>) -> Result<Self> {
        let projection: Option<Arc<[usize]>> = projection.map(Arc::from);
        let schema: SchemaRef = match &projection {
            Some(indices) => Arc::new(handle.schema().project(indices)?),
            None => handle.schema(),
        };
        let cache = Self::compute_properties(schema.clone(), handle.partition_count());
        Ok(Self {
            handle,
            projection,
            schema,
            cache: Arc::new(cache),
        })
    }

    fn compute_properties(schema: SchemaRef, partitions: usize) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            // Every consumer sees the shared source at its real width; partition
            // j of the base plan is broadcast to partition j of each consumer.
            // The base plan's own `Partitioning` is not re-declared here — it is
            // hidden from the plan tree, so an inherited `Hash` claim could not
            // be checked by anything, and under-claiming only ever costs an
            // exchange a consumer would otherwise elide.
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        )
    }
}

impl DisplayAs for BroadcastingExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "BroadcastingExec: partitions={} (base=",
                    self.properties().output_partitioning().partition_count()
                )?;
                self.handle.base_exec.fmt_as(t, f)?;
                write!(f, ")")
            }
        }
    }
}

impl ExecutionPlan for BroadcastingExec {
    fn name(&self) -> &'static str {
        "BroadcastingExec"
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
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let consumer = self.handle.add_consumer(partition, context)?;

        match &self.projection {
            // Apply the consumer's projection to each broadcast batch. `RecordBatch::project`
            // preserves the batch's schema metadata (checkpoint signals), so this is
            // transparent to the streaming/checkpoint protocol.
            Some(indices) => {
                let indices = indices.clone();
                let projected = consumer.map(move |item| {
                    item.and_then(|batch| batch.project(&indices).map_err(Into::into))
                });
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    self.schema.clone(),
                    projected,
                )))
            }
            None => Ok(Box::pin(consumer)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operators::coalesce::test_util::MarkerSourceExec;
    use arrow::array::Int32Array;
    use datafusion::prelude::SessionContext;

    async fn drain(mut stream: SendableRecordBatchStream) -> Vec<i32> {
        let mut ids = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            ids.extend((0..batch.num_rows()).map(|i| column.value(i)));
        }
        ids.sort();
        ids
    }

    fn two_partition_source() -> Arc<MarkerSourceExec> {
        Arc::new(MarkerSourceExec::new(vec![
            vec![MarkerSourceExec::batch(vec![1, 2], &[])],
            vec![MarkerSourceExec::batch(vec![3, 4], &[])],
        ]))
    }

    /// A shared source used to be coalesced to one stream before broadcasting,
    /// so a parallel source lost its parallelism the moment a second consumer
    /// appeared. Each consumer must now see the source at its real width.
    #[test]
    fn broadcasting_exec_exposes_the_base_plans_partitions() {
        let base = two_partition_source();
        let handle = Arc::new(SharedSourceHandle::new(base.schema(), base, 10, 1));
        let exec = BroadcastingExec::new(handle, None).unwrap();

        assert_eq!(exec.properties().output_partitioning().partition_count(), 2);
    }

    /// A partition must not open just because *another* partition reached the
    /// expected consumer count — the consumer registering last would miss
    /// everything broadcast meanwhile.
    ///
    /// Asserted on the gate directly rather than on delivered rows: registration
    /// is synchronous, so a test that only drains streams would let the broadcast
    /// task start after every consumer had already registered, and would pass
    /// even with the gate removed.
    #[tokio::test]
    async fn a_partition_opens_only_once_its_own_consumers_registered() {
        let base = two_partition_source();
        let handle = Arc::new(SharedSourceHandle::new(base.schema(), base, 10, 2));
        let first = BroadcastingExec::new(handle.clone(), None).unwrap();
        let second = BroadcastingExec::new(handle.clone(), None).unwrap();
        let ctx = SessionContext::new();

        let _first_0 = first.execute(0, ctx.task_ctx()).unwrap();
        let _first_1 = first.execute(1, ctx.task_ctx()).unwrap();
        assert!(
            !handle.partition_started(0),
            "one of two consumers is not enough to open a partition"
        );
        assert!(
            !handle.partition_started(1),
            "partition 1 must not open because partition 0's count rose"
        );

        let _second_0 = second.execute(0, ctx.task_ctx()).unwrap();
        assert!(
            handle.partition_started(0),
            "partition 0 has both its consumers now"
        );
        assert!(
            !handle.partition_started(1),
            "partition 1 still has only one consumer"
        );

        let _second_1 = second.execute(1, ctx.task_ctx()).unwrap();
        assert!(handle.partition_started(1));
    }

    /// Partition j of the base plan must reach partition j of every consumer —
    /// no cross-partition mixing, nothing dropped.
    #[tokio::test]
    async fn every_consumer_receives_every_partition() {
        let base = two_partition_source();
        let handle = Arc::new(SharedSourceHandle::new(base.schema(), base, 10, 2));
        let first = BroadcastingExec::new(handle.clone(), None).unwrap();
        let second = BroadcastingExec::new(handle, None).unwrap();

        let ctx = SessionContext::new();
        let first_0 = first.execute(0, ctx.task_ctx()).unwrap();
        let first_1 = first.execute(1, ctx.task_ctx()).unwrap();
        let second_0 = second.execute(0, ctx.task_ctx()).unwrap();
        let second_1 = second.execute(1, ctx.task_ctx()).unwrap();

        assert_eq!(drain(first_0).await, vec![1, 2]);
        assert_eq!(drain(second_0).await, vec![1, 2]);
        assert_eq!(
            drain(first_1).await,
            vec![3, 4],
            "partition 1 must carry its own rows"
        );
        assert_eq!(drain(second_1).await, vec![3, 4]);
    }

    /// Projections are per consumer, and must stay per consumer per partition.
    #[tokio::test]
    async fn each_partition_applies_the_consumers_projection() {
        let base = two_partition_source();
        let handle = Arc::new(SharedSourceHandle::new(base.schema(), base, 10, 1));
        let exec = BroadcastingExec::new(handle, Some(vec![0])).unwrap();

        let ctx = SessionContext::new();
        let partition_0 = exec.execute(0, ctx.task_ctx()).unwrap();
        let partition_1 = exec.execute(1, ctx.task_ctx()).unwrap();

        assert_eq!(drain(partition_0).await, vec![1, 2]);
        assert_eq!(drain(partition_1).await, vec![3, 4]);
    }
}
