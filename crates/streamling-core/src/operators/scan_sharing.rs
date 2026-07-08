//! Scan sharing allows multiple sub-pipelines to reuse the same source scan.
//! This is useful for many sources. E.g., in case Kafka sources where we want to read from a topic
//! only once even if multiple downstream transforms/sinks consume from it.

use crate::operators::broadcast::broadcast_stream::BroadcastStream;
use arrow_schema::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
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

/// Handle for a shared source that manages broadcasting to multiple consumers
pub struct SharedSourceHandle {
    schema: SchemaRef,
    base_exec: Arc<dyn ExecutionPlan>,
    broadcast_stream: Arc<Mutex<Option<Arc<BroadcastStream>>>>,
    is_started: Arc<Mutex<bool>>,
    channel_capacity: usize,
    expected_consumers: AtomicUsize,
    registered_consumers: Arc<AtomicUsize>,
}

impl Debug for SharedSourceHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedSourceHandle")
            .field("schema", &self.schema)
            .field("channel_capacity", &self.channel_capacity)
            .field("expected", &self.expected_consumers.load(Ordering::SeqCst))
            .field(
                "registered",
                &self.registered_consumers.load(Ordering::SeqCst),
            )
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
        Self {
            schema,
            base_exec,
            broadcast_stream: Arc::new(Mutex::new(None)),
            is_started: Arc::new(Mutex::new(false)),
            channel_capacity,
            expected_consumers: AtomicUsize::new(expected_consumers),
            registered_consumers: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn base_exec(&self) -> Arc<dyn ExecutionPlan> {
        self.base_exec.clone()
    }

    /// Initialize the broadcast stream if not already done
    pub fn get_or_start_broadcast_stream(&self) -> Arc<BroadcastStream> {
        let mut broadcast_opt = self.broadcast_stream.lock().unwrap();

        if let Some(broadcast) = broadcast_opt.as_ref() {
            return broadcast.clone();
        }

        let broadcast = Arc::new(BroadcastStream::new(
            self.schema.clone(),
            self.channel_capacity,
        ));
        *broadcast_opt = Some(broadcast.clone());
        broadcast
    }

    /// Register a consumer
    fn register_consumer(&self) {
        let registered = self.registered_consumers.fetch_add(1, Ordering::SeqCst) + 1;
        let expected = self.expected_consumers.load(Ordering::SeqCst);
        debug!("Consumer registered: {}/{}", registered, expected);
    }

    /// Start broadcast when all consumers are registered (called by each consumer).
    pub fn start_if_needed(&self, partition: usize, context: Arc<TaskContext>) -> Result<()> {
        let mut started = self.is_started.lock().unwrap();
        if *started {
            return Ok(());
        }

        let registered = self.registered_consumers.load(Ordering::SeqCst);
        let expected = self.expected_consumers.load(Ordering::SeqCst);

        if registered >= expected {
            *started = true;
            debug!("All {} consumers registered, starting broadcast", expected);
            let broadcast = self.get_or_start_broadcast_stream();
            let source_stream = self.base_exec.execute(partition, context)?;
            broadcast.start(source_stream);
        } else {
            debug!(
                "Not all consumers registered yet: {}/{}, waiting for more",
                registered, expected
            );
        }

        Ok(())
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
        let cache = Self::compute_properties(schema.clone());
        Ok(Self {
            handle,
            projection,
            schema,
            cache: Arc::new(cache),
        })
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

impl DisplayAs for BroadcastingExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "BroadcastingExec (base=")?;
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
        let broadcast = self.handle.get_or_start_broadcast_stream();
        let consumer = broadcast.add_consumer();
        self.handle.register_consumer();
        self.handle.start_if_needed(partition, context)?;

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
