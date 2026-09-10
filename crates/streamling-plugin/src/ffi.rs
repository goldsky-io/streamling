#![allow(non_local_definitions)]
//! This module defines the FFI interface for the plugin system, including types and traits.

use crate::CheckpointEpoch;
use abi_stable::StableAbi;
use abi_stable::derive_macro_reexports::NonExhaustive;
use abi_stable::external_types::crossbeam_channel::{RReceiver, RSender};
use abi_stable::external_types::parking_lot::mutex::RMutex;
use abi_stable::std_types::{RArc, RHashMap, RNone, RSome, RString};
use arrow::array::{Array, ArrayRef, RecordBatch, StructArray, make_array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ffi::{FFI_ArrowSchema, from_ffi, to_ffi};
use arrow_data::ffi::FFI_ArrowArray;
use crossbeam_channel::TrySendError;
use datafusion::common::DataFusionError;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

#[repr(C)]
#[derive(StableAbi, Debug)]
pub struct PluginOptions(RHashMap<RString, RString>);

impl PluginOptions {
    pub fn new(options: HashMap<String, String>) -> Self {
        PluginOptions(
            options
                .into_iter()
                .map(|(k, v)| (RString::from(k), RString::from(v)))
                .collect(),
        )
    }

    pub fn as_rust(&self) -> HashMap<String, String> {
        self.0
            .iter()
            .map(|t| (t.0.to_string(), t.1.to_string()))
            .collect()
    }
}

/// Logging configuration for the plugin.
#[repr(u8)]
#[derive(StableAbi, Debug, Clone)]
pub enum PluginLogging {
    Plain,
    Json,
}

impl PluginLogging {
    pub fn initialize_logging(&self) {
        if tracing::dispatcher::has_been_set() {
            return;
        }

        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;

        let env_filter = tracing_subscriber::EnvFilter::from_default_env();
        let init_result = match self {
            PluginLogging::Json => tracing_subscriber::registry()
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_writer(std::io::stderr)
                        .fmt_fields(tracing_subscriber::fmt::format::JsonFields::new())
                        .event_format(streamling_common::logging::FlatJsonFormat),
                )
                .with(env_filter)
                .try_init(),
            PluginLogging::Plain => tracing_subscriber::registry()
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_writer(std::io::stderr)
                        .with_thread_ids(true)
                        .with_thread_names(true),
                )
                .with(env_filter)
                .try_init(),
        };
        if init_result.is_err() {
            eprintln!("Logger already initialized; skipping plugin logging setup.");
        }
    }
}

/// Custom wrapper for FFI_ArrowSchema
/// DataFusion's wrapper (WrappedSchema) looks almost the same, but since FFI_ArrowSchema
/// doesn't implement Sync, we need to add a mutex to allow concurrent access
#[repr(C)]
#[derive(StableAbi)]
pub struct SafeArrowSchema {
    #[sabi(unsafe_opaque_field)]
    pub schema: RArc<RMutex<FFI_ArrowSchema>>,
}

impl SafeArrowSchema {
    pub fn new(schema: FFI_ArrowSchema) -> Self {
        SafeArrowSchema {
            schema: RArc::new(RMutex::new(schema)),
        }
    }
}

impl fmt::Debug for SafeArrowSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Doing a *non-blocking* lock keeps Debug printing from hanging.
        match self.schema.try_lock() {
            RSome(guard) => f
                .debug_struct("SafeArrowSchema")
                // Delegate to FFI_ArrowSchema’s Debug
                .field("schema", &*guard)
                .finish(),
            RNone => f
                .debug_struct("SafeArrowSchema")
                .field("schema", &"<locked>")
                .finish(),
        }
    }
}

impl From<SchemaRef> for SafeArrowSchema {
    fn from(value: SchemaRef) -> Self {
        SafeArrowSchema::new(FFI_ArrowSchema::try_from(value.as_ref()).unwrap())
    }
}

impl From<SafeArrowSchema> for SchemaRef {
    fn from(value: SafeArrowSchema) -> Self {
        let schema = value.schema.lock();
        Arc::new(Schema::try_from(&*schema).unwrap())
    }
}

impl From<DataType> for SafeArrowSchema {
    fn from(value: DataType) -> Self {
        let field = Field::new("_", value, true);
        SafeArrowSchema::new(FFI_ArrowSchema::try_from(&field).unwrap())
    }
}

impl From<SafeArrowSchema> for DataType {
    fn from(value: SafeArrowSchema) -> Self {
        let schema = value.schema.lock();
        let field = Field::try_from(&*schema).unwrap();
        field.data_type().clone()
    }
}

/// A single Arrow array (column) transported across the FFI boundary.
#[repr(C)]
#[derive(StableAbi)]
pub struct SafeArrowColumn {
    #[sabi(unsafe_opaque_field)]
    pub array: FFI_ArrowArray,
    #[sabi(unsafe_opaque_field)]
    pub field: RArc<RMutex<FFI_ArrowSchema>>,
}

/// A UDF argument transported across the FFI boundary, preserving scalar vs array semantics.
///
/// When `is_scalar` is true, `column` holds exactly one element representing a constant value.
/// The host encodes `ColumnarValue::Scalar` as a length-1 array; the plugin reconstructs the
/// scalar without needing to broadcast it to `N` rows first.
#[repr(C)]
#[derive(StableAbi)]
pub struct SafeUdfArg {
    pub column: SafeArrowColumn,
    pub is_scalar: bool,
}

impl From<ArrayRef> for SafeArrowColumn {
    fn from(value: ArrayRef) -> Self {
        let field = Field::new("_", value.data_type().clone(), true);
        let ffi_schema = FFI_ArrowSchema::try_from(&field).unwrap();
        let (ffi_array, _) = to_ffi(&value.to_data()).unwrap();
        SafeArrowColumn {
            array: ffi_array,
            field: RArc::new(RMutex::new(ffi_schema)),
        }
    }
}

impl From<SafeArrowColumn> for ArrayRef {
    fn from(value: SafeArrowColumn) -> Self {
        let schema = value.field.lock();
        let array_data = unsafe { from_ffi(value.array, &schema).unwrap() };
        make_array(array_data)
    }
}

#[repr(C)]
#[derive(StableAbi, Debug)]
pub struct SafeArrowArray {
    #[sabi(unsafe_opaque_field)]
    pub array: FFI_ArrowArray,
    pub schema: SafeArrowSchema,
}

impl From<SafeArrowArray> for RecordBatch {
    fn from(value: SafeArrowArray) -> Self {
        let schema = value.schema.schema.lock();
        let array_data = unsafe {
            from_ffi(value.array, &schema)
                .map_err(DataFusionError::from)
                .unwrap()
        };
        let array = make_array(array_data);
        let struct_array = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or(DataFusionError::Execution(
                "Unexpected array type during record batch collection in FFI_RecordBatchStream"
                    .to_string(),
            ))
            .unwrap();

        struct_array.into()
    }
}

impl From<RecordBatch> for SafeArrowArray {
    fn from(value: RecordBatch) -> Self {
        let schema: SafeArrowSchema = value.schema().into();

        let struct_array = StructArray::from(value);
        let (array, _) = to_ffi(&struct_array.into_data()).unwrap();

        SafeArrowArray { array, schema }
    }
}

#[repr(C)]
#[derive(StableAbi, Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct PluginCheckpointEpoch(pub u64);

impl From<PluginCheckpointEpoch> for CheckpointEpoch {
    fn from(value: PluginCheckpointEpoch) -> Self {
        CheckpointEpoch(value.0)
    }
}

#[repr(u8)]
#[derive(StableAbi, Debug)]
#[sabi(kind(WithNonExhaustive(
    size = [usize;12],
    traits(Debug),
    assert_nonexhaustive(PluginMetric),
)))]
#[non_exhaustive]
pub enum PluginMetric {
    Count {
        name: RString,
        value: u64,
        tags: RHashMap<RString, RString>,
    },
    Gauge {
        name: RString,
        value: u64,
        tags: RHashMap<RString, RString>,
    },
    Time {
        name: RString,
        duration_ms: u64,
        tags: RHashMap<RString, RString>,
    },
}

/// Dispatcher liveness metric names. The SDK dispatchers emit these on the
/// plugin metrics channel (a throttled heartbeat while the dispatch loop
/// spins, and enter/exit breadcrumbs around the checkpoint-marker hook); the
/// host intercepts them for shutdown liveness attribution instead of
/// forwarding them to telemetry. Shared as consts so the two sides cannot
/// drift. Emission is `try_send` (see [`PluginMetricsRecorder::dispatch_metric`]),
/// so a full metrics channel drops a marker rather than ever blocking the
/// dispatcher.
pub const DISPATCHER_HEARTBEAT_METRIC: &str = "dispatcher.heartbeat";
pub const DISPATCHER_HOOK_ENTER_METRIC: &str = "dispatcher.hook.enter";
pub const DISPATCHER_HOOK_EXIT_METRIC: &str = "dispatcher.hook.exit";
/// Tag key carrying the hook name on [`DISPATCHER_HOOK_ENTER_METRIC`].
pub const DISPATCHER_HOOK_TAG: &str = "hook";

#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct PluginMetricsRecorder {
    sender: RSender<PluginMetric_NE>,
}

impl PluginMetricsRecorder {
    pub fn new(sender: RSender<PluginMetric_NE>) -> Self {
        PluginMetricsRecorder { sender }
    }

    pub fn record_count(&self, name: &str, value: u64) {
        self.dispatch_metric(PluginMetric::Count {
            name: RString::from(name),
            value,
            tags: Default::default(),
        });
    }

    pub fn record_count_w_tags(&self, name: &str, value: u64, tags: Vec<(&str, &str)>) {
        let tags = tags
            .into_iter()
            .map(|(k, v)| (RString::from(k), RString::from(v)))
            .collect();
        self.dispatch_metric(PluginMetric::Count {
            name: RString::from(name),
            value,
            tags,
        });
    }

    pub fn record_latency(&self, name: &str, duration: Duration) {
        self.dispatch_metric(PluginMetric::Time {
            name: RString::from(name),
            duration_ms: duration.as_millis() as u64,
            tags: Default::default(),
        });
    }

    pub fn record_latency_w_tags(&self, name: &str, duration: Duration, tags: Vec<(&str, &str)>) {
        let tags = tags
            .into_iter()
            .map(|(k, v)| (RString::from(k), RString::from(v)))
            .collect();
        self.dispatch_metric(PluginMetric::Time {
            name: RString::from(name),
            duration_ms: duration.as_millis() as u64,
            tags,
        });
    }

    pub fn record_gauge(&self, name: &str, value: u64) {
        self.dispatch_metric(PluginMetric::Gauge {
            name: RString::from(name),
            value,
            tags: Default::default(),
        });
    }

    pub fn record_gauge_w_tags(&self, name: &str, value: u64, tags: Vec<(&str, &str)>) {
        let tags = tags
            .into_iter()
            .map(|(k, v)| (RString::from(k), RString::from(v)))
            .collect();
        self.dispatch_metric(PluginMetric::Gauge {
            name: RString::from(name),
            value,
            tags,
        });
    }

    pub fn dispatch_metric(&self, metric: PluginMetric) {
        match self.sender.try_send(NonExhaustive::new(metric)) {
            Ok(_) => {
                debug!("Successfully dispatched plugin metrics")
            }
            Err(e) => {
                warn!("Encountered error dispatching metrics. Error: {}", e);
            }
        }
    }
}

/// How long a loop draining a [`PluginChannel`] parks when the channel is
/// empty, on either side of the FFI boundary.
///
/// Deliberately not `yield_now()`: an empty channel is the steady state for a
/// live pipeline, and an immediate reschedule spins every runtime worker
/// instead of letting them park. 1 ms is what the metrics loop
/// (`streamling-core/src/plugin/telemetry.rs`) and the checkpoint-ack forwarder
/// (`table_provider.rs`) have always used: small enough to be invisible next to
/// a checkpoint interval, large enough that the poll costs nothing.
pub const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(1);

#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct PluginChannel {
    pub sender: RSender<PluginMsg_NE>,
    pub receiver: RReceiver<PluginMsg_NE>,
}

/// Budget left untouched when a control-message send gives up, so the host
/// still has time to notice the dispatcher exited and drain it.
const DRAIN_SEND_RESERVE: Duration = Duration::from_secs(2);

/// Retry policy for control-message sends (source completion, checkpoint
/// marker/finalizer/ack).
///
/// While the pipeline is running a full channel is ordinary backpressure and
/// the host is still reading, so retry indefinitely — giving up there would
/// drop a checkpoint message for no reason.
///
/// Once a drain starts that stops being true. The host's source forwarder
/// stops reading this channel for good after it has forwarded the batches it
/// snapshotted, so a retry against a full channel can never succeed. Without a
/// bound the dispatcher spins at 50ms forever, `start()` never returns, and the
/// host's plugin drain waits out its whole budget on a dispatcher that has
/// nothing left to do — reporting it as unflushed when in fact it had already
/// flushed.
///
/// This bounds the *wait*, not the outcome. Callers treat the resulting error
/// as a failed best-effort send, and every checkpoint path that can surface it
/// leaves the epoch unacked — so offsets stay uncommitted and the tail replays
/// rather than being falsely reported durable.
fn stop_retrying_once_the_drain_needs_us_gone()
-> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>> {
    Box::pin(async move {
        if !crate::shutdown::is_shutting_down() {
            return true;
        }
        crate::shutdown::remaining_budget() > DRAIN_SEND_RESERVE
    })
}

impl PluginChannel {
    pub fn new(channels: (RSender<PluginMsg_NE>, RReceiver<PluginMsg_NE>)) -> Self {
        let (sender, receiver) = channels;
        PluginChannel { sender, receiver }
    }

    pub async fn send_with_retry<CreatePayloadFn>(
        &self,
        runtime: &crate::r#async::PluginAsyncRuntimeObj,
        op_name: &str,
        create_payload: CreatePayloadFn,
    ) -> Result<(), crate::api::PluginError>
    where
        CreatePayloadFn: Fn() -> PluginMsg_NE,
    {
        self.send_with_retry_callback(
            runtime,
            op_name,
            create_payload,
            Some(stop_retrying_once_the_drain_needs_us_gone),
            Duration::from_millis(50),
        )
        .await
    }

    /// Send a message with retry logic if the channel is full.
    /// Uses try_send to avoid blocking the thread, and retries with a delay.
    ///
    /// # Arguments
    /// * `runtime` - The async runtime for sleeping between retries
    /// * `op_name` - Name used in error messages and logging
    /// * `create_payload` - The function to create the payload to send. This is called every send attempt.
    /// * `on_retry` - Optional callback executed on each retry attempt. Return false to stop retrying.
    pub async fn send_with_retry_callback<CreatePayloadFn, OnRetryFn>(
        &self,
        runtime: &crate::r#async::PluginAsyncRuntimeObj,
        op_name: &str,
        create_payload: CreatePayloadFn,
        on_retry: Option<OnRetryFn>,
        retry_delay: Duration,
    ) -> Result<(), crate::api::PluginError>
    where
        CreatePayloadFn: Fn() -> PluginMsg_NE,
        OnRetryFn: Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>>,
    {
        loop {
            match self.sender.try_send(create_payload()) {
                Ok(_) => return Ok(()),
                Err(TrySendError::Full(_)) => {
                    // Execute optional retry callback and check if we should continue
                    if let Some(ref callback) = on_retry
                        && !callback().await
                    {
                        return Err(crate::api::PluginError::Execution(format!(
                            "{} retry callback returned false, stopping retries",
                            op_name
                        )));
                    }

                    runtime.sleep(retry_delay.into()).await;
                }
                Err(TrySendError::Disconnected(_)) => {
                    return Err(crate::api::PluginError::Execution(format!(
                        "{} output channel disconnected",
                        op_name
                    )));
                }
            }
        }
    }
}

#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct PluginMetricsChannel {
    pub sender: RSender<PluginMetric_NE>,
    pub receiver: RReceiver<PluginMetric_NE>,
}

impl PluginMetricsChannel {
    pub fn new(channels: (RSender<PluginMetric_NE>, RReceiver<PluginMetric_NE>)) -> Self {
        let (sender, receiver) = channels;
        PluginMetricsChannel { sender, receiver }
    }
}

#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct PluginChannels {
    pub input: PluginChannel,
    pub output: PluginChannel,
    pub metrics: PluginMetricsChannel,
}

// Largest variant is NextBatch (13 words); 5 spare words in [usize; 18] for future growth
#[repr(u8)]
#[derive(StableAbi, Debug)]
#[sabi(kind(WithNonExhaustive(
    size = [usize;18],
    traits(Debug),
    assert_nonexhaustive(PluginMsg),
)))]
#[non_exhaustive]
pub enum PluginMsg {
    Init,
    NextBatch {
        data: SafeArrowArray,
    },
    CheckpointMarker {
        epoch: PluginCheckpointEpoch,
    },
    CheckpointAck {
        epoch: PluginCheckpointEpoch,
    },
    /// The epoch finalized end-to-end (every live sink acked it). Consumer
    /// contract: handling MUST be idempotent, MUST NOT block, and MUST NEVER
    /// wait for one specific epoch's Finalizer — at shutdown the host drops
    /// in-flight timer epochs without ever sending their Finalizers, and only
    /// a later terminal epoch (covering their work) finalizes. Treat this as
    /// "everything up to and including `epoch` is durable", not as a
    /// guaranteed per-epoch event.
    CheckpointFinalizer {
        epoch: PluginCheckpointEpoch,
    },
    /// Host → plugin (input channel): stop and clean up.
    ///
    /// Plugin → host (a SOURCE dispatcher's OUTPUT channel): the source
    /// stopped on its own (bounded work complete) — the host ends the
    /// source's record-batch stream on receipt so downstream sinks see
    /// end-of-stream and a job-mode pipeline can finish. Without the signal,
    /// a self-terminating plugin source leaves its stream open forever. The
    /// reverse direction reuses this existing variant ON PURPOSE: adding a
    /// new variant is rejected by abi_stable's layout check when the host
    /// expects more variants than an older library carries (the same
    /// asymmetry as prefix fields), which would break loading every existing
    /// plugin dylib. A host predating this meaning ignores output-direction
    /// Terminate (its match falls through), which is exactly the old
    /// behavior; no plugin has ever sent Terminate host-ward before this.
    Terminate,
    Topology {
        config: RString,
    },
    Error {
        message: RString,
    },
}
