//! This modules provides optional dispatching logic to that connects the channel-based FFI
//! functionality with the clean Rust API.
//! Users may choose to implement this dispatching logic in their plugins if needed.

use crate::api::{PreprocessorPlugin, SourcePlugin, TransformPlugin};
use crate::r#async::PluginAsyncRuntimeObj;
use crate::ffi::SafeArrowArray;
use crate::{PluginChannels, PluginError, PluginMsg, SinkPlugin};
use abi_stable::derive_macro_reexports::NonExhaustive;
use abi_stable::std_types::RString;
use arrow::array::RecordBatch;
use async_ffi::FutureExt;
use crossbeam_channel::TryRecvError;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, warn};

use crate::ffi::IDLE_POLL_INTERVAL;

/// Outcome of [`wait_for_initialization`]: distinguishes the two messages the
/// caller can legitimately receive on the input channel before any data flows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InitOutcome {
    /// The host has signaled the plugin to begin its main loop. The plugin
    /// should call its own `initialize()` and proceed to process data.
    Init,
    /// The host sent `Terminate` before `Init`. This happens whenever the
    /// pipeline tears down before any source's `ExecutionPlan::execute` runs
    /// (e.g. under `--validate` / `--dry-run`, or when topology setup fails
    /// after plugins have been constructed). The plugin's `initialize()`
    /// should NOT be called: it's the only place plugins are allowed to open
    /// runtime resources (network sockets, ClickHouse connections, etc.), and
    /// running it just to immediately tear down would do real I/O against
    /// environments that may not exist (e.g. a validator pod).
    Terminate,
}

/// Block until the host sends the first control message. Returns whether it
/// was `Init` (proceed to initialize) or `Terminate` (skip initialize and
/// shut down cleanly). Any other message, malformed wrapper, or channel
/// disconnect is treated as an error.
fn wait_for_initialization(channels: &PluginChannels) -> Result<InitOutcome, PluginError> {
    match channels.input.receiver.recv().map(|m| m.into_enum()) {
        Ok(Ok(PluginMsg::Init)) => Ok(InitOutcome::Init),
        Ok(Ok(PluginMsg::Terminate)) => Ok(InitOutcome::Terminate),
        Ok(Ok(_other)) => Err(PluginError::Execution(
            "Expected Init message as first message".to_string(),
        )),
        Ok(Err(_unwrap_err)) => Err(PluginError::Execution(
            "Malformed message wrapper during initialization".to_string(),
        )),
        Err(_recv_err) => Err(PluginError::Execution(
            "Channel disconnected during initialization".to_string(),
        )),
    }
}

use crate::ffi::{PluginCheckpointEpoch, PluginMetricsRecorder};

/// How often the dispatch loops emit their liveness heartbeat at most. Loop
/// iterations are far more frequent than this (the idle arm yields, it does
/// not sleep), so the throttle keeps the metrics channel traffic negligible.
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(250);

/// Emits dispatcher liveness markers on the plugin metrics channel: a
/// throttled heartbeat while the dispatch loop spins, and enter/exit
/// breadcrumbs around the checkpoint-marker hook (the one hook that runs
/// arbitrary plugin flush/retry code and has wedged in practice). Emission is
/// `try_send` under the hood, so a full channel drops a marker instead of
/// ever blocking the dispatcher. The host intercepts these metric names for
/// shutdown liveness attribution; they never reach user-facing telemetry.
struct LivenessBeacon {
    recorder: PluginMetricsRecorder,
    last_beat: Option<std::time::Instant>,
}

impl LivenessBeacon {
    fn new(channels: &PluginChannels) -> Self {
        LivenessBeacon {
            recorder: PluginMetricsRecorder::new(channels.metrics.sender.clone()),
            last_beat: None,
        }
    }

    /// Throttled heartbeat: emits on the first call, then at most once per
    /// [`HEARTBEAT_INTERVAL`].
    fn beat(&mut self) {
        if self
            .last_beat
            .is_none_or(|t| t.elapsed() >= HEARTBEAT_INTERVAL)
        {
            self.recorder
                .record_count(crate::ffi::DISPATCHER_HEARTBEAT_METRIC, 1);
            self.last_beat = Some(std::time::Instant::now());
        }
    }

    fn hook_enter(&self, hook: &str) {
        self.recorder.record_count_w_tags(
            crate::ffi::DISPATCHER_HOOK_ENTER_METRIC,
            1,
            vec![(crate::ffi::DISPATCHER_HOOK_TAG, hook)],
        );
    }

    /// Balances [`Self::hook_enter`]. Only call on hook success: an erroring
    /// hook leaves the enter breadcrumb unbalanced on purpose, so the host
    /// still attributes a dispatcher that died inside the hook.
    fn hook_exit(&self) {
        self.recorder
            .record_count(crate::ffi::DISPATCHER_HOOK_EXIT_METRIC, 1);
    }
}

/// Handle checkpoint marker message for any plugin type
async fn handle_checkpoint_marker(
    channels: &PluginChannels,
    epoch: PluginCheckpointEpoch,
    runtime: &PluginAsyncRuntimeObj,
) -> Result<(), PluginError> {
    channels
        .output
        .send_with_retry(runtime, "Checkpoint marker", || {
            NonExhaustive::new(PluginMsg::CheckpointMarker { epoch })
        })
        .await
}

/// Handle checkpoint finalizer message for any plugin type
async fn handle_checkpoint_finalizer(
    channels: &PluginChannels,
    epoch: PluginCheckpointEpoch,
    runtime: &PluginAsyncRuntimeObj,
) -> Result<(), PluginError> {
    channels
        .output
        .send_with_retry(runtime, "Checkpoint finalizer", || {
            NonExhaustive::new(PluginMsg::CheckpointFinalizer { epoch })
        })
        .await
}

/// Handle checkpoint ack message for sink plugins
async fn handle_checkpoint_ack(
    channels: &PluginChannels,
    epoch: PluginCheckpointEpoch,
    runtime: &PluginAsyncRuntimeObj,
) -> Result<(), PluginError> {
    channels
        .output
        .send_with_retry(runtime, "Checkpoint ack", || {
            NonExhaustive::new(PluginMsg::CheckpointAck { epoch })
        })
        .await
}

async fn handle_control_messages(
    channels: &PluginChannels,
    source_plugin: &Arc<dyn SourcePlugin>,
    runtime: &PluginAsyncRuntimeObj,
) -> Result<(), PluginError> {
    while !channels.input.receiver.is_empty() {
        match channels.input.receiver.recv().map(|m| m.into_enum()) {
            Ok(Ok(PluginMsg::Init)) => {
                return Err(PluginError::Execution(
                    "Received Init message after plugin was initialized".to_string(),
                ));
            }
            Ok(Ok(PluginMsg::CheckpointMarker { epoch })) => {
                source_plugin
                    .process_checkpoint_marker(epoch.into())
                    .await?;
                handle_checkpoint_marker(channels, epoch, runtime).await?;
            }
            Ok(Ok(PluginMsg::CheckpointFinalizer { epoch })) => {
                source_plugin
                    .process_checkpoint_finalizer(epoch.into())
                    .await?;
                handle_checkpoint_finalizer(channels, epoch, runtime).await?;
            }
            Ok(Ok(PluginMsg::Terminate)) => {
                source_plugin.terminate().await?;
                // Bounded wait for tasks spawned via `shutdown::spawn` so
                // buffered work they hold is flushed before the library goes
                // quiet. No-op when nothing was tracked.
                crate::shutdown::drain_tracked().await;
            }
            Err(e) => {
                return Err(PluginError::Execution(format!(
                    "Error receiving message from input channel: {e}"
                )));
            }
            _ => {}
        }
    }
    Ok(())
}

/// After a sink/transform plugin fails, keep consuming its input channel —
/// dropping data batches and swallowing checkpoint messages WITHOUT acking or
/// forwarding — until `Terminate` arrives (or the channel disconnects), then
/// surface the original error.
///
/// Exiting immediately on the error instead leaves a full input channel
/// behind: the host-side writer stays blocked on it, the plugin's stream
/// cannot end, `Terminate` cannot be delivered, and the failing drain rides
/// the watchdog into a hard exit (observed in the field as "input channel
/// still full 5s after ..." followed by a blown drain-budget slice).
///
/// Not acking/forwarding markers here is load-bearing: an epoch covering
/// dropped batches must never finalize, so its offsets stay uncommitted and
/// the dropped data replays on restart — at-least-once holds.
async fn drain_discard_after_failure<F>(
    channels: &PluginChannels,
    runtime: &PluginAsyncRuntimeObj,
    error: PluginError,
    terminate: F,
) -> Result<(), PluginError>
where
    F: std::future::Future<Output = Result<(), PluginError>>,
{
    error!(
        "Plugin failed; discarding queued input until Terminate so the channel cannot wedge the drain: {:?}",
        error
    );
    let mut beacon = LivenessBeacon::new(channels);
    let mut terminate = Some(terminate);
    let mut dropped_batches: u64 = 0;
    let mut swallowed_checkpoints: u64 = 0;

    loop {
        beacon.beat();
        match channels.input.receiver.try_recv().map(|m| m.into_enum()) {
            Ok(Ok(PluginMsg::NextBatch { .. })) => {
                dropped_batches += 1;
            }
            Ok(Ok(PluginMsg::CheckpointMarker { .. }))
            | Ok(Ok(PluginMsg::CheckpointFinalizer { .. })) => {
                swallowed_checkpoints += 1;
            }
            Ok(Ok(PluginMsg::Terminate)) => {
                // Best effort — the plugin already failed, and its cleanup
                // outcome must not mask the original error.
                if let Some(fut) = terminate.take()
                    && let Err(te) = fut.await
                {
                    warn!("Plugin terminate() after failure also errored: {:?}", te);
                }
                crate::shutdown::drain_tracked().await;
                break;
            }
            Err(TryRecvError::Empty) => {
                runtime.yield_now().await;
            }
            Err(TryRecvError::Disconnected) => break,
            _ => {}
        }
    }

    error!(
        "Plugin drained after failure: dropped {} data batch(es), swallowed {} checkpoint message(s) without acking (their epochs stay uncommitted; the data replays on restart)",
        dropped_batches, swallowed_checkpoints
    );
    Err(error)
}

pub struct SourcePluginDispatcher {
    channels: PluginChannels,
    source_plugin: Arc<dyn SourcePlugin>,
}

impl SourcePluginDispatcher {
    pub fn new(channels: PluginChannels, source_plugin: Arc<dyn SourcePlugin>) -> Self {
        SourcePluginDispatcher {
            channels,
            source_plugin,
        }
    }

    pub async fn start(&self, runtime: PluginAsyncRuntimeObj) -> Result<(), PluginError> {
        // If the host sends `Terminate` before `Init` (validation / early-teardown
        // path), short-circuit before `initialize()` runs. This is the contract
        // plugin authors rely on when keeping runtime I/O out of `new()`: the
        // host guarantees `initialize()` does not run when termination comes
        // first, so plugins can open network connections, DB clients, etc. there
        // without worrying about hermetic-validation environments.
        match wait_for_initialization(&self.channels)? {
            InitOutcome::Terminate => {
                self.source_plugin.terminate().await?;
                return Ok(());
            }
            InitOutcome::Init => {}
        }
        if !self.source_plugin.is_running() {
            return Ok(());
        }
        self.source_plugin.initialize().await?;
        let mut beacon = LivenessBeacon::new(&self.channels);

        loop {
            beacon.beat();
            // Generation loop
            // The idea is to continuously generate batches from the source plugin
            // and send them to the output channel, BUT it needs to occasionally check
            // the input channel for control messages (checkpoint markers, etc.). So there is
            // a timeout that's used to exit the generation loop and check the input channel.
            let source_plugin = self.source_plugin.clone();

            if !source_plugin.is_running() {
                break;
            }

            let runtime_clone = runtime.clone();
            let channels_clone = self.channels.clone();
            let source_plugin_clone = self.source_plugin.clone();
            let generate_batch_future = async move {
                match source_plugin.generate_batch().await {
                    Ok(batch) => {
                        let retry_callback = || -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>> {
                            let channels = channels_clone.clone();
                            let source_plugin = source_plugin_clone.clone();
                            let runtime = runtime_clone.clone();
                            Box::pin(async move {
                                // Handle control messages in case the output channel is full
                                let _ = handle_control_messages(&channels, &source_plugin, &runtime).await;
                                // Check if plugin is still running
                                source_plugin.is_running()
                            })
                        };

                        let _ = channels_clone.output.send_with_retry_callback(
                            &runtime_clone,
                            "Source plugin",
                            || {
                                let batch_data: SafeArrowArray = batch.clone().into();
                                NonExhaustive::new(PluginMsg::NextBatch { data: batch_data })
                            },
                            Some(retry_callback),
                            Duration::from_millis(50),
                        )
                        .await;
                        // Ignore errors - source plugin doesn't propagate them
                    }
                    Err(e) => {
                        error!("Error generating batch: {:?}", e);
                    }
                }
            }
            .into_ffi();

            runtime.spawn(generate_batch_future).await;

            // A failing checkpoint hook must not exit with a possibly-full
            // input channel behind it — the host-side control writer would
            // stay blocked and the drain would wedge (same rationale as the
            // sink/transform hook paths above).
            if let Err(e) =
                handle_control_messages(&self.channels, &self.source_plugin, &runtime).await
            {
                return drain_discard_after_failure(
                    &self.channels,
                    &runtime,
                    e,
                    self.source_plugin.terminate(),
                )
                .await;
            }
        }

        // The source stopped on its own (`is_running()` went false — bounded
        // work complete, or a Terminate was processed). Tell the host so it
        // can end this source's record-batch stream: without the signal a
        // bounded job's sinks never see end-of-stream and the pipeline cannot
        // complete. Output-direction Terminate is the completion signal (see
        // its doc on `PluginMsg` for why no new variant can be added). Best
        // effort — on the host-initiated Terminate path the stream is already
        // being torn down and the message is redundant; a host predating this
        // meaning ignores it.
        if let Err(e) = self
            .channels
            .output
            .send_with_retry(&runtime, "Source complete", || {
                NonExhaustive::new(PluginMsg::Terminate)
            })
            .await
        {
            warn!("Failed to notify host of source completion: {:?}", e);
        }

        Ok(())
    }
}

pub struct TransformPluginDispatcher {
    channels: PluginChannels,
    transform_plugin: Arc<dyn TransformPlugin>,
}

impl TransformPluginDispatcher {
    pub fn new(channels: PluginChannels, transform_plugin: Arc<dyn TransformPlugin>) -> Self {
        TransformPluginDispatcher {
            channels,
            transform_plugin,
        }
    }

    pub async fn start(&self, runtime: PluginAsyncRuntimeObj) -> Result<(), PluginError> {
        // See SourcePluginDispatcher::start for the rationale behind short-
        // circuiting on Terminate before calling `initialize()`.
        match wait_for_initialization(&self.channels)? {
            InitOutcome::Terminate => {
                self.transform_plugin.terminate().await?;
                return Ok(());
            }
            InitOutcome::Init => {}
        }
        if !self.transform_plugin.is_running() {
            return Ok(());
        }
        // See the sink dispatcher: plugin-error paths drain the input channel
        // before surfacing the error, so a failed transform cannot wedge the
        // host-side writer (and swallowed markers are deliberately NOT
        // forwarded — a marker riding past dropped data would let its epoch
        // finalize over lost rows).
        if let Err(e) = self.transform_plugin.initialize().await {
            return drain_discard_after_failure(
                &self.channels,
                &runtime,
                e,
                self.transform_plugin.terminate(),
            )
            .await;
        }
        let mut beacon = LivenessBeacon::new(&self.channels);

        loop {
            beacon.beat();
            if !self.transform_plugin.is_running() {
                break;
            }

            match self
                .channels
                .input
                .receiver
                .try_recv()
                .map(|m| m.into_enum())
            {
                Ok(Ok(PluginMsg::NextBatch { data })) => {
                    let batch: RecordBatch = data.into();

                    let processed_batch = match self.transform_plugin.process_batch(batch).await {
                        Ok(b) => b,
                        Err(e) => {
                            return drain_discard_after_failure(
                                &self.channels,
                                &runtime,
                                e,
                                self.transform_plugin.terminate(),
                            )
                            .await;
                        }
                    };

                    let transform_plugin = self.transform_plugin.clone();
                    let retry_callback =
                        || -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>> {
                            let plugin = transform_plugin.clone();
                            Box::pin(async move {
                                // Check if plugin is still running
                                plugin.is_running()
                            })
                        };

                    self.channels
                        .output
                        .send_with_retry_callback(
                            &runtime,
                            "Transform plugin",
                            || {
                                let batch_data: SafeArrowArray = processed_batch.clone().into();
                                NonExhaustive::new(PluginMsg::NextBatch { data: batch_data })
                            },
                            Some(retry_callback),
                            Duration::from_millis(50),
                        )
                        .await?;
                }
                Ok(Ok(PluginMsg::CheckpointMarker { epoch })) => {
                    beacon.hook_enter("checkpoint_marker");
                    if let Err(e) = self
                        .transform_plugin
                        .process_checkpoint_marker(epoch.into())
                        .await
                    {
                        return drain_discard_after_failure(
                            &self.channels,
                            &runtime,
                            e,
                            self.transform_plugin.terminate(),
                        )
                        .await;
                    }
                    beacon.hook_exit();
                    handle_checkpoint_marker(&self.channels, epoch, &runtime).await?;
                }
                Ok(Ok(PluginMsg::CheckpointFinalizer { epoch })) => {
                    if let Err(e) = self
                        .transform_plugin
                        .process_checkpoint_finalizer(epoch.into())
                        .await
                    {
                        return drain_discard_after_failure(
                            &self.channels,
                            &runtime,
                            e,
                            self.transform_plugin.terminate(),
                        )
                        .await;
                    }
                    handle_checkpoint_finalizer(&self.channels, epoch, &runtime).await?;
                }
                Ok(Ok(PluginMsg::Terminate)) => {
                    self.transform_plugin.terminate().await?;
                    crate::shutdown::drain_tracked().await;
                }
                Err(TryRecvError::Empty) => {
                    runtime.sleep(IDLE_POLL_INTERVAL.into()).await;
                }
                Err(TryRecvError::Disconnected) => {
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }
}

pub struct SinkPluginDispatcher {
    channels: PluginChannels,
    sink_plugin: Arc<dyn SinkPlugin>,
    plugin_metrics_recorder: PluginMetricsRecorder,
}

impl SinkPluginDispatcher {
    pub fn new(channels: PluginChannels, sink_plugin: Arc<dyn SinkPlugin>) -> Self {
        let metrics_sender = channels.metrics.sender.clone();
        SinkPluginDispatcher {
            channels,
            sink_plugin,
            plugin_metrics_recorder: PluginMetricsRecorder::new(metrics_sender),
        }
    }

    pub async fn start(&self, runtime: PluginAsyncRuntimeObj) -> Result<(), PluginError> {
        // See SourcePluginDispatcher::start for the rationale behind short-
        // circuiting on Terminate before calling `initialize()`.
        match wait_for_initialization(&self.channels)? {
            InitOutcome::Terminate => {
                self.sink_plugin.terminate().await?;
                return Ok(());
            }
            InitOutcome::Init => {}
        }
        if !self.sink_plugin.is_running() {
            return Ok(());
        }
        // Every plugin-error path below (initialize included — sink prechecks
        // run there while data batches queue up on the input channel) goes
        // through drain_discard_after_failure rather than returning directly:
        // exiting with a full input channel wedges the whole drain.
        if let Err(e) = self.sink_plugin.initialize().await {
            return drain_discard_after_failure(
                &self.channels,
                &runtime,
                e,
                self.sink_plugin.terminate(),
            )
            .await;
        }
        let mut beacon = LivenessBeacon::new(&self.channels);

        loop {
            beacon.beat();
            if !self.sink_plugin.is_running() {
                break;
            }

            match self
                .channels
                .input
                .receiver
                .try_recv()
                .map(|m| m.into_enum())
            {
                Ok(Ok(PluginMsg::NextBatch { data })) => {
                    let batch: RecordBatch = data.into();
                    let num_rows = batch.num_rows();
                    let plugin_process_batch = std::time::Instant::now();
                    let result = self.sink_plugin.process_batch(batch).await;
                    let duration = plugin_process_batch.elapsed();
                    match result {
                        Ok(()) => {
                            self.plugin_metrics_recorder
                                .record_count("output_rows", num_rows as u64);
                            self.plugin_metrics_recorder
                                .record_latency("elapsed_compute", duration);
                        }
                        Err(e) => {
                            // Propagate the error to fail the pipeline — but
                            // only after the input channel is drained. Any
                            // retry mechanism is the plugin's own business.
                            return drain_discard_after_failure(
                                &self.channels,
                                &runtime,
                                e,
                                self.sink_plugin.terminate(),
                            )
                            .await;
                        }
                    }
                }
                Ok(Ok(PluginMsg::CheckpointMarker { epoch })) => {
                    beacon.hook_enter("checkpoint_marker");
                    if let Err(e) = self
                        .sink_plugin
                        .process_checkpoint_marker(epoch.into())
                        .await
                    {
                        return drain_discard_after_failure(
                            &self.channels,
                            &runtime,
                            e,
                            self.sink_plugin.terminate(),
                        )
                        .await;
                    }
                    beacon.hook_exit();
                    handle_checkpoint_ack(&self.channels, epoch, &runtime).await?;
                }
                Ok(Ok(PluginMsg::CheckpointFinalizer { epoch })) => {
                    if let Err(e) = self
                        .sink_plugin
                        .process_checkpoint_finalizer(epoch.into())
                        .await
                    {
                        return drain_discard_after_failure(
                            &self.channels,
                            &runtime,
                            e,
                            self.sink_plugin.terminate(),
                        )
                        .await;
                    }
                }
                Ok(Ok(PluginMsg::Terminate)) => {
                    self.sink_plugin.terminate().await?;
                    crate::shutdown::drain_tracked().await;
                }
                Err(TryRecvError::Empty) => {
                    runtime.sleep(IDLE_POLL_INTERVAL.into()).await;
                }
                Err(TryRecvError::Disconnected) => {
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }
}

pub struct PreprocessorPluginDispatcher {
    channels: PluginChannels,
    preprocessor_plugin: Arc<dyn PreprocessorPlugin>,
}

impl PreprocessorPluginDispatcher {
    pub fn new(channels: PluginChannels, preprocessor_plugin: Arc<dyn PreprocessorPlugin>) -> Self {
        PreprocessorPluginDispatcher {
            channels,
            preprocessor_plugin,
        }
    }

    pub async fn start(&self) -> Result<(), PluginError> {
        match self.channels.input.receiver.recv().map(|m| m.into_enum()) {
            Ok(Ok(PluginMsg::Topology { config })) => {
                match self
                    .preprocessor_plugin
                    .preprocess_topology(config.into_string())
                    .await
                {
                    Ok(result) => {
                        self.channels
                            .output
                            .sender
                            .send(NonExhaustive::new(PluginMsg::Topology {
                                config: RString::from(result),
                            }))
                            .map_err(|e| {
                                PluginError::Execution(format!(
                                    "Failed to send topology response: {}",
                                    e
                                ))
                            })?;
                    }
                    Err(e) => {
                        let error_msg = e.to_string();
                        if let Err(send_err) =
                            self.channels
                                .output
                                .sender
                                .send(NonExhaustive::new(PluginMsg::Error {
                                    message: RString::from(error_msg),
                                }))
                        {
                            tracing::error!(
                                "Failed to send error message through plugin channel: {}",
                                send_err
                            );
                        }
                        return Err(e);
                    }
                }
            }
            Ok(Ok(PluginMsg::Terminate)) => return Ok(()),
            Ok(Ok(other)) => {
                return Err(PluginError::Execution(format!(
                    "Expected Topology message, got: {:?}",
                    other
                )));
            }
            Ok(Err(_)) => {
                return Err(PluginError::Execution(
                    "Malformed message wrapper".to_string(),
                ));
            }
            Err(e) => {
                return Err(PluginError::Execution(format!(
                    "Channel disconnected: {}",
                    e
                )));
            }
        }

        // Wait for Terminate
        match self.channels.input.receiver.recv().map(|m| m.into_enum()) {
            Ok(Ok(PluginMsg::Terminate)) => Ok(()),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::{PluginChannel, PluginChannels, PluginMetricsChannel, PluginMsg};
    use abi_stable::external_types::crossbeam_channel;
    use async_trait::async_trait;

    fn make_channels() -> PluginChannels {
        PluginChannels {
            input: PluginChannel::new(crossbeam_channel::bounded(8)),
            output: PluginChannel::new(crossbeam_channel::bounded(8)),
            metrics: PluginMetricsChannel::new(crossbeam_channel::bounded(8)),
        }
    }

    struct FailingPreprocessor {
        error_msg: String,
    }

    #[async_trait]
    impl PreprocessorPlugin for FailingPreprocessor {
        async fn preprocess_topology(&self, _config: String) -> Result<String, PluginError> {
            Err(PluginError::Execution(self.error_msg.clone()))
        }
    }

    struct SuccessPreprocessor {
        result: String,
    }

    #[async_trait]
    impl PreprocessorPlugin for SuccessPreprocessor {
        async fn preprocess_topology(&self, _config: String) -> Result<String, PluginError> {
            Ok(self.result.clone())
        }
    }

    #[tokio::test]
    async fn preprocessor_start_sends_error_on_preprocess_failure() {
        let channels = make_channels();
        let error_msg = "transform 'foo' missing required field 'type'";
        let plugin: Arc<dyn PreprocessorPlugin> = Arc::new(FailingPreprocessor {
            error_msg: error_msg.to_string(),
        });
        let dispatcher = PreprocessorPluginDispatcher::new(channels.clone(), plugin);

        channels
            .input
            .sender
            .send(NonExhaustive::new(PluginMsg::Topology {
                config: RString::from("some_config"),
            }))
            .unwrap();

        let result = dispatcher.start().await;
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains(error_msg),
            "start() should propagate the preprocessor error"
        );

        let output_msg = channels
            .output
            .receiver
            .try_recv()
            .expect("output channel should contain an Error message");
        match output_msg.into_enum() {
            Ok(PluginMsg::Error { message }) => {
                assert_eq!(message.as_str(), error_msg);
            }
            other => panic!("expected PluginMsg::Error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn preprocessor_start_returns_ok_on_terminate_before_topology() {
        let channels = make_channels();
        let plugin: Arc<dyn PreprocessorPlugin> = Arc::new(FailingPreprocessor {
            error_msg: "should not be called".to_string(),
        });
        let dispatcher = PreprocessorPluginDispatcher::new(channels.clone(), plugin);

        channels
            .input
            .sender
            .send(NonExhaustive::new(PluginMsg::Terminate))
            .unwrap();

        let result = dispatcher.start().await;
        assert!(result.is_ok(), "Terminate before Topology should succeed");
    }

    #[tokio::test]
    async fn preprocessor_start_errors_on_unexpected_message() {
        let channels = make_channels();
        let plugin: Arc<dyn PreprocessorPlugin> = Arc::new(FailingPreprocessor {
            error_msg: "should not be called".to_string(),
        });
        let dispatcher = PreprocessorPluginDispatcher::new(channels.clone(), plugin);

        channels
            .input
            .sender
            .send(NonExhaustive::new(PluginMsg::Init))
            .unwrap();

        let result = dispatcher.start().await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Expected Topology message"),
        );
    }

    #[tokio::test]
    async fn preprocessor_start_sends_topology_response_on_success() {
        let channels = make_channels();
        let plugin: Arc<dyn PreprocessorPlugin> = Arc::new(SuccessPreprocessor {
            result: "processed_config".to_string(),
        });
        let dispatcher = PreprocessorPluginDispatcher::new(channels.clone(), plugin);

        channels
            .input
            .sender
            .send(NonExhaustive::new(PluginMsg::Topology {
                config: RString::from("input_config"),
            }))
            .unwrap();

        channels
            .input
            .sender
            .send(NonExhaustive::new(PluginMsg::Terminate))
            .unwrap();

        let result = dispatcher.start().await;
        assert!(result.is_ok());

        let output_msg = channels
            .output
            .receiver
            .try_recv()
            .expect("output channel should contain a Topology response");
        match output_msg.into_enum() {
            Ok(PluginMsg::Topology { config }) => {
                assert_eq!(config.as_str(), "processed_config");
            }
            other => panic!("expected PluginMsg::Topology, got: {:?}", other),
        }
    }

    // ------------------------------------------------------------------
    // Source / Transform / Sink: Terminate-before-Init short-circuit.
    //
    // Validation/early-teardown paths cause `terminate_all_plugins` to send
    // `Terminate` to plugins whose `Init` was never sent. The dispatcher
    // contract is that `initialize()` is NOT called in that case, and that
    // the plugin's `terminate()` IS called. Plugin authors rely on this to
    // keep runtime I/O (network sockets, DB clients, etc.) inside
    // `initialize()` without breaking hermetic validation environments.
    // ------------------------------------------------------------------

    use crate::api::{SinkPlugin, SourcePlugin, SupportsGracefulShutdown, TransformPlugin};
    use crate::r#async::DirectTokioProxy;
    use arrow::datatypes::{Schema, SchemaRef};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// Records whether `initialize()` and `terminate()` were called. Used to
    /// assert short-circuit behavior across all three streaming dispatchers.
    #[derive(Default)]
    struct LifecycleRecorder {
        initialized: AtomicBool,
        terminated: AtomicUsize,
    }

    impl LifecycleRecorder {
        fn was_initialized(&self) -> bool {
            self.initialized.load(Ordering::SeqCst)
        }
        fn terminate_count(&self) -> usize {
            self.terminated.load(Ordering::SeqCst)
        }
    }

    fn empty_schema() -> SchemaRef {
        Arc::new(Schema::empty())
    }

    struct RecordingSource {
        recorder: Arc<LifecycleRecorder>,
        running: AtomicBool,
    }

    impl RecordingSource {
        fn new(recorder: Arc<LifecycleRecorder>) -> Self {
            Self {
                recorder,
                running: AtomicBool::new(true),
            }
        }
    }

    #[async_trait]
    impl SupportsGracefulShutdown for RecordingSource {
        fn is_running(&self) -> bool {
            self.running.load(Ordering::SeqCst)
        }
        async fn terminate(&self) -> Result<(), PluginError> {
            self.recorder.terminated.fetch_add(1, Ordering::SeqCst);
            self.running.store(false, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl SourcePlugin for RecordingSource {
        async fn initialize(&self) -> Result<(), PluginError> {
            self.recorder.initialized.store(true, Ordering::SeqCst);
            Ok(())
        }
        fn output_schema(&self) -> Result<SchemaRef, PluginError> {
            Ok(empty_schema())
        }
        async fn generate_batch(&self) -> Result<RecordBatch, PluginError> {
            Ok(RecordBatch::new_empty(empty_schema()))
        }
        async fn process_checkpoint_marker(
            &self,
            _epoch: crate::api::CheckpointEpoch,
        ) -> Result<(), PluginError> {
            Ok(())
        }
        async fn process_checkpoint_finalizer(
            &self,
            _epoch: crate::api::CheckpointEpoch,
        ) -> Result<(), PluginError> {
            Ok(())
        }
    }

    struct RecordingTransform {
        recorder: Arc<LifecycleRecorder>,
        running: AtomicBool,
    }

    impl RecordingTransform {
        fn new(recorder: Arc<LifecycleRecorder>) -> Self {
            Self {
                recorder,
                running: AtomicBool::new(true),
            }
        }
    }

    #[async_trait]
    impl SupportsGracefulShutdown for RecordingTransform {
        fn is_running(&self) -> bool {
            self.running.load(Ordering::SeqCst)
        }
        async fn terminate(&self) -> Result<(), PluginError> {
            self.recorder.terminated.fetch_add(1, Ordering::SeqCst);
            self.running.store(false, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl TransformPlugin for RecordingTransform {
        async fn initialize(&self) -> Result<(), PluginError> {
            self.recorder.initialized.store(true, Ordering::SeqCst);
            Ok(())
        }
        fn output_schema(&self) -> Result<SchemaRef, PluginError> {
            Ok(empty_schema())
        }
        async fn process_batch(&self, data: RecordBatch) -> Result<RecordBatch, PluginError> {
            Ok(data)
        }
        async fn process_checkpoint_marker(
            &self,
            _epoch: crate::api::CheckpointEpoch,
        ) -> Result<(), PluginError> {
            Ok(())
        }
        async fn process_checkpoint_finalizer(
            &self,
            _epoch: crate::api::CheckpointEpoch,
        ) -> Result<(), PluginError> {
            Ok(())
        }
    }

    struct RecordingSink {
        recorder: Arc<LifecycleRecorder>,
        running: AtomicBool,
    }

    impl RecordingSink {
        fn new(recorder: Arc<LifecycleRecorder>) -> Self {
            Self {
                recorder,
                running: AtomicBool::new(true),
            }
        }
    }

    #[async_trait]
    impl SupportsGracefulShutdown for RecordingSink {
        fn is_running(&self) -> bool {
            self.running.load(Ordering::SeqCst)
        }
        async fn terminate(&self) -> Result<(), PluginError> {
            self.recorder.terminated.fetch_add(1, Ordering::SeqCst);
            self.running.store(false, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl SinkPlugin for RecordingSink {
        async fn initialize(&self) -> Result<(), PluginError> {
            self.recorder.initialized.store(true, Ordering::SeqCst);
            Ok(())
        }
        async fn process_batch(&self, _data: RecordBatch) -> Result<(), PluginError> {
            Ok(())
        }
        async fn process_checkpoint_marker(
            &self,
            _epoch: crate::api::CheckpointEpoch,
        ) -> Result<(), PluginError> {
            Ok(())
        }
        async fn process_checkpoint_finalizer(
            &self,
            _epoch: crate::api::CheckpointEpoch,
        ) -> Result<(), PluginError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn source_start_skips_initialize_on_terminate_before_init() {
        let channels = make_channels();
        let recorder = Arc::new(LifecycleRecorder::default());
        let plugin: Arc<dyn SourcePlugin> = Arc::new(RecordingSource::new(recorder.clone()));
        let dispatcher = SourcePluginDispatcher::new(channels.clone(), plugin);

        channels
            .input
            .sender
            .send(NonExhaustive::new(PluginMsg::Terminate))
            .unwrap();

        let runtime = DirectTokioProxy::new().into_async_runtime_obj();
        let result = dispatcher.start(runtime).await;

        assert!(result.is_ok(), "Terminate-before-Init should return Ok");
        assert!(
            !recorder.was_initialized(),
            "initialize() must not run when host terminates first"
        );
        assert_eq!(
            recorder.terminate_count(),
            1,
            "terminate() must be called exactly once on Terminate-before-Init"
        );
    }

    #[tokio::test]
    async fn transform_start_skips_initialize_on_terminate_before_init() {
        let channels = make_channels();
        let recorder = Arc::new(LifecycleRecorder::default());
        let plugin: Arc<dyn TransformPlugin> = Arc::new(RecordingTransform::new(recorder.clone()));
        let dispatcher = TransformPluginDispatcher::new(channels.clone(), plugin);

        channels
            .input
            .sender
            .send(NonExhaustive::new(PluginMsg::Terminate))
            .unwrap();

        let runtime = DirectTokioProxy::new().into_async_runtime_obj();
        let result = dispatcher.start(runtime).await;

        assert!(result.is_ok(), "Terminate-before-Init should return Ok");
        assert!(
            !recorder.was_initialized(),
            "initialize() must not run when host terminates first"
        );
        assert_eq!(
            recorder.terminate_count(),
            1,
            "terminate() must be called exactly once on Terminate-before-Init"
        );
    }

    /// The sink dispatcher must emit its liveness markers on the metrics
    /// channel: a heartbeat once the loop starts, and balanced enter/exit
    /// breadcrumbs around a successful checkpoint-marker hook. The host's
    /// shutdown diagnostics attribute wedged drains from exactly these.
    #[tokio::test]
    async fn sink_dispatcher_emits_liveness_markers() {
        let channels = make_channels();
        let recorder = Arc::new(LifecycleRecorder::default());
        let plugin: Arc<dyn SinkPlugin> = Arc::new(RecordingSink::new(recorder.clone()));
        let dispatcher = SinkPluginDispatcher::new(channels.clone(), plugin);

        for msg in [
            PluginMsg::Init,
            PluginMsg::CheckpointMarker {
                epoch: crate::ffi::PluginCheckpointEpoch(1),
            },
            PluginMsg::Terminate,
        ] {
            channels.input.sender.send(NonExhaustive::new(msg)).unwrap();
        }

        let runtime = DirectTokioProxy::new().into_async_runtime_obj();
        dispatcher.start(runtime).await.unwrap();

        let mut names: Vec<String> = Vec::new();
        let mut enter_hook_tag = None;
        while let Ok(metric) = channels.metrics.receiver.try_recv() {
            if let Ok(crate::ffi::PluginMetric::Count { name, tags, .. }) = metric.into_enum() {
                if name.as_str() == crate::ffi::DISPATCHER_HOOK_ENTER_METRIC {
                    enter_hook_tag = tags
                        .iter()
                        .find(|kv| kv.0.as_str() == crate::ffi::DISPATCHER_HOOK_TAG)
                        .map(|kv| kv.1.to_string());
                }
                names.push(name.to_string());
            }
        }

        let pos = |wanted: &str| names.iter().position(|n| n == wanted);
        let heartbeat = pos(crate::ffi::DISPATCHER_HEARTBEAT_METRIC);
        let enter = pos(crate::ffi::DISPATCHER_HOOK_ENTER_METRIC);
        let exit = pos(crate::ffi::DISPATCHER_HOOK_EXIT_METRIC);
        assert!(heartbeat.is_some(), "expected a heartbeat, got: {names:?}");
        assert!(
            enter.is_some() && exit.is_some() && enter < exit,
            "expected balanced enter/exit breadcrumbs, got: {names:?}"
        );
        assert_eq!(
            enter_hook_tag.as_deref(),
            Some("checkpoint_marker"),
            "enter breadcrumb must name the hook"
        );
    }

    struct FailingInitSink {
        recorder: Arc<LifecycleRecorder>,
        running: AtomicBool,
    }

    #[async_trait]
    impl SupportsGracefulShutdown for FailingInitSink {
        fn is_running(&self) -> bool {
            self.running.load(Ordering::SeqCst)
        }
        async fn terminate(&self) -> Result<(), PluginError> {
            self.recorder.terminated.fetch_add(1, Ordering::SeqCst);
            self.running.store(false, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl SinkPlugin for FailingInitSink {
        async fn initialize(&self) -> Result<(), PluginError> {
            Err(PluginError::Internal(
                "failed to check if datasource exists".to_string(),
            ))
        }
        async fn process_batch(&self, _data: RecordBatch) -> Result<(), PluginError> {
            panic!("must not be called after initialize failed");
        }
        async fn process_checkpoint_marker(
            &self,
            _epoch: crate::api::CheckpointEpoch,
        ) -> Result<(), PluginError> {
            panic!("must not be called after initialize failed");
        }
        async fn process_checkpoint_finalizer(
            &self,
            _epoch: crate::api::CheckpointEpoch,
        ) -> Result<(), PluginError> {
            panic!("must not be called after initialize failed");
        }
    }

    // The F8 contract: a failed sink must drain its input channel (dropping
    // data, swallowing markers WITHOUT acking) until Terminate, then surface
    // the original error. Exiting with a full channel wedges the host-side
    // writer and the drain rides the watchdog. An acked marker here would be
    // worse: its epoch could finalize over the dropped data.
    #[tokio::test]
    async fn sink_failure_drains_input_without_acking() {
        let channels = make_channels();
        let recorder = Arc::new(LifecycleRecorder::default());
        let plugin: Arc<dyn SinkPlugin> = Arc::new(FailingInitSink {
            recorder: recorder.clone(),
            running: AtomicBool::new(true),
        });
        let dispatcher = SinkPluginDispatcher::new(channels.clone(), plugin);

        let batch = RecordBatch::try_from_iter([(
            "v",
            Arc::new(arrow::array::Int64Array::from(vec![1_i64])) as arrow::array::ArrayRef,
        )])
        .unwrap();
        for msg in [
            PluginMsg::Init,
            PluginMsg::NextBatch { data: batch.into() },
            PluginMsg::CheckpointMarker {
                epoch: crate::ffi::PluginCheckpointEpoch(7),
            },
            PluginMsg::Terminate,
        ] {
            channels.input.sender.send(NonExhaustive::new(msg)).unwrap();
        }

        let runtime = DirectTokioProxy::new().into_async_runtime_obj();
        let result = dispatcher.start(runtime).await;

        let err = result.expect_err("the original plugin error must surface");
        assert!(
            err.to_string()
                .contains("failed to check if datasource exists"),
            "unexpected error: {err:?}"
        );
        assert!(
            channels.input.receiver.is_empty(),
            "input channel must be fully drained after failure"
        );
        while let Ok(msg) = channels.output.receiver.try_recv() {
            assert!(
                !matches!(msg.into_enum(), Ok(PluginMsg::CheckpointAck { .. })),
                "no marker may be acked after the sink failed"
            );
        }
        assert_eq!(
            recorder.terminated.load(Ordering::SeqCst),
            1,
            "terminate() should run once (best effort) when Terminate arrives"
        );
    }

    struct FailingMarkerTransform {
        recorder: Arc<LifecycleRecorder>,
        running: AtomicBool,
    }

    #[async_trait]
    impl SupportsGracefulShutdown for FailingMarkerTransform {
        fn is_running(&self) -> bool {
            self.running.load(Ordering::SeqCst)
        }
        async fn terminate(&self) -> Result<(), PluginError> {
            self.recorder.terminated.fetch_add(1, Ordering::SeqCst);
            self.running.store(false, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl TransformPlugin for FailingMarkerTransform {
        async fn initialize(&self) -> Result<(), PluginError> {
            self.recorder.initialized.store(true, Ordering::SeqCst);
            Ok(())
        }
        fn output_schema(&self) -> Result<SchemaRef, PluginError> {
            Ok(empty_schema())
        }
        async fn process_batch(&self, data: RecordBatch) -> Result<RecordBatch, PluginError> {
            Ok(data)
        }
        async fn process_checkpoint_marker(
            &self,
            _epoch: crate::api::CheckpointEpoch,
        ) -> Result<(), PluginError> {
            Err(PluginError::Internal("marker hook failed".to_string()))
        }
        async fn process_checkpoint_finalizer(
            &self,
            _epoch: crate::api::CheckpointEpoch,
        ) -> Result<(), PluginError> {
            Ok(())
        }
    }

    // The F8 contract extended to checkpoint hooks: a transform whose marker
    // hook fails must drain its input channel (not exit with it possibly
    // full) and must NOT forward the failing marker — a forwarded marker
    // would let its epoch finalize over data the failed plugin dropped.
    #[tokio::test]
    async fn transform_marker_failure_drains_input_without_forwarding() {
        let channels = make_channels();
        let recorder = Arc::new(LifecycleRecorder::default());
        let plugin: Arc<dyn TransformPlugin> = Arc::new(FailingMarkerTransform {
            recorder: recorder.clone(),
            running: AtomicBool::new(true),
        });
        let dispatcher = TransformPluginDispatcher::new(channels.clone(), plugin);

        for msg in [
            PluginMsg::Init,
            PluginMsg::CheckpointMarker {
                epoch: crate::ffi::PluginCheckpointEpoch(3),
            },
            PluginMsg::CheckpointMarker {
                epoch: crate::ffi::PluginCheckpointEpoch(4),
            },
            PluginMsg::Terminate,
        ] {
            channels.input.sender.send(NonExhaustive::new(msg)).unwrap();
        }

        let runtime = DirectTokioProxy::new().into_async_runtime_obj();
        let result = dispatcher.start(runtime).await;

        let err = result.expect_err("the original marker-hook error must surface");
        assert!(
            err.to_string().contains("marker hook failed"),
            "unexpected error: {err:?}"
        );
        assert!(
            channels.input.receiver.is_empty(),
            "input channel must be fully drained after the marker-hook failure"
        );
        while let Ok(msg) = channels.output.receiver.try_recv() {
            assert!(
                !matches!(msg.into_enum(), Ok(PluginMsg::CheckpointMarker { .. })),
                "no marker may be forwarded after the transform failed"
            );
        }
        assert_eq!(
            recorder.terminated.load(Ordering::SeqCst),
            1,
            "terminate() should run once (best effort) when Terminate arrives"
        );
    }

    #[tokio::test]
    async fn sink_start_skips_initialize_on_terminate_before_init() {
        let channels = make_channels();
        let recorder = Arc::new(LifecycleRecorder::default());
        let plugin: Arc<dyn SinkPlugin> = Arc::new(RecordingSink::new(recorder.clone()));
        let dispatcher = SinkPluginDispatcher::new(channels.clone(), plugin);

        channels
            .input
            .sender
            .send(NonExhaustive::new(PluginMsg::Terminate))
            .unwrap();

        let runtime = DirectTokioProxy::new().into_async_runtime_obj();
        let result = dispatcher.start(runtime).await;

        assert!(result.is_ok(), "Terminate-before-Init should return Ok");
        assert!(
            !recorder.was_initialized(),
            "initialize() must not run when host terminates first"
        );
        assert_eq!(
            recorder.terminate_count(),
            1,
            "terminate() must be called exactly once on Terminate-before-Init"
        );
    }

    // ------------------------------------------------------------------
    // An idle dispatcher must park, not spin.
    //
    // The empty-input arm used to call `yield_now()`, which reschedules the
    // task immediately. A live pipeline's input channel is empty almost all
    // the time, so that turned every plugin node into a runtime-wide spin
    // loop (~0.29 cores per transform node with no data flowing). Guard the
    // contract rather than the CPU number: the empty path awaits the
    // runtime's timer and never `yield_now`.
    // ------------------------------------------------------------------

    use crate::r#async::{PluginAsyncRuntime, PluginAsyncRuntime_TO};
    use abi_stable::derive_macro_reexports::{RResult, TD_Opaque};
    use abi_stable::std_types::RDuration;
    use async_ffi::FfiFuture;

    #[derive(Clone, Default)]
    struct RuntimeCalls {
        sleeps: Arc<AtomicUsize>,
        yields: Arc<AtomicUsize>,
    }

    /// Delegates to the real runtime, recording *how* the caller waited.
    #[derive(Clone)]
    struct CountingRuntime {
        inner: DirectTokioProxy,
        calls: RuntimeCalls,
    }

    impl PluginAsyncRuntime for CountingRuntime {
        fn spawn(&self, fut: FfiFuture<()>) -> FfiFuture<()> {
            self.inner.spawn(fut)
        }
        fn sleep(&self, dur: RDuration) -> FfiFuture<()> {
            self.calls.sleeps.fetch_add(1, Ordering::SeqCst);
            self.inner.sleep(dur)
        }
        fn timeout(&self, dur: RDuration, fut: FfiFuture<()>) -> FfiFuture<RResult<(), ()>> {
            self.inner.timeout(dur, fut)
        }
        fn block_on(&self, fut: FfiFuture<()>) {
            self.inner.block_on(fut)
        }
        fn yield_now(&self) -> FfiFuture<()> {
            self.calls.yields.fetch_add(1, Ordering::SeqCst);
            self.inner.yield_now()
        }
    }

    fn counting_runtime(calls: &RuntimeCalls) -> PluginAsyncRuntimeObj {
        PluginAsyncRuntime_TO::from_value(
            CountingRuntime {
                inner: DirectTokioProxy::new(),
                calls: calls.clone(),
            },
            TD_Opaque,
        )
    }

    fn assert_parked_not_spun(calls: &RuntimeCalls) {
        assert_eq!(
            calls.yields.load(Ordering::SeqCst),
            0,
            "empty input must not busy-yield: it spins every runtime worker"
        );
        assert!(
            calls.sleeps.load(Ordering::SeqCst) > 0,
            "empty input must park on the runtime timer"
        );
    }

    /// Runs the loop against an empty input channel for 20ms, then stops it.
    async fn stop_after_idle_window(running: &AtomicBool) {
        tokio::time::sleep(Duration::from_millis(20)).await;
        running.store(false, Ordering::SeqCst);
    }

    #[tokio::test]
    async fn transform_dispatcher_parks_instead_of_spinning_on_empty_input() {
        let channels = make_channels();
        let recorder = Arc::new(LifecycleRecorder::default());
        let plugin = Arc::new(RecordingTransform::new(recorder));
        let dispatcher = TransformPluginDispatcher::new(
            channels.clone(),
            plugin.clone() as Arc<dyn TransformPlugin>,
        );

        channels
            .input
            .sender
            .send(NonExhaustive::new(PluginMsg::Init))
            .unwrap();

        let calls = RuntimeCalls::default();
        let (result, ()) = tokio::join!(
            dispatcher.start(counting_runtime(&calls)),
            stop_after_idle_window(&plugin.running)
        );
        result.expect("dispatcher should exit cleanly once the plugin stops");

        assert_parked_not_spun(&calls);
    }

    #[tokio::test]
    async fn sink_dispatcher_parks_instead_of_spinning_on_empty_input() {
        let channels = make_channels();
        let recorder = Arc::new(LifecycleRecorder::default());
        let plugin = Arc::new(RecordingSink::new(recorder));
        let dispatcher =
            SinkPluginDispatcher::new(channels.clone(), plugin.clone() as Arc<dyn SinkPlugin>);

        channels
            .input
            .sender
            .send(NonExhaustive::new(PluginMsg::Init))
            .unwrap();

        let calls = RuntimeCalls::default();
        let (result, ()) = tokio::join!(
            dispatcher.start(counting_runtime(&calls)),
            stop_after_idle_window(&plugin.running)
        );
        result.expect("dispatcher should exit cleanly once the plugin stops");

        assert_parked_not_spun(&calls);
    }
}
