//! Proves a source dispatcher still exits when the host has stopped reading
//! its output channel.
//!
//! `SourcePluginDispatcher::start` ends by telling the host it is done, via
//! `send_with_retry`. That retry could previously only stop on `Ok` or
//! `Disconnected` — and `Disconnected` is unreachable, because `PluginChannel`
//! owns both the sender and the receiver. So against a full channel it spun at
//! 50ms forever and `start()` never returned.
//!
//! That is exactly the drain-time shape: the host's source forwarder stops
//! reading this channel for good once it has forwarded the batches it
//! snapshotted, while the dispatcher keeps producing until it dequeues
//! `Terminate`. The dispatcher then wedged, the host's plugin drain waited out
//! its entire budget, and every plugin was reported unflushed — when in truth
//! the flush had already happened.
//!
//! Lives in `tests/` (its own process) on purpose: the shutdown signal is a
//! one-way process-global, so flipping it here cannot contaminate the SDK's
//! unit tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use abi_stable::derive_macro_reexports::TD_Opaque;
use abi_stable::external_types::crossbeam_channel;
use abi_stable::nonexhaustive_enum::NonExhaustive;
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_ffi::{FfiFuture, FutureExt as _};
use async_trait::async_trait;
use streamling_plugin::api::SupportsGracefulShutdown;
use streamling_plugin::ffi::PluginMetricsChannel;
use streamling_plugin::shutdown::{self, ShutdownSignal, ShutdownSignal_TO};
use streamling_plugin::{
    CheckpointEpoch, PluginChannel, PluginChannels, PluginError, PluginMsg, SourcePlugin,
    SourcePluginDispatcher,
};

/// Host-side stand-in. `remaining_budget_ms` counts down from a fixed start so
/// the bound under test is exercised the way the real host drives it.
#[derive(Clone)]
struct TestHostSignal {
    down: Arc<AtomicBool>,
    budget_ms: Arc<std::sync::atomic::AtomicU64>,
}

impl ShutdownSignal for TestHostSignal {
    fn is_shutting_down(&self) -> bool {
        self.down.load(Ordering::SeqCst)
    }
    fn cancelled(&self) -> FfiFuture<()> {
        let down = self.down.clone();
        async move {
            while !down.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        .into_ffi()
    }
    fn remaining_budget_ms(&self) -> u64 {
        self.budget_ms.load(Ordering::SeqCst)
    }
    fn request_shutdown(&self) {
        self.down.store(true, Ordering::SeqCst);
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]))
}

/// Produces nothing and stops as soon as it is told to. The dispatcher's exit
/// path is what is under test, not the plugin's behaviour.
struct QuietSource {
    running: AtomicBool,
}

#[async_trait]
impl SupportsGracefulShutdown for QuietSource {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl SourcePlugin for QuietSource {
    async fn initialize(&self) -> Result<(), PluginError> {
        Ok(())
    }
    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(schema())
    }
    async fn generate_batch(&self) -> Result<RecordBatch, PluginError> {
        Ok(RecordBatch::new_empty(schema()))
    }
    async fn process_checkpoint_marker(&self, _epoch: CheckpointEpoch) -> Result<(), PluginError> {
        Ok(())
    }
    async fn process_checkpoint_finalizer(
        &self,
        _epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        Ok(())
    }
}

/// The drain shape: output channel full and nobody reading it, `Terminate`
/// already queued, shutdown in progress. The dispatcher must still return.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_dispatcher_exits_when_the_host_stopped_reading_its_output() {
    let down = Arc::new(AtomicBool::new(false));
    let budget_ms = Arc::new(std::sync::atomic::AtomicU64::new(20_000));
    shutdown::install_shutdown_signal(ShutdownSignal_TO::from_value(
        TestHostSignal {
            down: down.clone(),
            budget_ms: budget_ms.clone(),
        },
        TD_Opaque,
    ));

    let chans = PluginChannels {
        input: PluginChannel::new(crossbeam_channel::bounded(8)),
        output: PluginChannel::new(crossbeam_channel::bounded(1)),
        metrics: PluginMetricsChannel::new(crossbeam_channel::bounded(8)),
    };

    // Fill the single output slot and never read it again — the host-side
    // forwarder having stopped after draining its snapshot.
    chans
        .output
        .sender
        .send(NonExhaustive::new(PluginMsg::Init))
        .unwrap();

    let input_sender = chans.input.sender.clone();
    input_sender
        .send(NonExhaustive::new(PluginMsg::Init))
        .unwrap();
    input_sender
        .send(NonExhaustive::new(PluginMsg::Terminate))
        .unwrap();

    // The host flips this out of band at SIGTERM.
    down.store(true, Ordering::SeqCst);

    let source = Arc::new(QuietSource {
        running: AtomicBool::new(true),
    });
    let dispatcher = SourcePluginDispatcher::new(chans, source);
    let runtime = streamling_plugin::r#async::DirectTokioProxy::new().into_async_runtime_obj();

    // Wind the host's budget down the way a real drain does, so the bound has
    // something to observe. Without the fix the dispatcher never returns and
    // this test fails on the timeout regardless of the budget.
    let winder = tokio::spawn(async move {
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let left = budget_ms.load(Ordering::SeqCst);
            budget_ms.store(left.saturating_sub(500), Ordering::SeqCst);
        }
    });

    let finished = tokio::time::timeout(
        Duration::from_secs(20),
        tokio::spawn(async move { dispatcher.start(runtime).await }),
    )
    .await;
    winder.abort();

    let joined = finished.expect(
        "the source dispatcher never returned: its completion notice is retrying against a \
         channel nobody is reading, so the host's plugin drain will wait out its whole budget \
         and then report a plugin that had already flushed as unflushed",
    );
    let result = joined.expect("dispatcher task panicked");
    assert!(
        result.is_ok(),
        "abandoning a best-effort completion notice must not fail the dispatcher: the host \
         reads the missing notice as 'not drained', which is already the safe outcome — the \
         epoch stays unacked and the tail replays. Got {result:?}"
    );
}
