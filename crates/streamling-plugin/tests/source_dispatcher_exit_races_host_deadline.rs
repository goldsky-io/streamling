//! Repro: a source dispatcher that emits a batch per iteration outlives the
//! host's plugin drain after Terminate (prod: "Plugin drain exceeded ...").

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

/// Mimics `SolanaBlocksSource`: `generate_batch` returns an (often empty)
/// batch every `batch_interval`, so the dispatcher sends a message every
/// iteration, and `terminate()` only flips `running`.
struct TickingSource {
    running: AtomicBool,
}

#[async_trait]
impl SupportsGracefulShutdown for TickingSource {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
    async fn terminate(&self) -> Result<(), PluginError> {
        eprintln!("terminate() called (main loop will break at next is_running check)");
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl SourcePlugin for TickingSource {
    async fn initialize(&self) -> Result<(), PluginError> {
        Ok(())
    }
    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(schema())
    }
    async fn generate_batch(&self) -> Result<RecordBatch, PluginError> {
        tokio::time::sleep(Duration::from_millis(50)).await;
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

/// Prod shape (streamling-prod 2026-11-05): budget 20s from SIGTERM, host
/// teardown deadline = budget - 2s, Solana output cap 1, host stops reading
/// the output channel at SIGTERM ("draining 0 in-flight"), Terminate sent a
/// few ms later. Scaled to a 6s budget. The dispatcher must be gone well
/// before the host's deadline; it must not race it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_dispatcher_exits_promptly_after_terminate() {
    const BUDGET: Duration = Duration::from_secs(6);
    const HOST_MARGIN: Duration = Duration::from_secs(2); // lib.rs teardown deadline margin
    let host_deadline = BUDGET - HOST_MARGIN;

    let down = Arc::new(AtomicBool::new(false));
    let budget_ms = Arc::new(std::sync::atomic::AtomicU64::new(BUDGET.as_millis() as u64));
    shutdown::install_shutdown_signal(ShutdownSignal_TO::from_value(
        TestHostSignal {
            down: down.clone(),
            budget_ms: budget_ms.clone(),
        },
        TD_Opaque,
    ));

    let chans = PluginChannels {
        input: PluginChannel::new(crossbeam_channel::bounded(50)),
        output: PluginChannel::new(crossbeam_channel::bounded(1)), // set_plugin_output_buffer!("solana_source", 1)
        metrics: PluginMetricsChannel::new(crossbeam_channel::bounded(4096)),
    };
    let input_sender = chans.input.sender.clone();
    input_sender
        .send(NonExhaustive::new(PluginMsg::Init))
        .unwrap();

    let source = Arc::new(TickingSource {
        running: AtomicBool::new(true),
    });
    let output_rx = chans.output.receiver.clone();
    let dispatcher = SourcePluginDispatcher::new(chans, source);
    let runtime = streamling_plugin::r#async::DirectTokioProxy::new().into_async_runtime_obj();
    let handle = tokio::spawn(async move { dispatcher.start(runtime).await });

    // SIGTERM: the host stops reading the output channel (nobody reads it in
    // this test) and flips the out-of-band signal; budget counts down in real time.
    let sigterm = std::time::Instant::now();
    // Control: a host that keeps reading must see a prompt exit.
    if std::env::var("CONTROL_HOST_KEEPS_READING").is_ok() {
        std::thread::spawn(move || while output_rx.recv().is_ok() {});
    }
    down.store(true, Ordering::SeqCst);
    let winder = {
        let budget_ms = budget_ms.clone();
        tokio::spawn(async move {
            loop {
                let left = BUDGET.saturating_sub(sigterm.elapsed());
                budget_ms.store(left.as_millis() as u64, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    };
    // Host: sinks drained, Terminate sent (prod: SIGTERM + 3ms .. +1.1s).
    tokio::time::sleep(Duration::from_millis(200)).await;
    input_sender
        .send(NonExhaustive::new(PluginMsg::Terminate))
        .unwrap();

    let res = tokio::time::timeout(host_deadline.saturating_sub(sigterm.elapsed()), handle).await;
    let exited_at = sigterm.elapsed();
    winder.abort();
    assert!(
        res.is_ok(),
        "dispatcher still running at the host teardown deadline ({host_deadline:?} after SIGTERM, \
         Terminate sent at +200ms): it is parked in the final `Source complete` send_with_retry \
         against a full output channel the host no longer reads; that retry only gives up when \
         remaining_budget <= DRAIN_SEND_RESERVE (2s), i.e. exactly at the host deadline"
    );
    eprintln!("dispatcher exited {exited_at:?} after SIGTERM");
}
