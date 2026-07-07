use datafusion::execution::RecordBatchStream;
use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    common::DataFusionError,
    error::Result as DFResult,
    physical_plan::SendableRecordBatchStream,
};
use futures::StreamExt;
use futures::future;
use futures::stream::Stream;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::mpsc::{Receiver, Sender, channel, error::TrySendError};
use tracing::{error, info, warn};

/// A batch stuck undelivered longer than this is treated as unrecoverable and
/// triggers a process restart. Generous on purpose: a sink that briefly stalls
/// (reconnecting, a slow write) self-heals well within this window, so a single
/// transient hiccup never restarts the whole shared source.
// ponytail: fixed 5min window; make it a config/env knob if some sink legitimately
// needs longer to recover.
const DEFAULT_STALL_DEADLINE: Duration = Duration::from_secs(300);

/// How often the watchdog checks for a stalled delivery.
const WATCHDOG_POLL_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub struct BroadcastStream {
    inner: Arc<BroadcastState>,
    stopped: Arc<AtomicBool>,
    channel_capacity: usize,
    /// Restart if a single fan-out delivery stays blocked longer than this.
    stall_deadline: Duration,
    /// `Some(t)` while a batch is mid-delivery (t = when it started); `None` when
    /// the loop is idle waiting on the source. Lets the watchdog tell a genuine
    /// wedge apart from a source that simply has no new data.
    in_flight: Arc<Mutex<Option<Instant>>>,
}

#[derive(Debug)]
struct BroadcastState {
    schema: SchemaRef,
    consumers: Mutex<Vec<Sender<DFResult<RecordBatch>>>>,
}

impl BroadcastStream {
    /// Create a new broadcast handle without starting the background task.
    /// Call `start()` after adding all consumers to avoid dropping batches.
    pub fn new(schema: SchemaRef, channel_capacity: usize) -> Self {
        Self::with_stall_deadline(schema, channel_capacity, DEFAULT_STALL_DEADLINE)
    }

    /// Same as `new`, with an explicit stall deadline (used by tests).
    pub(crate) fn with_stall_deadline(
        schema: SchemaRef,
        channel_capacity: usize,
        stall_deadline: Duration,
    ) -> Self {
        BroadcastStream {
            inner: Arc::new(BroadcastState {
                schema,
                consumers: Mutex::new(Vec::new()),
            }),
            stopped: Arc::new(AtomicBool::new(false)),
            channel_capacity,
            stall_deadline,
            in_flight: Arc::new(Mutex::new(None)),
        }
    }

    /// Start the background broadcasting task plus a watchdog that forces a
    /// restart if the fan-out gets wedged. Call after all consumers are added.
    pub fn start(&self, source_stream: SendableRecordBatchStream) {
        let clone_for_task = self.clone();
        tokio::spawn(async move {
            clone_for_task.run_broadcast(source_stream).await;
        });

        let in_flight = self.in_flight.clone();
        let stopped = self.stopped.clone();
        let deadline = self.stall_deadline;
        tokio::spawn(async move {
            Self::run_watchdog(in_flight, stopped, deadline, WATCHDOG_POLL_INTERVAL, || {
                // The shared source can't make progress and this is unrecoverable
                // in-process (a wedged consumer never drains). Exit so the
                // orchestrator restarts us from the last checkpoint.
                std::process::exit(1);
            })
            .await;
        });
    }

    /// Retry sending with fixed delay until success or channel closed.
    async fn try_send_batch_with_retry_forever(
        tx: &Sender<DFResult<RecordBatch>>,
        batch_result: &DFResult<RecordBatch>,
    ) -> Result<(), ()> {
        loop {
            let to_send = match batch_result {
                Ok(batch) => Ok(batch.clone()),
                Err(e) => Err(Self::clone_df_error(e)),
            };

            match tx.try_send(to_send) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(_)) => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                }
                Err(TrySendError::Closed(_)) => {
                    return Err(());
                }
            }
        }
    }

    /// The task that reads from the single source stream and broadcasts to all active consumers.
    async fn run_broadcast(&self, mut source_stream: SendableRecordBatchStream) {
        loop {
            if self.stopped.load(Ordering::SeqCst) {
                break;
            }

            match source_stream.next().await {
                Some(batch_result) => {
                    // Mark the delivery in-flight so the watchdog can tell a wedge
                    // (a batch we can't hand off) apart from an idle source.
                    *self.in_flight.lock() = Some(Instant::now());

                    // Concurrent retry sends to avoid deadlocks during consumer startup.
                    let consumers = self.inner.consumers.lock().clone();
                    let results = future::join_all(
                        consumers
                            .iter()
                            .map(|tx| Self::try_send_batch_with_retry_forever(tx, &batch_result)),
                    )
                    .await;

                    *self.in_flight.lock() = None;

                    // A closed consumer's branch already ended; actually prune it so
                    // it stops being retried and re-warned on every batch.
                    if results.iter().any(|r| r.is_err()) {
                        warn!(
                            "Consumer channel closed outside shutdown; removing it from the \
                             broadcast. A downstream branch ended abnormally."
                        );
                        self.inner.consumers.lock().retain(|tx| !tx.is_closed());
                    }
                }
                None => {
                    break;
                }
            }
        }

        // Stop the watchdog and let every consumer see end-of-stream.
        self.stopped.store(true, Ordering::SeqCst);
        self.inner.consumers.lock().clear(); // dropping all senders => receivers get None
    }

    /// Watch for a wedged fan-out and force a restart when it can't recover.
    ///
    /// We deliberately do NOT fail fast on a single stuck consumer. A sink that
    /// briefly stalls (reconnecting, a slow write) self-heals, and restarting the
    /// whole shared source -- every dataset it feeds -- for one transient hiccup is
    /// too aggressive. Instead we tolerate the stall for `deadline` (the head-of-
    /// line block pauses the source meanwhile); if the consumer drains in time the
    /// source resumes on its own with no restart. Only a delivery still stuck past
    /// `deadline` is treated as unrecoverable. Exiting the process also covers the
    /// all-consumers-wedged case, which an in-band error could not (no live
    /// consumer left to receive it).
    async fn run_watchdog(
        in_flight: Arc<Mutex<Option<Instant>>>,
        stopped: Arc<AtomicBool>,
        deadline: Duration,
        poll: Duration,
        on_stall: impl Fn(),
    ) {
        loop {
            tokio::time::sleep(poll).await;
            if stopped.load(Ordering::SeqCst) {
                return;
            }
            let started = *in_flight.lock();
            if let Some(t) = started {
                let stalled_for = t.elapsed();
                if stalled_for >= deadline {
                    error!(
                        stall_secs = stalled_for.as_secs(),
                        "Broadcast fan-out stuck past the stall deadline (a consumer stopped \
                         draining and did not recover); forcing a restart."
                    );
                    on_stall();
                    return;
                }
            }
        }
    }

    /// Add a new consumer. Returns a handle that can receive from this broadcast.
    pub fn add_consumer(&self) -> BroadcastConsumer {
        // Each consumer gets its own bounded receiver
        let (tx, rx) = channel(self.channel_capacity);

        let mut consumers = self.inner.consumers.lock();
        consumers.push(tx);

        BroadcastConsumer {
            schema: self.inner.schema.clone(),
            receiver: rx,
        }
    }

    /// Reconstruct a `DataFusionError` preserving the variant kind so that
    /// user-facing / internal classification survives the broadcast fan-out.
    /// `DataFusionError` does not implement `Clone`, so we rebuild it from its
    /// string representation while keeping the discriminant.
    fn clone_df_error(err: &DataFusionError) -> DataFusionError {
        match err {
            DataFusionError::Plan(msg) => DataFusionError::Plan(msg.clone()),
            DataFusionError::NotImplemented(msg) => DataFusionError::NotImplemented(msg.clone()),
            DataFusionError::Internal(msg) => DataFusionError::Internal(msg.clone()),
            DataFusionError::Execution(msg) => DataFusionError::Execution(msg.clone()),
            DataFusionError::External(boxed) => {
                if let Some(se) = (**boxed).downcast_ref::<crate::error::StreamlingError>() {
                    DataFusionError::External(Box::new(se.clone_flags_with_message()))
                } else {
                    warn!(
                        error = %boxed,
                        "clone_df_error: External error is not a StreamlingError; \
                         collapsing to Execution(String), which changes error classification"
                    );
                    DataFusionError::Execution(boxed.to_string())
                }
            }
            DataFusionError::Configuration(msg) => DataFusionError::Configuration(msg.clone()),
            DataFusionError::ResourcesExhausted(msg) => {
                DataFusionError::ResourcesExhausted(msg.clone())
            }
            DataFusionError::Context(ctx, inner) => {
                DataFusionError::Context(ctx.clone(), Box::new(Self::clone_df_error(inner)))
            }
            other => {
                warn!(
                    error = %other,
                    "clone_df_error: collapsing non-clonable variant to Execution(String), \
                     which changes error classification"
                );
                DataFusionError::Execution(other.to_string())
            }
        }
    }

    pub fn stop(&self) {
        info!("Stopping broadcast stream...");

        self.stopped.store(true, Ordering::SeqCst);

        // Also drop all consumers immediately
        let mut guard = self.inner.consumers.lock();
        guard.clear(); // dropping all senders => each consumer sees a `None` and shuts down
    }
}

pub struct BroadcastConsumer {
    schema: SchemaRef,
    receiver: Receiver<DFResult<RecordBatch>>,
}

impl Stream for BroadcastConsumer {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Forward poll to the underlying mpsc receiver
        Pin::new(&mut self.receiver).poll_recv(cx)
    }
}

impl RecordBatchStream for BroadcastConsumer {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clone_df_error_preserves_execution_message() {
        let original = DataFusionError::Execution("something went wrong".into());
        let cloned = BroadcastStream::clone_df_error(&original);

        match cloned {
            DataFusionError::Execution(msg) => {
                assert_eq!(msg, "something went wrong");
            }
            other => panic!("expected Execution variant, got: {other}"),
        }
    }

    #[test]
    fn clone_df_error_preserves_plan_message() {
        let original = DataFusionError::Plan("bad plan".into());
        let cloned = BroadcastStream::clone_df_error(&original);

        match cloned {
            DataFusionError::Plan(msg) => assert_eq!(msg, "bad plan"),
            other => panic!("expected Plan variant, got: {other}"),
        }
    }

    #[test]
    fn clone_df_error_preserves_not_implemented_message() {
        let original = DataFusionError::NotImplemented("not yet".into());
        let cloned = BroadcastStream::clone_df_error(&original);

        match cloned {
            DataFusionError::NotImplemented(msg) => assert_eq!(msg, "not yet"),
            other => panic!("expected NotImplemented variant, got: {other}"),
        }
    }

    #[test]
    fn clone_df_error_preserves_context_recursively() {
        let inner = DataFusionError::Execution("inner error".into());
        let original = DataFusionError::Context("outer context".into(), Box::new(inner));
        let cloned = BroadcastStream::clone_df_error(&original);

        match cloned {
            DataFusionError::Context(ctx, inner) => {
                assert_eq!(ctx, "outer context");
                match *inner {
                    DataFusionError::Execution(msg) => assert_eq!(msg, "inner error"),
                    other => panic!("expected inner Execution, got: {other}"),
                }
            }
            other => panic!("expected Context variant, got: {other}"),
        }
    }

    #[test]
    fn clone_df_error_preserves_internal_message() {
        let original = DataFusionError::Internal("invariant broken".into());
        let cloned = BroadcastStream::clone_df_error(&original);

        match cloned {
            DataFusionError::Internal(msg) => assert_eq!(msg, "invariant broken"),
            other => panic!("expected Internal variant, got: {other}"),
        }
    }

    #[test]
    fn clone_df_error_preserves_configuration_message() {
        let original = DataFusionError::Configuration("bad config".into());
        let cloned = BroadcastStream::clone_df_error(&original);

        match cloned {
            DataFusionError::Configuration(msg) => assert_eq!(msg, "bad config"),
            other => panic!("expected Configuration variant, got: {other}"),
        }
    }

    #[test]
    fn clone_df_error_preserves_resources_exhausted_message() {
        let original = DataFusionError::ResourcesExhausted("out of memory".into());
        let cloned = BroadcastStream::clone_df_error(&original);

        match cloned {
            DataFusionError::ResourcesExhausted(msg) => assert_eq!(msg, "out of memory"),
            other => panic!("expected ResourcesExhausted variant, got: {other}"),
        }
    }

    #[test]
    fn clone_df_error_warns_and_collapses_non_clonable_variant() {
        let original = DataFusionError::ArrowError(
            Box::new(datafusion::arrow::error::ArrowError::ComputeError(
                "arrow went wrong".into(),
            )),
            None,
        );
        let cloned = BroadcastStream::clone_df_error(&original);

        match cloned {
            DataFusionError::Execution(msg) => {
                assert!(
                    msg.contains("arrow went wrong"),
                    "message should contain the original error text, got: {msg}"
                );
            }
            other => panic!("expected Execution fallback, got: {other}"),
        }
    }

    #[test]
    fn clone_df_error_preserves_external_streamling_error() {
        use crate::error::StreamlingError;

        let se = StreamlingError::user("bad input").mark_retriable();
        let original = DataFusionError::External(Box::new(se));
        let cloned = BroadcastStream::clone_df_error(&original);

        let recovered = crate::error::StreamlingError::from(cloned);
        assert!(
            !recovered.is_internal(),
            "user-facing flag should survive clone"
        );
        assert!(
            recovered.is_retriable(),
            "retriable flag should survive clone"
        );
        assert_eq!(recovered.to_string(), "bad input");
    }

    #[tokio::test]
    async fn watchdog_fires_when_delivery_stuck_past_deadline() {
        // in_flight is Some and never clears, so the delivery is "stuck"; the
        // watchdog must fire once elapsed passes the (tiny) deadline.
        let in_flight = Arc::new(Mutex::new(Some(Instant::now())));
        let stopped = Arc::new(AtomicBool::new(false));
        let fired = Arc::new(AtomicBool::new(false));
        let fired_cb = fired.clone();

        let ran = tokio::time::timeout(
            Duration::from_secs(2),
            BroadcastStream::run_watchdog(
                in_flight,
                stopped,
                Duration::from_millis(1),
                Duration::from_millis(5),
                move || fired_cb.store(true, Ordering::SeqCst),
            ),
        )
        .await;

        assert!(
            ran.is_ok(),
            "watchdog should return after firing, not loop forever"
        );
        assert!(
            fired.load(Ordering::SeqCst),
            "watchdog should force a restart when a delivery is stuck past the deadline"
        );
    }

    #[tokio::test]
    async fn watchdog_ignores_idle_source() {
        // in_flight is None => the loop is idly waiting on the source, not wedged.
        let in_flight = Arc::new(Mutex::new(None));
        let stopped = Arc::new(AtomicBool::new(false));
        let fired = Arc::new(AtomicBool::new(false));
        let fired_cb = fired.clone();

        // The watchdog loops forever while idle, so this times out -- expected.
        let _ = tokio::time::timeout(
            Duration::from_millis(80),
            BroadcastStream::run_watchdog(
                in_flight,
                stopped,
                Duration::from_millis(1),
                Duration::from_millis(5),
                move || fired_cb.store(true, Ordering::SeqCst),
            ),
        )
        .await;

        assert!(
            !fired.load(Ordering::SeqCst),
            "watchdog must not fire while the source is merely idle (no batch in flight)"
        );
    }
}
