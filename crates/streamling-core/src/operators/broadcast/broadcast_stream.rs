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

/// Outcome of trying to deliver one batch to a single consumer.
enum SendOutcome {
    /// Accepted into the consumer's channel.
    Sent,
    /// The consumer's receiver was dropped — that branch has already ended.
    Closed,
    /// The channel stayed full past `stuck_timeout`: the consumer is alive but
    /// not draining. `run_broadcast` treats this as fatal.
    Stuck,
}

/// A consumer that cannot accept a single batch within this budget is treated
/// as wedged (not merely slow) and triggers a pipeline restart.
// ponytail: fixed 60s budget; promote to a config/env knob only if a legitimately
// slow sink ever needs longer to accept one batch.
const DEFAULT_STUCK_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
pub struct BroadcastStream {
    inner: Arc<BroadcastState>,
    stopped: Arc<AtomicBool>,
    channel_capacity: usize,
    /// Per-batch delivery budget before a non-draining consumer is fatal.
    stuck_timeout: Duration,
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
        Self::with_stuck_timeout(schema, channel_capacity, DEFAULT_STUCK_TIMEOUT)
    }

    /// Same as `new`, with an explicit stuck-consumer timeout (used by tests).
    pub(crate) fn with_stuck_timeout(
        schema: SchemaRef,
        channel_capacity: usize,
        stuck_timeout: Duration,
    ) -> Self {
        BroadcastStream {
            inner: Arc::new(BroadcastState {
                schema,
                consumers: Mutex::new(Vec::new()),
            }),
            stopped: Arc::new(AtomicBool::new(false)),
            channel_capacity,
            stuck_timeout,
        }
    }

    /// Start the background broadcasting task.
    /// Should be called after all consumers have been added.
    pub fn start(&self, source_stream: SendableRecordBatchStream) {
        let clone_for_task = self.clone();
        tokio::spawn(async move {
            clone_for_task.run_broadcast(source_stream).await;
        });
    }

    /// Retry sending until the batch is accepted, the channel closes, or the
    /// per-batch `stuck_timeout` elapses. A channel that never drains resolves
    /// to `SendOutcome::Stuck` instead of blocking the fan-out forever.
    async fn try_send_batch_bounded(
        tx: &Sender<DFResult<RecordBatch>>,
        batch_result: &DFResult<RecordBatch>,
        stuck_timeout: Duration,
    ) -> SendOutcome {
        let deadline = Instant::now() + stuck_timeout;
        loop {
            let to_send = match batch_result {
                Ok(batch) => Ok(batch.clone()),
                Err(e) => Err(Self::clone_df_error(e)),
            };

            match tx.try_send(to_send) {
                Ok(()) => return SendOutcome::Sent,
                Err(TrySendError::Full(_)) => {
                    if Instant::now() >= deadline {
                        return SendOutcome::Stuck;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Err(TrySendError::Closed(_)) => return SendOutcome::Closed,
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
                    // Concurrent bounded sends so a slow consumer at startup can't deadlock.
                    let consumers = self.inner.consumers.lock().clone();
                    let outcomes = future::join_all(consumers.iter().map(|tx| {
                        Self::try_send_batch_bounded(tx, &batch_result, self.stuck_timeout)
                    }))
                    .await;

                    // A consumer full past the timeout is wedged, not slow. One
                    // stuck consumer would otherwise freeze the whole shared source
                    // (head-of-line block, BDA: prod-oasis-consensus-raw-source), so
                    // fail the pipeline and let the process exit non-zero -> restart.
                    if outcomes.iter().any(|o| matches!(o, SendOutcome::Stuck)) {
                        error!(
                            timeout_secs = self.stuck_timeout.as_secs(),
                            "Broadcast consumer stopped draining past the stuck-timeout; \
                             failing the pipeline to force a restart."
                        );
                        self.fail_consumers().await;
                        break;
                    }

                    // A closed consumer's branch already ended (its own error, if
                    // any, propagated through its own sink). Actually prune it so it
                    // stops being retried and re-warned on every subsequent batch.
                    if !self.stopped.load(Ordering::SeqCst)
                        && outcomes.iter().any(|o| matches!(o, SendOutcome::Closed))
                    {
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

        // Cleanup: clear all consumers so they see an end-of-stream.
        self.inner.consumers.lock().clear(); // dropping all senders => receivers get None
    }

    /// Deliver a fatal error to every consumer that can still receive it. A
    /// healthy (draining) consumer accepts it and its sink propagates the error,
    /// tearing the pipeline down (non-zero exit -> orchestrator restart). A stuck
    /// consumer times out here and is dropped by the caller's cleanup.
    // ponytail: best-effort — if *every* consumer is wedged simultaneously none
    // can receive it; that all-stuck case is the watchdog's job, not this path.
    async fn fail_consumers(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        let consumers = self.inner.consumers.lock().clone();
        let sends = consumers.iter().map(|tx| async move {
            let err = DataFusionError::Execution(
                "broadcast: a consumer stopped draining; failing the pipeline to force a restart"
                    .to_string(),
            );
            let _ = tokio::time::timeout(self.stuck_timeout, tx.send(Err(err))).await;
        });
        future::join_all(sends).await;
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

    // Regression for the June-29 prod-oasis-consensus-raw-source wedge: one
    // consumer that stops draining must not freeze the fan-out. The healthy
    // consumer should be failed (fatal error) rather than starved forever.
    #[tokio::test]
    async fn stuck_consumer_fails_pipeline_instead_of_starving_others() {
        use datafusion::arrow::datatypes::Schema;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::stream;
        use std::time::Duration;

        let schema: SchemaRef = Arc::new(Schema::empty());
        let batches: Vec<DFResult<RecordBatch>> = (0..1000)
            .map(|_| Ok(RecordBatch::new_empty(schema.clone())))
            .collect();
        let source: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(batches),
        ));

        // Short stuck-timeout so the test doesn't wait the 60s production budget.
        let broadcast =
            BroadcastStream::with_stuck_timeout(schema.clone(), 1, Duration::from_millis(200));
        let mut consumer_a = broadcast.add_consumer(); // healthy, drained below
        let _consumer_b = broadcast.add_consumer(); // never drained -> goes Stuck
        broadcast.start(source);

        // Drain A to completion. It must terminate (no head-of-line hang) and see
        // the fatal error before end-of-stream.
        let outcome = tokio::time::timeout(Duration::from_secs(5), async {
            let mut saw_err = false;
            while let Some(item) = consumer_a.next().await {
                saw_err |= item.is_err();
            }
            saw_err
        })
        .await;

        match outcome {
            Ok(saw_err) => assert!(
                saw_err,
                "healthy consumer A should receive a fatal error when sibling B wedges"
            ),
            Err(_) => panic!("broadcast still head-of-line blocked: healthy consumer A hung"),
        }
    }
}
