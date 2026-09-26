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
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::sync::mpsc::{Receiver, Sender, channel, error::TrySendError};
use tracing::{info, warn};

/// How long a full consumer channel keeps being retried after shutdown has
/// been requested before the broadcast drops that consumer. Sized like the
/// Kafka sink's QueueFull window: long enough for a healthy consumer to drain
/// mid-drain, short enough to stay inside the shutdown watchdog budget.
const SHUTDOWN_STALLED_CONSUMER_WINDOW: tokio::time::Duration =
    tokio::time::Duration::from_secs(10);

#[derive(Clone, Debug)]
pub struct BroadcastStream {
    inner: Arc<BroadcastState>,
    stopped: Arc<AtomicBool>,
    channel_capacity: usize,
}

#[derive(Debug)]
struct BroadcastState {
    schema: SchemaRef,
    consumers: Mutex<Vec<ConsumerSlot>>,
}

/// A registered consumer plus the flag that poisons its stream when the
/// broadcast abandons it (see `run_broadcast`'s failure handling).
#[derive(Clone, Debug)]
struct ConsumerSlot {
    tx: Sender<DFResult<RecordBatch>>,
    abandoned: Arc<AtomicBool>,
}

/// Why a send to one consumer gave up.
enum SendFailure {
    /// The consumer's receiver is gone — routine at teardown.
    Closed,
    /// The consumer stayed full through the post-shutdown stalled window; at
    /// least one batch was never delivered to it.
    Stalled,
}

impl BroadcastStream {
    /// Create a new broadcast handle without starting the background task.
    /// Call `start()` after adding all consumers to avoid dropping batches.
    pub fn new(schema: SchemaRef, channel_capacity: usize) -> Self {
        BroadcastStream {
            inner: Arc::new(BroadcastState {
                schema,
                consumers: Mutex::new(Vec::new()),
            }),
            stopped: Arc::new(AtomicBool::new(false)),
            channel_capacity,
        }
    }

    /// Start the background broadcasting task.
    /// Should be called after all consumers have been added. The driver task
    /// is tracked by `scope` (DataPath stage): `run_broadcast` ends when the
    /// source stream ends — which shutdown forces — or when every consumer is
    /// gone, so the drain ladder observes its wind-down without needing to
    /// cancel it.
    pub fn start(
        &self,
        source_stream: SendableRecordBatchStream,
        scope: &crate::shutdown::ComponentScope,
    ) {
        let clone_for_task = self.clone();
        scope.spawn(async move {
            clone_for_task.run_broadcast(source_stream).await;
        });
    }

    /// Retry sending with fixed delay until success or channel closed.
    ///
    /// Once shutdown is requested the retry window becomes bounded: a consumer
    /// that is alive-but-stalled (e.g. a sink wedged against a sick backend)
    /// used to pin the broadcast — and with it every OTHER consumer of the
    /// shared scan — forever. A healthy
    /// consumer drains its channel well within the window, so the tail keeps
    /// flowing during a normal drain; only the stalled one gets dropped.
    async fn try_send_batch_with_retry_forever(
        tx: &Sender<DFResult<RecordBatch>>,
        batch_result: &DFResult<RecordBatch>,
        shutdown: &tokio::sync::watch::Receiver<bool>,
        stalled_window: tokio::time::Duration,
    ) -> Result<(), SendFailure> {
        let mut full_since: Option<tokio::time::Instant> = None;
        loop {
            let to_send = match batch_result {
                Ok(batch) => Ok(batch.clone()),
                Err(e) => Err(Self::clone_df_error(e)),
            };

            match tx.try_send(to_send) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(_)) => {
                    let since = *full_since.get_or_insert_with(tokio::time::Instant::now);
                    if *shutdown.borrow() && since.elapsed() >= stalled_window {
                        warn!(
                            "Broadcast consumer still full {:?} after shutdown was requested; \
                             dropping it so the drain can proceed",
                            since.elapsed()
                        );
                        return Err(SendFailure::Stalled);
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                }
                Err(TrySendError::Closed(_)) => {
                    return Err(SendFailure::Closed);
                }
            }
        }
    }

    /// The task that reads from the single source stream and broadcasts to all active consumers.
    async fn run_broadcast(&self, mut source_stream: SendableRecordBatchStream) {
        let shutdown = crate::shutdown::subscribe();
        loop {
            if self.stopped.load(Ordering::SeqCst) {
                break;
            }

            match source_stream.next().await {
                Some(batch_result) => {
                    // Concurrent retry sends to avoid deadlocks during consumer startup
                    let consumers = self.inner.consumers.lock().unwrap().clone();
                    let send_futures: Vec<_> = consumers
                        .iter()
                        .map(|slot| {
                            Self::try_send_batch_with_retry_forever(
                                &slot.tx,
                                &batch_result,
                                &shutdown,
                                SHUTDOWN_STALLED_CONSUMER_WINDOW,
                            )
                        })
                        .collect();

                    let results = future::join_all(send_futures).await;
                    // A failed consumer must actually be REMOVED, in both
                    // arms. Leaving it registered meant every subsequent
                    // batch paid the full stalled window again (one wedged
                    // sink × N queued batches could out-wait the whole
                    // shutdown budget), and — worse — a sink that unwedged
                    // AFTER a batch was skipped could drain the later
                    // batches plus the terminal marker, ack the terminal
                    // epoch, and let offsets commit over the gap it never
                    // received. The abandoned flag is set BEFORE the sender
                    // is dropped so the consumer's stream ends in an error
                    // (see BroadcastConsumer::poll_next): a failed sink is
                    // never deregistered, so its missing ack keeps the
                    // epoch from finalizing and the gap replays on restart.
                    let mut failed: Vec<ConsumerSlot> = Vec::new();
                    for (slot, result) in consumers.iter().zip(results) {
                        match result {
                            Ok(()) => {}
                            Err(SendFailure::Closed) => {
                                warn!(
                                    "Consumer channel closed, removing from broadcast. If this happens outside of a shutdown, this is a bug."
                                );
                                failed.push(slot.clone());
                            }
                            Err(SendFailure::Stalled) => {
                                slot.abandoned.store(true, Ordering::SeqCst);
                                warn!(
                                    "Abandoning the stalled broadcast consumer: its stream will end in an error so its epochs cannot finalize over the missed batch(es); the tail replays on restart"
                                );
                                failed.push(slot.clone());
                            }
                        }
                    }
                    if !failed.is_empty() {
                        let mut guard = self.inner.consumers.lock().unwrap();
                        guard.retain(|live| !failed.iter().any(|f| f.tx.same_channel(&live.tx)));
                    }
                }
                None => {
                    break;
                }
            }
        }

        // Cleanup: clear all consumers so they see an end-of-stream
        let mut guard = self.inner.consumers.lock().unwrap();
        guard.clear(); // dropping all senders => all receivers get None
    }

    /// Add a new consumer. Returns a handle that can receive from this broadcast.
    pub fn add_consumer(&self) -> BroadcastConsumer {
        // Each consumer gets its own bounded receiver
        let (tx, rx) = channel(self.channel_capacity);
        let abandoned = Arc::new(AtomicBool::new(false));

        let mut consumers = self.inner.consumers.lock().unwrap();
        consumers.push(ConsumerSlot {
            tx,
            abandoned: abandoned.clone(),
        });

        BroadcastConsumer {
            schema: self.inner.schema.clone(),
            receiver: rx,
            abandoned,
            abandonment_reported: false,
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
        let mut guard = self.inner.consumers.lock().unwrap();
        guard.clear(); // dropping all senders => each consumer sees a `None` and shuts down
    }
}

pub struct BroadcastConsumer {
    schema: SchemaRef,
    receiver: Receiver<DFResult<RecordBatch>>,
    /// Set by the broadcast BEFORE it drops this consumer's sender when it
    /// gives up on a stalled channel: at least one batch was never delivered,
    /// so the stream must end in an ERROR, never cleanly. A clean end would
    /// let the sink flush, complete Ok, and be deregistered from the
    /// expected-ack set — after which the terminal epoch could finalize and
    /// commit offsets over the batch this consumer never received. Failing
    /// the stream keeps the sink's future erroring instead: failed sinks are
    /// never deregistered, their missing acks stall the epoch, offsets stay
    /// uncommitted, and the gap replays on restart (at-least-once holds).
    abandoned: Arc<AtomicBool>,
    abandonment_reported: bool,
}

impl Stream for BroadcastConsumer {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Forward poll to the underlying mpsc receiver. On end-of-channel,
        // surface the abandonment (exactly once) instead of a clean end —
        // see the `abandoned` field for why this is load-bearing.
        match Pin::new(&mut self.receiver).poll_recv(cx) {
            Poll::Ready(None)
                if self.abandoned.load(Ordering::SeqCst) && !self.abandonment_reported =>
            {
                self.abandonment_reported = true;
                Poll::Ready(Some(Err(DataFusionError::Execution(
                    "broadcast abandoned this consumer: it stayed stalled through the \
                     post-shutdown window, so at least one batch was never delivered; \
                     failing the stream so its epochs cannot finalize over the gap \
                     (the missed tail replays on restart)"
                        .to_string(),
                ))))
            }
            other => other,
        }
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

    fn empty_batch() -> DFResult<RecordBatch> {
        let schema = Arc::new(arrow_schema::Schema::empty());
        Ok(RecordBatch::new_empty(schema))
    }

    /// The abandonment contract: a consumer the broadcast gave up on must
    /// still receive everything that WAS queued, and then end in an ERROR —
    /// never cleanly. A clean end would let the sink complete Ok, be
    /// deregistered, and the terminal epoch commit offsets over the batch
    /// this consumer never received.
    #[tokio::test]
    async fn abandoned_consumer_drains_queue_then_errors() {
        let schema = Arc::new(arrow_schema::Schema::empty());
        let bs = BroadcastStream::new(schema, 1);
        let mut consumer = bs.add_consumer();

        {
            let slots = bs.inner.consumers.lock().unwrap();
            slots[0]
                .tx
                .try_send(empty_batch())
                .expect("queue one batch");
            // Order matters: flag BEFORE the sender drops, exactly as
            // run_broadcast does it.
            slots[0].abandoned.store(true, Ordering::SeqCst);
        }
        bs.inner.consumers.lock().unwrap().clear(); // drop the sender

        let first = consumer.next().await;
        assert!(
            matches!(first, Some(Ok(_))),
            "the queued batch must still be delivered: {first:?}"
        );
        let second = consumer.next().await;
        match second {
            Some(Err(e)) => assert!(
                e.to_string().contains("broadcast abandoned this consumer"),
                "unexpected error: {e}"
            ),
            other => panic!("abandoned consumer must end in an error, got {other:?}"),
        }
        assert!(
            consumer.next().await.is_none(),
            "after the abandonment error the stream ends"
        );
    }

    /// Without abandonment, end-of-channel stays a clean end (healthy drain).
    #[tokio::test]
    async fn non_abandoned_consumer_ends_cleanly() {
        let schema = Arc::new(arrow_schema::Schema::empty());
        let bs = BroadcastStream::new(schema, 1);
        let mut consumer = bs.add_consumer();
        bs.inner.consumers.lock().unwrap().clear();
        assert!(consumer.next().await.is_none());
    }

    /// Regression: a consumer that is
    /// alive-but-stalled pinned the broadcast retry loop forever — and with it
    /// every other consumer of the shared scan. Once shutdown is requested and
    /// the window elapses, the stalled consumer must be dropped.
    #[tokio::test]
    async fn stalled_consumer_dropped_after_shutdown_window() {
        let (tx, _rx) = channel::<DFResult<RecordBatch>>(1);
        tx.try_send(empty_batch()).expect("fills the channel");
        // Receiver kept alive but never drained: alive-but-stalled.

        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(true);
        let batch = empty_batch();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            BroadcastStream::try_send_batch_with_retry_forever(
                &tx,
                &batch,
                &shutdown_rx,
                tokio::time::Duration::from_millis(100),
            ),
        )
        .await
        .expect("stalled consumer must be given up on, not retried forever");
        assert!(result.is_err(), "the stalled consumer must be dropped");
    }

    /// Without shutdown the loop keeps retrying and delivers once the consumer
    /// drains — the bounded window applies only to the drain.
    #[tokio::test]
    async fn stalled_consumer_recovers_without_shutdown() {
        let (tx, mut rx) = channel::<DFResult<RecordBatch>>(1);
        tx.try_send(empty_batch()).expect("fills the channel");

        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        // Free a slot after a while, well past the (irrelevant) window.
        // Test task; not part of any pipeline drain.
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            let _ = rx.recv().await;
            // Keep the receiver alive so the channel doesn't close.
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        });

        let batch = empty_batch();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            BroadcastStream::try_send_batch_with_retry_forever(
                &tx,
                &batch,
                &shutdown_rx,
                tokio::time::Duration::from_millis(100),
            ),
        )
        .await
        .expect("send must complete once the consumer drains");
        assert!(result.is_ok(), "no shutdown => keep retrying until it fits");
    }

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
}
