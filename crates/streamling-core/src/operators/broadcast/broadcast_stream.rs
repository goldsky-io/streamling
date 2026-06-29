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
use std::time::{Duration, Instant};
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::sync::mpsc::{Receiver, Sender, channel, error::TrySendError};
use tracing::{info, warn};

/// Component id under which multi-sink fan-out backpressure attribution is
/// emitted. The blocked-send counter carries this as its `id` tag and the slow
/// sink's reference name as the `downstream_id` tag.
const BROADCAST_COMPONENT_ID: &str = "multi_sink_broadcast";

#[derive(Clone, Debug)]
pub struct BroadcastStream {
    inner: Arc<BroadcastState>,
    stopped: Arc<AtomicBool>,
    channel_capacity: usize,
}

#[derive(Debug)]
struct BroadcastState {
    schema: SchemaRef,
    consumers: Mutex<Vec<BroadcastSender>>,
}

/// A registered broadcast consumer's sending half, paired with the identity of
/// the downstream node it feeds. `downstream_id` is the sink's reference name
/// for multi-sink fan-out (used to attribute blocked-send time to the slow
/// sink) and empty for consumers where attribution does not apply (the
/// MultiSinkExec passthrough output and scan-sharing fan-out).
#[derive(Clone, Debug)]
struct BroadcastSender {
    downstream_id: String,
    tx: Sender<DFResult<RecordBatch>>,
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
    /// Should be called after all consumers have been added.
    pub fn start(&self, source_stream: SendableRecordBatchStream) {
        let clone_for_task = self.clone();
        tokio::spawn(async move {
            clone_for_task.run_broadcast(source_stream).await;
        });
    }

    /// Retry sending with fixed delay until success or channel closed.
    ///
    /// Returns the time spent blocked on a full consumer channel (i.e. the
    /// backpressure that consumer exerted on the shared broadcast for this
    /// batch). On immediate success this is ~zero; while the consumer's channel
    /// is full it grows by the retry delay each iteration. Returning the
    /// `Duration` (rather than only emitting a metric) keeps the blocking
    /// behavior unit-testable by value, independent of the metrics recorder.
    async fn try_send_batch_with_retry_forever(
        tx: &Sender<DFResult<RecordBatch>>,
        batch_result: &DFResult<RecordBatch>,
    ) -> Result<Duration, ()> {
        let start = Instant::now();
        loop {
            let to_send = match batch_result {
                Ok(batch) => Ok(batch.clone()),
                Err(e) => Err(Self::clone_df_error(e)),
            };

            match tx.try_send(to_send) {
                Ok(()) => return Ok(start.elapsed()),
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
        // Resolved once; used to attribute per-sink blocked-send time in the
        // multi-sink fan-out case. Consumers with an empty `downstream_id`
        // (the passthrough output and scan-sharing fan-out) are not attributed.
        let metrics_recorder =
            crate::telemetry::recorder::get_control_plane_metrics_recorder(BROADCAST_COMPONENT_ID);
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
                        .map(|consumer| {
                            let downstream_id = consumer.downstream_id.clone();
                            let tx = consumer.tx.clone();
                            let batch_result = &batch_result;
                            async move {
                                let result =
                                    Self::try_send_batch_with_retry_forever(&tx, batch_result)
                                        .await;
                                (downstream_id, result)
                            }
                        })
                        .collect();

                    let results = future::join_all(send_futures).await;
                    for (downstream_id, result) in results {
                        match result {
                            Ok(blocked) => {
                                let blocked_ms = blocked.as_millis() as u64;
                                if blocked_ms > 0 && !downstream_id.is_empty() {
                                    metrics_recorder.record_count_w_tags(
                                        "backpressure_blocked_send",
                                        blocked_ms,
                                        vec![("downstream_id", downstream_id.as_str())],
                                    );
                                }
                            }
                            Err(()) => {
                                warn!(
                                    "Consumer channel closed, removing from broadcast. If this happens outside of a shutdown, this is a bug."
                                );
                            }
                        }
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
    ///
    /// `downstream_id` identifies the node this consumer feeds so that time the
    /// broadcast spends blocked on this consumer's full channel can be
    /// attributed to it (multi-sink fan-out passes the slow sink's reference
    /// name). Pass an empty string to opt out of attribution (the passthrough
    /// output and scan-sharing fan-out).
    pub fn add_consumer(&self, downstream_id: String) -> BroadcastConsumer {
        // Each consumer gets its own bounded receiver
        let (tx, rx) = channel(self.channel_capacity);

        let mut consumers = self.inner.consumers.lock().unwrap();
        consumers.push(BroadcastSender { downstream_id, tx });

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
        let mut guard = self.inner.consumers.lock().unwrap();
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

    fn one_row_batch() -> RecordBatch {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64]))]).unwrap()
    }

    /// A consumer that drains promptly imposes ~no backpressure: the retry
    /// helper returns near-zero blocked time. This is the control case for the
    /// attribution guarantee — fast sinks must not be charged.
    #[tokio::test]
    async fn blocked_send_is_near_zero_for_fast_consumer() {
        let batch_result: DFResult<RecordBatch> = Ok(one_row_batch());
        let (tx, mut rx) = channel::<DFResult<RecordBatch>>(4);

        let blocked = BroadcastStream::try_send_batch_with_retry_forever(&tx, &batch_result)
            .await
            .expect("send to open channel must succeed");
        let _ = rx.recv().await;

        assert!(
            blocked < Duration::from_millis(5),
            "fast consumer should not block, got {blocked:?}"
        );
    }

    /// When a consumer's channel is full, the broadcast producer blocks on it.
    /// The helper returns the time spent blocked, which is the per-edge signal
    /// `run_broadcast` attributes to the slow sink via its `downstream_id`. A
    /// slow consumer must accrue materially more blocked time than a fast one.
    #[tokio::test]
    async fn blocked_send_time_attributed_to_slow_consumer() {
        let batch_result: DFResult<RecordBatch> = Ok(one_row_batch());

        // Fast consumer: open channel, returns immediately.
        let (fast_tx, mut fast_rx) = channel::<DFResult<RecordBatch>>(4);
        let fast_blocked =
            BroadcastStream::try_send_batch_with_retry_forever(&fast_tx, &batch_result)
                .await
                .expect("fast send must succeed");
        let _ = fast_rx.recv().await;

        // Slow consumer: capacity 1, pre-filled so the next send blocks until a
        // slot is freed ~30ms later by a concurrent drain.
        let (slow_tx, mut slow_rx) = channel::<DFResult<RecordBatch>>(1);
        slow_tx
            .try_send(Ok(one_row_batch()))
            .expect("pre-fill the single slot");

        let sender = BroadcastStream::try_send_batch_with_retry_forever(&slow_tx, &batch_result);
        let drainer = async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            slow_rx.recv().await
        };
        let (slow_result, _drained) = tokio::join!(sender, drainer);
        let slow_blocked = slow_result.expect("slow send eventually succeeds after drain");

        assert!(
            slow_blocked >= Duration::from_millis(20),
            "slow consumer should block until the slot frees (~30ms), got {slow_blocked:?}"
        );
        assert!(
            slow_blocked > fast_blocked,
            "slow consumer ({slow_blocked:?}) must accrue more blocked time than fast ({fast_blocked:?})"
        );
    }

    /// A closed consumer channel surfaces as `Err`, which `run_broadcast` logs
    /// (typically during shutdown) rather than attributing as backpressure.
    #[tokio::test]
    async fn blocked_send_returns_err_when_channel_closed() {
        let batch_result: DFResult<RecordBatch> = Ok(one_row_batch());
        let (tx, rx) = channel::<DFResult<RecordBatch>>(1);
        drop(rx);
        let result = BroadcastStream::try_send_batch_with_retry_forever(&tx, &batch_result).await;
        assert!(result.is_err(), "closed channel must return Err");
    }
}
