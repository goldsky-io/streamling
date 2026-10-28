use crate::telemetry::node_time::OwnWork;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use parking_lot::Mutex;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::Instant;

/// An input stream that knows when an awaited batch became ready.
///
/// A consumer can poll, get `Pending`, and then do its own work before it polls
/// again (`ready_chunks`, `select!`). The input wakes the task when its batch is
/// ready, so the wait for input ends at that wake, not at the next poll; the
/// time after it is the consumer's.
pub struct WakeTimed {
    inner: SendableRecordBatchStream,
    work: Option<OwnWork>,
    woke_at: Arc<Mutex<Option<Instant>>>,
    waker: Option<(Waker, Waker)>,
}

/// How a batch whose poll had to wait became ready.
#[derive(Debug, Clone, Copy)]
pub struct Readiness {
    /// The input woke the task: from here the batch waited for the consumer.
    pub woke_at: Instant,
    /// The consumer polled again and got the batch.
    pub polled_at: Instant,
    /// The node's own-work counter just before that poll.
    pub(crate) work_before_poll: Option<u64>,
}

struct WakeRecorder {
    task: Waker,
    woke_at: Arc<Mutex<Option<Instant>>>,
}

impl Wake for WakeRecorder {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woke_at.lock().get_or_insert_with(Instant::now);
        self.task.wake_by_ref();
    }
}

impl WakeTimed {
    /// `work` is the node's own work run inside the poll (a SQL subtree), so
    /// a wait can leave it out.
    pub fn new(inner: SendableRecordBatchStream, work: Option<OwnWork>) -> Self {
        Self {
            inner,
            work,
            woke_at: Arc::new(Mutex::new(None)),
            waker: None,
        }
    }

    /// The next batch, and when it became ready if its poll had to wait.
    pub async fn next(&mut self) -> (Option<Result<RecordBatch>>, Option<Readiness>) {
        let mut waited = false;
        std::future::poll_fn(|cx| self.poll_timed(cx, &mut waited)).await
    }

    fn poll_timed(
        &mut self,
        cx: &mut Context<'_>,
        waited: &mut bool,
    ) -> Poll<(Option<Result<RecordBatch>>, Option<Readiness>)> {
        let woke_at = self.woke_at.lock().take();
        let polled_at = Instant::now();
        let work_before_poll = self.work.as_ref().map(OwnWork::nanos);
        let waker = self.recording_waker(cx.waker());
        match self.inner.poll_next_unpin(&mut Context::from_waker(&waker)) {
            Poll::Pending => {
                *waited = true;
                Poll::Pending
            }
            Poll::Ready(item) => {
                let readiness = woke_at.filter(|_| *waited).map(|woke_at| Readiness {
                    woke_at,
                    polled_at,
                    work_before_poll,
                });
                Poll::Ready((item, readiness))
            }
        }
    }

    /// Reused while the task's waker stays the same.
    fn recording_waker(&mut self, task: &Waker) -> Waker {
        match &self.waker {
            Some((cached_task, recording)) if cached_task.will_wake(task) => recording.clone(),
            _ => {
                let recording = Waker::from(Arc::new(WakeRecorder {
                    task: task.clone(),
                    woke_at: Arc::clone(&self.woke_at),
                }));
                self.waker = Some((task.clone(), recording.clone()));
                recording
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use std::time::Duration;

    fn timed(
        stream: impl futures::Stream<Item = Result<RecordBatch>> + Send + 'static,
    ) -> WakeTimed {
        let schema = Arc::new(Schema::empty());
        WakeTimed::new(
            Box::pin(RecordBatchStreamAdapter::new(schema, stream)),
            None,
        )
    }

    fn batch() -> RecordBatch {
        RecordBatch::new_empty(Arc::new(Schema::empty()))
    }

    #[tokio::test]
    async fn a_ready_batch_reports_no_wait() {
        let mut input = timed(futures::stream::iter([Ok(batch())]));
        let (item, readiness) = input.next().await;
        assert!(item.is_some());
        assert!(readiness.is_none());
    }

    /// The consumer polls late; the batch was ready at the wake.
    #[tokio::test]
    async fn readiness_is_the_wake_not_the_late_poll() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let mut input = timed(futures::stream::once(async move {
            rx.await.ok();
            Ok(batch())
        }));
        let mut next = std::pin::pin!(input.next());
        assert!(futures::poll!(next.as_mut()).is_pending());

        tx.send(()).unwrap();
        let sent_at = Instant::now();
        tokio::time::sleep(Duration::from_millis(30)).await;
        let (item, readiness) = next.await;

        assert!(item.is_some());
        let readiness = readiness.expect("the poll waited");
        assert!(readiness.woke_at <= sent_at + Duration::from_millis(5));
        assert!(readiness.polled_at >= readiness.woke_at + Duration::from_millis(25));
    }
}
