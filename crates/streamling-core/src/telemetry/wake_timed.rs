use crate::telemetry::node_time::NodeTime;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use parking_lot::Mutex;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll, Wake, Waker};

/// An input stream that times its consumer's wait for each batch.
///
/// A consumer can poll, get `Pending`, and then do its own work before it polls
/// again (`ready_chunks`, `select!`). The input wakes the task when its batch is
/// ready, so the wait (`starved`) pauses at that wake and the time until the
/// next poll is the consumer's (`gap`). The switch happens in the waker, so a
/// consumer that stays busy long after the wake shows as such while it lasts.
pub struct WakeTimed {
    inner: SendableRecordBatchStream,
    wait: Arc<Wait>,
    waker: Option<(Waker, Waker)>,
}

struct Wait {
    starved: Option<Arc<NodeTime>>,
    gap: Option<Arc<NodeTime>>,
    state: Mutex<WaitState>,
}

#[derive(Default)]
struct WaitState {
    phase: Phase,
    /// The consumer is inside a poll of the input: a wake now is not a gap.
    polling: bool,
    woke_in_poll: bool,
}

#[derive(Default, PartialEq, Eq)]
enum Phase {
    /// No batch asked for.
    #[default]
    Idle,
    /// Asked for, not ready: `starved`.
    Waiting,
    /// Ready, not yet taken: `gap`.
    Woken,
}

impl Wait {
    fn hand_over(&self, state: &mut WaitState) {
        if let Some(starved) = &self.starved {
            starved.pause();
        }
        if let Some(gap) = &self.gap {
            gap.open();
        }
        state.phase = Phase::Woken;
    }

    fn wake(&self) {
        let mut state = self.state.lock();
        if state.polling {
            state.woke_in_poll = true;
        } else if state.phase == Phase::Waiting {
            self.hand_over(&mut state);
        }
    }
}

/// The input may keep this waker after the stream is dropped, so it must not
/// keep the wait alive.
struct WakeRecorder {
    task: Waker,
    wait: Weak<Wait>,
}

impl Wake for WakeRecorder {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(wait) = self.wait.upgrade() {
            wait.wake();
        }
        self.task.wake_by_ref();
    }
}

impl Drop for WakeTimed {
    /// A wait cut short by dropping the stream is teardown: discarded.
    fn drop(&mut self) {
        let state = self.wait.state.lock();
        if state.phase == Phase::Idle {
            return;
        }
        if let Some(starved) = &self.wait.starved {
            starved.discard();
        }
        if state.phase == Phase::Woken
            && let Some(gap) = &self.wait.gap
        {
            gap.discard();
        }
    }
}

impl WakeTimed {
    pub fn new(
        inner: SendableRecordBatchStream,
        starved: Option<Arc<NodeTime>>,
        gap: Option<Arc<NodeTime>>,
    ) -> Self {
        Self {
            inner,
            wait: Arc::new(Wait {
                starved,
                gap,
                state: Mutex::new(WaitState::default()),
            }),
            waker: None,
        }
    }

    /// The next batch. Only a wait that delivered a batch is `starved`: one
    /// that ended in end-of-stream or an error drops its unflushed part.
    pub async fn next(&mut self) -> Option<Result<RecordBatch>> {
        std::future::poll_fn(|cx| self.poll_timed(cx)).await
    }

    fn poll_timed(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        {
            let mut state = self.wait.state.lock();
            if state.phase == Phase::Woken
                && let Some(gap) = &self.wait.gap
            {
                gap.close();
            }
            // The poll runs upstream work, so it is input wait again.
            if let Some(starved) = &self.wait.starved {
                match state.phase {
                    Phase::Idle => starved.open(),
                    Phase::Woken => starved.resume(),
                    Phase::Waiting => {}
                }
            }
            state.phase = Phase::Waiting;
            state.polling = true;
            state.woke_in_poll = false;
        }
        // Not under the state lock: the input may wake the task synchronously.
        let waker = self.recording_waker(cx.waker());
        let poll = self.inner.poll_next_unpin(&mut Context::from_waker(&waker));

        let mut state = self.wait.state.lock();
        state.polling = false;
        match poll {
            Poll::Pending => {
                if state.woke_in_poll {
                    self.wait.hand_over(&mut state);
                }
                Poll::Pending
            }
            Poll::Ready(item) => {
                state.phase = Phase::Idle;
                if let Some(starved) = &self.wait.starved {
                    if matches!(item, Some(Ok(_))) {
                        starved.close();
                    } else {
                        starved.discard();
                    }
                }
                Poll::Ready(item)
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
                    wait: Arc::downgrade(&self.wait),
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
    use crate::telemetry::recorder::get_metrics_recorder;
    use crate::telemetry::recorder::test_support::{TEST_LOCK, init_recorder_with_node};
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use std::time::{Duration, Instant};

    fn timed(
        stream: impl futures::Stream<Item = Result<RecordBatch>> + Send + 'static,
        id: &str,
    ) -> (WakeTimed, Arc<NodeTime>, Arc<NodeTime>) {
        let schema = Arc::new(Schema::empty());
        let starved = NodeTime::wall(get_metrics_recorder(), id, "starved", "");
        let gap = NodeTime::wall(get_metrics_recorder(), id, "busy", "");
        let input = WakeTimed::new(
            Box::pin(RecordBatchStreamAdapter::new(schema, stream)),
            Some(Arc::clone(&starved)),
            Some(Arc::clone(&gap)),
        );
        (input, starved, gap)
    }

    fn batch() -> RecordBatch {
        RecordBatch::new_empty(Arc::new(Schema::empty()))
    }

    /// The consumer polls late; the wait ended at the wake, and the time
    /// after it is the consumer's while it lasts, before the late poll.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn the_wait_ends_at_the_wake_not_at_the_late_poll() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let id = "wake_timed::late_poll";
        init_recorder_with_node(id);
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let (mut input, starved, gap) = timed(
            futures::stream::once(async move {
                rx.await.ok();
                Ok(batch())
            }),
            id,
        );
        let mut next = std::pin::pin!(input.next());
        assert!(futures::poll!(next.as_mut()).is_pending());

        let asked_at = Instant::now();
        tokio::time::sleep(Duration::from_millis(10)).await;
        tx.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            gap.total() >= Duration::from_millis(25),
            "{:?}",
            gap.total()
        );
        let item = next.await;

        assert!(item.is_some());
        let starved = starved.total();
        assert!(
            starved >= Duration::from_millis(10)
                && starved < asked_at.elapsed() - Duration::from_millis(25),
            "{starved:?}"
        );
    }

    /// A wait that ends in an error or end-of-stream is teardown, not a wait
    /// for data.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn only_a_wait_that_delivers_a_batch_is_starved() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let id = "wake_timed::only_delivered";
        init_recorder_with_node(id);
        let items = [
            Err(datafusion::error::DataFusionError::Execution(
                "upstream".into(),
            )),
            Ok(batch()),
        ];
        let (mut input, starved, _gap) = timed(
            futures::stream::iter(items).then(|item| async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                item
            }),
            id,
        );

        assert!(input.next().await.unwrap().is_err());
        assert!(input.next().await.unwrap().is_ok());
        let delivered = starved.total();
        assert!(input.next().await.is_none());
        assert_eq!(starved.total(), delivered, "end-of-stream adds nothing");
        // The 1s flusher may bank part of the error's wait, never all of it.
        assert!(
            delivered >= Duration::from_millis(20) && delivered < Duration::from_millis(40),
            "{delivered:?}"
        );
    }

    /// The input keeps the last waker after the stream is gone (a tokio
    /// channel's receiver slot, a broadcast slot). A dropped stream's open
    /// wait must not keep accruing through it.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn a_dropped_stream_stops_its_open_wait() {
        use crate::telemetry::recorder::test_support::node_wait_ms;

        let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let id = "wake_timed::dropped";
        init_recorder_with_node(id);
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(1);
        let (mut input, starved, gap) = timed(
            futures::stream::poll_fn(move |cx| rx.poll_recv(cx)),
            id,
        );
        drop((starved, gap));
        assert!(futures::poll!(std::pin::pin!(input.next())).is_pending());
        drop(input);

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(node_wait_ms(id, "starved", None), 0);
        drop(tx);
    }
}
