use crate::telemetry::recorder::MetricsRecorder;
use std::env;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Sender;

pub const DEFAULT_BACKPRESSURE_EVENT_THRESHOLD_MS: u64 = 100;
const BACKPRESSURE_EVENT_THRESHOLD_ENV: &str = "STREAMLING__BACKPRESSURE_EVENT_THRESHOLD_MS";

static BACKPRESSURE_EVENT_THRESHOLD_MS: OnceLock<u64> = OnceLock::new();

pub fn backpressure_event_threshold_ms() -> u64 {
    *BACKPRESSURE_EVENT_THRESHOLD_MS.get_or_init(|| {
        env::var(BACKPRESSURE_EVENT_THRESHOLD_ENV)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_BACKPRESSURE_EVENT_THRESHOLD_MS)
    })
}

pub fn record_idle_wait(
    metrics_recorder: Option<&MetricsRecorder>,
    metadata_id: &str,
    tags: &[(&str, &str)],
    start: Instant,
) {
    let Some(metrics_recorder) = metrics_recorder else {
        return;
    };
    metrics_recorder.record_time_w_tag_slice("node_idle_wait", start.elapsed(), tags, metadata_id);
}

pub fn record_backpressure_wait(
    metrics_recorder: Option<&MetricsRecorder>,
    metadata_id: &str,
    tags: &[(&str, &str)],
    start: Instant,
) {
    let Some(metrics_recorder) = metrics_recorder else {
        return;
    };
    let elapsed = start.elapsed();
    metrics_recorder.record_time_w_tag_slice("node_backpressure_wait", elapsed, tags, metadata_id);
    if elapsed >= Duration::from_millis(backpressure_event_threshold_ms()) {
        metrics_recorder.record_count_w_tag_slice("node_backpressure_events", 1, tags, metadata_id);
    }
}

pub fn record_batch_emptiness(
    metrics_recorder: Option<&MetricsRecorder>,
    metadata_id: &str,
    tags: &[(&str, &str)],
    rows: usize,
    empty_streak: &mut u64,
) {
    let Some(metrics_recorder) = metrics_recorder else {
        return;
    };
    if rows == 0 {
        *empty_streak += 1;
        metrics_recorder.record_count_w_tag_slice("node_empty_batch", 1, tags, metadata_id);
    } else {
        *empty_streak = 0;
    }

    metrics_recorder.record_gauge_w_tag_slice(
        "node_empty_streak",
        *empty_streak,
        tags,
        metadata_id,
    );
}

pub fn record_inflight_buffered(
    metrics_recorder: Option<&MetricsRecorder>,
    metadata_id: &str,
    tags: &[(&str, &str)],
    buffered: u64,
) {
    let Some(metrics_recorder) = metrics_recorder else {
        return;
    };
    metrics_recorder.record_gauge_w_tag_slice(
        "node_inflight_buffered",
        buffered,
        tags,
        metadata_id,
    );
}

/// Send a batch over `tx`, bracketing the send with inflight and backpressure metrics.
/// Returns `true` if the send succeeded, `false` if the receiver was dropped.
pub async fn send_batch_with_metrics<T>(
    tx: &Sender<T>,
    item: T,
    num_rows: u64,
    metrics_recorder: Option<&MetricsRecorder>,
    metadata_id: &str,
    tags: &[(&str, &str)],
) -> bool {
    record_inflight_buffered(metrics_recorder, metadata_id, tags, num_rows);
    let send_start = Instant::now();
    let sent = tx.send(item).await.is_ok();
    record_backpressure_wait(metrics_recorder, metadata_id, tags, send_start);
    record_inflight_buffered(metrics_recorder, metadata_id, tags, 0);
    sent
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::recorder::MetricsRecorder;

    const TAGS: [(&str, &str); 1] = [("execution_kind", "native")];

    // A default recorder has an empty metadata registry, so every emission is
    // dropped after the tag lookup. That is exactly what these tests want:
    // they assert the bookkeeping the helpers do around the emission.
    fn recorder() -> MetricsRecorder {
        MetricsRecorder::default()
    }

    #[test]
    fn empty_streak_counts_consecutive_empty_batches_and_resets() {
        let recorder = recorder();
        let mut streak = 0u64;

        for expected in 1..=3 {
            record_batch_emptiness(Some(&recorder), "node", &TAGS, 0, &mut streak);
            assert_eq!(streak, expected);
        }

        record_batch_emptiness(Some(&recorder), "node", &TAGS, 7, &mut streak);
        assert_eq!(streak, 0, "a non-empty batch resets the streak");

        record_batch_emptiness(Some(&recorder), "node", &TAGS, 0, &mut streak);
        assert_eq!(streak, 1, "the streak restarts from the next empty batch");
    }

    #[tokio::test]
    async fn send_batch_with_metrics_reports_delivery() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(1);
        let recorder = recorder();

        assert!(send_batch_with_metrics(&tx, 1, 1, Some(&recorder), "node", &TAGS).await);
        assert_eq!(rx.recv().await, Some(1));
    }

    #[tokio::test]
    async fn send_batch_with_metrics_reports_a_dropped_receiver() {
        let (tx, rx) = tokio::sync::mpsc::channel::<u32>(1);
        drop(rx);

        // No recorder installed: the helper must still report the send result
        // rather than short-circuiting on the missing telemetry.
        assert!(!send_batch_with_metrics(&tx, 1, 1, None, "node", &TAGS).await);
    }
}
