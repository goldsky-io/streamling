//! Axum server exposing `POST /preview`: rewrite sinks to blackhole, validate,
//! run a child pipeline for a bounded duration, and proxy its live-data SSE.

use crate::preview::child::{self, RunChild};
use crate::preview::duration::resolve_duration_secs;
use crate::preview::rewrite::rewrite_sinks_to_blackhole;
use axum::{
    body::Body,
    extract::{Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use futures::StreamExt;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Query parameters for `POST /preview`.
#[derive(Debug, Default, Deserialize)]
pub struct PreviewQuery {
    /// Requested preview duration in seconds. Resolved/clamped by
    /// [`resolve_duration_secs`].
    pub duration_seconds: Option<u64>,
}

/// Single-slot preview state guarded by a mutex.
struct Inner {
    /// The currently running preview child, if any.
    child: Option<RunChild>,
    /// Monotonically increasing generation id. Each new preview bumps this so
    /// that stale duration timers (from replaced previews) become no-ops.
    generation: u64,
}

/// Shared, cloneable state for the preview server.
#[derive(Clone)]
pub struct PreviewState {
    inner: Arc<Mutex<Inner>>,
    /// Reused HTTP client for connecting to the child's live-data SSE. Cloning a
    /// `reqwest::Client` is cheap and clones share the underlying connection pool.
    client: reqwest::Client,
}

impl PreviewState {
    /// Creates fresh state with no running child.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                child: None,
                generation: 0,
            })),
            client: reqwest::Client::new(),
        }
    }
}

/// Whether a timer/guard for `my_gen` should act, given the current generation.
///
/// Both the duration timer and the [`PreviewStreamGuard`] use this so that a
/// stale generation (one whose preview has already been replaced) never kills
/// the replacement child.
fn should_act(current_generation: u64, my_gen: u64) -> bool {
    current_generation == my_gen
}

/// Held alive for the lifetime of the proxied SSE stream. When the stream ends
/// — whether the duration elapsed, the pipeline finished, or the CLIENT
/// DISCONNECTED — this guard's Drop kills the still-current preview child and
/// drops the temp config file. Uses the same generation check as the duration
/// timer so it never kills a preview that has already been replaced.
struct PreviewStreamGuard {
    state: PreviewState,
    generation: u64,
    _temp: tempfile::NamedTempFile,
}

impl Drop for PreviewStreamGuard {
    fn drop(&mut self) {
        let state = self.state.clone();
        let my_gen = self.generation;
        // Drop is sync; spawn the async kill onto the current runtime if present.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let mut inner = state.inner.lock().await;
                if should_act(inner.generation, my_gen) {
                    if let Some(child) = inner.child.take() {
                        child.kill().await;
                    }
                }
            });
        }
    }
}

impl Default for PreviewState {
    fn default() -> Self {
        Self::new()
    }
}

/// Builds the axum router for the preview server.
pub fn build_router(state: PreviewState) -> Router {
    Router::new()
        .route("/preview", post(preview_handler))
        .with_state(state)
}

/// Handles `POST /preview`: rewrite sinks to blackhole, validate, (re)spawn the
/// child pipeline under replace semantics, and proxy its live-data SSE for a
/// bounded duration.
async fn preview_handler(
    State(state): State<PreviewState>,
    Query(query): Query<PreviewQuery>,
    body: String,
) -> Response {
    // 1. Rewrite sinks to blackhole; on Err -> 422.
    let rewritten = match rewrite_sinks_to_blackhole(&body) {
        Ok(yaml) => yaml,
        Err(e) => {
            return (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response();
        }
    };

    // 2. Write rewritten YAML to a temp file.
    let temp = match tempfile::Builder::new().suffix(".yaml").tempfile() {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to create temp file: {e}"),
            )
                .into_response();
        }
    };
    if let Err(e) = std::fs::write(temp.path(), rewritten.as_bytes()) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to write temp config: {e}"),
        )
            .into_response();
    }

    // 3. Validate; on Err -> 422.
    if let Err(msg) = child::validate_config(temp.path()).await {
        return (StatusCode::UNPROCESSABLE_ENTITY, msg).into_response();
    }

    // 4. Replace semantics: under the lock, bump generation, kill any in-flight
    //    child, spawn the new one, and store it.
    let (my_gen, admin_port) = {
        // NOTE: The mutex is intentionally held across `spawn_run_child` (up to
        // ~30s while the child boots). This is a deliberate trade-off for the
        // single-slot / one-at-a-time preview design: starts are serialized, and
        // replace semantics still work because a new request kills the old child
        // once it acquires this lock. The consequence is that concurrent POSTs
        // queue for up to ~30s during child startup. Do not restructure this
        // locking without revisiting the single-slot guarantee.
        let mut inner = state.inner.lock().await;
        inner.generation += 1;
        let my_gen = inner.generation;

        // Kill the previous child immediately so its stream ends right away.
        if let Some(old) = inner.child.take() {
            old.kill().await;
        }

        let run_child = match child::spawn_run_child(temp.path()).await {
            Ok(c) => c,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to spawn preview child: {e}"),
                )
                    .into_response();
            }
        };
        let admin_port = run_child.admin_port;
        inner.child = Some(run_child);
        (my_gen, admin_port)
    };

    // 5. Resolve the duration.
    let duration_secs = resolve_duration_secs(query.duration_seconds);

    // 8. Spawn a generation-guarded timer that kills the child after the
    //    resolved duration, but only if it is still the current child.
    {
        let state = state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(duration_secs)).await;
            let mut inner = state.inner.lock().await;
            if should_act(inner.generation, my_gen) {
                if let Some(c) = inner.child.take() {
                    c.kill().await;
                }
            }
        });
    }

    // 6. Connect to the child's live-data SSE; on connect error -> 502.
    let url = format!("http://127.0.0.1:{admin_port}/admin/live-data");
    let upstream = match state.client.get(&url).send().await {
        Ok(resp) => resp,
        Err(e) => {
            // Kill the child immediately rather than leaving it running until the
            // duration timer fires, but only if it is still the current child.
            {
                let mut inner = state.inner.lock().await;
                if should_act(inner.generation, my_gen) {
                    if let Some(c) = inner.child.take() {
                        c.kill().await;
                    }
                }
            }
            return (
                StatusCode::BAD_GATEWAY,
                format!("failed to connect to preview child live-data: {e}"),
            )
                .into_response();
        }
    };

    // 7. Proxy the SSE bytes, append a terminal `event: end` frame, and hold a
    //    guard alive for the lifetime of the stream. When the stream ends (client
    //    disconnect, duration timer, or pipeline completion) the guard's Drop
    //    kills the still-current child and drops the temp config file.
    let byte_stream = upstream.bytes_stream();
    let terminal = futures::stream::once(async {
        Ok::<_, reqwest::Error>(bytes::Bytes::from_static(
            b"event: end\ndata: {\"reason\":\"preview ended\"}\n\n",
        ))
    });
    let guard = PreviewStreamGuard {
        state: state.clone(),
        generation: my_gen,
        _temp: temp,
    };
    let guarded = byte_stream.chain(terminal).map(move |chunk| {
        let _g = &guard; // closure owns the guard; dropped when the stream is dropped
        chunk
    });

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(guarded))
        .expect("preview SSE response is valid")
}

#[cfg(test)]
mod tests {
    use super::{build_router, should_act, PreviewState};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt; // oneshot

    #[test]
    fn generation_guard_acts_only_on_matching_generation() {
        // A matching generation should act (the preview is still current).
        assert!(should_act(5, 5));
        // A stale generation must NOT act, so a replaced preview's timer/guard
        // never kills the replacement that bumped the current generation.
        assert!(!should_act(6, 5));
        assert!(!should_act(5, 4));
    }

    #[tokio::test]
    async fn generation_guard_leaves_replacement_intact_but_clears_match() {
        // Drive the Inner/generation decision directly to prove a stale
        // generation does not clear a replacement, while a matching one does.
        let state = PreviewState::new();

        // Simulate two generations on the single slot. We cannot construct a
        // real `RunChild` here, so we model the slot with `Option<bool>` logic
        // via the pure `should_act` decision: generation 1 is created, then
        // replaced by generation 2 (the current one).
        let current_generation = {
            let mut inner = state.inner.lock().await;
            inner.generation += 1; // gen 1
            let _gen1 = inner.generation;
            inner.generation += 1; // gen 2 (replacement)
            inner.generation
        };
        assert_eq!(current_generation, 2);

        // A guard created for the now-stale generation 1 must be a no-op.
        let stale_my_gen = 1;
        assert!(!should_act(current_generation, stale_my_gen));

        // A guard created for the current generation 2 must act.
        let live_my_gen = 2;
        assert!(should_act(current_generation, live_my_gen));
    }

    #[tokio::test]
    async fn invalid_yaml_returns_422() {
        let app = build_router(PreviewState::new());
        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/preview")
                    .header("content-type", "text/yaml")
                    .body(Body::from("::: not yaml :::"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }
}
