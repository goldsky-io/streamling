# Investigation: unclean shutdown in production (hang → SIGKILL → data loss)

**Date:** 2026-06-12
**Symptom:** When k8s sends SIGTERM, the pipeline starts shutting down, then hangs, and k8s SIGKILLs it after the grace period. In Job mode, the last few buffers are not drained, causing data loss at the tail. A prior fix (SourceComplete signals, intended for CI) did not work reliably.

This document covers two distinct failure families: pipelines **with plugins** (source/sink/transform plugins) and pipelines **without plugins** (e.g. Kafka/hybrid → ClickHouse). Both hang, for different root causes.

---

## Part 1 — Pipelines with plugins

### Root cause summary

There is no coordinated shutdown path. Three independent SIGTERM handlers race each other, and the one that kills plugins fires first — killing the *downstream* end of the pipeline while sources and sinks are still pushing data. Sink writes to a dead plugin then block forever on a bounded channel, wedging the tokio runtime. Separately, even on clean Job-mode completion, the process exits without awaiting the plugin sink's drain task, so the last queued buffers are cancelled mid-flush at runtime teardown.

### 1.1 Three competing SIGTERM handlers

| Handler | Location | Behavior on SIGTERM |
|---|---|---|
| Plugin shutdown watcher | `crates/streamling-core/src/plugin.rs:110-133` | Immediately calls `terminate_all_plugins()` — enqueues `PluginMsg::Terminate` while data is still flowing |
| Kafka source handler | `crates/streamling-connectors/src/table_providers/kafka.rs:1288-1298` | `break 'outer` — exits without flushing the partial batch |
| (nobody else) | — | ClickHouse and hybrid sources have **no** SIGTERM handling at all |

The termination order is inverted: plugins (the sinks' downstream) are killed first, the opposite of a drain.

### 1.2 The hang mechanism (streaming mode)

- `PluginSink::write_all` sends to the plugin over a **bounded crossbeam channel with synchronous, blocking `send().unwrap()`** calls, executed on tokio runtime threads (`crates/streamling-core/src/plugin/table_provider.rs:399-449`).
- The sink dispatcher processes its input FIFO; on `Terminate` it calls `sink_plugin.terminate()`, `is_running` flips false, and the dispatcher exits (`crates/streamling-plugin/src/dispatch.rs:358-412`). Everything the sink writes *after* the watcher-injected `Terminate` is never drained.
- The channel fills (default capacity ~128, `app_config.plugin.channel_capacity`), and the next `send` **parks a tokio worker thread forever**. It never yields, can't be cancelled, and the runtime can't shut down (dropping the runtime waits for workers to finish their current poll). Process is alive but stuck → SIGKILL.
- The same blocking-send hazard exists in `terminate_plugins` itself (`crates/streamling-core/src/plugin.rs:579-585`) if the channel is already full.

### 1.3 Sink futures cancelled mid-flight

`crates/streamling/src/lib.rs:1979-2001`: the `tokio::select!` treats "any plugin future completed" as terminal and **drops** `try_join_all(sink_futures)`, cancelling DataFusion execution with whatever was buffered in flight.

### 1.4 Job-mode tail data loss (no SIGTERM needed)

The plugin `execution_future` is only a join-handle wrapper — the dispatcher actually runs as a `runtime.spawn`ed task (`crates/streamling-plugin/src/lib.rs:447-453`). On clean Job completion:

1. Sink futures complete → the `select!` takes the sink branch and drops the plugin join handles (the dispatcher keeps running, still draining queued batches).
2. `terminate_all_plugins()` (`crates/streamling/src/lib.rs:2028`) enqueues `Terminate` and returns immediately — **nothing ever awaits the dispatcher finishing**.
3. `start_with` returns, `main` cleans up, `#[tokio::main]` drops the runtime, which **cancels the still-draining dispatcher task at its next await point**.

Anything still in the plugin's input channel (up to ~128 batches) plus the plugin's internal write buffers — whatever `terminate()` was about to flush — is destroyed. The last few buffers of a Job are exactly what's sitting there.

### 1.5 Why the CI fix (SourceComplete) didn't work reliably

- In the Kafka source, the `SourceComplete` handler (`kafka.rs:1446-1451`) pushes the marker into `checkpoint_messages_buffer` and then `break 'outer` — **before** `tx.send(record_batch)` at `kafka.rs:1459`. The freshly converted final batch is dropped, and the SourceComplete metadata never reaches the sink, so downstream can't drain deterministically.
- Even when SourceComplete flows correctly, it only ends the *source*; it does nothing about the unawaited plugin drain (1.4) or the `select!` cancellation (1.3), so the tail-loss window remains.

---

## Part 2 — Pipelines without plugins

### Root cause summary

The Kafka source's **lag-reporting task** is never told to stop on SIGTERM (its shutdown signal is only wired up for job-mode hybrid sources), so it outlives the entire shutdown sequence. At runtime teardown the task is cancelled and its **raw, unwrapped `StreamConsumer` is dropped on a tokio worker thread**, triggering the exact `rd_kafka_destroy()` deadlock the codebase already documents and protects against for the main consumer — but not for this one. The runtime's drop never completes; the process sits fully "shut down" but alive until SIGKILL.

### 2.1 The chain, step by step

1. **SIGTERM arrives.** The Kafka source's select branch catches it (`kafka.rs:1288-1298`), logs "Received shutdown signal", breaks out, unsubscribes/unassigns, and `forget()`s the main consumer — deliberately leaking it to skip `rd_kafka_destroy` (`kafka.rs:1470-1475`). This is the "it starts shutting down" part observed in production.
2. **The lag task is never signaled.** The source spawns `calculate_lag_task` with its **own dedicated `StreamConsumer`** (`kafka.rs:995-1000`, spawned at `kafka.rs:1026-1036`). Its only exit condition is the `shutdown_rx` watch channel flipping true (`kafka.rs:813-817`). But `shutdown_tx.send(true)` exists in exactly one place — `KafkaSourceTableProvider::shutdown()` at `kafka.rs:1669-1671` — and its only caller is the hybrid source's job-mode termination. The SIGTERM path never touches it. The lag task keeps looping, calling `fetch_watermarks` (blocking rdkafka call, 5s timeout per partition, `kafka.rs:834-837`).
3. **Everything else completes.** Stream ends, sink drains, `try_join_all` returns, the checkpoint coordinator stops (its loops are all bounded by the `running` flag — verified not the culprit), telemetry flushes, `main()` returns.
4. **Runtime teardown deadlocks.** Runtime drop cancels the lag task; cancellation drops its `StreamConsumer` **inline on a tokio worker thread**. That is precisely the failure mode described in `SafeKafkaConsumer`'s own doc comment (`kafka.rs:153-159`): dropping rdkafka consumers synchronously on a tokio worker can deadlock because `rd_kafka_destroy()` waits for internal rdkafka threads which may themselves be waiting for cleanup coordination. The main consumer was wrapped in `SafeKafkaConsumer` to fix exactly this; the lag consumer was not. Runtime drop waits on the wedged worker forever → hang → SIGKILL.

**Job mode hits the same bug, just earlier:** there `provider.shutdown()` *is* called, the lag task breaks out of its loop normally (`kafka.rs:816`) and drops the raw consumer on a worker thread at the end of `calculate_lag_task` (`kafka.rs:906`) — same deadlock, no runtime teardown needed.

### 2.2 Secondary hang vectors (real, but conditional)

- **ClickHouse sink retries forever.** All writes go through `retry_forever_with_backoff_async` (`clickhouse.rs:735, 823, 860`; impl in `crates/streamling-core/src/retry.rs:11`). If ClickHouse is erroring during the drain, the sink future never completes and `try_join_all` waits indefinitely. Worse, the resulting backpressure parks the Kafka source inside `tx.send().await` (`kafka.rs:1459`) — which is *outside* the select that listens for SIGTERM — so shutdown never even starts.
- **`consumer.commit(CommitMode::Sync)`** (`kafka.rs:1403`) blocks a worker with no timeout if the broker is unreachable; SIGTERM cannot be observed while it's in flight.
- **30s coordinator-stop timeout** (`crates/streamling/src/lib.rs:2033`) alone equals the default k8s grace period (and its log message wrongly says "5 seconds"). Any of the above eating even a few seconds pushes into SIGKILL territory.
- **Kafka SIGTERM partial-batch drop.** The SIGTERM `break 'outer` skips `convert_to_batch()` + `tx.send()`, dropping the partially accumulated batch and pending checkpoint messages (`kafka.rs:1288-1298` vs. `kafka.rs:1302-1466`). Recoverable via offset replay in streaming mode (offsets only commit on checkpoint finalize), but it widens the at-least-once window.
- **SafeKafkaConsumer error path.** On early-error exits, `SafeKafkaConsumer::drop` does `spawn_blocking(rd_kafka_destroy)` (`kafka.rs:190-205`). If that deadlocks in the blocking pool during runtime shutdown, runtime drop hangs the same way.
- **Per-iteration signal stream recreation.** The SIGTERM `Signal` stream is re-created at the top of every outer loop iteration (`kafka.rs:1158-1162`); a signal landing in the drop/recreate window is lost. Narrow race, but it exists.

---

## Part 3 — Recommended production fix

Principle: shut down **front-to-back** (sources first, plugins last), make every step awaited and time-bounded, and make shutdown work signal-driven through one mechanism.

1. **One top-level SIGTERM handler** (in `main.rs`/`Streamling`) that triggers source shutdown via the existing `shutdown_tx` watch channels (Kafka already has one — `kafka.rs:1669` — extend the hook to hybrid/ClickHouse sources). This makes the watch channel the single shutdown mechanism: it fixes the orphaned lag task (Part 2) and lets the per-source in-select SIGTERM handler and the plugin shutdown watcher (`plugin.rs:110-133`) be deleted.
2. **Kafka source: drain on shutdown.** On both the shutdown-signal and SourceComplete paths, break the *inner* loop only, so the final `convert_to_batch` + `tx.send` executes (and SourceComplete metadata ships with the final batch), then exit the outer loop.
3. **Fix the lag consumer teardown.** Wrap the lag consumer in `SafeKafkaConsumer` (or have the lag task explicitly `unsubscribe`/`forget` on exit like the main path). Join the lag task before the source task returns, or at minimum keep its consumer teardown off the runtime's worker threads. This is the minimal standalone fix for the non-plugin hang.
4. **Await plugin drain before exit.** After `try_join_all(sink_futures)` completes, send `Terminate`, then await the plugin execution futures with a deadline (grace period minus margin). Restructure the `select!` so plugin completion is only terminal on *failure*; on success the sink futures must still be awaited. This is the fix for Job-mode tail loss.
5. **Kill the blocking sends.** Replace `send().unwrap()` in `PluginSink::write_all` and `terminate_plugins` with `send_timeout` (or route through `spawn_blocking`) so a dead plugin produces a fast, visible error instead of a wedged worker thread.
6. **Bound the shutdown budget.** Derive a single deadline from `terminationGracePeriodSeconds`; lower the coordinator-stop timeout (and fix its log message); give the ClickHouse `retry_forever_with_backoff_async` loops a cancellation token tied to the shutdown signal so a sick sink can't pin the drain forever. Bump the grace period on Job pods to cover a real drain.

Items 2–4 directly stop the Job-mode tail loss; items 1 and 3 stop the hang-then-SIGKILL.

---

## Part 4 — Proposed shutdown architecture

The bugs above are symptoms. The underlying problem is that shutdown is **opt-in and self-managed**: every component invents its own signal handling, exit condition, and cleanup, and the process relies on all of them being right. The fix is to invert that: the harness owns shutdown, and a component author's obligations shrink to two small, local contracts.

### 4.1 One `ShutdownController`, cancellation tokens, tracked spawns

Built on `tokio_util::sync::CancellationToken` + `tokio_util::task::TaskTracker`. The controller is the *only* thing that listens for SIGTERM; components never touch `tokio::signal`.

```rust
#[async_trait]
pub trait PipelineComponent: Send + Sync {
    fn name(&self) -> &str;

    /// Runs until cancelled. Select on ctx.cancelled() at every wait point.
    async fn run(&self, ctx: &ComponentScope) -> Result<()>;

    /// Called after `run` returns/cancels. Flush state, emit final batch.
    /// The harness bounds this with a deadline — it cannot hang the process.
    async fn drain(&self, ctx: &ComponentScope) -> Result<()>;
}

impl ComponentScope {
    /// The ONLY way to spawn helper tasks (lag reporters, flush loops...).
    /// Registered in this component's TaskTracker: auto-cancelled and
    /// awaited at scope teardown. Orphans become impossible.
    pub fn spawn<F>(&self, name: &str, fut: F) -> TrackedHandle { ... }

    pub fn cancelled(&self) -> WaitForCancellationFuture { ... }
    pub fn deadline(&self) -> Instant { ... }   // remaining shutdown budget
}
```

Mapping to the bugs found:

| Bug | How the pattern eliminates it |
|---|---|
| Orphaned lag task (Part 2) | Helper tasks go through `ctx.spawn` → cancelled and awaited at scope teardown. Enforced by a clippy `disallowed_methods` lint on raw `tokio::spawn`/`std::thread::spawn` in connector crates. |
| Three racing SIGTERM handlers (1.1) | Deleted — only the controller listens. |
| Blocking crossbeam sends wedge workers (1.2) | Lint bans blocking `send`/`recv` in async code; plugin channels get an async facade (`send_timeout` raced against the token) so a dead peer yields an error, not a parked thread. |
| Inverted termination order (1.1) | Ordering lives in one place: the controller drains front-to-back. |
| Unawaited plugin drain (1.4) | Plugins are components; their scope is awaited like any other before exit. |

### 4.2 State capture = a final checkpoint, not bespoke cleanup code

Don't ask component authors to write shutdown logic — reuse the epoch machinery they already implement (Flink's stop-with-savepoint pattern):

1. Controller cancels **sources only** (front-to-back drain).
2. A source's `run` reacts to cancellation by finishing its current batch, **sending it** (fixes the partial-batch drop), emitting `SourceComplete`, and returning. Its `drain` persists source-local state (offsets, hybrid phase cursor).
3. The coordinator injects one final marker; sinks ack as they already do; the final Finalizer commits offsets/state.
4. Streams end naturally → sinks finish → plugins get `Terminate` and are **awaited** → coordinator stops.

A hybrid-source developer's entire obligation: *react to `ctx.cancelled()` at wait points, flush buffers in `drain`, handle markers correctly* — the last of which they already must do for checkpointing. No signals, no task bookkeeping, no ordering decisions.

### 4.3 The deadline ladder — the anti-hang guarantee

A component author can still write a `drain` that hangs. The guarantee can't come from trusting them; it comes from harness escalation:

```
SIGTERM
  → budget = grace_period − safety_margin, sliced per phase
  → cancel phase N, await its tracker with timeout
      → on timeout: abort() the tasks, log "component X blew its
        drain budget (waiting on: <task name>)", continue
  → after all phases: runtime.shutdown_timeout(remaining)
  → watchdog thread: hard std::process::exit() at the budget edge
```

Two properties:

- **The process always exits before k8s's grace period.** Even a wedged `rd_kafka_destroy` on a worker thread can't stop `process::exit` from a plain watchdog thread. "SIGKILL with unflushed everything" becomes "controlled exit where every well-behaved component flushed and exactly the misbehaving one is named in the logs."
- **Hangs become observable.** Today a hang is a silent pod kill; under the ladder it's a log line naming the component and task that missed its deadline.

### 4.4 Why a trait + harness rather than guidelines

A CONTRIBUTING doc saying "respond to shutdown signals, don't block, clean up your tasks" is the current state, informally — and it produced six variants of the same bug. The pattern works because the failure-prone parts (signal handling, ordering, awaiting, timeouts) are written once in the harness, and the per-component surface shrinks to two async fns whose worst-case failure is *bounded by construction*. The `tokio-graceful-shutdown` crate (SubsystemHandle, nested subsystems, per-subsystem timeouts) is this pattern packaged; given the FFI plugin boundary and DataFusion integration, a thin in-house version on tokio-util primitives is the better fit, with that crate as a reference.

### 4.5 Enforcement and migration

- **SIGTERM chaos e2e test:** run a pipeline, fire SIGTERM at a random point (mid-batch, during checkpoint, during bounded→unbounded transition), assert the process exits within N seconds *and* the sink contains every record up to the final checkpoint. This is the regression guard against a fourth signal handler appearing.
- **Migration order:** introduce `ShutdownController` + `ComponentScope`; port the Kafka source first (worst bugs, already has a half-built `shutdown_rx`), then hybrid/ClickHouse, then plugin lifecycle — deleting per-component signal handling as each ports.

---

## Part 5 — Verification pass: clarified assumptions and open questions

A second pass over the repo (and streamling-cloud) to validate the Part 4 design against how things actually work today.

### 5.1 Do we still need `SourceComplete` / `num_records_before_stop` under the new mechanism?

**How tests stop pipelines today.** E2E tests run the binary as a child process (`crates/streamling-e2e/src/streamling.rs:59-194`) and stop it exclusively via `STREAMLING__NUM_RECORDS_BEFORE_STOP` (`run_streamling_with_limit`, `streamling.rs:181-194`). The stop chain: the **sink** counts rows; at the limit it broadcasts `CheckpointMessage::SourceComplete(source_name)` over the checkpoint channel (`print.rs:115-126`, `blackhole.rs:105`, `memory.rs:162`, plugin sink `table_provider.rs:487-491`); the Kafka source polls for it on a 5ms interval that exists only when the env var is set (`kafka.rs:1146-1148`, probe at `1187-1206`); the source breaks out (dropping its final converted batch — bug 1.5) and the process winds down. So today `SourceComplete` is an ad-hoc **cancellation transport bolted onto the checkpoint data plane** — which is why it needed the `pending_checkpoint_messages` replay buffer (`kafka.rs:1129-1144`) to avoid eating markers, and why it never worked reliably.

**Answer:**

- **The trigger survives; the transport dies.** Tests still need "stop after the sink saw N rows," but it becomes one line in the sink: `controller.request_shutdown()` — the *same* code path SIGTERM takes. This is the biggest payoff: CI shutdown stops being a separate, less-tested mechanism (the original sin of the CI-only fix) and instead exercises the exact drain production uses, on every test run.
- **`SourceComplete` the message can be deleted outright.** Its remaining roles are all replaced:
  - `channels.rs` `completed_sources` cleanup so broadcast sends to dropped receivers don't error (`channels.rs:84-118`) → replaced by explicit `unsubscribe` at component-scope teardown (the API already exists, `channels.rs:67-73`).
  - Plugin checkpoint-channel cleanup at exit (`lib.rs:2017-2025`) → replaced by awaited plugin scope teardown.
  - Coordinator subscriber's handler is already a no-op (`checkpoint_management.rs:298-302`).
  - End-of-stream signaling needs no message at all: DataFusion stream end (sender dropped) is the natural EOS, and the hybrid source already relies on it (`hybrid.rs:1101-1114`).
- The kafka 5ms probe, its interval, and the replay buffer all get deleted with it.

### 5.2 Corrections and refinements to Part 4 from the code

- **Hybrid drain ≈ "let the final checkpoint finalize", not new code.** Hybrid phase state is already persisted at phase transitions with retry (`hybrid.rs:681-712`, called at `:807`), and restore has three fallback probes (`hybrid.rs:544-679`). What's *not* durable is the ClickHouse keyset cursor, which persists **only on finalized epochs** — a SIGTERM mid-bounded-phase today loses all progress since the last finalized epoch and re-scans (duplicate rows downstream). So the stop-with-final-checkpoint step in 4.2 isn't a nicety; it's what bounds re-processing on shutdown. The hybrid author's real gap is smaller than assumed: add cancellation awareness to the ClickHouse pagination loop (it currently has no shutdown check at all — confirmed; exit is only `stream.next() == None`), and the existing checkpoint machinery does the rest.
- **The hybrid marker-forwarder is already well-behaved** (250ms `recv_timeout` via `spawn_blocking`, biased select on its shutdown channel, joined and unsubscribed on exit — `hybrid.rs:1003-1054`, `1192-1194`). It's the model citizen for what `ComponentScope::spawn` formalizes.
- **The coordinator needs a small addition, not a rewrite.** The producer is purely interval-driven (`checkpoint_management.rs:321-478`); there is no on-demand trigger today. `trigger_final_checkpoint()` = a notify that short-circuits the interval sleep plus a "final" flag; the wait-for-finalization loop it needs already exists (`:358-413`). Confirmed `stop()` itself is benign: flips `running`, joins tasks, all loops poll `running` at ≤500ms (`:550-558`).
- **Plugin abort caveat for the deadline ladder.** FFI plugin tasks cannot be safely `abort()`ed mid-FFI-call. For the plugin phase, the ladder's escalation is: cancel → await with deadline → on breach, log the component, **leak the task**, and let the end-of-budget `process::exit` handle it. Never abort across the FFI boundary.
- **"Plugin future completed" must be disambiguated.** Today the dispatcher future returns `Ok(())` both on requested termination and on spontaneous exit (`dispatch.rs:358-415`). Under the new design, a plugin finishing *before* shutdown was requested should still fail the pipeline; after, it's a normal drain. The controller knows which phase it's in, so this becomes a trivial check.

### 5.3 Production deployment facts (streamling-cloud)

- The agent does **not** set `terminationGracePeriodSeconds` on pipeline pods — they get the k8s default **30s**. (A comment at `streamling-agent/src/k8s_helpers.rs:222` assumes "60s by default," which is wrong.) Meanwhile streamling's coordinator-stop timeout alone is 30s. The agent should set an explicit grace period (e.g. 120s for Jobs) and pass the budget to streamling (e.g. `STREAMLING__SHUTDOWN_BUDGET_SECS`) so the deadline ladder can slice it.
- `STREAMLING__JOB_MODE` is set by the agent (`streamling-agent/src/streamling.rs:1484`); `NUM_RECORDS_BEFORE_STOP` is never set in production — confirmed test-only, safe to repurpose/remove.

### 5.4 Open questions for the team (not answerable from code)

1. **Partial Kafka batch on streaming-mode SIGTERM:** drain-and-send it (consistent with Job mode, recommended) or drop it and rely on offset replay? Either is correct for at-least-once; sending narrows the duplicate window.
2. **Externally-built plugins** (Canton, EventBridge, community): how heavy are their `terminate()` flushes? Their phase of the deadline ladder needs a budget informed by real numbers.
3. **Multi-sink record limits:** today the first sink to hit its limit stops the whole pipeline; other sinks may have seen fewer rows. Keep that semantic for the new trigger, or wait for all sinks? (Keep-first is simpler and matches current test expectations.)
4. **Expose `request_shutdown` on the admin API?** The server already exists (`initializations.rs`); an ops-facing clean-stop endpoint falls out of the design nearly for free and gives tests a second trigger option.

---

## Part 6 — Component-by-component shutdown-hang audit

Final exhaustive pass (2026-06-12) over every component that runs during a pipeline's lifetime. Verdicts: **HANG** = can stall shutdown indefinitely (relative to a 30s grace period); **BOUNDED** = worst case bounded and roughly how long; **OK** = cannot meaningfully delay shutdown.

### 6.1 Newly found in this pass (not in Parts 1–2)

1. **Kafka sink `flush_producer` — HANG.** `loop { producer.flush(Timeout::After(10s)) }` retries **forever** until the flush succeeds (`kafka.rs:2012-2035`, called per-checkpoint at `:2514` and at stream end at `:2554`). `flush` is a synchronous rdkafka call blocking a tokio worker for up to 10s per attempt, and producers are created with `message.timeout.ms=600000` (`kafka.rs:2052`) — so with a broker outage at shutdown, queued messages keep the loop alive for up to 10 minutes, wedging a worker thread the whole time. The producer also has no `SafeKafkaConsumer`-style drop wrapper, so its `rd_kafka_destroy` runs on a worker thread at task teardown.
2. **ClickHouse *source* retries forever — HANG (prevents shutdown from starting).** The pagination loop's error path increments `query_retry_attempts`, but after 5 attempts only the *log level* changes — the loop `continue`s unconditionally with backoff capped at 30s (`clickhouse.rs:1309-1327`). Each query attempt is timeout-bounded (`SOURCE_QUERY_TIMEOUT_SECS`, `:1134-1146`), but the loop has **no shutdown check**, and neither the CH source nor the hybrid source listens for any signal. A ClickHouse outage mid-bounded-phase means the sink futures never complete, `try_join_all` never returns, and SIGTERM is never acted on.
3. **Kafka source startup wait — HANG.** `wait_for_initial_assignment_or_message` loops **forever** until partitions are assigned or a message arrives (`kafka.rs:603-643`) — each fetch has a timeout, the loop does not. The SIGTERM handler only exists inside the main consume loop, which hasn't been reached yet, so a SIGTERM during startup (broker unreachable, auth failure, stuck rebalance) is never observed. (`wait_for_assignment` by contrast is deadline-bounded, `kafka.rs:573-588`.)
4. **OTLP telemetry shutdown — HANG.** `main.rs:170-173` calls `provider.force_flush()` and `provider.shutdown()` with no timeout. If the collector endpoint is unreachable/stalled, the in-flight export blocks indefinitely. Ironically `shutdown_with_timeout` exists on the wrapper (`telemetry/provider.rs:230-232`) but is never used from main.
5. **Panic hook can hang a panicking process — HANG (conditional).** `install_global_panic_hook` calls `terminate_all_plugins()` from inside the panic handler (`error_format.rs:18-48`), which does blocking bounded-channel sends (`plugin.rs:579-585`). A panic while a plugin's input channel is full blocks the panicking thread forever — the process neither crashes nor exits.

### 6.2 Full component matrix

| Component | Verdict | Mechanism / bound | Citations |
|---|---|---|---|
| Kafka source main loop | **HANG** | SIGTERM only observed inside the select; blocked `tx.send().await` (backpressure) or `CommitMode::Sync` commit makes the signal unobservable; partial batch dropped on exit | `kafka.rs:1168-1298, 1403, 1459` |
| Kafka source startup waits | **HANG** | Unbounded assignment/message wait, no signal handling exists yet | `kafka.rs:603-643` |
| Kafka lag task | **HANG** | Never signaled outside job mode; raw `StreamConsumer` dropped on worker thread → `rd_kafka_destroy` deadlock at runtime teardown | `kafka.rs:795-907, 995-1036, 1669-1671` |
| Kafka sink | **HANG** | Infinite blocking flush loop; 10-min message timeout; unwrapped producer drop | `kafka.rs:2012-2035, 2052, 2514, 2554` |
| ClickHouse source (and hybrid bounded phases) | **HANG** | Infinite query retry with no shutdown check; no signal handling in CH/hybrid sources | `clickhouse.rs:1131-1330` |
| ClickHouse sink | **HANG** | `retry_forever_with_backoff_async` on every write; backpressure freezes the source outside its select | `clickhouse.rs:735, 823, 860`; `retry.rs:11` |
| Plugin sink writes | **HANG** | Blocking bounded crossbeam `send().unwrap()` on tokio workers to a possibly-dead plugin | `plugin/table_provider.rs:399-449` |
| Plugin termination path | **HANG** | Same blocking send in `terminate_plugins`; plugin `terminate()` itself unawaited/unbounded | `plugin.rs:575-589`; `lib.rs:2028` |
| Plugin shutdown watcher | cause of hangs elsewhere | Kills plugins first, inverting drain order | `plugin.rs:110-133` |
| Panic hook | **HANG** (during panics) | `terminate_all_plugins` blocking send inside panic handler | `error_format.rs:18-48` |
| OTLP telemetry flush/shutdown | **HANG** | No timeout on flush/shutdown to a dead collector; `shutdown_with_timeout` exists unused | `main.rs:170-173`; `telemetry/provider.rs:226-232` |
| Tokio runtime teardown | amplifier | Any worker wedged in a blocking call (rd_kafka_destroy, crossbeam send, sync flush) blocks `Runtime::drop` forever — converts every wedge above into a permanent process hang | `main.rs:192` (`#[tokio::main]`) |
| Checkpoint coordinator (3 tasks) | **BOUNDED** | All loops poll `running` at ≤500ms; `stop()` joins; outer 30s timeout (= whole default grace period — shrink it) | `checkpoint_management.rs:174-558`; `lib.rs:2033` |
| Checkpoint channels registry | OK | Unbounded crossbeam sends never block; cleanup keyed on SourceComplete/unsubscribe | `channels.rs:41-122` |
| Hybrid marker-forwarder | **BOUNDED** | 250ms recv_timeout via spawn_blocking, biased select, joined + unsubscribed | `hybrid.rs:1003-1054, 1192-1194` |
| Hybrid `save_state` | **BOUNDED** | 3 attempts, fixed backoffs | `hybrid.rs:681-712` |
| HTTP/webhook provider | **BOUNDED** | Single `operator_timeout_sec` per request; no retry loops | `http.rs:68` |
| State backends (in-memory / sqlite / postgres) | **BOUNDED** | No background threads or retry loops in the backends; postgres bounded by sqlx pool acquire timeout. Caveat: callers use `.unwrap()` on results (`kafka.rs:654`) — panic, not hang | `streamling-state/src/*` |
| Rebatch / batch accumulator | OK | Timer polled inline with the stream; `flush_all` on stream end | `batch_accumulator.rs:289-292` |
| Broadcast operator | OK | Shutdown-aware; exits with streams | `operators/broadcast/` |
| LiveDataInspect | **BOUNDED** | Watch-signal exit, ≤ refresh interval (default 30s — worth shrinking) | `inspect.rs:79-153` |
| Admin API server | OK | `handle.abort()`; axum task cancels cleanly | `admin_api.rs:135-149`; `main.rs:161-163` |
| Metrics recorder / logging | OK | Synchronous, no background workers | `telemetry/recorder.rs:88-95`; `logging.rs:14-82` |
| print / blackhole / memory sinks | OK (test-only) | Exit on stream end or record limit | `print.rs:115-126` |

### 6.3 Second sweep (pattern-based): six more findings

A wider, pattern-based pass (every `loop` without a shutdown check, raw thread spawns, blocking calls in async, `block_on`, HTTP clients without timeouts, the WASM runtime) found:

1. **External handler operator — HANG.** `send_with_retry` retries network errors, timeouts, 408/429/5xx **indefinitely** with backoff (its own doc says so — `operators/external_handlers.rs:495-511`; the loop in `retry.rs:119-150` only exits on non-retriable errors). Per-request timeouts exist (`operator_timeout_sec`, 10s connect — `external_handlers.rs:429-430`), but a down endpoint stalls the operator forever: sink futures never complete and the drain never starts. Same shape as the ClickHouse sink.
2. **WASM transform runner — HANG (un-cancellable).** `extism Plugin::call` is invoked with no timeout, fuel, or epoch deadline anywhere in `operators/wasm_runner.rs` (`:915`, `:974`). A WASM plugin stuck in a loop blocks a worker thread synchronously with **no possible cancellation** — the deadline ladder's `abort()` can't help; only the end-of-budget `process::exit` can. (Extism/wasmtime epoch interruption exists and should be enabled with a per-call deadline.)
3. **ClickHouse HTTP client has no request or connect timeout** (`clickhouse.rs:1451-1463` — only pool/keepalive settings). This *upgrades* the CH sink/source findings: not only do they retry forever, a **single attempt** against a black-holed endpoint hangs forever (the source's per-query `tokio::timeout` covers it, the sink's `retry_forever` does not — its inner future just never returns).
4. **`futures::executor::block_on` parks worker threads** in CH planning/startup paths — `load_split` (`clickhouse.rs:261`), `fetch_schema` (`:1636`), `fetch_sorting_keys` (`:1659`) — unbounded given finding 3 — and in the dynamic-table UDF (`functions/dynamic_table.rs:108-119`, bounded by sqlx pool timeouts). Kafka's schema fetch does this correctly (`block_in_place` first, `kafka.rs:297-299`) but its bound depends on `schema_registry_converter`'s internal client timeout (unverified — see open questions).
5. **Plugin ack sends retry forever (dispatcher can't terminate).** Plugin-side channel sends use `try_send` + sleep (`streamling-plugin/src/ffi.rs:393-430` — yields, doesn't wedge a thread) but loop **indefinitely** by default. The host drains the plugin's output channel only opportunistically once per batch (`plugin/table_provider.rs:457`); if the sink future is cancelled or slow, the dispatcher loops in the checkpoint-ack send (`dispatch.rs:87-98`) and never reaches `Terminate` — the execution future never completes.
6. **A swallowed SIGTERM is worse than no handler.** Once any `tokio::signal` registration happens (plugin watcher, kafka source), the default kill-on-SIGTERM disposition is permanently replaced process-wide. In windows where no live listener exists — between kafka outer-loop iterations (`kafka.rs:1158-1162` recreates the stream each iteration), or after the one-shot plugin watcher has already fired — a SIGTERM is **swallowed entirely**: no drain, no death, nothing. k8s then SIGKILLs at grace expiry having never had any effect.

Verified clean in this sweep: `safe_take`'s thread-per-call (joined immediately, `streamling-common/src/utils/arrow.rs:207-230`), flink-compat (pure utils), config preprocessors (no I/O loops), `wrapping.rs` `block_on`s (test-only code), side outputs' metrics task (yields, cancellable).

### 6.4 Third sweep (previously unaudited components): five more findings

1. **Postgres sink — HANG.** Inserts and deletes go through `retry_forever_with_backoff_async` with no timeout or cancellation (`table_providers/postgres/execution.rs:38-66, 89-113`, fanned out via `util/parallel.rs:13-51`). Same shape as the ClickHouse sink.
2. **Broadcast fan-out — HANG (conditional).** The shared-scan broadcast task retries `try_send` into each consumer's bounded channel **forever** with a 1ms sleep (`operators/broadcast/broadcast_stream.rs:57-77`, spawned at `:51`). A closed consumer exits cleanly, but a consumer that is alive-yet-stalled (e.g. a sink stuck in a retry loop) pins the broadcast — and with it every *other* consumer of the shared scan. It yields, so runtime teardown can cancel it, but the drain never completes.
3. **Preprocessor plugins — HANG (startup).** After sending the topology to a preprocessor plugin, the host does a synchronous, timeout-less `output_receiver.recv()` (`plugin/preprocessor.rs:73-76`). A preprocessor that never responds stalls startup, during which no signal handling exists at all.
4. **Plugin source stream never terminates — by design.** The `PluginSourceExec` loop (`plugin/table_provider.rs:149-269`) has **no exit condition**: `SourceComplete` from the coordinator channel falls into the "ignore other messages" arm (`:261-263`). The stream never ends, so plugin-source pipelines can only exit via the `select!` plugin-future race (1.3) — sink futures are *always* cancelled, never drained, on every plugin-source shutdown. It also forwards markers to the plugin with the same blocking `send().unwrap()` as the sink path (`:244-259`).
5. **Schema-registry fetch retries transient errors forever** (`table_providers/kafka/schema_registry.rs:75-140` via `retry_if_retriable`) — bounded only for *non-retriable* errors; a down registry at startup loops indefinitely (same family as the startup waits). The AZ-detection HTTP call is fine (5s timeout, `kafka/config_optimizer.rs:117-155`).

Verified clean in this sweep: pg_aggregation (one-shot setup, pool closed — `operators/pg_aggregation.rs:584-643`), scan-sharing registration (atomic-counter bounded), checkpointable/rebatch/filter/projection/unnest (pure streaming, terminate with input), clickhouse/kafka subdirectory helpers, util/parallel (bounded iff callee bounded).

### 6.5 What the matrix says about the design

Twenty-two distinct HANG vectors across three sweeps, clustering into four shapes — each mapped to a Part 4 mechanism:

1. **Unbounded retry/wait loops with no shutdown check** (CH source, CH sink, Postgres sink, Kafka sink flush, Kafka startup, schema-registry fetch, external handlers, broadcast fan-out, plugin ack sends, the exit-less plugin source loop, OTLP). Fix shape: every retry/wait loop takes the cancellation token; the retry helpers (`retry_forever_with_backoff_async`, `retry_if_retriable`, `send_with_retry`, `try_send_batch_with_retry_forever`) grow cancelled-aware variants and the bare loops migrate to them. The deadline ladder catches whatever is missed.
2. **Blocking calls on tokio workers** (crossbeam sends, sync Kafka commit/flush, rd_kafka_destroy, `futures::executor::block_on`, un-deadlined WASM calls). Fix shape: lint bans + async facades + `spawn_blocking` for unavoidable FFI teardown + extism epoch deadlines for WASM; the end-of-budget `process::exit` watchdog guarantees the process dies even when one slips through (the WASM case is *only* fixable by the deadline or the watchdog — `abort()` cannot interrupt it).
3. **Orphaned/unsignaled helper tasks** (lag task; anything spawned outside a tracked scope). Fix shape: `ComponentScope::spawn` only.
4. **Missing I/O timeouts** (ClickHouse reqwest client, OTLP exporter; schema-registry client unverified). Even with cancelled-aware retry loops, an attempt whose future never resolves stalls the loop body. Fix shape: every HTTP client gets request + connect timeouts at construction; add a clippy lint or wrapper for `reqwest::Client::builder()` without `.timeout()`.

Notably, the **signal-observation gaps** (startup waits, backpressured sends, sync commits, listener-recreation windows where SIGTERM is swallowed outright — §6.3.6) mean today's SIGTERM can have no effect whatsoever — the pipeline doesn't even *start* shutting down. The centralized controller fixes observation (one dedicated task always owns the signal); the token-aware loops fix reaction.

### 6.6 Audit termination

The audit ran as repeated sweeps with widening lenses until a sweep found nothing new:

- **Sweep 1** (by component): plugin lifecycle, kafka source/lag, coordinator, sinks → Parts 1–2 and §6.1.
- **Sweep 2** (by pattern: loops, raw threads, blocking calls, `block_on`, client timeouts, WASM) → §6.3.
- **Sweep 3** (previously unaudited components: postgres, broadcast, preprocessor, plugin source, operators) → §6.4.
- **Sweep 4** (fresh-eyes pass over the remainder: config, common, functions, utils, session/topology, validate, derive, state trait layer; plus long-sleep and thread-join greps) → **nothing new**. Everything it surfaced was already on the list, with one refinement worth recording: the `is_empty()`-then-blocking-`recv()` pattern (`dispatch.rs:105-106`, `plugin/table_provider.rs:232-233, 457-458`) is a check-then-act race that is benign today only because each channel happens to have a single consumer — fragile, and the async-facade migration (shape 2) removes it anyway.

Scope notes: plugin *implementations* that live outside this repo (Canton, EventBridge, community plugins) were not audited — their `terminate()` behavior is an open question (§5.4.2); `schema_registry_converter`'s internal HTTP timeout is unverified (external dependency).

---

## Appendix — How to confirm the non-plugin hang on a live pod

On a hung pod, check for the **absence** of the `"Lag task shutting down for ..."` log line (`kafka.rs:906`) after the `"Shutting down Kafka consumer: unsubscribing and unassigning"` line (`kafka.rs:1470`). A pod that printed the second but not the first is stuck in the lag-consumer teardown deadlock.

For the plugin hang, the tell is `"Terminating because a plugin future completed gracefully"` (`lib.rs:1984`) appearing while sink writes were still in flight, followed by silence.
