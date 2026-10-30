# Node-wait metrics: measurement model and pitfalls

`streamling_node_wait_milliseconds_total` helps localize where a streaming
pipeline is idle, but it is not a direct measurement of downstream service
latency. Its meaning depends on the async boundary at which it is measured.
Buffering, prefetch, concurrency, and stream polling can move observed wait
between nodes without changing the underlying bottleneck.

This document describes those boundaries, the resulting interpretation rules,
and the checks required when changing an operator or connector.

## What is measured

### Linear edges

For a normal single-consumer node, `WrappingExec` records:

- `state="blocked"`: time between yielding a batch and being polled again by
  the downstream consumer. For a task-decoupled producer that claims the
  `TaskWaitMeter` it is the task's output send wait instead (see below).
- `state="starved"`: time spent awaiting the next input batch from upstream.
  A wait that ends in end-of-stream or an error is teardown: its unflushed
  tail is dropped (see below).

Every series is a `NodeTime`. A process-wide flusher reports each open span
every second, so a wait shows while it lasts, not as one spike when it ends.
The unflushed tail of a wait that ends in end-of-stream, an error, or
cancellation is dropped. A span's close only banks its time; the flusher and
`Drop` emit, so the data path never takes the recorder's locks.

DataFusion records a SQL transform's compute only when an operator's poll
ends. The transform's forwarder task marks each poll of its subtree on the
`TaskWaitMeter`, so a flush during that poll banks `starved` only up to the
poll's start, and the poll's `busy` shows when the poll ends.

### Task-decoupled operators

An operator that runs its loop in a spawned task (HTTP handler, WASM, plugin,
and the Kafka, ClickHouse, file, and hybrid sources) hides that loop behind an
output channel. The wrapper's input poll then waits
on the operator's own work, not on upstream, so it cannot measure `starved`.

Such an operator claims the `TaskWaitMeter` its wrapper installs in the
`TaskContext` and brackets its awaits:

- input await (`next`, or `wait_input` for an external system) → `starved`;
- output send await → the edge's `blocked` (the wrapper hands its series to
  the meter, and does not time `blocked` itself). While the wrapper works on a
  batch it took (side outputs, live inspect), it pauses that series: a send
  held back by the wrapper is in no state;
- a wait on the sinks (`untimed`: the terminal checkpoint finalize) → no
  state;
- everything else in the task → `busy`.

The meter then reports the node's `starved` and `busy`. A new task-decoupled operator must claim the
meter, or its work reads as `starved`. Brackets end when their future is
dropped, so a wait inside a losing `select!` branch still counts.

A source is `starved` only while its external system has no data for it: a
Kafka broker wait, a ClickHouse failure backoff, an idle file poll. Fetching
and decoding available data is `busy`. A hybrid source reserves the meter at
`execute`, and each phase source claims it in turn; the latest claim owns it.
Each phase's own `WrappingExec` shares the reserved meter and emits no
`node_wait`, so the hybrid's triad is under its YAML name, not the phase ids.
A phase's sends go to the hybrid driver, not the edge, so they are in no
state. The driver times its own work (normalize) as `busy` and its edge sends
as `blocked`. The phase and the driver run at the same time, so a hybrid's
states can add up to more than its wall time.

A plugin source cannot be split: the plugin fetches and decodes out of
process, and the plugin protocol carries no wait signal. Its whole input poll
is `starved`, and its `busy` is zero.

A SQL transform's forwarder task cannot claim the meter: its input await
drives the SQL subtree inline, so compute runs inside the await. The wrapper
reads the subtree's DataFusion compute for its partition (`OwnWork`): that
compute is `busy`, and it is subtracted from the wrapper's `starved`, except
compute recorded while that wait is paused at a wake (below). The forwarder
only marks its subtree polls on the meter (see above). The split is
exact when the subtree keeps its input partitioning; a repartition inside the
subtree spreads one partition's compute across others.

### Sinks

`WrappingDataSink` records `state="starved"` for the time the sink waits on
its input stream, and `state="busy"` for the time the connector holds a
delivered batch before it asks for the next one. For a sink that prefetches,
`busy` starts when its input wakes, not when it polls again (see below).

The blocked series is attributed to the edge:

```text
id=<producer>, downstream_id=<immediate consumer>, state="blocked"
```

This is a **poll-demand measurement**. A long yield-to-resume gap (or send
wait, for a claiming producer) means the consumer stopped asking for more
data. It does not prove what the consumer did during that gap.

An operator that decouples through a channel task but does not claim the
meter (`CheckpointableExec` for a SQL transform, `PluginSourceExec`) keeps the
wrapper's yield-to-resume `blocked`. Its task keeps working while the wrapper
is `blocked`, until its output channel is full.

### Fan-out edges

A fan-out producer cannot use one yield-to-resume span to distinguish multiple
consumers. Its `WrappingExec` blocked emission is therefore suppressed.
`BroadcastStream` measures time blocked sending to each bounded consumer
channel and emits one series per edge.

Per-consumer sends happen concurrently, so their spans overlap. The producer's
own blocked time is the slowest edge:
`max by (id) (sum by (id, downstream_id) (rate(node_wait{state="blocked"}[w])))`.
`sum by (id)` over edges is aggregate per-edge effort and may be up to N times
wall-clock blocking for N simultaneous consumers.

### Partitions

Each partition of a node runs its own `WrappingExec` stream, and all of them add
into the same series: there is no partition label. A node with N partitions
can accrue up to N seconds per second in each state. Compare a node's states
as ratios (`state / (starved + busy + blocked)`); a ratio does not change with
the partition count. The states do not split one timeline: a node's work can
run in another task while its wrapper waits, so the ratios can add up to more
than 1. The overlaps:

- a hybrid source's phase and driver (see above);
- a SQL transform's subtree compute (`busy`), which runs in its forwarder
  task: while the wrapper is `blocked`, the forwarder keeps computing until
  its output channel (`internal_buffer_size`, 10 by default) is full;
- a SQL transform's subtree compute that runs off its partition's task (a
  repartition inside the subtree).

### Sink service time

Connector work such as an HTTP request or database insert is a different
measurement. Connectors record it as sink `elapsed_compute`. It should not
automatically be interpreted as producer blocked time: the sink may have queue
or concurrency capacity available while doing that work. A connector that
writes in a background task accepts batches quickly, so its `busy` grows only
once its own queue is full.

## Why wait moves between nodes

### Pull-driven batching preserves backpressure

`RebatchExec` uses `AsyncBatchAccumulator` to merge or split batches, but it is
still pull-driven:

1. Pull input until the current output batch is ready.
2. Yield that output.
3. Suspend until the consumer polls again.

It may turn many short yield gaps into one long gap, but it does not introduce
a background producer or look-ahead queue. Total blocked time therefore
continues to propagate through the rebatcher.

Batching alone is not the same as prefetch.

### Prefetch: an input wait ends at the input's wake

A consumer such as `ready_chunks(N)` polls all currently-ready input, gets
`Pending`, and then does slow work before it polls again. The node's input
poll stays `Pending` through that work. `WakeTimed` (`telemetry/wake_timed.rs`)
switches state in the waker, so the switch shows live, while the consumer
still works:

- the input wait (`starved`) runs from the poll to the wake;
- the gap from the wake to the consumer's next poll is the consumer's: `busy`
  for a sink, `blocked` on the edge for a `WrappingExec`;
- the final poll, which runs upstream work, is input wait again.

The wait pauses at the wake rather than ending, so a wait that ends in
end-of-stream or an error is still dropped, not `starved`.

Limits:

- The gap includes scheduler delay between the wake and the task running.
- tokio's cooperative budget can return `Pending` for a ready input and
  defer the wake until the task yields. That wait is booked `starved`,
  though the batch was ready.
- A connector that prefetches past end-of-stream writes its last chunk after
  the measured stream ended; that last write is not measured.
- A wake that brings no batch (the next poll is `Pending` again) still
  counts its gap as the consumer's; the wait then resumes.

### Bounded channels hide work until full

A bounded channel decouples producer and consumer:

- While capacity remains, sends complete quickly and there is no producer
  backpressure to report.
- Once full, blocked-send time is a clean edge-level signal.

Queue capacity therefore delays the appearance of blocked time. Tests must run
long enough to fill the queue and maintain overload.

An unbounded queue may prevent backpressure from ever reaching the host. Its
queue depth and service latency require separate metrics.

### Concurrency changes both behavior and measurement

Changing from chunked `join_all` execution to continuously replenished bounded
concurrency can make yield-to-resume blocking visible, but that is not a
metrics-only change. It can also change:

- request start times and burst shape;
- throughput and tail latency;
- head-of-line blocking;
- rate-limit and retry behavior;
- cancellation after an error or test record limit;
- checkpoint acknowledgement timing.

Do not change scheduling solely to make a metric match an expected shape
without treating the change as a connector behavior change.

## Exactly one blocked emitter per edge

Every edge must have one blocked emitter:

- Linear edge: producer `WrappingExec` yield-to-resume, or the task's output
  send when the producer claims the `TaskWaitMeter` (the wrapper hands its
  series to the meter and does not time it).
- Fan-out edge: per-consumer `BroadcastStream` send; producer wrapper
  suppressed.
- Any future explicitly metered queue boundary: queue send; the producer
  wrapper's yield-to-resume must not time the same edge.

If both wrapper suspension and queue send are emitted, the edge is
double-counted. If suppression and replacement emission get out of sync, the
edge disappears.

Attribution and measurement are separate concerns:

- `DownstreamAttributionRule` decides the `downstream_id`.
- The execution boundary decides the measured duration.

A correctly named edge can still have a misleading magnitude.

## Plugin boundaries

Plugins do not need to emit `node_wait` themselves:

- Plugin sources and transforms are observed by host `WrappingExec` nodes. The
  host plugin transform operator claims the `TaskWaitMeter`, so time the plugin
  spends on a batch is busy, not `starved`, and its output send wait is the
  edge's `blocked`. `PluginSourceExec` does not claim it: its edge's `blocked`
  is the wrapper's yield-to-resume.
- A plugin sink's feeding edge is observed by its upstream `WrappingExec`.
- Plugin input/output channels are bounded, so channel saturation propagates
  backpressure to the host boundary.

Plugin processing latency while its input channel still has room is not
blocked time. Plugins should emit their own service latency, queue depth, retry,
or remote-I/O metrics through the existing plugin metrics channel.

If a plugin moves accepted work into an internal unbounded queue, the host
cannot infer its backlog. The plugin must expose that state or use a bounded
boundary. Plugins should not emit host `node_wait` series directly because that
can duplicate the host-owned edge.

## Interpreting dashboards and queries

- Scope by `service_instance_id`; node names can be shared across pipelines.
- Use rates over a sustained window for bottleneck diagnosis. Short cumulative
  totals are sensitive to startup, shutdown, and buffer-fill transients.
- A near-zero blocked edge does not prove the consumer is fast. Check sink
  `elapsed_compute`, output throughput, queue metrics, and adjacent
  blocked/starved edges.
- A high starved value does not prove the pipeline lacks input. A decoupling
  consumer can shift downstream pressure into a node-local upstream await.
- For fan-out, use `max` over edges, not `sum`, when estimating wall-clock
  blocking.
- Compare states as ratios: every partition adds into the same series.
- Counter resets occur on process restart; do not compare raw totals across
  service instances.
- `downstream_id=""` is only the unresolved linear fallback. It should be rare.

`elapsed_compute` keeps its `main` semantics for `WrappingExec`: one
observation per delivered batch, the wrapper's input poll time. A SQL
transform's observation is its subtree compute only (one without subtree
metrics records poll time). The poll-time observation
is temporary: it goes once dashboards read `state="busy"` and a batch counter. Read busy from `state="busy"`, never as
`elapsed_compute - starved`. Sink connector service time is unaffected by this
compatibility rule.

## Test design

A useful wait-metric test should:

1. Pin both `id` and `downstream_id`.
2. Use a controlled slow consumer and a fast control.
3. Generate more work than all intervening buffer capacities.
4. Sustain overload longer than startup and metric-export intervals.
5. Assert a ratio or rate against the control where possible, rather than a
   tiny absolute non-zero value.
6. Verify output throughput so a missing metric is not mistaken for missing
   traffic.
7. Verify there is exactly one blocked emitter for the edge.
8. Check `starved` separately.
9. Exercise checkpoint markers, errors, shutdown, and ordering when the change
   touches buffering or concurrency.
10. Run with realistic streaming arrival timing; a fully preloaded source can
    produce different polling behavior.

Emit through `NodeTime`: it carries the sub-millisecond remainder (truncating
every batch independently can turn real high-throughput wait into zero) and
reports open spans while they last.

## Review checklist for new operators and connectors

Before adding or changing buffering, batching, concurrency, or plugin channels:

1. Where is the actual async backpressure boundary?
2. Is the queue bounded, and what is its capacity?
3. Does the consumer prefetch until `Pending`, up to a count, or in a
   background task?
4. Which component is the sole blocked emitter for each edge?
5. Does another emitter need suppression?
6. Are service time and producer blocked time being kept distinct?
7. Can wait be reclassified as `starved` or shifted to an adjacent edge?
8. Are fan-out spans concurrent, requiring `max` for wall-clock aggregation?
9. Do checkpoint ordering, acknowledgements, cancellation, and shutdown remain
   correct?
10. Do plugins expose internal queues or service work the host cannot observe?
11. Does the test sustain enough overload to exhaust every buffer?
12. Is a scheduling change being presented honestly as a behavior change
    rather than telemetry-only instrumentation?

