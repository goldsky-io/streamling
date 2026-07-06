# Plan: unified node-wait state metric (replaces `backpressure`, subsumes `input_wait`)

> **Status:** Implemented (reworks PR #38). See `telemetry/accumulator.rs`, `telemetry/recorder.rs`, `operators/wrapping.rs`, `operators/broadcast/broadcast_stream.rs`, and the `Node-wait metric` section of `AGENTS.md`.
> **Decision:** Unify the two *idle* states — **blocked** (formerly the `backpressure` counter) and **starved** (the previously-planned `input_wait`) — into a single state-labeled counter `streamling_node_wait_milliseconds_total{state="blocked|starved", downstream_id}`. Leave `elapsed_compute` (**busy**) as its own existing metric. Scope is contained to `streamling-core`.
> **Supersedes:** the earlier "split `input_wait` out of `elapsed_compute`" plan. `input_wait` is no longer a standalone histogram; it becomes `state="starved"` on this counter.

## Why this, and why now

The just-added backpressure counter and the planned `input_wait` both measure the same *kind* of thing: **time a node is idle** — either because it is waiting on a downstream consumer to accept output (blocked) or waiting on upstream to produce input (starved). Modeling them as two states of one metric is the CPU-utilization pattern (`node_cpu_seconds_total{mode=...}`) and lets a dashboard resolve every node into the classic **starved / busy / blocked** triad.

**Now is the cheapest possible moment.** `streamling_backpressure_milliseconds_total` is unmerged (open PR #38) and nothing depends on it yet. Reshaping it before it ships is free; renaming after dashboards/alerts exist would be a breaking migration. This plan therefore **reworks PR #38** rather than adding a metric on top of it.

The measurement machinery already built for backpressure is reused wholesale — only the emitted **metric name and tags** change. Attribution roles, the optimizer pass, the accumulators, and the single-emitter invariant are all unchanged.

## The metric

- **Instrument:** one monotonic `u64` counter in milliseconds.
- **Internal name:** `node_wait` (recommendation — see open questions for naming).
- **Exported name:** `streamling_node_wait_milliseconds_total` (OTel Prometheus exporter appends the unit and `_total`).
- **`state` label:** `"blocked"` or `"starved"`.
- **`downstream_id` label:** the consumer for a `blocked` edge; **always present**, set to `""` when not applicable (starved rows, and unattributed blocked). See "Label consistency" below.
- **Other tags:** the node's standard tag set (`id`, `topology_node_type`, `operator_type`, `service_instance_id`, telemetry labels) — same as every other per-node series.

| State | When it accrues | Emitter | `downstream_id` |
| --------- | ------------------------------------------------------------ | -------------------------------------- | --------------------------- |
| `blocked` | node suspended at `yield` after producing a batch, waiting for downstream to accept it; **or** blocked on a full fan-out channel | `WrappingExec` (linear) / `BroadcastStream` (fan-out) | consumer name (`""` if unattributed) |
| `starved` | node waiting in `data.next().await` for its input to be ready | `WrappingExec` | `""` |

The complement — `busy` — remains `streamling_elapsed_compute_milliseconds` and is **not** part of this metric.

## What changes

### 1. Registration (`recorder.rs` ~L818–827)

Replace the `backpressure` counter registration with a `node_wait` counter (unit `ms`, description covering both idle states). Because backpressure is unmerged, this is a clean rename — **no alias, no dual-emit.**

```rust
count_registry.insert(
    String::from("node_wait"),
    meter
        .u64_counter(add_service_prefix("node_wait"))
        .with_description(
            "Time a node is idle rather than doing useful work, split by state: \
             blocked (held back by a downstream consumer, attributed via downstream_id) \
             or starved (waiting on upstream for input), milliseconds",
        )
        .with_unit("ms")
        .build(),
);
```

### 2. `blocked` emission (rename of today's backpressure)

The measurement is unchanged; only the metric name and the added `state` tag change.

- **`wrapping.rs` (~L688–712)** — the `BackpressureAccumulator` yield→resume path. For each role:
  - `Edge(id)` → `record_count_w_tags("node_wait", ms, vec![("state","blocked"), ("downstream_id", id)], metadata_id)`
  - `Unattributed` → same, with `("downstream_id", "")`
  - `FanOutProducer` → still suppressed (emitted by the broadcast layer)
- **`broadcast_stream.rs` (~L204–219)** — the `BlockedSendAccumulator` per-consumer path. Data-plane and control-plane branches both emit `"node_wait"` with `("state","blocked")` plus the existing `downstream_id`.

`BackpressureRole` / the `DownstreamAttributionRule` optimizer pass stay exactly as-is — they already encode "who is the blocked edge's consumer." (Optionally rename `BackpressureRole` → `EdgeRole` for clarity; not required.)

### 3. `starved` emission (new; replaces the `input_wait` histogram idea)

In `wrapping.rs` (~L716–728), `batch_elapsed` (time in `data.next().await`) is folded into `elapsed_compute`. **Additively** record it as `state="starved"` on the counter as well (dual-emit — see §4 for why we keep the `elapsed_compute` fold), using a **remainder-carry accumulator** so sub-millisecond waits are lossless and directly comparable to `blocked`:

```rust
let batch_elapsed = batch_start.elapsed();
starved.add(batch_elapsed);
let starved_ms = starved.take_whole_millis();
if starved_ms > 0 {
    metrics_recorder.record_count_w_tags(
        "node_wait",
        starved_ms,
        vec![("state", "starved"), ("downstream_id", "")],
        &metric_metadata_id,
    );
}
// ... and, in the Ok branch, KEEP the existing fold for backward compatibility:
metrics_recorder.record_elapsed_compute(batch_elapsed, &metric_metadata_id);
```

`BackpressureAccumulator` and `BlockedSendAccumulator` are both "remainder-carry ms accumulators." Rather than add a third copy, generalize them into one shared `MillisAccumulator` (add duration, drain whole millis, retain the remainder) and use it for all three sites (blocked yield→resume, blocked-send, starved). This is a small, mechanical refactor that removes duplication.

> Note the counter choice for `starved` (vs. the histogram in the old plan): keeping all idle states as counters with the same accumulator makes the triad summable losslessly and keeps one code path. The only thing we lose vs. a histogram is per-batch wait *distribution*, which the triad doesn't need.

### 4. `elapsed_compute` compatibility — dual-emit (chosen)

`elapsed_compute` is a pre-existing, deployed metric (likely consumed by the companion Grafana dashboard). Moving `batch_elapsed` off it would silently change source/transform `elapsed_compute` — for a `WrappingExec`-wrapped source it would drop to the ~1 ms per-batch seed, breaking any dashboard using it as a latency proxy. To keep this **non-breaking** we chose the additive path:

- **(C) Dual-emit (chosen):** keep folding `batch_elapsed` into `elapsed_compute` **and** emit `node_wait{state="starved"}`. `elapsed_compute` keeps its exact historical value (no dashboard breaks); `starved` is purely additive. Pure compute is recoverable as `elapsed_compute - node_wait{state="starved"}` (clamp ≥ 0): `≈ seed` for a source, `≈ DataFusion compute` for a SQL transform. Documented in `AGENTS.md` with a deprecation note.
- **(A) Clean cut (deferred):** a future release drops the `elapsed_compute` input-wait fold so it means *compute only*. Do this once consumers have migrated to `node_wait{state="starved"}`.

Cost of (C): the input-wait span lives in two series, so don't naively sum `elapsed_compute + node_wait{state="starved"}`. Sinks are unaffected either way (their `elapsed_compute` is connector-recorded service time, not `batch_elapsed`).

## Label consistency: always emit `downstream_id`

In OTel/Prometheus each distinct attribute set is a separate series. If `blocked` carries `downstream_id` but `starved` omits it, the two states have different label *keys*, so cross-state arithmetic in PromQL (e.g. `busy_fraction = busy / (busy + starved + blocked)`) requires pre-aggregating the `downstream_id` label away from `blocked` first.

To keep queries frictionless, **always emit `downstream_id`**, using `""` for `starved` and for unattributed `blocked`. Then every `node_wait` series has identical label keys and cross-state math "just works" without `sum without (downstream_id)` gymnastics.

## The payoff: the starved / busy / blocked triad

| State | Series | Meaning |
| ----------- | ---------------------------------------------------- | -------------------------------------------------------------------- |
| **starved** | `node_wait{state="starved"}` | waiting on upstream (slow source / backpressure arriving from below) |
| **busy** | `elapsed_compute - node_wait{state="starved"}` | this node is the CPU/service bottleneck |
| **blocked** | `node_wait{state="blocked", downstream_id=...}` | held back by a specific downstream consumer |

A node's dominant state says whether it *is* the constraint (busy), a victim of something upstream (starved), or a victim of something downstream (blocked) — and `downstream_id` names the culprit for the blocked case. Two of the three states live in one metric; `busy` is `elapsed_compute` minus the `starved` fold (see §4) until the deprecated fold is removed, after which `elapsed_compute` alone is busy.

## What is explicitly out of scope

- **Folding `busy`/`elapsed_compute` into `node_wait`.** That would require touching every sink connector (`postgres`, `clickhouse`, `kafka`, `http`, `blackhole`) that records `elapsed_compute` directly, plus the DataFusion metric-folding path and existing e2e/dashboards — a cross-crate breaking change. The triad is still fully derivable with `elapsed_compute` left separate.
- Re-plumbing the `CheckpointableExec` channel or the transform execution model.

## Caveats & limitations

- **Transform entanglement.** For a channel-decoupled transform, `starved` (`batch_elapsed`) is *input starvation*; the compute it waits on runs concurrently in the `CheckpointableExec` pump task (where the DataFusion timers live). So `starved` and the transform's `elapsed_compute` are not perfectly orthogonal — the triad is cleanest for sources and CPU-bound SQL transforms. Document `starved` on a transform as "starved of input," not "pure upstream latency."
- **Regime dependence of `blocked`** (carried over from the backpressure design). For a directly-polled edge (e.g. `transform → sink` with no channel), `blocked` includes the downstream's service time, so there is a non-zero baseline; for a channel-decoupled edge it is near-zero when healthy. This is unchanged by the rename and is already documented in `AGENTS.md`.

## Migration impact

- **PR #38 is reworked**, not extended: the branch's `backpressure` counter, its emission sites, e2e helpers, and `AGENTS.md` section all change to `node_wait` + `state`.
- No external consumers to migrate (metric unmerged).

## Implementation checklist

- [ ] `recorder.rs` (~L818): rename the `backpressure` counter registration to `node_wait` (unit `ms`, two-state description).
- [ ] `wrapping.rs` (~L688): emit `node_wait` with `("state","blocked")` (+ `downstream_id`, `""` when unattributed) in the three `BackpressureRole` arms.
- [ ] `wrapping.rs` (~L716): add a starvation accumulator; record `batch_elapsed` as `node_wait{state="starved", downstream_id=""}` **and keep** recording it as `elapsed_compute` (dual-emit for backward compat — see §4).
- [ ] `broadcast_stream.rs` (~L204): emit `node_wait` with `("state","blocked")` in the data-plane branch (the control-plane fallback / `BROADCAST_COMPONENT_ID` was removed — the producer id is always threaded through).
- [ ] Generalize `BackpressureAccumulator` + `BlockedSendAccumulator` into one shared `MillisAccumulator` used by all three emission sites.
- [ ] (Optional) rename `BackpressureRole` → `EdgeRole`; the optimizer pass logic is unchanged.
- [ ] `crates/streamling-e2e/src/resources/prometheus.rs`: replace `backpressure_by_id_query` / `backpressure_by_downstream_query` with `node_wait` queries filtered by `state` (blocked-by-id, blocked-by-downstream, starved-by-id).
- [ ] `crates/streamling-e2e/tests/multi_sink.rs`, `scan_sharing.rs`: update assertions to the new metric/tags.
- [ ] `AGENTS.md`: replace the "Backpressure metric" section with a "Node-wait / utilization" section describing the `state` label, the two idle states, the (unchanged) single-emitter invariant, and the triad with `elapsed_compute` for busy.
- [ ] Run `just fix && just lint`; run the multi-sink and scan-sharing e2e tests.

## Testing plan

- **Unit (recorder):** `node_wait` counter is pre-registered — adapt `backpressure_counters_are_registered`.
- **Unit (wrapping):** with a registered node id and a slow downstream, assert `node_wait{state="blocked"}` accrues (adapt the existing backpressure wrapping test); with a slow upstream, assert `node_wait{state="starved"}` accrues. (`elapsed_compute` still receives the input-wait fold under dual-emit, so don't assert it drops.) (Mind the global-recorder `TEST_LOCK` and metadata-seeding pattern.)
- **Unit (accumulator):** keep the sub-millisecond remainder-carry tests, retargeted at the shared `MillisAccumulator`.
- **E2E:** the multi-sink and scan-sharing tests already assert per-consumer attribution — update them to `node_wait{state="blocked", downstream_id=...}` and add a `state="starved"` sanity assertion for a source.

## Open questions / decisions to confirm

1. **Metric name.** `node_wait` (recommended) vs. `node_idle` vs. `node_stall`. All read fine with a `state` label; `node_time` is avoided because busy is intentionally excluded, so "time" would overstate coverage.
2. **`downstream_id` on all series** (recommended, for label uniformity) vs. omitting it for `starved`.
3. Whether to rename `BackpressureRole` → `EdgeRole` now or leave it (cosmetic).
4. ~~Clean-cut (A) vs. additive-deprecate (C) for the `elapsed_compute` semantics change.~~ **Resolved: (C) dual-emit** — non-breaking; drop the fold in a later release.
