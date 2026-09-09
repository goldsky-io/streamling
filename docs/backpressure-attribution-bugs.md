# Backpressure attribution bugs — fix plan

**Status: both fixed and guarded end-to-end** (root cause confirmed for each;
red→green unit regression tests in `crates/streamling-core/src/optimizer.rs`
`attribution_tests`, plus e2e assertions in
`crates/streamling-e2e/tests/scan_sharing.rs`).

Two edge-attribution bugs were found during QA of the unified backpressure
edge-metric (`streamling_backpressure_milliseconds_total`, PR #38) while running
the branch image against prod pipelines and observing in Groundcover. Both cause
real backpressure to be emitted as an **untagged** self-series (no
`downstream_id`) instead of being attributed to a named edge. Both share the same
underlying cause: a **see-through `children()`** wrapper node
(`WrappingExec`/`RebatchExec` delegate `children()` to their inner) hides a
`WrappingExec` from the top-down attribution walk, so the wrapped node keeps its
construction-time `Unattributed` role and the sink/consumer name lands one node
too far upstream.

> **Deeper root cause found while adding e2e coverage.** The first fix attempt
> only tagged base-plans where a *compute node* separated the two wrappers. In
> the real (QA) shape the transform is an identity projection that DataFusion
> **elides**, so `W(producer)` wraps `W(source)` **directly** — and
> `WrappingExec::children()` is *itself* see-through, so the attribution walk
> skipped the inner `W(source)`. The correct fix makes the rule recurse into a
> `WrappingExec`'s immediate **`inner()`** rather than its see-through
> `children()`. There were also **two** stash sites for a scan-shared
> `base_exec` (a source path and a transform path); only the source path had been
> fixed. See "Deeper root cause" under Bug 1.

Feature invariant being violated (see `AGENTS.md` → "Backpressure metric"):
every backpressured edge should emit exactly one series tagged
`id=<producer>, downstream_id=<consumer>`; the untagged fallback is meant to be
*rare* (only when a linear node's downstream genuinely can't be resolved).

Attribution is stamped by `DownstreamAttributionRule` in
`crates/streamling-core/src/optimizer.rs`.

## QA evidence (Groundcover, prod, image `feat-node-backpressure-metrics-fb5f28c…`)

Values are accumulated-ms counters, scoped by `service_instance_id`.

| Case | Topology | Result |
|------|----------|--------|
| A (baseline) | dataset → sql → **blackhole** | ✅ all edges tagged (`…→qa_src_a→qa_sql_a→qa_bh_a`), rate ~6 ms/s |
| C (multi-sink) | dataset ⇉ {webhook, blackhole} | ✅ producer suppressed, 2 tagged edges, no untagged |
| **B (linear)** | dataset → sql → **webhook** | ❌ `qa_sql_b` untagged; edge shows as `qa_src_b→qa_web_b` |
| **D (scan-share)** | dataset ⇉ {sql→webhook, sql→blackhole} | ❌ `ethereum→qa_src_d` untagged (~173k ms); `qa_slow_d`(→webhook) untagged while `qa_fast_d`(→blackhole) tagged |

Case D is the cleanest controlled comparison: **same pipeline / same optimizer
run**, a transform feeding a webhook is untagged while a transform feeding a
blackhole is tagged.

---

## Bug 1 — the edge INTO a scan-sharing producer is never attributed

**Symptom.** For `source → producer(scan-shared) → {consumer_a, consumer_b}`,
the producer→consumer edges are attributed correctly (`qa_src_d→qa_slow_d`), but
the upstream `source→producer` edge carries a large value as an untagged
self-series (`id=ethereum…`, no `downstream_id`, ~173k ms in QA).

**Root cause (confirmed).** When a node has >1 consumer, scan sharing stashes
the producer's entire sub-plan (`WrappingExec(producer) → … → WrappingExec(source)`)
inside a `SharedSourceHandle.base_exec`
(`crates/streamling-core/src/operators/scan_sharing.rs`). The consumers see a
`BroadcastingExec` leaf whose `children()` returns `vec![]`
(`scan_sharing.rs:246`). `DownstreamAttributionRule` walks the main plan
top-down, so it **never descends into `base_exec`** — the source's `WrappingExec`
in there keeps its construction-time role (`Unattributed`) and emits untagged.
This is even acknowledged in the rule's doc comment: *"Scan-shared producer
`WrappingExec`s are stashed in `SharedSourceHandle` before this rule runs
(unreachable here)…"* — but the consequence for the **upstream** edge (a large,
real backpressure edge losing its `downstream_id`) was not intended.

**Fix (implemented).** Attribute the stashed sub-plan at construction, where it
is created and before it is hidden. Added
`attribute_scan_shared_producer_base_exec()` in `optimizer.rs` (a thin wrapper
over `attribute_downstream(base_exec, None, false)`) and call it from
`WrappingSourceTableProvider::scan()` in
`crates/streamling-core/src/operators/wrapping.rs` on the `wrapped_exec` before it
is passed to `SharedSourceHandle::new`. The producer's own `WrappingExec` is
already suppressed (`FanOutProducer`) and stays that way (its per-consumer edges
are emitted by the `BroadcastStream`), while every upstream `WrappingExec` (e.g.
the source) gets an `Edge(<nearest named downstream>)` stamp.

This is done at construction rather than in the rule because `base_exec` is shared
behind `Arc` across every `BroadcastingExec` leaf; attributing it once at
construction avoids rebuilding/duplicating the handle (which would break
scan-sharing by starting one broadcast per consumer).

### Deeper root cause (found via e2e)

The construction-time attribution above was necessary but **not sufficient**, and
the original unit test gave false confidence:

1. **See-through `children()` fusion.** `attribute_downstream`'s `WrappingExec`
   branch recursed via `wrapping.children()`. But `WrappingExec::children()` is
   see-through (it delegates to `inner`). When the scan-shared transform is an
   identity `SELECT` whose projection DataFusion **elides**, the stashed
   `base_exec` is `W(producer)` wrapping `W(source)` *directly* (no compute node
   between). `W(producer).children()` therefore returns the *source's* children,
   so `W(source)` was skipped and stayed `Unattributed`. The `mid()` node in the
   first unit test hid this by keeping the two wrappers non-adjacent.
   **Fix:** the rule now recurses into `wrapping.inner()` directly (new
   `WrappingExec::inner()` + `clone_with_role_and_inner()` accessors in
   `wrapping.rs`), so an immediately-adjacent `WrappingExec` is visited.
2. **Second stash site.** A scan-shared `base_exec` is stashed in **two** places:
   `WrappingSourceTableProvider::scan()` (scan-shared **source** leaf) *and* the
   `WrappingNode` physical-planning path (scan-shared **transform**, `wrapping.rs`
   ~L1199). Only the source path had the construction-time attribution; the
   transform path (`up_source → shared_producer(scan-shared)`) still stashed an
   un-attributed `exec`. **Fix:** call
   `attribute_scan_shared_producer_base_exec()` on both stash sites.

**Regression tests.**
- `scan_shared_producer_base_exec_attributes_upstream_edges` — `mid`-separated
  variant (producer stays `FanOutProducer`, `W(source)` becomes `Edge("producer")`).
- `scan_shared_producer_base_exec_attributes_fused_upstream_edge` — the **fused**
  (no-`mid`) variant matching the elided-identity QA shape; red before the
  `inner()`-recursion fix.
- `adjacent_wrappers_without_compute_node_all_get_stamps` — general linear
  `W(sql) ← W(source)` fusion, independent of scan sharing.
- e2e `test_scan_sharing_upstream_edge_attribution` — asserts the
  `up_source → shared_producer` edge carries `downstream_id="shared_producer"`
  (**verified failing** before the transform-path fix, passing after).

---

## Bug 2 — a transform feeding a webhook/HTTP sink loses its downstream edge

**Symptom.** An identity `SELECT *` transform whose consumer is a **webhook**
sink is emitted untagged; the same transform pattern feeding a **blackhole** sink
is tagged normally.

- Case B: `qa_sql_b` (→ `qa_web_b` webhook) untagged; the sink edge appears as
  `qa_src_b→qa_web_b` (attributed one node too far upstream).
- Case D: `qa_slow_d` (→ `qa_web_d` webhook) untagged **while** `qa_fast_d`
  (→ `qa_bh_d` blackhole) tagged, in the same pipeline.
- Recovery run (batched fast webhook, load removed): `qa_sql_b` **still**
  untagged → the bug is load-independent and sink-type-dependent.

**Root cause (confirmed).** It is not the sink type per se — it is a
`RebatchExec` inserted above the feeding transform. In the single-sink path
(`crates/streamling/src/lib.rs`, `wrap_with_rebatch` → `insert_into`), a sink that
sets `batch_size` gets a `RebatchExec` between its `DataSinkExec` and the feeding
transform's `WrappingExec`. The webhook sink sets `batch_size` (case A's blackhole
left it `None`, so **no** rebatch — which is why case A was clean).

`RebatchExec::children()` **delegates to `inner`** (see-through, exactly like
`WrappingExec`; `crates/streamling-core/src/operators/rebatch.rs`). When
`attribute_downstream` reached the `RebatchExec` it fell into the **generic**
branch, which calls `node.children()` — the see-through call returns the *inner
`WrappingExec`'s* children, so the feeding `WrappingExec` was **skipped entirely**.
It kept its `Unattributed` role (untagged series) and the sink name was passed
straight through to the next node up (`qa_src_b`), producing the observed
`qa_src_b→qa_web_b` mislabel. Case D is the same mechanism on the
`qa_slow_d→qa_web_d` single-sink branch; `qa_fast_d→qa_bh_d` (blackhole, no
rebatch) was correct in the same pipeline.

**Fix (implemented).** Added an explicit `RebatchExec` branch in
`attribute_downstream` (`optimizer.rs`) that recurses into `rebatch.inner()`
directly (with the named downstream passed through unchanged, since rebatch is not
an edge endpoint) and rebuilds via `rebatch.clone_with_inner(...)`, so the wrapped
`WrappingExec` is visited and stamped. Added `RebatchExec::inner()` and
`clone_with_inner()` accessors in `rebatch.rs`.

**Regression tests.**
- Unit: `optimizer::attribution_tests::rebatch_before_sink_still_attributes_feeding_transform`
  builds `DataSinkExec(web_sink) ← RebatchExec ← W(sql) ← mid ← W(source)`, runs
  the rule, and asserts `W(sql)` = `Edge("web_sink")` and `W(source)` =
  `Edge("sql")`. Before the fix it was `Unattributed`/mislabeled → failed
  (verified red). Also `rebatch_above_broadcasting_leaf_attributes_to_sink`.
- e2e: `test_linear_rebatch_webhook_edge_attribution` — a linear
  `source → transform → webhook(batch_size:1)` (the exact QA case B shape) asserts
  the `bp2_xform → bp2_web` edge series exists pinned to **both**
  `id="bp2_xform"` and `downstream_id="bp2_web"` (a downstream-only query could
  pass spuriously because the pre-fix bug mislabeled the sink name onto the wrong
  upstream node). The transform carries a `WHERE` so it is not an identity
  projection that could be inlined away.

---

## Execution order

1. ✅ Bug 1: failing test → fix (`attribute_scan_shared_producer_base_exec` +
   call in `wrapping.rs`) → unit test passes.
2. ✅ Bug 2: failing test (`RebatchExec` see-through repro, verified red) → fix
   (`RebatchExec` branch in `attribute_downstream` + accessors) → unit test passes.
3. ✅ e2e coverage added; **bug 1's e2e failed**, exposing the deeper root cause
   (see-through `WrappingExec::children()` fusion + a second, un-fixed
   transform-scan-sharing stash site). Fixed via `WrappingExec::inner()` recursion
   and attributing both stash sites.
4. ✅ `cargo test -p streamling-core --lib` (410 green, incl. 12 attribution unit
   tests) and `just e2e-test test_scan_sharing test_linear_rebatch_webhook_edge_attribution`
   (3/3 green). `just fix && just lint` clean.
5. (Optional) re-deploy the branch image and re-check cases B and D in Groundcover
   to confirm the untagged series are gone.

## Notes / non-issues

- The dataset source expands to a kafka source whose node id
  (`ethereum_receipt_transactions__1_2_0__…`) is identical across pipelines;
  always scope PromQL by `service_instance_id`. Not a bug.
- No double-counting was observed in any case (fan-out producer `WrappingExec`
  correctly suppressed in case C).
