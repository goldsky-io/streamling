# Preview Endpoint Design (APP-4884)

**Linear:** https://linear.app/goldsky/issue/APP-4884
**Date:** 2026-06-17
**Status:** Draft for review

## Problem

The frontend is building a feature that lets users preview a pipeline at any
stage of development — seeing data flow through each block of their config —
regardless of how complete the config is. To power this, we need a streamling
endpoint that takes a pipeline YAML, swaps its sinks for a blackhole sink,
validates it, runs it for a short bounded duration, and streams the resulting
data back to the caller (the same shape as the existing `inspect` / live-data
feature).

## Key constraint: preserve the existing single-pipeline model

Today `streamling` runs as a single process bound to **one** pipeline. The
live-data ("inspect") plumbing is a process-global singleton (`LiveDataInspect`,
an `OnceLock`) wired up once at startup. The engine assumes one pipeline,
run-once semantics, and has no built-in run-duration limit.

Rather than re-engineer those assumptions, **each preview runs as its own
fresh `streamling` child process.** Every child is still "one program = one
pipeline," so the existing engine, inspect singleton, blackhole sink, and
validation paths are reused **unchanged**. The only genuinely new code is a
thin orchestration layer that rewrites config, spawns/kills the child, and
proxies its data stream.

## Architecture overview

A new `streamling preview-server` subcommand runs a small Axum HTTP server.
On request it:

1. Receives a pipeline YAML.
2. Rewrites sinks → blackhole (config rewriter).
3. Validates via a fast `--validate` child process.
4. Runs the pipeline as a child `streamling` process for a bounded duration,
   with the admin/inspect API enabled on an internal port.
5. Proxies the child's `GET /admin/live-data` SSE stream back to the caller.
6. Kills the child when the duration elapses, the client disconnects, or a new
   preview replaces it.

Concurrency is **one preview at a time**: a new request replaces the in-flight
one.

## Components

Each component has a single purpose and is independently testable.

### 1. Config rewriter
Pure function: `String (yaml) -> Result<String (yaml)>`. Parses the topology,
applies the blackhole rules below, re-serializes. No I/O. Lives alongside the
topology types (`streamling-core/src/topology.rs` or a sibling module in
`streamling-config`).

**Blackhole rules:**
- **Sinks present** → replace *every* sink with a `blackhole` sink,
  **preserving each original sink's `from:`** so data still flows through the
  upstream nodes the inspect tap reads from.
- **No sinks** → append blackhole sink(s) pointed at the **terminal node(s)**
  (sources/transforms with no downstream consumer), so orphan-node validation
  passes and data flows all the way through every block.

Notes:
- The blackhole sink (`type: blackhole`) already exists and requires a `from:`.
- Other sink options (primary_key, batch_size, etc.) are dropped on swap; only
  `from:` is carried over.

### 2. Preview orchestrator
A single-slot async process manager. Responsibilities:
- Hold at most one running child (the "current preview").
- Write the rewritten config to a temp file.
- Spawn the child with the admin/inspect API enabled on an internal port,
  waiting for readiness before streaming.
- Arm a duration timer (default 180s). On expiry, gracefully kill the child,
  then force-kill if needed; remove the temp file.
- **Replace semantics:** when a new preview arrives, gracefully kill the
  existing child first (its SSE client receives a terminal event), then start
  the new one.
- Clean up on client disconnect.

### 3. SSE proxy
Connects to the child's `GET /admin/live-data` SSE endpoint and forwards events
to the HTTP caller verbatim, plus a terminal event on shutdown.

### 4. HTTP handler
`POST /preview` — ties the above together and maps failures to status codes.

## HTTP contract

### `POST /preview`
- **Body:** raw pipeline YAML (`Content-Type: text/yaml`).
- **Query param:** `duration_seconds` (optional, integer). Omitted → default
  **180**. Capped at **600** (values above the cap are clamped to 600).
- **Success:** `200`, `text/event-stream` (SSE). Events use the same shape as
  `/admin/live-data` (per-node JSON rows), so the frontend reuses existing
  inspect rendering. The stream ends when the duration elapses, the pipeline
  finishes, or the preview is replaced; a terminal event signals close.
- **Validation failure:** `422 Unprocessable Entity` with a JSON error body
  describing the failure (child stderr surfaced).

## Validation & edge cases

Two-phase, reusing existing machinery:

- **Phase 1 — validate:** spawn `streamling --validate --config <tmp>` — a
  fast, no-execution dry run that builds physical plans. Catches missing data
  source, broken transforms (e.g. bad SQL), schema/primary-key failures, and
  orphan nodes. Non-zero exit → `422` with stderr in the error body.
- **Phase 2 — run:** only if Phase 1 passed, spawn the real run + stream.

| Issue edge case          | Handling                                            |
| ------------------------ | --------------------------------------------------- |
| No sinks provided        | rewriter appends blackhole to terminal nodes (ok)   |
| Data source missing      | Phase-1 validate fails → 422                         |
| Transform has error      | Phase-1 validate fails → 422                         |
| Any validation fails     | Phase-1 validate fails → 422                         |

## Lifecycle / bounded duration

The orchestrator owns a `tokio::time::timeout` (default 180s, max 600s). On
expiry — or on client disconnect, or on replacement by a new preview — it sends
the child a graceful kill, waits briefly, then force-kills, and removes the temp
config. Because each preview is its own process, "kill" is process teardown; no
engine changes are required.

## Testing

- **Config rewriter (unit):** sinks present (single + multiple), no sinks
  (single + multiple terminal nodes), preserves `from:`, drops other options.
- **Validation mapping (integration):** each bad-config case yields `422` with
  a useful message; valid config proceeds to streaming.
- **Orchestrator (integration):** POST a valid config → SSE rows arrive →
  process is gone after the window; `duration_seconds` honored and clamped at
  600; a second concurrent request replaces the first (first stream gets a
  terminal event, second streams).

## Out of scope

- Multi-tenant concurrency beyond one-at-a-time (deferred; subprocess model
  already supports it later by lifting the single-slot constraint).
- Authentication/authorization on the endpoint (assumed handled by the caller /
  upstream layer).
- Persisting preview results.
