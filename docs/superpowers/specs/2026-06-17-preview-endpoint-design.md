# Preview Endpoint Design (APP-4884)

**Linear:** https://linear.app/goldsky/issue/APP-4884
**Date:** 2026-06-17 (revised 2026-06-18)
**Status:** Architecture revised — streamling portion implemented

## Problem

Users developing a pipeline on the frontend (or via the CLI) want to preview it
at any stage of development — seeing live data flow through each block of their
config — regardless of how complete the config is, **without writing to any real
sink**.

## Architecture (revised)

The original design baked deploy + timed-kill + an SSE proxy into a streamling
HTTP endpoint. That was the wrong tier: it reimplemented orchestration the
Goldsky control plane already owns, and bound the data plane to control-plane
concerns. The revised design splits preview into **mechanisms** (data plane) and
a **workflow** (control plane), composing primitives that already exist.

Three tiers are involved:

1. **streamling runtime** (this repo) — the per-pipeline execution engine.
   Already exposes per-pipeline live-data SSE at `/admin/live-data`.
2. **streamling cloud control API** (separate service) — exposes
   `/pipeline/{projectId}/...` deploy / validate / pause / delete, and proxies
   each pipeline's `/admin/live-data`.
3. **api-server** (goldsky BFF) — what the CLI and frontend call. Proxies the
   control API, adds auth/permissions/billing, and runs Temporal workflows.

### Request flow ("pre-deploy rewrite")

```
CLI / frontend
   │  pipeline YAML
   ▼
api-server  POST /…/pipelines/preview
   │ 1. rewrite:   YAML ──▶ streamling POST /rewrite ──▶ blackhole-swapped YAML
   │ 2. validate:  swapped YAML ──▶ existing validate primitive  (422 on failure)
   │ 3. deploy:    swapped YAML ──▶ existing deploy primitive as `_preview_<uuid>`
   │ 4. schedule:  Temporal workflow → sleep(ttl) → existing delete primitive
   │ 5. return:    { pipelineName, ttlSeconds }
   ▼
CLI / frontend  ──▶ existing  GET /…/streamling/v1/{name}/live-data?topology_node_keys=…  (SSE)
```

The deployed config already has blackhole sinks, so the preview pipeline
**cannot write to any real destination — safe by construction**. The deploy
machinery is unchanged: it just deploys a normal config that happens to have
blackhole sinks. Nothing in the control-plane tier needs to learn the word
"preview".

## Component responsibilities

### streamling runtime (this repo) — IMPLEMENTED
- **Pure sink-swap**: `rewrite_sinks_to_blackhole(yaml) -> yaml` — replaces every
  sink with a `blackhole` (preserving its `from`); if there are no sinks,
  appends a blackhole per terminal node so data flows through every block and
  the config validates. Topology-aware, so it lives here (in Rust, next to the
  topology types) rather than being reimplemented in api-server TypeScript.
- **`find_terminal_nodes`** in `topology_validation.rs`, sharing
  `collect_consumed_nodes` with the existing orphan check.
- Exposed as `POST /rewrite` (`--preview-server` mode) — stateless: YAML in →
  swapped YAML out, or `422` on unparseable YAML. Also a public library fn so
  the control API can call it in-process if it imports these crates.
- **No** deploy, validation invocation, TTL, or SSE proxy in streamling.

### streamling cloud control API (separate service)
- Reuses existing `deploy` / `validate` / `pause` / `delete`.
- Must support deploying an **ephemeral** pipeline under a generated name. (v1
  accepts a transient real pipeline that is auto-deleted; a fully invisible
  preview mode — excluded from lists/billing/quota — is a later enhancement.)

### api-server (goldsky BFF) — TO BUILD
- New preview route: rewrite → validate → deploy-ephemeral → schedule TTL
  teardown → return the pipeline name.
- **TTL** via a Temporal workflow (`deploy → sleep(ttl) → delete`), mirroring the
  existing auto-pause/delete pattern. Default 180s, cap 600s.
- Reuses the existing live-data SSE proxy for the actual data (no new transport).

### CLI — TO BUILD
- `preview` command: submit a local config, then stream live-data rows in the
  terminal (per node) until the TTL or Ctrl-C. (`--no-follow`/`--web` to just
  print a link is a possible later option.)

## Edge cases (unchanged from the ticket)
- No sinks provided → rewrite appends a blackhole per terminal node (not an
  error).
- Missing data source / broken transform / any validation failure → `422`,
  surfaced by api-server from the existing validate primitive (run on the
  **swapped** YAML, after rewrite).

## Decisions
- **Swap location:** pre-deploy rewrite (sanitize the config, then deploy
  normally) — smallest blast radius; the deploy tier is untouched.
- **TTL:** api-server Temporal workflow.
- **Ephemeral semantics (v1):** transient real pipeline (`_preview_<uuid>`),
  auto-deleted; invisible/un-billed preview mode deferred.
- **CLI UX:** stream rows in the terminal.

## Out of scope
- Auth/permissions (handled by api-server's existing middleware).
- A fully invisible/un-billed ephemeral deploy mode (control-plane enhancement).
- Multi-tenant concurrency limits on previews (policy in api-server if needed).

## Superseded
The original in-streamling orchestrator (child-process spawn + duration timer +
SSE proxy) and its implementation plan
(`docs/superpowers/plans/2026-06-17-preview-endpoint.md`) are superseded by this
revision. The streamling code has been reduced to the stateless `/rewrite`
endpoint + `rewrite_sinks_to_blackhole`.
