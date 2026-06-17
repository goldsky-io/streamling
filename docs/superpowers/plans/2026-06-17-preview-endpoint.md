# Preview Endpoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `streamling preview-server` mode exposing `POST /preview` that takes a pipeline YAML, swaps all sinks for blackhole, validates it, runs it as a child `streamling` process for a bounded duration, and streams the live data back as SSE.

**Architecture:** Each preview runs as its own fresh `streamling` child process, preserving the existing one-process-one-pipeline model. A thin orchestration layer (in `streamling-core::preview`) rewrites the config, validates via a `--validate` child, spawns a run child with the inspect/admin API enabled, and proxies the child's `/admin/live-data` SSE stream back to the caller. Concurrency is one preview at a time: a new request replaces the in-flight one.

**Tech Stack:** Rust, Axum 0.8 (HTTP server), reqwest 0.12 (SSE client/proxy), tokio (process + time), serde_yaml (config rewrite). All deps already in `streamling-core` except the tokio `process` feature.

**Spec:** `docs/superpowers/specs/2026-06-17-preview-endpoint-design.md` · **Linear:** APP-4884

---

## File Structure

| File | Responsibility |
| --- | --- |
| `crates/streamling-core/src/topology_validation.rs` (modify) | Add `find_terminal_nodes()` — sources/transforms with no downstream consumer. |
| `crates/streamling-core/src/preview/mod.rs` (create) | Module root; `run_preview_server(port)`; re-exports. |
| `crates/streamling-core/src/preview/rewrite.rs` (create) | Pure `rewrite_sinks_to_blackhole(yaml) -> String`. |
| `crates/streamling-core/src/preview/duration.rs` (create) | Pure `resolve_duration_secs()` + constants. |
| `crates/streamling-core/src/preview/child.rs` (create) | Child process mgmt: `validate_config()`, `spawn_run_child()`, port alloc, kill, readiness. |
| `crates/streamling-core/src/preview/server.rs` (create) | Axum router, `POST /preview` handler, single-slot orchestrator state, SSE byte-passthrough proxy. |
| `crates/streamling-core/src/lib.rs` (modify) | Register `pub mod preview;`. |
| `crates/streamling-core/Cargo.toml` (modify) | Add tokio `process` feature; add `tempfile` dev/runtime dep. |
| `crates/streamling/src/main.rs` (modify) | Add `--preview-server` / `--preview-server-port` flags; branch to preview server. |
| `crates/streamling/tests/preview.rs` (create) | Capstone e2e: real binary, `file` source, assert SSE rows + teardown. |

---

## Task 1: `find_terminal_nodes` in topology_validation

Terminal nodes = sources/transforms with zero downstream consumers. Reuses the same consumer analysis as `validate_no_orphan_nodes`, but **preserves original-case names** (needed as blackhole `from:` values) and returns them instead of erroring.

**Files:**
- Modify: `crates/streamling-core/src/topology_validation.rs`
- Test: `crates/streamling-core/src/topology_validation.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Write the failing tests**

Add to the existing `#[cfg(test)] mod tests` block (or create one at the end of the file):

```rust
#[cfg(test)]
mod terminal_node_tests {
    use super::find_terminal_nodes;

    #[test]
    fn single_sink_terminal_is_the_unconsumed_transform() {
        let yaml = r#"
sources:
  src:
    type: kafka
    topic: t
    primary_key: id
transforms:
  filt:
    type: sql
    sql: select * from src
    primary_key: id
"#;
        // `src` is consumed by `filt`; `filt` is consumed by nothing -> terminal.
        let terminals = find_terminal_nodes(yaml).unwrap();
        assert_eq!(terminals, vec!["filt".to_string()]);
    }

    #[test]
    fn multiple_independent_terminals_both_returned() {
        let yaml = r#"
sources:
  a:
    type: kafka
    topic: t1
    primary_key: id
  b:
    type: kafka
    topic: t2
    primary_key: id
transforms: {}
"#;
        let mut terminals = find_terminal_nodes(yaml).unwrap();
        terminals.sort();
        assert_eq!(terminals, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn node_consumed_by_sink_is_not_terminal() {
        let yaml = r#"
sources:
  src:
    type: kafka
    topic: t
    primary_key: id
transforms: {}
sinks:
  out:
    type: print
    from: src
"#;
        let terminals = find_terminal_nodes(yaml).unwrap();
        assert!(terminals.is_empty());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p streamling-core find_terminal_nodes 2>&1 | tail -20`
Expected: FAIL — `find_terminal_nodes` not found / unresolved import.

- [ ] **Step 3: Implement `find_terminal_nodes`**

Add this function to `crates/streamling-core/src/topology_validation.rs` (after `validate_no_orphan_nodes`). It mirrors the orphan analysis but keeps original-case names and treats an unparseable SQL query conservatively (returns all candidate nodes so the caller attaches a blackhole to each — always valid):

```rust
/// Returns the names (original case) of sources/transforms that have no
/// downstream consumer. Used by the preview rewriter to attach blackhole sinks
/// when the submitted config has no sinks.
///
/// Mirrors the consumer analysis in [`validate_no_orphan_nodes`]. If a SQL
/// transform cannot be parsed for table references, analysis is unreliable, so
/// every candidate node is returned (attaching a blackhole to each is always
/// valid and keeps data flowing through every block).
pub fn find_terminal_nodes(config: &str) -> crate::error::Result<Vec<String>> {
    let value: serde_yaml::Value =
        serde_yaml::from_str(config).map_err(|e| streamling_user_err!("invalid YAML: {}", e))?;

    let root = if let Some(def) = value.get("definition") {
        if def.is_mapping() { def } else { &value }
    } else {
        &value
    };

    // Candidate nodes: all sources + non-dynamic-table transforms, original case.
    let mut candidates: Vec<String> = Vec::new();
    if let Some(m) = root.get("sources").and_then(|v| v.as_mapping()) {
        for k in m.keys() {
            if let Some(name) = k.as_str() {
                candidates.push(name.to_string());
            }
        }
    }
    let transforms: Vec<(String, serde_yaml::Value)> = root
        .get("transforms")
        .and_then(|v| v.as_mapping())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| k.as_str().map(|s| (s.to_string(), v.clone())))
                .collect()
        })
        .unwrap_or_default();
    for (name, transform_val) in &transforms {
        let is_dynamic_table = transform_val
            .as_mapping()
            .and_then(|m| m.get("type"))
            .and_then(|v| v.as_str())
            == Some("dynamic_table");
        if !is_dynamic_table {
            candidates.push(name.clone());
        }
    }

    // Build the set of consumed node names (lowercased), from transform SQL
    // table refs / `from` fields and sink `from` fields.
    let mut consumed: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (_, transform_val) in &transforms {
        let mapping = match transform_val.as_mapping() {
            Some(m) => m,
            None => continue,
        };
        if let Some(sql) = mapping.get("sql").and_then(|v| v.as_str()) {
            match extract_table_references_from_sql(sql) {
                Ok(table_names) => {
                    for name in table_names {
                        consumed.insert(strip_sql_quotes(&name).to_lowercase());
                    }
                }
                Err(_) => {
                    // Unanalyzable: be conservative, treat every node as terminal.
                    return Ok(candidates);
                }
            }
        } else if let Some(from) = mapping.get("from").and_then(|v| v.as_str()) {
            consumed.insert(from.to_lowercase());
        }
    }
    if let Some(m) = root.get("sinks").and_then(|v| v.as_mapping()) {
        for (_, v) in m {
            if let Some(from) = v.get("from").and_then(|f| f.as_str()) {
                consumed.insert(from.to_lowercase());
            }
        }
    }

    Ok(candidates
        .into_iter()
        .filter(|name| !consumed.contains(&name.to_lowercase()))
        .collect())
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p streamling-core find_terminal_nodes 2>&1 | tail -20`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/streamling-core/src/topology_validation.rs
git commit -m "feat(preview): add find_terminal_nodes for blackhole attachment"
```

---

## Task 2: Config rewriter (`rewrite_sinks_to_blackhole`)

Pure function on raw YAML (`PipelineTopology` is `Deserialize`-only, so we manipulate `serde_yaml::Value`, not the typed struct).

**Files:**
- Create: `crates/streamling-core/src/preview/mod.rs`
- Create: `crates/streamling-core/src/preview/rewrite.rs`
- Modify: `crates/streamling-core/src/lib.rs`
- Test: `crates/streamling-core/src/preview/rewrite.rs` (inline)

- [ ] **Step 1: Register the module**

In `crates/streamling-core/src/lib.rs`, add alongside the other `pub mod` declarations:

```rust
pub mod preview;
```

Create `crates/streamling-core/src/preview/mod.rs` with:

```rust
pub mod duration;
pub mod rewrite;
```

(Later tasks add `pub mod child;`, `pub mod server;`, and `run_preview_server`.)

- [ ] **Step 2: Write the failing tests**

Create `crates/streamling-core/src/preview/rewrite.rs`:

```rust
//! Rewrites a submitted pipeline config so every sink becomes a blackhole sink,
//! enabling preview runs that exercise the whole topology without external writes.

use crate::topology_validation::find_terminal_nodes;
use serde_yaml::{Mapping, Value};

#[cfg(test)]
mod tests {
    use super::rewrite_sinks_to_blackhole;
    use serde_yaml::Value;

    fn parse(s: &str) -> Value {
        serde_yaml::from_str(s).unwrap()
    }

    #[test]
    fn replaces_existing_sink_with_blackhole_preserving_from() {
        let yaml = r#"
sources:
  src:
    type: kafka
    topic: t
    primary_key: id
transforms: {}
sinks:
  out:
    type: postgres
    from: src
    table: foo
    primary_key: id
"#;
        let rewritten = rewrite_sinks_to_blackhole(yaml).unwrap();
        let v = parse(&rewritten);
        let out = &v["sinks"]["out"];
        assert_eq!(out["type"], Value::from("blackhole"));
        assert_eq!(out["from"], Value::from("src"));
        // Non-from options are dropped.
        assert!(out.get("table").is_none());
        assert!(out.get("primary_key").is_none());
    }

    #[test]
    fn replaces_all_sinks() {
        let yaml = r#"
sources:
  src:
    type: kafka
    topic: t
    primary_key: id
transforms: {}
sinks:
  a:
    type: print
    from: src
  b:
    type: postgres
    from: src
    table: foo
    primary_key: id
"#;
        let v = parse(&rewrite_sinks_to_blackhole(yaml).unwrap());
        assert_eq!(v["sinks"]["a"]["type"], Value::from("blackhole"));
        assert_eq!(v["sinks"]["b"]["type"], Value::from("blackhole"));
    }

    #[test]
    fn appends_blackhole_when_no_sinks() {
        let yaml = r#"
sources:
  src:
    type: kafka
    topic: t
    primary_key: id
transforms:
  filt:
    type: sql
    sql: select * from src
    primary_key: id
"#;
        let v = parse(&rewrite_sinks_to_blackhole(yaml).unwrap());
        let sinks = v["sinks"].as_mapping().unwrap();
        // `filt` is the single terminal node.
        assert_eq!(sinks.len(), 1);
        let (_, sink) = sinks.iter().next().unwrap();
        assert_eq!(sink["type"], Value::from("blackhole"));
        assert_eq!(sink["from"], Value::from("filt"));
    }

    #[test]
    fn appends_blackhole_per_terminal_node() {
        let yaml = r#"
sources:
  a:
    type: kafka
    topic: t1
    primary_key: id
  b:
    type: kafka
    topic: t2
    primary_key: id
transforms: {}
"#;
        let v = parse(&rewrite_sinks_to_blackhole(yaml).unwrap());
        let sinks = v["sinks"].as_mapping().unwrap();
        assert_eq!(sinks.len(), 2);
        let froms: Vec<String> = sinks
            .iter()
            .map(|(_, s)| s["from"].as_str().unwrap().to_string())
            .collect();
        assert!(froms.contains(&"a".to_string()));
        assert!(froms.contains(&"b".to_string()));
    }

    #[test]
    fn invalid_yaml_errors() {
        assert!(rewrite_sinks_to_blackhole("::: not yaml :::").is_err());
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p streamling-core preview::rewrite 2>&1 | tail -20`
Expected: FAIL — `rewrite_sinks_to_blackhole` not found.

- [ ] **Step 4: Implement `rewrite_sinks_to_blackhole`**

Add to `crates/streamling-core/src/preview/rewrite.rs` (above the `#[cfg(test)]` block):

```rust
/// Builds a `{ type: blackhole, from: <from> }` sink mapping. `from` is omitted
/// when the original sink had none (validation will then reject it, which is the
/// desired behaviour for a malformed sink).
fn blackhole_value(from: Option<&str>) -> Value {
    let mut m = Mapping::new();
    m.insert(Value::from("type"), Value::from("blackhole"));
    if let Some(from) = from {
        m.insert(Value::from("from"), Value::from(from));
    }
    Value::Mapping(m)
}

/// Rewrites `yaml` so every sink is a blackhole sink. If the config has sinks,
/// each is replaced with a blackhole that keeps the original `from`. If it has
/// none, a blackhole is appended for every terminal node (sources/transforms
/// with no consumer) so the pipeline validates and data flows through each block.
pub fn rewrite_sinks_to_blackhole(yaml: &str) -> crate::error::Result<String> {
    use crate::streamling_user_err;

    let mut root: Value = serde_yaml::from_str(yaml)
        .map_err(|e| streamling_user_err!("invalid YAML: {}", e))?;

    // Resolve the mapping that holds sources/transforms/sinks (support an
    // optional `definition:` wrapper, matching validate_no_orphan_nodes).
    let has_definition = root
        .get("definition")
        .map(|d| d.is_mapping())
        .unwrap_or(false);

    // Collect original sink `from` values before mutating.
    let existing: Option<Vec<(Value, Option<String>)>> = {
        let container = if has_definition { &root["definition"] } else { &root };
        container
            .get("sinks")
            .and_then(|v| v.as_mapping())
            .filter(|m| !m.is_empty())
            .map(|m| {
                m.iter()
                    .map(|(k, v)| {
                        let from = v.get("from").and_then(|f| f.as_str()).map(String::from);
                        (k.clone(), from)
                    })
                    .collect()
            })
    };

    let new_sinks = match existing {
        Some(entries) => {
            let mut m = Mapping::new();
            for (key, from) in entries {
                m.insert(key, blackhole_value(from.as_deref()));
            }
            m
        }
        None => {
            let mut m = Mapping::new();
            for node in find_terminal_nodes(yaml)? {
                m.insert(
                    Value::from(format!("preview_blackhole_{node}")),
                    blackhole_value(Some(&node)),
                );
            }
            m
        }
    };

    let container = if has_definition {
        root.get_mut("definition").expect("checked above")
    } else {
        &mut root
    };
    let mapping = container
        .as_mapping_mut()
        .ok_or_else(|| streamling_user_err!("pipeline config root must be a mapping"))?;
    mapping.insert(Value::from("sinks"), Value::Mapping(new_sinks));

    serde_yaml::to_string(&root)
        .map_err(|e| streamling_user_err!("failed to serialize rewritten config: {}", e))
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p streamling-core preview::rewrite 2>&1 | tail -20`
Expected: PASS (5 tests).

- [ ] **Step 6: Commit**

```bash
git add crates/streamling-core/src/lib.rs crates/streamling-core/src/preview/
git commit -m "feat(preview): rewrite sinks to blackhole"
```

---

## Task 3: Duration resolution

**Files:**
- Create: `crates/streamling-core/src/preview/duration.rs`
- Test: same file (inline)

- [ ] **Step 1: Write the failing tests**

Create `crates/streamling-core/src/preview/duration.rs`:

```rust
//! Resolves the preview run duration from an optional request parameter.

/// Default preview duration when the caller omits `duration_seconds`.
pub const DEFAULT_PREVIEW_SECS: u64 = 180;
/// Hard upper bound on preview duration; larger requests are clamped down.
pub const MAX_PREVIEW_SECS: u64 = 600;

#[cfg(test)]
mod tests {
    use super::{resolve_duration_secs, DEFAULT_PREVIEW_SECS, MAX_PREVIEW_SECS};

    #[test]
    fn none_uses_default() {
        assert_eq!(resolve_duration_secs(None), DEFAULT_PREVIEW_SECS);
    }

    #[test]
    fn zero_uses_default() {
        assert_eq!(resolve_duration_secs(Some(0)), DEFAULT_PREVIEW_SECS);
    }

    #[test]
    fn in_range_passes_through() {
        assert_eq!(resolve_duration_secs(Some(42)), 42);
    }

    #[test]
    fn above_max_clamps() {
        assert_eq!(resolve_duration_secs(Some(99_999)), MAX_PREVIEW_SECS);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p streamling-core preview::duration 2>&1 | tail -20`
Expected: FAIL — `resolve_duration_secs` not found.

- [ ] **Step 3: Implement**

Add above the `#[cfg(test)]` block in `duration.rs`:

```rust
/// Resolves the effective preview duration in seconds. `None` or `0` yields the
/// default; anything above [`MAX_PREVIEW_SECS`] is clamped down.
pub fn resolve_duration_secs(requested: Option<u64>) -> u64 {
    match requested {
        None | Some(0) => DEFAULT_PREVIEW_SECS,
        Some(n) => n.min(MAX_PREVIEW_SECS),
    }
}
```

Register it (already added in Task 2's `mod.rs` as `pub mod duration;`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p streamling-core preview::duration 2>&1 | tail -20`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/streamling-core/src/preview/duration.rs
git commit -m "feat(preview): resolve and clamp preview duration"
```

---

## Task 4: Child process management

Spawns `streamling` children (the orchestrator re-invokes the current executable). Two operations: **validate** (run `--validate`, capture stdout JSON / exit code) and **spawn run** (start a pipeline with inspect enabled on a chosen port, poll until the admin API is reachable). Tested by the capstone e2e (Task 7); this task wires the dependency and the readiness/port helpers with focused unit tests where possible.

**Files:**
- Modify: `crates/streamling-core/Cargo.toml`
- Create: `crates/streamling-core/src/preview/child.rs`
- Modify: `crates/streamling-core/src/preview/mod.rs`

- [ ] **Step 1: Add the tokio `process` feature and `tempfile`**

In `crates/streamling-core/Cargo.toml`, ensure tokio enables `process` and `time`, and add `tempfile`. If tokio is declared as `tokio.workspace = true`, change it to add features locally:

```toml
tokio = { workspace = true, features = ["process", "time", "macros"] }
tempfile = "3"
```

Verify it builds:
Run: `cargo build -p streamling-core 2>&1 | tail -20`
Expected: builds clean.

- [ ] **Step 2: Write the failing test for port allocation**

Create `crates/streamling-core/src/preview/child.rs`:

```rust
//! Spawns and supervises `streamling` child processes for previews.

use crate::error::{Result, ResultExt};
use std::net::TcpListener;
use std::path::Path;
use std::time::Duration;
use tokio::process::{Child, Command};

#[cfg(test)]
mod tests {
    use super::pick_free_port;

    #[test]
    fn pick_free_port_returns_bindable_port() {
        let port = pick_free_port().unwrap();
        assert!(port > 0);
        // The port was free at selection time; we can bind it again here.
        let _l = std::net::TcpListener::bind(("127.0.0.1", port)).unwrap();
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo test -p streamling-core preview::child 2>&1 | tail -20`
Expected: FAIL — `pick_free_port` not found.

- [ ] **Step 4: Implement child management**

Add above the `#[cfg(test)]` block in `child.rs`:

```rust
/// Picks a currently-free TCP port by binding to port 0 and reading the
/// assigned port. There is an inherent race (the port could be taken before the
/// child binds it), acceptable for single-slot preview use.
pub fn pick_free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .streamling_context("failed to bind ephemeral port")?;
    let port = listener
        .local_addr()
        .streamling_context("failed to read local addr")?
        .port();
    Ok(port)
}

/// Path to the running streamling executable; children re-invoke it.
fn streamling_exe() -> Result<std::path::PathBuf> {
    std::env::current_exe().streamling_context("failed to resolve current exe")
}

/// Runs `streamling --validate --config <config_path>` and returns `Ok(())` if
/// the pipeline validates, or `Err(message)` with the captured output otherwise.
pub async fn validate_config(config_path: &Path) -> std::result::Result<(), String> {
    let exe = streamling_exe().map_err(|e| e.to_string())?;
    let output = Command::new(exe)
        .arg("--validate")
        .arg(config_path)
        .kill_on_drop(true)
        .output()
        .await
        .map_err(|e| format!("failed to run validation child: {e}"))?;
    if output.status.success() {
        Ok(())
    } else {
        // `--validate` prints JSON to stdout; fall back to stderr.
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let msg = if !stdout.trim().is_empty() {
            stdout.into_owned()
        } else {
            stderr.into_owned()
        };
        Err(msg)
    }
}

/// A spawned preview pipeline child plus the admin port its SSE is served on.
pub struct RunChild {
    pub child: Child,
    pub admin_port: u16,
}

impl RunChild {
    /// Gracefully terminates the child. `kill_on_drop` is also set as a backstop.
    pub async fn kill(mut self) {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }
}

/// Spawns `streamling <config_path>` with live-data inspect enabled on a freshly
/// allocated admin port, then waits until that port accepts connections.
pub async fn spawn_run_child(config_path: &Path) -> Result<RunChild> {
    let admin_port = pick_free_port()?;
    let exe = streamling_exe()?;
    let child = Command::new(exe)
        .arg(config_path)
        .env("STREAMLING__LIVE_DATA_INSPECT_ENABLED", "true")
        .env("STREAMLING__ADMIN_API_PORT", admin_port.to_string())
        .kill_on_drop(true)
        .spawn()
        .streamling_context("failed to spawn preview child")?;

    wait_for_port(admin_port, Duration::from_secs(30)).await?;
    Ok(RunChild { child, admin_port })
}

/// Polls `127.0.0.1:port` until a TCP connection succeeds or `timeout` elapses.
async fn wait_for_port(port: u16, timeout: Duration) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(crate::streamling_internal_err!(
                "preview child admin API did not come up on port {port} within {timeout:?}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
```

> **Note on error macros:** this uses `streamling_internal_err!`. If that macro name differs in `error.rs`, substitute the project's internal-error constructor (grep `macro_rules! streamling_` in `crates/streamling-core/src/error.rs`).

Add `pub mod child;` to `crates/streamling-core/src/preview/mod.rs`.

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p streamling-core preview::child 2>&1 | tail -20`
Expected: PASS (1 test).

- [ ] **Step 6: Commit**

```bash
git add crates/streamling-core/Cargo.toml crates/streamling-core/src/preview/
git commit -m "feat(preview): child process spawn, validate, and readiness"
```

---

## Task 5: HTTP server, orchestrator, and SSE proxy

Single-slot orchestrator: holds the current child behind a mutex; a new request kills the old one (replace semantics) before starting. The handler rewrites → writes temp config → validates → spawns → proxies the child's `/admin/live-data` bytes back, killing the child after the resolved duration.

**Files:**
- Create: `crates/streamling-core/src/preview/server.rs`
- Modify: `crates/streamling-core/src/preview/mod.rs`

- [ ] **Step 1: Write the failing test (router smoke + 422 on bad config)**

Create `crates/streamling-core/src/preview/server.rs` with the test module first:

```rust
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

#[cfg(test)]
mod tests {
    use super::{build_router, PreviewState};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt; // for `oneshot`

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
```

> Add `tower` to `crates/streamling-core/Cargo.toml` `[dev-dependencies]`: `tower = { version = "0.5", features = ["util"] }` (axum 0.8 re-exports a compatible `tower`; pin to the version already in `Cargo.lock` if present).

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p streamling-core preview::server 2>&1 | tail -20`
Expected: FAIL — `build_router` / `PreviewState` not found.

- [ ] **Step 3: Implement the server**

Add above the `#[cfg(test)]` block in `server.rs`:

```rust
/// Query parameters for `POST /preview`.
#[derive(Deserialize)]
pub struct PreviewQuery {
    /// Requested preview duration in seconds (clamped; defaults to 180).
    pub duration_seconds: Option<u64>,
}

/// Shared single-slot orchestrator state. Holds at most one running child.
#[derive(Clone)]
pub struct PreviewState {
    current: Arc<Mutex<Option<RunChild>>>,
}

impl PreviewState {
    pub fn new() -> Self {
        Self { current: Arc::new(Mutex::new(None)) }
    }
}

impl Default for PreviewState {
    fn default() -> Self {
        Self::new()
    }
}

/// Builds the preview router with the given state.
pub fn build_router(state: PreviewState) -> Router {
    Router::new()
        .route("/preview", post(preview_handler))
        .with_state(state)
}

/// 422 with a plain-text body describing the failure.
fn unprocessable(msg: impl Into<String>) -> Response {
    (StatusCode::UNPROCESSABLE_ENTITY, msg.into()).into_response()
}

async fn preview_handler(
    State(state): State<PreviewState>,
    Query(query): Query<PreviewQuery>,
    body: String,
) -> Response {
    // 1. Rewrite sinks -> blackhole (422 on invalid YAML).
    let rewritten = match rewrite_sinks_to_blackhole(&body) {
        Ok(s) => s,
        Err(e) => return unprocessable(format!("invalid config: {e}")),
    };

    // 2. Write to a temp file (kept alive for the child's lifetime).
    let mut temp = match tempfile::Builder::new()
        .prefix("streamling-preview-")
        .suffix(".yaml")
        .tempfile()
    {
        Ok(f) => f,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("temp file: {e}"))
                .into_response();
        }
    };
    use std::io::Write;
    if let Err(e) = temp.write_all(rewritten.as_bytes()) {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("write config: {e}"))
            .into_response();
    }
    let config_path = temp.path().to_path_buf();

    // 3. Validate via a `--validate` child (422 on failure).
    if let Err(msg) = child::validate_config(&config_path).await {
        return unprocessable(format!("validation failed: {msg}"));
    }

    // 4. Replace any in-flight preview (replace semantics).
    {
        let mut guard = state.current.lock().await;
        if let Some(old) = guard.take() {
            old.kill().await;
        }
        // 5. Spawn the run child with inspect enabled.
        let run_child = match child::spawn_run_child(&config_path).await {
            Ok(c) => c,
            Err(e) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, format!("spawn failed: {e}"))
                    .into_response();
            }
        };
        *guard = Some(run_child);
    }

    let admin_port = {
        let guard = state.current.lock().await;
        guard.as_ref().map(|c| c.admin_port)
    };
    let Some(admin_port) = admin_port else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "no running child").into_response();
    };

    let duration = Duration::from_secs(resolve_duration_secs(query.duration_seconds));

    // 6. Open the child's SSE stream and proxy its bytes back.
    let url = format!("http://127.0.0.1:{admin_port}/admin/live-data");
    let upstream = match reqwest::Client::new().get(&url).send().await {
        Ok(resp) => resp,
        Err(e) => {
            return (StatusCode::BAD_GATEWAY, format!("child SSE connect: {e}"))
                .into_response();
        }
    };

    // Keep the temp file alive until the stream ends, and arm the duration cap:
    // after `duration`, kill the current child, which ends the upstream stream.
    let state_for_timer = state.clone();
    tokio::spawn(async move {
        tokio::time::sleep(duration).await;
        let mut guard = state_for_timer.current.lock().await;
        if let Some(child) = guard.take() {
            child.kill().await;
        }
    });

    let byte_stream = upstream.bytes_stream();
    // Append a terminal SSE event when the upstream ends.
    let terminal = futures::stream::once(async {
        Ok::<_, reqwest::Error>(bytes::Bytes::from_static(
            b"event: end\ndata: {\"reason\":\"preview ended\"}\n\n",
        ))
    });
    // Move `temp` into the stream's closure so it is dropped (deleted) at end.
    let guarded = byte_stream.chain(terminal).map(move |chunk| {
        let _keep = &temp; // hold temp file handle for the stream's lifetime
        chunk
    });

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(guarded))
        .unwrap()
}
```

> **Dependency notes:**
> - Add `bytes` to `crates/streamling-core/Cargo.toml` if not already present (`bytes = "1"`); reqwest re-exports `bytes::Bytes` so it is in the lockfile.
> - `tower` is dev-only (for `oneshot` in the test).
> - The terminal-event closure holds `temp` by move so the temp file lives until the proxied stream completes.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p streamling-core preview::server 2>&1 | tail -20`
Expected: PASS (1 test — 422 on invalid YAML; this path returns before spawning any child).

- [ ] **Step 5: Add `run_preview_server` entrypoint**

Append to `crates/streamling-core/src/preview/mod.rs`:

```rust
pub mod child;
pub mod server;

use crate::error::{Result, ResultExt};

/// Runs the preview HTTP server on `0.0.0.0:{port}` until the process exits.
pub async fn run_preview_server(port: u16) -> Result<()> {
    let app = server::build_router(server::PreviewState::new());
    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .streamling_context("failed to bind preview server")?;
    tracing::info!("Preview server listening on {addr}");
    axum::serve(listener, app)
        .await
        .streamling_context("preview server error")?;
    Ok(())
}
```

(Ensure `mod.rs` declares each submodule exactly once: `pub mod duration; pub mod rewrite; pub mod child; pub mod server;`.)

- [ ] **Step 6: Commit**

```bash
git add crates/streamling-core/Cargo.toml crates/streamling-core/src/preview/
git commit -m "feat(preview): HTTP server, orchestrator, and SSE proxy"
```

---

## Task 6: CLI wiring (`--preview-server`)

**Files:**
- Modify: `crates/streamling/src/main.rs`

- [ ] **Step 1: Write the failing CLI test**

Add to the `#[cfg(test)] mod tests` in `crates/streamling/src/main.rs`:

```rust
#[test]
fn preview_server_flags_parse() {
    let cli = Cli::try_parse_from([
        "streamling",
        "--preview-server",
        "--preview-server-port",
        "9100",
    ])
    .unwrap();
    assert!(cli.preview_server);
    assert_eq!(cli.preview_server_port, 9100);
}

#[test]
fn preview_server_port_has_default() {
    let cli = Cli::try_parse_from(["streamling", "--preview-server"]).unwrap();
    assert!(cli.preview_server);
    assert_eq!(cli.preview_server_port, 8088);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p streamling preview_server 2>&1 | tail -20`
Expected: FAIL — no `preview_server` field.

- [ ] **Step 3: Add the flags and branch**

In the `Cli` struct in `crates/streamling/src/main.rs`, add fields after `validate`:

```rust
    /// Run as a preview HTTP server (POST /preview) instead of running a pipeline.
    #[arg(long)]
    preview_server: bool,

    /// Port for the preview server (with --preview-server).
    #[arg(long, default_value_t = 8088)]
    preview_server_port: u16,
```

In `main()`, branch before the normal config/run flow (right after `let cli = Cli::parse();` and the `validate`/`dry_run` bindings). Initialize standard logging so the server logs, then run the server:

```rust
    if cli.preview_server {
        // Minimal logging for the server process itself.
        use tracing_subscriber::prelude::*;
        let _ = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
            .with(build_env_filter("info"))
            .try_init();

        return match streamling_core::preview::run_preview_server(cli.preview_server_port).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                tracing::error!("preview server exited with error: {}", format_pretty_error(&e));
                ExitCode::FAILURE
            }
        };
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p streamling preview_server 2>&1 | tail -20`
Expected: PASS (2 tests). Also run `cargo test -p streamling cli_definition_is_valid` to confirm the clap definition stays valid.

- [ ] **Step 5: Commit**

```bash
git add crates/streamling/src/main.rs
git commit -m "feat(preview): add --preview-server CLI mode"
```

---

## Task 7: Capstone end-to-end test

Spawns the real built binary in preview-server mode, posts a sink-less CSV `file`-source pipeline (no external infra), and asserts the full wiring: `422` for a broken config, `200 text/event-stream` for a valid one, at least one SSE frame received, and server teardown. Uses `env!("CARGO_BIN_EXE_streamling")`, available to integration tests of the `streamling` crate.

> **Bounded-source caveat (read first):** the `file` source is *bounded* — it reaches EOF and the pipeline (and its admin server) shuts down. So a small CSV preview may end almost immediately, and the proxied stream may deliver only the terminal `event: end` frame before any `data:` frame is observed. The test therefore asserts *at least one SSE frame arrives* (data or terminal), not specifically a data row. Reliably observing live `data:` rows over the full window requires an *unbounded* source (Kafka), which belongs in the `streamling-e2e` Docker suite as a follow-up (see Notes). The CSV fixture is sized to a few rows; do not rely on timing.

**Files:**
- Create: `crates/streamling/tests/preview.rs`
- Modify: `crates/streamling/Cargo.toml` (`[dev-dependencies]`: `reqwest` (workspace), `tokio` with `process`/`time`/`macros`, `tempfile`, `futures`)

- [ ] **Step 1: Write the e2e test**

Create `crates/streamling/tests/preview.rs`. CSV file-source shape taken verbatim from `crates/streamling-e2e/tests/file_source.rs` (`type: file`, `path: <dir>/`, `format: csv`, `primary_key: id`):

```rust
//! End-to-end test for the preview server: real binary, CSV file source, SSE out.

use futures::StreamExt;
use std::time::Duration;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn wait_port(port: u16) {
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server port {port} never came up");
}

#[tokio::test]
async fn preview_validates_runs_and_tears_down() {
    let port = free_port();
    let mut server = tokio::process::Command::new(env!("CARGO_BIN_EXE_streamling"))
        .arg("--preview-server")
        .arg("--preview-server-port")
        .arg(port.to_string())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn preview server");
    wait_port(port).await;

    // --- Broken config (no source for the transform) -> 422 ---
    let broken = "transforms:\n  t:\n    type: sql\n    sql: select * from nope\n    primary_key: id\n";
    let bad = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/preview"))
        .header("content-type", "text/yaml")
        .body(broken)
        .send()
        .await
        .expect("post broken");
    assert_eq!(bad.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);

    // --- Valid sink-less CSV pipeline -> 200 SSE ---
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("csv_data");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::write(data_dir.join("data.csv"), "id,name\n1,alice\n2,bob\n3,carol\n").unwrap();

    let config = format!(
        "sources:\n  events:\n    type: file\n    path: {}/\n    format: csv\n    primary_key: id\ntransforms:\n  passthrough:\n    type: sql\n    sql: select * from events\n    primary_key: id\n",
        data_dir.display()
    );

    let resp = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/preview?duration_seconds=5"))
        .header("content-type", "text/yaml")
        .body(config)
        .send()
        .await
        .expect("post preview");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("text/event-stream")
    );

    // Assert at least one SSE frame arrives (a `data:` row OR the terminal
    // `event: end`); see the bounded-source caveat above.
    let mut stream = resp.bytes_stream();
    let mut saw_frame = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(2), stream.next()).await {
            Ok(Some(Ok(bytes))) => {
                let text = String::from_utf8_lossy(&bytes);
                if text.contains("data:") || text.contains("event: end") {
                    saw_frame = true;
                    break;
                }
            }
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => {}
        }
    }
    assert!(saw_frame, "expected at least one SSE frame from the preview stream");

    let _ = server.start_kill();
    let _ = server.wait().await;
}
```

- [ ] **Step 2: Run the e2e test**

Run: `cargo test -p streamling --test preview -- --nocapture 2>&1 | tail -40`
Expected: PASS — broken config → 422; valid config → 200 SSE with at least one frame; server torn down.

- [ ] **Step 3: Commit**

```bash
git add crates/streamling/Cargo.toml crates/streamling/tests/preview.rs
git commit -m "test(preview): e2e preview server validation, SSE, teardown"
```

---

## Task 8: Full build, fmt, clippy, and final verification

- [ ] **Step 1: Format and lint**

Run: `cargo fmt --all && cargo clippy -p streamling-core -p streamling --all-targets 2>&1 | tail -30`
Expected: no warnings introduced by new code (fix any that are).

- [ ] **Step 2: Run the full preview test suite**

Run: `cargo test -p streamling-core preview && cargo test -p streamling preview 2>&1 | tail -40`
Expected: all preview unit + e2e tests pass.

- [ ] **Step 3: Manual smoke (optional, documents usage)**

```bash
cargo run -p streamling -- --preview-server --preview-server-port 8088 &
curl -N -X POST 'http://127.0.0.1:8088/preview?duration_seconds=10' \
  -H 'content-type: text/yaml' --data-binary @pipeline.yaml
```
Expected: SSE `data:` lines for ~10s, then an `event: end` line.

- [ ] **Step 4: Commit any fixes**

```bash
git add -A
git commit -m "chore(preview): fmt, clippy, and verification fixes"
```

---

## Notes for the implementer

- **Error macros:** the plan references `streamling_user_err!`, `streamling_internal_err!`, and `streamling_context`. These appear in the existing code (`topology_validation.rs`, `main.rs`). If a name differs, grep `crates/streamling-core/src/error.rs` for the actual macro/extension-trait names and substitute.
- **`from` semantics:** preview swaps preserve each sink's `from` so upstream nodes stay consumed; the inspect tap reads sources/transforms (not sinks), so swapping sinks never removes a tapped node.
- **Replace semantics:** a second `POST /preview` kills the first child under the mutex before spawning; the first caller's proxied stream ends when its child dies, then receives the `event: end` terminal frame.
- **Duration cap:** enforced by a timer task that kills the current child after the resolved duration; killing the child closes its SSE, ending the proxied response.
- **Client-disconnect teardown (known limitation):** in this version the child is torn down by the duration timer or by the next request's replace step — *not* immediately on client disconnect. When the caller drops the connection, the proxied response body is dropped but the child keeps running until one of those two events. Given single-slot concurrency and a ≤600s cap, this is acceptable for v1. To make teardown immediate, wrap the proxied stream in a guard whose `Drop` spawns a task that kills `state.current` (the spec lists disconnect-teardown as a requirement; this is the one place v1 approximates it — call it out in the PR).
- **Bounded vs. unbounded sources (design note):** the child is the normal `streamling` binary, which tears down its admin/inspect server when the pipeline *completes*. Unbounded sources (Kafka) run until the duration timer kills them, so inspect stays up for the whole window — the common preview case. Bounded sources (file) reach EOF and exit early, ending the preview early; that is correct behaviour, but it makes live-row assertions timing-dependent (see Task 7's caveat). If product wants bounded-source previews to hold the window open (e.g. keep serving the last-seen rows for `duration_seconds`), that requires a child-side preview flag that defers admin-server teardown until SIGTERM — out of scope for v1; raise as a follow-up if needed.
