//! Host-side liveness diagnostics for plugin dispatchers.
//!
//! Plugins run on their own per-library runtime, so a wedged plugin cannot
//! starve the host — but the host also cannot see *inside* the plugin. What it
//! can see is the metrics channel: the SDK dispatchers emit lightweight
//! liveness markers there (a throttled `dispatcher.heartbeat`, and
//! `dispatcher.hook.enter`/`.exit` breadcrumbs around the checkpoint-marker
//! flush). The metrics forwarder intercepts these into the maps below instead
//! of forwarding them to telemetry.
//!
//! Everything here is `std`-sync only, on purpose: the shutdown watchdog is a
//! plain OS thread, and crossbeam channels need no runtime — so this
//! attribution stays readable even if every tokio worker in the process is
//! stalled. That is the whole point: the drain's normal "which plugin blew the
//! budget" logging is itself async and dies with the runtime; this must not.
//!
//! The canary complements it from the other side: a host-runtime task touches
//! a timestamp twice a second, and a monitor OS thread flags when the touch
//! goes stale — turning "does host-runtime starvation actually happen?" (today
//! only reachable via plugin UDF `block_on` or a legacy shared-runtime
//! library) into a measurement instead of an argument.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use abi_stable::external_types::crossbeam_channel::RReceiver;
use streamling_plugin::ffi::PluginMetric_NE;

/// Metric names the SDK dispatchers emit and the forwarder intercepts.
/// Aliased from the SDK so the emitting and intercepting sides cannot drift.
pub const HEARTBEAT_METRIC: &str = streamling_plugin::ffi::DISPATCHER_HEARTBEAT_METRIC;
pub const HOOK_ENTER_METRIC: &str = streamling_plugin::ffi::DISPATCHER_HOOK_ENTER_METRIC;
pub const HOOK_EXIT_METRIC: &str = streamling_plugin::ffi::DISPATCHER_HOOK_EXIT_METRIC;

/// Process start, the origin every age below is measured from.
static ORIGIN: LazyLock<Instant> = LazyLock::new(Instant::now);

fn now_ms() -> u64 {
    u64::try_from(ORIGIN.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Last time each plugin's dispatcher was heard from, in ms since [`ORIGIN`].
static LAST_HEARD: LazyLock<Mutex<HashMap<String, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The hook each plugin most recently entered, and whether it exited.
#[derive(Clone, Debug)]
struct HookState {
    hook: String,
    entered_at_ms: u64,
    completed: bool,
}

static HOOK_STATE: LazyLock<Mutex<HashMap<String, HookState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// A plugin's name paired with a clone of its metrics receiver.
type NamedMetricsReceiver = (String, RReceiver<PluginMetric_NE>);

/// Clones of each plugin instance's metrics receiver, kept so the watchdog can
/// drain messages a frozen forwarder never got to. Registered alongside the
/// instance registry; never removed (the dump is a process-exit affair).
static DIAG_RECEIVERS: LazyLock<Mutex<Vec<NamedMetricsReceiver>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

/// Canary timestamp, touched by a small host-runtime task. Zero = never
/// touched (canary not started); the monitor treats that as "no data", not
/// starvation.
static CANARY_MS: AtomicU64 = AtomicU64::new(0);

pub fn register_diag_receiver(plugin: &str, receiver: RReceiver<PluginMetric_NE>) {
    if let Ok(mut reg) = DIAG_RECEIVERS.lock() {
        reg.push((plugin.to_string(), receiver));
    }
}

pub fn record_heartbeat(plugin: &str) {
    if let Ok(mut map) = LAST_HEARD.lock() {
        map.insert(plugin.to_string(), now_ms());
    }
}

pub fn record_hook_enter(plugin: &str, hook: &str) {
    record_heartbeat(plugin);
    if let Ok(mut map) = HOOK_STATE.lock() {
        map.insert(
            plugin.to_string(),
            HookState {
                hook: hook.to_string(),
                entered_at_ms: now_ms(),
                completed: false,
            },
        );
    }
}

pub fn record_hook_exit(plugin: &str) {
    record_heartbeat(plugin);
    if let Ok(mut map) = HOOK_STATE.lock()
        && let Some(state) = map.get_mut(plugin)
    {
        state.completed = true;
    }
}

/// Intercept a plugin metric if it is a dispatcher liveness marker. Returns
/// `true` when the metric was diagnostic and must NOT be forwarded to
/// telemetry — the names are host-internal, not user-facing metrics.
pub fn intercept_metric(plugin: &str, metric: &streamling_plugin::ffi::PluginMetric) -> bool {
    let (name, hook) = metric_name_and_hook(metric);
    match name {
        Some(name) => intercept(plugin, name, hook.as_deref()),
        None => false,
    }
}

/// Intercept a dispatcher liveness metric. Returns `true` when the metric was
/// diagnostic and must NOT be forwarded to telemetry.
pub fn intercept(plugin: &str, name: &str, hook_tag: Option<&str>) -> bool {
    match name {
        HEARTBEAT_METRIC => {
            record_heartbeat(plugin);
            true
        }
        HOOK_ENTER_METRIC => {
            record_hook_enter(plugin, hook_tag.unwrap_or("unknown"));
            true
        }
        HOOK_EXIT_METRIC => {
            record_hook_exit(plugin);
            true
        }
        _ => false,
    }
}

/// One line per pending plugin for the drain-timeout warning: how long ago it
/// was last heard from, and whether it is sitting inside a hook.
pub fn describe_pending(pending: &BTreeSet<String>) -> String {
    let heard = LAST_HEARD.lock().ok();
    let hooks = HOOK_STATE.lock().ok();
    let now = now_ms();
    pending
        .iter()
        .map(|p| {
            let age = heard
                .as_ref()
                .and_then(|m| m.get(p))
                .map(|t| {
                    format!(
                        "last heard {:.1}s ago",
                        (now.saturating_sub(*t)) as f64 / 1e3
                    )
                })
                .unwrap_or_else(|| "never heard from".to_string());
            let hook = hooks
                .as_ref()
                .and_then(|m| m.get(p))
                .filter(|s| !s.completed)
                .map(|s| {
                    format!(
                        ", inside hook '{}' for {:.1}s",
                        s.hook,
                        (now.saturating_sub(s.entered_at_ms)) as f64 / 1e3
                    )
                })
                .unwrap_or_default();
            format!("{p}: {age}{hook}")
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// Touched by the host-runtime canary task; read by the monitor OS thread.
pub fn touch_canary() {
    CANARY_MS.store(now_ms().max(1), Ordering::Relaxed);
}

/// How stale the canary is. `None` until the canary task first runs.
pub fn canary_age() -> Option<Duration> {
    match CANARY_MS.load(Ordering::Relaxed) {
        0 => None,
        t => Some(Duration::from_millis(now_ms().saturating_sub(t))),
    }
}

/// Final attribution dump, called by the shutdown watchdog immediately before
/// `process::exit(1)`. OS-thread-safe by construction: `try_recv` on
/// crossbeam channels needs no runtime, locks are `try_lock` so a dying
/// process can never deadlock here, and output is `eprintln!` because the
/// tracing stack may be starved with everything else.
pub fn dump_to_stderr() {
    // First pull in whatever a frozen forwarder left unread, so the maps
    // reflect the plugins' last words rather than the forwarder's last poll.
    if let Ok(reg) = DIAG_RECEIVERS.try_lock() {
        for (plugin, rx) in reg.iter() {
            while let Ok(metric) = rx.try_recv() {
                if let Ok(m) = metric.into_enum() {
                    intercept_metric(plugin, &m);
                }
            }
        }
    }

    eprintln!("[streamling] plugin liveness at watchdog exit:");
    let now = now_ms();
    match (LAST_HEARD.try_lock(), HOOK_STATE.try_lock()) {
        (Ok(heard), Ok(hooks)) => {
            if heard.is_empty() && hooks.is_empty() {
                eprintln!("[streamling]   (no dispatcher liveness data recorded)");
            }
            for (plugin, t) in heard.iter() {
                let hook = hooks
                    .get(plugin)
                    .filter(|s| !s.completed)
                    .map(|s| {
                        format!(
                            " — inside hook '{}' for {:.1}s (SUSPECT)",
                            s.hook,
                            (now.saturating_sub(s.entered_at_ms)) as f64 / 1e3
                        )
                    })
                    .unwrap_or_default();
                eprintln!(
                    "[streamling]   {plugin}: last heard {:.1}s ago{hook}",
                    (now.saturating_sub(*t)) as f64 / 1e3
                );
            }
        }
        _ => eprintln!("[streamling]   (liveness maps locked; skipping)"),
    }
    if let Some(age) = canary_age() {
        eprintln!(
            "[streamling]   host-runtime canary age: {:.1}s{}",
            age.as_secs_f64(),
            if age > Duration::from_secs(3) {
                " (HOST RUNTIME STARVED)"
            } else {
                ""
            }
        );
    }
}

/// Extracts (metric name, hook tag) from a plugin metric, for the watchdog's
/// channel drain. Only Count metrics carry the liveness names.
fn metric_name_and_hook(
    metric: &streamling_plugin::ffi::PluginMetric,
) -> (Option<&str>, Option<String>) {
    use streamling_plugin::ffi::PluginMetric;
    match metric {
        PluginMetric::Count { name, tags, .. } => {
            let hook = tags
                .iter()
                .find(|kv| kv.0.as_str() == streamling_plugin::ffi::DISPATCHER_HOOK_TAG)
                .map(|kv| kv.1.as_str().to_string());
            (Some(name.as_str()), hook)
        }
        _ => (None, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_and_hooks_shape_the_pending_description() {
        record_heartbeat("sink-a");
        record_hook_enter("sink-b", "checkpoint_marker");

        let pending: BTreeSet<String> = ["sink-a", "sink-b", "sink-c"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let desc = describe_pending(&pending);
        assert!(desc.contains("sink-a: last heard"), "{desc}");
        assert!(
            desc.contains("sink-b: last heard") && desc.contains("inside hook 'checkpoint_marker'"),
            "{desc}"
        );
        assert!(desc.contains("sink-c: never heard from"), "{desc}");

        // Exit balances the enter: no longer reported as inside the hook.
        record_hook_exit("sink-b");
        let desc = describe_pending(&pending);
        assert!(!desc.contains("inside hook"), "{desc}");
    }

    #[test]
    fn intercept_consumes_only_liveness_metrics() {
        assert!(intercept("p", HEARTBEAT_METRIC, None));
        assert!(intercept("p", HOOK_ENTER_METRIC, Some("process_batch")));
        assert!(intercept("p", HOOK_EXIT_METRIC, None));
        assert!(!intercept("p", "output_rows", None));
    }

    #[test]
    fn canary_reports_no_data_until_touched() {
        // NOTE: shares the static with other tests in this binary; only the
        // post-touch property is asserted unconditionally.
        touch_canary();
        let age = canary_age().expect("touched canary must report an age");
        assert!(age < Duration::from_secs(1));
    }

    /// The watchdog-path dump must never panic or block, even with empty maps
    /// and no receivers.
    #[test]
    fn dump_is_infallible() {
        dump_to_stderr();
    }
}
