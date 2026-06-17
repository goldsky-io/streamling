//! Spawns and supervises `streamling` child processes for previews.

use crate::error::{Result, ResultExt};
use crate::streamling_err;
use std::net::TcpListener;
use std::path::Path;
use std::time::Duration;
use tokio::process::{Child, Command};

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
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(streamling_err!(
                "preview child admin API did not come up on port {port} within {timeout:?}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::pick_free_port;

    #[test]
    fn pick_free_port_returns_bindable_port() {
        let port = pick_free_port().unwrap();
        assert!(port > 0);
        let _l = std::net::TcpListener::bind(("127.0.0.1", port)).unwrap();
    }
}
