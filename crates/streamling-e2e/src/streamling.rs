//! Streamling binary execution helpers.

use crate::{E2eError, Result};
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use tokio::process::Command;
use tracing::info;

/// Output from running streamling with captured stdout/stderr
#[derive(Debug, Clone)]
pub struct StreamlingOutput {
    /// Exit status of the process
    pub status: ExitStatus,
    /// Captured stdout
    pub stdout: String,
    /// Captured stderr
    pub stderr: String,
}

/// Find the path to the streamling directory
/// Allows overrides via E2E_STREAMLING_DIR for cross-repo e2e tests (e.g. plugins repo).
/// Falls back to CARGO_MANIFEST_DIR-based workspace detection.
fn find_streamling_dir() -> Option<PathBuf> {
    std::env::var("E2E_STREAMLING_DIR")
        .ok()
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var("CARGO_MANIFEST_DIR")
                .ok()
                .and_then(|manifest_dir| {
                    Path::new(&manifest_dir)
                        .parent()
                        .and_then(|p| p.parent())
                        .map(|p| p.join("crates/streamling"))
                })
        })
}

fn construct_program_with_args(binary_path: Option<&Path>) -> (String, Vec<String>) {
    if let Some(bin) = binary_path {
        // Use pre-built binary
        (bin.to_string_lossy().to_string(), vec![])
    } else {
        // Use cargo run with package selector
        (
            "cargo".to_string(),
            vec![
                "run".to_string(),
                "--release".to_string(),
                "-p".to_string(),
                "streamling".to_string(),
                "--".to_string(),
            ],
        )
    }
}

/// Run the streamling binary with the given pipeline file
pub async fn run_streamling(
    pipeline_path: &Path,
    binary_path: Option<&Path>,
    env_vars: &[(String, String)],
) -> Result<ExitStatus> {
    let streamling_dir = find_streamling_dir();
    let (program, args) = construct_program_with_args(binary_path);

    let mut cmd = Command::new(&program);

    // Add cargo args if using cargo run
    for arg in &args {
        cmd.arg(arg);
    }

    // Run from crates/streamling/ so any relative paths in a pipeline (e.g. state.db)
    // resolve consistently. Config and the WASM runtime are embedded in the binary.
    if let Some(ref dir) = streamling_dir {
        cmd.current_dir(dir);
        info!("Running streamling from directory: {}", dir.display());
    }

    // Set pipeline location via environment variable (streamling reads config first)
    cmd.env(
        "STREAMLING__PIPELINE_DEFINITION_LOCATION",
        pipeline_path.to_string_lossy().to_string(),
    );

    // Add environment variables
    for (key, value) in env_vars {
        cmd.env(key, value);
    }

    info!(
        "Running streamling with pipeline: {}",
        pipeline_path.display()
    );

    // Check if we should show streamling output (default to true)
    let show_output = std::env::var("E2E_SHOW_STREAMLING_OUTPUT")
        .map(|v| v != "0" && v != "false")
        .unwrap_or(true);

    let status = if show_output {
        // Stream output in real-time
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let mut child = cmd.spawn()?;

        // Spawn tasks to stream stdout/stderr
        let stdout_handle = child.stdout.take().map(|stdout| {
            tokio::spawn(async move {
                use tokio::io::{AsyncBufReadExt, BufReader};
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::info!(target: "streamling", "{}", line);
                }
            })
        });

        let stderr_handle = child.stderr.take().map(|stderr| {
            tokio::spawn(async move {
                use tokio::io::{AsyncBufReadExt, BufReader};
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::warn!(target: "streamling", "{}", line);
                }
            })
        });

        // Wait for process to finish
        let exit_status = child.wait().await?;

        // Wait for output streams to finish
        if let Some(handle) = stdout_handle {
            let _ = handle.await;
        }
        if let Some(handle) = stderr_handle {
            let _ = handle.await;
        }

        exit_status
    } else {
        // Capture output (original behavior)
        let output = cmd.output().await?;

        // Log captured output
        if !output.stdout.is_empty() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            for line in stdout.lines() {
                tracing::debug!(target: "streamling", "{}", line);
            }
        }
        if !output.stderr.is_empty() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            for line in stderr.lines() {
                if output.status.success() {
                    tracing::debug!(target: "streamling", "{}", line);
                } else {
                    tracing::error!(target: "streamling", "{}", line);
                }
            }
        }

        output.status
    };

    if !status.success() {
        return Err(E2eError::StreamlingFailed(format!(
            "streamling exited with status: {:?}",
            status.code()
        )));
    }

    Ok(status)
}

/// Run streamling, send SIGTERM after `signal_after`, and require the process
/// to exit within `exit_deadline` of the signal (the graceful-shutdown
/// contract: drain and exit well inside the k8s grace period).
///
/// The child is placed in its own process group and the signal is sent to the
/// whole group, so the streamling binary receives it even when launched via
/// `cargo run` (where the direct child is cargo, which does not forward
/// signals). Returns the exit status; errors if the process misses the
/// deadline (after SIGKILLing the group so no orphans leak into later tests).
#[cfg(unix)]
pub async fn run_streamling_with_sigterm(
    pipeline_path: &Path,
    binary_path: Option<&Path>,
    env_vars: &[(String, String)],
    signal_after: std::time::Duration,
    exit_deadline: std::time::Duration,
) -> Result<(ExitStatus, String)> {
    let streamling_dir = find_streamling_dir();
    let (program, args) = construct_program_with_args(binary_path);

    let mut cmd = Command::new(&program);
    for arg in &args {
        cmd.arg(arg);
    }
    if let Some(ref dir) = streamling_dir {
        cmd.current_dir(dir);
        info!("Running streamling from directory: {}", dir.display());
    }
    cmd.env(
        "STREAMLING__PIPELINE_DEFINITION_LOCATION",
        pipeline_path.to_string_lossy().to_string(),
    );
    for (key, value) in env_vars {
        cmd.env(key, value);
    }
    // New process group (pgid == child pid) so `kill -- -PGID` reaches the
    // streamling binary through the `cargo run` wrapper.
    cmd.process_group(0);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    info!(
        "Running streamling with pipeline: {} (SIGTERM after {:?}, exit deadline {:?})",
        pipeline_path.display(),
        signal_after,
        exit_deadline
    );

    let mut child = cmd.spawn()?;
    let pid = child.id().ok_or_else(|| {
        E2eError::StreamlingFailed("child exited before a pid could be observed".to_string())
    })?;

    // Stream output so the drain behaviour is debuggable in test logs.
    let stdout_handle = child.stdout.take().map(|stdout| {
        tokio::spawn(async move {
            use tokio::io::{AsyncBufReadExt, BufReader};
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::info!(target: "streamling", "{}", line);
            }
        })
    });
    // Stderr is both streamed to the test log AND captured, so tests can
    // assert on drain diagnostics (e.g. no scope blew its budget slice).
    let stderr_handle = child.stderr.take().map(|stderr| {
        tokio::spawn(async move {
            use tokio::io::{AsyncBufReadExt, BufReader};
            let mut captured = String::new();
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::warn!(target: "streamling", "{}", line);
                captured.push_str(&line);
                captured.push('\n');
            }
            captured
        })
    });

    let send_group_signal = |sig: &str| {
        let _ = std::process::Command::new("kill")
            .args([sig, "--", &format!("-{}", pid)])
            .status();
    };

    tokio::time::sleep(signal_after).await;
    info!("Sending SIGTERM to streamling process group {}", pid);
    send_group_signal("-TERM");

    let waited = tokio::time::timeout(exit_deadline, child.wait()).await;

    if let Some(handle) = stdout_handle {
        let _ = handle.await;
    }
    let stderr_output = match stderr_handle {
        Some(handle) => handle.await.unwrap_or_default(),
        None => String::new(),
    };

    match waited {
        Ok(status) => Ok((status?, stderr_output)),
        Err(_) => {
            // Missed the deadline: this is exactly the hang-until-SIGKILL bug.
            // Kill the group so the test suite doesn't leak the process.
            send_group_signal("-KILL");
            let _ = child.wait().await;
            Err(E2eError::StreamlingFailed(format!(
                "streamling did not exit within {:?} of SIGTERM (graceful-shutdown hang)",
                exit_deadline
            )))
        }
    }
}

/// Run streamling and wait for it to process a specific number of records
/// This is useful for tests that need streamling to run until a condition is met
pub async fn run_streamling_with_limit(
    pipeline_path: &Path,
    binary_path: Option<&Path>,
    env_vars: &[(String, String)],
    record_limit: u64,
) -> Result<ExitStatus> {
    let mut all_env_vars = env_vars.to_vec();
    all_env_vars.push((
        "STREAMLING__NUM_RECORDS_BEFORE_STOP".to_string(),
        record_limit.to_string(),
    ));

    run_streamling(pipeline_path, binary_path, &all_env_vars).await
}

/// Run streamling and capture stdout/stderr for inspection
///
/// Unlike `run_streamling`, this function always captures output rather than streaming it,
/// and returns the captured output for parsing (e.g., print sink output).
pub async fn run_streamling_with_capture(
    pipeline_path: &Path,
    binary_path: Option<&Path>,
    env_vars: &[(String, String)],
) -> Result<StreamlingOutput> {
    let streamling_dir = find_streamling_dir();
    let (program, args) = construct_program_with_args(binary_path);

    let mut cmd = Command::new(&program);

    // Add cargo args if using cargo run
    for arg in &args {
        cmd.arg(arg);
    }

    // Run from crates/streamling/ so relative pipeline paths resolve consistently.
    // Config and the WASM runtime are embedded in the binary.
    if let Some(ref dir) = streamling_dir {
        cmd.current_dir(dir);
        info!("Running streamling from directory: {}", dir.display());
    }

    // Set pipeline location via environment variable
    cmd.env(
        "STREAMLING__PIPELINE_DEFINITION_LOCATION",
        pipeline_path.to_string_lossy().to_string(),
    );

    // Add environment variables
    for (key, value) in env_vars {
        cmd.env(key, value);
    }

    info!(
        "Running streamling with pipeline (capture mode): {}",
        pipeline_path.display()
    );

    // Capture output
    let output = cmd.output().await?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    // Log captured output for debugging
    if !stdout.is_empty() {
        for line in stdout.lines() {
            tracing::debug!(target: "streamling", "{}", line);
        }
    }
    if !stderr.is_empty() {
        for line in stderr.lines() {
            if output.status.success() {
                tracing::debug!(target: "streamling", "{}", line);
            } else {
                tracing::error!(target: "streamling", "{}", line);
            }
        }
    }

    if !output.status.success() {
        return Err(E2eError::StreamlingFailed(format!(
            "streamling exited with status: {:?}\nstderr: {}",
            output.status.code(),
            stderr
        )));
    }

    Ok(StreamlingOutput {
        status: output.status,
        stdout,
        stderr,
    })
}

/// Run streamling with capture and a record limit
pub async fn run_streamling_with_capture_and_limit(
    pipeline_path: &Path,
    binary_path: Option<&Path>,
    env_vars: &[(String, String)],
    record_limit: u64,
) -> Result<StreamlingOutput> {
    let mut all_env_vars = env_vars.to_vec();
    all_env_vars.push((
        "STREAMLING__NUM_RECORDS_BEFORE_STOP".to_string(),
        record_limit.to_string(),
    ));

    run_streamling_with_capture(pipeline_path, binary_path, &all_env_vars).await
}

/// Run streamling and return raw output, even on failure
///
/// Unlike `run_streamling_with_capture`, this function returns the output
/// regardless of the exit status. This is useful for tests that need to
/// inspect error messages from failed pipeline runs.
pub async fn run_streamling_raw(
    pipeline_path: &Path,
    binary_path: Option<&Path>,
    env_vars: &[(String, String)],
    extra_args: &[String],
) -> Result<StreamlingOutput> {
    let streamling_dir = find_streamling_dir();
    let (program, args) = construct_program_with_args(binary_path);

    let mut cmd = Command::new(&program);

    // Add cargo args if using cargo run
    for arg in &args {
        cmd.arg(arg);
    }

    for arg in extra_args {
        cmd.arg(arg);
    }

    // Run from crates/streamling/ so relative pipeline paths resolve consistently.
    // Config and the WASM runtime are embedded in the binary.
    if let Some(ref dir) = streamling_dir {
        cmd.current_dir(dir);
        info!("Running streamling from directory: {}", dir.display());
    }

    // Set pipeline location via environment variable
    cmd.env(
        "STREAMLING__PIPELINE_DEFINITION_LOCATION",
        pipeline_path.to_string_lossy().to_string(),
    );

    // Add environment variables
    for (key, value) in env_vars {
        cmd.env(key, value);
    }

    info!(
        "Running streamling with pipeline (raw mode): {}",
        pipeline_path.display()
    );

    // Capture output
    let output = cmd.output().await?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    // Log captured output for debugging
    if !stdout.is_empty() {
        for line in stdout.lines() {
            tracing::debug!(target: "streamling", "{}", line);
        }
    }
    if !stderr.is_empty() {
        for line in stderr.lines() {
            if output.status.success() {
                tracing::debug!(target: "streamling", "{}", line);
            } else {
                tracing::error!(target: "streamling", "{}", line);
            }
        }
    }

    // Return output regardless of exit status
    Ok(StreamlingOutput {
        status: output.status,
        stdout,
        stderr,
    })
}
