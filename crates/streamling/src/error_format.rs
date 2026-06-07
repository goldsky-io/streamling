use std::fmt::{Display, Write as _};
use streamling_core::plugin::terminate_all_plugins;
use streamling_core::utils::arrow::should_suppress_panic_logging;

pub fn format_pretty_error(message: impl Display) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "\n========== Streamling Error ==========");
    let _ = writeln!(out, "{}", sanitize(&message.to_string()));
    let _ = writeln!(out, "=====================================\n");
    out
}

fn sanitize(s: &str) -> String {
    // Ensure long SQL or YAML blocks remain readable
    s.trim().to_string()
}

pub fn install_global_panic_hook() {
    std::panic::set_hook(Box::new(|panic_info| {
        // Skip logging and termination for controlled panics (e.g., safe_take overflow handling).
        // These panics are expected and caught by catch_unwind - no need to alarm users.
        if should_suppress_panic_logging() {
            return;
        }

        let _ = terminate_all_plugins();
        let location = panic_info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_else(|| "<unknown>".to_string());
        let msg = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "<non-string panic payload>".to_string()
        };
        let pretty = format_pretty_error(format!("panic: {} at {}", msg, location));
        let bt = std::backtrace::Backtrace::force_capture();

        // Always write to stderr so panics are visible even before tracing is initialized
        eprintln!("{}", pretty);
        eprintln!("panic backtrace:\n{:#}", bt);

        tracing::error!(target = "streamling", "{}", pretty);
        tracing::error!(target = "streamling", "panic backtrace:\n{:#}", bt);
    }));
}
