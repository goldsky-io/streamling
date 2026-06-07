use serde::Serialize;
use std::fmt;
use std::sync::{Arc, Mutex};
use streamling_core::error::StreamlingError;
use tracing::Level;
use tracing::field::{Field, Visit};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

/// A single log event captured during `--validate` mode.
///
/// Used to collect warnings emitted by the pipeline so they can be surfaced
/// in the structured JSON validation output.
pub struct CapturedLog {
    pub level: Level,
    pub message: String,
}

/// A [`tracing_subscriber::Layer`] that captures log events into a shared buffer.
///
/// Attach this layer to a tracing subscriber to collect warnings (and other
/// log levels) during a pipeline dry-run so they can be included in the
/// [`ValidationOutput`] JSON response.
#[derive(Clone)]
pub struct LogCaptureLayer {
    logs: Arc<Mutex<Vec<CapturedLog>>>,
}

impl Default for LogCaptureLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl LogCaptureLayer {
    pub fn new() -> Self {
        Self {
            logs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn logs(&self) -> Arc<Mutex<Vec<CapturedLog>>> {
        self.logs.clone()
    }
}

impl<S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>> Layer<S>
    for LogCaptureLayer
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);

        let message = if visitor.message.is_empty() {
            visitor.debug_value.unwrap_or_default()
        } else {
            visitor.message
        };

        if !message.is_empty() {
            self.logs
                .lock()
                .expect("log capture mutex poisoned")
                .push(CapturedLog {
                    level: *event.metadata().level(),
                    message,
                });
        }
    }
}

#[derive(Default)]
struct MessageVisitor {
    message: String,
    debug_value: Option<String>,
}

impl Visit for MessageVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{:?}", value);
        } else if self.debug_value.is_none() {
            self.debug_value = Some(format!("{:?}", value));
        }
    }
}

#[derive(Serialize)]
pub struct ValidationOutput {
    /// Whether validation infrastructure ran to completion (not an internal failure).
    pub success: bool,
    /// Whether the pipeline is valid — `true` only when `errors` is empty.
    pub is_valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

impl ValidationOutput {
    /// Build validation output from the pipeline run result.
    ///
    /// `success` indicates whether validation itself ran to completion:
    /// - `true` when there is no error, or the error is user-facing (not internal)
    /// - `false` when the error is internal (infrastructure / platform failure)
    ///
    /// `is_valid` is the unambiguous "is the pipeline valid?" signal:
    /// - `true` only when `errors` is empty
    /// - `false` whenever there are any errors (user-facing or internal)
    pub fn build(run_error: Option<&StreamlingError>, logs: &[CapturedLog]) -> Self {
        let success = match run_error {
            None => true,
            Some(e) => !e.is_internal(),
        };

        let errors: Vec<String> = run_error
            .map(|e| e.to_string())
            .filter(|s| !s.trim().is_empty())
            .into_iter()
            .collect();

        let warnings: Vec<String> = logs
            .iter()
            .filter(|l| l.level == Level::WARN)
            .map(|l| l.message.clone())
            .collect();

        let is_valid = errors.is_empty();

        ValidationOutput {
            success,
            is_valid,
            errors,
            warnings,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_with_internal_error() {
        let err = StreamlingError::new("connection refused");
        let output = ValidationOutput::build(Some(&err), &[]);
        assert!(!output.success, "internal error → success should be false");
        assert!(
            !output.is_valid,
            "errors present → is_valid should be false"
        );
        assert_eq!(output.errors, vec!["connection refused"]);
        assert!(output.warnings.is_empty());
    }

    #[test]
    fn test_build_with_user_error() {
        let err = StreamlingError::user("primary key 'foo' not found in schema");
        let output = ValidationOutput::build(Some(&err), &[]);
        assert!(output.success, "user error → success should be true");
        assert!(
            !output.is_valid,
            "errors present → is_valid should be false"
        );
        assert_eq!(output.errors, vec!["primary key 'foo' not found in schema"]);
    }

    #[test]
    fn test_build_no_error() {
        let output = ValidationOutput::build(None, &[]);
        assert!(output.success);
        assert!(output.is_valid, "no errors → is_valid should be true");
        assert!(output.errors.is_empty());
    }

    #[test]
    fn test_build_warnings_from_logs() {
        let logs = vec![
            CapturedLog {
                level: Level::WARN,
                message: "a warning".to_string(),
            },
            CapturedLog {
                level: Level::ERROR,
                message: "this error log is ignored".to_string(),
            },
            CapturedLog {
                level: Level::WARN,
                message: "another warning".to_string(),
            },
        ];
        let output = ValidationOutput::build(None, &logs);
        assert!(output.success);
        assert!(
            output.is_valid,
            "warnings alone don't invalidate the pipeline"
        );
        assert!(output.errors.is_empty());
        assert_eq!(output.warnings, vec!["a warning", "another warning"]);
    }

    #[test]
    fn test_build_user_error_and_warnings() {
        let logs = vec![CapturedLog {
            level: Level::WARN,
            message: "some warning".to_string(),
        }];
        let err = StreamlingError::user("pipeline failed");
        let output = ValidationOutput::build(Some(&err), &logs);
        assert!(output.success, "user error → success should be true");
        assert!(
            !output.is_valid,
            "errors present → is_valid should be false"
        );
        assert_eq!(output.errors, vec!["pipeline failed"]);
        assert_eq!(output.warnings, vec!["some warning"]);
    }

    #[test]
    fn test_build_internal_error_and_warnings() {
        let logs = vec![CapturedLog {
            level: Level::WARN,
            message: "some warning".to_string(),
        }];
        let err = StreamlingError::new("infra failure");
        let output = ValidationOutput::build(Some(&err), &logs);
        assert!(!output.success, "internal error → success should be false");
        assert!(
            !output.is_valid,
            "errors present → is_valid should be false"
        );
        assert_eq!(output.errors, vec!["infra failure"]);
        assert_eq!(output.warnings, vec!["some warning"]);
    }
}
