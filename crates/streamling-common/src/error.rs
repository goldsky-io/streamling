use std::any::Any;
use std::backtrace::Backtrace;
use std::fmt;
use std::io::ErrorKind;

use datafusion::error::DataFusionError;

fn capture_backtrace(retriable: bool) -> Option<Backtrace> {
    if retriable {
        None
    } else {
        Some(Backtrace::force_capture())
    }
}

/// The standard error type for Streamling with support for retry and user-facing semantics.
///
/// # Flags
/// - `retriable`: Indicates the operation can be retried (e.g., transient network errors)
/// - `internal`: Internal errors contain implementation details; user-facing errors are safe to show
///
/// # Example
/// ```ignore
/// use crate::error::{StreamlingError, Result};
///
/// fn connect() -> Result<Connection> {
///     let conn = try_connect()
///         .map_err(|e| StreamlingError::retriable_with_cause("connection failed", e))?;
///     Ok(conn)
/// }
///
/// fn validate_input(input: &str) -> Result<()> {
///     if input.is_empty() {
///         return Err(StreamlingError::user("input cannot be empty"));
///     }
///     Ok(())
/// }
/// ```
pub struct StreamlingError {
    inner: anyhow::Error,
    retriable: bool,
    internal: bool,
    backtrace: Option<Backtrace>,
    /// Reference name of the pipeline topology node the error originated in,
    /// tagged once by the innermost `WrappingExec` that observes it.
    node: Option<String>,
}

impl StreamlingError {
    // =========================================================================
    // Core constructors
    // =========================================================================

    /// Create a new internal, non-retriable error.
    pub fn new<M>(message: M) -> Self
    where
        M: fmt::Display + fmt::Debug + Send + Sync + 'static,
    {
        Self {
            inner: anyhow::Error::msg(message),
            retriable: false,
            internal: true,
            backtrace: Some(Backtrace::force_capture()),
            node: None,
        }
    }

    /// Create an internal, non-retriable error without capturing a backtrace.
    ///
    /// Use for sentinel/wrapper errors where the original backtrace has already
    /// been logged or is not meaningful.
    pub fn new_without_backtrace<M>(message: M) -> Self
    where
        M: fmt::Display + fmt::Debug + Send + Sync + 'static,
    {
        Self {
            inner: anyhow::Error::msg(message),
            retriable: false,
            internal: true,
            backtrace: None,
            node: None,
        }
    }

    /// Create a user-facing, non-retriable error.
    ///
    /// Use this for validation errors, configuration errors, and other cases
    /// where the error message should be shown to the user.
    pub fn user<M>(message: M) -> Self
    where
        M: fmt::Display + fmt::Debug + Send + Sync + 'static,
    {
        Self {
            inner: anyhow::Error::msg(message),
            retriable: false,
            internal: false,
            backtrace: Some(Backtrace::force_capture()),
            node: None,
        }
    }

    /// Create an internal, retriable error.
    ///
    /// Use this for transient failures that may succeed on retry.
    /// No backtrace is captured to avoid overhead on hot retry paths.
    pub fn retriable<M>(message: M) -> Self
    where
        M: fmt::Display + fmt::Debug + Send + Sync + 'static,
    {
        Self {
            inner: anyhow::Error::msg(message),
            retriable: true,
            internal: true,
            backtrace: None,
            node: None,
        }
    }

    /// Create a StreamlingError from raw parts.
    ///
    /// Use this when constructing a StreamlingError from an external error type
    /// in a downstream crate that cannot use `From` impls (orphan rule).
    pub fn from_parts(inner: anyhow::Error, retriable: bool, internal: bool) -> Self {
        Self {
            inner,
            retriable,
            internal,
            backtrace: capture_backtrace(retriable),
            node: None,
        }
    }

    // =========================================================================
    // Constructors with cause
    // =========================================================================

    /// Create an internal, non-retriable error with a cause.
    pub fn with_cause<M, E>(message: M, cause: E) -> Self
    where
        M: fmt::Display + Send + Sync + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        Self {
            inner: anyhow::Error::from(cause).context(message),
            retriable: false,
            internal: true,
            backtrace: Some(Backtrace::force_capture()),
            node: None,
        }
    }

    /// Create a user-facing, non-retriable error with a cause.
    pub fn user_with_cause<M, E>(message: M, cause: E) -> Self
    where
        M: fmt::Display + Send + Sync + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        Self {
            inner: anyhow::Error::from(cause).context(message),
            retriable: false,
            internal: false,
            backtrace: Some(Backtrace::force_capture()),
            node: None,
        }
    }

    /// Create an internal, retriable error with a cause.
    /// No backtrace is captured to avoid overhead on hot retry paths.
    pub fn retriable_with_cause<M, E>(message: M, cause: E) -> Self
    where
        M: fmt::Display + Send + Sync + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        Self {
            inner: anyhow::Error::from(cause).context(message),
            retriable: true,
            internal: true,
            backtrace: None,
            node: None,
        }
    }

    // =========================================================================
    // Chainable modifiers
    // =========================================================================

    /// Mark this error as retriable, clearing any captured backtrace.
    #[must_use]
    pub fn mark_retriable(mut self) -> Self {
        self.retriable = true;
        self.backtrace = None;
        self
    }

    /// Mark this error as user-facing (not internal).
    #[must_use]
    pub fn mark_user_facing(mut self) -> Self {
        self.internal = false;
        self
    }

    /// Mark this error as internal.
    #[must_use]
    pub fn mark_internal(mut self) -> Self {
        self.internal = true;
        self
    }

    /// Strip the captured backtrace from this error.
    #[must_use]
    pub fn without_backtrace(mut self) -> Self {
        self.backtrace = None;
        self
    }

    /// Reconstruct this error from its Display output, preserving
    /// `retriable` and `internal` flags but discarding the backtrace.
    /// The cause chain is flattened to a single message string, so
    /// `.source()` on the result will return `None`.
    ///
    /// Used when a `DataFusionError` (which is not `Clone`) must be
    /// duplicated across broadcast fan-out channels.
    pub fn clone_flags_with_message(&self) -> Self {
        Self {
            inner: anyhow::Error::msg(self.to_string()),
            retriable: self.retriable,
            internal: self.internal,
            backtrace: None,
            node: self.node.clone(),
        }
    }

    /// Add context to an error, preserving flags and backtrace.
    #[must_use]
    pub fn context<C>(self, context: C) -> Self
    where
        C: fmt::Display + Send + Sync + 'static,
    {
        Self {
            inner: self.inner.context(context),
            retriable: self.retriable,
            internal: self.internal,
            backtrace: self.backtrace,
            node: self.node,
        }
    }

    // =========================================================================
    // Accessors
    // =========================================================================

    /// Returns true if this error can be retried.
    pub fn is_retriable(&self) -> bool {
        self.retriable
    }

    /// Returns true if this is an internal error (not user-facing).
    pub fn is_internal(&self) -> bool {
        self.internal
    }

    /// Returns a reference to the underlying anyhow::Error.
    pub fn inner(&self) -> &anyhow::Error {
        &self.inner
    }

    /// Returns the backtrace captured at error creation, if available.
    /// Non-retriable errors always have a backtrace; retriable errors do not.
    pub fn backtrace(&self) -> Option<&Backtrace> {
        self.backtrace.as_ref()
    }

    /// Tag this error with the topology node it originated in. First tag
    /// wins: every downstream `WrappingExec` sees the same error on its own
    /// `Err` branch, and only the innermost (closest to the failure) knows
    /// the true origin.
    #[must_use]
    pub fn with_node<N: Into<String>>(mut self, node: N) -> Self {
        if self.node.is_none() {
            self.node = Some(node.into());
        }
        self
    }

    /// Reference name of the topology node this error originated in, if known.
    pub fn node(&self) -> Option<&str> {
        self.node.as_deref()
    }
}

impl StreamlingError {
    /// Formats the error chain without any backtrace (user-facing).
    fn format_stripped(&self) -> String {
        strip_backtrace(&format!("{:?}", self.inner))
    }
}

impl fmt::Debug for StreamlingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.format_stripped())?;
        if let Some(bt) = &self.backtrace {
            // {:#} uses the full/alternate format which shows all captured frames
            // including the call chain above the error creation site.
            // The default {} format aggressively filters to only a few frames.
            write!(f, "\n\nbacktrace:\n{:#}", bt)?;
        }
        Ok(())
    }
}

impl fmt::Display for StreamlingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.format_stripped())
    }
}

impl std::error::Error for StreamlingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.inner.source()
    }
}

// =============================================================================
// From implementations
// =============================================================================

/// Automatic conversion from StreamlingError to DataFusionError.
///
/// Always wraps in `DataFusionError::External` so that the full
/// [`StreamlingError`] (including `retriable` / `internal` flags) is preserved
/// for lossless downcast recovery by `From<DataFusionError> for StreamlingError`.
///
/// Earlier versions mapped internal errors to `DataFusionError::Internal`, but
/// that variant's Display output appends misleading "file a bug report" text
/// which permanently polluted round-tripped error messages.
impl From<StreamlingError> for DataFusionError {
    fn from(err: StreamlingError) -> Self {
        DataFusionError::External(Box::new(err))
    }
}

/// Convert from anyhow::Error to StreamlingError (internal, non-retriable).
impl From<anyhow::Error> for StreamlingError {
    fn from(err: anyhow::Error) -> Self {
        Self {
            inner: err,
            retriable: false,
            internal: true,
            backtrace: Some(Backtrace::force_capture()),
            node: None,
        }
    }
}

/// Convert from std::io::Error to StreamlingError.
/// Certain I/O error kinds are marked as retriable (no backtrace captured for those).
impl From<std::io::Error> for StreamlingError {
    fn from(err: std::io::Error) -> Self {
        let retriable = matches!(
            err.kind(),
            ErrorKind::ConnectionRefused
                | ErrorKind::ConnectionReset
                | ErrorKind::ConnectionAborted
                | ErrorKind::NotConnected
                | ErrorKind::TimedOut
                | ErrorKind::Interrupted
        );
        Self {
            inner: err.into(),
            retriable,
            internal: true,
            backtrace: capture_backtrace(retriable),
            node: None,
        }
    }
}

/// Convert from arrow errors (internal, non-retriable).
///
/// Flattened via Display to avoid duplicate "Caused by" lines — some ArrowError
/// variants (ExternalError, IoError) embed their source's text in Display while
/// also returning it from source(), which causes anyhow to render it twice.
impl From<arrow_schema::ArrowError> for StreamlingError {
    fn from(err: arrow_schema::ArrowError) -> Self {
        Self {
            inner: anyhow::anyhow!("{}", err),
            retriable: false,
            internal: true,
            backtrace: Some(Backtrace::force_capture()),
            node: None,
        }
    }
}

/// Convert from DataFusion errors.
///
/// If the error is an `External` wrapping a [`StreamlingError`] (possibly
/// inside one or more `Context` layers), the original is recovered via
/// downcast so that flags like `retriable` and `internal` are preserved.
///
/// For native DataFusion errors, `Plan`, `NotImplemented`, and
/// `Configuration` variants are treated as user-facing because they
/// typically represent validation, unsupported-feature, or bad-settings
/// errors. All other variants are treated as internal.
impl From<DataFusionError> for StreamlingError {
    fn from(err: DataFusionError) -> Self {
        match try_recover_streamling_error(err) {
            Ok(recovered) => recovered,
            Err(err) => {
                let internal = !is_user_facing_datafusion_error(&err);
                Self {
                    inner: anyhow::anyhow!("{}", err),
                    retriable: false,
                    internal,
                    backtrace: Some(Backtrace::force_capture()),
                    node: None,
                }
            }
        }
    }
}

/// Try to recover a [`StreamlingError`] from a `DataFusionError::External`,
/// unwrapping any `Context` layers along the way. Context messages from
/// DataFusion are re-applied to the recovered error via
/// [`StreamlingError::context`].
///
/// Returns `Err(original)` (reconstructed) if the error does not contain a
/// `StreamlingError`.
fn try_recover_streamling_error(
    err: DataFusionError,
) -> std::result::Result<StreamlingError, DataFusionError> {
    match err {
        DataFusionError::External(boxed) => match boxed.downcast::<StreamlingError>() {
            Ok(se) => Ok(*se),
            Err(boxed) => Err(DataFusionError::External(boxed)),
        },
        DataFusionError::Context(msg, inner) => match try_recover_streamling_error(*inner) {
            Ok(se) => Ok(se.context(msg)),
            Err(inner) => Err(DataFusionError::Context(msg, Box::new(inner))),
        },
        other => Err(other),
    }
}

/// Iteratively unwrap `DataFusionError::Context` to check whether the
/// innermost error is a user-facing variant (`Plan`, `NotImplemented`,
/// or `Configuration`).
fn is_user_facing_datafusion_error(err: &DataFusionError) -> bool {
    let mut current = err;
    loop {
        match current {
            DataFusionError::Plan(_)
            | DataFusionError::NotImplemented(_)
            | DataFusionError::Configuration(_) => return true,
            DataFusionError::Context(_, inner) => current = inner,
            _ => return false,
        }
    }
}

// =============================================================================
// Context trait
// =============================================================================

/// Extension trait to add context to any Result that can be converted to StreamlingError.
///
/// For `Result<T, StreamlingError>`, flags are preserved.
/// For other error types, creates an internal, non-retriable error.
///
/// Named `ResultExt` to avoid conflict with `anyhow::Context`.
pub trait ResultExt<T> {
    /// Add context to an error.
    fn streamling_context<C>(self, context: C) -> Result<T>
    where
        C: fmt::Display + Send + Sync + 'static;

    /// Add context lazily (only evaluated on error).
    fn streamling_with_context<C, F>(self, f: F) -> Result<T>
    where
        C: fmt::Display + Send + Sync + 'static,
        F: FnOnce() -> C;
}

/// Helper trait to extract flags and backtrace from StreamlingError via downcasting.
trait ExtractFlags: Any {
    fn extract_flags(&self) -> Option<(bool, bool)>;
    fn take_backtrace(&mut self) -> Option<Backtrace>;
    fn take_node(&mut self) -> Option<String>;
    fn take_inner_anyhow(&mut self) -> Option<anyhow::Error>;
}

impl<E: std::error::Error + 'static> ExtractFlags for E {
    fn extract_flags(&self) -> Option<(bool, bool)> {
        (self as &dyn Any)
            .downcast_ref::<StreamlingError>()
            .map(|e| (e.retriable, e.internal))
    }

    fn take_backtrace(&mut self) -> Option<Backtrace> {
        (self as &mut dyn Any)
            .downcast_mut::<StreamlingError>()
            .and_then(|e| e.backtrace.take())
    }

    fn take_node(&mut self) -> Option<String> {
        (self as &mut dyn Any)
            .downcast_mut::<StreamlingError>()
            .and_then(|e| e.node.take())
    }

    fn take_inner_anyhow(&mut self) -> Option<anyhow::Error> {
        (self as &mut dyn Any)
            .downcast_mut::<StreamlingError>()
            // The source StreamlingError is consumed by map_err immediately after this
            // call, so the sentinel is never observed. We still use a descriptive value
            // in case future refactors break that invariant.
            .map(|e| std::mem::replace(&mut e.inner, anyhow::anyhow!("[error moved]")))
    }
}

/// Returns true when an error is a known type whose Display output embeds its
/// source()'s text, which would cause anyhow to render duplicated "Caused by" lines.
///
/// Uses an explicit type allowlist rather than a generic substring check to avoid
/// false positives when the source text is coincidentally a substring of the outer
/// error's own message.
fn has_duplicate_source<E: std::error::Error + 'static>(e: &E) -> bool {
    use std::any::TypeId;
    let tid = TypeId::of::<E>();
    (tid == TypeId::of::<DataFusionError>() || tid == TypeId::of::<arrow_schema::ArrowError>())
        && e.source().is_some()
}

impl<T, E> ResultExt<T> for std::result::Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn streamling_context<C>(self, context: C) -> Result<T>
    where
        C: fmt::Display + Send + Sync + 'static,
    {
        self.map_err(|mut e| {
            let (retriable, internal) = e.extract_flags().unwrap_or((false, true));
            let backtrace = e.take_backtrace().or_else(|| capture_backtrace(retriable));
            let node = e.take_node();
            let inner = match e.take_inner_anyhow() {
                Some(inner_anyhow) => inner_anyhow.context(context),
                None if has_duplicate_source(&e) => anyhow::anyhow!("{}", e).context(context),
                None => anyhow::Error::from(e).context(context),
            };
            StreamlingError {
                inner,
                retriable,
                internal,
                backtrace,
                node,
            }
        })
    }

    fn streamling_with_context<C, F>(self, f: F) -> Result<T>
    where
        C: fmt::Display + Send + Sync + 'static,
        F: FnOnce() -> C,
    {
        self.map_err(|mut e| {
            let (retriable, internal) = e.extract_flags().unwrap_or((false, true));
            let backtrace = e.take_backtrace().or_else(|| capture_backtrace(retriable));
            let node = e.take_node();
            let inner = match e.take_inner_anyhow() {
                Some(inner_anyhow) => inner_anyhow.context(f()),
                None if has_duplicate_source(&e) => anyhow::anyhow!("{}", e).context(f()),
                None => anyhow::Error::from(e).context(f()),
            };
            StreamlingError {
                inner,
                retriable,
                internal,
                backtrace,
                node,
            }
        })
    }
}

// =============================================================================
// Macros
// =============================================================================

/// Create an internal, non-retriable StreamlingError.
///
/// # Example
/// ```ignore
/// use streamling_core::streamling_err;
/// let err = streamling_err!("failed to process: {}", item_id);
/// ```
#[macro_export]
macro_rules! streamling_err {
    ($($arg:tt)*) => {
        $crate::error::StreamlingError::new(format!($($arg)*))
    };
}

/// Early return with an internal, non-retriable StreamlingError.
///
/// # Example
/// ```ignore
/// use streamling_core::streamling_bail;
/// if value < 0 {
///     streamling_bail!("value must be non-negative, got {}", value);
/// }
/// ```
#[macro_export]
macro_rules! streamling_bail {
    ($($arg:tt)*) => {
        return Err($crate::streamling_err!($($arg)*).into())
    };
}

/// Create a user-facing, non-retriable StreamlingError.
///
/// # Example
/// ```ignore
/// use streamling_core::streamling_user_err;
/// let err = streamling_user_err!("invalid configuration: {}", reason);
/// ```
#[macro_export]
macro_rules! streamling_user_err {
    ($($arg:tt)*) => {
        $crate::error::StreamlingError::user(format!($($arg)*))
    };
}

/// Early return with a user-facing, non-retriable StreamlingError.
///
/// # Example
/// ```ignore
/// use streamling_core::streamling_user_bail;
/// if input.is_empty() {
///     streamling_user_bail!("input cannot be empty");
/// }
/// ```
#[macro_export]
macro_rules! streamling_user_bail {
    ($($arg:tt)*) => {
        return Err($crate::streamling_user_err!($($arg)*).into())
    };
}

/// Create an internal, retriable StreamlingError.
///
/// # Example
/// ```ignore
/// use streamling_core::streamling_retriable_err;
/// let err = streamling_retriable_err!("connection to {} timed out", host);
/// ```
#[macro_export]
macro_rules! streamling_retriable_err {
    ($($arg:tt)*) => {
        $crate::error::StreamlingError::retriable(format!($($arg)*))
    };
}

// =============================================================================
// Type aliases and conversion traits
// =============================================================================

/// Convenience type alias for Results using StreamlingError.
pub type Result<T> = std::result::Result<T, StreamlingError>;

/// Extension trait to convert anyhow::Result to our Result type.
pub trait IntoStreamlingResult<T> {
    fn into_streamling(self) -> Result<T>;
}

impl<T> IntoStreamlingResult<T> for anyhow::Result<T> {
    fn into_streamling(self) -> Result<T> {
        self.map_err(StreamlingError::from)
    }
}

// =============================================================================
// Backtrace stripping
// =============================================================================

/// Strips embedded backtrace from an error message.
/// Handles multiple formats:
/// - "backtrace:" (standard anyhow format)
/// - "Stack backtrace:" (std::backtrace format)
pub fn strip_backtrace(msg: &str) -> String {
    let mut result = String::new();
    let mut skip_rest = false;

    for line in msg.lines() {
        if skip_rest {
            continue;
        }
        // Check if this line starts a backtrace section
        let trimmed = line.trim();
        if trimmed.starts_with("backtrace:") || trimmed.starts_with("Stack backtrace:") {
            skip_rest = true;
            continue;
        }
        if !result.is_empty() {
            result.push('\n');
        }
        result.push_str(line);
    }

    result.trim().to_string()
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    #[test]
    fn test_new_creates_internal_non_retriable() {
        let err = StreamlingError::new("test error");
        assert!(err.is_internal());
        assert!(!err.is_retriable());
        assert_eq!(err.to_string(), "test error");
    }

    #[test]
    fn test_user_creates_user_facing_non_retriable() {
        let err = StreamlingError::user("invalid input");
        assert!(!err.is_internal());
        assert!(!err.is_retriable());
        assert_eq!(err.to_string(), "invalid input");
    }

    #[test]
    fn test_retriable_creates_internal_retriable() {
        let err = StreamlingError::retriable("connection failed");
        assert!(err.is_internal());
        assert!(err.is_retriable());
        assert_eq!(err.to_string(), "connection failed");
    }

    #[test]
    fn test_with_cause() {
        let cause = std::io::Error::other("disk full");
        let err = StreamlingError::with_cause("write failed", cause);
        assert!(err.is_internal());
        assert!(!err.is_retriable());
        assert!(err.to_string().contains("write failed"));
        assert!(err.to_string().contains("disk full"));
    }

    #[test]
    fn test_user_with_cause() {
        let cause = std::io::Error::new(ErrorKind::NotFound, "file not found");
        let err = StreamlingError::user_with_cause("configuration error", cause);
        assert!(!err.is_internal());
        assert!(!err.is_retriable());
    }

    #[test]
    fn test_retriable_with_cause() {
        let cause = std::io::Error::new(ErrorKind::ConnectionRefused, "refused");
        let err = StreamlingError::retriable_with_cause("connection failed", cause);
        assert!(err.is_internal());
        assert!(err.is_retriable());
    }

    #[test]
    fn test_mark_methods() {
        let err = StreamlingError::new("test")
            .mark_retriable()
            .mark_user_facing();
        assert!(!err.is_internal());
        assert!(err.is_retriable());

        let err = err.mark_internal();
        assert!(err.is_internal());
    }

    #[test]
    fn test_context_preserves_flags() {
        let err = StreamlingError::retriable("original")
            .mark_user_facing()
            .context("added context");

        assert!(err.is_retriable());
        assert!(!err.is_internal());
        assert!(err.to_string().contains("added context"));
    }

    #[test]
    fn test_from_io_error_retriable() {
        let io_err = std::io::Error::new(ErrorKind::ConnectionRefused, "refused");
        let err = StreamlingError::from(io_err);
        assert!(err.is_retriable());
        assert!(err.is_internal());
    }

    #[test]
    fn test_from_io_error_non_retriable() {
        let io_err = std::io::Error::new(ErrorKind::NotFound, "not found");
        let err = StreamlingError::from(io_err);
        assert!(!err.is_retriable());
        assert!(err.is_internal());
    }

    #[test]
    fn test_from_anyhow() {
        let anyhow_err = anyhow!("anyhow error");
        let err = StreamlingError::from(anyhow_err);
        assert!(!err.is_retriable());
        assert!(err.is_internal());
    }

    #[test]
    fn test_display_single() {
        let err = StreamlingError::new("connection refused");
        assert_eq!(err.to_string(), "connection refused");
    }

    #[test]
    fn test_display_chain() {
        let err = StreamlingError::from(
            anyhow!("connection refused")
                .context("failed to connect to server")
                .context("failed to initialize client"),
        );
        let msg = err.to_string();
        assert!(msg.contains("failed to initialize client"), "got: {}", msg);
        assert!(msg.contains("Caused by:"), "got: {}", msg);
        assert!(
            msg.contains("0: failed to connect to server"),
            "got: {}",
            msg
        );
        assert!(msg.contains("1: connection refused"), "got: {}", msg);
    }

    #[test]
    fn test_auto_conversion_to_datafusion_always_external() {
        fn returns_streamling_error() -> Result<i32> {
            Err(StreamlingError::new("test error"))
        }

        fn returns_datafusion_result() -> datafusion::error::Result<i32> {
            let val = returns_streamling_error()?;
            Ok(val)
        }

        let result = returns_datafusion_result();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, DataFusionError::External(_)),
            "all StreamlingErrors should map to DataFusionError::External, got: {:?}",
            err,
        );
    }

    #[test]
    fn test_auto_conversion_to_datafusion_user() {
        fn returns_user_error() -> Result<i32> {
            Err(StreamlingError::user("bad input"))
        }

        fn returns_datafusion_result() -> datafusion::error::Result<i32> {
            let val = returns_user_error()?;
            Ok(val)
        }

        let result = returns_datafusion_result();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, DataFusionError::External(_)),
            "expected External error, got: {:?}",
            err,
        );
    }

    #[test]
    fn test_datafusion_plan_to_streamling_is_user_facing() {
        let df_err = DataFusionError::Plan("invalid config".to_string());
        let err = StreamlingError::from(df_err);
        assert!(!err.is_internal(), "Plan errors should be user-facing");
        assert!(!err.is_retriable());
    }

    #[test]
    fn test_datafusion_not_implemented_to_streamling_is_user_facing() {
        let df_err = DataFusionError::NotImplemented("unsupported feature".to_string());
        let err = StreamlingError::from(df_err);
        assert!(
            !err.is_internal(),
            "NotImplemented errors should be user-facing"
        );
    }

    #[test]
    fn test_datafusion_configuration_to_streamling_is_user_facing() {
        let df_err = DataFusionError::Configuration("invalid setting".to_string());
        let err = StreamlingError::from(df_err);
        assert!(
            !err.is_internal(),
            "Configuration errors should be user-facing"
        );
    }

    #[test]
    fn test_datafusion_execution_to_streamling_is_internal() {
        let df_err = DataFusionError::Execution("runtime failure".to_string());
        let err = StreamlingError::from(df_err);
        assert!(err.is_internal(), "Execution errors should be internal");
    }

    #[test]
    fn test_user_error_roundtrip_through_datafusion() {
        let original = StreamlingError::user("bad config value");
        let df_err: DataFusionError = original.into();
        let recovered = StreamlingError::from(df_err);
        assert!(
            !recovered.is_internal(),
            "user-facing flag should survive DataFusion round-trip"
        );
        assert_eq!(recovered.to_string(), "bad config value");
    }

    #[test]
    fn test_retriable_error_roundtrip_through_datafusion() {
        let original = StreamlingError::retriable("connection timeout");
        let df_err: DataFusionError = original.into();
        let recovered = StreamlingError::from(df_err);
        assert!(
            recovered.is_retriable(),
            "retriable flag should survive DataFusion round-trip"
        );
        assert_eq!(recovered.to_string(), "connection timeout");
    }

    #[test]
    fn test_internal_error_roundtrip_through_datafusion() {
        let original = StreamlingError::new("internal panic");
        let df_err: DataFusionError = original.into();
        let recovered = StreamlingError::from(df_err);
        assert!(
            recovered.is_internal(),
            "internal flag should survive DataFusion round-trip"
        );
        assert_eq!(
            recovered.to_string(),
            "internal panic",
            "round-tripped message must be exact — no DataFusion boilerplate",
        );
    }

    #[test]
    fn test_context_wrapped_external_roundtrip() {
        let original = StreamlingError::user("bad config");
        let df_err: DataFusionError = original.into();
        let wrapped = DataFusionError::Context("during planning".to_string(), Box::new(df_err));
        let recovered = StreamlingError::from(wrapped);
        assert!(
            !recovered.is_internal(),
            "user-facing flag should survive Context-wrapped External round-trip"
        );
        assert!(
            recovered.to_string().contains("during planning"),
            "Context message should be preserved, got: {}",
            recovered,
        );
        assert!(
            recovered.to_string().contains("bad config"),
            "original message should be preserved, got: {}",
            recovered,
        );
    }

    #[test]
    fn test_context_wrapped_plan_error_is_user_facing() {
        let inner = DataFusionError::Plan("bad config".to_string());
        let wrapped = DataFusionError::Context("during validation".to_string(), Box::new(inner));
        let err = StreamlingError::from(wrapped);
        assert!(
            !err.is_internal(),
            "Plan error inside Context should still be user-facing"
        );
    }

    #[test]
    fn test_nested_context_wrapped_plan_error_is_user_facing() {
        let inner = DataFusionError::Plan("bad config".to_string());
        let ctx1 = DataFusionError::Context("layer 1".to_string(), Box::new(inner));
        let ctx2 = DataFusionError::Context("layer 2".to_string(), Box::new(ctx1));
        let err = StreamlingError::from(ctx2);
        assert!(
            !err.is_internal(),
            "Plan error inside nested Context layers should still be user-facing"
        );
    }

    #[test]
    fn test_context_wrapped_execution_error_is_internal() {
        let inner = DataFusionError::Execution("runtime failure".to_string());
        let wrapped = DataFusionError::Context("during processing".to_string(), Box::new(inner));
        let err = StreamlingError::from(wrapped);
        assert!(
            err.is_internal(),
            "Execution error inside Context should remain internal"
        );
    }

    #[test]
    fn test_anyhow_to_streamling_conversion() {
        fn returns_anyhow() -> anyhow::Result<i32> {
            Err(anyhow!("anyhow error"))
        }

        fn uses_streamling() -> Result<i32> {
            let val = returns_anyhow().into_streamling()?;
            Ok(val)
        }

        let result = uses_streamling();
        assert!(result.is_err());
    }

    #[test]
    fn test_result_ext_preserves_flags() {
        fn inner() -> Result<i32> {
            Err(StreamlingError::retriable("retriable").mark_user_facing())
        }

        fn outer() -> Result<i32> {
            inner().streamling_context("outer context")
        }

        let err = outer().unwrap_err();
        assert!(err.is_retriable());
        assert!(!err.is_internal());
    }

    #[test]
    fn test_macros() {
        let err = streamling_err!("error {}", 42);
        assert!(err.is_internal());
        assert!(!err.is_retriable());

        let err = streamling_user_err!("user error");
        assert!(!err.is_internal());

        let err = streamling_retriable_err!("retry this");
        assert!(err.is_retriable());
    }

    #[test]
    fn test_streamling_bail() {
        fn may_fail(fail: bool) -> Result<()> {
            if fail {
                streamling_bail!("it failed");
            }
            Ok(())
        }

        assert!(may_fail(true).is_err());
        assert!(may_fail(false).is_ok());
    }

    #[test]
    fn test_inner_accessor() {
        let err = StreamlingError::new("test");
        let inner = err.inner();
        assert_eq!(inner.to_string(), "test");
    }

    #[test]
    fn test_strip_backtrace_no_backtrace() {
        let msg = "connection refused\n\nCaused by:\n  0: failed to connect";
        let result = strip_backtrace(msg);
        assert_eq!(result, msg);
    }

    #[test]
    fn test_strip_backtrace_anyhow_format() {
        let msg = "connection refused\n\nCaused by:\n  0: io error\n\nbacktrace:\n   0: fn main()";
        let result = strip_backtrace(msg);
        assert_eq!(result, "connection refused\n\nCaused by:\n  0: io error");
    }

    #[test]
    fn test_strip_backtrace_std_format() {
        let msg = "error message\n\nStack backtrace:\n   0: main";
        let result = strip_backtrace(msg);
        assert_eq!(result, "error message");
    }

    #[test]
    fn test_display_strips_backtrace() {
        let err = StreamlingError::new("test error");
        let display_output = err.to_string();
        assert!(!display_output.contains("backtrace:"));
        assert!(display_output.contains("test error"));
    }

    #[test]
    fn test_debug_includes_backtrace_for_non_retriable() {
        let err = StreamlingError::new("test error");
        let debug_output = format!("{:?}", err);
        assert!(debug_output.contains("test error"));
        assert!(
            debug_output.contains("backtrace:"),
            "non-retriable error Debug should include backtrace, got: {}",
            debug_output
        );
    }

    #[test]
    fn test_debug_no_backtrace_for_retriable() {
        let err = StreamlingError::retriable("transient error");
        let debug_output = format!("{:?}", err);
        assert!(debug_output.contains("transient error"));
        assert!(
            !debug_output.contains("backtrace:"),
            "retriable error Debug should not include backtrace, got: {}",
            debug_output
        );
    }

    #[test]
    fn test_non_retriable_has_backtrace() {
        let err = StreamlingError::new("fatal");
        assert!(err.backtrace().is_some());

        let err = StreamlingError::user("bad input");
        assert!(err.backtrace().is_some());

        let cause = std::io::Error::other("disk full");
        let err = StreamlingError::with_cause("write failed", cause);
        assert!(err.backtrace().is_some());

        let cause = std::io::Error::other("missing");
        let err = StreamlingError::user_with_cause("config error", cause);
        assert!(err.backtrace().is_some());
    }

    #[test]
    fn test_retriable_has_no_backtrace() {
        let err = StreamlingError::retriable("timeout");
        assert!(err.backtrace().is_none());

        let cause = std::io::Error::new(ErrorKind::ConnectionRefused, "refused");
        let err = StreamlingError::retriable_with_cause("connection failed", cause);
        assert!(err.backtrace().is_none());
    }

    #[test]
    fn test_context_preserves_backtrace() {
        let err = StreamlingError::new("original");
        assert!(err.backtrace().is_some());

        let err = err.context("added context");
        assert!(err.backtrace().is_some());
        assert!(err.to_string().contains("added context"));
    }

    #[test]
    fn test_from_io_retriable_no_backtrace() {
        let io_err = std::io::Error::new(ErrorKind::ConnectionRefused, "refused");
        let err = StreamlingError::from(io_err);
        assert!(err.is_retriable());
        assert!(err.backtrace().is_none());
    }

    #[test]
    fn test_from_io_non_retriable_has_backtrace() {
        let io_err = std::io::Error::new(ErrorKind::NotFound, "not found");
        let err = StreamlingError::from(io_err);
        assert!(!err.is_retriable());
        assert!(err.backtrace().is_some());
    }

    #[test]
    fn test_result_ext_preserves_backtrace_from_streamling_error() {
        fn inner() -> Result<i32> {
            Err(StreamlingError::new("original error"))
        }

        fn outer() -> Result<i32> {
            inner().streamling_context("outer context")
        }

        let err = outer().unwrap_err();
        assert!(
            err.backtrace().is_some(),
            "backtrace should be preserved from original StreamlingError"
        );
    }

    #[test]
    fn test_mark_retriable_clears_backtrace() {
        let err = StreamlingError::new("fatal error");
        assert!(err.backtrace().is_some());
        assert!(!err.is_retriable());

        let err = err.mark_retriable();
        assert!(err.is_retriable());
        assert!(
            err.backtrace().is_none(),
            "mark_retriable should clear the backtrace"
        );
    }

    #[test]
    fn test_result_ext_captures_backtrace_for_non_streamling_error() {
        fn inner() -> std::result::Result<i32, std::io::Error> {
            Err(std::io::Error::new(ErrorKind::NotFound, "not found"))
        }

        fn outer() -> Result<i32> {
            inner().streamling_context("outer context")
        }

        let err = outer().unwrap_err();
        assert!(!err.is_retriable());
        assert!(
            err.backtrace().is_some(),
            "backtrace should be captured for non-retriable errors from non-StreamlingError source"
        );
    }

    #[test]
    fn test_no_duplicate_messages_in_datafusion_error_chain() {
        fn returns_streamling_from_df() -> Result<i32> {
            let df_err =
                DataFusionError::Plan("No field named foo. Valid fields are bar, baz.".to_string());
            Err(StreamlingError::from(df_err))
        }

        fn wraps_with_context() -> Result<i32> {
            returns_streamling_from_df()
                .streamling_with_context(|| "transform 'x': failed to parse SQL")
        }

        let err = wraps_with_context().unwrap_err();
        let msg = err.to_string();

        let count = msg.matches("No field named foo").count();
        assert_eq!(
            count, 1,
            "Expected 'No field named foo' to appear exactly once, but found {} times in:\n{}",
            count, msg
        );

        assert!(
            msg.contains("transform 'x': failed to parse SQL"),
            "Should contain the context, got:\n{}",
            msg
        );
        assert!(
            msg.contains("No field named foo. Valid fields are bar, baz."),
            "Should contain the root cause, got:\n{}",
            msg
        );
    }

    #[test]
    fn test_from_datafusion_schema_error_strips_source_chain() {
        use datafusion::common::{Column, SchemaError as DFSchemaError};

        let schema_err = DFSchemaError::FieldNotFound {
            field: Box::new(Column::new_unqualified("number")),
            valid_fields: vec![Column::new_unqualified("block_number")],
        };
        let df_err = DataFusionError::SchemaError(Box::new(schema_err), Box::new(None));

        let streamling_err = StreamlingError::from(df_err);
        let msg = streamling_err.to_string();

        let count = msg.matches("No field named number").count();
        assert_eq!(
            count, 1,
            "Expected 'No field named number' once, got {} times in:\n{}",
            count, msg
        );
        assert!(
            msg.contains("Schema error:"),
            "Should contain the DataFusionError prefix, got:\n{}",
            msg
        );
    }

    #[test]
    fn test_from_datafusion_schema_error_no_duplicates_with_context() {
        use datafusion::common::{Column, SchemaError as DFSchemaError};

        fn returns_schema_error() -> Result<i32> {
            let schema_err = DFSchemaError::FieldNotFound {
                field: Box::new(Column::new_unqualified("number")),
                valid_fields: vec![Column::new_unqualified("block_number")],
            };
            let df_err = DataFusionError::SchemaError(Box::new(schema_err), Box::new(None));
            Err(StreamlingError::from(df_err))
        }

        fn wraps_with_context() -> Result<i32> {
            returns_schema_error()
                .streamling_with_context(|| "kafka source 'blocks': failed to create Kafka source")
        }

        let err = wraps_with_context().unwrap_err();
        let msg = err.to_string();

        let schema_count = msg.matches("Schema error:").count();
        assert_eq!(
            schema_count, 1,
            "Expected 'Schema error:' once (no duplicates), got {} times in:\n{}",
            schema_count, msg
        );

        let field_count = msg.matches("No field named number").count();
        assert_eq!(
            field_count, 1,
            "Expected 'No field named number' once, got {} times in:\n{}",
            field_count, msg
        );

        assert!(
            msg.contains("kafka source 'blocks': failed to create Kafka source"),
            "Should contain context, got:\n{}",
            msg
        );
        assert!(
            msg.contains("Did you mean 'block_number'"),
            "Should contain suggestion, got:\n{}",
            msg
        );
    }

    #[test]
    fn test_no_duplicate_messages_when_chaining_streamling_errors() {
        fn inner() -> Result<i32> {
            Err(StreamlingError::new("root cause message"))
        }

        fn middle() -> Result<i32> {
            inner().streamling_context("middle context")
        }

        fn outer() -> Result<i32> {
            middle().streamling_with_context(|| "outer context")
        }

        let err = outer().unwrap_err();
        let msg = err.to_string();

        let root_count = msg.matches("root cause message").count();
        assert_eq!(
            root_count, 1,
            "Expected 'root cause message' exactly once, but found {} times in:\n{}",
            root_count, msg
        );

        let middle_count = msg.matches("middle context").count();
        assert_eq!(
            middle_count, 1,
            "Expected 'middle context' exactly once, but found {} times in:\n{}",
            middle_count, msg
        );

        assert!(
            msg.contains("outer context"),
            "Should contain outer context, got:\n{}",
            msg
        );
    }

    #[test]
    fn test_datafusion_error_through_streamling_with_context_no_duplicates() {
        use datafusion::common::{Column, SchemaError as DFSchemaError};

        fn returns_df_result() -> std::result::Result<i32, DataFusionError> {
            let schema_err = DFSchemaError::FieldNotFound {
                field: Box::new(Column::new_unqualified("number")),
                valid_fields: vec![Column::new_unqualified("block_number")],
            };
            Err(DataFusionError::SchemaError(
                Box::new(schema_err),
                Box::new(None),
            ))
        }

        let err = returns_df_result()
            .streamling_with_context(|| "kafka source 'blocks': failed to create Kafka source")
            .unwrap_err();
        let msg = err.to_string();

        let schema_count = msg.matches("Schema error: No field named number").count();
        assert_eq!(
            schema_count, 1,
            "Expected 'Schema error: No field named number' once, got {} in:\n{}",
            schema_count, msg
        );

        assert!(
            msg.contains("kafka source 'blocks': failed to create Kafka source"),
            "Should contain context, got:\n{}",
            msg
        );
        assert!(
            msg.contains("Did you mean 'block_number'"),
            "Should contain suggestion, got:\n{}",
            msg
        );
    }

    #[test]
    fn test_non_streamling_error_preserves_source_chain() {
        #[derive(Debug)]
        struct Inner;
        impl fmt::Display for Inner {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "inner cause")
            }
        }
        impl std::error::Error for Inner {}

        #[derive(Debug)]
        struct Outer(Inner);
        impl fmt::Display for Outer {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "outer error")
            }
        }
        impl std::error::Error for Outer {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }

        let err: std::result::Result<i32, Outer> = Err(Outer(Inner));
        let err = err.streamling_context("added context").unwrap_err();
        let msg = err.to_string();

        assert!(
            msg.contains("added context"),
            "Should contain context, got:\n{}",
            msg
        );
        assert!(
            msg.contains("outer error"),
            "Should contain outer error, got:\n{}",
            msg
        );
        assert!(
            msg.contains("inner cause"),
            "Source chain should be preserved for non-duplicate errors, got:\n{}",
            msg
        );
    }

    #[test]
    fn test_has_duplicate_source_detects_duplication() {
        use datafusion::common::{Column, SchemaError as DFSchemaError};

        let schema_err = DFSchemaError::FieldNotFound {
            field: Box::new(Column::new_unqualified("x")),
            valid_fields: vec![Column::new_unqualified("y")],
        };
        let df_err = DataFusionError::SchemaError(Box::new(schema_err), Box::new(None));
        assert!(
            has_duplicate_source(&df_err),
            "DataFusion SchemaError should be detected as having duplicate source"
        );
    }

    #[test]
    fn test_arrow_external_error_no_duplicate_messages() {
        use arrow_schema::ArrowError;

        let inner = std::io::Error::other("connection refused");
        let arrow_err = ArrowError::ExternalError(Box::new(inner));

        let streamling_err = StreamlingError::from(arrow_err);
        let msg = streamling_err.to_string();

        let count = msg.matches("connection refused").count();
        assert_eq!(
            count, 1,
            "Expected 'connection refused' exactly once, but found {} times in:\n{}",
            count, msg
        );
    }

    #[test]
    fn test_node_tag_first_wins_and_survives_datafusion_roundtrip() {
        let e = StreamlingError::user("boom")
            .with_node("script_1")
            .with_node("sink_1");
        assert_eq!(e.node(), Some("script_1"), "first tag must win");

        let df: DataFusionError = e.into();
        let recovered = StreamlingError::from(df);
        assert_eq!(
            recovered.node(),
            Some("script_1"),
            "node tag must survive the DataFusionError round-trip"
        );
        assert!(!recovered.is_internal());

        // Context helpers must preserve the tag too.
        let with_ctx: Result<()> = Err(recovered).streamling_context("while sinking");
        assert_eq!(with_ctx.unwrap_err().node(), Some("script_1"));
    }
}
