//! Resolves the preview run duration from an optional request parameter.

/// Default preview duration when the caller omits `duration_seconds`.
pub const DEFAULT_PREVIEW_SECS: u64 = 180;
/// Hard upper bound on preview duration; larger requests are clamped down.
pub const MAX_PREVIEW_SECS: u64 = 600;

/// Resolves the effective preview duration in seconds. `None` or `0` yields the
/// default; anything above [`MAX_PREVIEW_SECS`] is clamped down.
pub fn resolve_duration_secs(requested: Option<u64>) -> u64 {
    match requested {
        None | Some(0) => DEFAULT_PREVIEW_SECS,
        Some(n) => n.min(MAX_PREVIEW_SECS),
    }
}

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
