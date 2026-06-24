//! Regression guard for the vendored mimalloc static override.
//!
//! `crates/streamling/build.rs` links mimalloc's single-translation-unit override
//! directly into every binary this crate produces (the `streamling` binary, and,
//! because `rustc-link-arg` applies to all targets, these integration tests too).
//! The invariant that makes plugin `cdylib`s safe is that the process's `malloc`
//! *is* mimalloc's `mi_malloc` (a single exported symbol), so a `dlopen`'d plugin
//! resolves its own `malloc` to the same heap as the host.
//!
//! If this test fails, the override is no longer active: the host and plugins
//! would use different allocators, and freeing a plugin-allocated object in the
//! host would corrupt the heap.

#![cfg(target_os = "linux")]

use libc::{RTLD_DEFAULT, c_void};
use std::ffi::CString;

#[test]
fn malloc_is_mimalloc() {
    let malloc = dlsym("malloc");
    let mi_malloc = dlsym("mi_malloc");

    assert!(
        mi_malloc.is_some(),
        "mi_malloc symbol not found: the mimalloc override is not linked into this binary"
    );
    assert_eq!(
        malloc, mi_malloc,
        "the process `malloc` does not resolve to mimalloc's `mi_malloc`; \
         the static override is not active, so dlopen'd plugins would NOT share \
         the host heap (cross-boundary free corruption risk)"
    );
}

fn dlsym(name: &str) -> Option<*mut c_void> {
    let c = CString::new(name).unwrap();
    // SAFETY: `name` is a well-known libc/mimalloc symbol; RTLD_DEFAULT searches
    // the global symbol scope. Null return just means the symbol is absent.
    unsafe {
        let p = libc::dlsym(RTLD_DEFAULT, c.as_ptr());
        (!p.is_null()).then_some(p)
    }
}
