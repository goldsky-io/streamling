use std::path::PathBuf;

// Path B (production-safe) mimalloc integration.
//
// We statically link mimalloc v3's single-translation-unit override
// (`src/static.c`, compiled with `-DMI_MALLOC_OVERRIDE`) directly into the
// executable so the binary *defines* `malloc`/`free`/`calloc`/`realloc`/
// `posix_memalign` as strong, exported symbols. With `--export-dynamic`
// (`-Wl,-E`) those land in the dynamic symbol table, so every `dlopen`'d
// plugin `cdylib` resolves its `malloc`/`free` to the *same* mimalloc heap as
// the host. One process-wide heap => cross-boundary alloc/free (the
// `abi_stable` `RVec`/`RString` ownership transfer) is safe.
//
// This deliberately does NOT use `#[global_allocator]`: that would give the
// host a *private* mimalloc heap while plugins keep libc `malloc`, so the host
// dropping a plugin-allocated object would be a cross-heap free. Symbol
// interposition is the only mechanism that yields a single shared heap.
fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let opt_out = std::env::var_os("STREAMLING_NO_MIMALLOC").is_some();

    // The static-override mechanism is glibc/Linux specific. On other targets
    // (and on explicit opt-out) we fall back to the platform default allocator,
    // which is the safe status quo (host and plugins both use libc `malloc`).
    if target_os != "linux" || opt_out {
        return;
    }

    let manifest_dir = PathBuf::from(
        std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set by Cargo"),
    );
    let vendor = manifest_dir.join("vendor/mimalloc");
    let static_c = vendor.join("src/static.c");
    let include = vendor.join("include");

    // `compile_intermediates` returns the bare object(s) without bundling them
    // into an archive or emitting link directives. We link the object directly
    // so it is included unconditionally (not subject to lazy `.a` resolution).
    let objects = cc::Build::new()
        .file(&static_c)
        .include(&include)
        .pic(true)
        // Emit the standard `malloc`/`free`/... override symbols.
        .define("MI_MALLOC_OVERRIDE", None)
        // Match mimalloc's release build: disable its internal asserts.
        .define("MI_BUILD_RELEASE", None)
        .define("NDEBUG", None)
        // Prevent the compiler from folding allocation calls into builtins that
        // would bypass the override.
        .flag("-fno-builtin-malloc")
        // Optimal TLS model for an allocator statically linked into the exe.
        .flag("-ftls-model=initial-exec")
        .compile_intermediates();

    for obj in &objects {
        println!("cargo:rustc-link-arg={}", obj.display());
    }

    // mimalloc runtime dependencies.
    println!("cargo:rustc-link-lib=pthread");
    println!("cargo:rustc-link-lib=rt");
    println!("cargo:rustc-link-lib=atomic");
    // Put the override symbols in the dynamic symbol table so plugins bind to them.
    println!("cargo:rustc-link-arg=-Wl,-E");

    println!("cargo:rerun-if-changed={}", static_c.display());
    println!("cargo:rerun-if-changed={}", include.display());
    println!("cargo:rerun-if-env-changed=STREAMLING_NO_MIMALLOC");
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARGET_OS");
}
