// Bake a process-wide mimalloc override into this binary so that host and
// dlopen'd plugin cdylibs share one allocator heap.
//
// We depend on `libmimalloc-sys` solely to fetch the upstream mimalloc v3
// sources; it exposes its include directory to us via `DEP_MIMALLOC_INCLUDE_DIR`
// (it has `links = "mimalloc"` and emits `cargo:INCLUDE_DIR`). From there we
// locate `src/static.c` — the single-translation-unit amalgamation — compile it
// ourselves with `-DMI_MALLOC_OVERRIDE`, and link the resulting object *directly*
// (not via a static archive). Linking the bare object guarantees unconditional
// inclusion across every target (binary and integration tests alike): a static
// archive would be lazily dropped because the binary references `malloc` as a
// dynamic symbol (resolved against libc at runtime), leaving no undefined
// `malloc` for the archive to satisfy.
//
// The binary thus *defines* `malloc`/`free`/`calloc`/`realloc`/`posix_memalign`
// as strong symbols; `--export-dynamic` (`-Wl,-E`) puts them in the dynamic
// symbol table, so every `dlopen`'d plugin `cdylib` resolves its `malloc`/`free`
// to the *same* mimalloc heap as the host. One process-wide heap => the
// `abi_stable` `RVec`/`RString` cross-boundary alloc/free is safe.
//
// This deliberately does NOT use `#[global_allocator]`: that would give the
// host a *private* mimalloc heap while plugins keep libc `malloc`, so the host
// dropping a plugin-allocated object would be a cross-heap free. Symbol
// interposition is the only mechanism that yields a single shared heap.
fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let opt_out = std::env::var_os("STREAMLING_NO_MIMALLOC").is_some();

    // The override is glibc/Linux specific (a symbol-interposition mechanism).
    // On other targets, and on explicit opt-out, fall back to the platform
    // default allocator — the safe status quo where host and plugins both use
    // libc `malloc`.
    if target_os != "linux" || opt_out {
        return;
    }

    // libmimalloc-sys exposes the mimalloc v3 include dir via `links = "mimalloc"`.
    let include_dir = std::env::var("DEP_MIMALLOC_INCLUDE_DIR").unwrap_or_else(|_| {
        panic!(
            "DEP_MIMALLOC_INCLUDE_DIR is not set; is `libmimalloc-sys` a dependency of this \
             crate? It must build before this build script so it can propagate its include dir."
        )
    });
    // static.c is the sibling `src` directory next to the exposed `include` dir.
    let static_c = std::path::Path::new(&include_dir)
        .parent() // .../c_src/mimalloc/v3
        .expect("include dir has a parent")
        .join("src")
        .join("static.c");

    // `compile_intermediates` returns the bare object(s) without bundling them
    // into an archive or emitting link directives. We link the object directly
    // so it is included unconditionally (not subject to lazy `.a` resolution).
    let objects = cc::Build::new()
        .file(&static_c)
        .include(&include_dir)
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

    println!("cargo:rerun-if-env-changed=STREAMLING_NO_MIMALLOC");
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARGET_OS");
}
