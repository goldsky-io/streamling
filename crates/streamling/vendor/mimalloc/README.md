# mimalloc v3.3.2 (vendored)

Upstream: https://github.com/microsoft/mimalloc (`v3.3.2` tag, commit `30b2d9d`).
License: MIT — see `LICENSE` in this directory.

## Why it's here

Streamling dynamically loads plugin `cdylib`s over an `abi_stable` FFI boundary
(`crates/streamling-core/src/plugin.rs`). Memory crosses that boundary through
`abi_stable` `RVec`/`RString`/`RBox`/`RArc` and the Arrow C Data Interface, and
plugin-allocated objects are dropped by the host. That is only safe when the
host and every plugin share a single allocator heap.

We therefore bake mimalloc's **static override** into the executable
(`crates/streamling/build.rs`): the single-translation-unit file `src/static.c`,
compiled with `-DMI_MALLOC_OVERRIDE`, is linked directly into the binary so it
defines `malloc`/`free`/`calloc`/`realloc`/`posix_memalign` as exported symbols.
With `--export-dynamic` those land in the dynamic symbol table, so `dlopen`'d
plugins resolve their `malloc` to the same mimalloc heap as the host.

This is deliberately **not** `#[global_allocator]`. That would give the host a
*private* mimalloc heap while plugins keep libc `malloc`, making cross-boundary
frees corrupt the heap. Only process-wide symbol interposition yields one shared
heap.

## Gating

The override is built only for `target_os = "linux"` (it is a glibc symbol
interposition mechanism). Set `STREAMLING_NO_MIMALLOC=1` to opt out and fall
back to the platform default allocator.

## Updating

Replace the contents of `include/` and `src/` here with a newer upstream release
and update the version in this file. Only `src/static.c` is compiled, but it
`#include`s the rest of `src/`, so keep the full `src/` and `include/` trees in
sync.
