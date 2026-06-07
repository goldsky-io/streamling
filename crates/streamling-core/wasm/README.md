# JavaScript Runtime

This runtime uses Extism's [js-pdk](https://github.com/extism/js-pdk) and [flechette](https://idl.uw.edu/flechette/) for Arrow IPC support. Current implementation relies on the
JavaScript's `eval` function. This approach was mentioned
[here](https://extism.org/blog/sandboxing-llm-generated-code/).

## Building

The runtime uses Arrow IPC as the transport layer. To build:

1. Install dependencies and bundle flechette:

```bash
npm install
npm run bundle
```

2. Compile bundled JavaScript to WASM:

```bash
npm run compile
```

Or run both steps at once:

```bash
npm run build
```

The build process:

- Bundles `runtime.js` with flechette using esbuild
- Compiles the bundled JavaScript to `runtime.wasm` using extism-js
