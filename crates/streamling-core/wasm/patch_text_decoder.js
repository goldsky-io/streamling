// Must be imported BEFORE "@uwdata/flechette" so this patch is installed
// before flechette's util/strings.js runs `new TextDecoder('utf-8')` at
// module load time (ES module evaluation order: the first `import` statement
// in a file's dependency graph runs first).
//
// Root cause (see /tmp/sbench/p2_report.md STEP 0): flechette decodes every
// UTF-8 string via `textDecoder.decode(buf.subarray(offset, offset + length))`.
// `subarray` returns a view sharing the WHOLE input Arrow IPC buffer's backing
// ArrayBuffer, not a copy. Measured inside this same QuickJS (extism js-pdk)
// sandbox: decoding a 10-byte view into a 10MB backing buffer 1000 times took
// ~300ms, vs ~2ms for 1000 decodes of a standalone 10-byte buffer -- roughly
// 150x slower. TextDecoder.decode() here scales with the backing buffer's
// size, not the view's own byte length. With one decode per string field per
// row, that turns per-batch cost into O(rows * batch_bytes).
//
// Fix: copy just the view's own bytes into a fresh (small) ArrayBuffer before
// decoding, whenever the view is smaller than its backing buffer. Copying a
// few bytes is cheap; decoding is what's expensive on a large backing buffer.
const originalDecode = globalThis.TextDecoder.prototype.decode;
globalThis.TextDecoder.prototype.decode = function fastDecode(view, options) {
  if (
    view &&
    typeof view.byteLength === "number" &&
    view.buffer &&
    view.byteLength < view.buffer.byteLength
  ) {
    view = view.slice();
  }
  return originalDecode.call(this, view, options);
};
