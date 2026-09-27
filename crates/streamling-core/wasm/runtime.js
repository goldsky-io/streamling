// Runtime for user-provided JS/TS script transforms. Reads Arrow IPC input
// bytes from the host, runs the user's `invoke(row)` function once per input
// row, and writes the result back to the host.
//
// Runs the user function over every input row, collects the returned row
// objects, and writes them as newline-delimited JSON (one JSON object per
// output row) via `Host.outputString`. The Rust side knows the output schema
// (declared `schema:` or the input schema) and decodes this JSON directly
// against it with arrow_json.
//
// `patch_text_decoder.js` is imported first so its `TextDecoder.prototype.decode`
// patch is installed before flechette's own module-scope decoder is created
// (flechette builds a `TextDecoder` at module load, in util/strings.js).
import "./patch_text_decoder.js";
import { tableFromIPC } from "@uwdata/flechette";

// JSON.stringify throws on BigInt; writing it as a string lets the Rust
// decoder parse it into int64/U256 columns.
BigInt.prototype.toJSON = function () {
  return this.toString();
};

// `eval(code)` compiles the user's script into a callable function. The
// compiled function is cached per plugin instance (keyed by the raw code
// string) so a script is only compiled once, not once per invoke() call.
const compiledFnCache = new Map();
function getCompiledFn(code) {
  let fn = compiledFnCache.get(code);
  if (!fn) {
    fn = eval("(" + code + ")");
    compiledFnCache.set(code, fn);
  }
  return fn;
}

function invoke() {
  try {
    const code = Config.get("code");
    const fn = getCompiledFn(code);

    const inputBytes = Host.inputBytes();
    // flechette expects a Uint8Array, not a raw ArrayBuffer.
    const inputUint8Array =
      inputBytes instanceof Uint8Array
        ? inputBytes
        : new Uint8Array(inputBytes);

    let inputTable;
    try {
      inputTable = tableFromIPC(inputUint8Array, { useProxy: true });
    } catch (error) {
      throw new Error(
        `Failed to decode Arrow IPC input: ${error.message}${
          error.stack ? "\n" + error.stack : ""
        }`
      );
    }

    const numRows = inputTable.numRows;
    const results = collectResults(fn, inputTable, numRows);

    // `results.map(...).join("\n")` builds the whole string in one pass;
    // repeated `+=` concatenation would copy the growing string on every
    // row. arrow_json doesn't need a trailing newline after the last row.
    const out = fixLoneSurrogates(results.map((row) => JSON.stringify(row)).join("\n"));

    return Host.outputString(out);
  } catch (error) {
    throw new Error(
      `script runtime error: ${error.message}${
        error.stack ? "\n" + error.stack : ""
      }`
    );
  }
}

// Runs the user function over every input row and returns the resulting
// row-object array: returning null filters a row out, returning an array
// expands one input row into many output rows. `_gs_op` is always copied
// over from the input row when the input has it, even if the user's
// function doesn't include it in its returned row; otherwise, when the
// returned row doesn't set `_gs_op` (or sets it to null/undefined), it
// defaults to "i" (insert).
function collectResults(fn, inputTable, numRows) {
  const results = [];

  for (let i = 0; i < numRows; i++) {
    let inputObj;
    try {
      inputObj = inputTable.get(i);
    } catch (error) {
      throw new Error(
        `Failed to get row ${i} from table: ${error.message}${
          error.stack ? "\n" + error.stack : ""
        }`
      );
    }
    try {
      const result = fn(inputObj);

      if (result === null) {
        continue;
      }

      if (Array.isArray(result)) {
        for (let j = 0; j < result.length; j++) {
          const row = result[j];

          if (row === null) {
            continue;
          }

          if (typeof row !== "object") {
            throw new Error(
              `Script must return an object, null, or array of objects. Array element at index ${j} is ${typeof row}`
            );
          }

          if ("_gs_op" in inputObj) {
            row._gs_op = inputObj._gs_op;
          } else if (row._gs_op === null || row._gs_op === undefined) {
            row._gs_op = "i";
          }

          results.push(row);
        }
        continue;
      }

      if (typeof result !== "object") {
        throw new Error(
          `Script must return an object, null, or array of objects, got ${typeof result}`
        );
      }

      if ("_gs_op" in inputObj) {
        result._gs_op = inputObj._gs_op;
      } else if (result._gs_op === null || result._gs_op === undefined) {
        result._gs_op = "i";
      }

      results.push(result);
    } catch (error) {
      throw new Error(formatError(error, inputObj, i + 1));
    }
  }

  return results;
}

// JSON.stringify writes an unpaired UTF-16 surrogate (half of a 4-byte
// character truncated or otherwise produced without its partner) as a
// lowercase `\udXXX`-style escape, and a valid surrogate pair as raw
// characters. The Rust JSON decoder rejects a lone surrogate escape, so
// replace each one with the U+FFFD replacement character escape -- the same
// result `TextEncoder` gives when it encodes a lone surrogate. A user string
// containing a literal backslash is written as `\\`, so this only matches an
// escape preceded by an even number of backslashes (an odd count means the
// backslash belongs to the user's text, not the start of a real escape).
//
// The regex scan below walks the whole output string, which is expensive in
// QuickJS on a large batch. A lone surrogate is rare, so a plain substring
// check skips the regex entirely in the common case where there's nothing
// to fix.
function fixLoneSurrogates(json) {
  if (json.indexOf("\\ud") === -1) {
    return json;
  }
  return json.replace(/(?<!\\)((?:\\\\)*)\\ud[89a-f][0-9a-f]{2}/g, "$1\\ufffd");
}

function truncateString(str, maxLength) {
  if (str.length > maxLength) {
    return `${str.substring(0, maxLength)}... (truncated, total length: ${
      str.length
    })`;
  }
  return str;
}

function formatError(error, input, lineNumber) {
  const MAX_INPUT_LENGTH = 1000;
  const errorType = error.constructor.name;
  let commonIssues = [];

  if (errorType === "TypeError") {
    commonIssues = [
      "Check if you're accessing properties that exist in the input",
      "Verify you're not trying to call methods on undefined values",
      "Ensure all required properties are present in the input",
    ];
  } else if (errorType === "SyntaxError") {
    commonIssues = [
      "Check for syntax errors in your script",
      "Verify all brackets and parentheses are properly closed",
      "Ensure all statements end with semicolons",
    ];
  } else {
    commonIssues = [
      "Check if your script returns a valid JSON object, null, or array of objects",
      "Verify all required properties are present",
      "Ensure no circular references in the returned object",
      "Check for undefined or null values in required fields",
      "Nested structures (objects, arrays) are supported in return values",
      "Return an array to expand one input row into multiple output rows",
    ];
  }

  let formattedError = `Error: ${error.message}\n\n`;
  formattedError += `Line: ${lineNumber}\n\n`;

  if (error.stack) {
    formattedError += `Stack trace:\n${error.stack}\n\n`;
  }

  let inputDataDisplay;
  try {
    const inputJson = JSON.stringify(input, null, 2);
    inputDataDisplay = truncateString(inputJson, MAX_INPUT_LENGTH);
  } catch (e) {
    inputDataDisplay = truncateString(String(input), MAX_INPUT_LENGTH);
  }

  formattedError += `Input data:\n${inputDataDisplay}\n\n`;

  formattedError += "Common issues:\n";
  commonIssues.forEach((issue) => {
    formattedError += `- ${issue}\n`;
  });

  return formattedError;
}

module.exports = { invoke };
