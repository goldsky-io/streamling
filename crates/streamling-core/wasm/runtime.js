// Runtime for user-provided JS/TS script transforms. Reads Arrow IPC input
// bytes from the host, runs the user's `invoke(row)` function once per input
// row, and writes the result back to the host.
//
// Two output paths, chosen by whether the "schema" config key is present:
//
// - Schema-aware (config key "schema" set): the host already knows every
//   output field's name (a JSON list of `{name, type}`, sent by the operator
//   when the pipeline declares `schema:`). This path skips building an Arrow
//   table on the output side entirely: it pushes each returned row's values
//   straight into one plain array per output column, then serializes those
//   columns to a JSON string (`{"columns": {name: [values...]}, "num_rows": N}`)
//   and hands it to the host with `Host.outputString`. `Host.outputString`
//   encodes the string to UTF-8 natively on the host side, so this path never
//   calls `TextEncoder` in JS.
// - Inferred (no "schema" config key): the output schema isn't known ahead of
//   time, so this path collects each returned row as an object, unions their
//   keys, builds an Arrow table from the resulting columns, and encodes it to
//   Arrow IPC file-format bytes via `Host.outputBytes`.
//
// `patch_text_decoder.js` is imported first so its `TextDecoder.prototype.decode`
// patch is installed before flechette's own module-scope decoder is created
// (flechette builds a `TextDecoder` at module load, in util/strings.js).
import "./patch_text_decoder.js";
import { tableFromIPC, tableFromArrays, tableToIPC } from "@uwdata/flechette";

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

// Config.get("schema") never changes across invoke() calls within one plugin
// instance, but re-parsing its JSON on every call is wasted work. Cache the
// parsed value keyed by the raw string.
const parsedSchemaCache = new Map();
function getParsedSchema(raw) {
  if (!raw) return null;
  let parsed = parsedSchemaCache.get(raw);
  if (!parsed) {
    parsed = JSON.parse(raw);
    parsedSchemaCache.set(raw, parsed);
  }
  return parsed;
}

function invoke() {
  try {
    const code = Config.get("code");
    const fn = getCompiledFn(code);

    const schemaConfigRaw = Config.get("schema");
    const outputSchema = getParsedSchema(schemaConfigRaw);

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

    if (outputSchema && outputSchema.length > 0) {
      const jsonString = runSchemaAware(fn, inputTable, numRows, outputSchema);
      return Host.outputString(jsonString);
    }

    const outputTable = runInferred(fn, inputTable, numRows);

    let outputBytes;
    try {
      // File format matches what the Rust side's FileReader expects.
      outputBytes = tableToIPC(outputTable, { format: "file" });
    } catch (error) {
      throw new Error(
        `Failed to encode Arrow IPC output: ${error.message}${
          error.stack ? "\n" + error.stack : ""
        }`
      );
    }

    return Host.outputBytes(outputBytes.buffer);
  } catch (error) {
    throw new Error(
      `script runtime error: ${error.message}${
        error.stack ? "\n" + error.stack : ""
      }`
    );
  }
}

// Inferred path: run the user function over every input row, collect the
// returned rows as plain objects, then build an Arrow table whose columns
// are the union of every returned row's keys.
function runInferred(fn, inputTable, numRows) {
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

      // Returning null filters the row out of the batch.
      if (result === null) {
        continue;
      }

      // Returning an array expands one input row into many output rows.
      if (Array.isArray(result)) {
        for (let j = 0; j < result.length; j++) {
          const row = result[j];

          // null entries in the array filter out that specific row.
          if (row === null) {
            continue;
          }

          if (typeof row !== "object") {
            throw new Error(
              `Script must return an object, null, or array of objects. Array element at index ${j} is ${typeof row}`
            );
          }

          // Always preserve _gs_op from the input, even if the user's
          // function doesn't include it in its returned row.
          if ("_gs_op" in inputObj) {
            row._gs_op = inputObj._gs_op;
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
      }

      results.push(result);
    } catch (error) {
      throw new Error(formatError(error, inputObj, i + 1));
    }
  }

  let outputTable;
  try {
    if (results.length === 0) {
      // Minimal table with one dummy column, for an all-filtered-out batch.
      outputTable = tableFromArrays({ _dummy: [] });
    } else {
      const allKeys = new Set();
      for (const result of results) {
        if (result && typeof result === "object") {
          for (const key of Object.keys(result)) {
            allKeys.add(key);
          }
        }
      }

      const columns = {};
      for (const key of allKeys) {
        columns[key] = results.map((row) => {
          if (row && typeof row === "object" && key in row) {
            return row[key] ?? null;
          }
          return null;
        });
      }

      try {
        outputTable = tableFromArrays(columns);
      } catch (error) {
        throw new Error(
          `Failed to create Arrow table from arrays: ${error.message}${
            error.stack ? "\n" + error.stack : ""
          }`
        );
      }
    }
  } catch (error) {
    throw new Error(
      `Failed to create Arrow table from results: ${error.message}${
        error.stack ? "\n" + error.stack : ""
      }`
    );
  }
  return outputTable;
}

// Schema-aware path: the caller already knows every output column's name
// (outputSchema), so each returned row's values are pushed straight into one
// plain array per column -- no row-object array, no key-union pass. The
// per-column arrays are serialized to one JSON string, which the caller
// writes to the host directly (no Arrow table is built on the output side).
function runSchemaAware(fn, inputTable, numRows, outputSchema) {
  const columnNames = outputSchema.map((f) => f.name);
  const columnArrays = new Map(columnNames.map((name) => [name, []]));
  let rowCount = 0;

  const pushRow = (row, inputObj) => {
    if ("_gs_op" in inputObj) row._gs_op = inputObj._gs_op;
    for (const name of columnNames) {
      const v = row[name];
      columnArrays.get(name).push(v === undefined ? null : v);
    }
    rowCount++;
  };

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

      if (result === null) continue;

      if (Array.isArray(result)) {
        for (let j = 0; j < result.length; j++) {
          const row = result[j];
          if (row === null) continue;
          if (typeof row !== "object") {
            throw new Error(
              `Script must return an object, null, or array of objects. Array element at index ${j} is ${typeof row}`
            );
          }
          pushRow(row, inputObj);
        }
        continue;
      }

      if (typeof result !== "object") {
        throw new Error(
          `Script must return an object, null, or array of objects, got ${typeof result}`
        );
      }

      pushRow(result, inputObj);
    } catch (error) {
      throw new Error(formatError(error, inputObj, i + 1));
    }
  }

  const columns = {};
  for (const name of columnNames) {
    columns[name] = columnArrays.get(name);
  }
  return JSON.stringify({ columns, num_rows: rowCount });
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
