// Runtime for user-provided JS/TS script transforms. Reads Arrow IPC input
// bytes from the host, runs the user's `invoke(row)` function once per input
// row, and writes the result back to the host.
//
// Two output paths, chosen by whether the "schema" config key is present:
//
// - Schema-aware (config key "schema" set): the host already knows every
//   output field's name and type (a JSON list of `{name, type}`, sent by the
//   operator when the pipeline declares `schema:`). This path always collects
//   the same row-object `results` array the inferred path builds, then tries
//   a JSON fast path: fill one plain array per declared column while checking
//   that every column was returned by every row and every value's JS kind
//   matches its declared type. When that holds, the columns serialize
//   straight to a JSON string (`{"columns": {name: [values...]}, "num_rows":
//   N}`) and `Host.outputString` writes it -- no Arrow table is built on the
//   output side. Whenever a row omits a declared column, returns a value of
//   the wrong kind, declares a non-scalar type, or the JSON would contain a
//   lone surrogate, the fast path is skipped and the same `results` array
//   runs through the inferred path's Arrow-table + IPC encoding instead, so
//   the output byte-for-byte matches what running without a schema would
//   produce for that batch.
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

// JSON.stringify escapes an unpaired UTF-16 surrogate (half of a 4-byte
// character truncated or otherwise produced without its partner) as a
// `\udXXX`-style escape in its output text, even though it leaves valid
// surrogate *pairs* unescaped. serde_json, which decodes the fast path's
// JSON string on the Rust side, rejects that escape -- a lone surrogate
// doesn't correspond to any Unicode scalar value. A batch whose fast-path
// JSON contains one falls back to the inferred path instead.
const LONE_SURROGATE_RE = /\\ud[89a-f][0-9a-f]{2}/i;

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
      const { results, fastPathJson } = runSchemaAware(
        fn,
        inputTable,
        numRows,
        outputSchema
      );
      if (fastPathJson !== null) {
        return Host.outputString(fastPathJson);
      }
      return outputArrowTable(resultsToArrowTable(results));
    }

    return outputArrowTable(runInferred(fn, inputTable, numRows));
  } catch (error) {
    throw new Error(
      `script runtime error: ${error.message}${
        error.stack ? "\n" + error.stack : ""
      }`
    );
  }
}

// Encodes an Arrow table to IPC file-format bytes and writes it to the host.
// Shared by the inferred path and by the schema-aware path's fallback (when
// the JSON fast path isn't safe for a batch).
function outputArrowTable(outputTable) {
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
}

// Runs the user function over every input row and returns the resulting
// row-object array: returning null filters a row out, returning an array
// expands one input row into many output rows, and `_gs_op` is always
// copied over from the input row when the input has it, even if the user's
// function doesn't include it in its returned row.
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

  return results;
}

// Inferred path: run the user function over every input row, collect the
// returned rows as plain objects, then build an Arrow table whose columns
// are the union of every returned row's keys.
function runInferred(fn, inputTable, numRows) {
  return resultsToArrowTable(collectResults(fn, inputTable, numRows));
}

// Builds an Arrow table from a row-object array: the columns are the union
// of every row's keys, with a missing key or an explicit null both reading
// back as null. An empty `results` array produces a minimal one-column
// table instead of a zero-column one.
function resultsToArrowTable(results) {
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

// Whether a declared output type has a direct JSON fast-path builder on the
// Rust side. Any other declared type (struct, list, etc.) forces the whole
// batch through the inferred path, where the value round-trips as Arrow
// through flechette's own type inference instead.
function isFastPathType(type) {
  return (
    type === "string" ||
    type === "int64" ||
    type === "float64" ||
    type === "boolean"
  );
}

// Whether `value` is safe to serialize into a `type`-declared JSON column:
// null/undefined always is (the Rust side nulls it, or defaults it for
// `_gs_op`), otherwise the JS value's kind must match the declared type
// exactly -- no coercion, since the JSON path never casts.
function valueMatchesType(value, type) {
  if (value === null || value === undefined) return true;
  switch (type) {
    case "string":
      return typeof value === "string";
    case "int64":
      return Number.isSafeInteger(value);
    case "float64":
      return typeof value === "number" && Number.isFinite(value);
    case "boolean":
      return typeof value === "boolean";
    default:
      return false;
  }
}

// Schema-aware path: collects the same row-object `results` array the
// inferred path builds, then attempts the JSON fast path -- filling one
// plain array per declared output column while tracking, per column,
// whether every row returned it (`seen`) and whether every returned value's
// kind matches its declared type (`kindOk`). The fast path is only safe when
// every declared type has a direct JSON builder, every non-`_gs_op` column
// was seen, and every value's kind matched; `_gs_op` itself only needs to be
// a string when present (its declared type is always "string", so the
// generic kind check already covers this). An empty `results` array, and any
// batch where the fast path isn't safe, returns with `fastPathJson: null` so
// the caller runs the existing inferred-path Arrow/IPC encoding on the same
// `results` array instead.
function runSchemaAware(fn, inputTable, numRows, outputSchema) {
  const results = collectResults(fn, inputTable, numRows);

  if (results.length === 0) {
    return { results, fastPathJson: null };
  }

  const fastPathAllowed = outputSchema.every((f) => isFastPathType(f.type));
  if (!fastPathAllowed) {
    return { results, fastPathJson: null };
  }

  const columnNames = outputSchema.map((f) => f.name);
  const typeByName = new Map(outputSchema.map((f) => [f.name, f.type]));
  const columns = {};
  const seen = new Map(columnNames.map((name) => [name, false]));
  let allKindOk = true;

  for (const name of columnNames) {
    columns[name] = new Array(results.length);
  }

  for (let i = 0; i < results.length; i++) {
    const row = results[i];
    for (const name of columnNames) {
      const has = row && typeof row === "object" && name in row;
      if (has) seen.set(name, true);
      const value = has ? row[name] : undefined;
      const normalized = value === undefined ? null : value;
      columns[name][i] = normalized;
      if (allKindOk && !valueMatchesType(normalized, typeByName.get(name))) {
        allKindOk = false;
      }
    }
  }

  const allSeen = columnNames.every(
    (name) => name === "_gs_op" || seen.get(name)
  );

  if (!allSeen || !allKindOk) {
    return { results, fastPathJson: null };
  }

  const jsonString = JSON.stringify({ columns, num_rows: results.length });
  if (LONE_SURROGATE_RE.test(jsonString)) {
    return { results, fastPathJson: null };
  }

  return { results, fastPathJson: jsonString };
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
