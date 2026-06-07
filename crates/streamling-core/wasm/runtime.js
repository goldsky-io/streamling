import { tableFromIPC, tableToIPC, tableFromArrays } from "@uwdata/flechette";

// Custom base64 encoding function (btoa equivalent)
function btoa(str) {
  const chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  let result = "";
  let i = 0;
  while (i < str.length) {
    const a = str.charCodeAt(i++);
    const hasB = i < str.length;
    const b = hasB ? str.charCodeAt(i++) : 0;
    const hasC = i < str.length;
    const c = hasC ? str.charCodeAt(i++) : 0;
    const bitmap = (a << 16) | (b << 8) | c;
    result += chars.charAt((bitmap >> 18) & 63);
    result += chars.charAt((bitmap >> 12) & 63);
    if (hasB) {
      result += chars.charAt((bitmap >> 6) & 63);
    } else {
      result += "=";
    }
    if (hasC) {
      result += chars.charAt(bitmap & 63);
    } else {
      result += "=";
    }
  }
  return result;
}

function invoke() {
  try {
    const code = Config.get("code");
    // Compile the function once outside the loop for better performance
    const fn = eval("(" + code + ")");

    // Read Arrow IPC bytes from host
    const inputBytes = Host.inputBytes();

    // Ensure we have a Uint8Array (flechette expects Uint8Array, not ArrayBuffer)
    const inputUint8Array =
      inputBytes instanceof Uint8Array
        ? inputBytes
        : new Uint8Array(inputBytes);

    // Decode Arrow IPC to table with proxy support for efficient iteration
    // Proxy allows for 'zero-copy' iteration over the table
    // but prevents things like Object.keys() from being called.
    // In testing this is faster and uses less memory.
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
    const results = [];

    // Process each row using table iterator with .get()
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

        // Allow null to filter out rows from the batch
        if (result === null) {
          continue;
        }

        // Support returning an array to expand one row into many rows
        if (Array.isArray(result)) {
          for (let j = 0; j < result.length; j++) {
            const row = result[j];

            // Allow null in array to filter out specific rows
            if (row === null) {
              continue;
            }

            if (typeof row !== "object") {
              throw new Error(
                `Script must return an object, null, or array of objects. Array element at index ${j} is ${typeof row}`
              );
            }

            // Always preserve _gs_op from input if it exists, even if user function doesn't include it
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

        // Always preserve _gs_op from input if it exists, even if user function doesn't include it
        if ("_gs_op" in inputObj) {
          result._gs_op = inputObj._gs_op;
        }

        results.push(result);
      } catch (error) {
        // Format the error with context and throw it
        throw new Error(formatError(error, inputObj, i + 1));
      }
    }

    // Convert results back to Arrow table
    let outputTable;
    try {
      if (results.length === 0) {
        // For empty results, create a minimal table with empty columns
        // We'll create a table with a single dummy column that we can remove
        outputTable = tableFromArrays({ _dummy: [] });
      } else {
        // Transform array of objects into columnar format (object of arrays)
        // Collect all unique keys from all result objects
        const allKeys = new Set();
        for (const result of results) {
          if (result && typeof result === "object") {
            const keys = Object.keys(result);
            for (const key of keys) {
              allKeys.add(key);
            }
          }
        }

        // Build columnar structure: { columnName: [value1, value2, ...] }
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
          console.error(`Error in tableFromArrays: ${error.message}`);
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

    // Encode table to Arrow IPC bytes
    // Try to encode in smaller chunks or with different options if it fails
    let outputBytes;
    try {
      // Use file format (matches what Rust expects)
      outputBytes = tableToIPC(outputTable, { format: "file" });

      if (outputBytes === null) {
        throw new Error("tableToIPC returned null even with explicit format");
      }
    } catch (error) {
      throw new Error(
        `Failed to encode Arrow IPC output: ${error.message}${
          error.stack ? "\n" + error.stack : ""
        }`
      );
    }

    // Return Arrow IPC bytes directly - extism will convert Uint8Array to Vec<u8>
    return Host.outputBytes(outputBytes.buffer);
  } catch (error) {
    // Catch any unexpected errors and provide context
    throw new Error(
      `script runtime error: ${error.message}${
        error.stack ? "\n" + error.stack : ""
      }`
    );
  }
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

  // Add type-specific guidance
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

  // Format input data with truncation if too large
  let inputDataDisplay;
  try {
    const inputJson = JSON.stringify(input, null, 2);
    inputDataDisplay = truncateString(inputJson, MAX_INPUT_LENGTH);
  } catch (e) {
    // If JSON stringify fails, show a simple representation
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
