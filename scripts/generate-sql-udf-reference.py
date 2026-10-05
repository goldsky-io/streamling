#!/usr/bin/env python3
"""Generate docs/sql-udfs.md from the SQL function registrations of a Streamling session.

The registries (CommonFunctions, StreamlingFunctions, and the Flink-compat JSON and string
registries) supply membership/order, each implementation supplies its SQL names and
DataFusion signature, and the annotations below summarize Rust docs and runtime checks
which are not expressible by DataFusion's broad `Any` / `VariadicAny` signatures.
Run: python3 scripts/generate-sql-udf-reference.py [--check]
"""

import argparse
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "crates/streamling-common/src/functions"
REGISTRY = ROOT / "crates/streamling-common/src/functions.rs"
OUTPUT = ROOT / "docs/sql-udfs.md"
SESSION = ROOT / "crates/streamling-core/src/session.rs"
CORE_REGISTRY = ROOT / "crates/streamling-core/src/functions.rs"
CORE_SOURCE = ROOT / "crates/streamling-core/src/functions"
FLINK = ROOT / "crates/streamling-flink-compat/src"

# params, return type, behavior and runtime constraints, SQL example.
# The source-derived signature below is deliberately also printed: a change to an
# accepted DataFusion type or registration is visible on regeneration.
DETAILS = {
    "now": ("", "Timestamp(Nanosecond, UTC)", "Current UTC timestamp, evaluated per batch; alias: `current_timestamp`.", "SELECT now();"),
    "current_time": ("", "Time64(Nanosecond)", "Current UTC time of day, evaluated per batch.", "SELECT current_time();"),
    "current_date": ("", "Date32", "Current UTC date, evaluated per batch; alias: `today`.", "SELECT current_date();"),
    "_gs_json_objects_to_clickhouse_tuples": ("json, keys", "Utf8", "Format selected keys from a JSON object array as ClickHouse tuples; keys are taken from the first list and must be a `List<Utf8>` with a non-nullable item field.", "SELECT _gs_json_objects_to_clickhouse_tuples('[{\"pubkey\":\"abc\"}]', ARRAY['pubkey']);"),
    "_gs_split_string_to_array": ("text[, separator]", "List<Utf8>", "Split text on separator (space by default). Literal arguments are read as one-row arrays, so mixing literal and column arguments fails on batches of more than one row. To split a column, omit the separator (spaces) or use DataFusion's `string_to_array`.", "SELECT _gs_split_string_to_array('a-b', '-');"),
    "_gs_generate_series": ("start, stop[, step]", "List<Int64>", "Inclusive integer series; step defaults to 1 and must not be zero, and a negative step counts down. Literal arguments are read as one-row arrays, so mixing literal and column arguments fails on batches of more than one row. For a literal start with a column stop, use DataFusion's built-in `generate_series`.", "SELECT _gs_generate_series(1, 5, 2);"),
    "_gs_zip_arrays": ("array1, array2[, array3[, array4]]", "List<Struct<f0: T0, f1: T1, ...>>", "Requires 2–4 `List` inputs; each row's lists must have equal length. Struct fields are named f0, f1, etc.", "SELECT _gs_zip_arrays(ARRAY[1, 2], ARRAY['a', 'b']);"),
    "array_enumerate": ("list", "List<Struct<index: Int64, value: T>>", "Requires `List<T>` (not LargeList); pairs each value with its zero-based index.", "SELECT array_enumerate(ARRAY['a', 'b']);"),
    "array_filter": ("list, field_name, value", "List<Struct<...>>", "Requires `List<Struct>` and non-null `Utf8` field name (literal or column). Named field and value must have matching types: Utf8, Boolean, Int8/16/32/64, UInt8/16/32/64, Float32/64, or a NULL value. Returns matching elements.", "SELECT array_filter(changes, 'kind', 'transfer') FROM events;"),
    "array_filter_first": ("list, field_name, value", "Struct<...> or NULL", "Same type rules as `array_filter`; returns the first matching struct, or NULL.", "SELECT array_filter_first(changes, 'kind', 'transfer') FROM events;"),
    "array_filter_in": ("list, field_name, values", "List<Struct<...>>", "Requires `List<Struct>`, non-null `Utf8` field name (literal or column), and a `List<Utf8>` or `List<Int64>` of comparison values (a direct Utf8 array is also accepted). Named field must be Utf8 or Int64 respectively; the first values list is used for all rows.", "SELECT array_filter_in(changes, 'kind', ARRAY['transfer', 'mint']) FROM events;"),
    "array_struct_field": ("list, field_name", "List<Utf8>", "Projects an Utf8 field from `List<Struct>`; field_name **must be a non-null Utf8 string literal**, not a column.", "SELECT array_struct_field(changes, 'kind') FROM events;"),
    "to_large_list": ("list", "LargeList<T>", "Accepts List<T>, LargeList<T>, or FixedSizeList<T>; converts offsets to 64-bit (passes through LargeList).", "SELECT to_large_list(items) FROM events;"),
    "_gs_xxhash": ("text", "Utf8", "XXH3-128 digest of UTF-8 text as 32 hex characters; non-cryptographic.", "SELECT _gs_xxhash('hello');"),
    "_gs_keccak256": ("text", "Utf8", "Keccak-256 hash as `0x`-prefixed hex; a `0x`-prefixed input is decoded as hex first.", "SELECT _gs_keccak256('hello');"),
    "_gs_conv_base": ("number, from_base, to_base", "Utf8", "Convert an Utf8 number between bases 2–36; an invalid number or a base outside 2–36 returns NULL. Bases are read as Int32 only when `from_base` is an Int32 column; otherwise both bases must be Utf8, so pass literal bases as strings (`'16'`, not `16`). Pass all three arguments as literals or all three as columns: literal bases combined with a `number` column are one-row arrays and fail on batches of more than one row.", "SELECT _gs_conv_base('FF', '16', '10'); -- '255'"),
    "coalesce_meta": ("first[, next, ...]", "FixedSizeBinary(32)", "First non-null value per row, preserving the first field's metadata; **runtime supports only identical FixedSizeBinary(32) arguments**, despite the variadic-any planner signature.", "SELECT coalesce_meta(primary_id, fallback_id) FROM events;"),
    "json_string": ("value", "Utf8", "JSON-serialize an Arrow value; Utf8/LargeUtf8 are JSON-quoted; NULL remains NULL.", "SELECT json_string(payload) FROM events;"),
    "_gs_from_base58": ("text", "Binary", "Decode Base58 Utf8 text; invalid input or NULL produces empty bytes.", "SELECT _gs_from_base58('3yZe7d');"),
    "_gs_hex_to_byte": ("hex_text", "Binary", "Decode Utf8 hex (optional 0x prefix) to bytes; invalid input or NULL produces empty bytes.", "SELECT _gs_hex_to_byte('deadbeef');"),
    "_gs_byte_to_hex": ("bytes", "Utf8", "Hex-encode Binary bytes; NULL remains NULL.", "SELECT _gs_byte_to_hex(_gs_hex_to_byte('deadbeef'));"),
    "reverse_bytes32": ("bytes", "FixedSizeBinary(32)", "Reverse the 32 bytes, preserving input field metadata and NULL values.", "SELECT reverse_bytes32(hash) FROM events;"),
    "_gs_map_to_array_struct": ("map", "List<Struct<key: Utf8, value: Utf8>>", "Convert a `Map<Utf8, Utf8>` with non-nullable key/value fields to an array of key/value structs; expects the exact Arrow map layout.", "SELECT _gs_map_to_array_struct(params) FROM events;"),
    "uuid7": ("", "Utf8", "Generate a fresh UUID version 7 per row.", "SELECT uuid7();"),
}

DECIMAL_DECLARATION = (
    "Precision and scale must be non-null Int64 literals, with 1 <= precision <= 65535 "
    "and 0 <= scale <= precision. NULL values remain NULL; invalid or non-fitting values error."
)

DETAILS.update({
    "decimal_arb_to_string": ("value", "Utf8", "Render a decimal_arb value as canonical decimal text; NULL remains NULL.", "SELECT decimal_arb_to_string(amount) FROM events;"),
    "decimal_arb_rescale": ("value, precision, scale", "decimal_arb or same list kind", "Losslessly re-encode a decimal_arb value or List/LargeList/FixedSizeList of decimal_arb at a new precision and scale; excess significant fractional digits are rejected, not rounded. " + DECIMAL_DECLARATION, "SELECT decimal_arb_rescale(amount, 100, 4) FROM events;"),
    "decimal_arb_restamp": ("value, template", "same type as value, with restored metadata", "Pass value buffers through unchanged, restoring decimal_arb metadata from template at matching leaf positions in scalars, structs, lists and maps. Does not rescale or validate the numeric bytes; intended for planner-generated expressions.", "SELECT decimal_arb_restamp(amount, amount) FROM events;"),
    "decimal_arb_to_sort_key": ("value", "LargeBinary", "Encode a numeric-order sort key for a decimal_arb value; NULL remains NULL. Used by the ORDER BY rewrite, not a decimal_arb result.", "SELECT decimal_arb_to_sort_key(amount) FROM events;"),
    "to_decimal_arb_from_string": ("text, precision, scale[, native_int_kind]", "decimal_arb", "Parse Utf8 decimal text. Optional native_int_kind is a non-null Utf8 literal 'u256' or 'i256'; it hints native integer sink routing when scale is 0 and is omitted from the result metadata otherwise. " + DECIMAL_DECLARATION, "SELECT to_decimal_arb_from_string('123.45', 100, 2);"),
    "try_to_decimal_arb_from_string": ("text, precision, scale[, native_int_kind]", "decimal_arb or NULL", "Like to_decimal_arb_from_string, but malformed or non-fitting values become NULL. Invalid declarations still error; precision/scale must be Int64 literals, and the optional native_int_kind literal obeys the same rules.", "SELECT try_to_decimal_arb_from_string('bad', 100, 2); -- NULL"),
    "to_decimal_arb_from_int": ("value, precision, scale", "decimal_arb", "Convert an Int8/16/32/64 or UInt8/16/32/64 value exactly. " + DECIMAL_DECLARATION, "SELECT to_decimal_arb_from_int(123, 100, 2);"),
    "legacy_wide_int_to_decimal_arb": ("value", "decimal_arb(78, 0)", "Upgrade a FixedSizeBinary(32) value carrying retired streamling.u256 or streamling.i256 metadata; preserves the native integer hint and NULL values. Untagged bytes are rejected.", "SELECT legacy_wide_int_to_decimal_arb(amount) FROM events;"),
})
for operation in ("add", "sub", "mul", "div", "mod"):
    name = f"decimal_arb_{operation}"
    extra = " A zero divisor errors." if operation in ("div", "mod") else ""
    if operation == "div":
        extra += " Result scale is max(left scale, 18), rounded half-even once."
    DETAILS[name] = ("left, right", "decimal_arb",
                     f"{operation.capitalize()} two decimal_arb values; NULL operands yield NULL. Output precision/scale widen according to the operation (precision capped at 65535)." + extra,
                     f"SELECT decimal_arb_to_string({name}(amount, amount)) FROM events;")
for operation, description in (("neg", "Negate"), ("abs", "Take the absolute value of")):
    name = f"decimal_arb_{operation}"
    DETAILS[name] = ("value", "decimal_arb", f"{description} a decimal_arb value, preserving precision/scale; NULL remains NULL.", f"SELECT {name}(amount) FROM events;")
for operation, comparison in (("eq", "="), ("neq", "<>"), ("lt", "<"), ("lte", "<="), ("gt", ">"), ("gte", ">=")):
    name = f"decimal_arb_{operation}"
    DETAILS[name] = ("left, right", "Boolean", f"Numeric comparison (`{comparison}`) of decimal_arb values at their declared scales; NULL operands yield NULL.", f"SELECT {name}(amount, amount) FROM events;")
for operation, description in (("greatest", "largest"), ("least", "smallest")):
    name = f"decimal_arb_{operation}"
    DETAILS[name] = ("value[, next, ...]", "decimal_arb", f"Numerically {description} non-NULL argument (at least one required); NULL only when all arguments are NULL. Inputs may have different scales; result uses a common precision/scale.", f"SELECT {name}(amount, amount) FROM events;")
for operation, description in (("min", "smallest"), ("max", "largest")):
    name = f"decimal_arb_array_{operation}"
    DETAILS[name] = ("list", "decimal_arb", f"Numerically {description} non-NULL element of a List/LargeList/FixedSizeList of decimal_arb; empty, NULL or all-NULL lists yield NULL. Preserves element precision/scale.", f"SELECT {name}(amounts) FROM events;")
DETAILS["decimal_arb_array_sort"] = (
    "list[, order[, nulls]]", "same list type",
    "Sort a List/LargeList of decimal_arb numerically without re-encoding elements. Optional string literals: order is 'ASC' (default) or 'DESC'; nulls is 'NULLS FIRST' (default) or 'NULLS LAST'. NULL lists remain NULL.",
    "SELECT decimal_arb_array_sort(amounts, 'DESC', 'NULLS LAST') FROM events;")
for bits in (128, 256):
    DETAILS[f"to_decimal_arb_from_decimal{bits}"] = (
        "value", "decimal_arb", f"Losslessly widen Decimal{bits} with nonnegative scale, preserving precision/scale and NULL values.",
        f"SELECT to_decimal_arb_from_decimal{bits}(amount) FROM events;")
    DETAILS[f"decimal_arb_to_decimal{bits}"] = (
        "value, precision, scale", f"Decimal{bits}(precision, scale)",
        f"Narrow decimal_arb to Decimal{bits}; precision/scale must be Int64 literals valid for the target Arrow decimal. Values are half-even rounded to the target scale; values outside the target precision error. NULL remains NULL.",
        f"SELECT decimal_arb_to_decimal{bits}(amount, {38 if bits == 128 else 76}, 2) FROM events;")

DECIMAL_AGGREGATE_DETAILS = {
    "sum": ("value", "decimal_arb", "Sum non-NULL decimal_arb values; widens precision by 16 (capped at 65535), preserving scale. Empty/all-NULL groups yield NULL.", "SELECT sum(amount) FROM events;"),
    "min": ("value", "decimal_arb", "Numerically smallest non-NULL decimal_arb value, preserving precision/scale. Empty/all-NULL groups yield NULL.", "SELECT min(amount) FROM events;"),
    "max": ("value", "decimal_arb", "Numerically largest non-NULL decimal_arb value, preserving precision/scale. Empty/all-NULL groups yield NULL.", "SELECT max(amount) FROM events;"),
    "avg": ("value", "decimal_arb", "Average non-NULL decimal_arb values; widens precision and scale by 1 (capped at 65535), with half-even rounding. Empty/all-NULL groups yield NULL.", "SELECT avg(amount) FROM events;"),
    "array_agg": ("value", "List<decimal_arb>", "Collect values using DataFusion's array_agg behavior, preserving decimal_arb element metadata. Numeric ordering is supplied by the session rewrite.", "SELECT array_agg(amount) FROM events;"),
}

# Execution checks supplement broad Any signatures and the exact Arrow input schema.
RUNTIME_TYPES = {
    "_gs_zip_arrays": "2–4 List<T> arguments",
    "array_enumerate": "List<T>",
    "array_filter": "List<Struct>, Utf8, matching field type or NULL (see below)",
    "array_filter_first": "List<Struct>, Utf8, matching field type or NULL (see below)",
    "array_filter_in": "List<Struct>, Utf8, List<Utf8> or List<Int64>",
    "array_struct_field": "List<Struct>, Utf8 literal",
    "to_large_list": "List<T> / LargeList<T> / FixedSizeList<T>",
    "coalesce_meta": "one or more identical FixedSizeBinary(32) values",
    "json_string": "one Arrow value (requires a JSON-serializable type)",
    "_gs_map_to_array_struct": "Map<Utf8, Utf8> (exact Arrow entry field layout)",
    "dynamic_table_check": "string table name; string, List<string> or LargeList<string> value (string dictionaries accepted)",
    "regexp_substr": "(Utf8, Utf8); 3- and 4-argument calls fail at execution",
    "regexp_instr": "string, string[, integer[, integer]]",
    "locate": "string, string[, integer]",
    "instr": "string, string[, integer[, integer]]",
    "bin": "one integer",
    "elt": "integer, then one or more values",
    "parse_url": "string, string[, string]",
    "split": "string, string",
    "split_index": "string, string, integer",
    "translate": "string, string, string",
    "unhex": "one string",
    "url_encode": "one string",
    "url_decode": "one string",
    "json_array": "zero or more values",
    "json_array_absent_on_null": "zero or more values",
    "json_object": "an even number of values (key, value, ...)",
    "json_object_absent_on_null": "an even number of values (key, value, ...)",
    "json_value": "string, string, then 0 or 5 string literals",
}
RUNTIME_TYPES.update({
    name: "decimal_arb (LargeBinary with extension metadata)"
    for name in DETAILS if name.startswith("decimal_arb_")
})
RUNTIME_TYPES.update({
    name: "two decimal_arb values" for name in DETAILS
    if name.startswith("decimal_arb_") and name.rsplit("_", 1)[-1] in
    {"add", "sub", "mul", "div", "mod", "eq", "neq", "lt", "lte", "gt", "gte"}
})
RUNTIME_TYPES.update({
    "decimal_arb_rescale": "decimal_arb or list of decimal_arb, Int64 literal, Int64 literal",
    "decimal_arb_restamp": "two Arrow values (matching metadata template shape)",
    "decimal_arb_greatest": "one or more decimal_arb values",
    "decimal_arb_least": "one or more decimal_arb values",
    "decimal_arb_array_min": "List / LargeList / FixedSizeList of decimal_arb",
    "decimal_arb_array_max": "List / LargeList / FixedSizeList of decimal_arb",
    "decimal_arb_array_sort": "List / LargeList of decimal_arb[, string literal[, string literal]]",
    "decimal_arb_to_decimal128": "decimal_arb, Int64 literal, Int64 literal",
    "decimal_arb_to_decimal256": "decimal_arb, Int64 literal, Int64 literal",
    "to_decimal_arb_from_decimal128": "Decimal128 with nonnegative scale",
    "to_decimal_arb_from_decimal256": "Decimal256 with nonnegative scale",
    "legacy_wide_int_to_decimal_arb": "FixedSizeBinary(32) with streamling.u256 / streamling.i256 metadata",
})
RUNTIME_TYPES.update({name: "decimal_arb (or DataFusion built-in input types)"
                      for name in DECIMAL_AGGREGATE_DETAILS})

# Functions registered on every session outside CommonFunctions, keyed by the
# lowercase callable name: params, return type, behavior, SQL example.
SESSION_DETAILS = {
    "dynamic_table_check": ("table_name, value", "Boolean", "True when `value` is in the named dynamic table. `table_name` is a string literal, or a string column whose rows all hold the same name; an unknown name fails at execution. A string `value` that is NULL returns NULL. A `List<Utf8>`/`LargeList<Utf8>` value returns true when any non-null element is in the table; a NULL list returns NULL and an empty list returns false.", "SELECT * FROM transfers WHERE dynamic_table_check('tracked_wallets', from_address);"),
}

FLINK_STRING_DETAILS = {
    "regexp_extract": ("text, pattern[, group]", "Utf8", "Capture `group` of the first match (0, the default, is the whole match; a NULL `group` uses the default). Invalid pattern, no match, or a negative or out-of-range group returns NULL.", "SELECT regexp_extract('foothebar', 'foo(.*?)(bar)', 2); -- 'bar'"),
    "regexp_extract_all": ("text, pattern[, group]", "List<Utf8>", "Capture `group` (default 1; 0 is the whole match) of every match. No match returns an empty list; an invalid pattern or a negative or out-of-range group returns NULL.", "SELECT regexp_extract_all('100-200, 300-400', '(\\d+)-(\\d+)', 1); -- ['100', '300']"),
    "regexp_substr": ("text, pattern", "Utf8", "First whole match; no match or an invalid pattern returns NULL. The planner also accepts 3- and 4-argument calls, but execution rejects them.", "SELECT regexp_substr('100-200, 300-400', '(\\d+)-(\\d+)$'); -- '300-400'"),
    "regexp_count": ("text, pattern", "Int64", "Number of non-overlapping matches; an invalid pattern returns NULL. Replaces DataFusion's built-in `regexp_count`, so its start and flags arguments are not available.", "SELECT regexp_count('a.b.c.d', '\\.'); -- 3"),
    "regexp_instr": ("text, pattern[, start[, occurrence]]", "Int64", "1-based position of the `occurrence`-th match (default 1), searching from `start` (default 1). No match returns 0, an invalid pattern returns NULL, and `start` or `occurrence` below 1 is an error. Replaces DataFusion's built-in `regexp_instr`.", "SELECT regexp_instr('hello world! Hello everyone!', 'Hello'); -- 14"),
    "regexp_replace": ("text, pattern, replacement", "Utf8", "Replace every match. `replacement` is literal: `$1` / `\\1` group references are not expanded. An invalid pattern returns NULL. Replaces DataFusion's built-in `regexp_replace`, so its flags argument is not available.", "SELECT regexp_replace('hello world! Hello everyone!', 'Hello', 'Hi'); -- 'hello world! Hi everyone!'"),
    "locate": ("substring, text[, start]", "Int64", "1-based position of the first `substring` at or after `start` (default 1); not found or `start` below 1 returns 0.", "SELECT locate('bar', 'foobarbar', 5); -- 7"),
    "instr": ("text, substring[, start[, occurrence]]", "Int64", "1-based position of the `occurrence`-th `substring` (default 1). A negative `start` searches backward from the end; `start` 0 or not found returns 0; `occurrence` below 1 returns NULL. Replaces DataFusion's `instr` alias of `strpos` (`strpos` keeps the built-in behavior).", "SELECT instr('foobarbar', 'bar', 1, 2); -- 7"),
    "bin": ("integer", "Utf8", "Binary representation of an integer; negative values are prefixed with `-`. Non-integer input is an error.", "SELECT bin(42); -- '101010'"),
    "elt": ("index, value1[, value2, ...]", "Utf8", "The `index`-th (1-based) value as text; a NULL or out-of-range index returns NULL. Non-string values are converted to text.", "SELECT elt(2, 'foo', 'bar', 'baz'); -- 'bar'"),
    "parse_url": ("url, part[, key]", "Utf8", "URL component: `HOST`, `PATH`, `QUERY` (with `key`, that query parameter's value), `REF`, `PROTOCOL`, `FILE`, `AUTHORITY` or `USERINFO` (case-insensitive). An unknown part or unparseable URL returns NULL.", "SELECT parse_url('http://user:pass@example.com/path?query=1#frag', 'QUERY', 'query'); -- '1'"),
    "split": ("text, delimiter", "List<Utf8>", "Split on a literal delimiter, keeping empty tokens; an empty delimiter splits into characters. NULL text or delimiter returns NULL.", "SELECT split(',123,,,123,', ','); -- ['', '123', '', '', '123', '']"),
    "split_index": ("text, delimiter, index", "Utf8", "The `index`-th (0-based) token after splitting on a literal delimiter; a negative or out-of-range index returns NULL. An empty delimiter leaves the text as a single token.", "SELECT split_index(topics, ',', 0) AS event_signature FROM logs;"),
    "translate": ("text, from, to", "Utf8", "Replace each character of `from` with the character at the same position in `to`; `from` characters with no counterpart are removed, and the first occurrence of a repeated `from` character wins. NULL `from` returns `text` unchanged; NULL `to` removes every `from` character. Replaces DataFusion's built-in `translate`.", "SELECT translate('cat', 'at', 'o'); -- 'co'"),
    "unhex": ("hex", "Binary", "Decode hex text; odd-length input is left-padded with a 0 nibble and any non-hex character returns NULL.", "SELECT unhex('466C696E6B'); -- bytes of 'Flink'"),
    "url_encode": ("text", "Utf8", "Form URL encoding: space becomes `+`, and bytes other than `A-Z a-z 0-9 - _ . *` become `%XX`.", "SELECT url_encode('a+b/c'); -- 'a%2Bb%2Fc'"),
    "url_decode": ("text", "Utf8", "Inverse of `url_encode`; malformed `%` escapes or a non-UTF-8 result return NULL.", "SELECT url_decode('foo+bar'); -- 'foo bar'"),
}

JSON_PATH = "Paths support `$`, `.key`, `[n]` and `['key']` steps only (no wildcards, recursive descent or filters); an invalid path is an error."

FLINK_JSON_DETAILS = {
    "json_quote": ("text", "Utf8", "Quote text as a JSON string literal, escaping `\"`, `\\`, control characters, `/` (as `\\/`) and non-ASCII characters (as `\\uXXXX`).", "SELECT json_quote('value'); -- '\"value\"'"),
    "json_exists": ("json, path[, on_error]", "Boolean", "True when `path` resolves to a non-null value; a missing path is false. `on_error` is a string literal applied to invalid JSON: `FALSE` (default), `TRUE`, `UNKNOWN` (NULL) or `ERROR`. " + JSON_PATH, "SELECT json_exists('{\"a\":1}', '$.a'); -- true"),
    "json_array": ("[value, ...]", "Utf8", "JSON array of the arguments; NULL becomes `null`. Strings that hold a JSON object or array are embedded as JSON, other strings are quoted; lists, structs and maps are nested.", "SELECT json_array(token_id, amount) FROM transfers;"),
    "json_array_absent_on_null": ("[value, ...]", "Utf8", "Same as `json_array`, but NULL arguments are omitted.", "SELECT json_array_absent_on_null(token_id, memo) FROM transfers;"),
    "json_object": ("[key, value, ...]", "Utf8", "JSON object from key/value pairs; requires an even number of arguments. NULL values become `null`; a NULL key is an error. Values are converted as in `json_array`.", "SELECT json_object('address', from_address, 'amount', amount) FROM transfers;"),
    "json_object_absent_on_null": ("[key, value, ...]", "Utf8", "Same as `json_object`, but pairs with a NULL value are omitted.", "SELECT json_object_absent_on_null('address', from_address, 'memo', memo) FROM transfers;"),
    "json": ("text", "Utf8", "Validate JSON text and re-serialize it compactly; empty or whitespace-only text returns NULL and invalid JSON is an error.", "SELECT json('{\"a\": 1}');"),
    "json_unquote": ("text", "Utf8", "Unescape a double-quoted JSON string; other text is returned unchanged.", "SELECT json_unquote('\"value\"');"),
    "json_query": ("json, path[, returning, wrapper, on_empty, on_error]", "Utf8", "JSON text of the object or array at `path`. A scalar at `path` counts as empty and returns NULL unless wrapped; use `json_value` for scalars. The 6-argument form takes string literals: `returning` is `STRING` or `ARRAY`; `wrapper` is `WITHOUT ARRAY` (default), `WITH CONDITIONAL ARRAY` or `WITH UNCONDITIONAL ARRAY`; `on_empty` and `on_error` are `NULL` (default), `EMPTY ARRAY`, `EMPTY OBJECT` or `ERROR`. " + JSON_PATH, "SELECT json_query('{\"a\":{\"b\":[1,2]}}', '$.a'); -- '{\"b\":[1,2]}'"),
    "json_value": ("json, path[, returning, on_empty, empty_default, on_error, error_default]", "Utf8", "Scalar at `path` as text (strings are unquoted). An object, array, JSON `null` or missing path counts as empty and returns NULL by default; invalid JSON returns NULL by default. The 7-argument form takes string literals: `returning` is `STRING`, `VARCHAR` or `CHAR`; `on_empty` and `on_error` are `NULL`, `ERROR` or `DEFAULT`, where `DEFAULT` returns the following argument. " + JSON_PATH, "SELECT json_value('{\"a\":null}', '$.a', 'STRING', 'DEFAULT', 'missing', 'NULL', NULL); -- 'missing'"),
    "parse_json": ("text[, allow_duplicate_keys]", "Utf8", "Validate JSON text and re-serialize it compactly; invalid JSON is an error and empty text returns NULL. `allow_duplicate_keys` must be a Boolean literal and is currently ignored.", "SELECT parse_json(metadata) FROM tokens;"),
    "try_parse_json": ("text[, allow_duplicate_keys]", "Utf8", "Same as `parse_json`, but invalid JSON returns NULL.", "SELECT try_parse_json(metadata) FROM tokens;"),
    "is_json": ("text[, type]", "Boolean", "Whether text parses as JSON of `type`, a string literal: `VALUE` (default), `OBJECT`, `ARRAY` or `SCALAR`. NULL input returns false.", "SELECT is_json(metadata, 'OBJECT') FROM tokens;"),
}

FLINK_JSON_AGGREGATE_DETAILS = {
    "json_arrayagg_null_on_null": ("value", "Utf8", "Aggregate: JSON array of the group's values, with NULL as `null`; values are converted as in `json_array`. An empty group returns NULL. `DISTINCT`, `ORDER BY` and `IGNORE NULLS` are rejected.", "SELECT owner, json_arrayagg_null_on_null(token_id) FROM transfers GROUP BY owner;"),
    "json_arrayagg_absent_on_null": ("value", "Utf8", "Same as `json_arrayagg_null_on_null`, but NULL values are omitted.", "SELECT owner, json_arrayagg_absent_on_null(token_id) FROM transfers GROUP BY owner;"),
    "json_objectagg_null_on_null": ("key, value", "Utf8", "Aggregate: JSON object of the group's key/value pairs, with NULL values as `null`. A NULL or duplicate key is an error; an empty group returns NULL. `DISTINCT`, `ORDER BY` and `IGNORE NULLS` are rejected.", "SELECT owner, json_objectagg_null_on_null(token_id, amount) FROM transfers GROUP BY owner;"),
    "json_objectagg_absent_on_null": ("key, value", "Utf8", "Same as `json_objectagg_null_on_null`, but pairs with a NULL value are omitted.", "SELECT owner, json_objectagg_absent_on_null(token_id, amount) FROM transfers GROUP BY owner;"),
}


def balanced(text: str, start: int) -> str:
    """Return the parenthesized Rust expression starting at start."""
    assert text[start] == "("
    depth = 0
    for end in range(start, len(text)):
        if text[end] == "(":
            depth += 1
        elif text[end] == ")":
            depth -= 1
            if depth == 0:
                return text[start + 1:end]
    raise ValueError("Unclosed Rust expression")


def create_udf_signature(text: str, symbol: str, expr: str) -> str:
    if "get_input_type()" in expr:
        input_type = text[text.index("fn get_input_type()"):text.index("pub fn " + symbol)]
        if "DataType::Map(" not in input_type or input_type.count("DataType::Utf8") != 2:
            raise ValueError(f"Unexpected map input type for {symbol}")
        return "(Map<Utf8, Utf8>)"
    vec = re.search(r"\bvec!\[([^]]+)\]", expr)
    assert vec, symbol
    return "(" + ", ".join(re.findall(r"DataType::(\w+)", vec.group(1))) + ")"


def parse_signature(kind: str, expr: str, symbol: str) -> str:
    """Render the arguments of a `Signature::<kind>(<expr>)` constructor."""
    if kind == "nullary":
        return "()"
    if kind == "user_defined":
        return "user-defined"
    if kind == "variadic_any" or "TypeSignature::VariadicAny" in expr:
        return "() or (Any, ...)" if "TypeSignature::Nullary" in expr else "(Any, ...)"
    if kind == "any":
        n = int(re.match(r"\s*(\d+)", expr).group(1))
        return "(" + ", ".join(["Any"] * n) + ")"
    if "TypeSignature::Any(" in expr:
        n = int(re.search(r"TypeSignature::Any\((\d+)\)", expr).group(1))
        return "(" + ", ".join(["Any"] * n) + ")"
    string = re.fullmatch(r"\s*TypeSignature::String\((\d+)\),\s*Volatility::\w+,?\s*", expr)
    if kind == "new" and string:
        return "(" + ", ".join(["String"] * int(string.group(1))) + ")"
    signatures = []
    vec_pattern = r"TypeSignature::Exact\(\s*vec!\[" if kind == "one_of" else r"^\s*vec!\["
    for vec in re.finditer(vec_pattern, expr):
        section = expr[vec.end():]
        # Nested Rust data types must not be split at their internal commas.
        depth = 0
        types = []
        current = []
        for ch in section:
            if ch == "]" and depth == 0:
                if current:
                    types.append("".join(current))
                break
            if ch in "([":
                depth += 1
            elif ch in ")]":
                depth -= 1
            if ch == "," and depth == 0:
                types.append("".join(current))
                current = []
            else:
                current.append(ch)
        names = []
        for value in types:
            if not value.strip():
                continue
            if "DataType::List(" in value:
                if re.findall(r"DataType::(\w+)", value) != ["List", "Utf8"]:
                    raise ValueError(f"Unexpected list input type for {symbol}: {value}")
                names.append("List<Utf8> (non-null items)")
            elif "DataType::FixedSizeBinary(32)" in value:
                names.append("FixedSizeBinary(32)")
            else:
                typ = re.fullmatch(r"\s*DataType::(\w+)\s*", value)
                if not typ:
                    raise ValueError(f"Unrecognized signature type for {symbol}: {value}")
                names.append(typ.group(1))
        signatures.append("(" + ", ".join(names) + ")")
    if not signatures or (kind == "variadic" and len(signatures) != 1):
        raise ValueError(f"Unrecognized signature for {symbol}: {expr}")
    if kind == "variadic":
        return signatures[0][:-1] + ", ...)"
    return " or ".join(signatures)


def source_signature(text: str, symbol: str, factory: bool) -> str:
    if factory:
        body = text[text.index("pub fn " + symbol + "("):]
        match = re.search(r"\bcreate_udf\s*\(", body)
        assert match, symbol
        return create_udf_signature(text, symbol, balanced(body, match.end() - 1))

    body = rust_block(text, rf"^impl {symbol} \{{")
    if symbol == "ToDecimalArbFromIntFunc":
        kinds = re.search(r"let int_kinds = \[([^]]+)\]", body)
        signature = re.search(r"TypeSignature::Exact\(vec!\[t, ([^]]+)\]\)", body)
        if not kinds or not signature or "int_kinds" not in body:
            raise ValueError("Unrecognized integer decimal_arb signature")
        return " or ".join(parse_signature("exact", f"vec![DataType::{kind}, {signature.group(1)}]", symbol)
                           for kind in re.findall(r"DataType::(\w+)", kinds.group(1)))
    match = re.search(r"signature:\s*Signature::(\w+)\s*\(", body)
    if not match:
        raise ValueError(f"Missing signature for {symbol}")
    return parse_signature(match.group(1), balanced(body, match.end() - 1), symbol)


def rust_doc_summary(text: str, symbol: str, sql_name: str) -> str:
    """Read adjacent Rust documentation, if the implementation has any."""
    # Some historical docs describe a narrower case, or claim overflow raises
    # an error when the implementation returns NULL. The reviewed annotations
    # below are authoritative for those cases.
    if sql_name in {"array_filter", "array_filter_first", "array_filter_in"}:
        return ""
    declaration = rf"(?:pub struct {symbol}\b|pub fn {symbol}\()"
    match = re.search(r"(?P<docs>(?:^///[^\n]*\n)+)(?:#\[[^\n]*\]\n)*" + declaration, text, re.M)
    if not match:
        return ""
    lines = [re.sub(r"^//[/!] ?", "", line).strip() for line in match.group("docs").splitlines()]
    lines = [line for line in lines if line and not line.startswith("#") and not (line.lstrip("`").startswith((sql_name + "(", sql_name.removeprefix("_gs_") + "(")) and "->" in line)]
    if not lines or lines[0].startswith(("Creates a ScalarUDF", "Creates the ")):
        return ""
    summary = ""
    for line in lines:
        summary += (" " if summary else "") + line
        if summary.endswith("."):
            break
    return summary


def common_expressions():
    block = rust_block(REGISTRY.read_text(), r"\bfn functions\(")
    block = re.sub(r"//[^\n]*", "", block)
    body = block.split("vec![", 1)[1].rsplit("]", 1)[0]
    depth, start = 0, 0
    for i, ch in enumerate(body):
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        elif ch == "," and depth == 0:
            if body[start:i].strip():
                yield body[start:i].strip()
            start = i + 1
    if body[start:].strip():
        yield body[start:].strip()


def variant_name(text: str, symbol: str, constructor: str) -> str:
    inherent = rust_block(text, rf"^impl {symbol} \{{")
    method = rust_block(inherent, rf"\bfn {constructor}\(")
    variant = re.search(r"Self::new\((\w+::\w+)\)", method)
    if not variant:
        raise ValueError(f"Unrecognized constructor {symbol}::{constructor}")
    implementation = rust_block(text, rf"^impl (?:Scalar|Aggregate)UDFImpl for {symbol} \{{")
    name_body = rust_block(implementation, r"\bfn name\(")
    if "self.extreme.name()" in name_body:
        name_body = rust_block(text, rf"^impl {variant.group(1).split('::')[0]} \{{")
    matches = re.findall(rf'\b{variant.group(1)} => "(\w+)"', name_body)
    if len(set(matches)) != 1:
        raise ValueError(f"Unrecognized name for {symbol}::{constructor}")
    return matches[0]


def registered():
    sources = {p.stem: p.read_text() for p in SOURCE.glob("*.rs")}
    for expr in common_expressions():
        if expr.startswith("DecimalArbBuiltinShim::wrap("):
            shim_builtin(expr)  # Reject unfamiliar wrappers instead of silently omitting them.
            continue
        match = re.fullmatch(r"ScalarUDF::from\((?:\w+::)?(\w+)::(\w+)\(\)\)", expr)
        factory = re.fullmatch(r"(create_\w+_udf)\(\)", expr)
        if not match and not factory:
            raise ValueError(f"Unrecognized registration in CommonFunctions::functions(): {expr}")
        symbol = match.group(1) if match else factory.group(1)
        candidates = [(module, text) for module, text in sources.items()
                      if (re.search(rf"\bimpl ScalarUDFImpl for {symbol}\b", text)
                          or re.search(rf"\bdecimal_arb_(?:binary|cmp)_op!\(\s*{symbol},", text)
                          or re.search(rf"\bpub fn {symbol}\(", text))]
        if len(candidates) != 1:
            raise ValueError(f"Expected exactly one source for {symbol}, got {[module for module, _ in candidates]}")
        module, text = candidates[0]
        macro = re.search(rf'(decimal_arb_(?:binary|cmp)_op)!\(\s*{symbol},\s*"(\w+)"', text)
        if macro:
            name = macro.group(2)
            template = rust_block(text, rf"\bmacro_rules! {macro.group(1)}\b")
            signature = re.search(r"signature:\s*Signature::(\w+)\s*\(", template)
            sig = parse_signature(signature.group(1), balanced(template, signature.end() - 1), symbol)
        else:
            if factory:
                body = text[text.index("pub fn " + symbol + "("):]
                name = re.search(r'\bcreate_udf\s*\(\s*"([\w]+)"', body).group(1)
            elif match.group(2) != "new":
                name = variant_name(text, symbol, match.group(2))
            else:
                names = fixed_name(text, symbol)
                if len(names) != 1:
                    raise ValueError(f"Missing fixed name for {symbol}")
                name = names[0]
            sig = source_signature(text, symbol, bool(factory))
        yield name, symbol, module, sig, rust_doc_summary(text, symbol, name)


def shim_builtin(expr: str) -> str:
    match = re.fullmatch(r"DecimalArbBuiltinShim::wrap\(\s*datafusion::(?:\w+::)+(\w+)\(\),?\s*\)", expr)
    if not match:
        raise ValueError(f"Unrecognized builtin shim: {expr}")
    return match.group(1).removesuffix("_udf")


def decimal_aggregates():
    path = SOURCE / "decimal_arb_aggregates.rs"
    text = path.read_text()
    manager = rust_block(SESSION.read_text(), r"^impl SessionManager \{")
    session = rust_block(manager, r"\bpub fn new\(")
    calls = re.findall(r"ctx\.register_udaf\((\w+)::(\w+)\(\)\);", session)
    if len(calls) != session.count("ctx.register_udaf("):
        raise ValueError("Unrecognized decimal_arb aggregate registration")
    for symbol, constructor in calls:
        names = fixed_name(text, symbol)
        name = names[0] if names else variant_name(text, symbol, constructor)
        sig = "DataFusion array_agg signature" if symbol == "DecimalArbArrayAggUdaf" else impl_signature(text, symbol)
        yield [name], [], path, symbol, sig


def rust_block(text: str, header: str) -> str | None:
    """Return the brace-delimited body following the unique match of header, if any."""
    matches = list(re.finditer(header, text, re.M))
    if not matches:
        return None
    if len(matches) > 1:
        raise ValueError(f"Ambiguous Rust item {header!r}")
    start = text.index("{", matches[0].end() - 1)
    depth = 0
    for end in range(start, len(text)):
        if text[end] == "{":
            depth += 1
        elif text[end] == "}":
            depth -= 1
            if depth == 0:
                return text[start:end + 1]
    raise ValueError(f"Unclosed Rust item {header!r}")


def impl_signature(text: str, symbol: str) -> str:
    """Signature of a UDF impl struct, following a delegate built with new_from_impl."""
    inherent = rust_block(text, rf"^impl {symbol} \{{")
    for block in (inherent, rust_block(text, rf"^impl (?:Scalar|Aggregate)UDFImpl for {symbol} \{{")):
        match = block and re.search(r"\bSignature::(\w+)\s*\(", block)
        if match:
            return parse_signature(match.group(1), balanced(block, match.end() - 1), symbol)
    delegate = inherent and re.search(r"new_from_impl\((\w+)::new\(", inherent)
    if not delegate:
        raise ValueError(f"Missing signature for {symbol}")
    return impl_signature(text, delegate.group(1))


def fixed_name(text: str, symbol: str) -> list[str]:
    block = rust_block(text, rf"^impl (?:Scalar|Aggregate)UDFImpl for {symbol} \{{") or ""
    match = re.search(r'fn name\(&self\) -> &str \{\s*"(\w+)"\s*\}', block)
    return [match.group(1)] if match else []


def session_registrations(path: Path, function: str):
    """Yield (kind, factory expression) for each ctx.register_* call in a Rust function."""
    body = rust_block(path.read_text(), rf"\bfn {function}\(")
    calls = list(re.finditer(r"\bctx\.register_(udf|udaf)\(", body))
    if len(calls) != body.count("ctx.register_") or not calls:
        raise ValueError(f"Unrecognized registration in {function}")
    for call in calls:
        yield call.group(1), balanced(body, call.end() - 1)


def flink_function(expr: str, sources: dict[Path, str]) -> tuple[list[str], list[str], Path, str, str]:
    """Resolve a registration expression to (callable names, other names, file, symbol, signature)."""
    factory = re.match(r"\s*(\w+)\(", expr).group(1)
    candidates = [(path, text) for path, text in sources.items() if re.search(rf"\bfn {factory}\(", text)]
    if len(candidates) != 1:
        raise ValueError(f"Expected exactly one source for {factory}, got {[str(path) for path, _ in candidates]}")
    path, text = candidates[0]
    body = rust_block(text, rf"\bfn {factory}\(")
    names = re.findall(r'"(\w+)"', expr) + re.findall(r'"(\w+)"', body)
    implementation = re.search(r"new_from_impl\((\w+)::new\(", body)
    if implementation:
        symbol = implementation.group(1)
        signature = impl_signature(text, symbol)
        names += fixed_name(text, symbol)
    else:
        match = re.search(r"\bcreate_udf\s*\(", body)
        if not match:
            raise ValueError(f"Unrecognized UDF factory {factory}")
        symbol = factory
        signature = create_udf_signature(text, symbol, balanced(body, match.end() - 1))
    # Unquoted SQL identifiers are lowercased before function lookup.
    callable_names = list(dict.fromkeys(name for name in names if name == name.lower()))
    if not callable_names:
        raise ValueError(f"{factory} has no lowercase SQL name")
    others = sorted({name for name in names if name.lower() not in callable_names})
    return callable_names, others, path, symbol, signature


def session_functions():
    """Yield the non-CommonFunctions layers in their session registration order."""
    session = SESSION.read_text()
    calls = ["register_json_functions(&ctx)", "register_string_aliases(&ctx)",
             "StreamlingFunctions::functions(", "ctx.register_udaf(", "register_plugin_udfs(&ctx)"]
    positions = [session.find(call) for call in calls]
    if -1 in positions or positions != sorted(positions):
        raise ValueError("Session function registration order changed; update this generator")

    sources = {path: path.read_text() for path in FLINK.rglob("*.rs") if path.name != "tests.rs"}
    json_rows, aggregate_rows = [], []
    for kind, expr in session_registrations(FLINK / "json/registry.rs", "register_json_functions"):
        (aggregate_rows if kind == "udaf" else json_rows).append(flink_function(expr, sources))
    string_rows = [flink_function(expr, sources)
                   for kind, expr in session_registrations(FLINK / "string/registry.rs", "register_string_aliases")]

    core = rust_block(CORE_REGISTRY.read_text(), r"\bfn functions\(")
    pushes = re.findall(r"funcs\.push\(ScalarUDF::from\((\w+)::new\(", core)
    if "CommonFunctions::functions()" not in core or len(pushes) != core.count("funcs.push("):
        raise ValueError("Unrecognized registration in StreamlingFunctions::functions()")
    core_sources = {path: path.read_text() for path in CORE_SOURCE.glob("*.rs")}
    core_rows = []
    for symbol in pushes:
        candidates = [(path, text) for path, text in core_sources.items()
                      if re.search(rf"\bimpl ScalarUDFImpl for {symbol}\b", text)]
        if len(candidates) != 1:
            raise ValueError(f"Expected exactly one source for {symbol}")
        path, text = candidates[0]
        core_rows.append((fixed_name(text, symbol), [], path, symbol, impl_signature(text, symbol)))

    registry = rust_block((FLINK / "string/registry.rs").read_text(), r"\bfn register_string_aliases\(")
    aliases = re.findall(r'\(\s*"(\w+)",\s*&\[([^\]]*)\],?\s*\)', registry)
    if len(aliases) != len(re.findall(r'\(\s*"\w+",\s*&\[', registry)):
        raise ValueError("Unrecognized entry in string ALIASES")
    aliases = [(canonical, re.findall(r'"(\w+)"', names)) for canonical, names in aliases]
    return core_rows, string_rows, json_rows, aggregate_rows, aliases


def anchor(name: str) -> str:
    return "udf-" + name.strip("_").replace("_", "-")


def table(rows) -> list[str]:
    out = ["| Function | Signature | Accepted types | Returns |", "| --- | --- | --- | --- |"]
    for name, params, result, accepted in rows:
        out.append(f"| [`{name}`](#{anchor(name)}) | `{name}({params})` | `{accepted}` | `{result}` |")
    return out + [""]


def entry(name, params, result, signature, description, example, source, symbol, rust_docs="", also=()) -> list[str]:
    out = [f'<a id="{anchor(name)}"></a>', "", f"### `{name}`", "",
           f"**Signature:** `{name}({params})` → `{result}`. **DataFusion signature:** `{signature}`.", ""]
    if also:
        out.extend(["Also callable as " + ", ".join(f"`{alias}`" for alias in also) + ".", ""])
    if rust_docs:
        out.extend([f"**Rust doc summary:** {rust_docs}", ""])
    link = source.relative_to(ROOT)
    return out + [description, "", "```sql", example, "```", "", f"Source: [`{link.name}`](../{link}) (`{symbol}`).", ""]


def session_section(title: str, intro: list[str], rows, details: dict) -> list[str]:
    names = [callable_names[0] for callable_names, *_ in rows]
    if len(names) != len(set(names)) or set(names) != set(details):
        raise ValueError(f"{title}: registry/documentation mismatch: missing={set(names) - set(details)}, stale={set(details) - set(names)}")
    out = [f"## {title}", "", *intro, ""]
    out += table((name, details[name][0], details[name][1], RUNTIME_TYPES.get(name, signature))
                 for name, (_, _, _, _, signature) in zip(names, rows))
    for (callable_names, _, path, symbol, signature) in rows:
        name = callable_names[0]
        params, result, description, example = details[name]
        out += entry(name, params, result, signature, description, example, path, symbol, also=callable_names[1:])
    return out


def render() -> str:
    rows = list(registered())
    names = [name for name, *_ in rows]
    if len(names) != len(set(names)) or set(names) != set(DETAILS):
        raise ValueError(f"Registry/documentation mismatch: missing={set(names) - set(DETAILS)}, stale={set(DETAILS) - set(names)}; duplicates={len(names) - len(set(names))}")
    core_rows, string_rows, json_rows, aggregate_rows, aliases = session_functions()
    decimal_rows = list(decimal_aggregates())
    shims = [shim_builtin(expr) for expr in common_expressions()
             if expr.startswith("DecimalArbBuiltinShim::wrap(")]
    if len(shims) != len(set(shims)):
        raise ValueError("Duplicate decimal_arb builtin shim")

    callable_names = names + [name for row in (*core_rows, *string_rows, *json_rows, *aggregate_rows) for name in row[0]]
    callable_names += [name for row in decimal_rows for name in row[0]] + shims
    callable_names += [alias for canonical, names_ in aliases for alias in names_ if alias == alias.lower() and alias != canonical]
    duplicates = sorted({name for name in callable_names if callable_names.count(name) > 1})
    if duplicates:
        raise ValueError(f"Functions registered twice; the later registration wins: {duplicates}")
    quoted_only = sorted({name for row in (*string_rows, *json_rows, *aggregate_rows) for name in row[1]}
                         | {alias for canonical, names_ in aliases for alias in names_
                            if alias != alias.lower() and alias.lower() not in (canonical, *names_)})

    out = [
        "# Engine SQL function reference",
        "",
        "<!-- Generated by python3 scripts/generate-sql-udf-reference.py; do not edit directly. -->",
        "<!-- CI checks freshness with python3 scripts/generate-sql-udf-reference.py --check. -->",
        "",
        "Every Streamling SQL session starts with DataFusion's built-in functions and then registers,",
        "in order (`crates/streamling-core/src/session.rs`):",
        "",
        "1. [Flink-compatible JSON functions](#flink-compatible-json-functions) and [aggregates](#flink-compatible-json-aggregates) (`register_json_functions`).",
        "2. [Flink-compatible string functions](#flink-compatible-string-functions) and [aliases of DataFusion built-ins](#flink-compatible-aliases-of-datafusion-built-ins) (`register_string_aliases`).",
        "3. [Streamling functions](#streamling-functions) (`CommonFunctions::functions()`) and [session functions](#session-functions) (`StreamlingFunctions::functions()`).",
        "4. [Decimal-aware aggregates](#decimal-aware-aggregates), overriding the corresponding DataFusion aggregates.",
        "5. Plugin-provided functions, which depend on which plugins a deployment loads and are not listed here.",
        "",
        "A later registration replaces an earlier function with the same name, so several functions below",
        "replace DataFusion built-ins; their entries say so. Unquoted SQL function names are lowercased",
        "before lookup, so `SPLIT_INDEX(...)` calls `split_index`. These mixed-case registrations are",
        "omitted below, because an unquoted call resolves to its lowercase spelling instead: "
        + ", ".join(f"`{name}`" for name in quoted_only) + ".",
        "",
        "Names beginning with `_gs_` are internal-style names but are registered under exactly those names.",
        "",
        "The **accepted types** column reflects the Rust DataFusion signature, narrowed by",
        "runtime checks where necessary. `Any` in a DataFusion signature means the planner",
        "accepts any type, *not* that the implementation can process all types. The runtime",
        "constraints below each function narrow that signature. `String` and *string* mean",
        "Utf8, LargeUtf8 or Utf8View. `decimal_arb` is a `LargeBinary` extension type",
        "with precision/scale metadata, not arbitrary bytes. Construct it with `to_decimal_arb_from_string`",
        "or `CAST(... AS DECIMAL(p, s))` for precision greater than 76.",
        "The retired `to_u256`, `u256_*`, `to_i256`, `i256_*` and `to_int64` functions are no longer registered.",
        "See [arbitrary-precision decimals](decimal-arbitrary-precision.md) for operators, casts and precision rules.",
        "",
        "## Streamling functions",
        "",
        "Registered by `CommonFunctions::functions()` in `crates/streamling-common/src/functions.rs`.",
        "",
    ]
    out += table((name, DETAILS[name][0], DETAILS[name][1], RUNTIME_TYPES.get(name, signature))
                 for name, _, _, signature, _ in rows)
    for name, symbol, module, signature, rust_docs in rows:
        params, result, description, example = DETAILS[name]
        out += entry(name, params, result, signature, description, example,
                     SOURCE / f"{module}.rs", symbol, rust_docs=rust_docs)
    out += [
        "## Decimal-aware builtin shims", "",
        "`CommonFunctions::functions()` also wraps these DataFusion built-ins to let decimal_arb",
        "arguments reach the session's analyzer rewrites. Non-decimal inputs keep DataFusion behavior;",
        "mixed decimal calls that reach execution without a rewrite error rather than using bytewise behavior.",
        "These remain built-in functions, not additional decimal_arb UDF names.", "",
        ", ".join(f"`{name}`" for name in shims) + ".", "",
        "Source: [`decimal_arb_builtin_shim.rs`](../crates/streamling-common/src/functions/decimal_arb_builtin_shim.rs).", "",
    ]
    out += session_section("Decimal-aware aggregates", [
        "Registered in `session.rs` after the scalar functions. Decimal inputs use the contracts below;",
        "other supported input types delegate to the corresponding DataFusion built-in aggregate.",
    ], decimal_rows, DECIMAL_AGGREGATE_DETAILS)

    out += session_section("Session functions", [
        "Registered by `StreamlingFunctions::functions()` in `crates/streamling-core/src/functions.rs`; it needs the session's dynamic table registry.",
    ], core_rows, SESSION_DETAILS)
    out += session_section("Flink-compatible string functions", [
        "Registered by `register_string_aliases` in `crates/streamling-flink-compat/src/string/registry.rs`.",
        "Unless noted, a NULL argument returns NULL, string arguments accept any *string* type and integer arguments accept any integer type.",
    ], string_rows, FLINK_STRING_DETAILS)
    out += session_section("Flink-compatible JSON functions", [
        "Registered by `register_json_functions` in `crates/streamling-flink-compat/src/json/registry.rs`.",
        "Unless noted, a NULL JSON or path argument returns NULL.",
    ], json_rows, FLINK_JSON_DETAILS)
    out += session_section("Flink-compatible JSON aggregates", [
        "Aggregate functions registered by `register_json_functions`.",
    ], aggregate_rows, FLINK_JSON_AGGREGATE_DETAILS)

    out += [
        "## Flink-compatible aliases of DataFusion built-ins",
        "",
        "`register_string_aliases` re-registers these DataFusion built-ins under extra names; behavior is the built-in's.",
        "",
        "| Built-in | Extra names |",
        "| --- | --- |",
    ]
    for canonical, names_ in aliases:
        extra = [alias for alias in names_ if alias == alias.lower() and alias != canonical]
        if extra:
            out.append(f"| `{canonical}` | " + ", ".join(f"`{alias}`" for alias in extra) + " |")
    out.append("")
    return "\n".join(out)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail if the generated reference is stale")
    args = parser.parse_args()
    content = render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != content:
            parser.error(f"{OUTPUT.relative_to(ROOT)} is stale; regenerate it")
    else:
        OUTPUT.write_text(content)
        documented = len(DETAILS) + len(SESSION_DETAILS) + len(FLINK_STRING_DETAILS) + len(FLINK_JSON_DETAILS) + len(FLINK_JSON_AGGREGATE_DETAILS) + len(DECIMAL_AGGREGATE_DETAILS)
        print(f"Generated {OUTPUT.relative_to(ROOT)} ({documented} functions)")


if __name__ == "__main__":
    main()
