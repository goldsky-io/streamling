#!/usr/bin/env python3
"""Generate docs/sql-udfs.md from CommonFunctions and its Rust implementations.

The registry supplies membership/order, each implementation supplies its SQL name and
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

# params, return type, behavior and runtime constraints, SQL example.
# The source-derived signature below is deliberately also printed: a change to an
# accepted DataFusion type or registration is visible on regeneration.
DETAILS = {
    "now": ("", "Timestamp(Nanosecond, UTC)", "Current UTC timestamp, evaluated per batch; alias: `current_timestamp`.", "SELECT now();"),
    "current_time": ("", "Time64(Nanosecond)", "Current UTC time of day, evaluated per batch.", "SELECT current_time();"),
    "current_date": ("", "Date32", "Current UTC date, evaluated per batch; alias: `today`.", "SELECT current_date();"),
    "_gs_json_objects_to_clickhouse_tuples": ("json, keys", "Utf8", "Format selected keys from a JSON object array as ClickHouse tuples; keys are taken from the first list and must be a `List<Utf8>` with a non-nullable item field.", "SELECT _gs_json_objects_to_clickhouse_tuples('[{\"pubkey\":\"abc\"}]', ARRAY['pubkey']);"),
    "_gs_split_string_to_array": ("text[, separator]", "List<Utf8>", "Split text on separator (space by default).", "SELECT _gs_split_string_to_array('a-b', '-');"),
    "_gs_generate_series": ("start, stop[, step]", "List<Int64>", "Inclusive integer series; step defaults to 1, must not be zero.", "SELECT _gs_generate_series(1, 5, 2);"),
    "_gs_zip_arrays": ("array1, array2[, array3[, array4]]", "List<Struct<f0: T0, f1: T1, ...>>", "Requires 2–4 `List` inputs; each row's lists must have equal length. Struct fields are named f0, f1, etc.", "SELECT _gs_zip_arrays(ARRAY[1, 2], ARRAY['a', 'b']);"),
    "array_enumerate": ("list", "List<Struct<index: Int64, value: T>>", "Requires `List<T>` (not LargeList); pairs each value with its zero-based index.", "SELECT array_enumerate(ARRAY['a', 'b']);"),
    "array_filter": ("list, field_name, value", "List<Struct<...>>", "Requires `List<Struct>` and non-null `Utf8` field name (literal or column). Named field and value must have matching types: Utf8, Boolean, Int8/16/32/64, UInt8/16/32/64, Float32/64, or a NULL value. Returns matching elements.", "SELECT array_filter(changes, 'kind', 'transfer') FROM events;"),
    "array_filter_first": ("list, field_name, value", "Struct<...> or NULL", "Same type rules as `array_filter`; returns the first matching struct, or NULL.", "SELECT array_filter_first(changes, 'kind', 'transfer') FROM events;"),
    "array_filter_in": ("list, field_name, values", "List<Struct<...>>", "Requires `List<Struct>`, non-null `Utf8` field name (literal or column), and a `List<Utf8>` or `List<Int64>` of comparison values (a direct Utf8 array is also accepted). Named field must be Utf8 or Int64 respectively; the first values list is used for all rows.", "SELECT array_filter_in(changes, 'kind', ARRAY['transfer', 'mint']) FROM events;"),
    "array_struct_field": ("list, field_name", "List<Utf8>", "Projects an Utf8 field from `List<Struct>`; field_name **must be a non-null Utf8 string literal**, not a column.", "SELECT array_struct_field(changes, 'kind') FROM events;"),
    "to_large_list": ("list", "LargeList<T>", "Accepts List<T>, LargeList<T>, or FixedSizeList<T>; converts offsets to 64-bit (passes through LargeList).", "SELECT to_large_list(items) FROM events;"),
    "_gs_xxhash": ("text", "Utf8", "XXH3-128 digest of UTF-8 text as 32 hex characters; non-cryptographic.", "SELECT _gs_xxhash('hello');"),
    "_gs_keccak256": ("text", "Utf8", "Keccak-256 hash as `0x`-prefixed hex; a `0x`-prefixed input is decoded as hex first.", "SELECT _gs_keccak256('hello');"),
    "_gs_conv_base": ("number, from_base, to_base", "Utf8", "Convert an Utf8 number between bases 2–36; both bases must be either Int32 or Utf8.", "SELECT _gs_conv_base('FF', 16, 10);"),
    "coalesce_meta": ("first[, next, ...]", "FixedSizeBinary(32)", "First non-null value per row, preserving the first field's metadata; **runtime supports only identical FixedSizeBinary(32) arguments**, despite the variadic-any planner signature.", "SELECT coalesce_meta(primary_id, fallback_id) FROM events;"),
    "json_string": ("value", "Utf8", "JSON-serialize an Arrow value; Utf8/LargeUtf8 are JSON-quoted; NULL remains NULL.", "SELECT json_string(payload) FROM events;"),
    "_gs_from_base58": ("text", "Binary", "Decode Base58 Utf8 text; invalid input or NULL produces empty bytes.", "SELECT _gs_from_base58('3yZe7d');"),
    "_gs_hex_to_byte": ("hex_text", "Binary", "Decode Utf8 hex (optional 0x prefix) to bytes; invalid input or NULL produces empty bytes.", "SELECT _gs_hex_to_byte('deadbeef');"),
    "_gs_byte_to_hex": ("bytes", "Utf8", "Hex-encode Binary bytes; NULL remains NULL.", "SELECT _gs_byte_to_hex(_gs_hex_to_byte('deadbeef'));"),
    "reverse_bytes32": ("bytes", "FixedSizeBinary(32)", "Reverse the 32 bytes, preserving input field metadata and NULL values.", "SELECT reverse_bytes32(hash) FROM events;"),
    "_gs_map_to_array_struct": ("map", "List<Struct<key: Utf8, value: Utf8>>", "Convert a `Map<Utf8, Utf8>` with non-nullable key/value fields to an array of key/value structs; expects the exact Arrow map layout.", "SELECT _gs_map_to_array_struct(params) FROM events;"),
    "to_u256": ("value", "U256 (FixedSizeBinary(32))", "Convert Utf8/LargeUtf8 decimal text, Int8/16/32/64, UInt8/16/32/64 or already-encoded 32 bytes to U256; rejects NULL and negative integers.", "SELECT u256_to_string(to_u256('123'));"),
    "u256_to_string": ("value", "Utf8", "Decode U256 big-endian 32-byte value as decimal text.", "SELECT u256_to_string(to_u256('123'));"),
    "to_i256": ("value", "I256 (FixedSizeBinary(32))", "Convert Utf8/LargeUtf8 text, Int8/16/32/64, UInt8/16/32/64 or already-encoded 32 bytes to signed I256; rejects NULL.", "SELECT i256_to_string(to_i256('-123'));"),
    "i256_to_string": ("value", "Utf8", "Decode signed I256 big-endian 32-byte value as decimal text.", "SELECT i256_to_string(to_i256('-123'));"),
    "to_int64": ("value", "Int64 or NULL", "Convert I256 to Int64; negative overflow returns NULL. Nonnegative values up to UInt64 max are bit-reinterpreted as signed Int64; larger values return NULL.", "SELECT to_int64(to_i256('-123'));"),
    "i256_neg": ("value", "I256", "Negate I256; NULL values are rejected.", "SELECT i256_to_string(i256_neg(to_i256('12')));"),
    "i256_abs": ("value", "I256", "Absolute value of I256; NULL values are rejected.", "SELECT i256_to_string(i256_abs(to_i256('-12')));"),
    "uuid7": ("", "Utf8", "Generate a fresh UUID version 7 per row.", "SELECT uuid7();"),
}

for family in ("u256", "i256"):
    for operation in ("add", "sub", "mul", "div", "mod"):
        name = f"{family}_{operation}"
        DETAILS[name] = (
            "left, right", family.upper(),
            f"{operation.capitalize()} two {family.upper()} values; rejects NULL operands. Division and modulo reject zero divisors; arithmetic errors on overflow where applicable.",
            f"SELECT {family}_to_string({name}(to_{family}('12'), to_{family}('3')));",
        )

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


def source_signature(text: str, symbol: str, factory: bool) -> str:
    if factory:
        body = text[text.index("pub fn " + symbol + "("):]
        match = re.search(r"\bcreate_udf\s*\(", body)
        assert match, symbol
        expr = balanced(body, match.end() - 1)
        if "get_input_type()" in expr:
            input_type = text[text.index("fn get_input_type()"):text.index("pub fn " + symbol)]
            if "DataType::Map(" not in input_type or input_type.count("DataType::Utf8") != 2:
                raise ValueError(f"Unexpected map input type for {symbol}")
            return "(Map<Utf8, Utf8>)"
        vec = re.search(r"\bvec!\[([^]]+)\]", expr)
        assert vec, symbol
        return "(" + ", ".join(re.findall(r"DataType::(\w+)", vec.group(1))) + ")"

    if symbol.endswith("Func") and re.search(rf"impl_[ui]256_binary_op!\({symbol},", text):
        kind = "U256" if symbol.startswith("U") else "I256"
        return f"({kind}, {kind})"
    body = text[text.index("impl " + symbol + " {"):]
    match = re.search(r"signature:\s*Signature::(\w+)\s*\(", body)
    if not match:
        raise ValueError(f"Missing signature for {symbol}")
    kind = match.group(1)
    expr = balanced(body, match.end() - 1)
    if kind == "nullary":
        return "()"
    if kind == "variadic_any" or "TypeSignature::VariadicAny" in expr:
        return "(Any, ...)"
    if "TypeSignature::Any(" in expr:
        n = int(re.search(r"TypeSignature::Any\((\d+)\)", expr).group(1))
        return "(" + ", ".join(["Any"] * n) + ")"
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
            elif "U256Type::new()" in value:
                names.append("U256")
            elif "I256Type::new()" in value:
                names.append("I256")
            else:
                typ = re.fullmatch(r"\s*DataType::(\w+)\s*", value)
                if not typ:
                    raise ValueError(f"Unrecognized signature type for {symbol}: {value}")
                names.append(typ.group(1))
        signatures.append("(" + ", ".join(names) + ")")
    if not signatures:
        raise ValueError(f"Unrecognized signature for {symbol}: {expr}")
    return " or ".join(signatures)


def rust_doc_summary(text: str, symbol: str, sql_name: str) -> str:
    """Read adjacent Rust documentation, if the implementation has any."""
    # Some historical docs describe a narrower case, or claim overflow raises
    # an error when the implementation returns NULL. The reviewed annotations
    # below are authoritative for those cases.
    if sql_name in {"array_filter", "array_filter_first", "array_filter_in", "to_int64"}:
        return ""
    declaration = rf"(?:pub struct {symbol}\b|pub fn {symbol}\()"
    match = re.search(r"(?P<docs>(?:^///[^\n]*\n)+)(?:#\[[^\n]*\]\n)*" + declaration, text, re.M)
    if not match and symbol == "ArrayEnumerateFunc":
        match = re.search(r"(?P<docs>(?:^//![^\n]*\n)+)", text, re.M)
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


def registered():
    registry = REGISTRY.read_text()
    block = registry.split("pub fn functions() -> Vec<ScalarUDF>", 1)[1].split("\n        ]", 1)[0]
    entries = re.findall(r"ScalarUDF::from\((?:\w+::)?(\w+)::new\(\)\)|(create_\w+_udf)\(\)", block)
    expressions = [
        line.strip().rstrip(",")
        for line in block.splitlines()
        if line.strip() not in ("{", "vec![") and not line.lstrip().startswith("//")
    ]
    if len(expressions) != len(entries):
        raise ValueError("Unrecognized registration in CommonFunctions::functions()")
    sources = {p.stem: p.read_text() for p in SOURCE.glob("*.rs")}
    for implementation, factory in entries:
        symbol = implementation or factory
        candidates = [(module, text) for module, text in sources.items()
                      if (re.search(rf"\bimpl ScalarUDFImpl for {symbol}\b", text)
                          or re.search(rf"\bimpl_[ui]256_binary_op!\({symbol},", text)
                          or re.search(rf"\bpub fn {symbol}\(", text))]
        if len(candidates) != 1:
            raise ValueError(f"Expected exactly one source for {symbol}, got {[module for module, _ in candidates]}")
        module, text = candidates[0]
        if factory:
            body = text[text.index("pub fn " + symbol + "("):]
            name = re.search(r'\bcreate_udf\s*\(\s*"([\w]+)"', body).group(1)
        else:
            macro = re.search(rf'impl_[ui]256_binary_op!\({symbol},\s*"(\w+)"', text)
            if macro:
                name = macro.group(1)
            else:
                body = text[text.index("impl ScalarUDFImpl for " + symbol):]
                name = re.search(r'fn name\(&self\).*?"(\w+)"', body, re.S).group(1)
        yield name, symbol, module, source_signature(text, symbol, bool(factory)), rust_doc_summary(text, symbol, name)


def render() -> str:
    rows = list(registered())
    names = [name for name, *_ in rows]
    if len(names) != len(set(names)) or set(names) != set(DETAILS):
        raise ValueError(f"Registry/documentation mismatch: missing={set(names) - set(DETAILS)}, stale={set(DETAILS) - set(names)}; duplicates={len(names) - len(set(names))}")
    out = [
        "# Engine SQL UDF reference",
        "",
        "<!-- Generated by python3 scripts/generate-sql-udf-reference.py; do not edit directly. -->",
        "",
        "These functions are registered by `CommonFunctions::functions()` in `crates/streamling-common/src/functions.rs`.",
        "They are available in Streamling SQL transforms alongside DataFusion's built-in SQL functions.",
        "Names beginning with `_gs_` are internal-style names but are registered under exactly those names.",
        "Plugin-provided functions depend on which plugins a deployment loads and are not listed here.",
        "",
        "The **accepted types** column reflects the Rust DataFusion signature, narrowed by",
        "runtime checks where necessary. `Any` in a DataFusion signature means the planner",
        "accepts any type, *not* that the implementation can process all types. The runtime",
        "constraints below each function narrow that signature. `U256` and `I256` are tagged",
        "`FixedSizeBinary(32)` values (big-endian), normally produced by `to_u256` and `to_i256`.",
        "",
        "| Function | Signature | Accepted types | Returns |",
        "| --- | --- | --- | --- |",
    ]
    for name, _, _, signature, _ in rows:
        params, result, _, _ = DETAILS[name]
        accepted = RUNTIME_TYPES.get(name, signature)
        out.append(f"| [`{name}`](#udf-{name.strip('_').replace('_', '-')}) | `{name}({params})` | `{accepted}` | `{result}` |")
    out.append("")
    for name, symbol, module, signature, rust_docs in rows:
        params, result, description, example = DETAILS[name]
        anchor = "udf-" + name.strip("_").replace("_", "-")
        out.extend([f'<a id="{anchor}"></a>', "", f"## `{name}`", "", f"**Signature:** `{name}({params})` → `{result}`. **DataFusion signature:** `{signature}`.", ""])
        if rust_docs:
            out.extend([f"**Rust doc summary:** {rust_docs}", ""])
        out.extend([description, "", "```sql", example, "```", "", f"Source: [`{module}.rs`](../crates/streamling-common/src/functions/{module}.rs) (`{symbol}`).", ""])
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
        print(f"Generated {OUTPUT.relative_to(ROOT)} ({len(DETAILS)} functions)")


if __name__ == "__main__":
    main()
