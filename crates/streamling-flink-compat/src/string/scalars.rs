use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BinaryBuilder, Int64Builder, ListBuilder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::scalar::ScalarValue;
use url::Url;

use super::helpers::{
    finalize_row_evaluation, integer_scalar_to_i64, prepare_row_evaluation,
    scalar_to_utf8_optional, string_scalar_to_owned,
};

pub(super) fn instr_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(InstrUdf::new()).with_aliases(["instr", "INSTR"])
}

pub(super) fn locate_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(LocateUdf::new()).with_aliases(["locate", "LOCATE"])
}

pub(super) fn bin_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(BinUdf::new()).with_aliases(["bin", "BIN"])
}

pub(super) fn elt_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(EltUdf::new()).with_aliases(["elt", "ELT"])
}

pub(super) fn parse_url_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ParseUrlUdf::new()).with_aliases(["parse_url", "PARSE_URL"])
}

pub(super) fn split_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(SplitUdf::new()).with_aliases(["split", "SPLIT"])
}

pub(super) fn split_index_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(SplitIndexUdf::new()).with_aliases(["SPLIT_INDEX", "split_index"])
}

pub(super) fn translate_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(TranslateUdf::new()).with_aliases([
        "translate",
        "TRANSLATE",
        "translate3",
        "TRANSLATE3",
    ])
}

pub(super) fn unhex_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(UnhexUdf::new()).with_aliases(["unhex", "UNHEX"])
}

pub(super) fn url_encode_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(UrlEncodeUdf::new()).with_aliases(["url_encode", "URL_ENCODE"])
}

pub(super) fn url_decode_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(UrlDecodeUdf::new()).with_aliases(["url_decode", "URL_DECODE"])
}

fn evaluate_instr(name: &str, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
    if !(2..=4).contains(&args.args.len()) {
        return Err(DataFusionError::Execution(format!(
            "{name} expects between two and four arguments, received {}",
            args.args.len()
        )));
    }

    let (all_scalar, arrays, row_count) = prepare_row_evaluation(&args)?;

    let mut builder = Int64Builder::with_capacity(row_count);

    for row in 0..row_count {
        let text = ScalarValue::try_from_array(arrays[0].as_ref(), row)?;
        let substring = ScalarValue::try_from_array(arrays[1].as_ref(), row)?;
        let start = if arrays.len() >= 3 {
            Some(ScalarValue::try_from_array(arrays[2].as_ref(), row)?)
        } else {
            None
        };
        let occurrence = if arrays.len() == 4 {
            Some(ScalarValue::try_from_array(arrays[3].as_ref(), row)?)
        } else {
            None
        };

        match instr_row(name, &text, &substring, start.as_ref(), occurrence.as_ref())? {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }

    let array: ArrayRef = Arc::new(builder.finish());
    finalize_row_evaluation(all_scalar, array)
}

fn evaluate_locate(name: &str, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
    if !(2..=3).contains(&args.args.len()) {
        return Err(DataFusionError::Execution(format!(
            "{name} expects two or three arguments, received {}",
            args.args.len()
        )));
    }

    let (all_scalar, arrays, row_count) = prepare_row_evaluation(&args)?;

    let mut builder = Int64Builder::with_capacity(row_count);

    for row in 0..row_count {
        let substring = ScalarValue::try_from_array(arrays[0].as_ref(), row)?;
        let text = ScalarValue::try_from_array(arrays[1].as_ref(), row)?;
        let start = if arrays.len() == 3 {
            Some(ScalarValue::try_from_array(arrays[2].as_ref(), row)?)
        } else {
            None
        };

        match locate_row(name, &substring, &text, start.as_ref())? {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }

    let array: ArrayRef = Arc::new(builder.finish());
    finalize_row_evaluation(all_scalar, array)
}

fn evaluate_bin(name: &str, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
    if args.args.len() != 1 {
        return Err(DataFusionError::Execution(format!(
            "{name} expects exactly one argument, received {}",
            args.args.len()
        )));
    }

    let (all_scalar, arrays, row_count) = prepare_row_evaluation(&args)?;

    let mut builder = StringBuilder::with_capacity(row_count, row_count);

    for row in 0..row_count {
        let value = ScalarValue::try_from_array(arrays[0].as_ref(), row)?;
        match bin_scalar(name, &value)? {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }

    let array: ArrayRef = Arc::new(builder.finish());
    finalize_row_evaluation(all_scalar, array)
}

fn evaluate_elt(name: &str, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
    if args.args.len() < 2 {
        return Err(DataFusionError::Execution(format!(
            "{name} expects at least two arguments, received {}",
            args.args.len()
        )));
    }

    let (all_scalar, arrays, row_count) = prepare_row_evaluation(&args)?;

    let mut builder = StringBuilder::with_capacity(row_count, row_count);

    for row in 0..row_count {
        let index_value = ScalarValue::try_from_array(arrays[0].as_ref(), row)?;
        let idx = match integer_scalar_to_i64(name, "index", &index_value)? {
            Some(v) => v,
            None => {
                builder.append_null();
                continue;
            }
        };

        if idx < 1 || idx > (arrays.len() - 1) as i64 {
            builder.append_null();
            continue;
        }

        let candidate = ScalarValue::try_from_array(arrays[idx as usize].as_ref(), row)?;
        match scalar_to_utf8_optional(name, "value", &candidate)? {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }

    let array: ArrayRef = Arc::new(builder.finish());
    finalize_row_evaluation(all_scalar, array)
}

fn evaluate_parse_url(name: &str, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
    if !(2..=3).contains(&args.args.len()) {
        return Err(DataFusionError::Execution(format!(
            "{name} expects two or three arguments, received {}",
            args.args.len()
        )));
    }

    let (all_scalar, arrays, row_count) = prepare_row_evaluation(&args)?;

    let mut builder = StringBuilder::with_capacity(row_count, row_count);

    for row in 0..row_count {
        let url_value = ScalarValue::try_from_array(arrays[0].as_ref(), row)?;
        let part_value = ScalarValue::try_from_array(arrays[1].as_ref(), row)?;
        let key_value = if arrays.len() == 3 {
            Some(ScalarValue::try_from_array(arrays[2].as_ref(), row)?)
        } else {
            None
        };

        let url = match string_scalar_to_owned(name, "url", &url_value)? {
            Some(value) => value,
            None => {
                builder.append_null();
                continue;
            }
        };

        let part = match string_scalar_to_owned(name, "part", &part_value)? {
            Some(value) => value.to_ascii_uppercase(),
            None => {
                builder.append_null();
                continue;
            }
        };

        let key = match key_value {
            Some(ref scalar) => string_scalar_to_owned(name, "key", scalar)?,
            None => None,
        };

        match parse_url_part(&url, &part, key.as_deref())? {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }

    let array: ArrayRef = Arc::new(builder.finish());
    finalize_row_evaluation(all_scalar, array)
}

fn evaluate_split(name: &str, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
    if args.args.len() != 2 {
        return Err(DataFusionError::Execution(format!(
            "{name} expects exactly two arguments, received {}",
            args.args.len()
        )));
    }

    let (all_scalar, arrays, row_count) = prepare_row_evaluation(&args)?;

    let mut builder = ListBuilder::new(StringBuilder::new());

    for row in 0..row_count {
        let string_value = ScalarValue::try_from_array(arrays[0].as_ref(), row)?;
        let delimiter_value = ScalarValue::try_from_array(arrays[1].as_ref(), row)?;

        match split_row(name, &string_value, &delimiter_value)? {
            None => builder.append_null(),
            Some(segments) => {
                let values_builder = builder.values();
                for segment in segments {
                    values_builder.append_value(&segment);
                }
                builder.append(true);
            }
        }
    }

    let array: ArrayRef = Arc::new(builder.finish());
    finalize_row_evaluation(all_scalar, array)
}

fn evaluate_split_index(name: &str, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
    if args.args.len() != 3 {
        return Err(DataFusionError::Execution(format!(
            "{name} expects exactly three arguments, received {}",
            args.args.len()
        )));
    }

    let (all_scalar, arrays, row_count) = prepare_row_evaluation(&args)?;

    let mut builder = StringBuilder::with_capacity(row_count, row_count);

    for row in 0..row_count {
        let string_value = ScalarValue::try_from_array(arrays[0].as_ref(), row)?;
        let delimiter_value = ScalarValue::try_from_array(arrays[1].as_ref(), row)?;
        let index_value = ScalarValue::try_from_array(arrays[2].as_ref(), row)?;

        let s = match string_scalar_to_owned(name, "string", &string_value)? {
            Some(v) => v,
            None => {
                builder.append_null();
                continue;
            }
        };
        let delim = match string_scalar_to_owned(name, "delimiter", &delimiter_value)? {
            Some(v) => v,
            None => {
                builder.append_null();
                continue;
            }
        };
        let idx = match integer_scalar_to_i64(name, "index", &index_value)? {
            Some(v) => v,
            None => {
                builder.append_null();
                continue;
            }
        };

        let parts: Vec<&str> = if delim.is_empty() {
            s.split_terminator('\0').collect() // won't match, force no parts
        } else {
            s.split(&delim).collect()
        };

        // Flink-style: 0-based index; return NULL if out-of-range
        if idx < 0 || (idx as usize) >= parts.len() {
            builder.append_null();
        } else {
            builder.append_value(parts[idx as usize]);
        }
    }

    let array: ArrayRef = Arc::new(builder.finish());
    finalize_row_evaluation(all_scalar, array)
}

fn evaluate_translate(name: &str, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
    if args.args.len() != 3 {
        return Err(DataFusionError::Execution(format!(
            "{name} expects exactly three arguments, received {}",
            args.args.len()
        )));
    }

    let (all_scalar, arrays, row_count) = prepare_row_evaluation(&args)?;

    let mut builder = StringBuilder::with_capacity(row_count, row_count);

    for row in 0..row_count {
        let expr_value = ScalarValue::try_from_array(arrays[0].as_ref(), row)?;
        let from_value = ScalarValue::try_from_array(arrays[1].as_ref(), row)?;
        let to_value = ScalarValue::try_from_array(arrays[2].as_ref(), row)?;

        let expr = match string_scalar_to_owned(name, "expr", &expr_value)? {
            Some(value) => value,
            None => {
                builder.append_null();
                continue;
            }
        };

        let from = match string_scalar_to_owned(name, "fromStr", &from_value)? {
            Some(value) => value,
            None => {
                builder.append_value(&expr);
                continue;
            }
        };

        if expr.is_empty() || from.is_empty() {
            builder.append_value(&expr);
            continue;
        }

        let to = string_scalar_to_owned(name, "toStr", &to_value)?.unwrap_or_default();
        let translated = translate_string(&expr, &from, &to);
        builder.append_value(&translated);
    }

    let array: ArrayRef = Arc::new(builder.finish());
    finalize_row_evaluation(all_scalar, array)
}

#[derive(Debug, Clone)]
struct SplitIndexUdf(ScalarUDF);

impl SplitIndexUdf {
    fn new() -> Self {
        Self(ScalarUDF::new_from_impl(SelfImpl::new()))
    }
}

impl ScalarUDFImpl for SplitIndexUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "split_index"
    }
    fn signature(&self) -> &Signature {
        self.0.signature()
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        evaluate_split_index(self.name(), args)
    }
}

#[derive(Debug, Clone)]
struct SelfImpl;

impl SelfImpl {
    fn new() -> Self {
        Self
    }
}

impl ScalarUDFImpl for SelfImpl {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "split_index"
    }
    fn signature(&self) -> &Signature {
        // Store the signature in a function-local static using a raw pointer to avoid extra deps
        #[allow(clippy::declare_interior_mutable_const)]
        const INIT: Option<Signature> = None;
        static mut SIG_PTR: *const Option<Signature> = std::ptr::null();
        unsafe {
            if SIG_PTR.is_null() {
                SIG_PTR = Box::into_raw(Box::new(INIT));
                let sig = Signature::one_of(vec![TypeSignature::Any(3)], Volatility::Volatile);
                let mut sig_box = Box::from_raw(SIG_PTR as *mut Option<Signature>);
                *sig_box = Some(sig);
                SIG_PTR = Box::into_raw(sig_box);
            }
            // SAFETY: initialized above
            (*SIG_PTR).as_ref().unwrap()
        }
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        evaluate_split_index(self.name(), args)
    }
}

fn evaluate_unhex(name: &str, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
    if args.args.len() != 1 {
        return Err(DataFusionError::Execution(format!(
            "{name} expects exactly one argument, received {}",
            args.args.len()
        )));
    }

    let (all_scalar, arrays, row_count) = prepare_row_evaluation(&args)?;

    let mut builder = BinaryBuilder::new();

    for row in 0..row_count {
        let expr_value = ScalarValue::try_from_array(arrays[0].as_ref(), row)?;

        match unhex_row(name, &expr_value)? {
            Some(bytes) => builder.append_value(&bytes),
            None => builder.append_null(),
        }
    }

    let array: ArrayRef = Arc::new(builder.finish());
    finalize_row_evaluation(all_scalar, array)
}

fn evaluate_url_encode(name: &str, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
    if args.args.len() != 1 {
        return Err(DataFusionError::Execution(format!(
            "{name} expects exactly one argument, received {}",
            args.args.len()
        )));
    }

    let (all_scalar, arrays, row_count) = prepare_row_evaluation(&args)?;

    let mut builder = StringBuilder::with_capacity(row_count, row_count);

    for row in 0..row_count {
        let expr_value = ScalarValue::try_from_array(arrays[0].as_ref(), row)?;

        match url_encode_row(name, &expr_value)? {
            Some(value) => builder.append_value(&value),
            None => builder.append_null(),
        }
    }

    let array: ArrayRef = Arc::new(builder.finish());
    finalize_row_evaluation(all_scalar, array)
}

fn evaluate_url_decode(name: &str, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
    if args.args.len() != 1 {
        return Err(DataFusionError::Execution(format!(
            "{name} expects exactly one argument, received {}",
            args.args.len()
        )));
    }

    let (all_scalar, arrays, row_count) = prepare_row_evaluation(&args)?;

    let mut builder = StringBuilder::with_capacity(row_count, row_count);

    for row in 0..row_count {
        let expr_value = ScalarValue::try_from_array(arrays[0].as_ref(), row)?;

        match url_decode_row(name, &expr_value)? {
            Some(value) => builder.append_value(&value),
            None => builder.append_null(),
        }
    }

    let array: ArrayRef = Arc::new(builder.finish());
    finalize_row_evaluation(all_scalar, array)
}

fn instr_row(
    name: &str,
    text: &ScalarValue,
    substring: &ScalarValue,
    start: Option<&ScalarValue>,
    occurrence: Option<&ScalarValue>,
) -> DataFusionResult<Option<i64>> {
    let text = match string_scalar_to_owned(name, "string", text)? {
        Some(value) => value,
        None => return Ok(None),
    };

    let substring = match string_scalar_to_owned(name, "substring", substring)? {
        Some(value) => value,
        None => return Ok(None),
    };

    let start = match start {
        Some(value) => match integer_scalar_to_i64(name, "start position", value)? {
            Some(v) => v,
            None => return Ok(None),
        },
        None => 1,
    };

    let occurrence = match occurrence {
        Some(value) => match integer_scalar_to_i64(name, "occurrence", value)? {
            Some(v) => v,
            None => return Ok(None),
        },
        None => 1,
    };

    if occurrence <= 0 {
        return Ok(None);
    }

    Ok(Some(instr_core(&text, &substring, start, occurrence)))
}

fn instr_core(text: &str, pattern: &str, start: i64, occurrence: i64) -> i64 {
    if pattern.is_empty() {
        return instr_empty_pattern(text, start, occurrence);
    }

    let total_chars = text.chars().count() as i64;
    if total_chars == 0 {
        return 0;
    }

    if start == 0 {
        return 0;
    }

    if start > 0 {
        return instr_forward(text, pattern, start, occurrence);
    }

    let reversed_text: String = text.chars().rev().collect();
    let reversed_pattern: String = pattern.chars().rev().collect();
    let pos = instr_forward(&reversed_text, &reversed_pattern, -start, occurrence);
    if pos == 0 {
        0
    } else {
        let pattern_chars = pattern.chars().count() as i64;
        total_chars + 2 - pos - pattern_chars
    }
}

fn instr_forward(text: &str, pattern: &str, start: i64, occurrence: i64) -> i64 {
    let total_chars = text.chars().count() as i64;
    if start > total_chars {
        return 0;
    }

    let mut char_offsets: Vec<usize> = text.char_indices().map(|(idx, _)| idx).collect();
    char_offsets.push(text.len());

    let mut remaining = occurrence;
    let mut current_char = (start - 1) as usize;
    let mut byte_start = char_offsets
        .get(current_char)
        .copied()
        .unwrap_or(text.len());

    while remaining > 0 {
        if byte_start > text.len() {
            return 0;
        }

        match text[byte_start..].find(pattern) {
            Some(byte_pos) => {
                let match_byte = byte_start + byte_pos;
                let match_char = match char_offsets.binary_search(&match_byte) {
                    Ok(pos) => pos,
                    Err(_) => {
                        return 0;
                    }
                };

                if remaining == 1 {
                    return match_char as i64 + 1;
                }

                remaining -= 1;
                current_char = match_char + 1;
                byte_start = char_offsets
                    .get(current_char)
                    .copied()
                    .unwrap_or(text.len());
            }
            None => return 0,
        }
    }

    0
}

fn instr_empty_pattern(text: &str, start: i64, occurrence: i64) -> i64 {
    let total_chars = text.chars().count() as i64 + 1;

    if start == 0 {
        return 0;
    }

    if start > 0 {
        let mut position = start.max(1);
        position = position.min(total_chars);
        let result = position + (occurrence - 1);
        return result.min(total_chars);
    }

    let mut position = total_chars + start;
    if position < 1 {
        position = 1;
    }
    let result = position - (occurrence - 1);
    if result < 1 { 1 } else { result }
}

fn locate_row(
    name: &str,
    substring: &ScalarValue,
    text: &ScalarValue,
    start: Option<&ScalarValue>,
) -> DataFusionResult<Option<i64>> {
    let substring = match string_scalar_to_owned(name, "substring", substring)? {
        Some(value) => value,
        None => return Ok(None),
    };

    let text = match string_scalar_to_owned(name, "string", text)? {
        Some(value) => value,
        None => return Ok(None),
    };

    let start = match start {
        Some(value) => match integer_scalar_to_i64(name, "start position", value)? {
            Some(v) => v,
            None => return Ok(None),
        },
        None => 1,
    };

    if start < 1 {
        return Ok(Some(0));
    }

    if substring.is_empty() {
        let total_chars = text.chars().count() as i64 + 1;
        let pos = start.min(total_chars);
        return Ok(Some(pos));
    }

    Ok(Some(instr_core(&text, &substring, start, 1)))
}

fn bin_scalar(name: &str, value: &ScalarValue) -> DataFusionResult<Option<String>> {
    match value {
        ScalarValue::Null => Ok(None),
        ScalarValue::Int8(opt) => Ok(opt.map(|v| format_integer(v as i128))),
        ScalarValue::Int16(opt) => Ok(opt.map(|v| format_integer(v as i128))),
        ScalarValue::Int32(opt) => Ok(opt.map(|v| format_integer(v as i128))),
        ScalarValue::Int64(opt) => Ok(opt.map(|v| format_integer(v as i128))),
        ScalarValue::UInt8(opt) => Ok(opt.map(|v| format_uinteger(v as u128))),
        ScalarValue::UInt16(opt) => Ok(opt.map(|v| format_uinteger(v as u128))),
        ScalarValue::UInt32(opt) => Ok(opt.map(|v| format_uinteger(v as u128))),
        ScalarValue::UInt64(opt) => Ok(opt.map(|v| format_uinteger(v as u128))),
        ScalarValue::Dictionary(_, inner) => bin_scalar(name, inner),
        other => Err(DataFusionError::Execution(format!(
            "{name} expects an integer argument, received {other:?}"
        ))),
    }
}

fn format_integer(value: i128) -> String {
    let magnitude = if value < 0 {
        (value as u128).wrapping_neg()
    } else {
        value as u128
    };
    let digits = format_uinteger(magnitude);
    if value < 0 {
        format!("-{digits}")
    } else {
        digits
    }
}

fn format_uinteger(value: u128) -> String {
    format!("{:b}", value)
}

fn split_row(
    func: &str,
    string: &ScalarValue,
    delimiter: &ScalarValue,
) -> DataFusionResult<Option<Vec<String>>> {
    let string = match string_scalar_to_owned(func, "string", string)? {
        Some(value) => value,
        None => return Ok(None),
    };

    let delimiter = match string_scalar_to_owned(func, "delimiter", delimiter)? {
        Some(value) => value,
        None => return Ok(None),
    };

    if delimiter.is_empty() {
        let segments = string.chars().map(|ch| ch.to_string()).collect();
        return Ok(Some(segments));
    }

    Ok(Some(split_preserve_all_tokens(&string, &delimiter)))
}

fn split_preserve_all_tokens(string: &str, delimiter: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut index = 0;
    while let Some(pos) = string[index..].find(delimiter) {
        let absolute = index + pos;
        result.push(string[index..absolute].to_string());
        index = absolute + delimiter.len();
    }
    result.push(string[index..].to_string());
    result
}

fn translate_string(expr: &str, from: &str, to: &str) -> String {
    let mut dict = HashMap::new();
    let mut to_iter = to.chars();

    for ch in from.chars() {
        let replacement = to_iter.next();
        if dict.contains_key(&ch) {
            continue;
        }
        let value = replacement.map(|c| c.to_string()).unwrap_or_default();
        dict.insert(ch, value);
    }

    let mut result = String::with_capacity(expr.len());
    for ch in expr.chars() {
        if let Some(replacement) = dict.get(&ch) {
            result.push_str(replacement);
        } else {
            result.push(ch);
        }
    }
    result
}

fn unhex_row(func: &str, value: &ScalarValue) -> DataFusionResult<Option<Vec<u8>>> {
    let text = match string_scalar_to_owned(func, "expr", value)? {
        Some(value) => value,
        None => return Ok(None),
    };

    Ok(unhex_string(&text))
}

fn unhex_string(value: &str) -> Option<Vec<u8>> {
    let bytes = value.as_bytes();
    if bytes.is_empty() {
        return Some(Vec::new());
    }

    let mut out = vec![0u8; bytes.len().div_ceil(2)];

    if bytes.len() >= 2 {
        let mut i = bytes.len() - 2;
        let mut j = out.len() - 1;
        loop {
            let hi = hex_value(bytes[i])?;
            let lo = hex_value(bytes[i + 1])?;
            out[j] = (hi << 4) | lo;

            if i < 2 {
                break;
            }
            i -= 2;
            j -= 1;
        }

        if bytes.len().is_multiple_of(2) {
            return Some(out);
        }

        hex_value(bytes[0])?;
        return Some(out);
    }

    hex_value(bytes[0])?;

    Some(out)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn url_encode_row(func: &str, value: &ScalarValue) -> DataFusionResult<Option<String>> {
    let text = match string_scalar_to_owned(func, "value", value)? {
        Some(value) => value,
        None => return Ok(None),
    };

    Ok(Some(url_encode_string(&text)))
}

fn url_encode_string(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'*' => {
                result.push(byte as char);
            }
            b' ' => result.push('+'),
            _ => {
                result.push('%');
                write!(&mut result, "{:02X}", byte).expect("formatting to succeed");
            }
        }
    }
    result
}

fn url_decode_row(func: &str, value: &ScalarValue) -> DataFusionResult<Option<String>> {
    let text = match string_scalar_to_owned(func, "value", value)? {
        Some(value) => value,
        None => return Ok(None),
    };

    Ok(url_decode_string(&text))
}

fn url_decode_string(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut result = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                result.push(b' ');
                index += 1;
            }
            b'%' => {
                if index + 2 >= bytes.len() {
                    return None;
                }
                let hi = hex_value(bytes[index + 1])?;
                let lo = hex_value(bytes[index + 2])?;
                result.push((hi << 4) | lo);
                index += 3;
            }
            other => {
                result.push(other);
                index += 1;
            }
        }
    }

    String::from_utf8(result).ok()
}

fn parse_url_part(url: &str, part: &str, key: Option<&str>) -> DataFusionResult<Option<String>> {
    let parsed = match Url::parse(url) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };

    let result = match part {
        "HOST" => parsed.host_str().map(|s| s.to_string()),
        "PATH" => Some(parsed.path().to_string()),
        "QUERY" => match key {
            Some(k) => {
                let mut pairs = parsed.query_pairs();
                pairs
                    .find(|(query_key, _)| query_key == k)
                    .map(|(_, v)| v.into_owned())
            }
            None => parsed.query().map(|s| s.to_string()),
        },
        "REF" => parsed.fragment().map(|s| s.to_string()),
        "PROTOCOL" => Some(parsed.scheme().to_string()),
        "FILE" => {
            let mut file = parsed.path().to_string();
            if let Some(query) = parsed.query() {
                file.push('?');
                file.push_str(query);
            }
            Some(file)
        }
        "AUTHORITY" => {
            let mut authority = String::new();
            if !parsed.username().is_empty() {
                authority.push_str(parsed.username());
                if let Some(password) = parsed.password() {
                    authority.push(':');
                    authority.push_str(password);
                }
                authority.push('@');
            }
            if let Some(host) = parsed.host_str() {
                authority.push_str(host);
            }
            if let Some(port) = parsed.port() {
                authority.push(':');
                authority.push_str(&port.to_string());
            }
            if authority.is_empty() {
                None
            } else {
                Some(authority)
            }
        }
        "USERINFO" => {
            if parsed.username().is_empty() {
                None
            } else {
                let mut userinfo = parsed.username().to_string();
                if let Some(password) = parsed.password() {
                    userinfo.push(':');
                    userinfo.push_str(password);
                }
                Some(userinfo)
            }
        }
        _ => None,
    };

    Ok(result)
}

#[derive(Debug)]
struct InstrUdf {
    signature: Signature,
}

impl InstrUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::VariadicAny], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for InstrUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "INSTR"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        evaluate_instr(self.name(), args)
    }
}

#[derive(Debug)]
struct LocateUdf {
    signature: Signature,
}

impl LocateUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::VariadicAny], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for LocateUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "LOCATE"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        evaluate_locate(self.name(), args)
    }
}

#[derive(Debug)]
struct BinUdf {
    signature: Signature,
}

impl BinUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::VariadicAny], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for BinUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "BIN"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        evaluate_bin(self.name(), args)
    }
}

#[derive(Debug)]
struct EltUdf {
    signature: Signature,
}

impl EltUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::VariadicAny], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for EltUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "ELT"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        evaluate_elt(self.name(), args)
    }
}

#[derive(Debug)]
struct ParseUrlUdf {
    signature: Signature,
}

impl ParseUrlUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::VariadicAny], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ParseUrlUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "PARSE_URL"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        evaluate_parse_url(self.name(), args)
    }
}

#[derive(Debug)]
struct SplitUdf {
    signature: Signature,
}

impl SplitUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::VariadicAny], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SplitUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "SPLIT"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        evaluate_split(self.name(), args)
    }
}

#[derive(Debug)]
struct TranslateUdf {
    signature: Signature,
}

impl TranslateUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::VariadicAny], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TranslateUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "TRANSLATE3"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        evaluate_translate(self.name(), args)
    }
}

#[derive(Debug)]
struct UnhexUdf {
    signature: Signature,
}

impl UnhexUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::VariadicAny], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for UnhexUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "UNHEX"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        evaluate_unhex(self.name(), args)
    }
}

#[derive(Debug)]
struct UrlEncodeUdf {
    signature: Signature,
}

impl UrlEncodeUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::VariadicAny], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for UrlEncodeUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "URL_ENCODE"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        evaluate_url_encode(self.name(), args)
    }
}

#[derive(Debug)]
struct UrlDecodeUdf {
    signature: Signature,
}

impl UrlDecodeUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::VariadicAny], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for UrlDecodeUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "URL_DECODE"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        evaluate_url_decode(self.name(), args)
    }
}
