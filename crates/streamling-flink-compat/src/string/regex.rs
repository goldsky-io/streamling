use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Builder, ListBuilder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::scalar::ScalarValue;
use regex::{NoExpand, Regex};

use super::helpers::{
    finalize_row_evaluation, integer_scalar_to_i64, prepare_row_evaluation, string_scalar_to_owned,
};

pub(super) fn regexp_extract_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(RegexpExtractUdf::new("REGEXP_EXTRACT"))
        .with_aliases(["regexp_extract", "regexpExtract"])
}

pub(super) fn regexp_extract_all_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(RegexpExtractAllUdf::new("REGEXP_EXTRACT_ALL"))
        .with_aliases(["regexp_extract_all", "regexpExtractAll"])
}

pub(super) fn regexp_substr_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(RegexpSubstrUdf::new("REGEXP_SUBSTR"))
        .with_aliases(["regexp_substr", "regexpSubstr"])
}

pub(super) fn regexp_count_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(RegexpCountUdf::new("REGEXP_COUNT"))
        .with_aliases(["regexp_count", "regexpCount"])
}

pub(super) fn regexp_instr_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(RegexpInstrUdf::new("regexp_instr"))
        .with_aliases(["REGEXP_INSTR", "regexpInstr"])
}

pub(super) fn regexp_replace_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(RegexpReplaceUdf::new("REGEXP_REPLACE"))
        .with_aliases(["regexp_replace", "regexpReplace"])
}

#[derive(Debug, Default)]
struct RegexCache {
    compiled: HashMap<String, CachedRegex>,
}

#[derive(Debug)]
enum CachedRegex {
    Valid(Arc<Regex>),
    Invalid,
}

impl RegexCache {
    fn get(&mut self, pattern: &str) -> Option<Arc<Regex>> {
        match self.compiled.get(pattern) {
            Some(CachedRegex::Valid(regex)) => Some(Arc::clone(regex)),
            Some(CachedRegex::Invalid) => None,
            None => match Regex::new(pattern) {
                Ok(regex) => {
                    let arc = Arc::new(regex);
                    self.compiled
                        .insert(pattern.to_string(), CachedRegex::Valid(Arc::clone(&arc)));
                    Some(arc)
                }
                Err(_) => {
                    self.compiled
                        .insert(pattern.to_string(), CachedRegex::Invalid);
                    None
                }
            },
        }
    }
}

pub(super) fn evaluate_regex_extract(
    name: &str,
    args: ScalarFunctionArgs,
    default_index: i64,
    allow_optional_index: bool,
) -> DataFusionResult<ColumnarValue> {
    let expected_arity = if allow_optional_index { 2..=3 } else { 2..=2 };
    if !expected_arity.contains(&args.args.len()) {
        return Err(DataFusionError::Execution(format!(
            "{name} expects {} arguments, received {}",
            if allow_optional_index {
                "two or three"
            } else {
                "exactly two"
            },
            args.args.len()
        )));
    }

    let (all_scalar, arrays, row_count) = prepare_row_evaluation(&args)?;

    let mut builder = StringBuilder::with_capacity(row_count, row_count);
    let mut cache = RegexCache::default();

    for row in 0..row_count {
        let text = ScalarValue::try_from_array(arrays[0].as_ref(), row)?;
        let pattern = ScalarValue::try_from_array(arrays[1].as_ref(), row)?;
        let index = if allow_optional_index && arrays.len() == 3 {
            Some(ScalarValue::try_from_array(arrays[2].as_ref(), row)?)
        } else {
            None
        };

        let value = regex_extract_row(
            name,
            &text,
            &pattern,
            index.as_ref(),
            default_index,
            &mut cache,
        )?;
        if let Some(value) = value {
            builder.append_value(&value);
        } else {
            builder.append_null();
        }
    }

    let array: ArrayRef = Arc::new(builder.finish());
    finalize_row_evaluation(all_scalar, array)
}

pub(super) fn evaluate_regex_extract_all(
    name: &str,
    args: ScalarFunctionArgs,
) -> DataFusionResult<ColumnarValue> {
    if !(2..=3).contains(&args.args.len()) {
        return Err(DataFusionError::Execution(format!(
            "{name} expects two or three arguments, received {}",
            args.args.len()
        )));
    }

    let (all_scalar, arrays, row_count) = prepare_row_evaluation(&args)?;

    let mut builder = ListBuilder::new(StringBuilder::new());
    let mut cache = RegexCache::default();

    for row in 0..row_count {
        let text = ScalarValue::try_from_array(arrays[0].as_ref(), row)?;
        let pattern = ScalarValue::try_from_array(arrays[1].as_ref(), row)?;
        let index = if arrays.len() == 3 {
            Some(ScalarValue::try_from_array(arrays[2].as_ref(), row)?)
        } else {
            None
        };

        match regex_extract_all_row(name, &text, &pattern, index.as_ref(), &mut cache)? {
            None => builder.append_null(),
            Some(values) => {
                let values_builder = builder.values();
                for entry in values {
                    match entry {
                        Some(value) => values_builder.append_value(&value),
                        None => values_builder.append_null(),
                    }
                }
                builder.append(true);
            }
        }
    }

    let array: ArrayRef = Arc::new(builder.finish());
    finalize_row_evaluation(all_scalar, array)
}

pub(super) fn evaluate_regex_count(
    name: &str,
    args: ScalarFunctionArgs,
) -> DataFusionResult<ColumnarValue> {
    if args.args.len() != 2 {
        return Err(DataFusionError::Execution(format!(
            "{name} expects exactly two arguments, received {}",
            args.args.len()
        )));
    }

    let (all_scalar, arrays, row_count) = prepare_row_evaluation(&args)?;

    let mut builder = Int64Builder::with_capacity(row_count);
    let mut cache = RegexCache::default();

    for row in 0..row_count {
        let text = ScalarValue::try_from_array(arrays[0].as_ref(), row)?;
        let pattern = ScalarValue::try_from_array(arrays[1].as_ref(), row)?;

        match regex_count_row(name, &text, &pattern, &mut cache)? {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }

    let array: ArrayRef = Arc::new(builder.finish());
    finalize_row_evaluation(all_scalar, array)
}

pub(super) fn evaluate_regex_instr(
    name: &str,
    args: ScalarFunctionArgs,
) -> DataFusionResult<ColumnarValue> {
    if !(2..=4).contains(&args.args.len()) {
        return Err(DataFusionError::Execution(format!(
            "{name} expects between two and four arguments, received {}",
            args.args.len()
        )));
    }

    let (all_scalar, arrays, row_count) = prepare_row_evaluation(&args)?;

    let mut builder = Int64Builder::with_capacity(row_count);
    let mut cache = RegexCache::default();

    for row in 0..row_count {
        let text = ScalarValue::try_from_array(arrays[0].as_ref(), row)?;
        let pattern = ScalarValue::try_from_array(arrays[1].as_ref(), row)?;
        let start = if arrays.len() >= 3 {
            Some(ScalarValue::try_from_array(arrays[2].as_ref(), row)?)
        } else {
            None
        };
        let occurrence = if arrays.len() >= 4 {
            Some(ScalarValue::try_from_array(arrays[3].as_ref(), row)?)
        } else {
            None
        };

        match regex_instr_row(
            name,
            &text,
            &pattern,
            start.as_ref(),
            occurrence.as_ref(),
            &mut cache,
        )? {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }

    let array: ArrayRef = Arc::new(builder.finish());
    finalize_row_evaluation(all_scalar, array)
}

pub(super) fn evaluate_regex_replace(
    name: &str,
    args: ScalarFunctionArgs,
) -> DataFusionResult<ColumnarValue> {
    if args.args.len() != 3 {
        return Err(DataFusionError::Execution(format!(
            "{name} expects exactly three arguments, received {}",
            args.args.len()
        )));
    }

    let (all_scalar, arrays, row_count) = prepare_row_evaluation(&args)?;

    let mut builder = StringBuilder::with_capacity(row_count, row_count);
    let mut cache = RegexCache::default();

    for row in 0..row_count {
        let text = ScalarValue::try_from_array(arrays[0].as_ref(), row)?;
        let pattern = ScalarValue::try_from_array(arrays[1].as_ref(), row)?;
        let replacement = ScalarValue::try_from_array(arrays[2].as_ref(), row)?;

        match regex_replace_row(name, &text, &pattern, &replacement, &mut cache)? {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }

    let array: ArrayRef = Arc::new(builder.finish());
    finalize_row_evaluation(all_scalar, array)
}

fn regex_extract_row(
    name: &str,
    text: &ScalarValue,
    pattern: &ScalarValue,
    index: Option<&ScalarValue>,
    default_index: i64,
    cache: &mut RegexCache,
) -> DataFusionResult<Option<String>> {
    let text = match string_scalar_to_owned(name, "string", text)? {
        Some(value) => value,
        None => return Ok(None),
    };

    let pattern = match string_scalar_to_owned(name, "pattern", pattern)? {
        Some(value) => value,
        None => return Ok(None),
    };

    let index = match index {
        Some(value) => integer_scalar_to_i64(name, "extract index", value)?,
        None => None,
    }
    .unwrap_or(default_index);

    if index < 0 {
        return Ok(None);
    }

    let Some(regex) = cache.get(&pattern) else {
        return Ok(None);
    };

    let captures = match regex.captures(&text) {
        Some(captures) => captures,
        None => return Ok(None),
    };

    let index_usize = index as usize;
    if index_usize >= captures.len() {
        return Ok(None);
    }

    Ok(captures.get(index_usize).map(|m| m.as_str().to_string()))
}

fn regex_extract_all_row(
    name: &str,
    text: &ScalarValue,
    pattern: &ScalarValue,
    index: Option<&ScalarValue>,
    cache: &mut RegexCache,
) -> DataFusionResult<Option<Vec<Option<String>>>> {
    let text = match string_scalar_to_owned(name, "string", text)? {
        Some(value) => value,
        None => return Ok(None),
    };

    let pattern = match string_scalar_to_owned(name, "pattern", pattern)? {
        Some(value) => value,
        None => return Ok(None),
    };

    let index = match index {
        Some(value) => integer_scalar_to_i64(name, "extract index", value)?,
        None => None,
    }
    .unwrap_or(1);

    if index < 0 {
        return Ok(None);
    }

    let Some(regex) = cache.get(&pattern) else {
        return Ok(None);
    };

    let captures_len = regex.captures_len();
    let max_group_index = captures_len.saturating_sub(1) as i64;
    if index != 0 && index > max_group_index {
        return Ok(None);
    }

    let index_usize = index as usize;
    let mut matches = Vec::new();
    for captures in regex.captures_iter(&text) {
        matches.push(captures.get(index_usize).map(|m| m.as_str().to_string()));
    }

    Ok(Some(matches))
}

fn regex_count_row(
    name: &str,
    text: &ScalarValue,
    pattern: &ScalarValue,
    cache: &mut RegexCache,
) -> DataFusionResult<Option<i64>> {
    let text = match string_scalar_to_owned(name, "string", text)? {
        Some(value) => value,
        None => return Ok(None),
    };

    let pattern = match string_scalar_to_owned(name, "pattern", pattern)? {
        Some(value) => value,
        None => return Ok(None),
    };

    let Some(regex) = cache.get(&pattern) else {
        return Ok(None);
    };

    Ok(Some(regex.find_iter(&text).count() as i64))
}

fn regex_instr_row(
    name: &str,
    text: &ScalarValue,
    pattern: &ScalarValue,
    start: Option<&ScalarValue>,
    occurrence: Option<&ScalarValue>,
    cache: &mut RegexCache,
) -> DataFusionResult<Option<i64>> {
    let text = match string_scalar_to_owned(name, "string", text)? {
        Some(value) => value,
        None => return Ok(None),
    };

    let pattern = match string_scalar_to_owned(name, "pattern", pattern)? {
        Some(value) => value,
        None => return Ok(None),
    };

    if pattern.is_empty() {
        return Ok(Some(0));
    }

    let start_pos = match start {
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

    if start_pos <= 0 {
        return Err(DataFusionError::Execution(format!(
            "{name} expects start position to be positive, received {start_pos}"
        )));
    }
    if occurrence <= 0 {
        return Err(DataFusionError::Execution(format!(
            "{name} expects occurrence to be positive, received {occurrence}"
        )));
    }

    let total_chars = text.chars().count() as i64;
    if start_pos > total_chars {
        return Ok(Some(0));
    }

    let Some(regex) = cache.get(&pattern) else {
        return Ok(None);
    };

    let mut char_offsets: Vec<usize> = text.char_indices().map(|(idx, _)| idx).collect();
    char_offsets.push(text.len());
    let start_index = char_offsets[(start_pos - 1) as usize];
    let mut remaining = occurrence;
    for mat in regex.find_iter(&text[start_index..]) {
        remaining -= 1;
        if remaining == 0 {
            let match_start = start_index + mat.start();
            return Ok(Some(match_start as i64 + 1));
        }
    }

    Ok(Some(0))
}

fn regex_replace_row(
    name: &str,
    text: &ScalarValue,
    pattern: &ScalarValue,
    replacement: &ScalarValue,
    cache: &mut RegexCache,
) -> DataFusionResult<Option<String>> {
    let text = match string_scalar_to_owned(name, "string", text)? {
        Some(value) => value,
        None => return Ok(None),
    };

    let pattern = match string_scalar_to_owned(name, "pattern", pattern)? {
        Some(value) => value,
        None => return Ok(None),
    };

    let Some(regex) = cache.get(&pattern) else {
        return Ok(None);
    };

    let replacement = match string_scalar_to_owned(name, "replacement", replacement)? {
        Some(value) => value,
        None => return Ok(None),
    };

    Ok(Some(
        regex
            .replace_all(&text, NoExpand(replacement.as_str()))
            .into_owned(),
    ))
}

#[derive(Debug)]
struct RegexpExtractUdf {
    signature: Signature,
    name: &'static str,
    default_group: i64,
    allow_optional_group: bool,
}

impl RegexpExtractUdf {
    fn new(name: &'static str) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int64]),
                ],
                Volatility::Immutable,
            ),
            name,
            default_group: 0,
            allow_optional_group: true,
        }
    }
}

impl ScalarUDFImpl for RegexpExtractUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        evaluate_regex_extract(
            self.name(),
            args,
            self.default_group,
            self.allow_optional_group,
        )
    }
}

#[derive(Debug)]
struct RegexpExtractAllUdf {
    signature: Signature,
    name: &'static str,
}

impl RegexpExtractAllUdf {
    fn new(name: &'static str) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int64]),
                ],
                Volatility::Immutable,
            ),
            name,
        }
    }
}

impl ScalarUDFImpl for RegexpExtractAllUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        self.name
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
        evaluate_regex_extract_all(self.name(), args)
    }
}

#[derive(Debug)]
struct RegexpSubstrUdf {
    signature: Signature,
    name: &'static str,
}

impl RegexpSubstrUdf {
    fn new(name: &'static str) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int64]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8,
                        DataType::Utf8,
                        DataType::Int64,
                        DataType::Int64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            name,
        }
    }
}

impl ScalarUDFImpl for RegexpSubstrUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        evaluate_regex_extract(self.name(), args, 0, false)
    }
}

#[derive(Debug)]
struct RegexpCountUdf {
    signature: Signature,
    name: &'static str,
}

impl RegexpCountUdf {
    fn new(name: &'static str) -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8])],
                Volatility::Immutable,
            ),
            name,
        }
    }
}

impl ScalarUDFImpl for RegexpCountUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        evaluate_regex_count(self.name(), args)
    }
}

#[derive(Debug)]
struct RegexpInstrUdf {
    signature: Signature,
    name: &'static str,
}

impl RegexpInstrUdf {
    fn new(name: &'static str) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            name,
        }
    }
}

impl ScalarUDFImpl for RegexpInstrUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        evaluate_regex_instr(self.name(), args)
    }
}

#[derive(Debug)]
struct RegexpReplaceUdf {
    signature: Signature,
    name: &'static str,
}

impl RegexpReplaceUdf {
    fn new(name: &'static str) -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Utf8,
                ])],
                Volatility::Immutable,
            ),
            name,
        }
    }
}

impl ScalarUDFImpl for RegexpReplaceUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        evaluate_regex_replace(self.name(), args)
    }
}
