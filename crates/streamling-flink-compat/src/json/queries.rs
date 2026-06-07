use std::any::Any;
use std::fmt::Write as FmtWrite;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanBuilder, StringArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::scalar::ScalarValue;

use simd_json::BorrowedValue;
use simd_json::prelude::Writable;

use super::shared::{finalize_row_evaluation, json_quote, normalise_keyword, parse_utf8_literal};

pub(crate) fn json_exists_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(JsonExistsUdf::new())
}

pub(crate) fn json_value_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(JsonValueUdf::new())
}

pub(crate) fn json_query_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(JsonQueryUdf::new())
}

#[derive(Debug)]
struct JsonExistsUdf {
    signature: Signature,
}

impl JsonExistsUdf {
    fn new() -> Self {
        Self {
            signature: Signature::variadic(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonExistsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "JSON_EXISTS"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if args.args.len() != 2 && args.args.len() != 3 {
            return Err(DataFusionError::Execution(format!(
                "JSON_EXISTS expects 2 or 3 arguments, got {}",
                args.args.len()
            )));
        }

        let json_input = &args.args[0];
        let path_input = &args.args[1];
        let options = JsonExistsOptions::from_args(&args.args)?;

        match (json_input, path_input) {
            (ColumnarValue::Scalar(json_scalar), ColumnarValue::Scalar(path_scalar)) => {
                let result = json_exists_scalar_with_options(json_scalar, path_scalar, &options)?;
                Ok(ColumnarValue::Scalar(result))
            }
            _ => {
                let json_array = get_utf8_array(json_input)?;
                let path_array = match path_input {
                    ColumnarValue::Scalar(path_scalar) => ParsedPath::from_scalar(path_scalar)?,
                    ColumnarValue::Array(arr) => ParsedPath::from_array(arr.clone())?,
                };

                let mut builder = BooleanBuilder::with_capacity(json_array.len());
                let mut scratch = Vec::new();

                for row in 0..json_array.len() {
                    if json_array.is_null(row) {
                        builder.append_null();
                        continue;
                    }

                    let json_value = json_array.value(row);
                    let maybe_path = path_array.value_for_row(row)?;

                    let status = match maybe_path {
                        None => JsonPathStatus::Missing,
                        Some(path) => evaluate_json_path_status(json_value, path, &mut scratch),
                    };

                    match apply_json_exists_status(status, &options)? {
                        Some(value) => builder.append_value(value),
                        None => builder.append_null(),
                    }
                }

                let array: ArrayRef = Arc::new(builder.finish());
                finalize_row_evaluation(false, array)
            }
        }
    }
}

#[derive(Debug)]
struct JsonValueUdf {
    signature: Signature,
}

impl JsonValueUdf {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonValueUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "JSON_VALUE"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if args.args.len() != 2 && args.args.len() != 7 {
            return Err(DataFusionError::Execution(format!(
                "JSON_VALUE expects 2 or 7 arguments, got {}",
                args.args.len()
            )));
        }

        let json_input = &args.args[0];
        let path_input = &args.args[1];
        let options = JsonValueOptions::from_args(&args.args)?;

        match (json_input, path_input) {
            (ColumnarValue::Scalar(json_scalar), ColumnarValue::Scalar(path_scalar)) => {
                let value = json_value_scalar_with_options(json_scalar, path_scalar, &options)?;
                Ok(ColumnarValue::Scalar(value))
            }
            _ => {
                let json_array = get_utf8_array(json_input)?;
                let path_array = match path_input {
                    ColumnarValue::Scalar(path_scalar) => ParsedPath::from_scalar(path_scalar)?,
                    ColumnarValue::Array(arr) => ParsedPath::from_array(arr.clone())?,
                };

                let mut builder = StringBuilder::with_capacity(json_array.len(), json_array.len());
                let mut scratch = Vec::new();

                for row in 0..json_array.len() {
                    if json_array.is_null(row) {
                        builder.append_null();
                        continue;
                    }

                    let json_value = json_array.value(row);
                    let maybe_path = path_array.value_for_row(row)?;

                    let status = match maybe_path {
                        None => JsonPathStatus::Missing,
                        Some(path) => evaluate_json_path_status(json_value, path, &mut scratch),
                    };

                    match apply_json_value_status(status, &options)? {
                        Some(value) => builder.append_value(value),
                        None => builder.append_null(),
                    }
                }

                let array: ArrayRef = Arc::new(builder.finish());
                finalize_row_evaluation(false, array)
            }
        }
    }
}

#[derive(Debug)]
struct JsonQueryUdf {
    signature: Signature,
}

impl JsonQueryUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8,
                        DataType::Utf8,
                        DataType::Utf8,
                        DataType::Utf8,
                        DataType::Utf8,
                        DataType::Utf8,
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for JsonQueryUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "JSON_QUERY"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let options = JsonQueryOptions::from_args(&args.args)?;

        let json_input = args
            .args
            .first()
            .ok_or_else(|| DataFusionError::Execution("JSON_QUERY requires a JSON input".into()))?;
        let path_input = args
            .args
            .get(1)
            .ok_or_else(|| DataFusionError::Execution("JSON_QUERY requires a path input".into()))?;

        match (json_input, path_input) {
            (ColumnarValue::Scalar(json_scalar), ColumnarValue::Scalar(path_scalar)) => {
                let result = json_query_scalar_with_options(json_scalar, path_scalar, &options)?;
                Ok(ColumnarValue::Scalar(result))
            }
            _ => {
                let json_array = get_utf8_array(json_input)?;
                let path_array = match path_input {
                    ColumnarValue::Scalar(path_scalar) => ParsedPath::from_scalar(path_scalar)?,
                    ColumnarValue::Array(arr) => ParsedPath::from_array(arr.clone())?,
                };

                let mut builder = StringBuilder::with_capacity(json_array.len(), json_array.len());
                let mut scratch = Vec::new();

                for row in 0..json_array.len() {
                    if json_array.is_null(row) {
                        builder.append_null();
                        continue;
                    }

                    let json_value = json_array.value(row);
                    let maybe_path = path_array.value_for_row(row)?;

                    let status = match maybe_path {
                        None => JsonPathStatus::Missing,
                        Some(path) => evaluate_json_path_status(json_value, path, &mut scratch),
                    };

                    match apply_json_query_status(status, &options)? {
                        Some(value) => builder.append_value(value),
                        None => builder.append_null(),
                    }
                }

                let array: ArrayRef = Arc::new(builder.finish());
                finalize_row_evaluation(false, array)
            }
        }
    }
}

#[derive(Clone, Copy)]
enum JsonQueryReturnType {
    String,
    Array,
}

#[allow(clippy::enum_variant_names)]
#[derive(Clone, Copy)]
enum JsonQueryWrapper {
    WithoutArray,
    ConditionalArray,
    UnconditionalArray,
}

#[derive(Clone, Copy)]
enum JsonQueryBehavior {
    Null,
    EmptyArray,
    EmptyObject,
    Error,
}

#[derive(Clone, Copy)]
struct JsonQueryOptions {
    return_type: JsonQueryReturnType,
    wrapper: JsonQueryWrapper,
    on_empty: JsonQueryBehavior,
    on_error: JsonQueryBehavior,
}

impl Default for JsonQueryOptions {
    fn default() -> Self {
        Self {
            return_type: JsonQueryReturnType::String,
            wrapper: JsonQueryWrapper::WithoutArray,
            on_empty: JsonQueryBehavior::Null,
            on_error: JsonQueryBehavior::Null,
        }
    }
}

impl JsonQueryOptions {
    fn from_args(args: &[ColumnarValue]) -> DataFusionResult<Self> {
        match args.len() {
            2 => Ok(Self::default()),
            6 => {
                let return_type = Self::parse_return_type(&args[2])?;
                let wrapper = Self::parse_wrapper(&args[3])?;
                let on_empty = Self::parse_behavior(&args[4], "ON EMPTY")?;
                let on_error = Self::parse_behavior(&args[5], "ON ERROR")?;
                let options = Self {
                    return_type,
                    wrapper,
                    on_empty,
                    on_error,
                };
                options.validate()?;
                Ok(options)
            }
            other => Err(DataFusionError::Execution(format!(
                "JSON_QUERY expects 2 or 6 arguments, got {other}"
            ))),
        }
    }

    fn parse_return_type(value: &ColumnarValue) -> DataFusionResult<JsonQueryReturnType> {
        let raw = parse_utf8_literal(value, "RETURNING clause")?;
        let normalised: String = raw
            .chars()
            .filter(|ch| !ch.is_ascii_whitespace())
            .collect::<String>()
            .to_ascii_uppercase();
        match normalised.as_str() {
            "STRING" | "VARCHAR" => Ok(JsonQueryReturnType::String),
            "ARRAY" | "ARRAY<STRING>" | "ARRAY<VARCHAR>" => Ok(JsonQueryReturnType::Array),
            other => Err(DataFusionError::Execution(format!(
                "Unsupported JSON_QUERY RETURNING type '{other}'"
            ))),
        }
    }

    fn parse_wrapper(value: &ColumnarValue) -> DataFusionResult<JsonQueryWrapper> {
        let normalised = normalise_keyword(&parse_utf8_literal(value, "WRAPPER clause")?);
        match normalised.as_str() {
            "WITHOUT ARRAY" | "WITHOUT ARRAY WRAPPER" => Ok(JsonQueryWrapper::WithoutArray),
            "WITH CONDITIONAL ARRAY" | "WITH CONDITIONAL ARRAY WRAPPER" => {
                Ok(JsonQueryWrapper::ConditionalArray)
            }
            "WITH UNCONDITIONAL ARRAY" | "WITH UNCONDITIONAL ARRAY WRAPPER" => {
                Ok(JsonQueryWrapper::UnconditionalArray)
            }
            other => Err(DataFusionError::Execution(format!(
                "Unsupported JSON_QUERY wrapper '{other}'"
            ))),
        }
    }

    fn parse_behavior(value: &ColumnarValue, context: &str) -> DataFusionResult<JsonQueryBehavior> {
        let normalised = normalise_keyword(&parse_utf8_literal(value, context)?);
        match normalised.as_str() {
            "NULL" => Ok(JsonQueryBehavior::Null),
            "EMPTY ARRAY" => Ok(JsonQueryBehavior::EmptyArray),
            "EMPTY OBJECT" => Ok(JsonQueryBehavior::EmptyObject),
            "ERROR" => Ok(JsonQueryBehavior::Error),
            other => Err(DataFusionError::Execution(format!(
                "Unsupported JSON_QUERY {context} behavior '{other}'"
            ))),
        }
    }

    fn validate(&self) -> DataFusionResult<()> {
        if matches!(self.return_type, JsonQueryReturnType::Array)
            && (matches!(self.on_empty, JsonQueryBehavior::EmptyObject)
                || matches!(self.on_error, JsonQueryBehavior::EmptyObject))
        {
            return Err(DataFusionError::Execution(
                "JSON_QUERY RETURNING ARRAY does not support EMPTY OBJECT behavior".to_string(),
            ));
        }
        Ok(())
    }

    fn handle_empty(&self) -> DataFusionResult<Option<String>> {
        self.handle_behavior(self.on_empty, "ON EMPTY")
    }

    fn handle_error(&self) -> DataFusionResult<Option<String>> {
        self.handle_behavior(self.on_error, "ON ERROR")
    }

    fn handle_behavior(
        &self,
        behavior: JsonQueryBehavior,
        clause: &str,
    ) -> DataFusionResult<Option<String>> {
        match behavior {
            JsonQueryBehavior::Null => Ok(None),
            JsonQueryBehavior::EmptyArray => Ok(Some("[]".to_string())),
            JsonQueryBehavior::EmptyObject => {
                if matches!(self.return_type, JsonQueryReturnType::Array) {
                    Err(DataFusionError::Execution(format!(
                        "JSON_QUERY {clause} does not support EMPTY OBJECT when RETURNING ARRAY"
                    )))
                } else {
                    Ok(Some("{}".to_string()))
                }
            }
            JsonQueryBehavior::Error => Err(DataFusionError::Execution(format!(
                "JSON_QUERY {clause} clause triggered error"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct JsonExistsOptions {
    on_error: JsonExistsBehavior,
}

#[derive(Debug, Clone, Copy)]
enum JsonExistsBehavior {
    True,
    False,
    Error,
    Unknown,
}

impl Default for JsonExistsBehavior {
    fn default() -> Self {
        Self::False
    }
}

impl JsonExistsOptions {
    fn from_args(args: &[ColumnarValue]) -> DataFusionResult<Self> {
        if args.len() <= 2 {
            return Ok(Self::default());
        }

        let behavior = parse_json_exists_behavior(&args[2])?;
        Ok(Self { on_error: behavior })
    }
}

fn parse_json_exists_behavior(value: &ColumnarValue) -> DataFusionResult<JsonExistsBehavior> {
    let text = match value {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) => v.clone(),
        ColumnarValue::Scalar(ScalarValue::Utf8View(Some(v))) => v.to_string(),
        ColumnarValue::Scalar(ScalarValue::Utf8(None))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(None)) => {
            return Err(DataFusionError::Execution(
                "JSON_EXISTS ON ERROR expects a non-null string literal".to_string(),
            ));
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "JSON_EXISTS ON ERROR expects a string literal, received {:?}",
                other
            )));
        }
    };

    match text.to_ascii_uppercase().as_str() {
        "TRUE" => Ok(JsonExistsBehavior::True),
        "FALSE" => Ok(JsonExistsBehavior::False),
        "ERROR" => Ok(JsonExistsBehavior::Error),
        "UNKNOWN" => Ok(JsonExistsBehavior::Unknown),
        other => Err(DataFusionError::Execution(format!(
            "Unsupported JSON_EXISTS ON ERROR behavior '{other}'"
        ))),
    }
}

#[derive(Clone, Copy)]
enum JsonValueBehavior {
    Null,
    Error,
    Default,
}

struct JsonValueOptions {
    on_empty: JsonValueBehavior,
    default_on_empty: Option<String>,
    on_error: JsonValueBehavior,
    default_on_error: Option<String>,
}

impl JsonValueOptions {
    fn new(
        on_empty: JsonValueBehavior,
        default_on_empty: Option<String>,
        on_error: JsonValueBehavior,
        default_on_error: Option<String>,
    ) -> Self {
        Self {
            on_empty,
            default_on_empty,
            on_error,
            default_on_error,
        }
    }

    fn from_args(args: &[ColumnarValue]) -> DataFusionResult<Self> {
        match args.len() {
            2 => Ok(Self::new(
                JsonValueBehavior::Null,
                None,
                JsonValueBehavior::Null,
                None,
            )),
            7 => {
                ensure_returning_type(&args[2])?;
                let on_empty = parse_json_value_behavior(&args[3], "ON EMPTY")?;
                let default_on_empty = parse_json_value_default(&args[4], "DEFAULT ON EMPTY")?;
                let on_error = parse_json_value_behavior(&args[5], "ON ERROR")?;
                let default_on_error = parse_json_value_default(&args[6], "DEFAULT ON ERROR")?;
                Ok(Self::new(
                    on_empty,
                    default_on_empty,
                    on_error,
                    default_on_error,
                ))
            }
            len => Err(DataFusionError::Execution(format!(
                "JSON_VALUE expects 2 or 7 arguments, got {len}"
            ))),
        }
    }

    fn handle_empty(&self) -> DataFusionResult<Option<String>> {
        self.resolve_behavior(self.on_empty, &self.default_on_empty, "ON EMPTY")
    }

    fn handle_error(&self) -> DataFusionResult<Option<String>> {
        self.resolve_behavior(self.on_error, &self.default_on_error, "ON ERROR")
    }

    fn resolve_behavior(
        &self,
        behavior: JsonValueBehavior,
        default: &Option<String>,
        context: &str,
    ) -> DataFusionResult<Option<String>> {
        match behavior {
            JsonValueBehavior::Null => Ok(None),
            JsonValueBehavior::Error => Err(DataFusionError::Execution(format!(
                "JSON_VALUE {} clause triggered error",
                context
            ))),
            JsonValueBehavior::Default => Ok(default.clone()),
        }
    }
}

#[derive(Debug, Clone)]
enum PathStep {
    Key(String),
    Index(usize),
}

fn parse_json_path(path: &str) -> DataFusionResult<Vec<PathStep>> {
    if path.is_empty() {
        return Err(DataFusionError::Execution(
            "JSON path cannot be empty".to_string(),
        ));
    }

    let mut chars = path.chars().peekable();
    match chars.next() {
        Some('$') => {}
        _ => {
            return Err(DataFusionError::Execution(
                "JSON path must start with '$'".to_string(),
            ));
        }
    }

    let mut steps = Vec::new();
    while let Some(&ch) = chars.peek() {
        match ch {
            '.' => {
                chars.next();
                let mut key = String::new();
                while let Some(&c) = chars.peek() {
                    if matches!(c, '.' | '[') {
                        break;
                    }
                    key.push(c);
                    chars.next();
                }
                if key.is_empty() {
                    return Err(DataFusionError::Execution(
                        "Invalid JSON path: empty field".to_string(),
                    ));
                }
                steps.push(PathStep::Key(key));
            }
            '[' => {
                chars.next();
                if let Some(&quote) = chars.peek() {
                    if quote == '\'' || quote == '"' {
                        chars.next();
                        let mut key = String::new();
                        while let Some(&c) = chars.peek() {
                            if c == quote {
                                break;
                            }
                            key.push(c);
                            chars.next();
                        }
                        if chars.next() != Some(quote) {
                            return Err(DataFusionError::Execution(
                                "Unterminated quoted key in JSON path".to_string(),
                            ));
                        }
                        if chars.next() != Some(']') {
                            return Err(DataFusionError::Execution(
                                "Expected closing ']' in JSON path".to_string(),
                            ));
                        }
                        steps.push(PathStep::Key(key));
                    } else {
                        let mut index_str = String::new();
                        while let Some(&c) = chars.peek() {
                            if c == ']' {
                                break;
                            }
                            if !c.is_ascii_digit() {
                                return Err(DataFusionError::Execution(
                                    "Array index in JSON path must be numeric".to_string(),
                                ));
                            }
                            index_str.push(c);
                            chars.next();
                        }
                        if chars.next() != Some(']') {
                            return Err(DataFusionError::Execution(
                                "Expected closing ']' in JSON path".to_string(),
                            ));
                        }
                        let index = index_str.parse::<usize>().map_err(|_| {
                            DataFusionError::Execution(
                                "Array index in JSON path is too large".to_string(),
                            )
                        })?;
                        steps.push(PathStep::Index(index));
                    }
                }
            }
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "Unexpected character '{ch}' in JSON path"
                )));
            }
        }
    }

    Ok(steps)
}

struct ParsedPath {
    values: Option<Vec<Option<Vec<PathStep>>>>,
    constant: Option<Vec<PathStep>>,
}

impl ParsedPath {
    fn from_scalar(path_scalar: &ScalarValue) -> DataFusionResult<Self> {
        let path = match path_scalar {
            ScalarValue::Utf8(Some(v)) => v.as_str(),
            ScalarValue::Utf8View(Some(v)) => v,
            ScalarValue::Null | ScalarValue::Utf8(None) | ScalarValue::Utf8View(None) => {
                return Ok(Self {
                    values: None,
                    constant: None,
                });
            }
            other => {
                return Err(DataFusionError::Execution(format!(
                    "JSON path expects Utf8 input, received scalar of type {other:?}"
                )));
            }
        };

        Ok(Self {
            values: None,
            constant: Some(parse_json_path(path)?),
        })
    }

    fn from_array(array: ArrayRef) -> DataFusionResult<Self> {
        let string_array = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("JSON path column must be Utf8".to_string())
            })?;

        let mut parsed = Vec::with_capacity(string_array.len());
        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                parsed.push(None);
            } else {
                let path = string_array.value(i);
                parsed.push(Some(parse_json_path(path)?));
            }
        }

        Ok(Self {
            values: Some(parsed),
            constant: None,
        })
    }

    fn value_for_row(&self, row: usize) -> DataFusionResult<Option<&[PathStep]>> {
        if let Some(constant) = &self.constant {
            return Ok(Some(constant.as_slice()));
        }

        let values = self.values.as_ref().ok_or_else(|| {
            DataFusionError::Internal("Expected per-row paths to be available".to_string())
        })?;

        match values.get(row) {
            Some(Some(steps)) => Ok(Some(steps.as_slice())),
            Some(None) => Ok(None),
            None => Err(DataFusionError::Internal(
                "Path array length mismatch".to_string(),
            )),
        }
    }
}

fn get_utf8_array(value: &ColumnarValue) -> DataFusionResult<&StringArray> {
    match value {
        ColumnarValue::Array(arr) => arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            DataFusionError::Execution("JSON path expects Utf8 array for JSON input".to_string())
        }),
        _ => Err(DataFusionError::Execution(
            "JSON function expects array input when arguments are not scalar".to_string(),
        )),
    }
}

enum JsonPathStatus {
    Scalar(JsonScalar),
    NonScalar(String),
    Null,
    Missing,
    Error,
}

#[derive(Clone)]
enum JsonScalar {
    String(String),
    Number(String),
    Boolean(bool),
}

impl JsonScalar {
    fn as_text(&self) -> String {
        match self {
            Self::String(value) | Self::Number(value) => value.clone(),
            Self::Boolean(value) => value.to_string(),
        }
    }

    fn as_json(&self) -> String {
        match self {
            Self::String(value) => json_quote(value),
            Self::Number(value) => value.clone(),
            Self::Boolean(value) => value.to_string(),
        }
    }
}

fn evaluate_json_path_status(
    json_str: &str,
    path: &[PathStep],
    scratch: &mut Vec<u8>,
) -> JsonPathStatus {
    scratch.clear();
    scratch.extend_from_slice(json_str.as_bytes());
    let parsed = match simd_json::to_borrowed_value(scratch.as_mut_slice()) {
        Ok(value) => value,
        Err(_) => return JsonPathStatus::Error,
    };

    let mut current = &parsed;
    for step in path {
        match (step, current) {
            (PathStep::Key(key), BorrowedValue::Object(map)) => {
                if let Some(child) = map.get(key.as_str()) {
                    current = child;
                } else {
                    return JsonPathStatus::Missing;
                }
            }
            (PathStep::Index(idx), BorrowedValue::Array(arr)) => {
                if let Some(child) = arr.get(*idx) {
                    current = child;
                } else {
                    return JsonPathStatus::Missing;
                }
            }
            _ => return JsonPathStatus::Missing,
        }
    }

    match current {
        BorrowedValue::Static(simd_json::value::StaticNode::Null) => JsonPathStatus::Null,
        BorrowedValue::Static(simd_json::value::StaticNode::Bool(b)) => {
            JsonPathStatus::Scalar(JsonScalar::Boolean(*b))
        }
        BorrowedValue::Static(simd_json::value::StaticNode::I64(v)) => {
            JsonPathStatus::Scalar(JsonScalar::Number(v.to_string()))
        }
        BorrowedValue::Static(simd_json::value::StaticNode::U64(v)) => {
            JsonPathStatus::Scalar(JsonScalar::Number(v.to_string()))
        }
        BorrowedValue::Static(simd_json::value::StaticNode::F64(v)) => {
            JsonPathStatus::Scalar(JsonScalar::Number(format_float(*v)))
        }
        BorrowedValue::String(s) => JsonPathStatus::Scalar(JsonScalar::String(s.to_string())),
        BorrowedValue::Array(_) | BorrowedValue::Object(_) => {
            JsonPathStatus::NonScalar(current.encode())
        }
    }
}

fn json_exists_scalar_with_options(
    json_scalar: &ScalarValue,
    path_scalar: &ScalarValue,
    options: &JsonExistsOptions,
) -> DataFusionResult<ScalarValue> {
    let json_str = match json_scalar {
        ScalarValue::Utf8(Some(v)) => v.as_str(),
        ScalarValue::Utf8View(Some(v)) => v,
        ScalarValue::Null | ScalarValue::Utf8(None) | ScalarValue::Utf8View(None) => {
            return Ok(ScalarValue::Boolean(None));
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "JSON_EXISTS expects Utf8 input as first argument, received scalar of type {other:?}"
            )));
        }
    };

    let path = match path_scalar {
        ScalarValue::Utf8(Some(v)) => v.as_str(),
        ScalarValue::Utf8View(Some(v)) => v,
        ScalarValue::Null | ScalarValue::Utf8(None) | ScalarValue::Utf8View(None) => {
            return Ok(ScalarValue::Boolean(None));
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "JSON_EXISTS expects Utf8 path as second argument, received scalar of type {other:?}"
            )));
        }
    };

    let steps = parse_json_path(path)?;
    let mut scratch = Vec::new();
    let status = evaluate_json_path_status(json_str, &steps, &mut scratch);

    Ok(ScalarValue::Boolean(apply_json_exists_status(
        status, options,
    )?))
}

fn json_query_scalar_with_options(
    json_scalar: &ScalarValue,
    path_scalar: &ScalarValue,
    options: &JsonQueryOptions,
) -> DataFusionResult<ScalarValue> {
    let json_str = match json_scalar {
        ScalarValue::Utf8(Some(v)) => v.as_str(),
        ScalarValue::Utf8View(Some(v)) => v,
        ScalarValue::Null | ScalarValue::Utf8(None) | ScalarValue::Utf8View(None) => {
            return Ok(ScalarValue::Utf8(None));
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "JSON_QUERY expects Utf8 input as first argument, received scalar of type {other:?}"
            )));
        }
    };

    let path = match path_scalar {
        ScalarValue::Utf8(Some(v)) => v.as_str(),
        ScalarValue::Utf8View(Some(v)) => v,
        ScalarValue::Null | ScalarValue::Utf8(None) | ScalarValue::Utf8View(None) => {
            return Ok(ScalarValue::Utf8(None));
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "JSON_QUERY expects Utf8 path as second argument, received scalar of type {other:?}"
            )));
        }
    };

    let steps = parse_json_path(path)?;
    let mut scratch = Vec::new();
    let status = evaluate_json_path_status(json_str, &steps, &mut scratch);
    let value = apply_json_query_status(status, options)?;
    Ok(ScalarValue::Utf8(value))
}

fn json_value_scalar_with_options(
    json_scalar: &ScalarValue,
    path_scalar: &ScalarValue,
    options: &JsonValueOptions,
) -> DataFusionResult<ScalarValue> {
    let json_str = match json_scalar {
        ScalarValue::Utf8(Some(v)) => v.as_str(),
        ScalarValue::Utf8View(Some(v)) => v,
        ScalarValue::Null | ScalarValue::Utf8(None) | ScalarValue::Utf8View(None) => {
            return Ok(ScalarValue::Utf8(None));
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "JSON_VALUE expects Utf8 input as first argument, received scalar of type {other:?}"
            )));
        }
    };

    let path = match path_scalar {
        ScalarValue::Utf8(Some(v)) => v.as_str(),
        ScalarValue::Utf8View(Some(v)) => v,
        ScalarValue::Null | ScalarValue::Utf8(None) | ScalarValue::Utf8View(None) => {
            return Ok(ScalarValue::Utf8(None));
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "JSON_VALUE expects Utf8 path as second argument, received scalar of type {other:?}"
            )));
        }
    };

    let steps = parse_json_path(path)?;
    let mut scratch = Vec::new();
    let status = evaluate_json_path_status(json_str, &steps, &mut scratch);
    let value = apply_json_value_status(status, options)?;
    Ok(ScalarValue::Utf8(value))
}

fn apply_json_exists_status(
    status: JsonPathStatus,
    options: &JsonExistsOptions,
) -> DataFusionResult<Option<bool>> {
    match status {
        JsonPathStatus::Scalar(_) | JsonPathStatus::NonScalar(_) => Ok(Some(true)),
        JsonPathStatus::Null | JsonPathStatus::Missing => Ok(Some(false)),
        JsonPathStatus::Error => match options.on_error {
            JsonExistsBehavior::True => Ok(Some(true)),
            JsonExistsBehavior::False => Ok(Some(false)),
            JsonExistsBehavior::Unknown => Ok(None),
            JsonExistsBehavior::Error => Err(DataFusionError::Execution(
                "JSON_EXISTS ON ERROR behavior triggered error".to_string(),
            )),
        },
    }
}

fn apply_json_query_status(
    status: JsonPathStatus,
    options: &JsonQueryOptions,
) -> DataFusionResult<Option<String>> {
    match status {
        JsonPathStatus::NonScalar(json) => match options.wrapper {
            JsonQueryWrapper::WithoutArray => Ok(Some(json)),
            JsonQueryWrapper::ConditionalArray => {
                if is_json_array_literal(&json) {
                    Ok(Some(json))
                } else {
                    Ok(Some(format!("[{json}]")))
                }
            }
            JsonQueryWrapper::UnconditionalArray => Ok(Some(format!("[{json}]"))),
        },
        JsonPathStatus::Scalar(scalar) => match options.wrapper {
            JsonQueryWrapper::WithoutArray => options.handle_empty(),
            JsonQueryWrapper::ConditionalArray | JsonQueryWrapper::UnconditionalArray => {
                let json_value = scalar.as_json();
                Ok(Some(format!("[{json_value}]")))
            }
        },
        JsonPathStatus::Null | JsonPathStatus::Missing => options.handle_empty(),
        JsonPathStatus::Error => options.handle_error(),
    }
}

fn apply_json_value_status(
    status: JsonPathStatus,
    options: &JsonValueOptions,
) -> DataFusionResult<Option<String>> {
    match status {
        JsonPathStatus::Scalar(value) => Ok(Some(value.as_text())),
        JsonPathStatus::NonScalar(_) | JsonPathStatus::Null | JsonPathStatus::Missing => {
            options.handle_empty()
        }
        JsonPathStatus::Error => options.handle_error(),
    }
}

fn format_float(value: f64) -> String {
    let mut buf = String::new();
    if value.fract() == 0.0 {
        write!(&mut buf, "{:.0}", value).unwrap();
    } else {
        write!(&mut buf, "{}", value).unwrap();
    }
    buf
}

fn is_json_array_literal(value: &str) -> bool {
    value
        .chars()
        .find(|ch| !ch.is_ascii_whitespace())
        .map(|ch| ch == '[')
        .unwrap_or(false)
}

fn parse_json_value_behavior(
    value: &ColumnarValue,
    context: &str,
) -> DataFusionResult<JsonValueBehavior> {
    let raw = match value {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) => v.clone(),
        ColumnarValue::Scalar(ScalarValue::Utf8View(Some(v))) => v.to_string(),
        ColumnarValue::Scalar(ScalarValue::Utf8(None))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(None)) => {
            return Err(DataFusionError::Execution(format!(
                "JSON_VALUE {} expects a non-null string literal",
                context
            )));
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "JSON_VALUE {} expects a string literal, received {:?}",
                context, other
            )));
        }
    };

    match raw.to_ascii_uppercase().as_str() {
        "NULL" => Ok(JsonValueBehavior::Null),
        "ERROR" => Ok(JsonValueBehavior::Error),
        "DEFAULT" => Ok(JsonValueBehavior::Default),
        other => Err(DataFusionError::Execution(format!(
            "Unrecognized JSON_VALUE {} directive '{}'",
            context, other
        ))),
    }
}

fn parse_json_value_default(
    value: &ColumnarValue,
    context: &str,
) -> DataFusionResult<Option<String>> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) => Ok(Some(v.clone())),
        ColumnarValue::Scalar(ScalarValue::Utf8View(Some(v))) => Ok(Some(v.to_string())),
        ColumnarValue::Scalar(ScalarValue::Utf8(None))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(None))
        | ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
        other => Err(DataFusionError::Execution(format!(
            "JSON_VALUE {} expects a string literal or NULL, received {:?}",
            context, other
        ))),
    }
}

fn ensure_returning_type(value: &ColumnarValue) -> DataFusionResult<()> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(v)))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(v))) => {
            let ty = v.to_ascii_uppercase();
            if ty == "STRING" || ty == "VARCHAR" || ty == "CHAR" {
                Ok(())
            } else {
                Err(DataFusionError::Execution(format!(
                    "Unsupported JSON_VALUE RETURNING type '{}'",
                    ty
                )))
            }
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(None))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(None)) => Ok(()),
        other => Err(DataFusionError::Execution(format!(
            "JSON_VALUE RETURNING clause expects a string literal, received {:?}",
            other
        ))),
    }
}
