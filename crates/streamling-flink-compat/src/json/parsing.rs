use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanBuilder, StringArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::scalar::ScalarValue;

use super::shared::{
    canonicalize_json_scalar, canonicalize_json_text, finalize_row_evaluation, normalise_keyword,
    parse_utf8_literal,
};

pub(crate) fn parse_json_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ParseJsonUdf::new(false))
}

pub(crate) fn try_parse_json_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ParseJsonUdf::new(true))
}

pub(crate) fn is_json_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(IsJsonUdf::new())
}

#[derive(Debug)]
struct ParseJsonUdf {
    signature: Signature,
    nullable_on_error: bool,
}

impl ParseJsonUdf {
    fn new(nullable_on_error: bool) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Boolean]),
                ],
                Volatility::Immutable,
            ),
            nullable_on_error,
        }
    }

    fn apply_scalar(
        &self,
        value: &ScalarValue,
        allow_duplicates: bool,
    ) -> DataFusionResult<ScalarValue> {
        match canonicalize_json_scalar(value, allow_duplicates) {
            Ok(scalar) => Ok(scalar),
            Err(_err) if self.nullable_on_error => Ok(ScalarValue::Utf8(None)),
            Err(err) => Err(err),
        }
    }

    fn allow_duplicates_from_arg(arg: Option<&ColumnarValue>) -> DataFusionResult<bool> {
        if let Some(value) = arg {
            match value {
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(flag))) => Ok(*flag),
                ColumnarValue::Scalar(ScalarValue::Boolean(None)) => Ok(false),
                ColumnarValue::Scalar(other) => Err(DataFusionError::Execution(format!(
                    "PARSE_JSON allow_duplicate_keys expects Boolean literal, received {other:?}"
                ))),
                ColumnarValue::Array(_) => Err(DataFusionError::Execution(
                    "PARSE_JSON allow_duplicate_keys expects scalar literal".to_string(),
                )),
            }
        } else {
            Ok(false)
        }
    }
}

impl ScalarUDFImpl for ParseJsonUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        if self.nullable_on_error {
            "TRY_PARSE_JSON"
        } else {
            "PARSE_JSON"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if args.args.is_empty() || args.args.len() > 2 {
            return Err(DataFusionError::Execution(format!(
                "{} expects 1 or 2 arguments, got {}",
                self.name(),
                args.args.len()
            )));
        }

        let allow_duplicates = Self::allow_duplicates_from_arg(args.args.get(1))?;

        match &args.args[0] {
            ColumnarValue::Scalar(value) => {
                let scalar = self.apply_scalar(value, allow_duplicates)?;
                Ok(ColumnarValue::Scalar(scalar))
            }
            ColumnarValue::Array(array) => {
                let string_array =
                    array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "{} expects Utf8 array for input",
                                self.name()
                            ))
                        })?;

                let mut builder =
                    StringBuilder::with_capacity(string_array.len(), string_array.len());
                for row in 0..string_array.len() {
                    if string_array.is_null(row) {
                        builder.append_null();
                        continue;
                    }

                    match canonicalize_json_text(string_array.value(row), allow_duplicates) {
                        Ok(Some(text)) => builder.append_value(text),
                        Ok(None) => builder.append_null(),
                        Err(_err) if self.nullable_on_error => builder.append_null(),
                        Err(err) => return Err(err),
                    }
                }

                let array: ArrayRef = Arc::new(builder.finish());
                finalize_row_evaluation(false, array)
            }
        }
    }
}

#[derive(Debug)]
struct IsJsonUdf {
    signature: Signature,
}

impl IsJsonUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for IsJsonUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "IS_JSON"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if args.args.is_empty() || args.args.len() > 2 {
            return Err(DataFusionError::Execution(format!(
                "IS_JSON expects 1 or 2 arguments, got {}",
                args.args.len()
            )));
        }

        let predicate = JsonTypePredicate::from_argument(args.args.get(1))?;

        match &args.args[0] {
            ColumnarValue::Scalar(value) => {
                let result = match value {
                    ScalarValue::Utf8(Some(v)) => predicate.evaluate(v),
                    ScalarValue::Utf8View(Some(v)) => predicate.evaluate(v),
                    ScalarValue::LargeUtf8(Some(v)) => predicate.evaluate(v),
                    ScalarValue::Utf8(None)
                    | ScalarValue::Utf8View(None)
                    | ScalarValue::LargeUtf8(None)
                    | ScalarValue::Null => false,
                    other => {
                        return Err(DataFusionError::Execution(format!(
                            "IS_JSON expects Utf8 input, received scalar of type {other:?}"
                        )));
                    }
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(result))))
            }
            ColumnarValue::Array(array) => {
                let string_array =
                    array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "IS_JSON expects Utf8 array for input".to_string(),
                            )
                        })?;

                let mut builder = BooleanBuilder::with_capacity(string_array.len());
                for row in 0..string_array.len() {
                    if string_array.is_null(row) {
                        builder.append_value(false);
                    } else {
                        builder.append_value(predicate.evaluate(string_array.value(row)));
                    }
                }
                let array: ArrayRef = Arc::new(builder.finish());
                finalize_row_evaluation(false, array)
            }
        }
    }
}

#[derive(Clone, Copy)]
enum JsonTypePredicate {
    Value,
    Object,
    Array,
    Scalar,
}

impl JsonTypePredicate {
    fn from_argument(arg: Option<&ColumnarValue>) -> DataFusionResult<Self> {
        if let Some(value) = arg {
            let raw = parse_utf8_literal(value, "IS_JSON type argument")?;
            match normalise_keyword(&raw).as_str() {
                "VALUE" => Ok(Self::Value),
                "OBJECT" => Ok(Self::Object),
                "ARRAY" => Ok(Self::Array),
                "SCALAR" => Ok(Self::Scalar),
                other => Err(DataFusionError::Execution(format!(
                    "Unsupported IS_JSON type '{other}'"
                ))),
            }
        } else {
            Ok(Self::Value)
        }
    }

    fn evaluate(&self, text: &str) -> bool {
        match self {
            Self::Value => serde_json::from_str::<serde_json::Value>(text).is_ok(),
            Self::Object => serde_json::from_str::<serde_json::Value>(text)
                .map(|v| v.is_object())
                .unwrap_or(false),
            Self::Array => serde_json::from_str::<serde_json::Value>(text)
                .map(|v| v.is_array())
                .unwrap_or(false),
            Self::Scalar => serde_json::from_str::<serde_json::Value>(text)
                .map(|v| !v.is_object() && !v.is_array())
                .unwrap_or(false),
        }
    }
}
