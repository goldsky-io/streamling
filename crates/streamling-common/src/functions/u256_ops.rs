use crate::types::u256::{self, U256Type};
use crate::{streamling_user_bail, streamling_user_err};
use arrow::array::{Array, FixedSizeBinaryArray, LargeStringArray, StringArray};
use arrow_schema::FieldRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use std::sync::Arc;

// ================================
// Conversion Functions
// ================================

/// Convert string or other types to U256
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToU256Func {
    signature: Signature,
}

impl Default for ToU256Func {
    fn default() -> Self {
        Self::new()
    }
}

impl ToU256Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8]),
                    TypeSignature::Exact(vec![DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::UInt64]),
                    TypeSignature::Exact(vec![DataType::Int32]),
                    TypeSignature::Exact(vec![DataType::UInt32]),
                    TypeSignature::Exact(vec![DataType::Int16]),
                    TypeSignature::Exact(vec![DataType::UInt16]),
                    TypeSignature::Exact(vec![DataType::Int8]),
                    TypeSignature::Exact(vec![DataType::UInt8]),
                    TypeSignature::Exact(vec![DataType::FixedSizeBinary(32)]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ToU256Func {
    fn name(&self) -> &str {
        "to_u256"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(U256Type::new())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(
            arrow_schema::Field::new(self.name(), U256Type::new(), false)
                .with_metadata(u256::U256Type::metadata()),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use arrow::array::{
            Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array,
            UInt64Array,
        };

        if args.args.is_empty() {
            streamling_user_bail!("to_u256 requires one argument");
        }

        let array = match &args.args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        let len = array.len();
        let mut builder = Vec::with_capacity(len);

        // Handle different input types
        match array.data_type() {
            DataType::Utf8 => {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..len {
                    if string_array.is_null(i) {
                        streamling_user_bail!("to_u256 does not support null values");
                    }
                    let s = string_array.value(i);
                    let value = u256::string_to_u256(s)?;
                    let bytes = u256::u256_to_bytes(&value);
                    builder.push(Some(bytes.to_vec()));
                }
            }
            DataType::LargeUtf8 => {
                let string_array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                for i in 0..len {
                    if string_array.is_null(i) {
                        streamling_user_bail!("to_u256 does not support null values");
                    }
                    let s = string_array.value(i);
                    let value = u256::string_to_u256(s)?;
                    let bytes = u256::u256_to_bytes(&value);
                    builder.push(Some(bytes.to_vec()));
                }
            }
            DataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..len {
                    if int_array.is_null(i) {
                        streamling_user_bail!("to_u256 does not support null values");
                    }
                    let val = int_array.value(i);
                    if val < 0 {
                        streamling_user_bail!("to_u256 does not support negative values: {}", val);
                    }
                    let value = u256::U256::from(val as u64);
                    let bytes = u256::u256_to_bytes(&value);
                    builder.push(Some(bytes.to_vec()));
                }
            }
            DataType::UInt64 => {
                let int_array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                for i in 0..len {
                    if int_array.is_null(i) {
                        streamling_user_bail!("to_u256 does not support null values");
                    }
                    let value = u256::U256::from(int_array.value(i));
                    let bytes = u256::u256_to_bytes(&value);
                    builder.push(Some(bytes.to_vec()));
                }
            }
            DataType::Int32 | DataType::Int16 | DataType::Int8 => {
                // Handle all signed integer types
                for i in 0..len {
                    if array.is_null(i) {
                        streamling_user_bail!("to_u256 does not support null values");
                    }
                    let val = match array.data_type() {
                        DataType::Int32 => array
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .value(i) as i64,
                        DataType::Int16 => array
                            .as_any()
                            .downcast_ref::<Int16Array>()
                            .unwrap()
                            .value(i) as i64,
                        DataType::Int8 => {
                            array.as_any().downcast_ref::<Int8Array>().unwrap().value(i) as i64
                        }
                        _ => unreachable!(),
                    };
                    if val < 0 {
                        streamling_user_bail!("to_u256 does not support negative values: {}", val);
                    }
                    let value = u256::U256::from(val as u64);
                    let bytes = u256::u256_to_bytes(&value);
                    builder.push(Some(bytes.to_vec()));
                }
            }
            DataType::UInt32 | DataType::UInt16 | DataType::UInt8 => {
                // Handle all unsigned integer types
                for i in 0..len {
                    if array.is_null(i) {
                        streamling_user_bail!("to_u256 does not support null values");
                    }
                    let val = match array.data_type() {
                        DataType::UInt32 => array
                            .as_any()
                            .downcast_ref::<UInt32Array>()
                            .unwrap()
                            .value(i) as u64,
                        DataType::UInt16 => array
                            .as_any()
                            .downcast_ref::<UInt16Array>()
                            .unwrap()
                            .value(i) as u64,
                        DataType::UInt8 => array
                            .as_any()
                            .downcast_ref::<UInt8Array>()
                            .unwrap()
                            .value(i) as u64,
                        _ => unreachable!(),
                    };
                    let value = u256::U256::from(val);
                    let bytes = u256::u256_to_bytes(&value);
                    builder.push(Some(bytes.to_vec()));
                }
            }
            DataType::FixedSizeBinary(32) => {
                // Passthrough: input is already U256
                return Ok(ColumnarValue::Array(array));
            }
            _ => {
                streamling_user_bail!(
                    "to_u256 does not support input type: {:?}",
                    array.data_type()
                );
            }
        }

        let result_array =
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(builder.into_iter(), 32)?;
        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }
}

/// Convert U256 to decimal string
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct U256ToStringFunc {
    signature: Signature,
}

impl Default for U256ToStringFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl U256ToStringFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![U256Type::new()], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for U256ToStringFunc {
    fn name(&self) -> &str {
        "u256_to_string"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(arrow_schema::Field::new(
            self.name(),
            DataType::Utf8,
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            streamling_user_bail!("u256_to_string requires one argument");
        }

        let array = match &args.args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        let binary_array = array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| streamling_user_err!("u256_to_string expects U256 input"))?;

        let mut builder = Vec::with_capacity(binary_array.len());
        for i in 0..binary_array.len() {
            if binary_array.is_null(i) {
                builder.push(None);
            } else {
                let bytes = binary_array.value(i);
                if bytes.len() != 32 {
                    streamling_user_bail!("u256_to_string expects 32 bytes, got {}", bytes.len());
                }
                let mut byte_array = [0u8; 32];
                byte_array.copy_from_slice(bytes);
                let value = u256::bytes_to_u256(&byte_array);
                builder.push(Some(u256::u256_to_string(&value)));
            }
        }

        let result_array = StringArray::from(builder);
        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }
}

// ================================
// Arithmetic Operations
// ================================

macro_rules! impl_u256_binary_op {
    ($name:ident, $func_name:expr, $op:ident) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $name {
            signature: Signature,
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $name {
            pub fn new() -> Self {
                Self {
                    signature: Signature::exact(
                        vec![U256Type::new(), U256Type::new()],
                        Volatility::Immutable,
                    ),
                }
            }
        }

        impl ScalarUDFImpl for $name {
            fn name(&self) -> &str {
                $func_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok(U256Type::new())
            }

            fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
                Ok(Arc::new(
                    arrow_schema::Field::new(self.name(), U256Type::new(), false)
                        .with_metadata(u256::U256Type::metadata()),
                ))
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                if args.args.len() != 2 {
                    streamling_user_bail!("{} requires two arguments", $func_name);
                }

                let left_array = match &args.args[0] {
                    ColumnarValue::Array(arr) => arr.clone(),
                    ColumnarValue::Scalar(scalar) => scalar.to_array()?,
                };

                let right_array = match &args.args[1] {
                    ColumnarValue::Array(arr) => arr.clone(),
                    ColumnarValue::Scalar(scalar) => scalar.to_array()?,
                };

                let left_binary = left_array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| {
                        streamling_user_err!("{} expects U256 input for left operand", $func_name)
                    })?;

                let right_binary = right_array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| {
                        streamling_user_err!("{} expects U256 input for right operand", $func_name)
                    })?;

                let left_len = left_binary.len();
                let right_len = right_binary.len();
                if left_len == 0 || right_len == 0 {
                    tracing::debug!(
                        target = "streamling_core::functions::u256_ops",
                        "{} received empty input: left_len={}, right_len={}",
                        $func_name,
                        left_len,
                        right_len
                    );
                    let empty_iter: std::vec::IntoIter<Option<Vec<u8>>> = Vec::new().into_iter();
                    let result_array =
                        FixedSizeBinaryArray::try_from_sparse_iter_with_size(empty_iter, 32)?;
                    return Ok(ColumnarValue::Array(Arc::new(result_array)));
                }

                let len = left_len.max(right_len);
                let mut builder = Vec::with_capacity(len);

                for i in 0..len {
                    let left_idx = if left_len == 1 { 0 } else { i };
                    let right_idx = if right_len == 1 { 0 } else { i };

                    if left_binary.is_null(left_idx) || right_binary.is_null(right_idx) {
                        streamling_user_bail!("{} does not support null values", $func_name);
                    }

                    let left_bytes = left_binary.value(left_idx);
                    let right_bytes = right_binary.value(right_idx);

                    if left_bytes.len() != 32 || right_bytes.len() != 32 {
                        streamling_user_bail!("{} expects 32 bytes", $func_name);
                    }

                    let mut left_array = [0u8; 32];
                    let mut right_array = [0u8; 32];
                    left_array.copy_from_slice(left_bytes);
                    right_array.copy_from_slice(right_bytes);

                    let result = u256::$op(&left_array, &right_array)?;
                    builder.push(Some(result.to_vec()));
                }

                let result_array =
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(builder.into_iter(), 32)?;
                Ok(ColumnarValue::Array(Arc::new(result_array)))
            }
        }
    };
}

impl_u256_binary_op!(U256AddFunc, "u256_add", add);
impl_u256_binary_op!(U256SubFunc, "u256_sub", sub);
impl_u256_binary_op!(U256MulFunc, "u256_mul", mul);
impl_u256_binary_op!(U256DivFunc, "u256_div", div);
impl_u256_binary_op!(U256ModFunc, "u256_mod", rem);

/// Helper function to get U256 UDF by name (used by operator rewriter)
pub fn get_u256_udf(name: &str) -> Result<datafusion::logical_expr::ScalarUDF> {
    use datafusion::logical_expr::ScalarUDF;

    match name {
        "u256_add" => Ok(ScalarUDF::from(U256AddFunc::new())),
        "u256_sub" => Ok(ScalarUDF::from(U256SubFunc::new())),
        "u256_mul" => Ok(ScalarUDF::from(U256MulFunc::new())),
        "u256_div" => Ok(ScalarUDF::from(U256DivFunc::new())),
        "u256_mod" => Ok(ScalarUDF::from(U256ModFunc::new())),
        "to_u256" => Ok(ScalarUDF::from(ToU256Func::new())),
        "u256_to_string" => Ok(ScalarUDF::from(U256ToStringFunc::new())),
        _ => Err(streamling_user_err!("Unknown U256 UDF: {}", name).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::u256::U256;

    #[test]
    fn test_to_u256() {
        let func = ToU256Func::new();
        let string_array = StringArray::from(vec!["12345", "67890"]);

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(string_array))],
            arg_fields: vec![Arc::new(arrow_schema::Field::new(
                "value",
                DataType::Utf8,
                false,
            ))],
            number_rows: 2,
            return_field: Arc::new(arrow_schema::Field::new("result", U256Type::new(), false)),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let binary_array = result_array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            assert_eq!(binary_array.len(), 2);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_to_u256_fixed_size_binary_passthrough() {
        // to_u256 over already-encoded u256 bytes (FixedSizeBinary(32)) must pass
        // the value through unchanged. The preprocessor relies on this to wrap
        // CASE results so the u256 extension metadata is re-declared on the output
        // field (mirrors to_i256, which already supports this).
        let func = ToU256Func::new();
        let bytes = u256::u256_to_bytes(&U256::from(12345u64));
        let input = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![Some(bytes.to_vec()), None].into_iter(),
            32,
        )
        .unwrap();

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(input))],
            arg_fields: vec![Arc::new(arrow_schema::Field::new(
                "value",
                U256Type::new(),
                true,
            ))],
            number_rows: 2,
            return_field: Arc::new(arrow_schema::Field::new("result", U256Type::new(), true)),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args).unwrap();
        let ColumnarValue::Array(arr) = result else {
            panic!("expected array result");
        };
        let binary = arr
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("result should be FixedSizeBinary");
        assert_eq!(binary.len(), 2);
        let mut recovered = [0u8; 32];
        recovered.copy_from_slice(binary.value(0));
        assert_eq!(u256::bytes_to_u256(&recovered), U256::from(12345u64));
        assert!(binary.is_null(1), "null should pass through");
    }

    #[test]
    fn test_u256_to_string_decodes_bytes_as_big_endian() {
        // u256_to_string is the only public path from the FixedSizeBinary(32)
        // buffer to a human-readable decimal — if it ever switched to little-
        // endian decoding (or a producer ever wrote LE bytes), value 1 would
        // surface as 2^248 and most multiplications would overflow.
        let bytes = u256::u256_to_bytes(&U256::from(1u64));
        let input = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![Some(bytes.to_vec())].into_iter(),
            32,
        )
        .unwrap();

        let func = U256ToStringFunc::new();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(input))],
            arg_fields: vec![Arc::new(arrow_schema::Field::new(
                "value",
                U256Type::new(),
                true,
            ))],
            number_rows: 1,
            return_field: Arc::new(arrow_schema::Field::new("result", DataType::Utf8, true)),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };
        let ColumnarValue::Array(arr) = func.invoke_with_args(args).unwrap() else {
            panic!("expected array result");
        };
        let s = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(s.value(0), "1");
    }

    #[test]
    fn test_u256_to_string_misread_le_bytes_yields_2_pow_248() {
        // Anti-regression for the ClickHouse hybrid endianness gap: ClickHouse
        // emits UInt256 columns as FixedSizeBinary(32) in little-endian, and
        // streamling stores big-endian. If the read-side normalizer
        // (normalize_batch_from_clickhouse) ever stops reversing those bytes,
        // u256_to_string reads 1 as 2^248 and downstream multiplication
        // overflows. Pin the symptom so a regression is loud.
        let mut le_one = [0u8; 32];
        le_one[0] = 1;
        let input = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![Some(le_one.to_vec())].into_iter(),
            32,
        )
        .unwrap();

        let func = U256ToStringFunc::new();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(input))],
            arg_fields: vec![Arc::new(arrow_schema::Field::new(
                "value",
                U256Type::new(),
                true,
            ))],
            number_rows: 1,
            return_field: Arc::new(arrow_schema::Field::new("result", DataType::Utf8, true)),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };
        let ColumnarValue::Array(arr) = func.invoke_with_args(args).unwrap() else {
            panic!("expected array result");
        };
        let s = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let expected = u256::u256_to_string(&(U256::from(1u64) << 248));
        assert_eq!(s.value(0), expected);
    }

    #[test]
    fn test_u256_add() {
        let func = U256AddFunc::new();

        let a = u256::u256_to_bytes(&U256::from(100u64));
        let b = u256::u256_to_bytes(&U256::from(50u64));

        let left_array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![Some(a.to_vec())].into_iter(),
            32,
        )
        .unwrap();
        let right_array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![Some(b.to_vec())].into_iter(),
            32,
        )
        .unwrap();

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(left_array)),
                ColumnarValue::Array(Arc::new(right_array)),
            ],
            arg_fields: vec![
                Arc::new(arrow_schema::Field::new("a", U256Type::new(), false)),
                Arc::new(arrow_schema::Field::new("b", U256Type::new(), false)),
            ],
            number_rows: 1,
            return_field: Arc::new(arrow_schema::Field::new("result", U256Type::new(), false)),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let binary_array = result_array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            let result_bytes = binary_array.value(0);
            let mut byte_array = [0u8; 32];
            byte_array.copy_from_slice(result_bytes);
            let result_val = u256::bytes_to_u256(&byte_array);
            assert_eq!(result_val, U256::from(150u64));
        } else {
            panic!("Expected array result");
        }
    }
}
