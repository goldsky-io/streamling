use crate::types::i256::{self, I256Type};
use crate::{streamling_user_bail, streamling_user_err};
use arrow::array::{Array, FixedSizeBinaryArray, Int64Array, LargeStringArray, StringArray};
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

/// Convert string or other types to I256
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToI256Func {
    signature: Signature,
}

impl Default for ToI256Func {
    fn default() -> Self {
        Self::new()
    }
}

impl ToI256Func {
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

impl ScalarUDFImpl for ToI256Func {
    fn name(&self) -> &str {
        "to_i256"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(I256Type::new())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(
            arrow_schema::Field::new(self.name(), I256Type::new(), false)
                .with_metadata(i256::I256Type::metadata()),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use arrow::array::{
            Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array,
            UInt64Array,
        };

        if args.args.is_empty() {
            streamling_user_bail!("to_i256 requires one argument");
        }

        let array = match &args.args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        let len = array.len();
        let mut builder = Vec::with_capacity(len);

        match array.data_type() {
            DataType::Utf8 => {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..len {
                    if string_array.is_null(i) {
                        streamling_user_bail!("to_i256 does not support null values");
                    }
                    let s = string_array.value(i);
                    let value = i256::string_to_i256(s)?;
                    let bytes = i256::i256_to_bytes(&value);
                    builder.push(Some(bytes.to_vec()));
                }
            }
            DataType::LargeUtf8 => {
                let string_array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                for i in 0..len {
                    if string_array.is_null(i) {
                        streamling_user_bail!("to_i256 does not support null values");
                    }
                    let s = string_array.value(i);
                    let value = i256::string_to_i256(s)?;
                    let bytes = i256::i256_to_bytes(&value);
                    builder.push(Some(bytes.to_vec()));
                }
            }
            DataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..len {
                    if int_array.is_null(i) {
                        streamling_user_bail!("to_i256 does not support null values");
                    }
                    let value = i256::I256::from_i64(int_array.value(i));
                    let bytes = i256::i256_to_bytes(&value);
                    builder.push(Some(bytes.to_vec()));
                }
            }
            DataType::UInt64 => {
                let int_array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                for i in 0..len {
                    if int_array.is_null(i) {
                        streamling_user_bail!("to_i256 does not support null values");
                    }
                    let value = i256::I256::from_u256(i256::U256::from(int_array.value(i)));
                    let bytes = i256::i256_to_bytes(&value);
                    builder.push(Some(bytes.to_vec()));
                }
            }
            DataType::Int32 | DataType::Int16 | DataType::Int8 => {
                for i in 0..len {
                    if array.is_null(i) {
                        streamling_user_bail!("to_i256 does not support null values");
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
                    let value = i256::I256::from_i64(val);
                    let bytes = i256::i256_to_bytes(&value);
                    builder.push(Some(bytes.to_vec()));
                }
            }
            DataType::UInt32 | DataType::UInt16 | DataType::UInt8 => {
                for i in 0..len {
                    if array.is_null(i) {
                        streamling_user_bail!("to_i256 does not support null values");
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
                    let value = i256::I256::from_u256(i256::U256::from(val));
                    let bytes = i256::i256_to_bytes(&value);
                    builder.push(Some(bytes.to_vec()));
                }
            }
            DataType::FixedSizeBinary(32) => {
                // Passthrough: input is already I256
                return Ok(ColumnarValue::Array(array));
            }
            _ => {
                streamling_user_bail!(
                    "to_i256 does not support input type: {:?}",
                    array.data_type()
                );
            }
        }

        let result_array =
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(builder.into_iter(), 32)?;
        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }
}

/// Convert I256 to decimal string
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct I256ToStringFunc {
    signature: Signature,
}

impl Default for I256ToStringFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl I256ToStringFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![I256Type::new()], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for I256ToStringFunc {
    fn name(&self) -> &str {
        "i256_to_string"
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
            streamling_user_bail!("i256_to_string requires one argument");
        }

        let array = match &args.args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        let binary_array = array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| streamling_user_err!("i256_to_string expects I256 input"))?;

        let mut builder = Vec::with_capacity(binary_array.len());
        for i in 0..binary_array.len() {
            if binary_array.is_null(i) {
                builder.push(None);
            } else {
                let bytes = binary_array.value(i);
                if bytes.len() != 32 {
                    streamling_user_bail!("i256_to_string expects 32 bytes, got {}", bytes.len());
                }
                let mut byte_array = [0u8; 32];
                byte_array.copy_from_slice(bytes);
                let value = i256::bytes_to_i256(&byte_array);
                builder.push(Some(i256::i256_to_string(&value)));
            }
        }

        let result_array = StringArray::from(builder);
        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }
}

/// Convert I256 to Int64 (errors if value overflows i64 range)
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToInt64Func {
    signature: Signature,
}

impl Default for ToInt64Func {
    fn default() -> Self {
        Self::new()
    }
}

impl ToInt64Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![I256Type::new()], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToInt64Func {
    fn name(&self) -> &str {
        "to_int64"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(arrow_schema::Field::new(
            self.name(),
            DataType::Int64,
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            streamling_user_bail!("to_int64 requires one argument");
        }

        let array = match &args.args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        let binary_array = array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| streamling_user_err!("to_int64 expects I256 input"))?;

        let mut builder: Vec<Option<i64>> = Vec::with_capacity(binary_array.len());
        for i in 0..binary_array.len() {
            if binary_array.is_null(i) {
                builder.push(None);
            } else {
                let bytes = binary_array.value(i);
                if bytes.len() != 32 {
                    streamling_user_bail!("to_int64 expects 32 bytes, got {}", bytes.len());
                }

                // I256 is stored as 32 big-endian bytes (two's complement).
                // We can convert to i64 when the upper 24 bytes are uniform:
                //
                //   all 0x00 → value is in [0, 2^64-1] (u64 range).
                //              The low 8 bytes are returned as-is, reinterpreting
                //              the bit pattern as signed i64.
                //   all 0xFF with byte 24 sign-bit set
                //           → value is in [-2^63, -1] (negative i64 range).
                //              The low 8 bytes are the two's-complement i64.
                //
                // Anything else overflows and returns NULL.
                let all_zero = bytes[..24].iter().all(|&b| b == 0x00);
                let all_ones = bytes[..24].iter().all(|&b| b == 0xFF);
                if all_zero || (all_ones && bytes[24] & 0x80 != 0) {
                    let val = i64::from_be_bytes(bytes[24..32].try_into().unwrap());
                    builder.push(Some(val));
                } else {
                    builder.push(None);
                }
            }
        }

        let result_array = Int64Array::from(builder);
        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }
}

// ================================
// Arithmetic Operations
// ================================

macro_rules! impl_i256_binary_op {
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
                        vec![I256Type::new(), I256Type::new()],
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
                Ok(I256Type::new())
            }

            fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
                Ok(Arc::new(
                    arrow_schema::Field::new(self.name(), I256Type::new(), false)
                        .with_metadata(i256::I256Type::metadata()),
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
                        streamling_user_err!("{} expects I256 input for left operand", $func_name)
                    })?;

                let right_binary = right_array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| {
                        streamling_user_err!("{} expects I256 input for right operand", $func_name)
                    })?;

                let left_len = left_binary.len();
                let right_len = right_binary.len();
                if left_len == 0 || right_len == 0 {
                    tracing::debug!(
                        target = "streamling_core::functions::i256_ops",
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

                    let result = i256::$op(&left_array, &right_array)?;
                    builder.push(Some(result.to_vec()));
                }

                let result_array =
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(builder.into_iter(), 32)?;
                Ok(ColumnarValue::Array(Arc::new(result_array)))
            }
        }
    };
}

impl_i256_binary_op!(I256AddFunc, "i256_add", add);
impl_i256_binary_op!(I256SubFunc, "i256_sub", sub);
impl_i256_binary_op!(I256MulFunc, "i256_mul", mul);
impl_i256_binary_op!(I256DivFunc, "i256_div", div);
impl_i256_binary_op!(I256ModFunc, "i256_mod", rem);

/// Helper function to get I256 UDF by name (used by operator rewriter)
pub fn get_i256_udf(name: &str) -> Result<datafusion::logical_expr::ScalarUDF> {
    use datafusion::logical_expr::ScalarUDF;

    match name {
        "i256_add" => Ok(ScalarUDF::from(I256AddFunc::new())),
        "i256_sub" => Ok(ScalarUDF::from(I256SubFunc::new())),
        "i256_mul" => Ok(ScalarUDF::from(I256MulFunc::new())),
        "i256_div" => Ok(ScalarUDF::from(I256DivFunc::new())),
        "i256_mod" => Ok(ScalarUDF::from(I256ModFunc::new())),
        "i256_neg" => Ok(ScalarUDF::from(I256NegFunc::new())),
        "i256_abs" => Ok(ScalarUDF::from(I256AbsFunc::new())),
        "to_i256" => Ok(ScalarUDF::from(ToI256Func::new())),
        "i256_to_string" => Ok(ScalarUDF::from(I256ToStringFunc::new())),
        "to_int64" => Ok(ScalarUDF::from(ToInt64Func::new())),
        _ => Err(streamling_user_err!("Unknown I256 UDF: {}", name).into()),
    }
}

// Unary operations

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct I256NegFunc {
    signature: Signature,
}

impl Default for I256NegFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl I256NegFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![I256Type::new()], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for I256NegFunc {
    fn name(&self) -> &str {
        "i256_neg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(I256Type::new())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(
            arrow_schema::Field::new(self.name(), I256Type::new(), false)
                .with_metadata(i256::I256Type::metadata()),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            streamling_user_bail!("i256_neg requires one argument");
        }

        let array = match &args.args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        let binary_array = array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| streamling_user_err!("i256_neg expects I256 input"))?;

        let mut builder = Vec::with_capacity(binary_array.len());
        for i in 0..binary_array.len() {
            if binary_array.is_null(i) {
                streamling_user_bail!("i256_neg does not support null values");
            }
            let bytes = binary_array.value(i);
            if bytes.len() != 32 {
                streamling_user_bail!("i256_neg expects 32 bytes, got {}", bytes.len());
            }
            let mut byte_array = [0u8; 32];
            byte_array.copy_from_slice(bytes);
            let result = i256::neg(&byte_array)?;
            builder.push(Some(result.to_vec()));
        }

        let result_array =
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(builder.into_iter(), 32)?;
        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct I256AbsFunc {
    signature: Signature,
}

impl Default for I256AbsFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl I256AbsFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![I256Type::new()], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for I256AbsFunc {
    fn name(&self) -> &str {
        "i256_abs"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(I256Type::new())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(
            arrow_schema::Field::new(self.name(), I256Type::new(), false)
                .with_metadata(i256::I256Type::metadata()),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            streamling_user_bail!("i256_abs requires one argument");
        }

        let array = match &args.args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        let binary_array = array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| streamling_user_err!("i256_abs expects I256 input"))?;

        let mut builder = Vec::with_capacity(binary_array.len());
        for i in 0..binary_array.len() {
            if binary_array.is_null(i) {
                streamling_user_bail!("i256_abs does not support null values");
            }
            let bytes = binary_array.value(i);
            if bytes.len() != 32 {
                streamling_user_bail!("i256_abs expects 32 bytes, got {}", bytes.len());
            }
            let mut byte_array = [0u8; 32];
            byte_array.copy_from_slice(bytes);
            let result = i256::abs(&byte_array)?;
            builder.push(Some(result.to_vec()));
        }

        let result_array =
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(builder.into_iter(), 32)?;
        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::i256::I256;

    #[test]
    fn test_to_i256() {
        let func = ToI256Func::new();
        let string_array = StringArray::from(vec!["12345", "-67890"]);

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(string_array))],
            arg_fields: vec![Arc::new(arrow_schema::Field::new(
                "value",
                DataType::Utf8,
                false,
            ))],
            number_rows: 2,
            return_field: Arc::new(arrow_schema::Field::new("result", I256Type::new(), false)),
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

    fn make_i256_array(values: Vec<Option<i64>>) -> Arc<FixedSizeBinaryArray> {
        let items: Vec<Option<Vec<u8>>> = values
            .into_iter()
            .map(|v| v.map(|n| i256::i256_to_bytes(&I256::from_i64(n)).to_vec()))
            .collect();
        Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(items.into_iter(), 32).unwrap(),
        )
    }

    #[test]
    fn test_to_int64_positive() {
        let func = ToInt64Func::new();
        let input = make_i256_array(vec![Some(12345), Some(0), Some(i64::MAX)]);

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input)],
            arg_fields: vec![Arc::new(
                arrow_schema::Field::new("value", I256Type::new(), false)
                    .with_metadata(I256Type::metadata()),
            )],
            number_rows: 3,
            return_field: Arc::new(arrow_schema::Field::new("result", DataType::Int64, true)),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(arr) = result {
            let int_array = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(int_array.value(0), 12345);
            assert_eq!(int_array.value(1), 0);
            assert_eq!(int_array.value(2), i64::MAX);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_to_int64_negative() {
        let func = ToInt64Func::new();
        let input = make_i256_array(vec![Some(-1), Some(-67890)]);

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input)],
            arg_fields: vec![Arc::new(
                arrow_schema::Field::new("value", I256Type::new(), false)
                    .with_metadata(I256Type::metadata()),
            )],
            number_rows: 2,
            return_field: Arc::new(arrow_schema::Field::new("result", DataType::Int64, true)),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(arr) = result {
            let int_array = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(int_array.value(0), -1);
            assert_eq!(int_array.value(1), -67890);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_to_int64_min() {
        let func = ToInt64Func::new();
        // Use string_to_i256 to avoid I256::from_i64 overflow on i64::MIN
        let min_val = i256::string_to_i256(&i64::MIN.to_string()).unwrap();
        let bytes = i256::i256_to_bytes(&min_val);
        let input = Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![Some(bytes.to_vec())].into_iter(),
                32,
            )
            .unwrap(),
        );

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input)],
            arg_fields: vec![Arc::new(
                arrow_schema::Field::new("value", I256Type::new(), false)
                    .with_metadata(I256Type::metadata()),
            )],
            number_rows: 1,
            return_field: Arc::new(arrow_schema::Field::new("result", DataType::Int64, true)),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(arr) = result {
            let int_array = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(int_array.value(0), i64::MIN);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_to_int64_u64_max_reinterprets_as_minus_one() {
        // u64::MAX stored as positive I256 should reinterpret as i64 -1.
        // This is the Solana rent_epoch "rent exempt" value.
        let val = i256::string_to_i256("18446744073709551615").unwrap(); // u64::MAX
        assert_eq!(eval_to_int64(Some(&val)), Some(-1i64));
    }

    #[test]
    fn test_to_int64_i64_max_plus_one_reinterprets() {
        // i64::MAX + 1 = 2^63, stored as positive I256, reinterprets as i64::MIN
        let val = i256::string_to_i256("9223372036854775808").unwrap();
        assert_eq!(eval_to_int64(Some(&val)), Some(i64::MIN));
    }

    #[test]
    fn test_to_int64_overflow() {
        // 2^64 does NOT fit in u64, so it should return NULL
        let val = i256::string_to_i256("18446744073709551616").unwrap(); // u64::MAX + 1
        assert_eq!(eval_to_int64(Some(&val)), None);
    }

    #[test]
    fn test_to_int64_negative_overflow() {
        // -(2^63 + 1) does NOT fit in i64, should return NULL
        let val = i256::string_to_i256("-9223372036854775809").unwrap(); // i64::MIN - 1
        assert_eq!(eval_to_int64(Some(&val)), None);
    }

    /// Helper: run ToInt64Func on a single I256 value and return the result.
    fn eval_to_int64(value: Option<&i256::I256>) -> Option<i64> {
        let func = ToInt64Func::new();
        let input = Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![value.map(|v| i256::i256_to_bytes(v).to_vec())].into_iter(),
                32,
            )
            .unwrap(),
        );

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input)],
            arg_fields: vec![Arc::new(
                arrow_schema::Field::new("value", I256Type::new(), false)
                    .with_metadata(I256Type::metadata()),
            )],
            number_rows: 1,
            return_field: Arc::new(arrow_schema::Field::new("result", DataType::Int64, true)),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(arr) = result {
            let int_array = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            if int_array.is_null(0) {
                None
            } else {
                Some(int_array.value(0))
            }
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_to_int64_null() {
        let func = ToInt64Func::new();
        let input = Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![None::<Vec<u8>>].into_iter(),
                32,
            )
            .unwrap(),
        );

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input)],
            arg_fields: vec![Arc::new(
                arrow_schema::Field::new("value", I256Type::new(), true)
                    .with_metadata(I256Type::metadata()),
            )],
            number_rows: 1,
            return_field: Arc::new(arrow_schema::Field::new("result", DataType::Int64, true)),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(arr) = result {
            let int_array = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            assert!(int_array.is_null(0));
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_i256_add() {
        let func = I256AddFunc::new();

        let a = i256::i256_to_bytes(&I256::from_i64(100));
        let b = i256::i256_to_bytes(&I256::from_i64(-50));

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
                Arc::new(arrow_schema::Field::new("a", I256Type::new(), false)),
                Arc::new(arrow_schema::Field::new("b", I256Type::new(), false)),
            ],
            number_rows: 1,
            return_field: Arc::new(arrow_schema::Field::new("result", I256Type::new(), false)),
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
            let result_val = i256::bytes_to_i256(&byte_array);
            assert_eq!(result_val, I256::from_i64(50));
        } else {
            panic!("Expected array result");
        }
    }
}
