use crate::functions::util::unary_string_to_string;
use arrow_schema::FieldRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sha3::{Digest, Keccak256};
use std::any::Any;
use std::sync::Arc;

/// Provides Keccak256 cryptographic hashing for strings.
///
/// Keccak256 is the original algorithm that was selected as SHA-3,
/// commonly used in Ethereum and other blockchain applications.
/// Handles both regular strings and hex-encoded strings (prefixed with "0x").
///
/// # Arguments
/// * `string` - The input string to hash (regular or hex-encoded with 0x prefix)
///
/// # Returns
/// A hexadecimal string representation of the 256-bit hash with 0x prefix
///
/// # Examples
/// * `keccak256('hello')` returns hash of UTF-8 bytes
/// * `keccak256('0xdeadbeef')` returns hash of hex-decoded bytes
#[derive(Debug)]
pub struct Keccak256Func {
    signature: Signature,
}

impl Default for Keccak256Func {
    fn default() -> Self {
        Self::new()
    }
}

impl Keccak256Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Keccak256Func {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_gs_keccak256"
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
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        unary_string_to_string(&args, self.name(), |input| {
            if let Some(stripped) = input.strip_prefix("0x") {
                // Special treatment for hex strings
                hex::decode(stripped)
                    .ok()
                    .map(|bytes| format!("0x{}", hex::encode(Keccak256::digest(&bytes))))
            } else {
                Some(format!(
                    "0x{}",
                    hex::encode(Keccak256::digest(input.as_bytes()))
                ))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, StringArray};

    #[test]
    fn test_keccak256_regular_string() {
        let func = Keccak256Func::new();

        let string_array = StringArray::from(vec!["hello"]);

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(string_array))],
            arg_fields: vec![Arc::new(arrow_schema::Field::new(
                "string",
                DataType::Utf8,
                false,
            ))],
            number_rows: 1,
            return_field: Arc::new(arrow_schema::Field::new("result", DataType::Utf8, false)),
        };

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let string_array = result_array.as_any().downcast_ref::<StringArray>().unwrap();

            let hash = string_array.value(0);
            assert!(hash.starts_with("0x"));
            assert_eq!(hash.len(), 66); // 0x + 64 hex chars
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_keccak256_hex_string() {
        let func = Keccak256Func::new();

        let string_array = StringArray::from(vec!["0xdeadbeef"]);

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(string_array))],
            arg_fields: vec![Arc::new(arrow_schema::Field::new(
                "string",
                DataType::Utf8,
                false,
            ))],
            number_rows: 1,
            return_field: Arc::new(arrow_schema::Field::new("result", DataType::Utf8, false)),
        };

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let string_array = result_array.as_any().downcast_ref::<StringArray>().unwrap();

            let hash = string_array.value(0);
            assert!(hash.starts_with("0x"));
            assert_eq!(hash.len(), 66);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_keccak256_null() {
        let func = Keccak256Func::new();

        let string_array = StringArray::from(vec![Option::<&str>::None]);

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(string_array))],
            arg_fields: vec![Arc::new(arrow_schema::Field::new(
                "string",
                DataType::Utf8,
                false,
            ))],
            number_rows: 1,
            return_field: Arc::new(arrow_schema::Field::new("result", DataType::Utf8, false)),
        };

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let string_array = result_array.as_any().downcast_ref::<StringArray>().unwrap();

            assert!(string_array.is_null(0));
        } else {
            panic!("Expected array result");
        }
    }
}
