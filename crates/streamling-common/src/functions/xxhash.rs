use crate::functions::util::unary_string_to_string;
use arrow_schema::FieldRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;
use xxhash_rust::xxh3::xxh3_128;

/// Provides XXH128 non-cryptographic hashing for strings.
///
/// Uses the XXH3 algorithm (128-bit variant) from the xxHash family,
/// which provides excellent speed and quality for non-cryptographic purposes.
///
/// # Arguments
/// * `string` - The input string to hash
///
/// # Returns
/// A hexadecimal string representation of the 128-bit hash
///
/// # Examples
/// * `xxhash('hello')` returns a 32-character hex string
#[derive(Debug)]
pub struct XxHashFunc {
    signature: Signature,
}

impl Default for XxHashFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl XxHashFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for XxHashFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_gs_xxhash"
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
            let hash = xxh3_128(input.as_bytes());
            Some(format!("{:032x}", hash))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, StringArray};

    #[test]
    fn test_xxhash() {
        let func = XxHashFunc::new();

        let string_array = StringArray::from(vec![Some("hello"), Some("world"), None]);

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(string_array))],
            arg_fields: vec![Arc::new(arrow_schema::Field::new(
                "string",
                DataType::Utf8,
                false,
            ))],
            number_rows: 3,
            return_field: Arc::new(arrow_schema::Field::new("result", DataType::Utf8, false)),
        };

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let string_array = result_array.as_any().downcast_ref::<StringArray>().unwrap();

            assert!(!string_array.is_null(0));
            assert!(!string_array.is_null(1));
            assert!(string_array.is_null(2));

            // Check that we get consistent 32-character hex strings
            assert_eq!(string_array.value(0).len(), 32);
            assert_eq!(string_array.value(1).len(), 32);
        } else {
            panic!("Expected array result");
        }
    }
}
