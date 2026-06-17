//! UUID7Func generates UUID version 7 strings.
//! UUID v7 is time-ordered and includes a timestamp component, making it suitable for use as a
//! database primary key or for sorting by creation time.

use arrow_schema::Field;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;
use uuid::Uuid;

use crate::{streamling_err, streamling_user_bail};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Uuid7Func {
    signature: Signature,
}

impl Default for Uuid7Func {
    fn default() -> Self {
        Self::new()
    }
}

impl Uuid7Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for Uuid7Func {
    fn name(&self) -> &str {
        "uuid7"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<arrow_schema::FieldRef> {
        Ok(Field::new(self.name(), DataType::Utf8, false).into())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(streamling_err!("return_field_from_args should be called instead").into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !args.args.is_empty() {
            streamling_user_bail!("{} function does not accept arguments", self.name());
        }

        // Generate a UUID v7 for each row in the batch
        let mut builder = datafusion::arrow::array::StringBuilder::new();
        for _ in 0..args.number_rows {
            let uuid = Uuid::now_v7();
            builder.append_value(uuid.to_string());
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use datafusion::arrow::array::{Array, StringArray};

    #[test]
    fn test_uuid7_func_generates_valid_uuids() {
        let func = Uuid7Func::new();

        let args = ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![],
            number_rows: 3,
            return_field: Field::new("uuid7", DataType::Utf8, false).into(),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();

            assert_eq!(string_array.len(), 3);

            // Verify each value is a valid UUID v7
            for i in 0..3 {
                let uuid_str = string_array.value(i);
                let uuid = Uuid::parse_str(uuid_str).expect("Should be a valid UUID");

                // Verify it's version 7 (SortRand is the enum value for UUID v7)
                assert_eq!(uuid.get_version(), Some(uuid::Version::SortRand));
            }

            // Verify UUIDs are different (very unlikely to be the same)
            assert_ne!(string_array.value(0), string_array.value(1));
            assert_ne!(string_array.value(1), string_array.value(2));
        } else {
            panic!("Expected Array result");
        }
    }

    #[test]
    fn test_uuid7_func_rejects_arguments() {
        let func = Uuid7Func::new();

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(
                datafusion::common::ScalarValue::Utf8(Some("test".to_string())),
            )],
            arg_fields: vec![Field::new("arg", DataType::Utf8, false).into()],
            number_rows: 1,
            return_field: Field::new("uuid7", DataType::Utf8, false).into(),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("does not accept arguments")
        );
    }
}
