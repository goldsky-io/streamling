//! VolatileCurrentTimeFunc is a clone of the built-in CurrentTimeFunc, but with an important difference:
//! it's Volatile, which means that the function is actually called for every batch. The built-in
//! CurrentTimeFunc is Stable, which means that it returns the same value during the entire query execution.

use datafusion::arrow::array::Time64NanosecondArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::DataType::Time64;
use datafusion::arrow::datatypes::TimeUnit::Nanosecond;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

use crate::error::ResultExt;
use crate::streamling_user_bail;

#[derive(Debug)]
pub struct VolatileCurrentTimeFunc {
    signature: Signature,
}

impl Default for VolatileCurrentTimeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl VolatileCurrentTimeFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for VolatileCurrentTimeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "current_time"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Time64(Nanosecond))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !args.args.is_empty() {
            streamling_user_bail!("{} function does not accept arguments", self.name());
        }

        // Get current time in nanoseconds since Unix epoch, then extract time portion (nanoseconds since midnight)
        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .streamling_context("Failed to get current time")?
            .as_nanos() as i64;

        // Extract time portion (nanoseconds since midnight)
        let time_nanos = now_nanos % 86400000000000; // 24 * 60 * 60 * 1_000_000_000

        let time_vec = vec![time_nanos; args.number_rows];
        let array = Time64NanosecondArray::from(time_vec);

        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use datafusion::arrow::array::Array;

    #[test]
    fn test_current_time_func_returns_different_times_per_batch() {
        let func = VolatileCurrentTimeFunc::new();

        let batch_sizes = [1, 3, 2];
        let mut times = Vec::new();

        for &batch_size in &batch_sizes {
            let args = ScalarFunctionArgs {
                args: vec![],
                arg_fields: vec![],
                number_rows: batch_size,
                return_field: Field::new("current_time", Time64(Nanosecond), false).into(),
            };

            let result = func.invoke_with_args(args).unwrap();

            if let ColumnarValue::Array(array) = result {
                let time_array = array
                    .as_any()
                    .downcast_ref::<Time64NanosecondArray>()
                    .unwrap();

                // All times in a batch should be the same
                let first_time = time_array.value(0);
                for i in 1..batch_size {
                    assert_eq!(
                        time_array.value(i),
                        first_time,
                        "All times in a batch should be identical"
                    );
                }

                times.push(first_time);
            } else {
                panic!("Expected Array result");
            }

            // Small delay to ensure different times between batches
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Verify that times from different batches are different
        assert!(times[0] < times[1], "Second batch should have later time");
        assert!(times[1] < times[2], "Third batch should have later time");
    }
}
