//! VolatileNowFunc is a clone of the built-in NowFunc, but with an important difference:
//! it's Volatile, which means that the function is actually called for every batch. The built-in
//! NowFunc is Stable, which means that it returns the same value during the entire query execution.

use arrow_schema::{Field, FieldRef};
use datafusion::arrow::array::TimestampNanosecondArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::DataType::Timestamp;
use datafusion::arrow::datatypes::TimeUnit::Nanosecond;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

use crate::error::ResultExt;
use crate::{streamling_err, streamling_user_bail};

#[derive(Debug)]
pub struct VolatileNowFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for VolatileNowFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl VolatileNowFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Volatile),
            aliases: vec!["current_timestamp".to_string()],
        }
    }
}

impl ScalarUDFImpl for VolatileNowFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "now"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Field::new(
            self.name(),
            Timestamp(Nanosecond, Some("+00:00".into())),
            false,
        )
        .into())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(streamling_err!("return_field_from_args should be called instead").into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !args.args.is_empty() {
            streamling_user_bail!("{} function does not accept arguments", self.name());
        }

        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .streamling_context("Failed to get current time")?
            .as_nanos() as i64;

        let ts_vec = vec![now_nanos; args.number_rows];
        let array = TimestampNanosecondArray::from(ts_vec).with_timezone_utc();

        Ok(ColumnarValue::Array(Arc::new(array)))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use datafusion::arrow::array::Array;

    #[test]
    fn test_now_func_returns_different_timestamps_per_batch() {
        let func = VolatileNowFunc::new();

        let batch_sizes = [1, 3, 2];
        let mut timestamps = Vec::new();

        for &batch_size in &batch_sizes {
            let args = ScalarFunctionArgs {
                args: vec![],
                arg_fields: vec![],
                number_rows: batch_size,
                return_field: Field::new(
                    "now",
                    Timestamp(Nanosecond, Some("+00:00".into())),
                    false,
                )
                .into(),
            };

            let result = func.invoke_with_args(args).unwrap();

            if let ColumnarValue::Array(array) = result {
                let ts_array = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();

                // All timestamps in a batch should be the same
                let first_ts = ts_array.value(0);
                for i in 1..batch_size {
                    assert_eq!(
                        ts_array.value(i),
                        first_ts,
                        "All timestamps in a batch should be identical"
                    );
                }

                timestamps.push(first_ts);
            } else {
                panic!("Expected Array result");
            }

            // Small delay to ensure different timestamps between batches
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Verify that timestamps from different batches are different
        assert!(
            timestamps[0] < timestamps[1],
            "Second batch should have later timestamp"
        );
        assert!(
            timestamps[1] < timestamps[2],
            "Third batch should have later timestamp"
        );
    }
}
