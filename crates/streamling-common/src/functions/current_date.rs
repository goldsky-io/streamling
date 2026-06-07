//! VolatileCurrentDateFunc is a clone of the built-in CurrentDateFunc, but with an important difference:
//! it's Volatile, which means that the function is actually called for every batch. The built-in
//! CurrentDateFunc is Stable, which means that it returns the same value during the entire query execution.

use crate::streamling_user_bail;
use chrono::{Datelike, NaiveDate, Utc};
use datafusion::arrow::array::Date32Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::DataType::Date32;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct VolatileCurrentDateFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for VolatileCurrentDateFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl VolatileCurrentDateFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Volatile),
            aliases: vec![String::from("today")],
        }
    }
}

impl ScalarUDFImpl for VolatileCurrentDateFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "current_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !args.args.is_empty() {
            streamling_user_bail!("{} function does not accept arguments", self.name());
        }

        let now_utc = Utc::now();
        let days_since_epoch = now_utc.num_days_from_ce()
            - NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .num_days_from_ce();

        let date_vec = vec![days_since_epoch; args.number_rows];
        let array = Date32Array::from(date_vec);

        Ok(ColumnarValue::Array(Arc::new(array)))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}
