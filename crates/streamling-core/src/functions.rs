pub use streamling_common::functions::*;

use crate::dynamic_table::DynamicTableRegistry;
use crate::functions::dynamic_table::DynamicTableCheckFunc;
use datafusion::logical_expr::ScalarUDF;

pub mod dynamic_table;

pub struct StreamlingFunctions {}

impl StreamlingFunctions {
    pub fn functions(dynamic_table_registry: DynamicTableRegistry) -> Vec<ScalarUDF> {
        let mut funcs = streamling_common::functions::CommonFunctions::functions();
        funcs.push(ScalarUDF::from(DynamicTableCheckFunc::new(
            dynamic_table_registry,
        )));
        funcs
    }
}
