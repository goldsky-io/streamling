pub use streamling_common::functions::*;

use crate::dynamic_table::DynamicTableRegistry;
use crate::functions::dynamic_table::DynamicTableCheckFunc;
use crate::functions::lookup::LookupEqFunc;
use crate::table::TableRegistry;
use crate::table::read::TableUdf;
use datafusion::logical_expr::ScalarUDF;

pub mod dynamic_table;
pub mod lookup;

pub struct StreamlingFunctions {}

impl StreamlingFunctions {
    pub fn functions(
        dynamic_table_registry: DynamicTableRegistry,
        tables: TableRegistry,
    ) -> Vec<ScalarUDF> {
        let mut funcs = streamling_common::functions::CommonFunctions::functions();
        funcs.push(ScalarUDF::from(DynamicTableCheckFunc::new(
            dynamic_table_registry,
        )));
        funcs.push(ScalarUDF::new_from_impl(TableUdf::new(LookupEqFunc::new(
            tables,
        ))));
        funcs
    }
}
