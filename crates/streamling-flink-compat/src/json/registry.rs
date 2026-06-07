use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::SessionContext;

use super::aggregates::json_arrayagg_udaf;
use super::aggregates::json_objectagg_udaf;
use super::constructors::{
    json_array_udf, json_object_udf, json_quote_udf, json_udf, json_unquote_udf,
};
use super::parsing::{is_json_udf, parse_json_udf, try_parse_json_udf};
use super::queries::{json_exists_udf, json_query_udf, json_value_udf};
use super::shared::JsonConstructorNullBehavior;

/// Register the Flink JSON compatibility layer with the provided
/// `SessionContext`.
///
/// This wires up constructors, query helpers, and the aggregate functions that
/// extend DataFusion’s built-ins with Flink semantic differences (such as the
/// ON EMPTY / ON ERROR behaviours).
pub fn register_json_functions(ctx: &SessionContext) -> DataFusionResult<()> {
    ctx.register_udf(json_quote_udf().with_aliases(["json_quote"]));
    ctx.register_udf(json_exists_udf().with_aliases(["json_exists"]));
    ctx.register_udf(json_array_udf(
        "JSON_ARRAY",
        ["json_array"],
        JsonConstructorNullBehavior::NullOnNull,
    ));
    ctx.register_udf(json_array_udf(
        "JSON_ARRAY_ABSENT_ON_NULL",
        ["json_array_absent_on_null"],
        JsonConstructorNullBehavior::AbsentOnNull,
    ));
    ctx.register_udf(json_object_udf(
        "JSON_OBJECT",
        ["json_object"],
        JsonConstructorNullBehavior::NullOnNull,
    ));
    ctx.register_udf(json_object_udf(
        "JSON_OBJECT_ABSENT_ON_NULL",
        ["json_object_absent_on_null"],
        JsonConstructorNullBehavior::AbsentOnNull,
    ));
    ctx.register_udf(json_udf().with_aliases(["json"]));
    ctx.register_udf(json_unquote_udf().with_aliases(["json_unquote"]));
    ctx.register_udf(json_query_udf().with_aliases(["json_query"]));
    ctx.register_udf(json_value_udf().with_aliases(["json_value"]));
    ctx.register_udf(parse_json_udf().with_aliases(["parse_json"]));
    ctx.register_udf(try_parse_json_udf().with_aliases(["try_parse_json"]));
    ctx.register_udf(is_json_udf().with_aliases(["is_json"]));
    ctx.register_udaf(json_arrayagg_udaf(
        "JSON_ARRAYAGG_NULL_ON_NULL",
        JsonConstructorNullBehavior::NullOnNull,
        ["json_arrayagg_null_on_null"],
    ));
    ctx.register_udaf(json_arrayagg_udaf(
        "JSON_ARRAYAGG_ABSENT_ON_NULL",
        JsonConstructorNullBehavior::AbsentOnNull,
        ["json_arrayagg_absent_on_null"],
    ));
    ctx.register_udaf(json_objectagg_udaf(
        "JSON_OBJECTAGG_NULL_ON_NULL",
        JsonConstructorNullBehavior::NullOnNull,
        ["json_objectagg_null_on_null"],
    ));
    ctx.register_udaf(json_objectagg_udaf(
        "JSON_OBJECTAGG_ABSENT_ON_NULL",
        JsonConstructorNullBehavior::AbsentOnNull,
        ["json_objectagg_absent_on_null"],
    ));
    Ok(())
}
