use super::shared::json_quote;
use super::*;
use crate::register_string_aliases;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

#[test]
fn reports_missing_functions() {
    let missing = flink_only_functions(
        ["upper", "lower", "startsWith"],
        ["lower", "trim", "startswith"],
    );

    assert_eq!(
        missing.into_iter().collect::<Vec<_>>(),
        vec!["upper".to_string()]
    );
}

#[test]
fn ignores_case_and_whitespace_differences() {
    let missing = flink_only_functions(["  JSON_VALUE  "], ["json_value", "json_query"]);

    assert!(missing.is_empty());
}

#[test]
fn preserves_first_canonical_form() {
    let missing = flink_only_functions(["Json_Value", "JSON_VALUE"], ["json_other"]);

    assert_eq!(
        missing.into_iter().collect::<Vec<_>>(),
        vec!["Json_Value".to_string()]
    );
}

#[test]
fn skips_blank_entries() {
    let missing = flink_only_functions(["upper", "   "], ["upper"]);

    assert!(missing.is_empty());
}

#[test]
fn datafusion_builtin_names_are_not_empty() {
    let names = datafusion_builtin_function_names();

    assert!(!names.is_empty());
    assert!(names.contains("lower"));
}

#[test]
fn computes_difference_using_live_datafusion_registry() {
    let datafusion_names = datafusion_builtin_function_names();
    let missing = flink_only_functions(["lower", "STARTSWITH", "JSON_VALUE"], &datafusion_names);

    assert_eq!(
        missing.contains("STARTSWITH"),
        !datafusion_names.contains("STARTSWITH")
    );
    assert!(!missing.contains("lower"));
    assert_eq!(
        missing.contains("JSON_VALUE"),
        !datafusion_names.contains("JSON_VALUE")
    );
}

#[test]
fn json_quote_escapes_characters_like_flink() {
    assert_eq!(json_quote("foo"), "\"foo\"");
    assert_eq!(json_quote("foo\"bar"), "\"foo\\\"bar\"");
    assert_eq!(json_quote("foo/bar"), "\"foo\\/bar\"");
    assert_eq!(json_quote("line\nbreak"), "\"line\\nbreak\"");
    assert_eq!(json_quote("\u{00E9}"), "\"\\u00e9\"");
}

#[tokio::test]
async fn string_aliases_cover_common_spellings() {
    let ctx = SessionContext::new();
    register_string_aliases(&ctx).expect("alias registration succeeds");

    let df = ctx
        .sql(
            "SELECT \
                STARTSWITH('foobar', 'foo') AS starts_with_value, \
                ENDSWITH('foobar', 'bar') AS ends_with_value, \
                lowerCase('BAR') AS lower_case_value, \
                LEFT('foobar', 3) AS left_value, \
                SUBSTR('abcdef', 2, 3) AS substr_value",
        )
        .await
        .expect("query compiles");

    let batches = df.collect().await.expect("query executes");
    assert_eq!(batches.len(), 1);
    let starts_with_value =
        ScalarValue::try_from_array(batches[0].column(0), 0).expect("starts_with scalar");
    let ends_with_value =
        ScalarValue::try_from_array(batches[0].column(1), 0).expect("ends_with scalar");
    let lower_case_value =
        ScalarValue::try_from_array(batches[0].column(2), 0).expect("lower_case scalar");
    let left_value = ScalarValue::try_from_array(batches[0].column(3), 0).expect("left scalar");
    let substr_value = ScalarValue::try_from_array(batches[0].column(4), 0).expect("substr scalar");

    let string_value = |value: &ScalarValue| match value {
        ScalarValue::Utf8(Some(v)) => Some(v.as_str().to_string()),
        ScalarValue::Utf8View(Some(v)) => Some(v.to_string()),
        ScalarValue::Utf8(None) | ScalarValue::Utf8View(None) => None,
        _ => None,
    };

    assert_eq!(starts_with_value, ScalarValue::Boolean(Some(true)));
    assert_eq!(ends_with_value, ScalarValue::Boolean(Some(true)));
    assert_eq!(string_value(&lower_case_value).as_deref(), Some("bar"));
    assert_eq!(string_value(&left_value).as_deref(), Some("foo"));
    assert_eq!(string_value(&substr_value).as_deref(), Some("bcd"));
}

#[tokio::test]
async fn json_quote_udf_available_via_registration() {
    let ctx = SessionContext::new();
    register_json_functions(&ctx).expect("json registration succeeds");

    let df = ctx
        .sql("SELECT JSON_QUOTE('value'), JSON_QUOTE(CAST(NULL AS STRING))")
        .await
        .expect("query compiles");
    let batches = df.collect().await.expect("query executes");
    let first = ScalarValue::try_from_array(batches[0].column(0), 0).unwrap();
    let second = ScalarValue::try_from_array(batches[0].column(1), 0).unwrap();
    assert_eq!(first, ScalarValue::Utf8(Some("\"value\"".to_string())));
    assert_eq!(second, ScalarValue::Utf8(None));
}

#[tokio::test]
async fn json_exists_handles_scalar_and_array_inputs() {
    let ctx = SessionContext::new();
    register_json_functions(&ctx).expect("json registration succeeds");

    let df = ctx
        .sql("SELECT JSON_EXISTS('{\"a\":1}', '$.a') AS value")
        .await
        .expect("query compiles");
    let batches = df.collect().await.expect("query executes");
    let value = ScalarValue::try_from_array(batches[0].column(0), 0).unwrap();
    assert_eq!(value, ScalarValue::Boolean(Some(true)));

    let df_array = ctx
        .sql(
            "SELECT JSON_EXISTS(col_json, col_path) \
             FROM (VALUES ('{\"a\":1}', '$.a')) AS t(col_json, col_path)",
        )
        .await
        .expect("query compiles");
    let batches_array = df_array.collect().await.expect("query executes");
    let value_array = ScalarValue::try_from_array(batches_array[0].column(0), 0).unwrap();
    assert_eq!(value_array, ScalarValue::Boolean(Some(true)));
}

#[tokio::test]
async fn json_exists_on_error_behaviour_applies() {
    let ctx = SessionContext::new();
    register_json_functions(&ctx).expect("json registration succeeds");

    let df = ctx
        .sql("SELECT JSON_EXISTS('not json', '$.a', 'ERROR')")
        .await
        .expect("query compiles");
    let err = df.collect().await.expect_err("query should fail");
    assert!(
        err.to_string()
            .contains("JSON_EXISTS ON ERROR behavior triggered error")
    );

    let df_unknown = ctx
        .sql("SELECT JSON_EXISTS('not json', '$.a', 'UNKNOWN')")
        .await
        .expect("query compiles");
    let batches = df_unknown.collect().await.expect("query executes");
    let value = ScalarValue::try_from_array(batches[0].column(0), 0).unwrap();
    assert_eq!(value, ScalarValue::Boolean(None));
}

#[tokio::test]
async fn json_value_with_defaults() {
    let ctx = SessionContext::new();
    register_json_functions(&ctx).expect("json registration succeeds");

    let df = ctx
        .sql(
            "SELECT JSON_VALUE('{\"a\":null}', '$.a', 'STRING', 'DEFAULT', 'missing', 'NULL', NULL)",
        )
        .await
        .expect("query compiles");
    let batches = df.collect().await.expect("query executes");
    let value = ScalarValue::try_from_array(batches[0].column(0), 0).unwrap();
    assert_eq!(value, ScalarValue::Utf8(Some("missing".to_string())));
}

#[tokio::test]
async fn json_value_errors_on_invalid_defaults() {
    let ctx = SessionContext::new();
    register_json_functions(&ctx).expect("json registration succeeds");

    let df = ctx
        .sql("SELECT JSON_VALUE('not-json', '$.a', 'STRING', 'NULL', NULL, 'DEFAULT', 'oops')")
        .await
        .expect("query compiles");
    let batches = df.collect().await.expect("query executes");
    let value = ScalarValue::try_from_array(batches[0].column(0), 0).unwrap();
    assert_eq!(value, ScalarValue::Utf8(Some("oops".to_string())));

    let err = match ctx
        .sql("SELECT JSON_VALUE('not-json', '$.a', 'STRING', 'NULL', NULL, 'ERROR', NULL)")
        .await
    {
        Ok(df) => df
            .collect()
            .await
            .expect_err("expected JSON_VALUE ON ERROR failure"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("JSON_VALUE ON ERROR clause triggered error")
    );
}

#[tokio::test]
async fn json_query_behaviour_variants() {
    let ctx = SessionContext::new();
    register_json_functions(&ctx).expect("json registration succeeds");

    let df = ctx
        .sql("SELECT JSON_QUERY('{\"a\":{\"b\":[1,2]}}', '$.a') AS value")
        .await
        .expect("query compiles");
    let batches = df.collect().await.expect("query executes");
    let value = ScalarValue::try_from_array(batches[0].column(0), 0).expect("convert scalar value");
    assert_eq!(value, ScalarValue::Utf8(Some("{\"b\":[1,2]}".to_string())));

    let df_array = ctx
        .sql("SELECT JSON_QUERY('{\"a\":[1,2]}', '$.a') AS value")
        .await
        .expect("query compiles");
    let batches_array = df_array.collect().await.expect("query executes");
    let array_value =
        ScalarValue::try_from_array(batches_array[0].column(0), 0).expect("convert scalar value");
    assert_eq!(array_value, ScalarValue::Utf8(Some("[1,2]".to_string())));

    let df_scalar = ctx
        .sql("SELECT JSON_QUERY('{\"a\":1}', '$.a') AS value")
        .await
        .expect("query compiles");
    let batches_scalar = df_scalar.collect().await.expect("query executes");
    let scalar_value =
        ScalarValue::try_from_array(batches_scalar[0].column(0), 0).expect("convert scalar value");
    assert_eq!(scalar_value, ScalarValue::Utf8(None));
}

#[tokio::test]
async fn json_query_conditional_wrapper_handles_scalars_and_objects() {
    let ctx = SessionContext::new();
    register_json_functions(&ctx).expect("json registration succeeds");

    let df_object = ctx
        .sql(
            "SELECT JSON_QUERY('{\"a\":{\"b\":1}}', '$.a', 'STRING', \
             'WITH CONDITIONAL ARRAY', 'NULL', 'NULL') AS value",
        )
        .await
        .expect("query compiles");
    let batches_object = df_object.collect().await.expect("query executes");
    let value_object =
        ScalarValue::try_from_array(batches_object[0].column(0), 0).expect("convert scalar value");
    assert_eq!(
        value_object,
        ScalarValue::Utf8(Some("[{\"b\":1}]".to_string()))
    );

    let df_scalar = ctx
        .sql(
            "SELECT JSON_QUERY('1', '$', 'STRING', 'WITH CONDITIONAL ARRAY', 'NULL', 'NULL') AS value",
        )
        .await
        .expect("query compiles");
    let batches_scalar = df_scalar.collect().await.expect("query executes");
    let value_scalar =
        ScalarValue::try_from_array(batches_scalar[0].column(0), 0).expect("convert scalar value");
    assert_eq!(value_scalar, ScalarValue::Utf8(Some("[1]".to_string())));
}

#[tokio::test]
async fn json_query_unconditional_wrapper_nests_arrays() {
    let ctx = SessionContext::new();
    register_json_functions(&ctx).expect("json registration succeeds");

    let df = ctx
        .sql(
            "SELECT JSON_QUERY('[1,2]', '$', 'STRING', 'WITH UNCONDITIONAL ARRAY', 'NULL', 'NULL') AS value",
        )
        .await
        .expect("query compiles");
    let batches = df.collect().await.expect("query executes");
    let value = ScalarValue::try_from_array(batches[0].column(0), 0).expect("convert scalar value");
    assert_eq!(value, ScalarValue::Utf8(Some("[[1,2]]".to_string())));
}

#[tokio::test]
async fn json_query_on_empty_behavior_applies() {
    let ctx = SessionContext::new();
    register_json_functions(&ctx).expect("json registration succeeds");

    let df = ctx
        .sql(
            "SELECT JSON_QUERY('{}', '$.missing', 'STRING', 'WITHOUT ARRAY', 'EMPTY OBJECT', 'NULL') AS value",
        )
        .await
        .expect("query compiles");
    let batches = df.collect().await.expect("query executes");
    let value = ScalarValue::try_from_array(batches[0].column(0), 0).expect("convert scalar value");
    assert_eq!(value, ScalarValue::Utf8(Some("{}".to_string())));
}

#[tokio::test]
async fn json_query_on_error_behavior_applies() {
    let ctx = SessionContext::new();
    register_json_functions(&ctx).expect("json registration succeeds");

    let df = ctx
        .sql(
            "SELECT JSON_QUERY('not-json', '$.a', 'STRING', 'WITHOUT ARRAY', 'NULL', 'EMPTY ARRAY') AS value",
        )
        .await
        .expect("query compiles");
    let batches = df.collect().await.expect("query executes");
    let value = ScalarValue::try_from_array(batches[0].column(0), 0).expect("convert scalar value");
    assert_eq!(value, ScalarValue::Utf8(Some("[]".to_string())));
}

#[tokio::test]
async fn json_query_rejects_empty_object_with_array_return_type() {
    let ctx = SessionContext::new();
    register_json_functions(&ctx).expect("json registration succeeds");

    let err = ctx
        .sql(
            "SELECT JSON_QUERY('{}', '$', 'ARRAY<STRING>', 'WITHOUT ARRAY', 'EMPTY OBJECT', 'NULL') AS value",
        )
        .await
        .expect("query compiles")
        .collect()
        .await
        .expect_err("query should fail");

    assert!(
        err.to_string()
            .contains("RETURNING ARRAY does not support EMPTY OBJECT behavior")
    );
}
