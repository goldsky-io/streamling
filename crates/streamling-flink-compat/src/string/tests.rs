use super::register_string_aliases;
use datafusion::arrow::array::{Array, ArrayRef, ListArray, StringArray};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

#[tokio::test]
async fn regexp_extract_matches_expected_groups() {
    let ctx = SessionContext::new();
    register_string_aliases(&ctx).expect("alias registration succeeds");

    let df = ctx
        .sql(
            "SELECT \
                REGEXP_EXTRACT('foothebar', 'foo(.*?)(bar)') AS match0, \
                REGEXP_EXTRACT('foothebar', 'foo(.*?)(bar)', 2) AS match2, \
                REGEXP_EXTRACT('abc', '(', 1) AS invalid_pattern, \
                REGEXP_EXTRACT('abc', '(a)(b)(c)', -1) AS negative_index",
        )
        .await
        .expect("query compiles");

    let batches = df.collect().await.expect("query executes");
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];

    let match0 = ScalarValue::try_from_array(batch.column(0), 0).expect("match0 scalar");
    let match2 = ScalarValue::try_from_array(batch.column(1), 0).expect("match2 scalar");
    let invalid = ScalarValue::try_from_array(batch.column(2), 0).expect("invalid scalar");
    let negative = ScalarValue::try_from_array(batch.column(3), 0).expect("negative scalar");

    assert_eq!(match0, ScalarValue::Utf8(Some("foothebar".to_string())));
    assert_eq!(match2, ScalarValue::Utf8(Some("bar".to_string())));
    assert_eq!(invalid, ScalarValue::Utf8(None));
    assert_eq!(negative, ScalarValue::Utf8(None));
}

#[tokio::test]
async fn regexp_extract_all_mirrors_flink_semantics() {
    let ctx = SessionContext::new();
    register_string_aliases(&ctx).expect("alias registration succeeds");

    let df = ctx
        .sql(
            "SELECT \
                REGEXP_EXTRACT_ALL('abcdeabde', '(ab)([a-z]+)(e)', 2) AS grouped, \
                REGEXP_EXTRACT_ALL('abcdeabde', 'abcdeabde') AS default_index_without_group, \
                REGEXP_EXTRACT_ALL('abcdeabde', '(abcdeabde)', 2) AS missing_group, \
                REGEXP_EXTRACT_ALL('100-200, 300-400', '(\\d+)-(\\d+)', 1) AS digits, \
                REGEXP_EXTRACT_ALL('100-200, 300-400', '[a-z]', 0) AS no_matches, \
                REGEXP_EXTRACT_ALL('abcdeabde', '(abcdeabde)', -1) AS negative_index",
        )
        .await
        .expect("query compiles");

    let batches = df.collect().await.expect("query executes");
    let batch = &batches[0];

    let grouped = list_column_values(batch.column(0));
    assert_eq!(grouped, Some(vec![Some("cdeabd".to_string())]));

    let default_index_without_group = list_column_values(batch.column(1));
    assert_eq!(default_index_without_group, None);

    let missing_group = list_column_values(batch.column(2));
    assert_eq!(missing_group, None);

    let digits = list_column_values(batch.column(3));
    assert_eq!(
        digits,
        Some(vec![Some("100".to_string()), Some("300".to_string())])
    );

    let no_matches = list_column_values(batch.column(4));
    assert_eq!(no_matches, Some(Vec::new()));

    let negative_index = list_column_values(batch.column(5));
    assert_eq!(negative_index, None);
}

#[tokio::test]
async fn regexp_substr_extracts_entire_match() {
    let ctx = SessionContext::new();
    register_string_aliases(&ctx).expect("alias registration succeeds");

    let df = ctx
        .sql(
            "SELECT \
                REGEXP_SUBSTR('hello world! Hello everyone!', 'Hello') AS first_match, \
                REGEXP_SUBSTR('100-200, 300-400', '(\\d+)-(\\d+)$') AS trailing_match, \
                REGEXP_SUBSTR('100-200, 300-400', '[a-z]') AS no_match, \
                REGEXP_SUBSTR('abc', '(') AS invalid_pattern",
        )
        .await
        .expect("query compiles");

    let batches = df.collect().await.expect("query executes");
    let batch = &batches[0];

    let first_match = ScalarValue::try_from_array(batch.column(0), 0).expect("first");
    let trailing = ScalarValue::try_from_array(batch.column(1), 0).expect("trailing");
    let no_match = ScalarValue::try_from_array(batch.column(2), 0).expect("no match");
    let invalid = ScalarValue::try_from_array(batch.column(3), 0).expect("invalid");

    assert_eq!(first_match, ScalarValue::Utf8(Some("Hello".to_string())));
    assert_eq!(trailing, ScalarValue::Utf8(Some("300-400".to_string())));
    assert_eq!(no_match, ScalarValue::Utf8(None));
    assert_eq!(invalid, ScalarValue::Utf8(None));
}

#[tokio::test]
async fn regexp_count_matches_flink_cases() {
    let ctx = SessionContext::new();
    register_string_aliases(&ctx).expect("alias registration succeeds");

    let df = ctx
        .sql(
            "SELECT \
                REGEXP_COUNT('hello world! Hello everyone!', 'Hello') AS case_sensitive, \
                REGEXP_COUNT('abcabcabc', 'abcab') AS overlapping, \
                REGEXP_COUNT('abcd', 'z') AS not_found, \
                REGEXP_COUNT('^abc', '\\^abc') AS escaped, \
                REGEXP_COUNT('a.b.c.d', '\\.') AS dot_matches, \
                REGEXP_COUNT('a*b*c*d', '\\*') AS star_matches, \
                REGEXP_COUNT('abc123xyz456', '\\d') AS digit_matches, \
                REGEXP_COUNT('abc', '(') AS invalid_pattern, \
                REGEXP_COUNT(CAST(NULL AS STRING), 'abc') AS null_text, \
                REGEXP_COUNT('abc', CAST(NULL AS STRING)) AS null_pattern",
        )
        .await
        .expect("query compiles");

    let batches = df.collect().await.expect("query executes");
    let batch = &batches[0];

    let value = |index| ScalarValue::try_from_array(batch.column(index), 0).unwrap();

    assert_eq!(value(0), ScalarValue::Int64(Some(1)));
    assert_eq!(value(1), ScalarValue::Int64(Some(1)));
    assert_eq!(value(2), ScalarValue::Int64(Some(0)));
    assert_eq!(value(3), ScalarValue::Int64(Some(1)));
    assert_eq!(value(4), ScalarValue::Int64(Some(3)));
    assert_eq!(value(5), ScalarValue::Int64(Some(3)));
    assert_eq!(value(6), ScalarValue::Int64(Some(6)));
    assert_eq!(value(7), ScalarValue::Int64(None));
    assert_eq!(value(8), ScalarValue::Int64(None));
    assert_eq!(value(9), ScalarValue::Int64(None));
}

#[tokio::test]
async fn regexp_instr_matches_flink_cases() {
    let ctx = SessionContext::new();
    register_string_aliases(&ctx).expect("alias registration succeeds");

    let df = ctx
        .sql(
            "SELECT \
                REGEXP_INSTR('hello world! Hello everyone!', 'Hello') AS first_match, \
                REGEXP_INSTR('hello world! Hello everyone!', 'Hello', 1, 2) AS second_match, \
                REGEXP_INSTR('abc', '(', 1, 1) AS invalid_pattern",
        )
        .await
        .expect("query compiles");

    let batches = df.collect().await.expect("query executes");
    let batch = &batches[0];

    let value = |index| ScalarValue::try_from_array(batch.column(index), 0).unwrap();

    assert_eq!(value(0), ScalarValue::Int64(Some(14)));
    assert_eq!(value(1), ScalarValue::Int64(Some(0)));
    assert_eq!(value(2), ScalarValue::Int64(None));
}

#[tokio::test]
async fn regexp_replace_matches_flink_cases() {
    let ctx = SessionContext::new();
    register_string_aliases(&ctx).expect("alias registration succeeds");

    let df = ctx
        .sql(
            "SELECT \
                REGEXP_REPLACE('hello world! Hello everyone!', 'Hello', 'Hi') AS replace_all, \
                REGEXP_REPLACE('abc', '(', 'x') AS invalid_pattern",
        )
        .await
        .expect("query compiles");

    let batches = df.collect().await.expect("query executes");
    let batch = &batches[0];

    let value = |index| ScalarValue::try_from_array(batch.column(index), 0).unwrap();

    assert_eq!(
        value(0),
        ScalarValue::Utf8(Some("hello world! Hi everyone!".to_string()))
    );
    assert_eq!(value(1), ScalarValue::Utf8(None));
}

#[tokio::test]
async fn instr_behaves_like_flink() {
    let ctx = SessionContext::new();
    register_string_aliases(&ctx).expect("alias registration succeeds");

    let df = ctx
        .sql(
            "SELECT \
                INSTR('foobarbar', 'bar') AS first_occurrence, \
                INSTR('foobarbar', 'bar', 1, 2) AS second_occurrence, \
                INSTR('foobar', 'baz') AS not_found",
        )
        .await
        .expect("query compiles");

    let batches = df.collect().await.expect("query executes");
    let batch = &batches[0];

    let value = |index| ScalarValue::try_from_array(batch.column(index), 0).unwrap();

    assert_eq!(value(0), ScalarValue::Int64(Some(4)));
    assert_eq!(value(1), ScalarValue::Int64(Some(7)));
    assert_eq!(value(2), ScalarValue::Int64(Some(0)));
}

#[tokio::test]
async fn locate_behaves_like_flink() {
    let ctx = SessionContext::new();
    register_string_aliases(&ctx).expect("alias registration succeeds");

    let df = ctx
        .sql(
            "SELECT \
                LOCATE('bar', 'foobarbar') AS first_occurrence, \
                LOCATE('bar', 'foobarbar', 5) AS search_from_index",
        )
        .await
        .expect("query compiles");

    let batches = df.collect().await.expect("query executes");
    let batch = &batches[0];

    let value = |index| ScalarValue::try_from_array(batch.column(index), 0).unwrap();

    assert_eq!(value(0), ScalarValue::Int64(Some(4)));
    assert_eq!(value(1), ScalarValue::Int64(Some(7)));
}

#[tokio::test]
async fn bin_behaves_like_flink() {
    let ctx = SessionContext::new();
    register_string_aliases(&ctx).expect("alias registration succeeds");

    let df = ctx
        .sql("SELECT BIN(42) AS positive, BIN(-5) AS negative, BIN(NULL) AS null_value")
        .await
        .expect("query compiles");

    let batches = df.collect().await.expect("query executes");
    let batch = &batches[0];

    assert_eq!(
        ScalarValue::try_from_array(batch.column(0), 0).unwrap(),
        ScalarValue::Utf8(Some("101010".to_string()))
    );
    assert_eq!(
        ScalarValue::try_from_array(batch.column(1), 0).unwrap(),
        ScalarValue::Utf8(Some("-101".to_string()))
    );
    assert_eq!(
        ScalarValue::try_from_array(batch.column(2), 0).unwrap(),
        ScalarValue::Utf8(None)
    );
}

#[tokio::test]
async fn elt_returns_selected_element() {
    let ctx = SessionContext::new();
    register_string_aliases(&ctx).expect("alias registration succeeds");

    let df = ctx
        .sql(
            "SELECT \
                ELT(2, 'foo', 'bar', 'baz') AS second_value, \
                ELT(4, 'foo', 'bar') AS out_of_range, \
                ELT(CAST(NULL AS INT), 'foo', 'bar') AS null_index",
        )
        .await
        .expect("query compiles");

    let batches = df.collect().await.expect("query executes");
    let batch = &batches[0];

    let value = |index| ScalarValue::try_from_array(batch.column(index), 0).unwrap();

    assert_eq!(value(0), ScalarValue::Utf8(Some("bar".to_string())));
    assert_eq!(value(1), ScalarValue::Utf8(None));
    assert_eq!(value(2), ScalarValue::Utf8(None));
}

#[tokio::test]
async fn parse_url_matches_flink_extracts() {
    let ctx = SessionContext::new();
    register_string_aliases(&ctx).expect("alias registration succeeds");

    let df = ctx
        .sql(
            "SELECT \
                PARSE_URL('http://user:pass@example.com/path?query=1#frag', 'HOST') AS host_value, \
                PARSE_URL('http://user:pass@example.com/path?query=1#frag', 'PATH') AS path_value, \
                PARSE_URL('http://user:pass@example.com/path?query=1#frag', 'QUERY') AS query_value, \
                PARSE_URL('http://user:pass@example.com/path?query=1#frag', 'QUERY', 'query') AS query_param, \
                PARSE_URL('http://user:pass@example.com/path?query=1#frag', 'REF') AS fragment_value, \
                PARSE_URL('http://user:pass@example.com/path?query=1#frag', 'FILE') AS file_value, \
                PARSE_URL('http://user:pass@example.com/path?query=1#frag', 'AUTHORITY') AS authority_value, \
                PARSE_URL('http://user:pass@example.com/path?query=1#frag', 'USERINFO') AS userinfo_value, \
                PARSE_URL('invalid:url', 'HOST') AS invalid_value"
        )
        .await
        .expect("query compiles");

    let batches = df.collect().await.expect("query executes");
    let batch = &batches[0];

    let value = |index| ScalarValue::try_from_array(batch.column(index), 0).unwrap();

    assert_eq!(value(0), ScalarValue::Utf8(Some("example.com".to_string())));
    assert_eq!(value(1), ScalarValue::Utf8(Some("/path".to_string())));
    assert_eq!(value(2), ScalarValue::Utf8(Some("query=1".to_string())));
    assert_eq!(value(3), ScalarValue::Utf8(Some("1".to_string())));
    assert_eq!(value(4), ScalarValue::Utf8(Some("frag".to_string())));
    assert_eq!(
        value(5),
        ScalarValue::Utf8(Some("/path?query=1".to_string()))
    );
    assert_eq!(
        value(6),
        ScalarValue::Utf8(Some("user:pass@example.com".to_string()))
    );
    assert_eq!(value(7), ScalarValue::Utf8(Some("user:pass".to_string())));
    assert_eq!(value(8), ScalarValue::Utf8(None));
}

#[tokio::test]
async fn split_preserves_tokens_like_flink() {
    let ctx = SessionContext::new();
    register_string_aliases(&ctx).expect("alias registration succeeds");

    let df = ctx
        .sql(
            "SELECT \
                SPLIT('123,123,23', ',') AS basic, \
                SPLIT('123,123,23', CAST(NULL AS STRING)) AS null_delimiter, \
                SPLIT(CAST(NULL AS STRING), ',') AS null_input, \
                SPLIT(',123,123', ',') AS leading_empty, \
                SPLIT(',123,123,', ',') AS trailing_empty, \
                SPLIT(',123,,,123,', ',') AS repeated_empty, \
                SPLIT('abc', '') AS character_split",
        )
        .await
        .expect("query compiles");

    let batches = df.collect().await.expect("query executes");
    let batch = &batches[0];

    assert_eq!(
        list_column_values(batch.column(0)),
        Some(vec![
            Some("123".to_string()),
            Some("123".to_string()),
            Some("23".to_string()),
        ])
    );
    assert_eq!(list_column_values(batch.column(1)), None);
    assert_eq!(list_column_values(batch.column(2)), None);
    assert_eq!(
        list_column_values(batch.column(3)),
        Some(vec![
            Some(String::new()),
            Some("123".to_string()),
            Some("123".to_string()),
        ])
    );
    assert_eq!(
        list_column_values(batch.column(4)),
        Some(vec![
            Some(String::new()),
            Some("123".to_string()),
            Some("123".to_string()),
            Some(String::new()),
        ])
    );
    assert_eq!(
        list_column_values(batch.column(5)),
        Some(vec![
            Some(String::new()),
            Some("123".to_string()),
            Some(String::new()),
            Some(String::new()),
            Some("123".to_string()),
            Some(String::new()),
        ])
    );
    assert_eq!(
        list_column_values(batch.column(6)),
        Some(vec![
            Some("a".to_string()),
            Some("b".to_string()),
            Some("c".to_string()),
        ])
    );
}

#[tokio::test]
async fn translate3_matches_flink_semantics() {
    let ctx = SessionContext::new();
    register_string_aliases(&ctx).expect("alias registration succeeds");

    let df = ctx
        .sql(
            "SELECT \
                TRANSLATE('hello', 'el', 'ip') AS swapped, \
                TRANSLATE('aba', 'ab', '12') AS digits, \
                TRANSLATE('aba', 'aa', '12') AS duplicate_mapping, \
                TRANSLATE('cat', 'at', 'o') AS removal, \
                TRANSLATE('abc', 'abc', CAST(NULL AS STRING)) AS drop_all, \
                TRANSLATE('abc', CAST(NULL AS STRING), 'xyz') AS null_from, \
                TRANSLATE(CAST(NULL AS STRING), 'abc', 'xyz') AS null_expr, \
                TRANSLATE('', 'abc', 'xyz') AS empty_expr",
        )
        .await
        .expect("query compiles");

    let batches = df.collect().await.expect("query executes");
    let batch = &batches[0];
    let value = |index| ScalarValue::try_from_array(batch.column(index), 0).unwrap();

    assert_eq!(value(0), ScalarValue::Utf8(Some("hippo".to_string())));
    assert_eq!(value(1), ScalarValue::Utf8(Some("121".to_string())));
    assert_eq!(value(2), ScalarValue::Utf8(Some("1b1".to_string())));
    assert_eq!(value(3), ScalarValue::Utf8(Some("co".to_string())));
    assert_eq!(value(4), ScalarValue::Utf8(Some(String::new())));
    assert_eq!(value(5), ScalarValue::Utf8(Some("abc".to_string())));
    assert_eq!(value(6), ScalarValue::Utf8(None));
    assert_eq!(value(7), ScalarValue::Utf8(Some(String::new())));
}

#[tokio::test]
async fn unhex_matches_flink_cases() {
    let ctx = SessionContext::new();
    register_string_aliases(&ctx).expect("alias registration succeeds");

    let df = ctx
        .sql(
            "SELECT \
                UNHEX('') AS empty_value, \
                UNHEX('1') AS single_nibble, \
                UNHEX('146') AS odd_length, \
                UNHEX('466C696E6B') AS flink_bytes, \
                UNHEX('z') AS invalid_char, \
                UNHEX('1-') AS invalid_pair",
        )
        .await
        .expect("query compiles");

    let batches = df.collect().await.expect("query executes");
    let batch = &batches[0];
    let value = |index| ScalarValue::try_from_array(batch.column(index), 0).unwrap();

    assert_eq!(value(0), ScalarValue::Binary(Some(vec![])));
    assert_eq!(value(1), ScalarValue::Binary(Some(vec![0])));
    assert_eq!(value(2), ScalarValue::Binary(Some(vec![0, 0x46])));
    assert_eq!(
        value(3),
        ScalarValue::Binary(Some(vec![0x46, 0x6C, 0x69, 0x6E, 0x6B]))
    );
    assert_eq!(value(4), ScalarValue::Binary(None));
    assert_eq!(value(5), ScalarValue::Binary(None));
}

#[tokio::test]
async fn url_encode_and_decode_match_flink() {
    let ctx = SessionContext::new();
    register_string_aliases(&ctx).expect("alias registration succeeds");

    let df = ctx
        .sql(
            "SELECT \
                URL_ENCODE('foo bar') AS encode_space, \
                URL_ENCODE('a+b/c') AS encode_special, \
                URL_ENCODE(CAST(NULL AS STRING)) AS encode_null, \
                URL_ENCODE('☃') AS encode_unicode, \
                URL_DECODE('foo+bar') AS decode_space, \
                URL_DECODE('a%2Bb%2Fc') AS decode_special, \
                URL_DECODE('%2') AS decode_invalid_short, \
                URL_DECODE('%zz') AS decode_invalid_hex, \
                URL_DECODE(CAST(NULL AS STRING)) AS decode_null, \
                URL_DECODE(URL_ENCODE('☃')) AS roundtrip_unicode",
        )
        .await
        .expect("query compiles");

    let batches = df.collect().await.expect("query executes");
    let batch = &batches[0];
    let value = |index| ScalarValue::try_from_array(batch.column(index), 0).unwrap();

    assert_eq!(value(0), ScalarValue::Utf8(Some("foo+bar".to_string())));
    assert_eq!(value(1), ScalarValue::Utf8(Some("a%2Bb%2Fc".to_string())));
    assert_eq!(value(2), ScalarValue::Utf8(None));
    assert_eq!(value(3), ScalarValue::Utf8(Some("%E2%98%83".to_string())));
    assert_eq!(value(4), ScalarValue::Utf8(Some("foo bar".to_string())));
    assert_eq!(value(5), ScalarValue::Utf8(Some("a+b/c".to_string())));
    assert_eq!(value(6), ScalarValue::Utf8(None));
    assert_eq!(value(7), ScalarValue::Utf8(None));
    assert_eq!(value(8), ScalarValue::Utf8(None));
    assert_eq!(value(9), ScalarValue::Utf8(Some("☃".to_string())));
}

#[tokio::test]
async fn regexp_alias_accepts_lowercase_calls() {
    let ctx = SessionContext::new();
    register_string_aliases(&ctx).expect("alias registration succeeds");

    let df = ctx
        .sql("SELECT regexp('Flink', 'F.*') AS positive, regexp('Flink', 'foo') AS negative")
        .await
        .expect("query compiles");

    let batches = df.collect().await.expect("query executes");
    let batch = &batches[0];
    let value = |index| ScalarValue::try_from_array(batch.column(index), 0).unwrap();

    assert_eq!(value(0), ScalarValue::Boolean(Some(true)));
    assert_eq!(value(1), ScalarValue::Boolean(Some(false)));
}

#[tokio::test]
async fn like_and_similar_behave_like_flink() {
    let ctx = SessionContext::new();
    register_string_aliases(&ctx).expect("alias registration succeeds");

    let df = ctx
        .sql(
            "SELECT \
                'foobar' LIKE 'foo%' AS like_match, \
                'foobar' LIKE 'bar%' AS like_miss, \
                CAST(NULL AS STRING) LIKE 'foo%' AS like_null, \
                SIMILAR('foobar', 'foo.*') AS similar_match, \
                SIMILAR('bar', 'foo.*') AS similar_miss, \
                SIMILAR(CAST(NULL AS STRING), 'foo.*') AS similar_null",
        )
        .await
        .expect("query compiles");

    let batches = df.collect().await.expect("query executes");
    let batch = &batches[0];
    let value = |index| ScalarValue::try_from_array(batch.column(index), 0).unwrap();

    assert_eq!(value(0), ScalarValue::Boolean(Some(true)));
    assert_eq!(value(1), ScalarValue::Boolean(Some(false)));
    assert_eq!(value(2), ScalarValue::Boolean(None));
    assert_eq!(value(3), ScalarValue::Boolean(Some(true)));
    assert_eq!(value(4), ScalarValue::Boolean(Some(false)));
    assert_eq!(value(5), ScalarValue::Boolean(None));
}

fn list_column_values(column: &ArrayRef) -> Option<Vec<Option<String>>> {
    let list_array = column.as_any().downcast_ref::<ListArray>().unwrap();
    if list_array.is_null(0) {
        return None;
    }

    let values = list_array.value(0);
    let string_array = values.as_any().downcast_ref::<StringArray>().unwrap();
    let mut result = Vec::with_capacity(string_array.len());
    for i in 0..string_array.len() {
        if string_array.is_null(i) {
            result.push(None);
        } else {
            result.push(Some(string_array.value(i).to_string()));
        }
    }
    Some(result)
}
