//! Second-round adversarial SQL checks; production source is unchanged.
use arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, LargeBinaryArray, LargeListArray, StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use streamling_common::types::decimal_arb::{
    DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue,
};
use streamling_core::dynamic_table::DynamicTableRegistry;
use streamling_core::session::SessionManager;

fn decimal_values(name: &str, scale: u32, vals: &[Option<&str>]) -> ArrayRef {
    let mut b = DecimalArbArrayBuilder::with_capacity(vals.len(), name, 100, scale).unwrap();
    for v in vals {
        match v {
            Some(v) => b.append_str(v).unwrap(),
            None => b.append_null(),
        }
    }
    Arc::new(b.finish().into_inner().0)
}
fn session() -> SessionManager {
    let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new(), 1).unwrap();
    let a = decimal_values("a", 0, &[Some("255"), Some("256"), Some("-1"), None]);
    let b = decimal_values("b", 2, &[Some("256"), Some("255"), Some("0"), Some("1")]);
    let c = decimal_values("c", 0, &[Some("256"), Some("256"), Some("0"), Some("1")]);
    let array_child = Arc::new(DecimalArbType::field("item", 100, 0, true).unwrap());
    let list = LargeListArray::new(
        array_child.clone(),
        OffsetBuffer::new(vec![0i64, 2, 4, 6, 8].into()),
        decimal_values(
            "item",
            0,
            &[
                Some("255"),
                Some("256"),
                Some("-1"),
                Some("0"),
                None,
                Some("255"),
                Some("0"),
                None,
            ],
        ),
        None,
    );
    let nested = StructArray::new(
        vec![Arc::new(
            DecimalArbType::field("amount", 100, 0, true).unwrap(),
        )]
        .into(),
        vec![a.clone()],
        None,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        DecimalArbType::field("a", 100, 0, true).unwrap(),
        DecimalArbType::field("b", 100, 2, true).unwrap(),
        DecimalArbType::field("c", 100, 0, true).unwrap(),
        Field::new("vals", DataType::LargeList(array_child), true),
        Field::new("nested", nested.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            a,
            b,
            c,
            Arc::new(list),
            Arc::new(nested),
        ],
    )
    .unwrap();
    sm.session_context().register_batch("t", batch).unwrap();
    let narrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("a", DataType::Decimal128(30, 0), true),
        Field::new("b", DataType::Decimal128(30, 2), true),
        Field::new("c", DataType::Decimal128(30, 0), true),
    ]));
    let narrow = RecordBatch::try_new(
        narrow_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(
                arrow::array::Decimal128Array::from(vec![Some(255i128), Some(256), Some(-1), None])
                    .with_precision_and_scale(30, 0)
                    .unwrap(),
            ),
            Arc::new(
                arrow::array::Decimal128Array::from(vec![
                    Some(25600i128),
                    Some(25500),
                    Some(0),
                    Some(100),
                ])
                .with_precision_and_scale(30, 2)
                .unwrap(),
            ),
            Arc::new(
                arrow::array::Decimal128Array::from(vec![
                    Some(256i128),
                    Some(256),
                    Some(0),
                    Some(1),
                ])
                .with_precision_and_scale(30, 0)
                .unwrap(),
            ),
        ],
    )
    .unwrap();
    sm.session_context().register_batch("n", narrow).unwrap();
    sm
}
async fn run(sm: &SessionManager, sql: &str) -> datafusion::error::Result<Vec<RecordBatch>> {
    let (plan, _) = sm.create_supported_logical_plan(sql.to_owned()).await?;
    let df = sm.new_df(plan);
    let optimized = df.clone().into_optimized_plan()?;
    eprintln!("SQL {sql}\nPLAN {}", optimized.display_indent());
    let result = df.collect().await;
    eprintln!("RESULT {result:?}");
    result
}
fn boolean_rows(batches: &[RecordBatch]) -> Vec<Option<bool>> {
    batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        })
        .collect()
}
fn decimal_rows(batches: &[RecordBatch], scale: u32) -> Vec<Option<String>> {
    batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            (0..a.len())
                .map(|i| {
                    (!a.is_null(i)).then(|| {
                        DecimalArbValue::from_canonical_bytes_at_scale(a.value(i), scale)
                            .unwrap()
                            .as_bigdecimal()
                            .normalized()
                            .to_string()
                    })
                })
                .collect::<Vec<_>>()
        })
        .collect()
}
macro_rules! check_bool {
    ($name:ident,$sql:literal,$expected:expr) => {
        #[tokio::test]
        async fn $name() {
            let actual = run(&session(), $sql).await.unwrap();
            assert_eq!(boolean_rows(&actual), $expected);
        }
    };
}
macro_rules! check_decimal {
    ($name:ident,$sql:literal,$scale:expr,$expected:expr) => {
        #[tokio::test]
        async fn $name() {
            let actual = run(&session(), $sql).await.unwrap();
            assert_eq!(decimal_rows(&actual, $scale), $expected);
        }
    };
}
check_decimal!(
    greatest_null_arg_keeps_numeric_order,
    "SELECT greatest(a,c,NULL) FROM t WHERE id=1",
    0,
    vec![Some("256".into())]
);
check_decimal!(
    least_null_arg_keeps_numeric_order,
    "SELECT least(a,c,NULL) FROM t WHERE id=1",
    0,
    vec![Some("255".into())]
);
check_decimal!(
    greatest_three_args,
    "SELECT greatest(a,c,to_decimal_arb_from_string('257',100,0)) FROM t WHERE id=1",
    0,
    vec![Some("257".into())]
);
check_bool!(
    greatest_integer_parent_comparison,
    "SELECT greatest(a,0)<c FROM t WHERE id=1",
    vec![Some(true)]
);
check_bool!(
    greatest_mixed_scale_parent_comparison,
    "SELECT greatest(a,b)<to_decimal_arb_from_string('257',100,0) FROM t WHERE id=1",
    vec![Some(true)]
);
check_bool!(
    least_mixed_scale_parent_comparison,
    "SELECT least(a,b)<c FROM t WHERE id=1",
    vec![Some(true)]
);
check_decimal!(
    list_column_array_min,
    "SELECT array_min(vals) FROM t",
    0,
    vec![
        Some("255".into()),
        Some("-1".into()),
        Some("255".into()),
        Some("0".into())
    ]
);
check_decimal!(
    list_column_array_max,
    "SELECT array_max(vals) FROM t",
    0,
    vec![
        Some("256".into()),
        Some("0".into()),
        Some("255".into()),
        Some("0".into())
    ]
);
check_bool!(
    list_element_comparison,
    "SELECT vals[1]<vals[2] FROM t WHERE id IN(1,2)",
    vec![Some(true), Some(true)]
);
check_bool!(
    literal_array_element_comparison,
    "SELECT ([a,c])[1]<c FROM t WHERE id=1",
    vec![Some(true)]
);
check_bool!(
    nested_struct_comparison,
    "SELECT nested.amount<c FROM t WHERE id=1",
    vec![Some(true)]
);
check_bool!(
    simple_case_null_when_still_compares_numerically,
    "SELECT CASE c WHEN NULL THEN false WHEN b THEN true ELSE false END FROM t WHERE id=1",
    vec![Some(true)]
);
check_bool!(
    nullif_null_keeps_metadata_for_parent,
    "SELECT NULLIF(a,NULL)<c FROM t WHERE id=1",
    vec![Some(true)]
);
check_bool!(
    coalesce_mixed_scales_parent_comparison,
    "SELECT coalesce(a,b)<c FROM t WHERE id=1",
    vec![Some(true)]
);
check_bool!(
    case_mixed_scales_parent_comparison,
    "SELECT (CASE WHEN id=1 THEN a ELSE b END)<c FROM t WHERE id=1",
    vec![Some(true)]
);
check_bool!(
    list_array_has_mixed_scale,
    "SELECT array_has(vals,b) FROM t WHERE id=1",
    vec![Some(true)]
);

#[tokio::test]
#[ignore = "Exploratory diagnostic only: prints query results without a correctness assertion; focused regressions below cover demonstrated failures"]
async fn deep_cast_and_arithmetic_probe() {
    let sm = session();
    for sql in [
        "SELECT greatest(a,0)+1 FROM t WHERE id=1",
        "SELECT (CASE WHEN id=1 THEN a ELSE c END)+1 FROM t WHERE id=1",
        "SELECT coalesce(a,c)+1 FROM t WHERE id=1",
        "SELECT CAST(vals[1] AS VARCHAR) FROM t WHERE id=1",
        "SELECT CAST(greatest(a,0) AS VARCHAR) FROM t WHERE id=1",
        "SELECT CAST(greatest(a,b) AS VARCHAR) FROM t WHERE id=1",
        "SELECT CAST(NULLIF(a,NULL) AS VARCHAR) FROM t WHERE id=1",
        "SELECT ARRAY_DISTINCT([a,to_decimal_arb_from_string('255',100,2)]) FROM t WHERE id=1",
        "SELECT array_sort(vals) FROM t WHERE id=1",
        "SELECT coalesce(a,NULL)<c FROM t WHERE id=1",
        "SELECT a IS NOT DISTINCT FROM NULL FROM t",
        "SELECT a NOT IN(NULL,to_decimal_arb_from_string('255',100,2)) FROM t",
    ] {
        let result = run(&sm, sql).await;
        eprintln!("PROBE {sql}\n{result:?}");
    }
}

static NEXT_DECIMAL: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(256);
#[derive(Debug, PartialEq, Eq, Hash)]
struct NextDecimal {
    nullable: bool,
    signature: datafusion::logical_expr::Signature,
}
impl datafusion::logical_expr::ScalarUDFImpl for NextDecimal {
    fn name(&self) -> &str {
        "next_decimal_v2"
    }
    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::LargeBinary)
    }
    fn return_field_from_args(
        &self,
        _: datafusion::logical_expr::ReturnFieldArgs,
    ) -> datafusion::error::Result<arrow_schema::FieldRef> {
        Ok(Arc::new(DecimalArbType::field(
            "next_decimal_v2",
            100,
            0,
            self.nullable,
        )?))
    }
    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::error::Result<datafusion::logical_expr::ColumnarValue> {
        let mut b =
            DecimalArbArrayBuilder::with_capacity(args.number_rows, "next_decimal_v2", 100, 0)?;
        for _ in 0..args.number_rows {
            let next = NEXT_DECIMAL.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            b.append_str(&next.to_string())?;
        }
        Ok(datafusion::logical_expr::ColumnarValue::Array(Arc::new(
            b.finish().into_inner().0,
        )))
    }
}
#[tokio::test]
#[serial_test::serial(decimal_sequence_v2)]
async fn greatest_evaluates_volatile_operand_once() {
    let sm = session();
    NEXT_DECIMAL.store(256, std::sync::atomic::Ordering::SeqCst);
    sm.session_context()
        .register_udf(datafusion::logical_expr::ScalarUDF::from(NextDecimal {
            nullable: false,
            signature: datafusion::logical_expr::Signature::exact(
                vec![],
                datafusion::logical_expr::Volatility::Volatile,
            ),
        }));
    let batches = run_once(
        &sm,
        "SELECT greatest(next_decimal_v2(),c) FROM t WHERE id=1",
    )
    .await
    .unwrap();
    eprintln!(
        "VOLATILE CALLS {}",
        NEXT_DECIMAL.load(std::sync::atomic::Ordering::SeqCst) - 256
    );
    assert_eq!(decimal_rows(&batches, 0), vec![Some("257".into())]);
}

fn volatile_session() -> SessionManager {
    let sm = session();
    NEXT_DECIMAL.store(256, std::sync::atomic::Ordering::SeqCst);
    sm.session_context()
        .register_udf(datafusion::logical_expr::ScalarUDF::from(NextDecimal {
            nullable: false,
            signature: datafusion::logical_expr::Signature::exact(
                vec![],
                datafusion::logical_expr::Volatility::Volatile,
            ),
        }));
    sm
}
#[tokio::test]
#[serial_test::serial(decimal_sequence_v2)]
async fn nullif_evaluates_volatile_operand_once() {
    let batches = run_once(
        &volatile_session(),
        "SELECT nullif(next_decimal_v2(),c) FROM t WHERE id=1",
    )
    .await
    .unwrap();
    eprintln!(
        "VOLATILE CALLS {}",
        NEXT_DECIMAL.load(std::sync::atomic::Ordering::SeqCst) - 256
    );
    assert_eq!(decimal_rows(&batches, 0), vec![Some("257".into())]);
}
#[tokio::test]
#[serial_test::serial(decimal_sequence_v2)]
async fn simple_case_evaluates_volatile_operand_once() {
    let batches=run_once(&volatile_session(),"SELECT CASE next_decimal_v2() WHEN to_decimal_arb_from_string('0',100,0) THEN 1 WHEN to_decimal_arb_from_string('257',100,0) THEN 2 ELSE 3 END FROM t WHERE id=1").await.unwrap();
    let result = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    eprintln!(
        "VOLATILE CALLS {}",
        NEXT_DECIMAL.load(std::sync::atomic::Ordering::SeqCst) - 256
    );
    assert_eq!(result, 2);
}
#[tokio::test]
#[serial_test::serial(decimal_sequence_v2)]
async fn nullsafe_equality_evaluates_volatile_operand_once() {
    let batches=run_once(&volatile_session(),"SELECT next_decimal_v2() IS NOT DISTINCT FROM to_decimal_arb_from_string('257',100,0) FROM t WHERE id=1").await.unwrap();
    eprintln!(
        "VOLATILE CALLS {}",
        NEXT_DECIMAL.load(std::sync::atomic::Ordering::SeqCst) - 256
    );
    assert_eq!(boolean_rows(&batches), vec![Some(true)]);
}

check_bool!(
    array_distinct_cross_scale_equivalence,
    "SELECT cardinality(array_distinct([a,to_decimal_arb_from_string('255',100,2)]))=1 FROM t WHERE id=1",
    vec![Some(true)]
);
check_bool!(
    literal_array_cross_scale_equality,
    "SELECT [a]=[to_decimal_arb_from_string('255',100,2)] FROM t WHERE id=1",
    vec![Some(true)]
);

#[tokio::test]
async fn array_sort_column_uses_numeric_order() {
    let batches = run(&session(), "SELECT array_sort(vals) FROM t WHERE id=1")
        .await
        .unwrap();
    let list = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<LargeListArray>()
        .unwrap();
    let values = list.value(0);
    let arr = values.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    let actual = (0..arr.len())
        .map(|i| {
            DecimalArbValue::from_canonical_bytes_at_scale(arr.value(i), 0)
                .unwrap()
                .to_canonical_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, vec!["255", "256"]);
}

async fn query_booleans(sm: &SessionManager, sql: &str) -> Result<Vec<Option<bool>>, String> {
    let (plan, _) = sm
        .create_supported_logical_plan(sql.to_owned())
        .await
        .map_err(|e| e.to_string())?;
    let batches = sm.new_df(plan).collect().await.map_err(|e| e.to_string())?;
    Ok(boolean_rows(&batches))
}
#[tokio::test]
async fn differential_decimal128_expression_matrix() {
    let sm = session();
    let atoms = [
        "a",
        "b",
        "c",
        "coalesce(a,c)",
        "coalesce(a,b)",
        "coalesce(a,NULL)",
        "nullif(a,c)",
        "nullif(a,NULL)",
        "greatest(a,c)",
        "greatest(a,b)",
        "greatest(a,c,NULL)",
        "greatest(a,0)",
        "least(a,c)",
        "least(a,b)",
        "CASE WHEN id=1 THEN a ELSE c END",
        "CASE WHEN id=1 THEN a ELSE b END",
    ];
    let expressions = atoms
        .iter()
        .flat_map(|a| {
            [
                a.to_string(),
                format!("coalesce(({a}),c)"),
                format!("CASE WHEN id=1 THEN ({a}) ELSE c END"),
            ]
        })
        .collect::<Vec<_>>();
    let mut equal = 0usize;
    let mut wrong = Vec::new();
    let mut rejected = Vec::new();
    let mut oracle_rejected = Vec::new();
    for left in &expressions {
        for op in [
            "<",
            "<=",
            "=",
            "!=",
            "IS DISTINCT FROM",
            "IS NOT DISTINCT FROM",
        ] {
            for right in ["a", "b", "c", "0"] {
                let predicate = format!("({left}) {op} {right}");
                let expected = query_booleans(&sm, &format!("SELECT {predicate} FROM n")).await;
                let actual = query_booleans(&sm, &format!("SELECT {predicate} FROM t")).await;
                match (expected,actual) {
                    (Ok(expected),Ok(actual)) if expected==actual=>equal+=1,
                    (Ok(expected),Ok(actual))=>wrong.push(serde_json::json!({"predicate":predicate,"actual":actual,"expected":expected})),
                    (Ok(_),Err(e))=>rejected.push(serde_json::json!({"predicate":predicate,"error":e.lines().next().unwrap_or("")})),
                    (Err(e),_)=>oracle_rejected.push(serde_json::json!({"predicate":predicate,"error":e.lines().next().unwrap_or("")})),
                }
            }
        }
    }
    eprintln!(
        "MATRIX_COUNTS equal={equal} wrong={} rejected={} oracle_rejected={}",
        wrong.len(),
        rejected.len(),
        oracle_rejected.len()
    );
    eprintln!(
        "MATRIX_REPORT={}",
        serde_json::json!({"equal":equal,"wrong":wrong,"rejected":rejected,"oracle_rejected":oracle_rejected})
    );
    assert!(
        wrong.is_empty(),
        "{} predicates produced different Boolean/null outcomes from native Decimal128; see MATRIX_REPORT",
        wrong.len()
    );
}

async fn run_once(sm: &SessionManager, sql: &str) -> datafusion::error::Result<Vec<RecordBatch>> {
    let (plan, _) = sm.create_supported_logical_plan(sql.to_owned()).await?;
    eprintln!("RUN_ONCE {sql}\n{}", plan.display_indent());
    sm.new_df(plan).collect().await
}
#[tokio::test]
#[serial_test::serial(decimal_sequence_v2)]
async fn volatile_operand_control() {
    let batches = run_once(
        &volatile_session(),
        "SELECT next_decimal_v2() FROM t WHERE id=1",
    )
    .await
    .unwrap();
    assert_eq!(decimal_rows(&batches, 0), vec![Some("257".into())]);
    assert_eq!(NEXT_DECIMAL.load(std::sync::atomic::Ordering::SeqCst), 257);
}
#[tokio::test]
#[serial_test::serial(decimal_sequence_v2)]
async fn nullable_nullsafe_equality_evaluates_volatile_once() {
    let sm = session();
    NEXT_DECIMAL.store(256, std::sync::atomic::Ordering::SeqCst);
    sm.session_context()
        .register_udf(datafusion::logical_expr::ScalarUDF::from(NextDecimal {
            nullable: true,
            signature: datafusion::logical_expr::Signature::exact(
                vec![],
                datafusion::logical_expr::Volatility::Volatile,
            ),
        }));
    let batches=run_once(&sm,"SELECT next_decimal_v2() IS NOT DISTINCT FROM to_decimal_arb_from_string('257',100,0) FROM t WHERE id=1").await.unwrap();
    eprintln!(
        "NULLABLE_VOLATILE_CALLS {}",
        NEXT_DECIMAL.load(std::sync::atomic::Ordering::SeqCst) - 256
    );
    assert_eq!(boolean_rows(&batches), vec![Some(true)]);
}

#[tokio::test]
async fn differential_decimal128_nullable_predicates() {
    let sm = session();
    let mut predicates = Vec::new();
    for subject in ["a", "b", "c"] {
        for first in ["a", "b", "c", "NULL", "0", "CAST(NULL AS DECIMAL(20,0))"] {
            for second in ["a", "b", "c", "NULL", "0", "CAST(NULL AS DECIMAL(20,0))"] {
                for negation in ["", "NOT "] {
                    predicates.push(format!("{subject} {negation}BETWEEN {first} AND {second}"));
                    predicates.push(format!("{subject} {negation}IN ({first},{second})"));
                }
                predicates.push(format!(
                    "CASE {subject} WHEN {first} THEN true WHEN {second} THEN true ELSE false END"
                ));
            }
        }
    }
    let mut equal = 0usize;
    let mut wrong = Vec::new();
    let mut rejected = Vec::new();
    let mut oracle_rejected = Vec::new();
    for predicate in predicates {
        let expected = query_booleans(&sm, &format!("SELECT {predicate} FROM n")).await;
        let actual = query_booleans(&sm, &format!("SELECT {predicate} FROM t")).await;
        match (expected, actual) {
            (Ok(expected), Ok(actual)) if expected == actual => equal += 1,
            (Ok(expected), Ok(actual)) => wrong.push(
                serde_json::json!({"predicate":predicate,"actual":actual,"expected":expected}),
            ),
            (Ok(_), Err(e)) => rejected.push(
                serde_json::json!({"predicate":predicate,"error":e.lines().next().unwrap_or("")}),
            ),
            (Err(e), _) => oracle_rejected.push(
                serde_json::json!({"predicate":predicate,"error":e.lines().next().unwrap_or("")}),
            ),
        }
    }
    eprintln!(
        "NULL_MATRIX_COUNTS equal={equal} wrong={} rejected={} oracle_rejected={}",
        wrong.len(),
        rejected.len(),
        oracle_rejected.len()
    );
    eprintln!(
        "NULL_MATRIX_REPORT={}",
        serde_json::json!({"equal":equal,"wrong":wrong,"rejected":rejected,"oracle_rejected":oracle_rejected})
    );
    assert!(
        wrong.is_empty(),
        "{} nullable predicates silently disagree with native Decimal128",
        wrong.len()
    );
}

check_bool!(
    between_untyped_null_bound_preserves_unknown,
    "SELECT a BETWEEN NULL AND c FROM t WHERE id=1",
    vec![None]
);
check_bool!(
    between_untyped_null_bound_preserves_false,
    "SELECT a BETWEEN c AND NULL FROM t WHERE id=3",
    vec![Some(false)]
);
check_bool!(
    row_constructor_cross_scale_equality,
    "SELECT (c,a)=(b,a) FROM t WHERE id=1",
    vec![Some(true)]
);
check_bool!(
    named_struct_cross_scale_equality,
    "SELECT named_struct('v',c)=named_struct('v',b) FROM t WHERE id=1",
    vec![Some(true)]
);
check_bool!(
    named_struct_same_scale_ordering,
    "SELECT named_struct('v',a)<named_struct('v',c) FROM t WHERE id=1",
    vec![Some(true)]
);

async fn json_rows(sql: &str) -> Vec<serde_json::Value> {
    use streamling_common::formats::FromArrowConverter;
    let batches = run_once(&session(), sql).await.unwrap();
    let converter = streamling_common::formats::json::FromArrowToJsonConverter::new();
    let mut values = Vec::new();
    for batch in batches {
        eprintln!("JSON_OUTPUT_SCHEMA {:?}", batch.schema());
        for bytes in converter.convert_from_batch(&batch).unwrap() {
            let value = serde_json::from_slice(&bytes).unwrap();
            eprintln!("JSON_OUTPUT {value}");
            values.push(value);
        }
    }
    values
}
#[tokio::test]
async fn array_append_rescales_new_element() {
    assert_eq!(
        json_rows("SELECT array_append(vals,b) AS v FROM t WHERE id=1").await,
        vec![serde_json::json!({"v":["255","256","256"]})]
    );
}
#[tokio::test]
async fn array_prepend_rescales_new_element() {
    assert_eq!(
        json_rows("SELECT array_prepend(b,vals) AS v FROM t WHERE id=1").await,
        vec![serde_json::json!({"v":["256","255","256"]})]
    );
}
#[tokio::test]
async fn array_replace_rescales_replacement() {
    assert_eq!(
        json_rows("SELECT array_replace(vals,a,b) AS v FROM t WHERE id=1").await,
        vec![serde_json::json!({"v":["256","256"]})]
    );
}
#[tokio::test]
async fn array_literal_preserves_numeric_elements() {
    assert_eq!(
        json_rows("SELECT [a,c] AS v FROM t WHERE id=1").await,
        vec![serde_json::json!({"v":["255","256"]})]
    );
}
#[tokio::test]
async fn named_struct_preserves_numeric_element() {
    assert_eq!(
        json_rows("SELECT named_struct('v',a) AS n FROM t WHERE id=1").await,
        vec![serde_json::json!({"n":{"v":"255"}})]
    );
}
#[tokio::test]
async fn array_remove_matches_numeric_value() {
    assert_eq!(
        json_rows("SELECT array_remove(vals,b) AS v FROM t WHERE id=1").await,
        vec![serde_json::json!({"v":["255"]})]
    );
}

check_bool!(
    nvl2_same_scale_parent_comparison,
    "SELECT nvl2(a,a,c)<c FROM t WHERE id=1",
    vec![Some(true)]
);
check_bool!(
    nvl2_mixed_scale_parent_comparison,
    "SELECT nvl2(a,a,b)<c FROM t WHERE id=1",
    vec![Some(true)]
);
check_bool!(
    nullif_reverse_decimal_argument,
    "SELECT nullif(c,b) IS NULL FROM t WHERE id=1",
    vec![Some(true)]
);

#[tokio::test]
async fn decimal_source_projection_control() {
    assert_eq!(
        json_rows("SELECT a AS v FROM t WHERE id=1").await,
        vec![serde_json::json!({"v":"255"})]
    );
    assert_eq!(
        json_rows("SELECT vals AS v FROM t WHERE id=1").await,
        vec![serde_json::json!({"v":["255","256"]})]
    );
    assert_eq!(
        json_rows("SELECT nested AS v FROM t WHERE id=1").await,
        vec![serde_json::json!({"v":{"amount":"255"}})]
    );
}

#[tokio::test]
async fn prior_fixed_width_positive_order_control() {
    use arrow::array::FixedSizeBinaryBuilder;
    let ctx = datafusion::prelude::SessionContext::new();
    let mut a = FixedSizeBinaryBuilder::new(32);
    let mut b = FixedSizeBinaryBuilder::new(32);
    let mut a_bytes = [0u8; 32];
    a_bytes[31] = 255;
    let mut b_bytes = [0u8; 32];
    b_bytes[30] = 1;
    a.append_value(a_bytes).unwrap();
    b.append_value(b_bytes).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::FixedSizeBinary(32), false),
        Field::new("b", DataType::FixedSizeBinary(32), false),
    ]));
    ctx.register_batch(
        "old",
        RecordBatch::try_new(schema, vec![Arc::new(a.finish()), Arc::new(b.finish())]).unwrap(),
    )
    .unwrap();
    for query in [
        "SELECT greatest(a,b,NULL)=b FROM old",
        "SELECT least(a,b,NULL)=a FROM old",
        "SELECT array_min([a,b])=a FROM old",
        "SELECT array_max([a,b])=b FROM old",
        "SELECT (a,a)<(b,a) FROM old",
        "SELECT a BETWEEN NULL AND b IS NULL FROM old",
    ] {
        let batches = ctx.sql(query).await.unwrap().collect().await.unwrap();
        assert_eq!(boolean_rows(&batches), vec![Some(true)], "{query}");
    }
}

#[tokio::test]
async fn derived_projection_branch_metadata() {
    let sm = session();
    let mut wrong = Vec::new();
    let mut rejected = Vec::new();
    for expr in [
        "CASE WHEN id=1 THEN a ELSE c END",
        "coalesce(a,c)",
        "greatest(a,c)",
        "nullif(a,c)",
    ] {
        for shape in [
            format!("SELECT v<c FROM(SELECT id,({expr}) AS v,c FROM t) AS q"),
            format!("WITH q AS(SELECT id,({expr}) AS v,c FROM t) SELECT v<c FROM q"),
            format!(
                "WITH q AS(SELECT id,({expr}) AS v,c FROM t),r AS(SELECT * FROM q) SELECT v<c FROM r"
            ),
        ] {
            let expected = query_booleans(&sm, &shape.replace("FROM t", "FROM n"))
                .await
                .unwrap();
            match query_booleans(&sm, &shape).await {
                Ok(actual) if actual == expected => {}
                Ok(actual) => {
                    wrong.push(serde_json::json!({"sql":shape,"actual":actual,"expected":expected}))
                }
                Err(error) => rejected.push(serde_json::json!({"sql":shape,"error":error})),
            }
        }
    }
    eprintln!(
        "DERIVED_REPORT={}",
        serde_json::json!({"wrong":wrong,"rejected":rejected})
    );
    assert!(
        wrong.is_empty(),
        "{} derived/CTE projections silently differ from native decimals",
        wrong.len()
    );
}

#[tokio::test]
async fn derived_same_scale_case_parent_comparison() {
    let batches = run(
        &session(),
        "SELECT v<c FROM(SELECT id,CASE WHEN id=1 THEN a ELSE c END AS v,c FROM t) AS q",
    )
    .await
    .unwrap();
    assert_eq!(
        boolean_rows(&batches),
        vec![Some(true), Some(false), Some(false), Some(false)]
    );
}

#[tokio::test]
async fn scalar_subquery_decimal_comparison_diagnostic() {
    assert_eq!(
        query_booleans(
            &session(),
            "SELECT a=(SELECT b FROM t WHERE id=2) FROM t WHERE id=1"
        )
        .await
        .unwrap(),
        vec![Some(true)]
    );
}
#[tokio::test]
#[ignore = "Native Decimal128 projection IN subquery is also unsupported by the physical planner; not a decimal regression"]
async fn in_subquery_compares_decimal_values() {
    let sm = session();
    let expected = query_booleans(&sm, "SELECT a IN(SELECT b FROM n) FROM n")
        .await
        .unwrap();
    let actual = query_booleans(&sm, "SELECT a IN(SELECT b FROM t) FROM t")
        .await
        .unwrap();
    assert_eq!(actual, expected);
}
#[tokio::test]
#[ignore = "Native Decimal128 projection NOT IN subquery is also unsupported by the physical planner; not a decimal regression"]
async fn not_in_subquery_compares_decimal_values() {
    let sm = session();
    let expected = query_booleans(&sm, "SELECT a NOT IN(SELECT b FROM n) FROM n")
        .await
        .unwrap();
    let actual = query_booleans(&sm, "SELECT a NOT IN(SELECT b FROM t) FROM t")
        .await
        .unwrap();
    assert_eq!(actual, expected);
}

#[tokio::test]
#[ignore = "Native Decimal128 IN filter hits unsupported HashJoin PartitionMode Auto; not a decimal regression"]
async fn in_filter_subquery_compares_decimal_values() {
    let sm = session();
    let expected = query_booleans(&sm, "SELECT true FROM n WHERE a IN(SELECT b FROM n)")
        .await
        .unwrap();
    let actual = query_booleans(&sm, "SELECT true FROM t WHERE a IN(SELECT b FROM t)")
        .await
        .unwrap();
    assert_eq!(actual, expected);
}
#[tokio::test]
async fn not_in_filter_subquery_compares_decimal_values() {
    let sm = session();
    let expected = query_booleans(&sm, "SELECT true FROM n WHERE a NOT IN(SELECT b FROM n)")
        .await
        .unwrap();
    let actual = query_booleans(&sm, "SELECT true FROM t WHERE a NOT IN(SELECT b FROM t)")
        .await
        .unwrap();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn not_in_filter_subquery_correct_row_ids() {
    let batches = run(
        &session(),
        "SELECT id FROM t WHERE a NOT IN(SELECT b FROM t)",
    )
    .await
    .unwrap();
    let ids = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![Some(3)]);
}

#[tokio::test]
async fn derived_same_scale_case_filter_correct_row_ids() {
    let batches = run(
        &session(),
        "SELECT id FROM(SELECT id,CASE WHEN id=1 THEN a ELSE c END AS v,c FROM t) AS q WHERE v<c",
    )
    .await
    .unwrap();
    let ids = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![Some(1)]);
}

#[tokio::test]
#[serial_test::serial(decimal_sequence_v2)]
async fn volatile_native_decimal_operation_control() {
    let sm = session();
    sm.session_context()
        .register_udf(datafusion::logical_expr::ScalarUDF::from(NextDecimal {
            nullable: true,
            signature: datafusion::logical_expr::Signature::exact(
                vec![],
                datafusion::logical_expr::Volatility::Volatile,
            ),
        }));
    let value = "decimal_arb_to_decimal128(next_decimal_v2(),30,0)";
    for expression in [
        format!("greatest({value},CAST(256 AS DECIMAL(30,0)))=257"),
        format!("nullif({value},CAST(256 AS DECIMAL(30,0)))=257"),
        format!("CASE {value} WHEN 0 THEN 1 WHEN 257 THEN 2 ELSE 3 END=2"),
        format!("{value} IS NOT DISTINCT FROM CAST(257 AS DECIMAL(30,0))"),
    ] {
        NEXT_DECIMAL.store(256, std::sync::atomic::Ordering::SeqCst);
        let batches = run_once(&sm, &format!("SELECT {expression} FROM t WHERE id=1"))
            .await
            .unwrap();
        assert_eq!(boolean_rows(&batches), vec![Some(true)], "{expression}");
        assert_eq!(
            NEXT_DECIMAL.load(std::sync::atomic::Ordering::SeqCst),
            257,
            "{expression} must evaluate its argument once"
        );
    }
}

check_decimal!(
    unquoted_u64_overflow_literal_cast_is_exact,
    "SELECT CAST(18446744073709551617 AS DECIMAL(77,0)) FROM t WHERE id=1",
    0,
    vec![Some("18446744073709551617".into())]
);
check_decimal!(
    quoted_u64_overflow_literal_cast_control,
    "SELECT CAST('18446744073709551617' AS DECIMAL(77,0)) FROM t WHERE id=1",
    0,
    vec![Some("18446744073709551617".into())]
);

#[derive(Debug, PartialEq, Eq, Hash)]
struct LegacyCoercionProbe {
    signature: datafusion::logical_expr::Signature,
}
impl datafusion::logical_expr::ScalarUDFImpl for LegacyCoercionProbe {
    fn name(&self) -> &str {
        "legacy_to_u256_coercion_probe"
    }
    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::error::Result<datafusion::logical_expr::ColumnarValue> {
        eprintln!("LEGACY_COERCED_INPUT {:?}", args.args);
        Ok(datafusion::logical_expr::ColumnarValue::Scalar(
            datafusion::common::ScalarValue::Utf8(Some(format!("{:?}", args.args))),
        ))
    }
}
#[tokio::test]
#[ignore = "Attribution diagnostic only: prints original U256 signature coercion without a correctness assertion; the separate p77 literal regression has a real before/after oracle"]
async fn legacy_big_integer_literal_coercion_diagnostic() {
    use datafusion::logical_expr::{ScalarUDF, Signature, TypeSignature, Volatility};
    let sm = session();
    // Original ToU256Func's exact signature alternatives, kept in their original order.
    let types = vec![
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Int64,
        DataType::UInt64,
        DataType::Int32,
        DataType::UInt32,
        DataType::Int16,
        DataType::UInt16,
        DataType::Int8,
        DataType::UInt8,
        DataType::FixedSizeBinary(32),
    ];
    sm.session_context()
        .register_udf(ScalarUDF::from(LegacyCoercionProbe {
            signature: Signature::one_of(
                types
                    .into_iter()
                    .map(|t| TypeSignature::Exact(vec![t]))
                    .collect(),
                Volatility::Immutable,
            ),
        }));
    for sql in [
        "SELECT legacy_to_u256_coercion_probe(18446744073709551617) FROM t WHERE id=1",
        "SELECT legacy_to_u256_coercion_probe(123456789012345678901234567890) FROM t WHERE id=1",
    ] {
        eprintln!("LEGACY_COERCION_RESULT {:?}", run(&sm, sql).await);
    }
}

check_bool!(
    mixed_scale_case_does_not_equate_255_and_2_55,
    "SELECT (CASE WHEN id=1 THEN a ELSE b END)=to_decimal_arb_from_string('2.55',100,2) FROM t WHERE id=1",
    vec![Some(false)]
);
check_bool!(
    mixed_scale_greatest_does_not_equate_256_and_25600,
    "SELECT greatest(a,b)=to_decimal_arb_from_string('25600',100,0) FROM t WHERE id=1",
    vec![Some(false)]
);
check_bool!(
    array_has_does_not_equate_255_and_2_55,
    "SELECT array_has(vals,to_decimal_arb_from_string('2.55',100,2)) FROM t WHERE id=1",
    vec![Some(false)]
);
