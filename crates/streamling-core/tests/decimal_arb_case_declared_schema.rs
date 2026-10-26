//! A transform's output column that is the result of a `CASE` over `decimal_arb` must keep its
//! decimal type for the transforms (and sinks) planned against it.
//!
//! DataFusion derives the output field of a `CASE` from a bare `LargeBinary`; the analyzer rewrite
//! restores the decimal metadata with `decimal_arb_with_meta`, but a pipeline registers each SQL
//! transform as a view over the *unanalyzed* plan and the extension nodes around it capture that
//! plan's schema. Everything planned afterwards then saw raw bytes: `CAST(v AS VARCHAR)` read the
//! encoding as UTF-8 and failed at runtime (or produced garbage), `v + 1` failed to plan.
//!
//! These tests chain two transforms the way the pipeline builder does.
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    datasource::{MemTable, ViewTable, provider_as_source},
    logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder, dml::InsertOp},
    prelude::SessionContext,
};
use std::sync::Arc;
use streamling_common::types::decimal_arb::{
    DecimalArbArrayBuilder, DecimalArbType, NativeIntKind,
};
use streamling_core::{
    dynamic_table::DynamicTableRegistry,
    operators::{
        checkpointable::CheckpointableNode,
        wrapping::{WrappingNode, WrappingSourceTableProvider},
    },
    session::SessionManager,
};

/// Rows: id 1..=4 with a = 1500000, 2000000, 3, NULL (decimal_arb(78, 0), `u256` hinted).
fn source() -> MemTable {
    let mut builder = DecimalArbArrayBuilder::with_capacity(4, "a", 78, 0).unwrap();
    builder.append_str("1500000").unwrap();
    builder.append_str("2000000").unwrap();
    builder.append_str("3").unwrap();
    builder.append_null();
    let a_field = DecimalArbType::with_native_int_kind(
        DecimalArbType::field("a", 78, 0, true).unwrap(),
        NativeIntKind::U256,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        a_field,
        Field::new("_gs_op", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(builder.finish().into_inner().0),
            Arc::new(StringArray::from(vec!["i", "i", "i", "i"])),
        ],
    )
    .unwrap();
    MemTable::try_new(schema, vec![vec![batch]]).unwrap()
}

struct Pipeline {
    sm: SessionManager,
    ctx: SessionContext,
}

impl Pipeline {
    fn new() -> Self {
        let sm = SessionManager::new(100, 10, DynamicTableRegistry::new(), 1).unwrap();
        let ctx = sm.session_context();
        let provider = WrappingSourceTableProvider::new(
            Arc::new(source()),
            format!("case_schema_source_{}", uuid::Uuid::new_v4()),
            None,
            None,
        );
        ctx.register_table("t", Arc::new(provider)).unwrap();
        Pipeline { sm, ctx }
    }

    /// Plan `sql` and register it as the transform `name`, as the pipeline builder does: the
    /// declared plan goes into the extension nodes and a view over them is what the next
    /// transform is planned against. Returns the declared output schema.
    async fn transform(&self, name: &str, sql: &str) -> Result<Arc<Schema>, String> {
        let (sql_plan, _source) = self
            .sm
            .create_supported_logical_plan(sql.into())
            .await
            .map_err(|e| e.to_string())?;
        let declared = sql_plan.schema().inner().clone();
        let checkpoint = LogicalPlan::Extension(Extension {
            node: Arc::new(CheckpointableNode::new(sql_plan, 10, name.into())),
        });
        let wrapped = LogicalPlan::Extension(Extension {
            node: Arc::new(WrappingNode::new_with_non_null_cols(
                checkpoint,
                format!("case_schema_{name}_{}", uuid::Uuid::new_v4()),
                false,
                vec!["id".into()],
                None,
            )),
        });
        self.ctx
            .register_table(name, Arc::new(ViewTable::new(wrapped, None)))
            .map_err(|e| e.to_string())?;
        Ok(declared)
    }

    /// Drain the transform `name` into a sink table and return the rows.
    async fn drain(&self, name: &str) -> Result<Vec<RecordBatch>, String> {
        let view = self
            .ctx
            .table(name)
            .await
            .map_err(|e| e.to_string())?
            .into_unoptimized_plan();
        let target =
            Arc::new(MemTable::try_new(view.schema().inner().clone(), vec![vec![]]).unwrap());
        let sink = format!("sink_{name}");
        self.ctx.register_table(&sink, target.clone()).unwrap();
        let insert = LogicalPlanBuilder::insert_into(
            view,
            &sink,
            provider_as_source(target),
            InsertOp::Append,
        )
        .unwrap()
        .build()
        .unwrap();
        let df = self.sm.new_df(insert);
        let physical = df.create_physical_plan().await.map_err(|e| e.to_string())?;
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            datafusion::physical_plan::collect(physical, Arc::new(df.task_ctx())),
        )
        .await
        .expect("bounded plan must drain")
        .map_err(|e| e.to_string())?;
        self.ctx
            .table(&sink)
            .await
            .map_err(|e| e.to_string())?
            .collect()
            .await
            .map_err(|e| e.to_string())
    }
}

fn texts(batches: &[RecordBatch], column: &str) -> Vec<Option<String>> {
    let mut out = vec![];
    for batch in batches {
        let array = batch.column_by_name(column).unwrap();
        let array = arrow::compute::cast(array, &DataType::Utf8).unwrap();
        let array = array.as_any().downcast_ref::<StringArray>().unwrap();
        out.extend(array.iter().map(|v| v.map(str::to_string)));
    }
    out
}

const CASE_SAME_SCALE: &str = "SELECT id, CASE WHEN id > 1 THEN a ELSE a END AS v FROM t";
/// Branches at different scales: the division result is `decimal_arb(_, 18)`, the other branch
/// scale 0. The analyzer brings both to the common scale; the declared schema must say so too.
const CASE_MIXED_SCALE: &str = "SELECT id, CASE WHEN id <= 2 THEN a / 1000 ELSE a END AS v FROM t";

#[tokio::test]
async fn case_output_is_declared_as_decimal_arb() {
    for sql in [CASE_SAME_SCALE, CASE_MIXED_SCALE] {
        let p = Pipeline::new();
        let declared = p.transform("up", sql).await.unwrap();
        let v = declared.field_with_name("v").unwrap();
        assert!(
            DecimalArbType::is_decimal_arb_field(v),
            "declared schema of a CASE over decimal_arb lost its decimal type: {v:?}\nSQL: {sql}"
        );
    }
}

#[tokio::test]
async fn case_output_declares_the_scale_the_analyzer_settles_on() {
    let p = Pipeline::new();
    let declared = p.transform("up", CASE_MIXED_SCALE).await.unwrap();
    let v = declared.field_with_name("v").unwrap();
    let (_, scale) = DecimalArbType::precision_scale_from_field(v).expect("decimal_arb field");
    assert_eq!(
        scale, 18,
        "division yields scale 18 and the CASE merges to it"
    );
}

#[tokio::test]
async fn text_cast_downstream_of_a_case_prints_the_number() {
    let p = Pipeline::new();
    p.transform("up", CASE_SAME_SCALE).await.unwrap();
    p.transform("down", "SELECT id, CAST(v AS VARCHAR) AS txt FROM up")
        .await
        .unwrap();
    let rows = p.drain("down").await.unwrap();
    assert_eq!(
        texts(&rows, "txt"),
        vec![
            Some("1500000".into()),
            Some("2000000".into()),
            Some("3".into()),
            None
        ]
    );
}

#[tokio::test]
async fn text_cast_downstream_of_a_mixed_scale_case_prints_the_number() {
    let p = Pipeline::new();
    p.transform("up", CASE_MIXED_SCALE).await.unwrap();
    p.transform("down", "SELECT id, CAST(v AS TEXT) AS txt FROM up")
        .await
        .unwrap();
    let rows = p.drain("down").await.unwrap();
    assert_eq!(
        texts(&rows, "txt"),
        vec![
            Some("1500.000000000000000000".into()),
            Some("2000.000000000000000000".into()),
            Some("3.000000000000000000".into()),
            None
        ]
    );
}

#[tokio::test]
async fn arithmetic_downstream_of_a_case_plans_and_computes() {
    let p = Pipeline::new();
    p.transform("up", CASE_SAME_SCALE).await.unwrap();
    p.transform("down", "SELECT id, CAST(v + 1 AS TEXT) AS txt FROM up")
        .await
        .expect("v + 1 over a CASE column must plan");
    let rows = p.drain("down").await.unwrap();
    assert_eq!(
        texts(&rows, "txt"),
        vec![
            Some("1500001".into()),
            Some("2000001".into()),
            Some("4".into()),
            None
        ]
    );
}

#[tokio::test]
async fn columns_that_were_already_declared_correctly_are_untouched() {
    // COALESCE and a plain column carry their metadata at plan time; the fix must not add a
    // projection (or change the schema) for them.
    let p = Pipeline::new();
    let declared = p
        .transform("up", "SELECT id, a, COALESCE(a, 0) AS c FROM t")
        .await
        .unwrap();
    for name in ["a", "c"] {
        assert!(DecimalArbType::is_decimal_arb_field(
            declared.field_with_name(name).unwrap()
        ));
    }
    let (plan, _) =
        p.sm.create_supported_logical_plan("SELECT id, a, COALESCE(a, 0) AS c FROM t".into())
            .await
            .unwrap();
    // Projection(id, a, c, _gs_op) directly over the scan: no extra relabelling layer.
    match &plan {
        LogicalPlan::Projection(projection) => assert!(
            matches!(projection.input.as_ref(), LogicalPlan::TableScan(_)),
            "unexpected extra layer:\n{}",
            plan.display_indent()
        ),
        other => panic!("unexpected plan shape: {}", other.display_indent()),
    }
}
