//! How reads find their mark: the side output that applies a table stamps
//! the [`Mark`] of each table it applied into the batch's schema metadata,
//! which Streamling's operators carry along, and physical planning appends a
//! [`MarkExpr`] reading that stamp to every call of a [`TableFunction`].

use super::InMemoryTable;
use super::versions::{Mark, Marks, VersionRetention};
use crate::error::Result;
use crate::operators::rebatch::RebatchExec;
use crate::operators::scan_sharing::BroadcastingExec;
use crate::operators::wrapping::{SideOutputExec, WrappingExec};
use crate::{streamling_err, streamling_user_err};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Prefix of the batch metadata key holding a table's mark; the table's
/// name follows.
const MARK_METADATA_PREFIX: &str = "streamling.table_mark.";
/// The name of the argument [`MarkExpr`] appends.
const MARK_ARGUMENT: &str = "__table_mark";

fn mark_key(table: &str) -> String {
    format!("{MARK_METADATA_PREFIX}{table}")
}

/// The schema metadata entries that stamp `marks` on a batch.
pub fn mark_stamps(marks: Marks) -> HashMap<String, String> {
    marks
        .into_iter()
        .map(|(table, Mark(mark))| (mark_key(&table), mark.to_string()))
        .collect()
}

/// Evaluates to the mark of `table` stamped into the batch it evaluates, or
/// null when there is none.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MarkExpr {
    table: String,
    key: String,
}

impl MarkExpr {
    fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            key: mark_key(table),
        }
    }
}

impl fmt::Display for MarkExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "table_mark({})", self.table)
    }
}

impl PhysicalExpr for MarkExpr {
    fn return_field(&self, _input_schema: &Schema) -> DataFusionResult<FieldRef> {
        Ok(Arc::new(Field::new(MARK_ARGUMENT, DataType::UInt64, true)))
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let mark = batch
            .schema_ref()
            .metadata()
            .get(&self.key)
            .map(|mark| {
                mark.parse::<u64>().map_err(|_| {
                    streamling_err!("invalid mark '{}' of table '{}'", mark, self.table)
                })
            })
            .transpose()?;
        Ok(ColumnarValue::Scalar(ScalarValue::UInt64(mark)))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }

    /// Its value changes from batch to batch.
    fn is_volatile_node(&self) -> bool {
        true
    }
}

/// A scalar function that reads a table. Registered as a [`TableUdf`], each
/// call reads the table as of the mark of the batch it evaluates.
pub trait TableFunction: Debug + Send + Sync + 'static {
    fn name(&self) -> &str;

    fn signature(&self) -> &Signature;

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DataFusionResult<FieldRef>;

    /// The table a call reads, from its literal arguments.
    fn table(&self, literals: &[Option<&ScalarValue>]) -> Result<Arc<InMemoryTable>>;

    /// `mark` is the table's mark on the batch `args` come from, if any.
    fn invoke(
        &self,
        args: ScalarFunctionArgs,
        mark: Option<Mark>,
    ) -> DataFusionResult<ColumnarValue>;
}

/// The scalar UDF of a [`TableFunction`].
#[derive(Debug)]
pub struct TableUdf(Box<dyn TableFunction>);

impl TableUdf {
    pub fn new(function: impl TableFunction) -> Self {
        Self(Box::new(function))
    }
}

// The function holds runtime state, so it is identified by its name and
// signature.
impl PartialEq for TableUdf {
    fn eq(&self, other: &Self) -> bool {
        self.0.name() == other.0.name() && self.0.signature() == other.0.signature()
    }
}
impl Eq for TableUdf {}
impl Hash for TableUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.name().hash(state);
        self.0.signature().hash(state);
    }
}

fn is_mark_argument(field: &FieldRef) -> bool {
    field.name() == MARK_ARGUMENT
}

impl ScalarUDFImpl for TableUdf {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn signature(&self) -> &Signature {
        self.0.signature()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Err(streamling_err!(
            "{}: the return type depends on the table; return_field_from_args resolves it",
            self.0.name()
        )
        .into())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DataFusionResult<FieldRef> {
        let own_arguments = match args.arg_fields.last() {
            Some(field) if is_mark_argument(field) => args.arg_fields.len() - 1,
            _ => args.arg_fields.len(),
        };
        self.0.return_field_from_args(ReturnFieldArgs {
            arg_fields: &args.arg_fields[..own_arguments],
            scalar_arguments: &args.scalar_arguments[..own_arguments],
        })
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let mut mark = None;
        if args.arg_fields.last().is_some_and(is_mark_argument) {
            args.arg_fields.pop();
            let Some(ColumnarValue::Scalar(ScalarValue::UInt64(stamp))) = args.args.pop() else {
                return Err(streamling_err!(
                    "{}: the table mark must be a UInt64 scalar",
                    self.0.name()
                )
                .into());
            };
            mark = stamp.map(Mark);
        }
        self.0.invoke(args, mark)
    }
}

/// Appends the table's mark on the batch to each call in `expr` of a
/// [`TableFunction`] whose table keeps versions per batch, so it reads the
/// table as of that batch. `input` is the plan whose batches `expr`
/// evaluates. Calls that already have it are left alone.
pub fn bind_table_marks(
    expr: Arc<dyn PhysicalExpr>,
    input: &Arc<dyn ExecutionPlan>,
) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
    expr.transform_up(|expr| {
        let Some(args) = bound_arguments(&expr, input)? else {
            return Ok(Transformed::no(expr));
        };
        Ok(Transformed::yes(expr.with_new_children(args)?))
    })
    .data()
}

/// `expr`'s arguments plus its mark, if it's an unbound call of a table
/// function whose table keeps versions per batch.
fn bound_arguments(
    expr: &Arc<dyn PhysicalExpr>,
    input: &Arc<dyn ExecutionPlan>,
) -> Result<Option<Vec<Arc<dyn PhysicalExpr>>>> {
    let Some(call) = expr.downcast_ref::<ScalarFunctionExpr>() else {
        return Ok(None);
    };
    let Some(TableUdf(function)) = call.fun().inner().downcast_ref::<TableUdf>() else {
        return Ok(None);
    };
    if call
        .args()
        .last()
        .is_some_and(|arg| arg.downcast_ref::<MarkExpr>().is_some())
    {
        return Ok(None);
    }
    let literals: Vec<Option<&ScalarValue>> = call
        .args()
        .iter()
        .map(|arg| arg.downcast_ref::<Literal>().map(Literal::value))
        .collect();
    let table = function.table(&literals)?;
    let VersionRetention::PerBatch { stream } = table.retention() else {
        return Ok(None);
    };
    if let PathToStream::Merged(merger) = path_to_stream(input, stream) {
        return Err(streamling_user_err!(
            "{}('{}') reads batches that '{}' merges after the table's input applied them, so it can't read the table as of one batch; read the table before '{}'",
            function.name(),
            table.name,
            merger,
            merger
        ));
    }
    let mut args = call.args().to_vec();
    args.push(Arc::new(MarkExpr::new(&table.name)));
    Ok(Some(args))
}

/// How a plan's batches come from the side output of a stream.
enum PathToStream {
    Unreached,
    Direct,
    /// Through the node named, which merges batches.
    Merged(String),
}

fn path_to_stream(plan: &Arc<dyn ExecutionPlan>, stream: &str) -> PathToStream {
    if plan
        .downcast_ref::<SideOutputExec>()
        .is_some_and(|side_output| side_output.reference_name() == stream)
    {
        return PathToStream::Direct;
    }
    let mut path = PathToStream::Unreached;
    for input in inputs(plan) {
        match path_to_stream(&input, stream) {
            PathToStream::Merged(merger) => return PathToStream::Merged(merger),
            PathToStream::Direct => path = PathToStream::Direct,
            PathToStream::Unreached => {}
        }
    }
    match (path, plan.downcast_ref::<RebatchExec>()) {
        (PathToStream::Direct, Some(rebatch)) => {
            PathToStream::Merged(rebatch.reference_name().to_string())
        }
        (path, _) => path,
    }
}

/// The plans whose batches `plan` reads, including those its children
/// don't show.
fn inputs(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
    if let Some(broadcast) = plan.downcast_ref::<BroadcastingExec>() {
        return vec![broadcast.base()];
    }
    // Its children are its inner plan's.
    if let Some(wrapping) = plan.downcast_ref::<WrappingExec>() {
        return vec![Arc::clone(wrapping.inner())];
    }
    plan.children().into_iter().cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::COLUMN_NAME_OP;
    use crate::dynamic_table::DynamicTableRegistry;
    use crate::operators::filter::StreamingFilterExec;
    use crate::operators::projection::StreamingProjectionExec;
    use crate::session::SessionManager;
    use crate::side_output::with_metadata;
    use crate::topology::DEFAULT_MAX_DEAD_RATIO;
    use arrow::array::{AsArray, Int64Array, StringArray};
    use datafusion::common::DFSchema;
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::{col, lit};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::projection::ProjectionExec;

    const STREAM: &str = "app::src";
    const LOOKUP: &str = "lookup_eq('names', 'id', id, 'name')";

    fn ids_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    /// A batch of `src`, stamped with `marks` when given.
    fn ids(marks: Option<&Marks>) -> RecordBatch {
        let batch =
            RecordBatch::try_new(ids_schema(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        match marks {
            Some(marks) => with_metadata(batch, mark_stamps(marks.clone())).unwrap(),
            None => batch,
        }
    }

    /// A session with source `src` (`id`) and table `names` (key `id`), whose
    /// stream is `STREAM`, applied with `1 → a` and then `1 → b`. Returns the
    /// session and the two applies' marks.
    fn session() -> (SessionManager, Vec<Marks>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, false),
        ]));
        let names = InMemoryTable::try_new(
            "names",
            &schema,
            &["id".to_string()],
            "app::names".to_string(),
            DEFAULT_MAX_DEAD_RATIO,
            None,
            VersionRetention::PerBatch {
                stream: STREAM.to_string(),
            },
        )
        .unwrap();
        let marks = ["a", "b"].map(|name| {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(StringArray::from(vec![name])),
                    Arc::new(StringArray::from(vec!["u"])),
                ],
            )
            .unwrap();
            names.apply(&batch, &[]).unwrap()
        });
        let session_manager = SessionManager::new(100, 10, DynamicTableRegistry::new(), 1).unwrap();
        session_manager.tables().register(Arc::new(names));
        session_manager
            .session_context()
            .register_table(
                "src",
                Arc::new(MemTable::try_new(ids_schema(), vec![vec![ids(None)]]).unwrap()),
            )
            .unwrap();
        (session_manager, marks.to_vec())
    }

    async fn plan(session_manager: &SessionManager, sql: &str) -> Arc<dyn ExecutionPlan> {
        session_manager
            .session_context()
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }

    fn evaluated_names(expr: &Arc<dyn PhysicalExpr>, batch: &RecordBatch) -> Option<String> {
        let names = expr.evaluate(batch).unwrap().into_array(1).unwrap();
        names
            .as_string::<i32>()
            .iter()
            .next()
            .flatten()
            .map(str::to_string)
    }

    #[tokio::test]
    async fn projected_lookups_read_the_table_as_of_their_batch() {
        let (session_manager, marks) = session();
        let plan = plan(
            &session_manager,
            &format!("SELECT id, {LOOKUP} AS name FROM src"),
        )
        .await;
        let projection = plan
            .downcast_ref::<StreamingProjectionExec>()
            .expect("a streaming projection");
        let (lookup, alias) = &projection.expr()[1];
        assert_eq!(alias, "name");
        assert!(lookup.to_string().contains("table_mark(names)"), "{lookup}");
        assert_eq!(
            plan.schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["id", "name"],
            "the mark is an argument, not a column"
        );

        assert_eq!(
            evaluated_names(lookup, &ids(Some(&marks[0]))),
            Some("a".to_string())
        );
        assert_eq!(
            evaluated_names(lookup, &ids(Some(&marks[1]))),
            Some("b".to_string())
        );
        assert_eq!(
            evaluated_names(lookup, &ids(None)),
            Some("b".to_string()),
            "a batch without a mark reads the latest rows"
        );
    }

    #[tokio::test]
    async fn filtered_lookups_read_the_table_as_of_their_batch() {
        let (session_manager, marks) = session();
        let plan = plan(
            &session_manager,
            &format!("SELECT id FROM src WHERE {LOOKUP} = 'a'"),
        )
        .await;
        let filter = plan
            .downcast_ref::<StreamingFilterExec>()
            .expect("a streaming filter");
        let matches = |batch: &RecordBatch| {
            let matched = filter
                .predicate()
                .evaluate(batch)
                .unwrap()
                .into_array(1)
                .unwrap();
            matched.as_boolean().value(0)
        };
        assert!(matches(&ids(Some(&marks[0]))));
        assert!(!matches(&ids(Some(&marks[1]))));
    }

    /// The unbound `lookup_eq` call over `src`'s schema.
    fn lookup_call(session_manager: &SessionManager) -> Arc<dyn PhysicalExpr> {
        let session_state = session_manager.session_state();
        let lookup_eq = session_state
            .scalar_functions()
            .get("lookup_eq")
            .expect("lookup_eq is registered");
        let call = lookup_eq.call(vec![lit("names"), lit("id"), col("id"), lit("name")]);
        let schema = DFSchema::try_from(ids_schema().as_ref().clone()).unwrap();
        session_state.create_physical_expr(call, &schema).unwrap()
    }

    fn project(
        lookup: Arc<dyn PhysicalExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> DataFusionResult<StreamingProjectionExec> {
        StreamingProjectionExec::from_original(ProjectionExec::try_new(
            vec![(lookup, "name".to_string())],
            input,
        )?)
    }

    fn side_output(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(SideOutputExec::new(input, STREAM.to_string(), Vec::new()))
    }

    #[test]
    fn rejects_a_lookup_after_its_stream_is_rebatched() {
        let (session_manager, _) = session();
        let source: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(ids_schema()));
        let rebatched: Arc<dyn ExecutionPlan> = Arc::new(RebatchExec::new(
            side_output(Arc::clone(&source)),
            10,
            None,
            "enrich".to_string(),
        ));

        let err = project(lookup_call(&session_manager), rebatched)
            .expect_err("the lookup must be rejected")
            .to_string();
        assert!(err.contains("'enrich' merges"), "{err}");

        let rebatched_before_the_stream = side_output(Arc::new(RebatchExec::new(
            source,
            10,
            None,
            "upstream".to_string(),
        )));
        project(lookup_call(&session_manager), rebatched_before_the_stream)
            .expect("rebatching before the table's input is fine");
    }

    #[test]
    fn binding_is_idempotent() {
        let (session_manager, _) = session();
        let projection = Arc::new(
            project(
                lookup_call(&session_manager),
                side_output(Arc::new(EmptyExec::new(ids_schema()))),
            )
            .unwrap(),
        );
        let rebuilt = Arc::clone(&projection)
            .with_new_children(vec![Arc::clone(projection.input())])
            .unwrap();
        let rebuilt = rebuilt
            .downcast_ref::<StreamingProjectionExec>()
            .expect("a streaming projection");
        assert_eq!(
            rebuilt.expr()[0]
                .0
                .to_string()
                .matches("table_mark")
                .count(),
            1
        );
    }
}
