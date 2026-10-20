//! How reads find their position: the side output that applies a table
//! stamps each batch's [`Position`] into the batch's schema metadata, which
//! Streamling's operators carry along, and physical planning appends a
//! [`PositionExpr`] reading that stamp to every call of a [`TableFunction`].

use super::InMemoryTable;
use super::snapshot::{Position, SnapshotRetention};
use crate::error::{Result, ResultExt};
use crate::operators::rebatch::RebatchExec;
use crate::operators::scan_sharing::BroadcastingExec;
use crate::operators::wrapping::{SideOutputExec, WrappingExec};
use crate::{streamling_err, streamling_user_err};
use arrow::array::{RecordBatch, RecordBatchOptions};
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

/// Prefix of the batch metadata key holding a node's position; the node's
/// name follows.
const POSITION_METADATA_PREFIX: &str = "streamling.table_position.";
/// The name of the argument [`PositionExpr`] appends.
const POSITION_ARGUMENT: &str = "__table_position";

fn position_key(stream: &str) -> String {
    format!("{POSITION_METADATA_PREFIX}{stream}")
}

/// `batch` with its position on `stream` in its schema metadata.
pub fn stamp_position(batch: RecordBatch, stream: &str, position: Position) -> Result<RecordBatch> {
    let mut metadata = batch.schema().metadata().clone();
    metadata.insert(position_key(stream), position.to_string());
    let schema = Arc::new(Schema::new_with_metadata(
        batch.schema().fields().clone(),
        metadata,
    ));
    RecordBatch::try_new_with_options(
        schema,
        batch.columns().to_vec(),
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .streamling_context("failed to stamp the batch's table position")
}

/// The position on `stream` stamped into a batch's schema `metadata`.
pub fn stamped_position(
    metadata: &HashMap<String, String>,
    stream: &str,
) -> Result<Option<Position>> {
    metadata
        .get(&position_key(stream))
        .map(|position| position.parse())
        .transpose()
}

/// Evaluates to the position on `stream` stamped into the batch it
/// evaluates, or null when there is none.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PositionExpr {
    stream: String,
    key: String,
}

impl PositionExpr {
    fn new(stream: &str) -> Self {
        Self {
            stream: stream.to_string(),
            key: position_key(stream),
        }
    }
}

impl fmt::Display for PositionExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "table_position({})", self.stream)
    }
}

impl PhysicalExpr for PositionExpr {
    fn return_field(&self, _input_schema: &Schema) -> DataFusionResult<FieldRef> {
        Ok(Arc::new(Field::new(
            POSITION_ARGUMENT,
            DataType::Utf8,
            true,
        )))
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
            batch.schema_ref().metadata().get(&self.key).cloned(),
        )))
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
/// call reads the table as of the position of the batch it evaluates.
pub trait TableFunction: Debug + Send + Sync + 'static {
    fn name(&self) -> &str;

    fn signature(&self) -> &Signature;

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DataFusionResult<FieldRef>;

    /// The table a call reads, from its literal arguments.
    fn table(&self, literals: &[Option<&ScalarValue>]) -> Result<Arc<InMemoryTable>>;

    /// `position` is the position of the batch `args` come from, if known.
    fn invoke(
        &self,
        args: ScalarFunctionArgs,
        position: Option<Position>,
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

fn is_position_argument(field: &FieldRef) -> bool {
    field.name() == POSITION_ARGUMENT
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
            Some(field) if is_position_argument(field) => args.arg_fields.len() - 1,
            _ => args.arg_fields.len(),
        };
        self.0.return_field_from_args(ReturnFieldArgs {
            arg_fields: &args.arg_fields[..own_arguments],
            scalar_arguments: &args.scalar_arguments[..own_arguments],
        })
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let mut position = None;
        if args.arg_fields.last().is_some_and(is_position_argument) {
            args.arg_fields.pop();
            let Some(ColumnarValue::Scalar(ScalarValue::Utf8(stamp))) = args.args.pop() else {
                return Err(streamling_err!(
                    "{}: the table position must be a string",
                    self.0.name()
                )
                .into());
            };
            position = stamp.map(|stamp| stamp.parse()).transpose()?;
        }
        self.0.invoke(args, position)
    }
}

/// Appends the position of the batch to each call in `expr` of a
/// [`TableFunction`] whose table keeps snapshots per position, so it reads
/// the table as of that batch. `input` is the plan whose batches `expr`
/// evaluates. Calls that already have it are left alone.
pub fn bind_table_positions(
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

/// `expr`'s arguments plus its position, if it's an unbound call of a table
/// function whose table keeps snapshots per position.
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
        .is_some_and(|arg| arg.downcast_ref::<PositionExpr>().is_some())
    {
        return Ok(None);
    }
    let literals: Vec<Option<&ScalarValue>> = call
        .args()
        .iter()
        .map(|arg| arg.downcast_ref::<Literal>().map(Literal::value))
        .collect();
    let table = function.table(&literals)?;
    let SnapshotRetention::PerPosition { stream } = table.retention() else {
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
    args.push(Arc::new(PositionExpr::new(stream)));
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

    fn ids(position: Option<Position>) -> RecordBatch {
        let batch =
            RecordBatch::try_new(ids_schema(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        match position {
            Some(position) => stamp_position(batch, STREAM, position).unwrap(),
            None => batch,
        }
    }

    fn at(seq: u64) -> Option<Position> {
        Some(Position { partition: 0, seq })
    }

    /// A session with source `src` (`id`) and table `names` (key `id`), whose
    /// stream is `STREAM`, applied at seq 0 with `1 → a` and seq 1 with `1 → b`.
    fn session() -> SessionManager {
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
            SnapshotRetention::PerPosition {
                stream: STREAM.to_string(),
            },
        )
        .unwrap();
        for (seq, name) in [(0, "a"), (1, "b")] {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(StringArray::from(vec![name])),
                    Arc::new(StringArray::from(vec!["u"])),
                ],
            )
            .unwrap();
            names.apply(&batch, at(seq)).unwrap();
        }
        let session_manager = SessionManager::new(100, 10, DynamicTableRegistry::new(), 1).unwrap();
        session_manager.tables().register(Arc::new(names));
        session_manager
            .session_context()
            .register_table(
                "src",
                Arc::new(MemTable::try_new(ids_schema(), vec![vec![ids(None)]]).unwrap()),
            )
            .unwrap();
        session_manager
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
        let session_manager = session();
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
        assert!(
            lookup.to_string().contains("table_position(app::src)"),
            "{lookup}"
        );
        assert_eq!(
            plan.schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["id", "name"],
            "the position is an argument, not a column"
        );

        assert_eq!(evaluated_names(lookup, &ids(at(0))), Some("a".to_string()));
        assert_eq!(evaluated_names(lookup, &ids(at(1))), Some("b".to_string()));
        assert_eq!(
            evaluated_names(lookup, &ids(None)),
            Some("b".to_string()),
            "a batch without a position reads the latest snapshot"
        );
    }

    #[tokio::test]
    async fn filtered_lookups_read_the_table_as_of_their_batch() {
        let session_manager = session();
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
        assert!(matches(&ids(at(0))));
        assert!(!matches(&ids(at(1))));
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
        let session_manager = session();
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
        let session_manager = session();
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
                .matches("table_position")
                .count(),
            1
        );
    }
}
