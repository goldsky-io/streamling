//! Applies a table's SQL output to the table from inside its upstream's
//! `SideOutputExec`, before the upstream yields the batch to any consumer, so
//! every node downstream of the upstream reads the table with the batch
//! already applied. A node on a sibling branch, even one reading the same
//! source, isn't synchronized with the table.

use crate::session::SessionManager;
use crate::side_output::{SideOutputPlacement, SourceSideOutput};
use crate::streamling_err;
use crate::table::InMemoryTable;
use arrow::array::{AsArray, RecordBatch, RecordBatchOptions};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{Fields, SchemaRef};
use async_trait::async_trait;
use datafusion::common::{DFSchema, Result};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::physical_expr::PhysicalExpr;
use parking_lot::Mutex;
use std::fmt;
use std::sync::Arc;

/// Applies each batch of the table's upstream to the table.
pub struct TableSideOutput {
    table: Arc<InMemoryTable>,
    projection: Vec<Expr>,
    filter: Option<Expr>,
    /// The table SQL's output schema, which the table was built from.
    output_schema: SchemaRef,
    /// Qualifies the columns in `projection` and `filter`.
    upstream: String,
    session_state: SessionState,
    compiled_query: Mutex<Option<Arc<CompiledQuery>>>,
}

/// `projection` and `filter` compiled against one set of upstream fields.
struct CompiledQuery {
    fields: Fields,
    projection: Vec<Arc<dyn PhysicalExpr>>,
    filter: Option<Arc<dyn PhysicalExpr>>,
}

impl TableSideOutput {
    /// `sql_plan` is the table's SQL over `upstream`: a projection with an
    /// optional filter.
    pub fn try_new(
        table: Arc<InMemoryTable>,
        sql_plan: &LogicalPlan,
        upstream: String,
        session_state: SessionState,
    ) -> Result<Self> {
        let (projection, filter) = SessionManager::extract_expressions_from_sql_query(sql_plan)?;
        Ok(Self {
            table,
            projection,
            filter,
            output_schema: Arc::clone(sql_plan.schema().inner()),
            upstream,
            session_state,
            compiled_query: Mutex::new(None),
        })
    }

    /// The table SQL's output for `batch`.
    fn evaluate(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let compiled_query = self.compiled_query(&batch.schema())?;
        let input = match &compiled_query.filter {
            Some(filter) => {
                let mask = filter.evaluate(batch)?.into_array(batch.num_rows())?;
                let mask = mask.as_boolean_opt().ok_or_else(|| {
                    DataFusionError::from(streamling_err!(
                        "table '{}': the filter must return booleans, got {}",
                        self.table.name,
                        mask.data_type()
                    ))
                })?;
                filter_record_batch(batch, mask)?
            }
            None => batch.clone(),
        };
        let columns = compiled_query
            .projection
            .iter()
            .map(|expr| expr.evaluate(&input)?.into_array(input.num_rows()))
            .collect::<Result<Vec<_>>>()?;
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(&self.output_schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(input.num_rows())),
        )?)
    }

    /// Compiled per set of fields because physical columns are positional,
    /// and an upstream may hand over its consumer's projection: a Kafka
    /// source without a filter does when the table is its only consumer.
    fn compiled_query(&self, schema: &SchemaRef) -> Result<Arc<CompiledQuery>> {
        let mut cache = self.compiled_query.lock();
        if let Some(compiled_query) = cache
            .as_ref()
            .filter(|compiled_query| compiled_query.fields == *schema.fields())
        {
            return Ok(Arc::clone(compiled_query));
        }
        let compiled_query = Arc::new(self.compile(schema)?);
        *cache = Some(Arc::clone(&compiled_query));
        Ok(compiled_query)
    }

    fn compile(&self, schema: &SchemaRef) -> Result<CompiledQuery> {
        let df_schema =
            DFSchema::try_from_qualified_schema(self.upstream.as_str(), schema.as_ref())?;
        let compile = |expr: &Expr| {
            self.session_state
                .create_physical_expr(expr.clone(), &df_schema)
        };
        Ok(CompiledQuery {
            fields: schema.fields().clone(),
            projection: self.projection.iter().map(compile).collect::<Result<_>>()?,
            filter: self.filter.as_ref().map(compile).transpose()?,
        })
    }
}

#[async_trait]
impl SourceSideOutput for TableSideOutput {
    async fn process(&self, batch: &RecordBatch) -> Result<()> {
        Ok(self.table.apply(&self.evaluate(batch)?)?)
    }

    /// Readers of the source only see the rows its filter keeps.
    fn placement(&self) -> SideOutputPlacement {
        SideOutputPlacement::AfterSourceFilter
    }
}

impl fmt::Debug for TableSideOutput {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TableSideOutput")
            .field("table", &self.table.name)
            .field("upstream", &self.upstream)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::COLUMN_NAME_OP;
    use crate::dynamic_table::DynamicTableRegistry;
    use crate::topology::DEFAULT_MAX_DEAD_RATIO;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;

    /// `extra` sits between `id` and `name`, so a projected batch moves `name`.
    fn src_fields() -> Vec<Field> {
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("extra", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, false),
        ]
    }

    /// The side output of table `t` (key `id`) defined by `sql` over `src`.
    async fn side_output(sql: &str) -> TableSideOutput {
        let session_manager = SessionManager::new(100, 10, DynamicTableRegistry::new(), 1).unwrap();
        session_manager
            .register_table(
                "src",
                Arc::new(EmptyTable::new(Arc::new(Schema::new(src_fields())))),
            )
            .unwrap();
        let (plan, upstream) = session_manager
            .create_supported_logical_plan(sql.to_string())
            .await
            .unwrap();
        let table = InMemoryTable::try_new(
            "t",
            plan.schema().inner(),
            &["id".to_string()],
            "app::t".to_string(),
            DEFAULT_MAX_DEAD_RATIO,
        )
        .unwrap();
        TableSideOutput::try_new(
            Arc::new(table),
            &plan,
            upstream,
            session_manager.session_state(),
        )
        .unwrap()
    }

    fn src_batch(rows: &[(i64, &str)]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(src_fields())),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
                Arc::new(StringArray::from(vec![None::<&str>; rows.len()])),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
                Arc::new(StringArray::from(vec!["i"; rows.len()])),
            ],
        )
        .unwrap()
    }

    fn names(table: &InMemoryTable, ids: &[i64]) -> Vec<Option<String>> {
        let keys: ArrayRef = Arc::new(Int64Array::from(ids.to_vec()));
        table
            .lookup(&keys, "name")
            .unwrap()
            .as_string::<i32>()
            .iter()
            .map(|name| name.map(str::to_string))
            .collect()
    }

    #[tokio::test]
    async fn applies_the_whole_batch_before_returning() {
        let output = side_output("SELECT id, name FROM src").await;
        output
            .process(&src_batch(&[(1, "a"), (2, "b"), (1, "a2")]))
            .await
            .unwrap();
        assert_eq!(
            names(&output.table, &[1, 2]),
            [Some("a2".to_string()), Some("b".to_string())]
        );
    }

    #[tokio::test]
    async fn applies_only_the_rows_the_filter_keeps() {
        let output = side_output("SELECT id, name FROM src WHERE id > 1").await;
        output
            .process(&src_batch(&[(1, "a"), (2, "b")]))
            .await
            .unwrap();
        assert_eq!(names(&output.table, &[1, 2]), [None, Some("b".to_string())]);
    }

    /// An upstream may hand over its consumer's projection, in another order.
    #[tokio::test]
    async fn compiles_against_the_fields_of_each_batch() {
        let output = side_output("SELECT id, name FROM src").await;
        let projected = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, true),
                Field::new("id", DataType::Int64, false),
                Field::new(COLUMN_NAME_OP, DataType::Utf8, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["p"])),
                Arc::new(Int64Array::from(vec![5])),
                Arc::new(StringArray::from(vec!["i"])),
            ],
        )
        .unwrap();
        output.process(&projected).await.unwrap();
        output.process(&src_batch(&[(6, "q")])).await.unwrap();
        assert_eq!(
            names(&output.table, &[5, 6]),
            [Some("p".to_string()), Some("q".to_string())]
        );
    }

    /// An idle source's checkpoint markers ride empty batches.
    #[tokio::test]
    async fn applies_an_empty_batch() {
        let output = side_output("SELECT id, name FROM src").await;
        output.process(&src_batch(&[])).await.unwrap();
        assert_eq!(names(&output.table, &[1]), [None]);
    }
}
