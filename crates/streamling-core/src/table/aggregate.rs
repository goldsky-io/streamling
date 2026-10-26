//! The `table_agg` transform: `SELECT <key>, <aggregates> FROM <table> GROUP BY
//! <key>` over a table's live rows, maintained incrementally and read by
//! lookups.

use super::{InMemoryTable, Marks, OP_DELETE, TableRegistry};
use crate::checkpoints::checkpoint_management::CheckpointEpoch;
use crate::data::COLUMN_NAME_OP;
use crate::error::{Result, ResultExt, StreamlingError};
use crate::topology::DEFAULT_MAX_DEAD_RATIO;
use crate::{streamling_bail, streamling_err, streamling_user_bail, streamling_user_err};
use arrow::array::{Array, ArrayRef, AsArray, RecordBatch, StringArray, UInt32Array};
use arrow::compute::{filter, filter_record_batch, is_not_null, take};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::row::{RowConverter, Rows, SortField};
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{Column, DFSchema, DFSchemaRef, ScalarValue, TableReference};
use datafusion::execution::SessionState;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::{Accumulator, Expr, LogicalPlan, Volatility};
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::aggregate::{AggregateFunctionExpr, LoweredAggregateBuilder};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

/// The aggregate functions whose accumulators retract exactly what they added.
const SUPPORTED_FUNCTIONS: [&str; 3] = ["count", "sum", "avg"];
const SQL_SHAPE: &str = "SELECT <key>, <aggregates> FROM <table> [WHERE ...] GROUP BY <key>";
const OP_UPDATE: &str = "u";

/// Registers the `table_agg` named `name`, whose SQL `plan` reads a table in
/// `tables`: `tables` gets its output, which lookups read, and the table
/// updates it on every change to its live rows.
pub fn register(
    name: &str,
    plan: &LogicalPlan,
    tables: &TableRegistry,
    session_state: &SessionState,
    metric_metadata_id: String,
) -> Result<()> {
    let plan = session_state.analyzer().execute_and_check(
        plan.clone(),
        session_state.config_options(),
        |_, _| {},
    )?;
    let (aggregate, base) =
        TableAggregate::try_new(name, &plan, tables, session_state, metric_metadata_id)?;
    tables.register(Arc::clone(&aggregate.output));
    base.add_aggregate(Arc::new(aggregate));
    Ok(())
}

struct Group {
    /// The group's live rows in the table, rows with null arguments included.
    rows: u64,
    accumulators: Vec<Box<dyn Accumulator>>,
}

/// A `table_agg`. Its table calls [`Self::update`] with every change to its
/// live rows, under its write lock, so the groups always aggregate exactly
/// the live rows. The groups are stored in `output`, an [`InMemoryTable`]
/// keyed by the GROUP BY expression, which lookups read. `output` keeps its
/// own version history, written and marked in the table's lock windows, so a
/// batch's readers see `output` at the same moment as the table. The
/// accumulators are never read as of a mark.
pub(super) struct TableAggregate {
    name: String,
    /// `filter`, `key` and `arguments` read the table's stored schema.
    filter: Option<Arc<dyn PhysicalExpr>>,
    key: Arc<dyn PhysicalExpr>,
    converter: RowConverter,
    aggregates: Vec<Arc<AggregateFunctionExpr>>,
    /// Each aggregate's argument expressions.
    arguments: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    /// The key, then each aggregate's value: what `projection` reads.
    aggregate_schema: SchemaRef,
    projection: Vec<Arc<dyn PhysicalExpr>>,
    /// The projection's schema plus `_gs_op`.
    output_schema: SchemaRef,
    output: Arc<InMemoryTable>,
    groups: Mutex<HashMap<Box<[u8]>, Group>>,
}

/// One side of an update: the rows that count, their encoded keys and each
/// aggregate's arguments.
struct GroupInputs {
    keys: Rows,
    arguments: Vec<Vec<ArrayRef>>,
}

impl TableAggregate {
    /// `plan` is analyzed, so its expressions are type-coerced.
    fn try_new(
        name: &str,
        plan: &LogicalPlan,
        tables: &TableRegistry,
        session_state: &SessionState,
        metric_metadata_id: String,
    ) -> Result<(Self, Arc<InMemoryTable>)> {
        let unsupported = |plan: &LogicalPlan| {
            streamling_user_err!(
                "table_agg '{}': unsupported {}; the SQL must be `{}`",
                name,
                plan.display(),
                SQL_SHAPE
            )
        };
        let LogicalPlan::Projection(projection) = plan else {
            return Err(unsupported(plan));
        };
        let aggregate = match projection.input.as_ref() {
            LogicalPlan::Aggregate(aggregate) => aggregate,
            LogicalPlan::Filter(filter)
                if matches!(filter.input.as_ref(), LogicalPlan::Aggregate(_)) =>
            {
                streamling_user_bail!("table_agg '{}': HAVING is not supported", name)
            }
            other => return Err(unsupported(other)),
        };
        let [group_expr] = aggregate.group_expr.as_slice() else {
            return Err(Self::needs_one_key(name));
        };
        if matches!(group_expr, Expr::GroupingSet(_)) {
            return Err(Self::needs_one_key(name));
        }
        let (filter_expr, input) = match aggregate.input.as_ref() {
            LogicalPlan::Filter(filter) => (Some(&filter.predicate), filter.input.as_ref()),
            input => (None, input),
        };
        let (qualifier, base) = Self::base_table(input, tables).ok_or_else(|| {
            streamling_user_err!(
                "table_agg '{}': the SQL must read one table, got {}",
                name,
                input.display()
            )
        })?;

        let key_expr = group_expr.clone().unalias_nested().data;
        let aggregate_exprs: Vec<Expr> = aggregate
            .aggr_expr
            .iter()
            .map(|expr| expr.clone().unalias_nested().data)
            .collect();
        for expr in filter_expr
            .into_iter()
            .chain([&key_expr])
            .chain(&aggregate_exprs)
            .chain(&projection.expr)
        {
            Self::check_expression(name, expr)?;
        }
        for expr in &aggregate_exprs {
            Self::check_aggregate(name, expr)?;
        }
        let (key_qualifier, key_field) = aggregate.schema.qualified_field(0);
        let key_column = Expr::Column(Column::from((key_qualifier, key_field)));
        let Some(key_position) = projection
            .expr
            .iter()
            .position(|expr| expr.clone().unalias_nested().data == key_column)
        else {
            streamling_user_bail!(
                "table_agg '{}': the SQL must select the GROUP BY expression, which is the key lookups use",
                name
            )
        };
        let projected_fields = projection.schema.inner().fields();
        if projected_fields.find(COLUMN_NAME_OP).is_some() {
            streamling_user_bail!(
                "table_agg '{}': the SQL output cannot contain a '{}' column",
                name,
                COLUMN_NAME_OP
            );
        }

        let input_schema = Arc::new(DFSchema::try_from_qualified_schema(
            qualifier,
            base.schema.as_ref(),
        )?);
        let compile = |expr: &Expr, schema: &DFSchemaRef| -> Result<Arc<dyn PhysicalExpr>> {
            let expr = Self::simplifier(session_state, schema).simplify(expr.clone())?;
            Ok(session_state.create_physical_expr(expr, schema)?)
        };
        let aggregates = aggregate_exprs
            .iter()
            .map(|expr| {
                let expr = Self::simplifier(session_state, &input_schema).simplify(expr.clone())?;
                Ok(LoweredAggregateBuilder::new(
                    &expr,
                    &input_schema,
                    base.schema.as_ref(),
                    session_state.execution_props(),
                )
                .build()?
                .aggregate)
            })
            .collect::<Result<Vec<_>>>()?;
        let aggregate_schema = Arc::clone(aggregate.schema.inner());
        // A group's value must fit the column the projection reads it from.
        for (aggregate, field) in aggregates
            .iter()
            .zip(aggregate_schema.fields().iter().skip(1))
        {
            let data_type = aggregate
                .create_sliding_accumulator()?
                .evaluate()?
                .data_type();
            if data_type != *field.data_type() {
                streamling_bail!(
                    "table_agg '{}': {} evaluates to {}, but its column is {}",
                    name,
                    aggregate.name(),
                    data_type,
                    field.data_type()
                );
            }
        }
        let output_schema = Arc::new(Schema::new(
            projected_fields
                .iter()
                .cloned()
                .chain([Arc::new(Field::new(COLUMN_NAME_OP, DataType::Utf8, false))])
                .collect::<Fields>(),
        ));
        let output = Arc::new(InMemoryTable::try_new(
            name,
            &output_schema,
            &[projected_fields[key_position].name().clone()],
            metric_metadata_id,
            DEFAULT_MAX_DEAD_RATIO,
            None,
            base.retention().clone(),
        )?);
        let converter = RowConverter::new(vec![SortField::new(key_field.data_type().clone())])
            .map_err(|e| {
                streamling_user_err!("table_agg '{}': unsupported key type: {}", name, e)
            })?;
        let aggregate = Self {
            name: name.to_string(),
            filter: filter_expr
                .map(|expr| compile(expr, &input_schema))
                .transpose()?,
            key: compile(&key_expr, &input_schema)?,
            converter,
            arguments: aggregates
                .iter()
                .map(|aggregate| aggregate.expressions())
                .collect(),
            aggregates,
            projection: projection
                .expr
                .iter()
                .map(|expr| compile(expr, &aggregate.schema))
                .collect::<Result<_>>()?,
            aggregate_schema,
            output_schema,
            output,
            groups: Mutex::new(HashMap::new()),
        };
        Ok((aggregate, base))
    }

    fn needs_one_key(name: &str) -> StreamlingError {
        streamling_user_err!(
            "table_agg '{}': the SQL needs exactly one GROUP BY expression; for a composite key, group by a concatenation such as `a || ':' || b`",
            name
        )
    }

    /// The table `input` reads, and the name the SQL qualifies its columns
    /// with. A table is planned as a view, so `FROM t` reads
    /// `SubqueryAlias(t)` and `FROM t x` reads `SubqueryAlias(x,
    /// SubqueryAlias(t))`.
    fn base_table(
        input: &LogicalPlan,
        tables: &TableRegistry,
    ) -> Option<(TableReference, Arc<InMemoryTable>)> {
        let LogicalPlan::SubqueryAlias(alias) = input else {
            return None;
        };
        let table = match alias.input.as_ref() {
            LogicalPlan::SubqueryAlias(inner) => inner.alias.to_string(),
            _ => alias.alias.to_string(),
        };
        tables.get(&table).map(|table| (alias.alias.clone(), table))
    }

    /// A row the table removes must pass the filter and land in the group it
    /// did when it was added, and a group's values only refresh when it
    /// changes, so every expression must be deterministic.
    fn check_expression(name: &str, expr: &Expr) -> Result<()> {
        if expr
            .column_refs()
            .iter()
            .any(|column| column.name == COLUMN_NAME_OP)
        {
            streamling_user_bail!(
                "table_agg '{}': can't read '{}', which tables don't store",
                name,
                COLUMN_NAME_OP
            );
        }
        if expr.exists(|expr| {
            Ok(matches!(
                expr,
                Expr::Exists(_) | Expr::InSubquery(_) | Expr::ScalarSubquery(_)
            ))
        })? {
            streamling_user_bail!("table_agg '{}': subqueries are not supported", name);
        }
        if expr.exists(|expr| {
            Ok(matches!(
                expr,
                Expr::ScalarFunction(function)
                    if function.func.signature().volatility != Volatility::Immutable
            ))
        })? {
            streamling_user_bail!(
                "table_agg '{}': expressions must be deterministic, got {}",
                name,
                expr
            );
        }
        Ok(())
    }

    fn check_aggregate(name: &str, expr: &Expr) -> Result<()> {
        let Expr::AggregateFunction(AggregateFunction { func, params }) = expr else {
            streamling_bail!("table_agg '{}': expected an aggregate, got {}", name, expr);
        };
        let function = func.name();
        if !SUPPORTED_FUNCTIONS.contains(&function) {
            streamling_user_bail!(
                "table_agg '{}': unsupported aggregate function '{}'; supported: {}",
                name,
                function,
                SUPPORTED_FUNCTIONS.join(", ")
            );
        }
        if params.distinct {
            streamling_user_bail!(
                "table_agg '{}': {}(DISTINCT ...) is not supported",
                name,
                function
            );
        }
        if params.filter.is_some() {
            streamling_user_bail!(
                "table_agg '{}': {}(...) FILTER (WHERE ...) is not supported",
                name,
                function
            );
        }
        if !params.order_by.is_empty() {
            streamling_user_bail!(
                "table_agg '{}': {}(... ORDER BY ...) is not supported",
                name,
                function
            );
        }
        Ok(())
    }

    /// Folds what the analyzer leaves for the simplifier, such as `coalesce`,
    /// which fails if invoked unsimplified.
    fn simplifier(session_state: &SessionState, schema: &DFSchemaRef) -> ExprSimplifier {
        ExprSimplifier::new(
            SimplifyContext::builder()
                .with_schema(Arc::clone(schema))
                .with_config_options(Arc::clone(session_state.config_options()))
                .build(),
        )
    }

    /// Retracts `removed` and adds `added`, rows of the table's stored schema
    /// that stopped and started being live, then applies the changed groups
    /// to `output`: an update, or a delete for a group left without rows.
    pub(super) fn update(&self, removed: &RecordBatch, added: &RecordBatch) -> Result<()> {
        let removed = self.group_inputs(removed)?;
        let added = self.group_inputs(added)?;
        // Each changed group's rows in `removed` and in `added`.
        let mut changes: HashMap<&[u8], [Vec<u32>; 2]> = HashMap::new();
        for (side, rows) in [&removed, &added].into_iter().enumerate() {
            for (row, key) in rows.keys.iter().enumerate() {
                changes.entry(key.data()).or_default()[side].push(row as u32);
            }
        }
        if changes.is_empty() {
            return Ok(());
        }

        let mut groups = self.groups.lock();
        let mut keys = Vec::with_capacity(changes.len());
        let mut values: Vec<Vec<ScalarValue>> =
            vec![Vec::with_capacity(changes.len()); self.aggregates.len()];
        let mut ops = Vec::with_capacity(changes.len());
        for (key, [removed_rows, added_rows]) in changes {
            let group = match groups.entry(key.into()) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(_) if !removed_rows.is_empty() => {
                    streamling_bail!(
                        "table_agg '{}': retracted rows from a group it doesn't have",
                        self.name
                    )
                }
                Entry::Vacant(entry) => entry.insert(self.new_group()?),
            };
            let (removed_rows, added_rows) = (
                UInt32Array::from(removed_rows),
                UInt32Array::from(added_rows),
            );
            for ((accumulator, removed_arguments), added_arguments) in group
                .accumulators
                .iter_mut()
                .zip(&removed.arguments)
                .zip(&added.arguments)
            {
                if !added_rows.is_empty() {
                    accumulator.update_batch(&Self::take_rows(added_arguments, &added_rows)?)?;
                }
                if !removed_rows.is_empty() {
                    accumulator
                        .retract_batch(&Self::take_rows(removed_arguments, &removed_rows)?)?;
                }
            }
            group.rows = (group.rows + added_rows.len() as u64)
                .checked_sub(removed_rows.len() as u64)
                .ok_or_else(|| {
                    streamling_err!(
                        "table_agg '{}': retracted more rows than a group has",
                        self.name
                    )
                })?;
            for (values, accumulator) in values.iter_mut().zip(&mut group.accumulators) {
                values.push(accumulator.evaluate()?);
            }
            if group.rows == 0 {
                groups.remove(key);
                ops.push(OP_DELETE);
            } else {
                ops.push(OP_UPDATE);
            }
            keys.push(key);
        }

        let parser = self.converter.parser();
        let mut columns = self
            .converter
            .convert_rows(keys.iter().map(|key| parser.parse(key)))
            .streamling_context("failed to decode the group keys")?;
        for values in values {
            columns.push(ScalarValue::iter_to_array(values)?);
        }
        let aggregated = RecordBatch::try_new(Arc::clone(&self.aggregate_schema), columns)
            .streamling_with_context(|| {
                format!("table_agg '{}': invalid group values", self.name)
            })?;
        let mut columns = self
            .projection
            .iter()
            .map(|expr| {
                expr.evaluate(&aggregated)?
                    .into_array(aggregated.num_rows())
            })
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        columns.push(Arc::new(StringArray::from(ops)));
        let changelog = RecordBatch::try_new(Arc::clone(&self.output_schema), columns)
            .streamling_with_context(|| format!("table_agg '{}': invalid output", self.name))?;
        self.output.write(&changelog)
    }

    /// Starts an apply of the table's batch on `output`.
    pub(super) fn begin(&self) {
        self.output.begin();
    }

    /// Finishes the table's apply of a batch that carried the markers of
    /// `epochs` on `output`. Called under the table's lock, with the table's
    /// own mark.
    pub(super) fn finish(&self, epochs: &[CheckpointEpoch]) -> Marks {
        self.output.finish(epochs)
    }

    pub(super) fn record_gauges(&self) {
        self.output.record_gauges();
    }

    /// The rows of `batch` that count: those the filter keeps, without a
    /// NULL key, which lookups can't find. Their keys are encoded as bytes
    /// for the group map, next to each aggregate's evaluated arguments, in
    /// the shape `update_batch` and `retract_batch` take.
    fn group_inputs(&self, batch: &RecordBatch) -> Result<GroupInputs> {
        let batch = match &self.filter {
            Some(filter) => {
                let mask = filter.evaluate(batch)?.into_array(batch.num_rows())?;
                let mask = mask.as_boolean_opt().ok_or_else(|| {
                    streamling_err!(
                        "table_agg '{}': the filter must return booleans, got {}",
                        self.name,
                        mask.data_type()
                    )
                })?;
                filter_record_batch(batch, mask)?
            }
            None => batch.clone(),
        };
        let key = self.key.evaluate(&batch)?.into_array(batch.num_rows())?;
        let (batch, key) = if key.null_count() > 0 {
            let keyed = is_not_null(&key)?;
            (filter_record_batch(&batch, &keyed)?, filter(&key, &keyed)?)
        } else {
            (batch, key)
        };
        let keys = self
            .converter
            .convert_columns(&[key])
            .streamling_context("failed to encode the group keys")?;
        let arguments = self
            .arguments
            .iter()
            .map(|arguments| {
                arguments
                    .iter()
                    .map(|argument| argument.evaluate(&batch)?.into_array(batch.num_rows()))
                    .collect::<datafusion::common::Result<Vec<_>>>()
            })
            .collect::<datafusion::common::Result<_>>()?;
        Ok(GroupInputs { keys, arguments })
    }

    fn take_rows(columns: &[ArrayRef], rows: &UInt32Array) -> Result<Vec<ArrayRef>> {
        Ok(columns
            .iter()
            .map(|column| take(column.as_ref(), rows, None))
            .collect::<std::result::Result<_, _>>()?)
    }

    fn new_group(&self) -> Result<Group> {
        Ok(Group {
            rows: 0,
            accumulators: self
                .aggregates
                .iter()
                .map(|aggregate| aggregate.create_sliding_accumulator())
                .collect::<datafusion::common::Result<_>>()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::COLUMN_NAME_OP;
    use crate::dynamic_table::DynamicTableRegistry;
    use crate::session::SessionManager;
    use crate::table::{InMemoryTable, Mark, VersionRetention, WindowConfig};
    use crate::topology::DEFAULT_MAX_DEAD_RATIO;
    use arrow::array::{
        Array, ArrayRef, AsArray, Decimal128Array, Float64Array, Int64Array, LargeBinaryArray,
        RecordBatch, StringArray,
    };
    use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema, SchemaRef};
    use datafusion::catalog::view::ViewTable;
    use datafusion::datasource::empty::EmptyTable;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::collections::{BTreeMap, HashMap};
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Duration;
    use streamling_common::types::decimal_arb::{
        DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue,
    };

    const BASE: &str = "transfers";
    const AGGREGATE: &str = "volumes";
    const DECIMAL_PRECISION: u32 = 38;
    const DECIMAL_SCALE: u32 = 2;

    /// Table `transfers` (key `id`) over `SELECT id, address, amount FROM
    /// src`, registered as `lib.rs` registers a table: in the table registry,
    /// and as a view of its SQL.
    struct Fixture {
        session_manager: SessionManager,
        base: Arc<InMemoryTable>,
        /// The table's SQL output schema, `_gs_op` included.
        input_schema: SchemaRef,
    }

    impl Fixture {
        /// A GC pass runs on every apply when `window` is set.
        async fn new(amount: Field, window: Option<&str>) -> Self {
            Self::with_retention(amount, window, VersionRetention::Latest).await
        }

        async fn with_retention(
            amount: Field,
            window: Option<&str>,
            retention: VersionRetention,
        ) -> Self {
            let session_manager =
                SessionManager::new(100, 10, DynamicTableRegistry::new(), 1).unwrap();
            session_manager
                .register_table(
                    "src",
                    Arc::new(EmptyTable::new(Arc::new(Schema::new(vec![
                        Field::new("id", DataType::Int64, false),
                        Field::new("address", DataType::Utf8, true),
                        amount,
                        Field::new(COLUMN_NAME_OP, DataType::Utf8, false),
                    ])))),
                )
                .unwrap();
            let (plan, _) = session_manager
                .create_supported_logical_plan("SELECT id, address, amount FROM src".to_string())
                .await
                .unwrap();
            let input_schema = Arc::clone(plan.schema().inner());
            let session_state = session_manager.session_state();
            let base = Arc::new(
                InMemoryTable::try_new(
                    BASE,
                    &input_schema,
                    &["id".to_string()],
                    format!("app::{BASE}"),
                    DEFAULT_MAX_DEAD_RATIO,
                    window.map(|predicate| WindowConfig {
                        predicate,
                        session_state: &session_state,
                        gc_interval: Duration::ZERO,
                    }),
                    retention,
                )
                .unwrap(),
            );
            session_manager.tables().register(Arc::clone(&base));
            session_manager
                .register_table(BASE, Arc::new(ViewTable::new(plan, None)))
                .unwrap();
            Self {
                session_manager,
                base,
                input_schema,
            }
        }

        async fn with_int_amounts(window: Option<&str>) -> Self {
            Self::new(Field::new("amount", DataType::Int64, true), window).await
        }

        async fn register(&self, sql: &str) -> Result<()> {
            let plan = self
                .session_manager
                .create_logical_plan(sql.to_string())
                .await?;
            register(
                AGGREGATE,
                &plan,
                self.session_manager.tables(),
                &self.session_manager.session_state(),
                format!("app::{AGGREGATE}"),
            )
        }

        async fn register_error(&self, sql: &str) -> String {
            match self.register(sql).await {
                Ok(()) => panic!("expected `{sql}` to be rejected"),
                Err(err) => err.to_string(),
            }
        }

        /// Applies `(id, address, amount, _gs_op)` rows to the base table.
        fn apply(&self, rows: &[(i64, Option<&str>, Option<i64>, &str)]) {
            self.apply_marked(rows, &[]);
        }

        /// Applies a batch that carried the markers of `epochs`, and returns
        /// the marks its readers see.
        fn apply_marked(
            &self,
            rows: &[(i64, Option<&str>, Option<i64>, &str)],
            epochs: &[u64],
        ) -> Marks {
            let amounts: ArrayRef = Arc::new(Int64Array::from_iter(rows.iter().map(|row| row.2)));
            self.apply_columns(rows, amounts, epochs)
        }

        fn apply_decimals(&self, rows: &[(i64, Option<&str>, Option<&str>, &str)]) {
            let mut amounts = DecimalArbArrayBuilder::with_capacity(
                rows.len(),
                "amount",
                DECIMAL_PRECISION,
                DECIMAL_SCALE,
            )
            .unwrap();
            for row in rows {
                match row.2 {
                    Some(amount) => amounts.append_str(amount).unwrap(),
                    None => amounts.append_null(),
                }
            }
            let (amounts, _, _) = amounts.finish().into_inner();
            let rows: Vec<_> = rows
                .iter()
                .map(|row| (row.0, row.1, None::<i64>, row.3))
                .collect();
            self.apply_columns(&rows, Arc::new(amounts), &[]);
        }

        fn apply_columns(
            &self,
            rows: &[(i64, Option<&str>, Option<i64>, &str)],
            amounts: ArrayRef,
            epochs: &[u64],
        ) -> Marks {
            let batch = RecordBatch::try_new(
                Arc::clone(&self.input_schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
                    Arc::new(StringArray::from_iter(rows.iter().map(|row| row.1))),
                    amounts,
                    Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
                ],
            )
            .unwrap();
            let epochs: Vec<CheckpointEpoch> =
                epochs.iter().copied().map(CheckpointEpoch).collect();
            self.base.apply(&batch, &epochs).unwrap()
        }

        fn lookup(&self, keys: &[&str], column: &str) -> ArrayRef {
            self.lookup_at(keys, column, None)
        }

        fn lookup_at(&self, keys: &[&str], column: &str, mark: Option<Mark>) -> ArrayRef {
            let keys: ArrayRef = Arc::new(StringArray::from(keys.to_vec()));
            self.session_manager
                .tables()
                .get(AGGREGATE)
                .expect("the aggregate is registered as a table")
                .lookup(&[keys], column, mark)
                .unwrap()
        }

        fn ints(&self, keys: &[&str], column: &str) -> Vec<Option<i64>> {
            self.ints_at_mark(keys, column, None)
        }

        /// The aggregate's `column` as a reader of the batch that returned
        /// `marks` sees it.
        fn ints_at(&self, keys: &[&str], column: &str, marks: &Marks) -> Vec<Option<i64>> {
            self.ints_at_mark(keys, column, mark_of(marks, AGGREGATE))
        }

        fn ints_at_mark(
            &self,
            keys: &[&str],
            column: &str,
            mark: Option<Mark>,
        ) -> Vec<Option<i64>> {
            self.lookup_at(keys, column, mark)
                .as_primitive::<Int64Type>()
                .iter()
                .collect()
        }

        fn floats(&self, keys: &[&str], column: &str) -> Vec<Option<f64>> {
            self.lookup(keys, column)
                .as_primitive::<Float64Type>()
                .iter()
                .collect()
        }

        fn decimals(&self, keys: &[&str], column: &str) -> Vec<Option<DecimalArbValue>> {
            let values = self.lookup(keys, column);
            let values = values
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .expect("decimal_arb is LargeBinary");
            values
                .iter()
                .map(|bytes| {
                    bytes.map(|bytes| {
                        DecimalArbValue::from_canonical_bytes_at_scale(bytes, DECIMAL_SCALE)
                            .unwrap()
                    })
                })
                .collect()
        }
    }

    fn decimal(value: &str) -> Option<DecimalArbValue> {
        Some(DecimalArbValue::from_str(value).unwrap())
    }

    const SUM_AND_COUNT: &str =
        "SELECT address, sum(amount) AS total, count(*) AS n FROM transfers GROUP BY address";

    #[tokio::test]
    async fn aggregates_the_live_rows_per_key() {
        let fixture = Fixture::with_int_amounts(None).await;
        fixture
            .register("SELECT address, sum(amount) AS total, count(*) AS n, avg(amount) AS mean FROM transfers GROUP BY address")
            .await
            .unwrap();

        fixture.apply(&[
            (1, Some("a"), Some(10), "i"),
            (2, Some("a"), Some(5), "i"),
            (3, Some("b"), Some(7), "i"),
        ]);

        assert_eq!(
            fixture.ints(&["a", "b", "c"], "total"),
            [Some(15), Some(7), None]
        );
        assert_eq!(
            fixture.ints(&["a", "b", "c"], "n"),
            [Some(2), Some(1), None]
        );
        assert_eq!(fixture.floats(&["a", "b"], "mean"), [Some(7.5), Some(7.0)]);
    }

    #[tokio::test]
    async fn lookup_row_and_lookup_exists_read_the_groups() {
        let fixture = Fixture::with_int_amounts(None).await;
        fixture.register(SUM_AND_COUNT).await.unwrap();
        fixture.apply(&[(1, Some("a"), Some(10), "i"), (2, Some("a"), Some(5), "i")]);

        let batches = fixture
            .session_manager
            .session_context()
            .sql(&format!(
                "SELECT lookup_row('{AGGREGATE}', 'address', address)['total'] AS total, \
                 lookup_exists('{AGGREGATE}', 'address', address) AS found \
                 FROM (VALUES ('a'), ('b')) AS v(address)"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let totals: Vec<Option<i64>> = batches
            .iter()
            .flat_map(|batch| batch.column(0).as_primitive::<Int64Type>().iter())
            .collect();
        assert_eq!(totals, [Some(15), None]);
        let found: Vec<Option<bool>> = batches
            .iter()
            .flat_map(|batch| batch.column(1).as_boolean().iter())
            .collect();
        assert_eq!(found, [Some(true), Some(false)]);
    }

    #[tokio::test]
    async fn upserts_and_deletes_retract_the_rows_they_replace() {
        let fixture = Fixture::with_int_amounts(None).await;
        fixture.register(SUM_AND_COUNT).await.unwrap();
        fixture.apply(&[
            (1, Some("a"), Some(10), "i"),
            (2, Some("a"), Some(5), "i"),
            (3, Some("b"), Some(7), "i"),
        ]);

        // 3 moves from b to a, which empties b.
        fixture.apply(&[
            (1, Some("a"), Some(20), "u"),
            (2, None, None, "d"),
            (3, Some("a"), Some(1), "u"),
        ]);

        assert_eq!(fixture.ints(&["a", "b"], "total"), [Some(21), None]);
        assert_eq!(fixture.ints(&["a", "b"], "n"), [Some(2), None]);
    }

    #[tokio::test]
    async fn only_the_last_version_of_a_key_in_a_batch_counts() {
        let fixture = Fixture::with_int_amounts(None).await;
        fixture.register(SUM_AND_COUNT).await.unwrap();

        fixture.apply(&[
            (1, Some("a"), Some(10), "i"),
            (1, Some("a"), Some(30), "u"),
            (2, Some("b"), Some(4), "i"),
            (2, None, None, "d"),
        ]);

        assert_eq!(fixture.ints(&["a", "b"], "total"), [Some(30), None]);
        assert_eq!(fixture.ints(&["a", "b"], "n"), [Some(1), None]);
    }

    #[tokio::test]
    async fn rows_the_where_rejects_never_count() {
        let fixture = Fixture::with_int_amounts(None).await;
        fixture
            .register("SELECT address, sum(amount) AS total, count(*) AS n FROM transfers WHERE amount > 5 GROUP BY address")
            .await
            .unwrap();
        fixture.apply(&[(1, Some("a"), Some(3), "i"), (2, Some("a"), Some(10), "i")]);
        assert_eq!(fixture.ints(&["a"], "total"), [Some(10)]);

        // Removing a row the filter rejected retracts nothing.
        fixture.apply(&[(1, None, None, "d")]);
        assert_eq!(fixture.ints(&["a"], "n"), [Some(1)]);

        fixture.apply(&[(2, Some("a"), Some(4), "u")]);
        assert_eq!(fixture.ints(&["a"], "n"), [None]);
    }

    #[tokio::test]
    async fn a_group_whose_arguments_are_all_null_stays() {
        let fixture = Fixture::with_int_amounts(None).await;
        fixture
            .register("SELECT address, sum(amount) AS total, count(amount) AS counted, count(*) AS n FROM transfers GROUP BY address")
            .await
            .unwrap();

        fixture.apply(&[(1, Some("a"), None, "i")]);

        assert_eq!(fixture.ints(&["a"], "total"), [None]);
        assert_eq!(fixture.ints(&["a"], "counted"), [Some(0)]);
        assert_eq!(fixture.ints(&["a"], "n"), [Some(1)]);
    }

    #[tokio::test]
    async fn rows_with_a_null_key_are_not_aggregated() {
        let fixture = Fixture::with_int_amounts(None).await;
        fixture.register(SUM_AND_COUNT).await.unwrap();

        fixture.apply(&[(1, None, Some(5), "i"), (2, Some("a"), Some(1), "i")]);
        fixture.apply(&[(1, None, None, "d")]);

        assert_eq!(fixture.ints(&["a"], "total"), [Some(1)]);
    }

    #[tokio::test]
    async fn window_eviction_retracts_the_evicted_rows() {
        let fixture = Fixture::with_int_amounts(Some("amount >= 10")).await;
        fixture.register(SUM_AND_COUNT).await.unwrap();

        // A GC pass runs at the end of an apply, so it evicts the apply's rows
        // too.
        fixture.apply(&[(1, Some("a"), Some(5), "i"), (2, Some("a"), Some(20), "i")]);
        assert_eq!(fixture.ints(&["a"], "total"), [Some(20)]);
        assert_eq!(fixture.ints(&["a"], "n"), [Some(1)]);
    }

    /// Amounts stand in for event times. Each apply's eviction happens before
    /// any lookup sees it, so a late row never counts.
    #[tokio::test]
    async fn watermark_eviction_retracts_rows_in_the_same_apply() {
        let fixture = Fixture::with_int_amounts(Some("amount >= watermark(amount) - 60")).await;
        fixture.register(SUM_AND_COUNT).await.unwrap();
        for (id, amount, expected) in [
            (1, 0, 1),
            (2, 30, 2),
            (3, 50, 3),
            (4, 120, 1),
            (5, 10, 1),
            (6, 125, 2),
        ] {
            fixture.apply(&[(id, Some("a"), Some(amount), "i")]);
            assert_eq!(
                fixture.ints(&["a"], "n"),
                [Some(expected)],
                "after {amount}"
            );
        }
    }

    fn mark_of(marks: &Marks, table: &str) -> Option<Mark> {
        marks
            .iter()
            .find(|(name, _)| name == table)
            .map(|(_, mark)| *mark)
    }

    async fn versioned_fixture(window: Option<&str>) -> Fixture {
        let fixture = Fixture::with_retention(
            Field::new("amount", DataType::Int64, true),
            window,
            VersionRetention::PerBatch,
        )
        .await;
        fixture.register(SUM_AND_COUNT).await.unwrap();
        fixture
    }

    /// The README's watermark walkthrough, read back as of each apply after
    /// all of them ran.
    #[tokio::test]
    async fn aggregates_read_as_of_a_batch() {
        let fixture = versioned_fixture(Some("amount >= watermark(amount) - 60")).await;
        let applies = [
            (1, 0, 1),
            (2, 30, 2),
            (3, 50, 3),
            (4, 120, 1),
            (5, 10, 1),
            (6, 125, 2),
        ];
        let marks: Vec<Marks> = applies
            .iter()
            .map(|(id, amount, _)| {
                fixture.apply_marked(&[(*id, Some("a"), Some(*amount), "i")], &[])
            })
            .collect();
        for ((_, amount, expected), marks) in applies.iter().zip(&marks) {
            assert_eq!(
                fixture.ints_at(&["a"], "n", marks),
                [Some(*expected)],
                "after {amount}"
            );
        }
        assert_eq!(fixture.ints(&["a"], "n"), [Some(2)], "the latest");
    }

    #[tokio::test]
    async fn a_removed_group_reads_as_missing_in_between() {
        let fixture = versioned_fixture(None).await;
        let inserted = fixture.apply_marked(&[(1, Some("a"), Some(5), "i")], &[]);
        let deleted = fixture.apply_marked(&[(1, Some("a"), Some(5), "d")], &[]);
        let reinserted = fixture.apply_marked(&[(2, Some("a"), Some(7), "i")], &[]);

        assert_eq!(fixture.ints_at(&["a"], "total", &inserted), [Some(5)]);
        assert_eq!(fixture.ints_at(&["a"], "total", &deleted), [None]);
        assert_eq!(fixture.ints_at(&["a"], "total", &reinserted), [Some(7)]);
    }

    fn finalize(epoch: u64) {
        crate::checkpoints::channels::send(
            crate::checkpoints::checkpoint_management::CHECKPOINT_COORDINATOR_CHANNEL,
            crate::checkpoints::checkpoint_management::CheckpointMessage::Finalizer(
                CheckpointEpoch(epoch),
            ),
        )
        .unwrap();
    }

    /// Errors when the aggregate's history a reader of the batch that
    /// returned `marks` needs was collected.
    fn assert_aggregate_collected(fixture: &Fixture, marks: &Marks) {
        let keys: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
        let output = fixture.session_manager.tables().get(AGGREGATE).unwrap();
        let err = output
            .lookup(&[keys], "total", mark_of(marks, AGGREGATE))
            .unwrap_err();
        assert!(err.to_string().contains("already collected"), "{err}");
    }

    /// `#[serial]`: the coordinator channel is process-wide.
    #[tokio::test]
    #[serial_test::serial]
    async fn a_finalizer_collects_the_outputs_history_on_the_next_apply() {
        // Far above the epochs other tests record.
        const EPOCH: u64 = 2_000_000;
        let fixture = versioned_fixture(None).await;
        let first = fixture.apply_marked(&[(1, Some("a"), Some(5), "i")], &[]);
        let second = fixture.apply_marked(&[(1, Some("a"), Some(6), "u")], &[EPOCH]);
        finalize(EPOCH);

        fixture.apply_marked(&[], &[]);

        assert_aggregate_collected(&fixture, &first);
        assert_eq!(fixture.ints_at(&["a"], "total", &second), [Some(6)]);
    }

    const MODEL_ADDRESSES: [&str; 2] = ["a", "b"];

    /// `(id, address, amount, _gs_op)`.
    type Row = (i64, Option<&'static str>, Option<i64>, &'static str);

    /// Applies batches from two partitions taking turns, and returns their
    /// marks in apply order. Partition 0 owns ids 1, 2 and 6, partition 1
    /// the others, as a topic keyed by id would place them; group `a` gets
    /// rows from both. `epochs`' markers ride partition 1's first batch and
    /// partition 0's second.
    fn apply_two_partitions(fixture: &Fixture, epochs: &[u64]) -> Vec<Marks> {
        let batches: [(&[Row], &[u64]); 6] = [
            (
                &[(1, Some("a"), Some(10), "i"), (2, Some("b"), Some(20), "i")],
                &[],
            ),
            (
                &[(3, Some("a"), Some(1), "i"), (4, Some("b"), Some(2), "i")],
                epochs,
            ),
            (
                &[(1, Some("b"), Some(11), "u"), (2, None, None, "d")],
                epochs,
            ),
            (
                &[(3, Some("a"), Some(5), "u"), (5, Some("a"), Some(7), "i")],
                &[],
            ),
            (&[(6, Some("b"), Some(100), "i")], &[]),
            (&[(4, None, None, "d")], &[]),
        ];
        batches
            .into_iter()
            .map(|(rows, epochs)| fixture.apply_marked(rows, epochs))
            .collect()
    }

    /// Asserts that the aggregate, as a reader of the batch that returned
    /// `marks` sees it, is `SUM_AND_COUNT` over the table's rows as that
    /// reader sees them.
    fn assert_matches_the_table_at(fixture: &Fixture, marks: &Marks) {
        const IDS: [i64; 6] = [1, 2, 3, 4, 5, 6];
        let ids: ArrayRef = Arc::new(Int64Array::from(IDS.to_vec()));
        let base_mark = mark_of(marks, BASE);
        let addresses = fixture
            .base
            .lookup(&[Arc::clone(&ids)], "address", base_mark)
            .unwrap();
        let amounts = fixture.base.lookup(&[ids], "amount", base_mark).unwrap();
        let mut expected: BTreeMap<&str, (i64, i64)> = BTreeMap::new();
        for (address, amount) in addresses
            .as_string::<i32>()
            .iter()
            .zip(amounts.as_primitive::<Int64Type>().iter())
        {
            if let (Some(address), Some(amount)) = (address, amount) {
                let (total, n) = expected.entry(address).or_default();
                *total += amount;
                *n += 1;
            }
        }
        for address in MODEL_ADDRESSES {
            let (total, n) = match expected.get(address) {
                Some((total, n)) => (Some(*total), Some(*n)),
                None => (None, None),
            };
            assert_eq!(
                fixture.ints_at(&[address], "total", marks),
                [total],
                "{address} at {marks:?}"
            );
            assert_eq!(
                fixture.ints_at(&[address], "n", marks),
                [n],
                "{address} at {marks:?}"
            );
        }
    }

    #[tokio::test]
    async fn the_aggregate_and_its_table_agree_after_every_batch_across_partitions() {
        let fixture = versioned_fixture(None).await;
        for marks in apply_two_partitions(&fixture, &[]) {
            assert_matches_the_table_at(&fixture, &marks);
        }
    }

    /// `#[serial]`: the coordinator channel is process-wide.
    #[tokio::test]
    #[serial_test::serial]
    async fn after_collection_the_aggregate_and_its_table_still_agree() {
        // Far above the epochs other tests record.
        const EPOCH: u64 = 3_000_000;
        let fixture = versioned_fixture(None).await;
        // The cut is partition 1's first batch's mark, so reads from it on
        // still work.
        let marks = apply_two_partitions(&fixture, &[EPOCH]);
        finalize(EPOCH);
        let next = fixture.apply_marked(&[], &[]);

        assert_aggregate_collected(&fixture, &marks[0]);
        for marks in marks[1..].iter().chain([&next]) {
            assert_matches_the_table_at(&fixture, marks);
        }
    }

    #[tokio::test]
    async fn the_key_and_outputs_can_be_expressions_over_an_aliased_table() {
        let fixture = Fixture::with_int_amounts(None).await;
        fixture
            .register("SELECT lower(t.address) AS k, sum(t.amount) * 2 AS doubled FROM transfers t GROUP BY 1")
            .await
            .unwrap();

        fixture.apply(&[(1, Some("A"), Some(1), "i"), (2, Some("a"), Some(2), "i")]);

        assert_eq!(fixture.ints(&["a", "A"], "doubled"), [Some(6), None]);
    }

    #[tokio::test]
    async fn decimal_arb_sums_retract_exactly() {
        let fixture = Fixture::new(
            DecimalArbType::field("amount", DECIMAL_PRECISION, DECIMAL_SCALE, true).unwrap(),
            None,
        )
        .await;
        fixture
            .register("SELECT address, sum(amount) AS total, avg(amount) AS mean FROM transfers GROUP BY address")
            .await
            .unwrap();
        fixture.apply_decimals(&[
            (1, Some("a"), Some("1.10"), "i"),
            (2, Some("a"), Some("2.20"), "i"),
        ]);
        assert_eq!(fixture.decimals(&["a"], "total"), [decimal("3.30")]);

        fixture.apply_decimals(&[(1, Some("a"), Some("0.05"), "u"), (2, None, None, "d")]);

        assert_eq!(fixture.decimals(&["a"], "total"), [decimal("0.05")]);
    }

    /// Random upserts, deletes and evictions, checked after every apply
    /// against sums and counts folded from a model of the table's live rows.
    #[tokio::test]
    async fn matches_the_live_rows_after_random_changes() {
        const WINDOW_MIN_AMOUNT: i64 = 3;
        const IDS: i64 = 20;
        const STEPS: usize = 300;
        const ADDRESSES: [&str; 4] = ["a", "b", "c", "d"];
        let fixture = Fixture::with_int_amounts(Some("amount >= 3")).await;
        fixture.register(SUM_AND_COUNT).await.unwrap();
        let mut rng = StdRng::seed_from_u64(7);
        let mut live: HashMap<i64, (Option<&str>, Option<i64>)> = HashMap::new();

        for _ in 0..STEPS {
            let mut rows = Vec::new();
            for _ in 0..rng.gen_range(0..8) {
                let id = rng.gen_range(0..IDS);
                if rng.gen_bool(0.2) {
                    live.remove(&id);
                    rows.push((id, None, None, "d"));
                } else {
                    let address =
                        (!rng.gen_bool(0.1)).then(|| ADDRESSES[rng.gen_range(0..ADDRESSES.len())]);
                    let amount = (!rng.gen_bool(0.1)).then(|| rng.gen_range(0..10));
                    live.insert(id, (address, amount));
                    rows.push((id, address, amount, "u"));
                }
            }
            fixture.apply(&rows);
            // The apply's GC pass evicts what it left outside.
            live.retain(|_, (_, amount)| amount.is_some_and(|amount| amount >= WINDOW_MIN_AMOUNT));

            let mut expected: BTreeMap<&str, (Option<i64>, i64)> = BTreeMap::new();
            for (address, amount) in live.values() {
                if let Some(address) = address {
                    let (total, n) = expected.entry(address).or_default();
                    *total = match (*total, amount) {
                        (Some(total), Some(amount)) => Some(total + amount),
                        (total, amount) => total.or(*amount),
                    };
                    *n += 1;
                }
            }
            for address in ADDRESSES {
                let (total, n) = match expected.get(address) {
                    Some((total, n)) => (*total, Some(*n)),
                    None => (None, None),
                };
                assert_eq!(fixture.ints(&[address], "total"), [total], "{address}");
                assert_eq!(fixture.ints(&[address], "n"), [n], "{address}");
            }
        }
    }

    #[tokio::test]
    async fn rejects_unsupported_sql() {
        let fixture = Fixture::with_int_amounts(None).await;
        for (sql, expected) in [
            (
                "SELECT address, id, sum(amount) FROM transfers GROUP BY address, id",
                "exactly one GROUP BY expression",
            ),
            (
                "SELECT sum(amount) FROM transfers",
                "exactly one GROUP BY expression",
            ),
            (
                "SELECT sum(amount) AS total FROM transfers GROUP BY address",
                "must select the GROUP BY expression",
            ),
            (
                "SELECT address, sum(amount) FROM transfers GROUP BY address HAVING sum(amount) > 1",
                "HAVING is not supported",
            ),
            (
                "SELECT address, sum(DISTINCT amount) FROM transfers GROUP BY address",
                "DISTINCT",
            ),
            (
                "SELECT address, sum(amount) FILTER (WHERE amount > 1) FROM transfers GROUP BY address",
                "FILTER",
            ),
            (
                "SELECT address, max(amount) FROM transfers GROUP BY address",
                "unsupported aggregate function 'max'",
            ),
            (
                "SELECT address, sum(amount) FROM transfers WHERE random() > 0.5 GROUP BY address",
                "deterministic",
            ),
            (
                "SELECT address, sum(amount) AS total, now() AS at FROM transfers GROUP BY address",
                "deterministic",
            ),
            (
                "SELECT address, sum(lookup_eq('transfers', 'id', id, 'amount')) FROM transfers GROUP BY address",
                "deterministic",
            ),
            (
                "SELECT address, count(*) FROM transfers WHERE lookup_exists('transfers', 'id', id) GROUP BY address",
                "deterministic",
            ),
            (
                "SELECT address, sum(lookup_row('transfers', 'id', id)['amount']) FROM transfers GROUP BY address",
                "deterministic",
            ),
            (
                "SELECT address, sum(amount) FROM transfers WHERE _gs_op = 'i' GROUP BY address",
                "_gs_op",
            ),
            (
                "SELECT address, sum(amount) FROM src GROUP BY address",
                "must read one table",
            ),
            (
                "SELECT t.address, sum(t.amount) FROM transfers t JOIN transfers u ON t.id = u.id GROUP BY t.address",
                "JOIN queries not supported",
            ),
            (
                "SELECT address, sum(amount) FROM transfers WHERE id IN (SELECT id FROM src) GROUP BY address",
                "subqueries are not supported",
            ),
            (
                "SELECT address, sum(amount) FROM transfers GROUP BY address ORDER BY address",
                "unsupported Sort",
            ),
        ] {
            let err = fixture.register_error(sql).await;
            assert!(err.contains(expected), "{sql}: {err}");
        }
    }

    /// A table backend stores each group's `state()` and restores it with
    /// `merge_batch`, so a restored group must retract and evaluate exactly
    /// like the group it came from, for every function and type `table_agg`
    /// supports.
    #[tokio::test]
    async fn every_supported_aggregate_round_trips_its_state() {
        const ROUND_TRIP: &str = "SELECT address, count(*) AS n, count(amount) AS counted, sum(amount) AS total, avg(amount) AS mean FROM transfers GROUP BY address";
        let mut decimal_arb_amounts =
            DecimalArbArrayBuilder::with_capacity(4, "amount", DECIMAL_PRECISION, DECIMAL_SCALE)
                .unwrap();
        for amount in [Some("1.50"), Some("2.25"), None, Some("4.00")] {
            match amount {
                Some(amount) => decimal_arb_amounts.append_str(amount).unwrap(),
                None => decimal_arb_amounts.append_null(),
            }
        }
        let (decimal_arb_amounts, _, _) = decimal_arb_amounts.finish().into_inner();
        let cases: [(Field, ArrayRef); 4] = [
            (
                Field::new("amount", DataType::Int64, true),
                Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(4)])),
            ),
            (
                Field::new("amount", DataType::Float64, true),
                Arc::new(Float64Array::from(vec![
                    Some(1.5),
                    Some(2.25),
                    None,
                    Some(4.0),
                ])),
            ),
            (
                Field::new(
                    "amount",
                    DataType::Decimal128(DECIMAL_PRECISION as u8, DECIMAL_SCALE as i8),
                    true,
                ),
                Arc::new(
                    Decimal128Array::from(vec![Some(150), Some(225), None, Some(400)])
                        .with_precision_and_scale(DECIMAL_PRECISION as u8, DECIMAL_SCALE as i8)
                        .unwrap(),
                ),
            ),
            (
                DecimalArbType::field("amount", DECIMAL_PRECISION, DECIMAL_SCALE, true).unwrap(),
                Arc::new(decimal_arb_amounts),
            ),
        ];

        for (amount, amounts) in cases {
            let data_type = amount.data_type().clone();
            let fixture = Fixture::new(amount, None).await;
            let session_state = fixture.session_manager.session_state();
            let plan = fixture
                .session_manager
                .create_logical_plan(ROUND_TRIP.to_string())
                .await
                .unwrap();
            let plan = session_state
                .analyzer()
                .execute_and_check(plan, session_state.config_options(), |_, _| {})
                .unwrap();
            let (aggregate, _) = TableAggregate::try_new(
                AGGREGATE,
                &plan,
                fixture.session_manager.tables(),
                &session_state,
                format!("app::{AGGREGATE}"),
            )
            .unwrap();
            let rows = amounts.len();
            let batch = RecordBatch::try_new(
                Arc::clone(&fixture.input_schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(0..rows as i64)),
                    Arc::new(StringArray::from(vec!["a"; rows])),
                    amounts,
                    Arc::new(StringArray::from(vec!["i"; rows])),
                ],
            )
            .unwrap();

            for (function, arguments) in aggregate.aggregates.iter().zip(&aggregate.arguments) {
                let case = format!("{} over {data_type}", function.name());
                let arguments = arguments
                    .iter()
                    .map(|argument| argument.evaluate(&batch)?.into_array(rows))
                    .collect::<datafusion::common::Result<Vec<_>>>()
                    .unwrap();
                let mut live = function.create_sliding_accumulator().unwrap();
                live.update_batch(&arguments).unwrap();
                let state = live
                    .state()
                    .unwrap()
                    .iter()
                    .map(ScalarValue::to_array)
                    .collect::<datafusion::common::Result<Vec<_>>>()
                    .unwrap();
                let mut restored = function.create_sliding_accumulator().unwrap();
                restored.merge_batch(&state).unwrap();

                for retracted in [0..1, 1..rows] {
                    let retracted: Vec<ArrayRef> = arguments
                        .iter()
                        .map(|argument| argument.slice(retracted.start, retracted.len()))
                        .collect();
                    live.retract_batch(&retracted).unwrap();
                    restored
                        .retract_batch(&retracted)
                        .unwrap_or_else(|err| panic!("{case}: {err}"));
                    assert_eq!(
                        restored.evaluate().unwrap(),
                        live.evaluate().unwrap(),
                        "{case}"
                    );
                }
            }
        }
    }
}
