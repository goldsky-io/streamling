//! A table's `window`: the predicate a table keeps its rows by, planned once
//! and compiled for each GC pass with `now()` and `watermark()` folded.
//!
//! A range window compares a column, or a `CAST` of one, with an expression
//! of `now()` and `watermark()`. A pass folds that expression to a bound and
//! has the backend evict the rows below it. Any other window is evaluated
//! over every stored row.

use super::watermark::{WATERMARK_NAME, WatermarkFunc, Watermarks};
use crate::error::{Result, ResultExt};
use crate::{streamling_err, streamling_user_bail, streamling_user_err};
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch};
use arrow::compute::{not, prep_null_mask_filter};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, DFSchemaRef, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::execution::{FunctionRegistry, SessionState};
use datafusion::functions_aggregate::min_max::MaxAccumulator;
use datafusion::logical_expr::expr::{Cast, TryCast};
use datafusion::logical_expr::{Accumulator, BinaryExpr, Expr, ExprSchemable, Operator, ScalarUDF};
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::physical_expr::PhysicalExpr;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

/// The column a window reads for the wall-clock time a row was applied.
pub const WALL_CLOCK_COLUMN: &str = "__timestamp";
pub(super) const WALL_CLOCK_TIME_ZONE: &str = "UTC";

/// Integers up to this magnitude convert to `Float64` and back exactly.
const F64_EXACT_INTEGER: f64 = (1u64 << f64::MANTISSA_DIGITS) as f64;
/// Integers up to this magnitude convert to `Float32` and back exactly.
const F32_EXACT_INTEGER: f64 = (1u64 << f32::MANTISSA_DIGITS) as f64;

/// A table's `window`.
pub struct WindowConfig<'a> {
    /// SQL over the table's columns, [`WALL_CLOCK_COLUMN`] and
    /// `watermark(column)`. The table keeps the rows it is true for.
    pub predicate: &'a str,
    pub session_state: &'a SessionState,
    /// Minimum time between GC passes.
    pub gc_interval: Duration,
}

/// A planned [`WindowConfig`].
pub(super) struct Window {
    /// Type-coerced but not simplified, so each GC pass folds `now()` and
    /// `watermark()` anew.
    predicate: Expr,
    /// The stored schema, which `predicate` reads.
    schema: DFSchemaRef,
    reads_wall_clock: bool,
    /// The columns `watermark()` reads, by name and position in `schema`.
    watermark_columns: Vec<(String, usize)>,
    watermarks: Watermarks,
    /// Knows `watermark()`, unlike the pipeline's session.
    session_state: SessionState,
    gc_interval: Duration,
    /// The column a range window compares.
    range_column: Option<String>,
}

/// A [`Window`] with `now()` folded to the current time and `watermark()` to
/// the current watermarks.
pub(super) struct CompiledWindow {
    predicate: Arc<dyn PhysicalExpr>,
    /// For a range window, the rows to evict: below the bound, or equal when
    /// inclusive, or null. In the column's type.
    pub(super) eviction: Option<(ScalarValue, bool)>,
}

impl Window {
    /// Plans `config.predicate` over `fields`, the stored columns, which get
    /// [`WALL_CLOCK_COLUMN`] appended when the predicate reads it.
    pub(super) fn try_new(table: &str, config: &WindowConfig, fields: &Fields) -> Result<Self> {
        if fields.find(WALL_CLOCK_COLUMN).is_some() {
            streamling_user_bail!(
                "table '{}': the SQL output cannot contain '{}', which a window reserves for the wall-clock time",
                table,
                WALL_CLOCK_COLUMN
            );
        }
        let invalid = |err: DataFusionError| {
            streamling_user_err!(
                "table '{}': invalid window '{}': {}",
                table,
                config.predicate,
                err
            )
        };
        let wall_clock = Field::new(
            WALL_CLOCK_COLUMN,
            DataType::Timestamp(TimeUnit::Nanosecond, Some(WALL_CLOCK_TIME_ZONE.into())),
            false,
        );
        let with_wall_clock = DFSchema::try_from(Schema::new(
            fields
                .iter()
                .cloned()
                .chain([Arc::new(wall_clock)])
                .collect::<Fields>(),
        ))?;
        let watermarks = Watermarks::default();
        let mut session_state = config.session_state.clone();
        session_state.register_udf(Arc::new(ScalarUDF::from(WatermarkFunc::new(Arc::clone(
            &watermarks,
        )))))?;
        let predicate = session_state
            .create_logical_expr(config.predicate, &with_wall_clock)
            .map_err(invalid)?;
        let reads_wall_clock = predicate
            .column_refs()
            .iter()
            .any(|column| column.name == WALL_CLOCK_COLUMN);
        let schema = Arc::new(if reads_wall_clock {
            with_wall_clock
        } else {
            DFSchema::try_from(Schema::new(fields.clone()))?
        });
        let predicate = ExprSimplifier::new(Self::simplify_context(&session_state, &schema))
            .coerce(predicate, &schema)
            .map_err(invalid)?;
        let data_type = predicate.get_type(&schema).map_err(invalid)?;
        if data_type != DataType::Boolean {
            streamling_user_bail!(
                "table '{}': the window must be a boolean expression, got {}",
                table,
                data_type
            );
        }
        let mut watermark_columns = Vec::new();
        predicate
            .apply(|expr| {
                if let Expr::ScalarFunction(function) = expr
                    && function.name() == WATERMARK_NAME
                {
                    let [Expr::Column(column)] = function.args.as_slice() else {
                        return Err(DataFusionError::Plan(format!(
                            "{WATERMARK_NAME}() takes one column of the table, got {expr}"
                        )));
                    };
                    let index = schema.inner().index_of(&column.name)?;
                    watermark_columns.push((column.name.clone(), index));
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .map_err(invalid)?;
        for (name, index) in &watermark_columns {
            let data_type = schema.field(*index).data_type();
            watermarks
                .lock()
                .insert(name.clone(), ScalarValue::try_from(data_type)?);
        }
        let mut window = Self {
            predicate,
            schema,
            reads_wall_clock,
            watermark_columns,
            watermarks,
            session_state,
            gc_interval: config.gc_interval,
            range_column: None,
        };
        window.range_column = window.find_range_column().map_err(invalid)?;
        if window.range_column.is_none() {
            warn!(
                "table '{}': window '{}' isn't a range, so each GC pass evaluates every stored row. Compare a column, not a function of it, with an expression of now() or watermark(), such as `ts >= watermark(ts) - interval '1 hour'`",
                table, config.predicate
            );
        }
        Ok(window)
    }

    fn simplify_context(session_state: &SessionState, schema: &DFSchemaRef) -> SimplifyContext {
        SimplifyContext::builder()
            .with_schema(Arc::clone(schema))
            .with_config_options(Arc::clone(session_state.config_options()))
            .with_current_time()
            .build()
    }

    /// The predicate with `now()` and `watermark()` folded.
    fn simplify(&self) -> datafusion::common::Result<Expr> {
        ExprSimplifier::new(Self::simplify_context(&self.session_state, &self.schema))
            .simplify(self.predicate.clone())
    }

    pub(super) fn compile(&self) -> datafusion::common::Result<CompiledWindow> {
        let predicate = self.simplify()?;
        let eviction = self
            .range_column
            .as_ref()
            .and_then(|column| self.eviction(&predicate, column));
        Ok(CompiledWindow {
            predicate: self
                .session_state
                .create_physical_expr(predicate, &self.schema)?,
            eviction,
        })
    }

    /// The column of a range window. A NULL watermark folds the comparison
    /// to NULL, so the check substitutes zeros. Only called before the table
    /// is in use.
    fn find_range_column(&self) -> datafusion::common::Result<Option<String>> {
        let watermarks = self.watermarks.lock().clone();
        for (name, value) in &watermarks {
            if let Ok(zero) = ScalarValue::new_zero(&value.data_type()) {
                self.watermarks.lock().insert(name.clone(), zero);
            }
        }
        let predicate = self.simplify();
        *self.watermarks.lock() = watermarks;
        let predicate = predicate?;
        Ok(self
            .compared_column(&predicate)
            .filter(|column| self.eviction(&predicate, column).is_some()))
    }

    /// The column `predicate` compares with a literal, directly or through a
    /// cast.
    fn compared_column(&self, predicate: &Expr) -> Option<String> {
        let (column_side, _, _) = Self::comparison(predicate)?;
        match column_side {
            Expr::Column(column) => Some(column.name.clone()),
            Expr::Cast(Cast { expr, .. }) | Expr::TryCast(TryCast { expr, .. }) => {
                match expr.as_ref() {
                    Expr::Column(column) => Some(column.name.clone()),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// `predicate` as `column side`, the literal, and whether the column
    /// side must be strictly greater, if it compares an expression with a
    /// literal from above.
    fn comparison(predicate: &Expr) -> Option<(&Expr, &ScalarValue, bool)> {
        let Expr::BinaryExpr(BinaryExpr { left, op, right }) = predicate else {
            return None;
        };
        match (left.as_ref(), op, right.as_ref()) {
            (column, Operator::GtEq, Expr::Literal(bound, _)) => Some((column, bound, false)),
            (column, Operator::Gt, Expr::Literal(bound, _)) => Some((column, bound, true)),
            (Expr::Literal(bound, _), Operator::LtEq, column) => Some((column, bound, false)),
            (Expr::Literal(bound, _), Operator::Lt, column) => Some((column, bound, true)),
            _ => None,
        }
    }

    /// The rows a folded range `predicate` over `column` keeps out, as an
    /// eviction bound in the column's type, if it converts exactly.
    fn eviction(&self, predicate: &Expr, column: &str) -> Option<(ScalarValue, bool)> {
        let (column_side, bound, strict) = Self::comparison(predicate)?;
        if bound.is_null() {
            return None;
        }
        let column_type = self
            .schema
            .field_with_unqualified_name(column)
            .ok()?
            .data_type();
        match column_side {
            Expr::Column(compared) if compared.name == column => {
                // Keeping `col >= b` evicts `col < b`; keeping `col > b`
                // evicts `col <= b`.
                Some((Self::same_value(bound, column_type)?, strict))
            }
            Expr::Cast(Cast { expr, field }) | Expr::TryCast(TryCast { expr, field }) if matches!(expr.as_ref(), Expr::Column(compared) if compared.name == column) => {
                Self::integer_eviction(bound, field.data_type(), column_type, strict)
            }
            _ => None,
        }
    }

    /// `bound` in `column_type`, if it's the same value: the same type, or a
    /// timestamp of the same unit in another time zone.
    fn same_value(bound: &ScalarValue, column_type: &DataType) -> Option<ScalarValue> {
        match (bound.data_type(), column_type) {
            (bound_type, column_type) if bound_type == *column_type => Some(bound.clone()),
            (DataType::Timestamp(bound_unit, _), DataType::Timestamp(column_unit, _))
                if bound_unit == *column_unit =>
            {
                bound.cast_to(column_type).ok()
            }
            _ => None,
        }
    }

    /// The eviction bound of `CAST(col AS cast_type) >= b` (or `>`) for an
    /// integer column cast to a float, where every integer in reach converts
    /// exactly: keeping `CAST(col) >= b` keeps `col >= ceil(b)`, and keeping
    /// `CAST(col) > b` keeps `col > floor(b)`.
    fn integer_eviction(
        bound: &ScalarValue,
        cast_type: &DataType,
        column_type: &DataType,
        strict: bool,
    ) -> Option<(ScalarValue, bool)> {
        let exact = match cast_type {
            DataType::Float64 => F64_EXACT_INTEGER,
            DataType::Float32 => F32_EXACT_INTEGER,
            _ => return None,
        };
        if !column_type.is_integer() {
            return None;
        }
        let bound = match bound {
            ScalarValue::Float64(Some(bound)) => *bound,
            ScalarValue::Float32(Some(bound)) => f64::from(*bound),
            _ => return None,
        };
        let integer = if strict { bound.floor() } else { bound.ceil() };
        // The integers next to the bound must convert exactly, so every
        // other integer falls on the same side as its neighbour.
        if !integer.is_finite() || integer.abs() >= exact {
            return None;
        }
        let converted = ScalarValue::Int64(Some(integer as i64))
            .cast_to(column_type)
            .ok()?;
        (!converted.is_null()).then_some((converted, strict))
    }

    pub(super) fn reads_watermark(&self) -> bool {
        !self.watermark_columns.is_empty()
    }

    pub(super) fn reads_wall_clock(&self) -> bool {
        self.reads_wall_clock
    }

    pub(super) fn gc_interval(&self) -> Duration {
        self.gc_interval
    }

    pub(super) fn range_column(&self) -> Option<&str> {
        self.range_column.as_deref()
    }

    /// The stored schema.
    pub(super) fn schema(&self) -> &SchemaRef {
        self.schema.inner()
    }

    /// Raises each watermark to its column's largest value in `batch`, rows
    /// of the stored schema. Returns whether one moved.
    pub(super) fn advance_watermarks(&self, batch: &RecordBatch) -> Result<bool> {
        let mut watermarks = self.watermarks.lock();
        let mut moved = false;
        for (name, index) in &self.watermark_columns {
            let mut max = MaxAccumulator::try_new(batch.schema().field(*index).data_type())?;
            max.update_batch(&[Arc::clone(batch.column(*index))])?;
            let max = max.evaluate()?;
            let watermark = watermarks
                .get_mut(name)
                .expect("every watermark column has a watermark");
            // A null max is less than any value, and equal to a null watermark.
            if max > *watermark {
                *watermark = max;
                moved = true;
            }
        }
        Ok(moved)
    }

    /// The rows of `batch`, in the stored schema, that `compiled` keeps out:
    /// those its predicate is false or NULL for.
    pub(super) fn outside(
        &self,
        compiled: &CompiledWindow,
        batch: &RecordBatch,
    ) -> Result<BooleanArray> {
        let inside = compiled
            .predicate
            .evaluate(batch)?
            .into_array(batch.num_rows())?;
        let inside = inside.as_boolean_opt().ok_or_else(|| {
            streamling_err!(
                "the window must return booleans, got {}",
                inside.data_type()
            )
        })?;
        // `prep_null_mask_filter` panics without a null buffer.
        Ok(if inside.null_count() > 0 {
            not(&prep_null_mask_filter(inside))?
        } else {
            not(inside)?
        })
    }

    /// The watermarks as a one-row batch in Arrow IPC, for the store's meta.
    pub(super) fn encode_watermarks(&self) -> Result<Vec<u8>> {
        let watermarks = self.watermarks.lock();
        let (fields, columns): (Vec<Field>, Vec<ArrayRef>) = self
            .watermark_columns
            .iter()
            .map(|(name, _)| {
                let value = &watermarks[name];
                Ok((Field::new(name, value.data_type(), true), value.to_array()?))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .unzip();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
        let mut bytes = Vec::new();
        let mut writer = StreamWriter::try_new(&mut bytes, &batch.schema())?;
        writer.write(&batch)?;
        writer.finish()?;
        drop(writer);
        Ok(bytes)
    }

    /// Restores the watermarks [`Self::encode_watermarks`] stored. A column
    /// the window no longer reads, or reads with another type, is ignored.
    pub(super) fn restore_watermarks(&self, bytes: &[u8]) -> Result<()> {
        let mut reader = StreamReader::try_new(bytes, None)
            .streamling_context("failed to read the stored watermarks")?;
        let Some(batch) = reader.next().transpose()? else {
            return Ok(());
        };
        let mut watermarks = self.watermarks.lock();
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            if let Some(watermark) = watermarks.get_mut(field.name())
                && watermark.data_type() == *field.data_type()
            {
                *watermark = ScalarValue::try_from_array(column, 0)?;
            }
        }
        Ok(())
    }
}
