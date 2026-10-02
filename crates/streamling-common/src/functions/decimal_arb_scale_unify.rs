//! `OptimizerRule` that brings decimal_arb columns to one common scale wherever
//! DataFusion matches or merges rows on the raw `LargeBinary` bytes.
//!
//! The canonical encoding carries no scale — the scale lives only in field
//! metadata — so the same number at two scales has two different byte
//! strings. Two plan shapes let those meet without any expression the
//! `DecimalArbExprPlanner` or `DecimalArbExprRewrite` could intercept:
//!
//! - **`UNION` / `UNION ALL`.** Branches at different scales feed differently
//!   encoded rows into one column. DataFusion's union keeps only the metadata
//!   the branches agree on at planning time, then lets the *last* branch's
//!   `(precision, scale)` win during coercion, so every merging operator above
//!   it (`ORDER BY`, `GROUP BY`, `LIMIT`, `SUM`) read the other branch's rows
//!   off by a power of ten: `1` came out as `0.01`.
//! - **`JOIN` equality keys.** `JOIN … USING (v)`, `NATURAL JOIN`, and the
//!   semi/anti joins that `IN (SELECT …)` / `NOT IN (SELECT …)` decorrelate
//!   into hash the key bytes, so equal numbers at different scales never
//!   matched — the join silently returned nothing (or, for `NOT IN`,
//!   everything). `ON a.v = b.v` was already correct: that is a `BinaryExpr`,
//!   which the planner routes to `decimal_arb_eq`.
//!
//! - **`UNNEST`.** The unnested column is rebuilt from the element's data type
//!   alone, so it lost the decimal_arb metadata; it is restored here.
//! - **Comparisons over derived tables.** The analyzer rewrite sees each
//!   node's planner-time schema, so `v < c` above `SELECT CASE … AS v` was
//!   left as a byte comparison; with schemas recomputed it is routed here.
//!
//! Each affected union input / join key is wrapped in
//! `decimal_arb_rescale(expr, p, s)` at the widest scale present, which is
//! exact. Runs after DataFusion's own optimizer rules (it is appended), so the
//! decorrelated joins already exist when it looks.

use crate::functions::decimal_arb_ops::DecimalArbRescaleFunc;
use crate::functions::decimal_arb_ops::common_precision_scale;
use crate::functions::decimal_arb_predicate_optimizer::DecimalArbExprRewrite;
use crate::types::decimal_arb::DecimalArbType;
use arrow_schema::{DataType, Field};
use datafusion::common::DataFusionError;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema};
use datafusion::error::Result;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{
    Expr, ExprSchemable, Join, LogicalPlan, Projection, ScalarUDF, Union, Unnest, lit,
};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use std::sync::Arc;

#[derive(Debug)]
pub struct DecimalArbScaleUnifyRule {
    rescale: Arc<ScalarUDF>,
    /// Comparison rewrite re-applied once schemas are recomputed; see
    /// [`DecimalArbExprRewrite::rewrite_comparison`].
    comparisons: DecimalArbExprRewrite,
}

impl Default for DecimalArbScaleUnifyRule {
    fn default() -> Self {
        Self::new()
    }
}

/// A `LargeBinary` that carries no extension type at all: a decimal_arb that
/// lost its metadata somewhere upstream. Its bytes sit at an unknown scale,
/// so nothing can rescale them — the only honest outcome is a plan error.
fn bare_large_binary(field: &arrow_schema::Field) -> bool {
    matches!(field.data_type(), arrow_schema::DataType::LargeBinary)
        && !field
            .metadata()
            .contains_key(arrow_schema::extension::EXTENSION_TYPE_NAME_KEY)
}

/// Does `plan` produce column `name` as a NULL literal? Type coercion turns
/// `SELECT NULL` next to a decimal_arb (a UNION branch, a `NOT IN (SELECT
/// NULL …)` subquery) into `CAST(NULL AS LargeBinary)`: a bare LargeBinary
/// with no bytes at any scale, not a column that lost its metadata.
fn projects_null(plan: &LogicalPlan, name: &str) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => projection
            .schema
            .fields()
            .iter()
            .zip(&projection.expr)
            .any(|(field, expr)| field.name() == name && is_null_valued(expr)),
        // SubqueryAlias, Limit, Sort, Filter, Distinct … keep the column.
        other => match other.inputs().as_slice() {
            [input] => projects_null(input, name),
            _ => false,
        },
    }
}

fn is_null_valued(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(value, _) => value.is_null(),
        Expr::Alias(alias) => is_null_valued(&alias.expr),
        Expr::Cast(cast) => is_null_valued(&cast.expr),
        Expr::TryCast(cast) => is_null_valued(&cast.expr),
        _ => false,
    }
}

impl DecimalArbScaleUnifyRule {
    pub fn new() -> Self {
        Self {
            rescale: Arc::new(ScalarUDF::from(DecimalArbRescaleFunc::new())),
            comparisons: DecimalArbExprRewrite::new(),
        }
    }

    /// Route comparisons whose decimal_arb operand only became visible after
    /// the analyzer ran (a derived table's `CASE … AS v`, a CTE) to the
    /// decimal_arb UDFs. The analyzer pass saw the planner's schema for such
    /// inputs — bare `LargeBinary` — and left `v < c` to compare bytes.
    fn late_comparisons(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let schema = merge_schema(&plan.inputs());
        // Output column names are part of the schema an optimizer rule must
        // preserve; `v < c` keeps its name over `decimal_arb_lt(v, c)`.
        let names = NamePreserver::new(&plan);
        let rewritten = plan.map_expressions(|expr| {
            let original = names.save(&expr);
            Ok(expr
                .transform_up(|e| self.comparisons.rewrite_comparison(e, &schema))?
                .update_data(|e| original.restore(e)))
        })?;
        if rewritten.transformed {
            rewritten.map_data(|plan| plan.recompute_schema())
        } else {
            Ok(rewritten)
        }
    }

    fn rescale(&self, expr: Expr, precision: u32, scale: u32) -> Expr {
        Expr::ScalarFunction(ScalarFunction {
            func: self.rescale.clone(),
            args: vec![expr, lit(precision as i64), lit(scale as i64)],
        })
    }

    fn unify_union(&self, union: Union) -> Result<Transformed<LogicalPlan>> {
        let n_cols = union.schema.fields().len();

        // Per output column: the common target if every input is decimal_arb
        // there and they disagree on (precision, scale); otherwise leave it.
        let mut targets: Vec<Option<(u32, u32)>> = Vec::with_capacity(n_cols);
        for i in 0..n_cols {
            let fields: Vec<Option<&arrow_schema::Field>> = union
                .inputs
                .iter()
                .map(|input| input.schema().fields().get(i).map(|f| f.as_ref()))
                .collect();
            let metas: Vec<Option<(u32, u32)>> = fields
                .iter()
                .map(|f| f.and_then(DecimalArbType::precision_scale_from_field))
                .collect();
            // A branch that lost its decimal_arb metadata next to branches
            // that kept it cannot be rescaled, and the union's output field
            // would read its bytes at another branch's scale.
            if metas.iter().any(Option::is_some)
                && let Some(branch) = fields.iter().position(|f| f.is_some_and(bare_large_binary))
                && !fields[branch].is_some_and(|f| projects_null(&union.inputs[branch], f.name()))
            {
                return Err(DataFusionError::Plan(format!(
                    "decimal_arb: UNION column '{}' is decimal_arb in one branch but a LargeBinary \
                     without decimal_arb metadata in branch {} (1-based); its bytes sit at an \
                     unknown scale and cannot be unified — restore the metadata on that branch \
                     (for example with to_decimal_arb_from_string or decimal_arb_restamp)",
                    union.schema.field(i).name(),
                    branch + 1,
                )));
            }
            targets.push(match metas.into_iter().collect::<Option<Vec<_>>>() {
                Some(m) if m.windows(2).any(|w| w[0] != w[1]) => Some(common_precision_scale(&m)?),
                _ => None,
            });
        }
        if targets.iter().all(Option::is_none) {
            return Ok(Transformed::no(LogicalPlan::Union(union)));
        }

        let mut new_inputs = Vec::with_capacity(union.inputs.len());
        for input in union.inputs {
            let schema = input.schema().clone();
            let mut exprs = Vec::with_capacity(n_cols);
            let mut changed = false;
            for (i, field) in schema.fields().iter().enumerate() {
                let (qualifier, _) = schema.qualified_field(i);
                let col = Expr::Column(Column::from((qualifier, field.as_ref())));
                match (
                    targets.get(i).copied().flatten(),
                    DecimalArbType::precision_scale_from_field(field.as_ref()),
                ) {
                    (Some((p, s)), Some(have)) if have != (p, s) => {
                        // Keep the qualifier so references above the union
                        // still resolve.
                        exprs.push(
                            self.rescale(col, p, s)
                                .alias_qualified(qualifier.cloned(), field.name()),
                        );
                        changed = true;
                    }
                    _ => exprs.push(col),
                }
            }
            new_inputs.push(if changed {
                Arc::new(LogicalPlan::Projection(Projection::try_new(exprs, input)?))
            } else {
                input
            });
        }
        Ok(Transformed::yes(LogicalPlan::Union(
            Union::try_new_with_loose_types(new_inputs)?,
        )))
    }

    fn unify_join(&self, join: Join) -> Result<Transformed<LogicalPlan>> {
        let Join {
            left,
            right,
            on,
            filter,
            join_type,
            join_constraint,
            schema,
            null_equality,
            null_aware,
        } = join;

        let meta = |expr: &Expr, side: &LogicalPlan| {
            expr.to_field(side.schema().as_ref())
                .ok()
                .and_then(|(_, f)| DecimalArbType::precision_scale_from_field(f.as_ref()))
        };
        let bare = |expr: &Expr, side: &LogicalPlan| {
            expr.to_field(side.schema().as_ref())
                .ok()
                .is_some_and(|(_, f)| bare_large_binary(f.as_ref()))
                && !matches!(expr, Expr::Column(c) if projects_null(side, &c.name))
        };
        let lost_metadata = |decimal: &Expr, other: &Expr| {
            DataFusionError::Plan(format!(
                "decimal_arb: join key `{other}` is a LargeBinary without decimal_arb metadata \
                 while `{decimal}` is decimal_arb; raw byte equality would never match — \
                 restore the metadata on that side (for example with \
                 to_decimal_arb_from_string or decimal_arb_restamp)"
            ))
        };

        let mut changed = false;
        let mut new_on = Vec::with_capacity(on.len());
        for (l, r) in on {
            match (meta(&l, &left), meta(&r, &right)) {
                (Some(_), None) if bare(&r, &right) => return Err(lost_metadata(&l, &r)),
                (None, Some(_)) if bare(&l, &left) => return Err(lost_metadata(&r, &l)),
                (Some(a), Some(b)) if a != b => {
                    let target = common_precision_scale(&[a, b])?;
                    let l = if a == target {
                        l
                    } else {
                        self.rescale(l, target.0, target.1)
                    };
                    let r = if b == target {
                        r
                    } else {
                        self.rescale(r, target.0, target.1)
                    };
                    new_on.push((l, r));
                    changed = true;
                }
                _ => new_on.push((l, r)),
            }
        }

        let rebuilt = LogicalPlan::Join(Join {
            left,
            right,
            on: new_on,
            filter,
            join_type,
            join_constraint,
            schema,
            null_equality,
            null_aware,
        });
        Ok(if changed {
            Transformed::yes(rebuilt)
        } else {
            Transformed::no(rebuilt)
        })
    }
}

impl DecimalArbScaleUnifyRule {
    /// Restore decimal_arb metadata on `UNNEST` output columns.
    ///
    /// DataFusion derives the unnested field from the element / child *data
    /// type* alone, so a `List<decimal_arb>` unnested to rows, or a struct
    /// with a decimal_arb member flattened to columns, came out as bare
    /// `LargeBinary`: the JSON writer printed hex, and a comparison on the
    /// unnested value compared bytes. The element field itself still carries
    /// the metadata, so copy it onto the output schema (the physical operator
    /// takes its schema from here).
    fn restamp_unnest(&self, unnest: Unnest) -> Result<Transformed<LogicalPlan>> {
        let input_schema = unnest.input.schema();
        let mut changed = false;
        let mut fields = Vec::with_capacity(unnest.schema.fields().len());
        for (i, (qualifier, field)) in unnest.schema.iter().enumerate() {
            let Some(&dep) = unnest.dependency_indices.get(i) else {
                fields.push((qualifier.cloned(), Arc::clone(field)));
                continue;
            };
            let input_field = input_schema.field(dep);
            let source: Option<&Arc<Field>> = if unnest.struct_type_columns.contains(&dep) {
                match input_field.data_type() {
                    DataType::Struct(children) => children
                        .iter()
                        .find(|c| format!("{}.{}", input_field.name(), c.name()) == *field.name()),
                    _ => None,
                }
            } else {
                unnest
                    .list_type_columns
                    .iter()
                    .find(|(idx, col)| *idx == dep && col.output_column.name == *field.name())
                    .and_then(|(_, col)| {
                        let mut element: Option<&Arc<Field>> = None;
                        let mut current = input_field.data_type();
                        for _ in 0..col.depth {
                            match current {
                                DataType::List(e)
                                | DataType::LargeList(e)
                                | DataType::FixedSizeList(e, _)
                                | DataType::ListView(e)
                                | DataType::LargeListView(e) => {
                                    element = Some(e);
                                    current = e.data_type();
                                }
                                _ => return None,
                            }
                        }
                        element
                    })
            };
            match source {
                Some(source)
                    if source.data_type() == field.data_type()
                        && source.metadata() != field.metadata()
                        && !source.metadata().is_empty() =>
                {
                    let mut metadata = field.metadata().clone();
                    metadata.extend(source.metadata().clone());
                    fields.push((
                        qualifier.cloned(),
                        Arc::new(field.as_ref().clone().with_metadata(metadata)),
                    ));
                    changed = true;
                }
                _ => fields.push((qualifier.cloned(), Arc::clone(field))),
            }
        }
        if !changed {
            return Ok(Transformed::no(LogicalPlan::Unnest(unnest)));
        }
        let schema = DFSchema::new_with_metadata(fields, unnest.schema.metadata().clone())?
            .with_functional_dependencies(unnest.schema.functional_dependencies().clone())?;
        Ok(Transformed::yes(LogicalPlan::Unnest(Unnest {
            schema: Arc::new(schema),
            ..unnest
        })))
    }
}

impl OptimizerRule for DecimalArbScaleUnifyRule {
    fn name(&self) -> &str {
        "decimal_arb_scale_unify"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        // Inner unions/joins first, so an outer one sees unified inputs.
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let plan = match plan {
            LogicalPlan::Union(union) => self.unify_union(union)?,
            LogicalPlan::Join(join) => self.unify_join(join)?,
            LogicalPlan::Unnest(unnest) => self.restamp_unnest(unnest)?,
            other => Transformed::no(other),
        };
        plan.transform_data(|plan| self.late_comparisons(plan))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    /// `a(v decimal_arb(20, 2))` and `b(v LargeBinary)` — the same column
    /// name, one branch having lost its decimal_arb metadata upstream.
    fn session() -> SessionContext {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_optimizer_rule(Arc::new(DecimalArbScaleUnifyRule::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);
        let decimal = Arc::new(Schema::new(vec![
            DecimalArbType::field("v", 20, 2, true).unwrap(),
        ]));
        let bare = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::LargeBinary,
            true,
        )]));
        ctx.register_table(
            "a",
            Arc::new(MemTable::try_new(decimal, vec![vec![]]).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "b",
            Arc::new(MemTable::try_new(bare, vec![vec![]]).unwrap()),
        )
        .unwrap();
        ctx
    }

    async fn plan_error(ctx: &SessionContext, sql: &str) -> String {
        let df = ctx.sql(sql).await.unwrap();
        match df.into_optimized_plan() {
            Ok(plan) => panic!("{sql}: expected a plan error, got {plan}"),
            Err(e) => e.to_string(),
        }
    }

    #[tokio::test]
    async fn union_branch_that_lost_its_metadata_is_a_plan_error() {
        // Left alone, the union's output field would take branch 1's scale
        // and read branch 2's bytes at it — silently off by a power of ten.
        let ctx = session();
        let err = plan_error(&ctx, "SELECT v FROM a UNION ALL SELECT v FROM b").await;
        assert!(
            err.contains("without decimal_arb metadata in branch 2"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn a_null_branch_or_key_is_not_a_lost_metadata_error() {
        // `SELECT NULL` is coerced to `CAST(NULL AS LargeBinary)` next to a
        // decimal_arb column: no bytes at any scale, nothing to unify.
        let ctx = session();
        for sql in [
            "SELECT v FROM a UNION ALL SELECT NULL FROM b",
            "SELECT v FROM a WHERE v NOT IN (SELECT NULL FROM b)",
        ] {
            let df = ctx.sql(sql).await.unwrap();
            df.into_optimized_plan()
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
    }

    #[tokio::test]
    async fn join_key_that_lost_its_metadata_is_a_plan_error() {
        let ctx = session();
        let err = plan_error(&ctx, "SELECT a.v FROM a JOIN b ON a.v = b.v").await;
        assert!(err.contains("join key"), "{err}");
    }
}
