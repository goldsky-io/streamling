//! File source backed by DataFusion's file readers.
//!
//! One provider ([`FileSourceTableProvider`]) serves two modes that share schema
//! inference, object-store registration, scan pushdown, the per-partition read
//! loop, checkpoint participation, and the `_gs_op = 'i'` change-operation
//! contract:
//!
//! - **Bounded** lists the matching files once at scan time, drops the ones a
//!   persisted [`BoundedProgress`] already covers, and reads the rest across
//!   `parallelism` output partitions that share one queue of unopened files, as
//!   DataFusion's `DataSourceExec` does; the job terminates on its own.
//!   Checkpoints cover the files each partition fully emitted, and the last
//!   partition to finish closes the source with a terminal checkpoint, so a
//!   restarted job resumes instead of re-reading everything.
//! - **Continuous** keeps watching the path: every `poll_interval` one partition
//!   lists the prefix (or HEADs the path when it names a single object) and
//!   queues the files a persisted [`FileWatermark`] doesn't cover yet, for the
//!   `parallelism` output partitions to share; the source never self-terminates.
//!   The watermark advances over the files committed in `(last_modified, path)`
//!   order.
//!
//! Because file reads are append-only, a constant `_gs_op = 'i'` column is
//! synthesized when the inferred schema lacks it. Files that already carry
//! `_gs_op` pass through unwrapped, but the column must be a non-nullable Utf8
//! column to match what downstream consumers expect.

mod bounded;
mod checkpoint;
mod continuous;
mod discovery;
mod progress;
mod provider;
mod reader;
#[cfg(test)]
mod test_support;

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatchOptions;
use datafusion::common::DataFusionError;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr_rewriter::unnormalize_col;

use streamling_core::data::RowKind;

pub use bounded::{BoundedProgress, PathRange};
pub use continuous::FileWatermark;
pub use progress::FileKey;
pub use provider::{FileSourceReadMode, FileSourceTableProvider};

/// A scan projection over the published schema, split into the columns the
/// reader reads and how each output column is filled.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ScanProjection {
    /// File/partition column indices pushed into the reader; `None` reads all.
    read: Option<Vec<usize>>,
    columns: Vec<OutputColumn>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutputColumn {
    /// A column of the batch the reader returns, by position.
    Read(usize),
    /// The synthesized constant `_gs_op`.
    Op,
}

impl ScanProjection {
    /// `table_columns` counts the reader's file and partition columns. Any index
    /// past them is the synthesized `_gs_op`, which the published schema places
    /// last.
    fn new(projection: Option<&Vec<usize>>, table_columns: usize, append_op: bool) -> Self {
        let Some(requested) = projection else {
            let mut columns: Vec<OutputColumn> =
                (0..table_columns).map(OutputColumn::Read).collect();
            if append_op {
                columns.push(OutputColumn::Op);
            }
            return Self {
                read: None,
                columns,
            };
        };
        let mut read: Vec<usize> = requested
            .iter()
            .copied()
            .filter(|index| *index < table_columns)
            .collect();
        read.sort_unstable();
        read.dedup();
        let columns = requested
            .iter()
            .map(|index| match read.binary_search(index) {
                Ok(position) => OutputColumn::Read(position),
                Err(_) => OutputColumn::Op,
            })
            .collect();
        Self {
            read: Some(read),
            columns,
        }
    }
}

/// Builds the source's output from a reader batch: each output column comes from
/// the batch or is the constant `_gs_op = 'i'`, in the requested order.
fn build_output_batch(
    batch: RecordBatch,
    columns: &[OutputColumn],
    output_schema: &SchemaRef,
) -> DataFusionResult<RecordBatch> {
    let num_rows = batch.num_rows();
    let op_value = RowKind::Insert.to_str();
    let columns: Vec<ArrayRef> = columns
        .iter()
        .map(|column| match column {
            OutputColumn::Read(position) => batch.column(*position).clone(),
            OutputColumn::Op => Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                op_value.as_str(),
                num_rows,
            ))) as ArrayRef,
        })
        .collect();
    // The row count is explicit so a projection with no columns keeps it.
    Ok(RecordBatch::try_new_with_options(
        output_schema.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )?)
}

/// The filters whose columns all exist in the reader's file or partition schema,
/// unqualified so they resolve against it. A filter on a synthesized `_gs_op`
/// has nothing to prune and is never pushed.
fn pushable_filters(filters: &[Expr], table_schema: &Schema) -> Vec<Expr> {
    filters
        .iter()
        .filter(|filter| {
            filter
                .column_refs()
                .iter()
                .all(|column| table_schema.field_with_name(&column.name).is_ok())
        })
        .map(|filter| unnormalize_col(filter.clone()))
        .collect()
}

fn to_df_err<E>(e: E) -> DataFusionError
where
    E: std::error::Error + Send + Sync + 'static,
{
    DataFusionError::External(Box::new(e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_providers::file::test_support::*;
    use datafusion::arrow::array::Array;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::datasource::TableProvider;
    use datafusion::logical_expr::{col, lit};
    use streamling_core::data::COLUMN_NAME_OP;

    /// The synthesized `_gs_op` sits last in the published schema, so any
    /// projection that asks for it — in any order — must get it back in place,
    /// with the file columns pushed into the reader.
    #[tokio::test]
    async fn bounded_scan_inserts_gs_op_into_out_of_order_projections() {
        let dir = temp_dir_with(
            "projection",
            &[(
                "people.csv".to_string(),
                "id,name,city\n1,alice,paris\n".to_string(),
            )],
        );
        let (session_manager, provider) = bounded_provider(
            &dir,
            "projection_src",
            Some(1),
            bounded_backend("projection"),
            None,
        )
        .await;
        let state = session_manager.session_state();

        // Published schema: id, name, city, then the synthesized _gs_op.
        let plan = provider
            .scan(&state, Some(&vec![3, 2, 0]), &[], None)
            .await
            .unwrap();
        let batches = collect_partitions(&plan, &session_manager).await;
        let batch = batches.iter().find(|batch| batch.num_rows() > 0).unwrap();
        let names: Vec<&str> = batch
            .schema_ref()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        assert_eq!(names, vec![COLUMN_NAME_OP, "city", "id"]);
        let op = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let city = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!((op.value(0), city.value(0)), ("i", "paris"));
        assert_eq!(int64_values(&batches, "id"), vec![1]);

        // Only `_gs_op`: no file column is read, but the row count survives.
        let plan = provider
            .scan(&state, Some(&vec![3]), &[], None)
            .await
            .unwrap();
        let batches = collect_partitions(&plan, &session_manager).await;
        let _ = std::fs::remove_dir_all(&dir);
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 1);
        assert!(batches.iter().all(|batch| batch.num_columns() == 1));
    }

    #[test]
    fn pushable_filters_skip_columns_the_reader_lacks() {
        let table_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("dt", DataType::Utf8, false),
        ]);
        let filters = vec![
            col("file_src.id").gt(lit(1)),
            col(COLUMN_NAME_OP).eq(lit("i")),
            col("dt").eq(lit("2024-01-01")),
        ];
        assert_eq!(
            pushable_filters(&filters, &table_schema),
            vec![col("id").gt(lit(1)), col("dt").eq(lit("2024-01-01"))],
            "file and partition predicates are pushed unqualified; a synthesized _gs_op one is not"
        );
    }
}
