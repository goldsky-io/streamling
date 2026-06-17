use std::cmp::{self, Ordering};
use std::sync::Arc;
use std::task::{Poll, ready};

use crate::checkpoints::checkpoint_management::{CheckpointMessage, extract_checkpoint_messages};
use arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, Int64Array, LargeListArray, ListArray,
    PrimitiveArray, Scalar, StructArray, new_null_array,
};
use arrow::compute::kernels::cmp::lt;
use arrow::compute::kernels::length::length;
use arrow::compute::kernels::zip::zip;
use arrow::compute::{cast, is_not_null, sum};
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::{
    HashMap, HashSet, Result, UnnestOptions, exec_datafusion_err, exec_err, internal_err,
};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::metrics::{
    self, BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, RecordOutput,
};
use datafusion::physical_plan::unnest::UnnestExec as DataFusionUnnestExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};
use tracing::trace;

/// Wrapper around DataFusion's UnnestExec with streaming support
///
/// Unnest the given columns (either with type struct or list)
/// For list unnesting, each rows is vertically transformed into multiple rows
/// For struct unnesting, each columns is horizontally transformed into multiple columns,
/// Thus the original RecordBatch with dimension (n x m) may have new dimension (n' x m')
///
/// See [`UnnestOptions`] for more details and an example.
#[derive(Debug, Clone)]
pub struct StreamingUnnestExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// The schema once the unnest is applied
    schema: SchemaRef,
    /// Indices of the list-typed columns in the input schema
    list_column_indices: Vec<ListUnnest>,
    /// Indices of the struct-typed columns in the input schema
    struct_column_indices: Vec<usize>,
    /// Options
    options: UnnestOptions,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: Arc<PlanProperties>,
    /// Copy of the original DataFusion UnnestExec for method delegation
    original_unnest: DataFusionUnnestExec,
}

impl StreamingUnnestExec {
    /// Create a new [StreamingUnnestExec].
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        list_column_indices: Vec<ListUnnest>,
        struct_column_indices: Vec<usize>,
        schema: SchemaRef,
        options: UnnestOptions,
        original_unnest: DataFusionUnnestExec,
    ) -> Self {
        let cache = Self::compute_properties(&input, Arc::clone(&schema));

        StreamingUnnestExec {
            input,
            schema,
            list_column_indices,
            struct_column_indices,
            options,
            metrics: Default::default(),
            cache: Arc::new(cache),
            original_unnest,
        }
    }

    /// Create from DataFusion's UnnestExec
    pub fn from_original(original_unnest: DataFusionUnnestExec) -> Result<Self> {
        // Extract information from the original unnest
        let input = original_unnest.input().clone();
        let schema = original_unnest.schema();
        let list_column_indices = original_unnest
            .list_column_indices()
            .iter()
            .map(|idx| ListUnnest {
                index_in_input_schema: idx.index_in_input_schema,
                depth: idx.depth,
            })
            .collect();
        let struct_column_indices = original_unnest.struct_column_indices().to_vec();
        let options = original_unnest.options().clone();

        let cache = Self::compute_properties(&input, Arc::clone(&schema));

        Ok(StreamingUnnestExec {
            input,
            schema,
            list_column_indices,
            struct_column_indices,
            options,
            metrics: Default::default(),
            cache: Arc::new(cache),
            original_unnest,
        })
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(input: &Arc<dyn ExecutionPlan>, schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            input.output_partitioning().to_owned(),
            input.pipeline_behavior(),
            input.boundedness(),
        )
    }

    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Indices of the list-typed columns in the input schema
    pub fn list_column_indices(&self) -> &[ListUnnest] {
        &self.list_column_indices
    }

    /// Indices of the struct-typed columns in the input schema
    pub fn struct_column_indices(&self) -> &[usize] {
        &self.struct_column_indices
    }

    pub fn options(&self) -> &UnnestOptions {
        &self.options
    }
}

impl DisplayAs for StreamingUnnestExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "UnnestExec")
            }
            DisplayFormatType::TreeRender => {
                write!(f, "")
            }
        }
    }
}

impl ExecutionPlan for StreamingUnnestExec {
    fn name(&self) -> &'static str {
        "UnnestExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StreamingUnnestExec::new(
            Arc::clone(&children[0]),
            self.list_column_indices.clone(),
            self.struct_column_indices.clone(),
            Arc::clone(&self.schema),
            self.options.clone(),
            self.original_unnest.clone(),
        )))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let metrics = UnnestMetrics::new(partition, &self.metrics);

        Ok(Box::pin(UnnestStream {
            input,
            schema: Arc::clone(&self.schema),
            list_type_columns: self.list_column_indices.clone(),
            struct_column_indices: self.struct_column_indices.iter().copied().collect(),
            options: self.options.clone(),
            metrics,
            resolved_schema: None,
            pending_checkpoints: Vec::new(),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[derive(Clone, Debug)]
struct UnnestMetrics {
    /// Execution metrics
    baseline_metrics: BaselineMetrics,
    /// Number of batches consumed
    input_batches: metrics::Count,
    /// Number of rows consumed
    input_rows: metrics::Count,
    /// Number of batches produced
    output_batches: metrics::Count,
}

impl UnnestMetrics {
    fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let input_batches = MetricBuilder::new(metrics).counter("input_batches", partition);

        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);

        let output_batches = MetricBuilder::new(metrics).counter("output_batches", partition);

        Self {
            baseline_metrics: BaselineMetrics::new(metrics, partition),
            input_batches,
            input_rows,
            output_batches,
        }
    }
}

/// A stream that issues [RecordBatch]es with unnested column data.
struct UnnestStream {
    /// Input stream
    input: SendableRecordBatchStream,
    /// Unnested schema (static, from the execution plan)
    schema: Arc<Schema>,
    /// represents all unnest operations to be applied to the input (input index, depth)
    /// e.g unnest(col1),unnest(unnest(col1)) where col1 has index 1 in original input schema
    /// then list_type_columns = [ListUnnest{1,1},ListUnnest{1,2}]
    list_type_columns: Vec<ListUnnest>,
    struct_column_indices: HashSet<usize>,
    /// Options
    options: UnnestOptions,
    /// Metrics
    metrics: UnnestMetrics,
    /// Resolved schema from actual batch processing. This may differ from `schema` if
    /// type promotion occurred (e.g., Utf8 -> LargeUtf8 via safe_take fallback).
    /// Used to ensure empty batches use consistent types with non-empty batches.
    resolved_schema: Option<SchemaRef>,
    /// Pending checkpoint messages from early empty batches.
    /// These are queued until we have a resolved schema to ensure schema consistency.
    /// They will be emitted with the first non-empty batch or when the stream ends.
    pending_checkpoints: Vec<CheckpointMessage>,
}

impl RecordBatchStream for UnnestStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[async_trait]
impl Stream for UnnestStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl UnnestStream {
    /// Helper to create an empty batch with checkpoint metadata
    fn create_empty_checkpoint_batch(
        &self,
        checkpoint_messages: &[CheckpointMessage],
    ) -> Result<RecordBatch> {
        use std::collections::HashMap as StdHashMap;
        let mut metadata = StdHashMap::new();
        crate::checkpoints::checkpoint_management::enrich_batch_metadata_with_checkpoints(
            &mut metadata,
            checkpoint_messages,
        );
        // Use resolved_schema if available to ensure consistent types
        let base_fields = self
            .resolved_schema
            .as_ref()
            .map(|s| s.fields().clone())
            .unwrap_or_else(|| self.schema.fields().clone());
        let enriched_schema = Arc::new(Schema::new_with_metadata(base_fields, metadata));
        let empty_arrays: Vec<ArrayRef> = enriched_schema
            .fields()
            .iter()
            .map(|field| new_null_array(field.data_type(), 0))
            .collect();
        RecordBatch::try_new(enriched_schema, empty_arrays).map_err(Into::into)
    }

    /// Separate implementation function that unpins the [`UnnestStream`] so
    /// that partial borrows work correctly
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            return Poll::Ready(match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    let elapsed_compute = self.metrics.baseline_metrics.elapsed_compute().clone();
                    let timer = elapsed_compute.timer();
                    self.metrics.input_batches.add(1);
                    self.metrics.input_rows.add(batch.num_rows());

                    // Extract checkpoint messages from input batch before processing
                    let checkpoint_messages =
                        extract_checkpoint_messages(batch.schema().metadata());

                    let result = build_batch(
                        &batch,
                        &self.schema,
                        &self.list_type_columns,
                        &self.struct_column_indices,
                        &self.options,
                    )?;
                    timer.done();

                    let result_batch = match result {
                        Some(mut batch) => {
                            // Track the resolved schema from actual batch processing.
                            // This may have different types than self.schema if type promotion
                            // occurred (e.g., Utf8 -> LargeUtf8 via safe_take fallback).
                            self.resolved_schema = Some(batch.schema());

                            // Merge any pending checkpoints from early empty batches
                            // into this non-empty batch to ensure they're propagated
                            if !self.pending_checkpoints.is_empty() {
                                use std::collections::HashMap as StdHashMap;
                                let mut all_checkpoints =
                                    std::mem::take(&mut self.pending_checkpoints);
                                all_checkpoints.extend(checkpoint_messages.clone());
                                let mut metadata = StdHashMap::new();
                                crate::checkpoints::checkpoint_management::enrich_batch_metadata_with_checkpoints(
                                    &mut metadata,
                                    &all_checkpoints,
                                );
                                // Rebuild batch with merged checkpoint metadata
                                let enriched_schema = Arc::new(Schema::new_with_metadata(
                                    batch.schema().fields().clone(),
                                    metadata,
                                ));
                                batch = RecordBatch::try_new(
                                    enriched_schema,
                                    batch.columns().to_vec(),
                                )?;
                            }
                            batch
                        }
                        None => {
                            // Empty result - queue checkpoints if schema not yet resolved
                            if !checkpoint_messages.is_empty() {
                                if self.resolved_schema.is_none() {
                                    // Schema not yet resolved - queue checkpoints to avoid
                                    // emitting empty batch with potentially wrong schema types.
                                    // These will be emitted with the first non-empty batch or at stream end.
                                    self.pending_checkpoints.extend(checkpoint_messages);
                                    continue;
                                } else {
                                    // Schema resolved - safe to emit empty batch with correct types
                                    self.create_empty_checkpoint_batch(&checkpoint_messages)?
                                }
                            } else {
                                // No checkpoint metadata, safe to skip
                                continue;
                            }
                        }
                    };
                    self.metrics.output_batches.add(1);
                    (&result_batch).record_output(&self.metrics.baseline_metrics);

                    // If result batch is empty but has checkpoint metadata, emit it
                    // Otherwise, only emit non-empty batches
                    if result_batch.num_rows() == 0
                        && extract_checkpoint_messages(result_batch.schema().metadata()).is_empty()
                    {
                        continue;
                    }
                    Some(Ok(result_batch))
                }
                None => {
                    trace!(
                        "Processed {} probe-side input batches containing {} rows and \
                        produced {} output batches containing {} rows in {}",
                        self.metrics.input_batches,
                        self.metrics.input_rows,
                        self.metrics.output_batches,
                        self.metrics.baseline_metrics.output_rows(),
                        self.metrics.baseline_metrics.elapsed_compute(),
                    );
                    // Stream ended - emit any pending checkpoints as an empty batch
                    if !self.pending_checkpoints.is_empty() {
                        let checkpoints = std::mem::take(&mut self.pending_checkpoints);
                        // Use resolved_schema if available, otherwise fall back to self.schema
                        // (if no non-empty batch was ever processed, no type promotion occurred)
                        Some(self.create_empty_checkpoint_batch(&checkpoints))
                    } else {
                        None
                    }
                }
                Some(Err(e)) => Some(Err(e)),
            });
        }
    }
}

/// Given a set of struct column indices to flatten
/// try converting the column in input into multiple subfield columns
/// For example
/// struct_col: [a: struct(item: int, name: string), b: int]
/// with a batch
/// {a: {item: 1, name: "a"}, b: 2},
/// {a: {item: 3, name: "b"}, b: 4]
/// will be converted into
/// {a.item: 1, a.name: "a", b: 2},
/// {a.item: 3, a.name: "b", b: 4}
fn flatten_struct_cols(
    input_batch: &[Arc<dyn Array>],
    schema: &SchemaRef,
    metadata: &std::collections::HashMap<String, String>,
    struct_column_indices: &HashSet<usize>,
) -> Result<RecordBatch> {
    // horizontal expansion because of struct unnest
    let columns_expanded: Vec<ArrayRef> = input_batch
        .iter()
        .enumerate()
        .map(|(idx, column_data)| match struct_column_indices.get(&idx) {
            Some(_) => match column_data.data_type() {
                DataType::Struct(_) => {
                    let struct_arr = column_data.as_any().downcast_ref::<StructArray>().unwrap();
                    Ok(struct_arr.columns().to_vec())
                }
                data_type => internal_err!(
                    "expecting column {} from input plan to be a struct, got {:?}",
                    idx,
                    data_type
                ),
            },
            None => Ok(vec![Arc::clone(column_data)]),
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();

    // Build schema from actual array types to handle type upgrades (e.g., Utf8 -> LargeUtf8)
    // This ensures schema matches the actual data types returned by safe_take
    let mut new_fields = Vec::new();
    let mut expanded_idx = 0;
    for (idx, column_data) in input_batch.iter().enumerate() {
        match struct_column_indices.get(&idx) {
            Some(_) => {
                // Struct columns are expanded into multiple fields
                match column_data.data_type() {
                    DataType::Struct(fields) => {
                        for field in fields {
                            // Use the actual array type, not the schema field type
                            // This handles cases where safe_take upgraded Utf8 -> LargeUtf8
                            let actual_array = &columns_expanded[expanded_idx];
                            new_fields.push(Field::new(
                                field.name(),
                                actual_array.data_type().clone(),
                                field.is_nullable(),
                            ));
                            expanded_idx += 1;
                        }
                    }
                    _ => unreachable!("struct_column_indices should only contain struct types"),
                }
            }
            None => {
                // Non-struct columns: use actual array type
                let actual_array = &columns_expanded[expanded_idx];
                let original_field = schema.field(idx);
                new_fields.push(Field::new(
                    original_field.name(),
                    actual_array.data_type().clone(),
                    original_field.is_nullable(),
                ));
                expanded_idx += 1;
            }
        }
    }

    // Create schema with actual array types and metadata
    // Metadata (including checkpoint messages) is preserved from the input batch
    // This ensures checkpoint metadata propagates through the unnest operation
    let enriched_schema = Arc::new(Schema::new_with_metadata(new_fields, metadata.clone()));

    Ok(RecordBatch::try_new(enriched_schema, columns_expanded)?)
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct ListUnnest {
    pub index_in_input_schema: usize,
    pub depth: usize,
}

/// This function is used to execute the unnesting on multiple columns all at once, but
/// one level at a time, and is called n times, where n is the highest recursion level among
/// the unnest exprs in the query.
///
/// For example giving the following query:
/// ```sql
/// select unnest(colA, max_depth:=3) as P1, unnest(colA,max_depth:=2) as P2, unnest(colB, max_depth:=1) as P3 from temp;
/// ```
/// Then the total times this function being called is 3
///
/// It needs to be aware of which level the current unnesting is, because if there exists
/// multiple unnesting on the same column, but with different recursion levels, say
/// **unnest(colA, max_depth:=3)** and **unnest(colA, max_depth:=2)**, then the unnesting
/// of expr **unnest(colA, max_depth:=3)** will start at level 3, while unnesting for expr
/// **unnest(colA, max_depth:=2)** has to start at level 2
///
/// Set *colA* as a 3-dimension columns and *colB* as an array (1-dimension). As stated,
/// this function is called with the descending order of recursion depth
///
/// Depth = 3
/// - colA(3-dimension) unnest into temp column temp_P1(2_dimension) (unnesting of P1 starts
///   from this level)
/// - colA(3-dimension) having indices repeated by the unnesting operation above
/// - colB(1-dimension) having indices repeated by the unnesting operation above
///
/// Depth = 2
/// - temp_P1(2-dimension) unnest into temp column temp_P1(1-dimension)
/// - colA(3-dimension) unnest into temp column temp_P2(2-dimension) (unnesting of P2 starts
///   from this level)
/// - colB(1-dimension) having indices repeated by the unnesting operation above
///
/// Depth = 1
/// - temp_P1(1-dimension) unnest into P1
/// - temp_P2(2-dimension) unnest into P2
/// - colB(1-dimension) unnest into P3 (unnesting of P3 starts from this level)
///
/// The returned array will has the same size as the input batch
/// and only contains original columns that are not being unnested.
fn list_unnest_at_level(
    batch: &[ArrayRef],
    list_type_unnests: &[ListUnnest],
    temp_unnested_arrs: &mut HashMap<ListUnnest, ArrayRef>,
    level_to_unnest: usize,
    options: &UnnestOptions,
) -> Result<Option<Vec<ArrayRef>>> {
    // Extract unnestable columns at this level
    let (arrs_to_unnest, list_unnest_specs): (Vec<Arc<dyn Array>>, Vec<_>) = list_type_unnests
        .iter()
        .filter_map(|unnesting| {
            if level_to_unnest == unnesting.depth {
                return Some((
                    Arc::clone(&batch[unnesting.index_in_input_schema]),
                    *unnesting,
                ));
            }
            // This means the unnesting on this item has started at higher level
            // and need to continue until depth reaches 1
            if level_to_unnest < unnesting.depth {
                return Some((
                    Arc::clone(temp_unnested_arrs.get(unnesting).unwrap()),
                    *unnesting,
                ));
            }
            None
        })
        .unzip();

    // Filter out so that list_arrays only contain column with the highest depth
    // at the same time, during iteration remove this depth so next time we don't have to unnest them again
    let longest_length = find_longest_length(&arrs_to_unnest, options)?;
    let unnested_length = longest_length.as_primitive::<Int64Type>();
    let total_length = if unnested_length.is_empty() {
        0
    } else {
        let sum_result = sum(unnested_length)
            .ok_or_else(|| exec_datafusion_err!("Failed to calculate the total unnested length"))?;
        // Check for overflow: ensure the sum can fit in usize
        // Also check that it doesn't exceed a practical limit for arrow-select operations
        // Arrow-select may use u32 internally for some operations, so we limit to u32::MAX
        if sum_result < 0 {
            return Err(exec_datafusion_err!(
                "Total unnested length cannot be negative: {}",
                sum_result
            ));
        }
        // Check if it exceeds u32::MAX (arrow-select may use u32 internally for some operations)
        // This prevents overflow in arrow-select's take operation
        if sum_result > u32::MAX as i64 {
            return Err(exec_datafusion_err!(
                "Total unnested length {} exceeds maximum supported size (u32::MAX = {})",
                sum_result,
                u32::MAX
            ));
        }
        sum_result as usize
    };
    if total_length == 0 {
        return Ok(None);
    }

    // Unnest all the list arrays
    let unnested_temp_arrays =
        unnest_list_arrays(arrs_to_unnest.as_ref(), unnested_length, total_length)?;

    // Create the take indices array for other columns
    let take_indices = create_take_indices(unnested_length, total_length);
    unnested_temp_arrays
        .into_iter()
        .zip(list_unnest_specs.iter())
        .for_each(|(flatten_arr, unnesting)| {
            temp_unnested_arrs.insert(*unnesting, flatten_arr);
        });

    let repeat_mask: Vec<bool> = batch
        .iter()
        .enumerate()
        .map(|(i, _)| {
            // Check if the column is needed in future levels (levels below the current one)
            let needed_in_future_levels = list_type_unnests.iter().any(|unnesting| {
                unnesting.index_in_input_schema == i && unnesting.depth < level_to_unnest
            });

            // Check if the column is involved in unnesting at any level
            let is_involved_in_unnesting = list_type_unnests
                .iter()
                .any(|unnesting| unnesting.index_in_input_schema == i);

            // Repeat columns needed in future levels or not unnested.
            needed_in_future_levels || !is_involved_in_unnesting
        })
        .collect();

    // Dimension of arrays in batch is untouched, but the values are repeated
    // as the side effect of unnesting
    let ret = repeat_arrs_from_indices(batch, &take_indices, &repeat_mask)?;

    Ok(Some(ret))
}
struct UnnestingResult {
    arr: ArrayRef,
    depth: usize,
}

/// For each row in a `RecordBatch`, some list/struct columns need to be unnested.
/// - For list columns: We will expand the values in each list into multiple rows,
///   taking the longest length among these lists, and shorter lists are padded with NULLs.
/// - For struct columns: We will expand the struct columns into multiple subfield columns.
///
/// For columns that don't need to be unnested, repeat their values until reaching the longest length.
///
/// Note: unnest has a big difference in behavior between Postgres and DuckDB
///
/// Take this example
///
/// 1. Postgres
/// ```ignored
/// create table temp (
///     i integer[][][], j integer[]
/// )
/// insert into temp values ('{{{1,2},{3,4}},{{5,6},{7,8}}}', '{1,2}');
/// select unnest(i), unnest(j) from temp;
/// ```
///
/// Result
/// ```text
///     1   1
///     2   2
///     3
///     4
///     5
///     6
///     7
///     8
/// ```
/// 2. DuckDB
/// ```ignore
///     create table temp (i integer[][][], j integer[]);
///     insert into temp values ([[[1,2],[3,4]],[[5,6],[7,8]]], [1,2]);
///     select unnest(i,recursive:=true), unnest(j,recursive:=true) from temp;
/// ```
/// Result:
/// ```text
///
///     ┌────────────────────────────────────────────────┬────────────────────────────────────────────────┐
///     │ unnest(i, "recursive" := CAST('t' AS BOOLEAN)) │ unnest(j, "recursive" := CAST('t' AS BOOLEAN)) │
///     │                     int32                      │                     int32                      │
///     ├────────────────────────────────────────────────┼────────────────────────────────────────────────┤
///     │                                              1 │                                              1 │
///     │                                              2 │                                              2 │
///     │                                              3 │                                              1 │
///     │                                              4 │                                              2 │
///     │                                              5 │                                              1 │
///     │                                              6 │                                              2 │
///     │                                              7 │                                              1 │
///     │                                              8 │                                              2 │
///     └────────────────────────────────────────────────┴────────────────────────────────────────────────┘
/// ```
///
/// The following implementation refer to DuckDB's implementation
fn build_batch(
    batch: &RecordBatch,
    schema: &SchemaRef,
    list_type_columns: &[ListUnnest],
    struct_column_indices: &HashSet<usize>,
    options: &UnnestOptions,
) -> Result<Option<RecordBatch>> {
    let transformed = match list_type_columns.len() {
        0 => flatten_struct_cols(
            batch.columns(),
            schema,
            batch.schema().metadata(),
            struct_column_indices,
        ),
        _ => {
            let mut temp_unnested_result = HashMap::new();
            let max_recursion = list_type_columns
                .iter()
                .fold(0, |highest_depth, ListUnnest { depth, .. }| {
                    cmp::max(highest_depth, *depth)
                });

            // This arr always has the same column count with the input batch
            let mut flatten_arrs = vec![];

            // Original batch has the same columns
            // All unnesting results are written to temp_batch
            for depth in (1..=max_recursion).rev() {
                let input = match depth == max_recursion {
                    true => batch.columns(),
                    false => &flatten_arrs,
                };
                let Some(temp_result) = list_unnest_at_level(
                    input,
                    list_type_columns,
                    &mut temp_unnested_result,
                    depth,
                    options,
                )?
                else {
                    return Ok(None);
                };
                flatten_arrs = temp_result;
            }
            let unnested_array_map: HashMap<usize, Vec<UnnestingResult>> =
                temp_unnested_result.into_iter().fold(
                    HashMap::new(),
                    |mut acc,
                     (
                        ListUnnest {
                            index_in_input_schema,
                            depth,
                        },
                        flattened_array,
                    )| {
                        acc.entry(index_in_input_schema)
                            .or_default()
                            .push(UnnestingResult {
                                arr: flattened_array,
                                depth,
                            });
                        acc
                    },
                );
            let output_order: HashMap<ListUnnest, usize> = list_type_columns
                .iter()
                .enumerate()
                .map(|(order, unnest_def)| (*unnest_def, order))
                .collect();

            // One original column may be unnested multiple times into separate columns
            let mut multi_unnested_per_original_index = unnested_array_map
                .into_iter()
                .map(
                    // Each item in unnested_columns is the result of unnesting the same input column
                    // we need to sort them to conform with the original expression order
                    // e.g unnest(unnest(col)) must goes before unnest(col)
                    |(original_index, mut unnested_columns)| {
                        unnested_columns.sort_by(
                            |UnnestingResult { depth: depth1, .. },
                             UnnestingResult { depth: depth2, .. }|
                             -> Ordering {
                                output_order
                                    .get(&ListUnnest {
                                        depth: *depth1,
                                        index_in_input_schema: original_index,
                                    })
                                    .unwrap()
                                    .cmp(
                                        output_order
                                            .get(&ListUnnest {
                                                depth: *depth2,
                                                index_in_input_schema: original_index,
                                            })
                                            .unwrap(),
                                    )
                            },
                        );
                        (
                            original_index,
                            unnested_columns
                                .into_iter()
                                .map(|result| result.arr)
                                .collect::<Vec<_>>(),
                        )
                    },
                )
                .collect::<HashMap<_, _>>();

            let ret = flatten_arrs
                .into_iter()
                .enumerate()
                .flat_map(|(col_idx, arr)| {
                    // Convert original column into its unnested version(s)
                    // Plural because one column can be unnested with different recursion level
                    // and into separate output columns
                    match multi_unnested_per_original_index.remove(&col_idx) {
                        Some(unnested_arrays) => unnested_arrays,
                        None => vec![arr],
                    }
                })
                .collect::<Vec<_>>();

            flatten_struct_cols(
                &ret,
                schema,
                batch.schema().metadata(),
                struct_column_indices,
            )
        }
    }?;
    Ok(Some(transformed))
}

/// Find the longest list length among the given list arrays for each row.
///
/// For example if we have the following two list arrays:
///
/// ```ignore
/// l1: [1, 2, 3], null, [], [3]
/// l2: [4,5], [], null, [6, 7]
/// ```
///
/// If `preserve_nulls` is false, the longest length array will be:
///
/// ```ignore
/// longest_length: [3, 0, 0, 2]
/// ```
///
/// whereas if `preserve_nulls` is true, the longest length array will be:
///
///
/// ```ignore
/// longest_length: [3, 1, 1, 2]
/// ```
///
fn find_longest_length(list_arrays: &[ArrayRef], options: &UnnestOptions) -> Result<ArrayRef> {
    // The length of a NULL list
    let null_length = if options.preserve_nulls {
        Scalar::new(Int64Array::from_value(1, 1))
    } else {
        Scalar::new(Int64Array::from_value(0, 1))
    };
    let list_lengths: Vec<ArrayRef> = list_arrays
        .iter()
        .map(|list_array| {
            let mut length_array = length(list_array)?;
            // Make sure length arrays have the same type. Int64 is the most general one.
            length_array = cast(&length_array, &DataType::Int64)?;
            length_array = zip(&is_not_null(&length_array)?, &length_array, &null_length)?;
            Ok(length_array)
        })
        .collect::<Result<_>>()?;

    let longest_length = list_lengths.iter().skip(1).try_fold(
        Arc::clone(&list_lengths[0]),
        |longest, current| {
            let is_lt = lt(&longest, &current)?;
            zip(&is_lt, &current, &longest)
        },
    )?;
    Ok(longest_length)
}

/// Trait defining common methods used for unnesting, implemented by list array types.
trait ListArrayType: Array {
    /// Returns a reference to the values of this list.
    fn values(&self) -> &ArrayRef;

    /// Returns the start and end offset of the values for the given row.
    fn value_offsets(&self, row: usize) -> (i64, i64);
}

impl ListArrayType for ListArray {
    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn value_offsets(&self, row: usize) -> (i64, i64) {
        let offsets = self.value_offsets();
        (offsets[row].into(), offsets[row + 1].into())
    }
}

impl ListArrayType for LargeListArray {
    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn value_offsets(&self, row: usize) -> (i64, i64) {
        let offsets = self.value_offsets();
        (offsets[row], offsets[row + 1])
    }
}

impl ListArrayType for FixedSizeListArray {
    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn value_offsets(&self, row: usize) -> (i64, i64) {
        let start = self.value_offset(row) as i64;
        (start, start + self.value_length() as i64)
    }
}

/// Unnest multiple list arrays according to the length array.
fn unnest_list_arrays(
    list_arrays: &[ArrayRef],
    length_array: &PrimitiveArray<Int64Type>,
    capacity: usize,
) -> Result<Vec<ArrayRef>> {
    let typed_arrays = list_arrays
        .iter()
        .map(|list_array| match list_array.data_type() {
            DataType::List(_) => Ok(list_array.as_list::<i32>() as &dyn ListArrayType),
            DataType::LargeList(_) => Ok(list_array.as_list::<i64>() as &dyn ListArrayType),
            DataType::FixedSizeList(_, _) => {
                Ok(list_array.as_fixed_size_list() as &dyn ListArrayType)
            }
            other => exec_err!("Invalid unnest datatype {other }"),
        })
        .collect::<Result<Vec<_>>>()?;

    typed_arrays
        .iter()
        .map(|list_array| unnest_list_array(*list_array, length_array, capacity))
        .collect::<Result<_>>()
}

/// Unnest a list array according the target length array.
///
/// Consider a list array like this:
///
/// ```ignore
/// [1], [2, 3, 4], null, [5], [],
/// ```
///
/// and the length array is:
///
/// ```ignore
/// [2, 3, 2, 1, 2]
/// ```
///
/// If the length of a certain list is less than the target length, pad with NULLs.
/// So the unnested array will look like this:
///
/// ```ignore
/// [1, null, 2, 3, 4, null, null, 5, null, null]
/// ```
///
fn unnest_list_array(
    list_array: &dyn ListArrayType,
    length_array: &PrimitiveArray<Int64Type>,
    capacity: usize,
) -> Result<ArrayRef> {
    // Validate capacity doesn't exceed safe limits
    if capacity > u32::MAX as usize {
        return Err(exec_datafusion_err!(
            "Capacity {} exceeds maximum supported size (u32::MAX = {})",
            capacity,
            u32::MAX
        ));
    }
    let values = list_array.values();
    let mut take_indices_builder = PrimitiveArray::<Int64Type>::builder(capacity);
    for row in 0..list_array.len() {
        let mut value_length = 0i64;
        if !list_array.is_null(row) {
            let (start, end) = list_array.value_offsets(row);
            value_length = end - start;
            // Validate that the indices we're creating are within safe bounds
            if start < 0 || end < 0 {
                return Err(exec_datafusion_err!(
                    "Invalid array offsets: start={}, end={}",
                    start,
                    end
                ));
            }
            // Check if the range would exceed safe limits
            if (end - start) as u64 > u32::MAX as u64 {
                return Err(exec_datafusion_err!(
                    "Array slice length {} exceeds maximum supported size (u32::MAX = {})",
                    end - start,
                    u32::MAX
                ));
            }
            for i in start..end {
                take_indices_builder.append_value(i)
            }
        }
        let target_length = length_array.value(row);
        debug_assert!(
            value_length <= target_length,
            "value length is beyond the longest length"
        );
        // Pad with NULL values
        let padding_needed = target_length - value_length;
        if padding_needed < 0 {
            return Err(exec_datafusion_err!(
                "Negative padding needed: value_length={}, target_length={}",
                value_length,
                target_length
            ));
        }
        if padding_needed as u64 > u32::MAX as u64 {
            return Err(exec_datafusion_err!(
                "Padding needed {} exceeds maximum supported size (u32::MAX = {})",
                padding_needed,
                u32::MAX
            ));
        }
        for _ in 0..padding_needed {
            take_indices_builder.append_null();
        }
    }
    let take_indices_array = take_indices_builder.finish();
    safe_take(values, &take_indices_array)
}

/// Creates take indices that will be used to expand all columns except for the list type
/// [`columns`](UnnestExec::list_column_indices) that is being unnested.
/// Every column value needs to be repeated multiple times according to the length array.
///
/// If the length array looks like this:
///
/// ```ignore
/// [2, 3, 1]
/// ```
/// Then [`create_take_indices`] will return an array like this
///
/// ```ignore
/// [0, 0, 1, 1, 1, 2]
/// ```
///
fn create_take_indices(
    length_array: &PrimitiveArray<Int64Type>,
    capacity: usize,
) -> PrimitiveArray<Int64Type> {
    // `find_longest_length()` guarantees this.
    debug_assert!(
        length_array.null_count() == 0,
        "length array should not contain nulls"
    );
    // Validate capacity doesn't exceed safe limits
    if capacity > u32::MAX as usize {
        // This should have been caught earlier, but add a safety check
        panic!(
            "create_take_indices called with capacity {} which exceeds u32::MAX",
            capacity
        );
    }
    let mut builder = PrimitiveArray::<Int64Type>::builder(capacity);
    for (index, repeat) in length_array.iter().enumerate() {
        // The length array should not contain nulls, so unwrap is safe
        let repeat = repeat.unwrap();
        // Check if repeat is within safe bounds
        if repeat < 0 {
            panic!("Negative repeat value {} in create_take_indices", repeat);
        }
        if repeat as u64 > u32::MAX as u64 {
            panic!(
                "Repeat value {} exceeds u32::MAX in create_take_indices",
                repeat
            );
        }
        // Convert to usize for the range, but we've already validated it fits
        let repeat_usize = repeat as usize;
        (0..repeat_usize).for_each(|_| builder.append_value(index as i64));
    }
    builder.finish()
}

/// Create a batch of arrays based on an input `batch` and a `indices` array.
/// The `indices` array is used by the take kernel to repeat values in the arrays
/// that are marked with `true` in the `repeat_mask`. Arrays marked with `false`
/// in the `repeat_mask` will be replaced with arrays filled with nulls of the
/// appropriate length.
///
/// For example if we have the following batch:
///
/// ```ignore
/// c1: [1], null, [2, 3, 4], null, [5, 6]
/// c2: 'a', 'b',  'c', null, 'd'
/// ```
///
/// then the `unnested_list_arrays` contains the unnest column that will replace `c1` in
/// the final batch if `preserve_nulls` is true:
///
/// ```ignore
/// c1: 1, null, 2, 3, 4, null, 5, 6
/// ```
///
/// And the `indices` array contains the indices that are used by `take` kernel to
/// repeat the values in `c2`:
///
/// ```ignore
/// 0, 1, 2, 2, 2, 3, 4, 4
/// ```
///
/// so that the final batch will look like:
///
/// ```ignore
/// c1: 1, null, 2, 3, 4, null, 5, 6
/// c2: 'a', 'b', 'c', 'c', 'c', null, 'd', 'd'
/// ```
///
/// The `repeat_mask` determines whether an array's values are repeated or replaced with nulls.
/// For example, if the `repeat_mask` is:
///
/// ```ignore
/// [true, false]
/// ```
///
/// The final batch will look like:
///
/// ```ignore
/// c1: 1, null, 2, 3, 4, null, 5, 6  // Repeated using `indices`
/// c2: null, null, null, null, null, null, null, null  // Replaced with nulls
use crate::utils::arrow::safe_take;

fn repeat_arrs_from_indices(
    batch: &[ArrayRef],
    indices: &PrimitiveArray<Int64Type>,
    repeat_mask: &[bool],
) -> Result<Vec<Arc<dyn Array>>> {
    batch
        .iter()
        .zip(repeat_mask.iter())
        .map(|(arr, &repeat)| {
            if repeat {
                // Use safe_take wrapper to avoid overflow in arrow-select's take kernel with large arrays
                safe_take(arr, indices)
            } else {
                // Fix: use indices.len() instead of arr.len() for output length
                Ok(new_null_array(arr.data_type(), indices.len()))
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoints::checkpoint_management::{
        CheckpointEpoch, CheckpointMessage, enrich_batch_metadata_with_checkpoints,
        extract_checkpoint_messages,
    };
    use arrow::array::{Int32Array, ListBuilder};
    use arrow::datatypes::{Field, Fields};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::StreamExt;
    use futures::stream;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_unnest_preserves_checkpoint_metadata_on_empty_result() -> Result<()> {
        // Create an input batch with checkpoint metadata
        // The batch will have a list column with all empty lists, which will cause build_batch to return None
        let checkpoint_messages = vec![CheckpointMessage::Marker {
            epoch: CheckpointEpoch(42),
            created_at_ms: 1000,
        }];
        let mut metadata = HashMap::new();
        enrich_batch_metadata_with_checkpoints(&mut metadata, &checkpoint_messages);

        // Create input schema with a list column
        let input_fields = Fields::from(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "items",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                false,
            ),
        ]);
        let input_schema = Arc::new(Schema::new_with_metadata(input_fields, metadata));

        // Create a batch with empty lists (this will cause build_batch to return None)
        let mut list_builder = ListBuilder::new(Int32Array::builder(0));
        // Add two empty lists
        list_builder.append_value([]);
        list_builder.append_value([]);
        let empty_list = list_builder.finish();

        let input_batch = RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(empty_list), // Empty lists
            ],
        )?;

        // Create output schema (after unnest, the list column becomes a regular column)
        let output_fields = Fields::from(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("item", DataType::Int32, true),
        ]);
        let output_schema = Arc::new(Schema::new(output_fields));

        // Create a mock input stream
        let input_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&input_schema),
            stream::once(async move { Ok(input_batch) }),
        ));

        let metrics = UnnestMetrics::new(0, &ExecutionPlanMetricsSet::new());
        let mut unnest_stream = UnnestStream {
            input: input_stream,
            schema: Arc::clone(&output_schema),
            list_type_columns: vec![ListUnnest {
                index_in_input_schema: 1,
                depth: 1,
            }],
            struct_column_indices: HashSet::new(),
            options: UnnestOptions::default(),
            metrics,
            resolved_schema: None,
            pending_checkpoints: Vec::new(),
        };

        // Process the stream
        let mut batches = Vec::new();
        while let Some(result) = unnest_stream.next().await {
            batches.push(result?);
        }

        // Verify that we got an empty batch with checkpoint metadata preserved
        assert_eq!(batches.len(), 1, "Should emit one batch even if empty");
        let output_batch = &batches[0];
        assert_eq!(output_batch.num_rows(), 0, "Output batch should be empty");

        // Verify checkpoint metadata is preserved
        let extracted_messages = extract_checkpoint_messages(output_batch.schema().metadata());
        assert_eq!(
            extracted_messages.len(),
            1,
            "Should preserve checkpoint metadata"
        );
        assert!(
            matches!(
                &extracted_messages[0],
                CheckpointMessage::Marker {
                    epoch: CheckpointEpoch(42),
                    ..
                }
            ),
            "Checkpoint message should match"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_unnest_preserves_checkpoint_metadata_on_non_empty_result() -> Result<()> {
        // Create an input batch with checkpoint metadata
        // The batch will have non-empty lists, so build_batch will return a non-empty result
        let checkpoint_messages = vec![CheckpointMessage::Marker {
            epoch: CheckpointEpoch(42),
            created_at_ms: 0,
        }];
        let mut metadata = HashMap::new();
        enrich_batch_metadata_with_checkpoints(&mut metadata, &checkpoint_messages);

        // Create input schema with a list column
        let input_fields = Fields::from(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "items",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                false,
            ),
        ]);
        let input_schema = Arc::new(Schema::new_with_metadata(input_fields, metadata));

        // Create a batch with non-empty lists
        let mut list_builder = ListBuilder::new(Int32Array::builder(0));
        list_builder.values().append_value(1);
        list_builder.values().append_value(2);
        list_builder.append(true);
        list_builder.values().append_value(3);
        list_builder.append(true);
        let list_array = list_builder.finish();

        let input_batch = RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![
                Arc::new(Int32Array::from(vec![10, 20])),
                Arc::new(list_array),
            ],
        )?;

        // Create output schema (after unnest, the list column becomes a regular column)
        let output_fields = Fields::from(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("item", DataType::Int32, true),
        ]);
        let output_schema = Arc::new(Schema::new(output_fields));

        // Create a mock input stream
        let input_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&input_schema),
            stream::once(async move { Ok(input_batch) }),
        ));

        let metrics = UnnestMetrics::new(0, &ExecutionPlanMetricsSet::new());
        let mut unnest_stream = UnnestStream {
            input: input_stream,
            schema: Arc::clone(&output_schema),
            list_type_columns: vec![ListUnnest {
                index_in_input_schema: 1,
                depth: 1,
            }],
            struct_column_indices: HashSet::new(),
            options: UnnestOptions::default(),
            metrics,
            resolved_schema: None,
            pending_checkpoints: Vec::new(),
        };

        // Process the stream
        let mut batches = Vec::new();
        while let Some(result) = unnest_stream.next().await {
            batches.push(result?);
        }

        // Verify that we got a non-empty batch with checkpoint metadata preserved
        assert_eq!(batches.len(), 1, "Should emit one batch");
        let output_batch = &batches[0];
        assert!(
            output_batch.num_rows() > 0,
            "Output batch should not be empty"
        );

        // Verify checkpoint metadata is preserved
        let extracted_messages = extract_checkpoint_messages(output_batch.schema().metadata());
        assert_eq!(
            extracted_messages.len(),
            1,
            "Should preserve checkpoint metadata"
        );
        assert!(
            matches!(
                &extracted_messages[0],
                CheckpointMessage::Marker {
                    epoch: CheckpointEpoch(42),
                    created_at_ms: 0
                }
            ),
            "Checkpoint message should match"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_batch_uses_resolved_schema_from_prior_batch() -> Result<()> {
        // This test verifies that empty batches (when build_batch returns None)
        // use the resolved schema from prior non-empty batches, ensuring schema consistency
        // when type promotion might occur (e.g., Utf8 -> LargeUtf8 via safe_take fallback)

        use arrow::array::StringBuilder;

        // Create first batch with non-empty string lists
        let mut list_builder = ListBuilder::new(StringBuilder::new());
        list_builder.values().append_value("hello");
        list_builder.values().append_value("world");
        list_builder.append(true);
        let non_empty_list = list_builder.finish();

        let input_fields = Fields::from(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "items",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
        ]);
        let input_schema = Arc::new(Schema::new(input_fields.clone()));

        let batch1 = RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(non_empty_list),
            ],
        )?;

        // Create second batch with empty lists (will cause build_batch to return None)
        let mut empty_list_builder = ListBuilder::new(StringBuilder::new());
        empty_list_builder.append_value(std::iter::empty::<Option<&str>>()); // Empty list
        let empty_list = empty_list_builder.finish();

        // Add checkpoint metadata to the second batch to force it to emit an empty batch
        let checkpoint_messages = vec![CheckpointMessage::Marker {
            epoch: CheckpointEpoch(100),
            created_at_ms: 0,
        }];
        let mut metadata = HashMap::new();
        enrich_batch_metadata_with_checkpoints(&mut metadata, &checkpoint_messages);
        let input_schema_with_metadata =
            Arc::new(Schema::new_with_metadata(input_fields, metadata));

        let batch2 = RecordBatch::try_new(
            input_schema_with_metadata,
            vec![Arc::new(Int32Array::from(vec![2])), Arc::new(empty_list)],
        )?;

        // Output schema (after unnest)
        let output_fields = Fields::from(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("item", DataType::Utf8, true),
        ]);
        let output_schema = Arc::new(Schema::new(output_fields));

        // Create input stream with both batches
        let input_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&input_schema),
            stream::iter(vec![Ok(batch1), Ok(batch2)]),
        ));

        let metrics = UnnestMetrics::new(0, &ExecutionPlanMetricsSet::new());
        let mut unnest_stream = UnnestStream {
            input: input_stream,
            schema: Arc::clone(&output_schema),
            list_type_columns: vec![ListUnnest {
                index_in_input_schema: 1,
                depth: 1,
            }],
            struct_column_indices: HashSet::new(),
            options: UnnestOptions::default(),
            metrics,
            resolved_schema: None,
            pending_checkpoints: Vec::new(),
        };

        // Process the stream
        let mut batches = Vec::new();
        while let Some(result) = unnest_stream.next().await {
            batches.push(result?);
        }

        // Should have two batches: one non-empty from batch1, one empty from batch2
        assert_eq!(batches.len(), 2, "Should emit two batches");

        let first_batch = &batches[0];
        let second_batch = &batches[1];

        assert!(
            first_batch.num_rows() > 0,
            "First batch should be non-empty"
        );
        assert_eq!(second_batch.num_rows(), 0, "Second batch should be empty");

        // Critical: Both batches should have the same schema (field types)
        // This ensures schema consistency for downstream operators like concat
        let first_schema = first_batch.schema();
        let second_schema = second_batch.schema();

        for (first_field, second_field) in first_schema
            .fields()
            .iter()
            .zip(second_schema.fields().iter())
        {
            assert_eq!(
                first_field.data_type(),
                second_field.data_type(),
                "Field '{}' has inconsistent types: non-empty batch has {:?}, empty batch has {:?}",
                first_field.name(),
                first_field.data_type(),
                second_field.data_type()
            );
        }

        Ok(())
    }
}
