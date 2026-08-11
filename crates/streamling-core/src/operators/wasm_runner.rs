//! An operator that can execute provided WASM code. Currently, it relies on the runtime's
//! `eval` method.
//!
//! # Performance Optimizations
//!
//! This module supports several optimizations for high-throughput scenarios:
//!
//! 1. **WASM Plugin Pool**: Uses `extism::Pool` to maintain multiple WASM plugin instances
//!    that can process batches concurrently. Configure via `parallelism` parameter.
//!
//! 2. **Configurable Parallelism**: Set the number of concurrent WASM runners to balance
//!    throughput and resource usage (default: 4).
//!
//! 3. **Batch Accumulation**: Accumulates smaller batches into larger ones before processing
//!    to reduce WASM call overhead. Configure via `batch_size` parameter.

mod transpiler;

use crate::formats::ipc::{FromArrowToIpcConverter, FromIpcToArrowConverter};
use crate::formats::{FromArrowConverter, ToArrowConverter};
use crate::operators::wasm_runner::transpiler::TsToJSTranspiler;
use crate::utils::batch::enrich_batch_with_metadata;
use arrow_schema::SchemaRef;
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, DFSchemaRef, Statistics};
use datafusion::error::DataFusionError;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, execute_input_stream,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use extism::{Manifest, Plugin, Pool, Wasm};
use futures::StreamExt;
use std::cmp::{Eq, Ord, PartialEq, PartialOrd};
use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::{self, debug, error, info};

const WASM_FUNCTION_INVOKE: &str = "invoke";

/// Default number of WASM plugin instances in the pool
const DEFAULT_POOL_SIZE: usize = 4;

/// Default minimum batch size for accumulation (0 = no accumulation, process each batch immediately)
const DEFAULT_BATCH_SIZE: usize = 0;

/// The JS/TS WASM runtime, embedded so the binary is self-contained. Used when
/// `runtime_wasm_file_path` is not set; an explicit path overrides it.
const EMBEDDED_RUNTIME_WASM: &[u8] = include_bytes!("../../wasm/runtime.wasm");

pub struct WasmRunnerNode {
    pub input: LogicalPlan,
    language: String,
    script: String,
    runtime_wasm_file_path: Option<String>,
    internal_buffer_size: u32,
    schema_map: Option<BTreeMap<String, String>>,
    schema: Option<Arc<DFSchema>>,
    /// Number of WASM plugin instances to use for parallel processing.
    /// Higher values allow more concurrent batch processing but use more memory.
    /// Default is 4.
    parallelism: usize,
    /// Minimum number of rows to accumulate before processing.
    /// Smaller batches are combined until this threshold is reached.
    /// Set to 0 to disable accumulation and process each batch immediately.
    /// Default is 0 (disabled).
    batch_size: usize,
}

impl WasmRunnerNode {
    pub fn new(
        input: LogicalPlan,
        language: String,
        script: String,
        runtime_wasm_file_path: Option<String>,
        internal_buffer_size: u32,
        schema_map: Option<BTreeMap<String, String>>,
    ) -> Self {
        Self::with_options(
            input,
            language,
            script,
            runtime_wasm_file_path,
            internal_buffer_size,
            schema_map,
            DEFAULT_POOL_SIZE,
            DEFAULT_BATCH_SIZE,
        )
    }

    /// Create a new WasmRunnerNode with configurable pool size.
    ///
    /// # Arguments
    /// * `input` - The input logical plan
    /// * `language` - The script language ("javascript", "js", "typescript", "ts")
    /// * `script` - The script code
    /// * `runtime_wasm_file_path` - Path to the WASM runtime file
    /// * `internal_buffer_size` - Size of the internal buffer for batch processing
    /// * `schema_map` - Optional output schema mapping
    /// * `parallelism` - Number of WASM plugin instances for parallel processing (default: 4)
    #[allow(clippy::too_many_arguments)]
    pub fn with_parallelism(
        input: LogicalPlan,
        language: String,
        script: String,
        runtime_wasm_file_path: Option<String>,
        internal_buffer_size: u32,
        schema_map: Option<BTreeMap<String, String>>,
        parallelism: usize,
    ) -> Self {
        Self::with_options(
            input,
            language,
            script,
            runtime_wasm_file_path,
            internal_buffer_size,
            schema_map,
            parallelism,
            DEFAULT_BATCH_SIZE,
        )
    }

    /// Create a new WasmRunnerNode with full configuration options.
    ///
    /// # Arguments
    /// * `input` - The input logical plan
    /// * `language` - The script language ("javascript", "js", "typescript", "ts")
    /// * `script` - The script code
    /// * `runtime_wasm_file_path` - Path to the WASM runtime file
    /// * `internal_buffer_size` - Size of the internal buffer for batch processing
    /// * `schema_map` - Optional output schema mapping
    /// * `parallelism` - Number of WASM plugin instances for parallel processing (default: 4)
    /// * `batch_size` - Minimum rows to accumulate before processing (0 = disabled)
    #[allow(clippy::too_many_arguments)]
    pub fn with_options(
        input: LogicalPlan,
        language: String,
        script: String,
        runtime_wasm_file_path: Option<String>,
        internal_buffer_size: u32,
        schema_map: Option<BTreeMap<String, String>>,
        parallelism: usize,
        batch_size: usize,
    ) -> Self {
        // Build DF schema directly here. If a target schema is provided, use it;
        // otherwise default to the input plan's schema.
        let schema = if let Some(ref schema_map) = schema_map {
            let arrow_schema = Self::create_arrow_schema_from_map(schema_map).unwrap();
            let df_schema = DFSchema::try_from(arrow_schema.as_ref().clone()).unwrap();
            Some(Arc::new(df_schema))
        } else {
            Some(Arc::new(input.schema().as_ref().clone()))
        };

        // Ensure parallelism is at least 1
        let parallelism = parallelism.max(1);

        Self {
            input,
            language,
            script,
            runtime_wasm_file_path,
            internal_buffer_size,
            schema_map,
            schema,
            parallelism,
            batch_size,
        }
    }

    pub fn get_output_schema(&self) -> Result<SchemaRef> {
        if let Some(schema_map) = &self.schema_map {
            Self::create_arrow_schema_from_map(schema_map)
        } else {
            // Convert input schema to Arrow schema
            let df_schema = self.input.schema();
            let arrow_schema = df_schema.as_arrow().clone();
            Ok(Arc::new(arrow_schema))
        }
    }

    pub fn create_arrow_schema_from_map(
        schema_map: &BTreeMap<String, String>,
    ) -> Result<SchemaRef> {
        let schema = crate::schema::arrow_schema_from_type_map(schema_map)?;

        // Always ensure _gs_op is present as a string type (non-nullable)
        if schema_map.contains_key(crate::data::COLUMN_NAME_OP) {
            return Ok(schema);
        }

        let mut fields = schema.fields().to_vec();
        fields.push(Arc::new(Field::new(
            crate::data::COLUMN_NAME_OP,
            DataType::Utf8,
            false, // non-nullable
        )));
        Ok(Arc::new(Schema::new(fields)))
    }
}

impl Debug for WasmRunnerNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for WasmRunnerNode {
    fn name(&self) -> &str {
        "WasmRunner"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.schema.as_ref().unwrap()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "WasmRunner")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");

        Ok(Self {
            input: inputs.swap_remove(0),
            language: self.language.clone(),
            script: self.script.clone(),
            runtime_wasm_file_path: self.runtime_wasm_file_path.clone(),
            internal_buffer_size: self.internal_buffer_size,
            schema_map: self.schema_map.clone(),
            schema: self.schema.clone(),
            parallelism: self.parallelism,
            batch_size: self.batch_size,
        })
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }
}

pub struct WasmRunnerExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for WasmRunnerExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(
            if let Some(wasm_runner_node) = node.as_any().downcast_ref::<WasmRunnerNode>() {
                let input_physical = planner
                    .create_physical_plan(&wasm_runner_node.input, session_state)
                    .await
                    .unwrap();

                let output_schema = wasm_runner_node.get_output_schema()?;

                let wasm_exec = Arc::new(WasmRunnerExec::new(
                    input_physical,
                    wasm_runner_node.language.clone(),
                    wasm_runner_node.script.clone(),
                    wasm_runner_node.runtime_wasm_file_path.clone(),
                    wasm_runner_node.internal_buffer_size,
                    output_schema,
                    wasm_runner_node.parallelism,
                    wasm_runner_node.batch_size,
                ));
                Some(wasm_exec)
            } else {
                None
            },
        )
    }
}

struct WasmRunnerExec {
    input: Arc<dyn ExecutionPlan>,
    language: String,
    script: String,
    runtime_wasm_file_path: Option<String>,
    internal_buffer_size: u32,
    cache: Arc<PlanProperties>,
    schema: SchemaRef,
    /// Number of WASM plugin instances in the pool
    parallelism: usize,
    /// Minimum rows to accumulate before processing (0 = disabled)
    batch_size: usize,
    /// Pre-transpiled code (cached for efficiency)
    transpiled_code: String,
}

impl WasmRunnerExec {
    #[allow(clippy::too_many_arguments)]
    fn new(
        input: Arc<dyn ExecutionPlan>,
        language: String,
        script: String,
        runtime_wasm_file_path: Option<String>,
        internal_buffer_size: u32,
        schema: SchemaRef,
        parallelism: usize,
        batch_size: usize,
    ) -> Self {
        let transpiler = TsToJSTranspiler::new();
        let cache = Self::compute_properties(schema.clone());

        // Pre-transpile the code at construction time for efficiency
        let transpiled_code = match language.as_str() {
            "javascript" | "js" => script.clone(),
            "typescript" | "ts" => transpiler.transpile(script.as_str()).unwrap_or_else(|e| {
                error!(
                    "Transpilation error during WasmRunnerExec construction: {}",
                    e
                );
                script.clone() // Fallback to original, will fail at runtime
            }),
            _ => script.clone(),
        };

        info!(
            "WasmRunnerExec created with parallelism={}, batch_size={}",
            parallelism, batch_size
        );

        Self {
            input,
            language,
            script,
            runtime_wasm_file_path,
            internal_buffer_size,
            cache: Arc::new(cache),
            schema,
            parallelism,
            batch_size,
            transpiled_code,
        }
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        )
    }

    fn create_wasm_plugin(&self) -> Plugin {
        let wasm_file = match &self.runtime_wasm_file_path {
            Some(path) => Wasm::file(path.clone()),
            None => Wasm::data(EMBEDDED_RUNTIME_WASM),
        };
        // Use pre-transpiled code for efficiency
        let wasm_manifest =
            Manifest::new([wasm_file]).with_config_key("code", self.transpiled_code.clone());
        Plugin::new(&wasm_manifest, [], true).unwrap()
    }

    /// Create a pool of WASM plugins for concurrent processing
    fn create_wasm_pool(&self) -> Pool {
        let runtime_wasm_file_path = self.runtime_wasm_file_path.clone();
        let transpiled_code = self.transpiled_code.clone();

        debug!(
            "Creating WASM plugin pool with {} instances",
            self.parallelism
        );

        Pool::new(move || {
            let wasm_file = match &runtime_wasm_file_path {
                Some(path) => Wasm::file(path.clone()),
                None => Wasm::data(EMBEDDED_RUNTIME_WASM),
            };
            let wasm_manifest =
                Manifest::new([wasm_file]).with_config_key("code", transpiled_code.clone());
            Plugin::new(&wasm_manifest, [], true)
        })
    }
}

impl Debug for WasmRunnerExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "WasmRunnerExec")
    }
}

impl DisplayAs for WasmRunnerExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "WasmRunnerExec (lang: {}), partitions={}",
                    self.language,
                    self.properties().output_partitioning().partition_count()
                )
            }
        }
    }
}

impl ExecutionPlan for WasmRunnerExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(WasmRunnerExec::new(
            children[0].clone(),
            self.language.clone(),
            self.script.clone(),
            self.runtime_wasm_file_path.clone(),
            self.internal_buffer_size,
            self.schema.clone(),
            self.parallelism,
            self.batch_size,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let data = execute_input_stream(
            Arc::clone(&self.input),
            Arc::clone(&self.input.schema()),
            partition,
            Arc::clone(&context),
        )?;

        let output_schema = self.schema.clone();
        let input_schema = self.input.schema();
        let parallelism = self.parallelism;
        let batch_size = self.batch_size;

        // Create WASM pool for concurrent processing
        let wasm_pool = if parallelism > 1 {
            Some(Arc::new(self.create_wasm_pool()))
        } else {
            None
        };

        // Fallback to single plugin if parallelism is 1
        let single_plugin = if parallelism == 1 {
            Some(std::sync::Mutex::new(self.create_wasm_plugin()))
        } else {
            None
        };

        let mut builder = RecordBatchReceiverStreamBuilder::new(
            self.schema(),
            self.internal_buffer_size as usize,
        );
        let tx = builder.tx();

        builder.spawn(async move {
            let mut stream = data;

            // For parallelism > 1, we can process batches concurrently
            // For parallelism == 1, we process sequentially (backward compatible)
            if parallelism > 1 {
                // Concurrent processing with pool
                Self::execute_with_pool(
                    &mut stream,
                    wasm_pool.as_ref().unwrap(),
                    output_schema,
                    input_schema,
                    batch_size,
                    parallelism,
                    tx,
                )
                .await
            } else {
                // Sequential processing with single plugin
                Self::execute_sequential(
                    &mut stream,
                    single_plugin.as_ref().unwrap(),
                    output_schema,
                    input_schema,
                    batch_size,
                    tx,
                )
                .await
            }
        });

        Ok(builder.build())
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema())))
    }
}

impl WasmRunnerExec {
    /// Execute batches sequentially with a single WASM plugin
    /// Supports batch accumulation when batch_size > 0
    async fn execute_sequential(
        stream: &mut SendableRecordBatchStream,
        plugin_mutex: &std::sync::Mutex<Plugin>,
        output_schema: SchemaRef,
        input_schema: SchemaRef,
        batch_size: usize,
        tx: tokio::sync::mpsc::Sender<Result<RecordBatch>>,
    ) -> Result<()> {
        let arrow_to_ipc_converter = FromArrowToIpcConverter::new();
        let mut from_ipc_converter = FromIpcToArrowConverter::new(output_schema.clone());

        // Batch accumulation state
        let mut accumulated_batches: Vec<RecordBatch> = Vec::new();
        let mut accumulated_rows: usize = 0;

        while let Some(batch) = stream.next().await {
            match batch {
                Ok(batch) => {
                    // If batch accumulation is disabled (batch_size == 0), process immediately
                    if batch_size == 0 {
                        let result = Self::process_single_batch(
                            &batch,
                            plugin_mutex,
                            &arrow_to_ipc_converter,
                            &mut from_ipc_converter,
                            &output_schema,
                        );

                        match result {
                            Ok(output_batch) => {
                                if tx.send(Ok(output_batch)).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                let _ = tx.send(Err(e)).await;
                                break;
                            }
                        }
                    } else {
                        // Accumulate batches until we reach batch_size
                        let batch_rows = batch.num_rows();
                        if batch_rows > 0 {
                            accumulated_batches.push(batch);
                            accumulated_rows += batch_rows;
                        }

                        // Process when we've accumulated enough rows
                        if accumulated_rows >= batch_size {
                            let combined_batch =
                                Self::combine_batches(&accumulated_batches, &input_schema)?;
                            accumulated_batches.clear();
                            accumulated_rows = 0;

                            let result = Self::process_single_batch(
                                &combined_batch,
                                plugin_mutex,
                                &arrow_to_ipc_converter,
                                &mut from_ipc_converter,
                                &output_schema,
                            );

                            match result {
                                Ok(output_batch) => {
                                    if tx.send(Ok(output_batch)).await.is_err() {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    let _ = tx.send(Err(e)).await;
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    break;
                }
            }
        }

        // Process any remaining accumulated batches
        if !accumulated_batches.is_empty() {
            let combined_batch = Self::combine_batches(&accumulated_batches, &input_schema)?;
            let result = Self::process_single_batch(
                &combined_batch,
                plugin_mutex,
                &arrow_to_ipc_converter,
                &mut from_ipc_converter,
                &output_schema,
            );

            if let Ok(output_batch) = result {
                let _ = tx.send(Ok(output_batch)).await;
            }
        }

        Ok(())
    }

    /// Combine multiple batches into a single batch using concat_batches
    fn combine_batches(batches: &[RecordBatch], schema: &SchemaRef) -> Result<RecordBatch> {
        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }

        if batches.len() == 1 {
            return Ok(batches[0].clone());
        }

        // Preserve metadata from the last batch (most recent checkpoint info)
        let last_metadata = batches.last().unwrap().schema().metadata().clone();

        let combined = concat_batches(schema, batches)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        // Re-apply metadata to the combined batch
        enrich_batch_with_metadata(combined, last_metadata).map_err(|e| e.into())
    }

    /// Execute batches concurrently using a pool of WASM plugins
    /// Supports batch accumulation when batch_size > 0
    async fn execute_with_pool(
        stream: &mut SendableRecordBatchStream,
        pool: &Arc<Pool>,
        output_schema: SchemaRef,
        input_schema: SchemaRef,
        batch_size: usize,
        parallelism: usize,
        tx: tokio::sync::mpsc::Sender<Result<RecordBatch>>,
    ) -> Result<()> {
        // Batch accumulation state
        let mut accumulated_batches: Vec<RecordBatch> = Vec::new();
        let mut accumulated_rows: usize = 0;

        while let Some(batch) = stream.next().await {
            match batch {
                Ok(batch) => {
                    // Determine the batch to process (either immediate or accumulated)
                    let batch_to_process = if batch_size == 0 {
                        // No accumulation, process immediately
                        Some(batch)
                    } else {
                        // Accumulate batches
                        let batch_rows = batch.num_rows();
                        if batch_rows > 0 {
                            accumulated_batches.push(batch);
                            accumulated_rows += batch_rows;
                        }

                        if accumulated_rows >= batch_size {
                            let combined =
                                Self::combine_batches(&accumulated_batches, &input_schema)?;
                            accumulated_batches.clear();
                            accumulated_rows = 0;
                            Some(combined)
                        } else {
                            None
                        }
                    };

                    if let Some(batch) = batch_to_process {
                        Self::parallel_process_batch(
                            &batch,
                            pool,
                            &output_schema,
                            parallelism,
                            &tx,
                        )
                        .await?;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    break;
                }
            }
        }

        // Process any remaining accumulated batches
        if !accumulated_batches.is_empty() {
            let combined = Self::combine_batches(&accumulated_batches, &input_schema)?;
            Self::parallel_process_batch(&combined, pool, &output_schema, parallelism, &tx).await?;
        }

        Ok(())
    }

    /// Process a batch in parallel by slicing it and using buffered stream
    async fn parallel_process_batch(
        batch: &RecordBatch,
        pool: &Arc<Pool>,
        output_schema: &SchemaRef,
        parallelism: usize,
        tx: &tokio::sync::mpsc::Sender<Result<RecordBatch>>,
    ) -> Result<()> {
        let total_rows = batch.num_rows();
        if total_rows == 0 {
            // For empty batches, emit an empty batch with the output schema and preserved metadata
            let input_metadata = batch.schema().metadata().clone();
            let empty_batch = RecordBatch::new_empty(output_schema.clone());
            let result =
                enrich_batch_with_metadata(empty_batch, input_metadata).map_err(|e| e.into());
            let _ = tx.send(result).await;
            return Ok(());
        }

        // Calculate effective workers and slice the batch
        let effective_workers = parallelism.min(total_rows).max(1);
        let shard_capacity = total_rows.div_ceil(effective_workers);

        let mut slices: Vec<RecordBatch> = Vec::with_capacity(effective_workers);
        for i in 0..effective_workers {
            let start = i.saturating_mul(shard_capacity);
            let remaining = total_rows.saturating_sub(start);
            let len = remaining.min(shard_capacity);
            if len > 0 {
                slices.push(batch.slice(start, len));
            }
        }

        // Process slices in parallel using buffered (preserves order unlike buffer_unordered)
        let pool = Arc::clone(pool);
        let output_schema = output_schema.clone();
        let batch_start = std::time::Instant::now();

        let results_with_timing: Vec<(Result<RecordBatch>, std::time::Duration)> =
            futures::stream::iter(slices.into_iter())
                .map(|slice| {
                    let pool = Arc::clone(&pool);
                    let output_schema = output_schema.clone();
                    async move {
                        let start = std::time::Instant::now();
                        let result = tokio::task::spawn_blocking(move || {
                            Self::process_batch_with_pool(&slice, &pool, &output_schema)
                        })
                        .await
                        .map_err(|e| {
                            DataFusionError::from(crate::streamling_err!(
                                "WASM task join error: {}",
                                e
                            ))
                        });
                        let elapsed = start.elapsed();
                        // Flatten the nested Result: Result<Result<RecordBatch>, JoinError> -> Result<RecordBatch>
                        let flattened = match result {
                            Ok(inner) => inner,
                            Err(e) => Err(e),
                        };
                        (flattened, elapsed)
                    }
                })
                .buffered(effective_workers)
                .collect()
                .await;

        // Log summary for all slices
        let total_elapsed = batch_start.elapsed();
        let slice_times: Vec<_> = results_with_timing.iter().map(|(_, d)| *d).collect();
        let min_time = slice_times.iter().min().copied().unwrap_or_default();
        let max_time = slice_times.iter().max().copied().unwrap_or_default();
        debug!(
            "WASM parallel batch: {} rows, {} slices, total={:?}, slice_times=[min={:?}, max={:?}]",
            total_rows,
            results_with_timing.len(),
            total_elapsed,
            min_time,
            max_time
        );

        // Collect successful batches and check for errors
        let mut successful_batches: Vec<RecordBatch> = Vec::new();
        for (result, _) in results_with_timing {
            match result {
                Ok(batch) => successful_batches.push(batch),
                Err(e) => {
                    // Send error and return early
                    let _ = tx.send(Err(e)).await;
                    return Ok(());
                }
            }
        }

        // Merge all successful batches into one and send
        if successful_batches.is_empty() {
            // All slices produced empty results, send empty batch with correct schema
            let input_metadata = batch.schema().metadata().clone();
            let empty_batch = RecordBatch::new_empty(output_schema.clone());
            let result =
                enrich_batch_with_metadata(empty_batch, input_metadata).map_err(|e| e.into());
            let _ = tx.send(result).await;
        } else if successful_batches.len() == 1 {
            // Only one batch, send directly
            let _ = tx.send(Ok(successful_batches.remove(0))).await;
        } else {
            // Multiple batches, merge them preserving metadata from input
            let input_metadata = batch.schema().metadata().clone();
            let merged = concat_batches(&output_schema, &successful_batches)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            let result = enrich_batch_with_metadata(merged, input_metadata).map_err(|e| e.into());
            let _ = tx.send(result).await;
        }

        Ok(())
    }

    /// Process a single batch using a mutex-protected plugin (sequential mode)
    fn process_single_batch(
        batch: &RecordBatch,
        plugin_mutex: &std::sync::Mutex<Plugin>,
        arrow_to_ipc_converter: &FromArrowToIpcConverter,
        from_ipc_converter: &mut FromIpcToArrowConverter,
        output_schema: &SchemaRef,
    ) -> Result<RecordBatch> {
        let input_metadata = batch.schema().metadata().clone();

        debug!(
            "wasm runner received batch with {} rows and {} columns",
            batch.num_rows(),
            batch.num_columns(),
        );

        if batch.num_rows() == 0 {
            let empty_batch = RecordBatch::new_empty(output_schema.clone());
            return enrich_batch_with_metadata(empty_batch, input_metadata).map_err(|e| e.into());
        }

        // Serialize RecordBatch to Arrow IPC bytes
        let ipc_bytes_vec = arrow_to_ipc_converter.convert_from_batch(batch)?;

        if ipc_bytes_vec.is_empty() {
            let empty_batch = RecordBatch::new_empty(output_schema.clone());
            return enrich_batch_with_metadata(empty_batch, input_metadata).map_err(|e| e.into());
        }

        let ipc_bytes = &ipc_bytes_vec[0];

        // Call WASM plugin with Arrow IPC bytes
        let output_ipc_bytes = {
            let mut plugin = plugin_mutex.lock().map_err(|e| {
                DataFusionError::from(crate::streamling_err!("failed to lock WASM plugin: {}", e))
            })?;

            plugin
                .call::<Vec<u8>, Vec<u8>>(WASM_FUNCTION_INVOKE, ipc_bytes.clone())
                .map_err(|e| {
                    error!("Script encountered errors: \n{}", e);
                    DataFusionError::from(crate::streamling_user_err!(
                        "WASM script execution failed: {}",
                        e
                    ))
                })?
        };

        // Buffer and convert output
        from_ipc_converter.buffer(output_ipc_bytes);
        let output_batch = from_ipc_converter.convert_to_batch()?;
        enrich_batch_with_metadata(output_batch, input_metadata).map_err(|e| e.into())
    }

    /// Process a single batch using the plugin pool (concurrent mode)
    fn process_batch_with_pool(
        batch: &RecordBatch,
        pool: &Pool,
        output_schema: &SchemaRef,
    ) -> Result<RecordBatch> {
        let input_metadata = batch.schema().metadata().clone();

        if batch.num_rows() == 0 {
            let empty_batch = RecordBatch::new_empty(output_schema.clone());
            return enrich_batch_with_metadata(empty_batch, input_metadata).map_err(|e| e.into());
        }

        let arrow_to_ipc_converter = FromArrowToIpcConverter::new();
        let mut from_ipc_converter = FromIpcToArrowConverter::new(output_schema.clone());

        // Serialize RecordBatch to Arrow IPC bytes
        let ipc_bytes_vec = arrow_to_ipc_converter.convert_from_batch(batch)?;

        if ipc_bytes_vec.is_empty() {
            let empty_batch = RecordBatch::new_empty(output_schema.clone());
            return enrich_batch_with_metadata(empty_batch, input_metadata).map_err(|e| e.into());
        }

        let ipc_bytes = &ipc_bytes_vec[0];

        // Get a plugin instance from the pool and call it
        let output_ipc_bytes = {
            let mut plugin_handle = pool
                .get(std::time::Duration::from_secs(60))
                .map_err(|e| {
                    DataFusionError::from(crate::streamling_err!(
                        "failed to get WASM plugin from pool: {}",
                        e
                    ))
                })?
                .ok_or_else(|| {
                    DataFusionError::from(crate::streamling_err!(
                        "timed out waiting for WASM plugin from pool (60s)"
                    ))
                })?;

            plugin_handle
                .call::<Vec<u8>, Vec<u8>>(WASM_FUNCTION_INVOKE, ipc_bytes.clone())
                .map_err(|e| {
                    error!("Script encountered errors: \n{}", e);
                    DataFusionError::from(crate::streamling_user_err!(
                        "WASM script execution failed: {}",
                        e
                    ))
                })?
        };

        // Buffer and convert output
        from_ipc_converter.buffer(output_ipc_bytes);
        let output_batch = from_ipc_converter.convert_to_batch()?;
        enrich_batch_with_metadata(output_batch, input_metadata).map_err(|e| e.into())
    }
}

impl PartialEq for WasmRunnerNode {
    fn eq(&self, other: &Self) -> bool {
        self.language == other.language
            && self.script == other.script
            && self.runtime_wasm_file_path == other.runtime_wasm_file_path
            && self.internal_buffer_size == other.internal_buffer_size
            && self.schema_map == other.schema_map
            && self.parallelism == other.parallelism
            && self.batch_size == other.batch_size
    }
}

impl Eq for WasmRunnerNode {}

impl PartialOrd for WasmRunnerNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WasmRunnerNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.language
            .cmp(&other.language)
            .then(self.script.cmp(&other.script))
            .then(
                self.runtime_wasm_file_path
                    .cmp(&other.runtime_wasm_file_path),
            )
            .then(self.internal_buffer_size.cmp(&other.internal_buffer_size))
            .then(self.schema_map.cmp(&other.schema_map))
            .then(self.parallelism.cmp(&other.parallelism))
            .then(self.batch_size.cmp(&other.batch_size))
    }
}

impl std::hash::Hash for WasmRunnerNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.language.hash(state);
        self.script.hash(state);
        self.runtime_wasm_file_path.hash(state);
        self.internal_buffer_size.hash(state);
        self.schema_map.hash(state);
        self.parallelism.hash(state);
        self.batch_size.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::json::{FromArrowToJsonConverter, JsonToArrowConverter};
    use crate::operators::planner::StreamlingQueryPlanner;

    use datafusion::arrow::array::{Array, BooleanArray, Int64Array, StringArray};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::Extension;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    // Implement From for WasmRunnerNode to LogicalPlan
    impl From<WasmRunnerNode> for LogicalPlan {
        fn from(node: WasmRunnerNode) -> Self {
            LogicalPlan::Extension(Extension {
                node: Arc::new(node),
            })
        }
    }

    #[tokio::test]
    async fn test_wasm_with_provided_schema() {
        // Create a test session with StreamlingQueryPlanner
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(StreamlingQueryPlanner::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);

        let input_data = [
            r#"{"id": 1, "name": "test1", "active": true, "scores": [1, 2, 3]}"#,
            r#"{"id": 2, "name": "test2", "active": false, "scores": [4, 5, 6]}"#,
        ];

        let expected_output = [
            r#"{"_gs_op":"i","id":1,"name":"TEST1","scores_sum":6}"#,
            r#"{"_gs_op":"i","id":2,"name":"TEST2","scores_sum":15}"#,
        ];

        // Create input schema, the output schema should be inferred from the schema
        let input_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("active", arrow_schema::DataType::Boolean, false),
            arrow_schema::Field::new(
                "scores",
                arrow_schema::DataType::List(
                    arrow_schema::Field::new("item", arrow_schema::DataType::Int64, false).into(),
                ),
                false,
            ),
        ]));

        // Convert input JSON to Arrow batch
        let mut input_converter = JsonToArrowConverter::new(input_schema.clone(), true, None);
        for json_str in input_data {
            input_converter.buffer(json_str.to_string());
        }
        let input_batch = input_converter.convert_to_batch().unwrap();

        ctx.register_batch("test_table", input_batch).unwrap();

        let script = r#"
        function invoke(data) {
            return {
                id: data.id,
                name: data.name.toUpperCase(),
                scores_sum: data.scores.reduce((a, b) => a + b, 0),
            };
        }
        "#;

        let mut schema_map = BTreeMap::new();
        schema_map.insert("id".to_string(), "int64".to_string());
        schema_map.insert("name".to_string(), "string".to_string());
        schema_map.insert("scores_sum".to_string(), "int64".to_string());
        let wasm_node = WasmRunnerNode::new(
            ctx.table("test_table")
                .await
                .unwrap()
                .into_optimized_plan()
                .unwrap(),
            "javascript".to_string(),
            script.to_string(),
            None,
            1000,
            Some(schema_map),
        );

        let df = match ctx.execute_logical_plan(wasm_node.into()).await {
            Ok(df) => df,
            Err(e) => {
                eprintln!("Error executing logical plan: {:?}", e);
                panic!("Error executing logical plan: {:?}", e);
            }
        };
        let batches = match df.collect().await {
            Ok(batches) => batches,
            Err(e) => {
                eprintln!("Error collecting batches: {:?}", e);
                panic!("Error collecting batches: {:?}", e);
            }
        };

        // Convert all output batches to JSON for verification
        let arrow_to_json_converter = FromArrowToJsonConverter::new();
        let mut output_strings: Vec<String> = Vec::new();
        for batch in &batches {
            if batch.num_rows() > 0 {
                let output_json = arrow_to_json_converter.convert_from_batch(batch).unwrap();
                output_strings.extend(
                    output_json
                        .into_iter()
                        .map(|bytes| String::from_utf8(bytes).unwrap()),
                );
            }
        }

        // Verify the schema (includes _gs_op which is always added)
        let schema = batches[0].schema();
        assert_eq!(
            schema.fields().len(),
            4,
            "Schema should have 4 fields (3 user fields + _gs_op)"
        );
        assert_eq!(
            schema.field_with_name("id").unwrap().data_type(),
            &arrow_schema::DataType::Int64
        );
        assert_eq!(
            schema.field_with_name("name").unwrap().data_type(),
            &arrow_schema::DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("scores_sum").unwrap().data_type(),
            &arrow_schema::DataType::Int64
        );

        // Verify the data by comparing JSON strings
        assert_eq!(output_strings.len(), expected_output.len());
        for (actual, expected) in output_strings.iter().zip(expected_output.iter()) {
            // Parse both JSON strings to compare them structurally
            let actual_json: serde_json::Value = serde_json::from_str(actual).unwrap();
            let expected_json: serde_json::Value = serde_json::from_str(expected).unwrap();
            assert_eq!(actual_json, expected_json);
        }
    }

    #[tokio::test]
    async fn test_wasm_filter_rows_with_null() {
        // Create a test session with StreamlingQueryPlanner
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(StreamlingQueryPlanner::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);

        // Input data with some rows that should be filtered out
        let input_data = [
            r#"{"id": 1, "name": "test1", "active": true}"#,
            r#"{"id": 2, "name": "test2", "active": false}"#,
            r#"{"id": 3, "name": "test3", "active": true}"#,
            r#"{"id": 4, "name": "test4", "active": false}"#,
        ];

        // Expected output should only include rows where active is true
        let expected_output = [
            r#"{"_gs_op":"i","id":1,"name":"TEST1"}"#,
            r#"{"_gs_op":"i","id":3,"name":"TEST3"}"#,
        ];

        // Create input schema
        let input_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("active", arrow_schema::DataType::Boolean, false),
        ]));

        // Convert input JSON to Arrow batch
        let mut input_converter = JsonToArrowConverter::new(input_schema.clone(), true, None);
        for json_str in input_data {
            input_converter.buffer(json_str.to_string());
        }
        let input_batch = input_converter.convert_to_batch().unwrap();

        ctx.register_batch("test_table", input_batch).unwrap();

        // Script that returns null to filter out rows where active is false
        let script = r#"
        function invoke(data) {
            // Filter out rows where active is false by returning null
            if (!data.active) {
                return null;
            }
            return {
                id: data.id,
                name: data.name.toUpperCase(),
            };
        }
        "#;

        let mut schema_map = BTreeMap::new();
        schema_map.insert("id".to_string(), "int64".to_string());
        schema_map.insert("name".to_string(), "string".to_string());
        let wasm_node = WasmRunnerNode::new(
            ctx.table("test_table")
                .await
                .unwrap()
                .into_optimized_plan()
                .unwrap(),
            "javascript".to_string(),
            script.to_string(),
            None,
            1000,
            Some(schema_map),
        );

        let df = match ctx.execute_logical_plan(wasm_node.into()).await {
            Ok(df) => df,
            Err(e) => {
                eprintln!("Error executing logical plan: {:?}", e);
                panic!("Error executing logical plan: {:?}", e);
            }
        };
        let batches = match df.collect().await {
            Ok(batches) => batches,
            Err(e) => {
                eprintln!("Error collecting batches: {:?}", e);
                panic!("Error collecting batches: {:?}", e);
            }
        };

        // Verify we got batches (may be empty if all rows filtered)
        assert!(!batches.is_empty(), "Should have at least one batch");

        // Convert output batch back to JSON for verification
        let from_json_converter = FromArrowToJsonConverter::new();
        let mut all_output_strings = Vec::new();
        for batch in &batches {
            if batch.num_rows() > 0 {
                let output_json = from_json_converter.convert_from_batch(batch).unwrap();
                let output_strings: Vec<String> = output_json
                    .into_iter()
                    .map(|bytes| String::from_utf8(bytes).unwrap())
                    .collect();
                all_output_strings.extend(output_strings);
            }
        }

        // Verify the filtered output
        assert_eq!(
            all_output_strings.len(),
            expected_output.len(),
            "Should have filtered out inactive rows"
        );

        // Verify the data by comparing JSON strings
        for (actual, expected) in all_output_strings.iter().zip(expected_output.iter()) {
            // Parse both JSON strings to compare them structurally
            let actual_json: serde_json::Value = serde_json::from_str(actual).unwrap();
            let expected_json: serde_json::Value = serde_json::from_str(expected).unwrap();
            assert_eq!(actual_json, expected_json);
        }
    }

    #[tokio::test]
    async fn test_wasm_nested_row_types() {
        // Create a test session with StreamlingQueryPlanner
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(StreamlingQueryPlanner::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);

        let input_data = [
            r#"{"id": 1, "name": "test1", "address": {"street": "123 Main", "city": "NYC"}}"#,
            r#"{"id": 2, "name": "test2", "address": {"street": "456 Oak", "city": "LA"}}"#,
        ];

        // Create input schema
        let input_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new(
                "address",
                arrow_schema::DataType::Struct(arrow_schema::Fields::from(vec![
                    arrow_schema::Field::new("street", arrow_schema::DataType::Utf8, false),
                    arrow_schema::Field::new("city", arrow_schema::DataType::Utf8, false),
                ])),
                false,
            ),
        ]));

        // Convert input JSON to Arrow batch
        let mut input_converter = JsonToArrowConverter::new(input_schema.clone(), true, None);
        for json_str in input_data {
            input_converter.buffer(json_str.to_string());
        }
        let input_batch = input_converter.convert_to_batch().unwrap();

        ctx.register_batch("test_table", input_batch).unwrap();

        // Script that returns nested structures
        let script = r#"
        function invoke(data) {
            return {
                id: data.id,
                name: data.name.toUpperCase(),
                address: {
                    street: data.address.street,
                    city: data.address.city,
                    full: data.address.street + ", " + data.address.city
                },
                tags: ["tag1", "tag2"]
            };
        }
        "#;

        // Create output schema with nested struct and array
        let mut schema_map = BTreeMap::new();
        schema_map.insert("id".to_string(), "int64".to_string());
        schema_map.insert("name".to_string(), "string".to_string());
        schema_map.insert("address".to_string(), "struct".to_string());
        schema_map.insert("tags".to_string(), "list".to_string());

        // Note: The schema_map currently only supports flat types, but nested types
        // should work when the schema is inferred from the input or when properly
        // specified. For this test, we'll use the input schema as output schema
        // to demonstrate nested types work.
        let wasm_node = WasmRunnerNode::new(
            ctx.table("test_table")
                .await
                .unwrap()
                .into_optimized_plan()
                .unwrap(),
            "javascript".to_string(),
            script.to_string(),
            None,
            1000,
            None, // Use input schema to allow nested types
        );

        let df = match ctx.execute_logical_plan(wasm_node.into()).await {
            Ok(df) => df,
            Err(e) => {
                eprintln!("Error executing logical plan: {:?}", e);
                panic!("Error executing logical plan: {:?}", e);
            }
        };
        let batches = match df.collect().await {
            Ok(batches) => batches,
            Err(e) => {
                eprintln!("Error collecting batches: {:?}", e);
                panic!("Error collecting batches: {:?}", e);
            }
        };

        // Verify we got batches
        assert!(!batches.is_empty(), "Should have at least one batch");

        // Convert output batch back to JSON for verification
        let from_json_converter = FromArrowToJsonConverter::new();
        let mut all_output_strings = Vec::new();
        for batch in &batches {
            if batch.num_rows() > 0 {
                let output_json = from_json_converter.convert_from_batch(batch).unwrap();
                let output_strings: Vec<String> = output_json
                    .into_iter()
                    .map(|bytes| String::from_utf8(bytes).unwrap())
                    .collect();
                all_output_strings.extend(output_strings);
            }
        }

        // Verify we got output
        assert_eq!(all_output_strings.len(), 2, "Should have 2 output rows");

        // Verify nested structure is present in output
        let first_output: serde_json::Value = serde_json::from_str(&all_output_strings[0]).unwrap();
        assert_eq!(first_output["id"], 1);
        assert_eq!(first_output["name"], "TEST1");
        assert!(
            first_output["address"].is_object(),
            "Address should be an object"
        );
    }

    #[tokio::test]
    async fn test_wasm_one_to_many_row_expansion() {
        // Create a test session with StreamlingQueryPlanner
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(StreamlingQueryPlanner::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);

        // Input data - one row with an array field
        let input_data = [
            r#"{"id": 1, "name": "test1", "items": ["item1", "item2", "item3"]}"#,
            r#"{"id": 2, "name": "test2", "items": ["itemA", "itemB"]}"#,
        ];

        // Create input schema
        let input_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new(
                "items",
                arrow_schema::DataType::List(
                    arrow_schema::Field::new("item", arrow_schema::DataType::Utf8, false).into(),
                ),
                false,
            ),
        ]));

        // Convert input JSON to Arrow batch
        let mut input_converter = JsonToArrowConverter::new(input_schema.clone(), true, None);
        for json_str in input_data {
            input_converter.buffer(json_str.to_string());
        }
        let input_batch = input_converter.convert_to_batch().unwrap();

        ctx.register_batch("test_table", input_batch).unwrap();

        // Script that returns an array to expand one row into many rows
        let script = r#"
        function invoke(data) {
            // Return an array to expand one input row into multiple output rows
            return data.items.map(function(item) {
                return {
                    id: data.id,
                    name: data.name,
                    item: item
                };
            });
        }
        "#;

        let mut schema_map = BTreeMap::new();
        schema_map.insert("id".to_string(), "int64".to_string());
        schema_map.insert("name".to_string(), "string".to_string());
        schema_map.insert("item".to_string(), "string".to_string());
        let wasm_node = WasmRunnerNode::new(
            ctx.table("test_table")
                .await
                .unwrap()
                .into_optimized_plan()
                .unwrap(),
            "javascript".to_string(),
            script.to_string(),
            None,
            1000,
            Some(schema_map),
        );

        let df = match ctx.execute_logical_plan(wasm_node.into()).await {
            Ok(df) => df,
            Err(e) => {
                eprintln!("Error executing logical plan: {:?}", e);
                panic!("Error executing logical plan: {:?}", e);
            }
        };
        let batches = match df.collect().await {
            Ok(batches) => batches,
            Err(e) => {
                eprintln!("Error collecting batches: {:?}", e);
                panic!("Error collecting batches: {:?}", e);
            }
        };

        // Verify we got batches
        assert!(!batches.is_empty(), "Should have at least one batch");

        // Convert output batch back to JSON for verification
        let from_json_converter = FromArrowToJsonConverter::new();
        let mut all_output_strings = Vec::new();
        for batch in &batches {
            if batch.num_rows() > 0 {
                let output_json = from_json_converter.convert_from_batch(batch).unwrap();
                let output_strings: Vec<String> = output_json
                    .into_iter()
                    .map(|bytes| String::from_utf8(bytes).unwrap())
                    .collect();
                all_output_strings.extend(output_strings);
            }
        }

        // Verify we got expanded output: 3 rows from first input + 2 rows from second = 5 total
        assert_eq!(
            all_output_strings.len(),
            5,
            "Should have expanded 2 input rows into 5 output rows"
        );

        // Verify the expanded data
        let first_output: serde_json::Value = serde_json::from_str(&all_output_strings[0]).unwrap();
        assert_eq!(first_output["id"], 1);
        assert_eq!(first_output["name"], "test1");
        assert_eq!(first_output["item"], "item1");

        let third_output: serde_json::Value = serde_json::from_str(&all_output_strings[2]).unwrap();
        assert_eq!(third_output["id"], 1);
        assert_eq!(third_output["item"], "item3");

        let fourth_output: serde_json::Value =
            serde_json::from_str(&all_output_strings[3]).unwrap();
        assert_eq!(fourth_output["id"], 2);
        assert_eq!(fourth_output["item"], "itemA");
    }

    #[tokio::test]
    async fn test_wasm_null_fields_decoded_correctly() {
        // Create a test session with StreamlingQueryPlanner
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(StreamlingQueryPlanner::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);

        // Input data
        let input_data = [r#"{"id": 1, "name": "test1"}"#];

        // Create input schema
        let input_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
        ]));

        // Convert input JSON to Arrow batch
        let mut input_converter = JsonToArrowConverter::new(input_schema.clone(), true, None);
        for json_str in input_data {
            input_converter.buffer(json_str.to_string());
        }
        let input_batch = input_converter.convert_to_batch().unwrap();

        ctx.register_batch("test_table", input_batch).unwrap();

        // Script that returns all fields, with some explicitly set to null
        let script = r#"
        function invoke(data) {
            return {
                id: data.id,
                name: data.name,
                optional_string: null,
                optional_int: null,
                optional_bool: null
            };
        }
        "#;

        // Create output schema with nullable fields
        let mut schema_map = BTreeMap::new();
        schema_map.insert("id".to_string(), "int64".to_string());
        schema_map.insert("name".to_string(), "string".to_string());
        schema_map.insert("optional_string".to_string(), "string".to_string());
        schema_map.insert("optional_int".to_string(), "int64".to_string());
        schema_map.insert("optional_bool".to_string(), "boolean".to_string());

        let wasm_node = WasmRunnerNode::new(
            ctx.table("test_table")
                .await
                .unwrap()
                .into_optimized_plan()
                .unwrap(),
            "javascript".to_string(),
            script.to_string(),
            None,
            1000,
            Some(schema_map),
        );

        let df = match ctx.execute_logical_plan(wasm_node.into()).await {
            Ok(df) => df,
            Err(e) => {
                eprintln!("Error executing logical plan: {:?}", e);
                panic!("Error executing logical plan: {:?}", e);
            }
        };
        let batches = match df.collect().await {
            Ok(batches) => batches,
            Err(e) => {
                eprintln!("Error collecting batches: {:?}", e);
                panic!("Error collecting batches: {:?}", e);
            }
        };

        // Verify we got batches
        assert!(!batches.is_empty(), "Should have at least one batch");

        // Verify the schema has all fields (including _gs_op which is always added)
        let schema = batches[0].schema();
        assert_eq!(
            schema.fields().len(),
            6,
            "Schema should have 6 fields (5 user fields + _gs_op)"
        );
        assert!(
            schema.field_with_name("id").is_ok(),
            "Schema should have 'id' field"
        );
        assert!(
            schema.field_with_name("name").is_ok(),
            "Schema should have 'name' field"
        );
        assert!(
            schema.field_with_name("optional_string").is_ok(),
            "Schema should have 'optional_string' field"
        );
        assert!(
            schema.field_with_name("optional_int").is_ok(),
            "Schema should have 'optional_int' field"
        );
        assert!(
            schema.field_with_name("optional_bool").is_ok(),
            "Schema should have 'optional_bool' field"
        );

        // Verify all fields are nullable
        assert!(
            schema
                .field_with_name("optional_string")
                .unwrap()
                .is_nullable(),
            "optional_string should be nullable"
        );
        assert!(
            schema
                .field_with_name("optional_int")
                .unwrap()
                .is_nullable(),
            "optional_int should be nullable"
        );
        assert!(
            schema
                .field_with_name("optional_bool")
                .unwrap()
                .is_nullable(),
            "optional_bool should be nullable"
        );

        // Verify the batch has all 6 columns (5 user fields + _gs_op)
        assert_eq!(
            batches[0].num_columns(),
            6,
            "Batch should have 6 columns (5 user fields + _gs_op)"
        );

        // Verify null values are correctly represented
        let optional_string_col = batches[0]
            .column_by_name("optional_string")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(
            optional_string_col.is_null(0),
            "optional_string should be null"
        );

        let optional_int_col = batches[0]
            .column_by_name("optional_int")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(optional_int_col.is_null(0), "optional_int should be null");

        let optional_bool_col = batches[0]
            .column_by_name("optional_bool")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(optional_bool_col.is_null(0), "optional_bool should be null");

        // Verify non-null fields have values
        let id_col = batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1, "id should be 1");
        assert!(!id_col.is_null(0), "id should not be null");

        let name_col = batches[0]
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "test1", "name should be 'test1'");
        assert!(!name_col.is_null(0), "name should not be null");
    }

    #[tokio::test]
    async fn test_wasm_missing_and_undefined_columns_preserve_types() {
        // Test that when columns are missing (undefined) or null, they still return with correct types
        // Create a test session with StreamlingQueryPlanner
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(StreamlingQueryPlanner::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);

        // Input data
        let input_data = [
            r#"{"id": 1, "name": "test1"}"#,
            r#"{"id": 2, "name": "test2"}"#,
            r#"{"id": 3, "name": "test3"}"#,
        ];

        // Create input schema
        let input_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
        ]));

        // Convert input JSON to Arrow batch
        let mut input_converter = JsonToArrowConverter::new(input_schema.clone(), true, None);
        for json_str in input_data {
            input_converter.buffer(json_str.to_string());
        }
        let input_batch = input_converter.convert_to_batch().unwrap();

        ctx.register_batch("test_table", input_batch).unwrap();

        // Script that returns objects with:
        // - Row 1: all fields present
        // - Row 2: some fields missing (undefined), some null
        // - Row 3: some fields missing (undefined), some present
        let script = r#"
        function invoke(data) {
            if (data.id === 1) {
                // First row: all fields present
                return {
                    id: data.id,
                    name: data.name,
                    score: 100.5,
                    active: true,
                    count: 42
                };
            } else if (data.id === 2) {
                // Second row: some fields missing (undefined), some explicitly null
                return {
                    id: data.id,
                    name: data.name,
                    score: null,
                    // active is missing (undefined)
                    count: null
                };
            } else {
                // Third row: some fields missing (undefined), some present
                return {
                    id: data.id,
                    name: data.name,
                    score: 75.25,
                    // active is missing (undefined)
                    count: 10
                };
            }
        }
        "#;

        // Create output schema with various types
        let mut schema_map = BTreeMap::new();
        schema_map.insert("id".to_string(), "int64".to_string());
        schema_map.insert("name".to_string(), "string".to_string());
        schema_map.insert("score".to_string(), "float64".to_string());
        schema_map.insert("active".to_string(), "boolean".to_string());
        schema_map.insert("count".to_string(), "int64".to_string());

        // Use parallelism=1 because this test relies on all rows being processed together
        // to correctly handle missing/undefined fields across rows in the same IPC batch
        let wasm_node = WasmRunnerNode::with_parallelism(
            ctx.table("test_table")
                .await
                .unwrap()
                .into_optimized_plan()
                .unwrap(),
            "javascript".to_string(),
            script.to_string(),
            None,
            1000,
            Some(schema_map),
            1, // parallelism=1 to process all rows together
        );

        let df = match ctx.execute_logical_plan(wasm_node.into()).await {
            Ok(df) => df,
            Err(e) => {
                eprintln!("Error executing logical plan: {:?}", e);
                panic!("Error executing logical plan: {:?}", e);
            }
        };
        let batches = match df.collect().await {
            Ok(batches) => batches,
            Err(e) => {
                eprintln!("Error collecting batches: {:?}", e);
                panic!("Error collecting batches: {:?}", e);
            }
        };

        // Verify we got batches
        assert!(!batches.is_empty(), "Should have at least one batch");

        // With parallelism=1, all rows should be in the same batch
        let combined_batch = &batches[0];
        let schema = combined_batch.schema();

        // Verify the schema has all fields (including _gs_op which is always added)
        assert_eq!(
            schema.fields().len(),
            6,
            "Schema should have 6 fields (5 user fields + _gs_op)"
        );

        // Verify all columns exist and have correct types
        let id_field = schema.field_with_name("id").unwrap();
        assert_eq!(id_field.data_type(), &arrow_schema::DataType::Int64);
        assert!(id_field.is_nullable());

        let name_field = schema.field_with_name("name").unwrap();
        assert_eq!(name_field.data_type(), &arrow_schema::DataType::Utf8);
        assert!(name_field.is_nullable());

        let score_field = schema.field_with_name("score").unwrap();
        assert_eq!(score_field.data_type(), &arrow_schema::DataType::Float64);
        assert!(score_field.is_nullable(), "score should be nullable");

        let active_field = schema.field_with_name("active").unwrap();
        assert_eq!(active_field.data_type(), &arrow_schema::DataType::Boolean);
        assert!(active_field.is_nullable(), "active should be nullable");

        let count_field = schema.field_with_name("count").unwrap();
        assert_eq!(count_field.data_type(), &arrow_schema::DataType::Int64);
        assert!(count_field.is_nullable());

        // Verify the batch has all 6 columns (5 user fields + _gs_op)
        assert_eq!(
            combined_batch.num_columns(),
            6,
            "Batch should have 6 columns (5 user fields + _gs_op)"
        );
        assert_eq!(combined_batch.num_rows(), 3, "Batch should have 3 rows");

        // Verify column types are correct by downcasting
        use datafusion::arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};

        // Verify id column (Int64Array)
        let id_col = combined_batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column should be Int64Array");
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 2);
        assert_eq!(id_col.value(2), 3);

        // Verify name column (StringArray)
        let name_col = combined_batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column should be StringArray");
        assert_eq!(name_col.value(0), "test1");
        assert_eq!(name_col.value(1), "test2");
        assert_eq!(name_col.value(2), "test3");

        // Verify score column (Float64Array) - should be Float64 even when null/missing
        let score_col = combined_batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("score column should be Float64Array even when null/missing");
        assert_eq!(score_col.value(0), 100.5);
        assert!(score_col.is_null(1), "score should be null in row 2");
        assert_eq!(score_col.value(2), 75.25);

        // Verify active column (BooleanArray) - should be Boolean even when missing
        let active_col = combined_batch
            .column_by_name("active")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("active column should be BooleanArray even when missing");
        assert!(active_col.value(0));
        assert!(
            active_col.is_null(1),
            "active should be null in row 2 (was missing)"
        );
        assert!(
            active_col.is_null(2),
            "active should be null in row 3 (was missing)"
        );

        // Verify count column (Int64Array) - should be Int64 even when null/missing
        let count_col = combined_batch
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count column should be Int64Array even when null/missing");
        assert_eq!(count_col.value(0), 42);
        assert!(count_col.is_null(1), "count should be null in row 2");
        assert_eq!(count_col.value(2), 10);
    }

    #[tokio::test]
    async fn test_wasm_null_fields_with_one_to_many_expansion() {
        // Create a test session with StreamlingQueryPlanner
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(StreamlingQueryPlanner::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);

        // Input data with items array
        let input_data = [r#"{"id": 1, "name": "test1", "items": ["item1", "item2"]}"#];

        // Create input schema
        let input_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new(
                "items",
                arrow_schema::DataType::List(
                    arrow_schema::Field::new("item", arrow_schema::DataType::Utf8, false).into(),
                ),
                false,
            ),
        ]));

        // Convert input JSON to Arrow batch
        let mut input_converter = JsonToArrowConverter::new(input_schema.clone(), true, None);
        for json_str in input_data {
            input_converter.buffer(json_str.to_string());
        }
        let input_batch = input_converter.convert_to_batch().unwrap();

        ctx.register_batch("test_table", input_batch).unwrap();

        // Script that returns an array with some fields set to null
        let script = r#"
        function invoke(data) {
            return data.items.map(function(item, index) {
                return {
                    id: data.id,
                    name: data.name,
                    item: item,
                    optional_string: index === 0 ? "present" : null,
                    optional_int: index === 0 ? 42 : null,
                    optional_bool: index === 0 ? true : null
                };
            });
        }
        "#;

        // Create output schema with nullable fields
        let mut schema_map = BTreeMap::new();
        schema_map.insert("id".to_string(), "int64".to_string());
        schema_map.insert("name".to_string(), "string".to_string());
        schema_map.insert("item".to_string(), "string".to_string());
        schema_map.insert("optional_string".to_string(), "string".to_string());
        schema_map.insert("optional_int".to_string(), "int64".to_string());
        schema_map.insert("optional_bool".to_string(), "boolean".to_string());

        let wasm_node = WasmRunnerNode::new(
            ctx.table("test_table")
                .await
                .unwrap()
                .into_optimized_plan()
                .unwrap(),
            "javascript".to_string(),
            script.to_string(),
            None,
            1000,
            Some(schema_map),
        );

        let df = match ctx.execute_logical_plan(wasm_node.into()).await {
            Ok(df) => df,
            Err(e) => {
                eprintln!("Error executing logical plan: {:?}", e);
                panic!("Error executing logical plan: {:?}", e);
            }
        };
        let batches = match df.collect().await {
            Ok(batches) => batches,
            Err(e) => {
                eprintln!("Error collecting batches: {:?}", e);
                panic!("Error collecting batches: {:?}", e);
            }
        };

        // Verify we got batches
        assert!(!batches.is_empty(), "Should have at least one batch");

        // Verify the schema has all 7 fields (6 user fields + _gs_op)
        let schema = batches[0].schema();
        assert_eq!(
            schema.fields().len(),
            7,
            "Schema should have 7 fields (6 user fields + _gs_op)"
        );

        // Verify the batch has all 7 columns (6 user fields + _gs_op)
        assert_eq!(
            batches[0].num_columns(),
            7,
            "Batch should have 7 columns (6 user fields + _gs_op)"
        );

        // Collect all rows from all batches
        let mut all_output_strings = Vec::new();
        let from_json_converter = FromArrowToJsonConverter::new();
        for batch in &batches {
            if batch.num_rows() > 0 {
                let output_json = from_json_converter.convert_from_batch(batch).unwrap();
                let output_strings: Vec<String> = output_json
                    .into_iter()
                    .map(|bytes| String::from_utf8(bytes).unwrap())
                    .collect();
                all_output_strings.extend(output_strings);
            }
        }

        // Verify we got 2 rows (one-to-many expansion)
        assert_eq!(
            all_output_strings.len(),
            2,
            "Should have expanded 1 input row into 2 output rows"
        );

        // Verify first row has non-null optional fields
        let first_output: serde_json::Value = serde_json::from_str(&all_output_strings[0]).unwrap();
        assert_eq!(first_output["id"], 1);
        assert_eq!(first_output["item"], "item1");
        assert_eq!(first_output["optional_string"], "present");
        assert_eq!(first_output["optional_int"], 42);
        assert_eq!(first_output["optional_bool"], true);

        // Verify second row has null optional fields
        let second_output: serde_json::Value =
            serde_json::from_str(&all_output_strings[1]).unwrap();
        assert_eq!(second_output["id"], 1);
        assert_eq!(second_output["item"], "item2");
        assert!(second_output["optional_string"].is_null());
        assert!(second_output["optional_int"].is_null());
        assert!(second_output["optional_bool"].is_null());

        // Combine all batches for Arrow array verification
        let combined_batch = concat_batches(&batches[0].schema(), &batches).unwrap();

        // Verify null values are correctly represented in Arrow arrays
        let optional_string_col = combined_batch
            .column_by_name("optional_string")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(
            !optional_string_col.is_null(0),
            "First row optional_string should not be null"
        );
        assert_eq!(optional_string_col.value(0), "present");
        assert!(
            optional_string_col.is_null(1),
            "Second row optional_string should be null"
        );

        let optional_int_col = combined_batch
            .column_by_name("optional_int")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(
            !optional_int_col.is_null(0),
            "First row optional_int should not be null"
        );
        assert_eq!(optional_int_col.value(0), 42);
        assert!(
            optional_int_col.is_null(1),
            "Second row optional_int should be null"
        );

        let optional_bool_col = combined_batch
            .column_by_name("optional_bool")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(
            !optional_bool_col.is_null(0),
            "First row optional_bool should not be null"
        );
        assert!(optional_bool_col.value(0));
        assert!(
            optional_bool_col.is_null(1),
            "Second row optional_bool should be null"
        );
    }

    #[tokio::test]
    async fn test_wasm_transform_returning_empty_array() {
        // Test that a TypeScript transform returning an empty array [] doesn't crash the arrow deserializer
        // This is a regression test for the bug where returning [] would cause:
        // "Arrow IPC file contains no batches" error
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(StreamlingQueryPlanner::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);

        // Input data - multiple rows
        let input_data = [
            r#"{"id": 1, "name": "test1", "items": []}"#,
            r#"{"id": 2, "name": "test2", "items": []}"#,
        ];

        // Create input schema
        let input_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new(
                "items",
                arrow_schema::DataType::List(
                    arrow_schema::Field::new("item", arrow_schema::DataType::Utf8, false).into(),
                ),
                false,
            ),
        ]));

        // Convert input JSON to Arrow batch
        let mut input_converter = JsonToArrowConverter::new(input_schema.clone(), true, None);
        for json_str in input_data {
            input_converter.buffer(json_str.to_string());
        }
        let input_batch = input_converter.convert_to_batch().unwrap();

        ctx.register_batch("test_table", input_batch).unwrap();

        // Script that returns an empty array [] for each row
        // This should expand to zero output rows, not crash
        let script = r#"
        function invoke(data) {
            // Return empty array - this should result in zero output rows
            // Previously this caused: "Arrow IPC file contains no batches"
            return [];
        }
        "#;

        let mut schema_map = BTreeMap::new();
        schema_map.insert("id".to_string(), "int64".to_string());
        schema_map.insert("name".to_string(), "string".to_string());
        schema_map.insert("item".to_string(), "string".to_string());

        let wasm_node = WasmRunnerNode::new(
            ctx.table("test_table")
                .await
                .unwrap()
                .into_optimized_plan()
                .unwrap(),
            "javascript".to_string(),
            script.to_string(),
            None,
            1000,
            Some(schema_map),
        );

        let df = match ctx.execute_logical_plan(wasm_node.into()).await {
            Ok(df) => df,
            Err(e) => {
                eprintln!("Error executing logical plan: {:?}", e);
                panic!("Error executing logical plan: {:?}", e);
            }
        };
        let batches = match df.collect().await {
            Ok(batches) => batches,
            Err(e) => {
                eprintln!("Error collecting batches: {:?}", e);
                panic!(
                    "Error collecting batches: {:?}. This likely means the empty array return crashed the arrow deserializer.",
                    e
                );
            }
        };

        // Verify we got batches (may be empty)
        // The key is that this doesn't crash
        assert!(!batches.is_empty(), "Should have at least one batch");

        // Count total rows across all batches
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 0,
            "Should have zero output rows when returning empty array"
        );

        // Verify the schema is correct (output schema, not input)
        let schema = batches[0].schema();
        assert!(
            schema.field_with_name("id").is_ok() || schema.fields().len() == 4,
            "Schema should have output fields"
        );
    }

    #[tokio::test]
    async fn test_wasm_transform_returning_null_for_all_rows() {
        // Test that a TypeScript transform returning null for ALL rows doesn't crash the arrow deserializer
        // This is a regression test for the bug where filtering out all rows would cause:
        // "Arrow IPC file contains no batches" error
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(StreamlingQueryPlanner::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);

        // Input data - multiple rows
        let input_data = [
            r#"{"id": 1, "name": "test1", "active": false}"#,
            r#"{"id": 2, "name": "test2", "active": false}"#,
            r#"{"id": 3, "name": "test3", "active": false}"#,
        ];

        // Create input schema
        let input_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("active", arrow_schema::DataType::Boolean, false),
        ]));

        // Convert input JSON to Arrow batch
        let mut input_converter = JsonToArrowConverter::new(input_schema.clone(), true, None);
        for json_str in input_data {
            input_converter.buffer(json_str.to_string());
        }
        let input_batch = input_converter.convert_to_batch().unwrap();

        ctx.register_batch("test_table", input_batch).unwrap();

        // Script that returns null for every row (filters out all rows)
        // This should result in zero output rows, not crash
        let script = r#"
        function invoke(data) {
            // Return null to filter out ALL rows
            // Previously this caused: "Arrow IPC file contains no batches"
            return null;
        }
        "#;

        let mut schema_map = BTreeMap::new();
        schema_map.insert("id".to_string(), "int64".to_string());
        schema_map.insert("name".to_string(), "string".to_string());

        let wasm_node = WasmRunnerNode::new(
            ctx.table("test_table")
                .await
                .unwrap()
                .into_optimized_plan()
                .unwrap(),
            "javascript".to_string(),
            script.to_string(),
            None,
            1000,
            Some(schema_map),
        );

        let df = match ctx.execute_logical_plan(wasm_node.into()).await {
            Ok(df) => df,
            Err(e) => {
                eprintln!("Error executing logical plan: {:?}", e);
                panic!("Error executing logical plan: {:?}", e);
            }
        };
        let batches = match df.collect().await {
            Ok(batches) => batches,
            Err(e) => {
                eprintln!("Error collecting batches: {:?}", e);
                panic!(
                    "Error collecting batches: {:?}. This likely means returning null for all rows crashed the arrow deserializer.",
                    e
                );
            }
        };

        // Verify we got batches (may be empty)
        // The key is that this doesn't crash
        assert!(!batches.is_empty(), "Should have at least one batch");

        // Count total rows across all batches
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 0,
            "Should have zero output rows when returning null for all rows"
        );

        // Verify the schema is correct (output schema, not input)
        let schema = batches[0].schema();
        assert!(
            schema.field_with_name("id").is_ok() || schema.fields().len() == 3,
            "Schema should have output fields"
        );
    }

    #[tokio::test]
    async fn test_wasm_empty_batch_with_schema_change_and_metadata() {
        // Test that empty batches are handled correctly with schema changes and metadata propagation
        // Create a test session with StreamlingQueryPlanner
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(StreamlingQueryPlanner::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);

        // Create input schema (different from output schema)
        let input_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("input_field1", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("input_field2", arrow_schema::DataType::Utf8, false),
        ]));

        // Create an empty batch with input schema and metadata
        use std::collections::HashMap;
        let mut metadata = HashMap::new();
        metadata.insert("custom_metadata".to_string(), "test_value".to_string());
        metadata.insert("another_key".to_string(), "another_value".to_string());
        // Add checkpoint messages - these should be removed
        use crate::checkpoints::checkpoint_management::CHECKPOINT_MESSAGES_KEY;
        metadata.insert(
            CHECKPOINT_MESSAGES_KEY.to_string(),
            r#"[{"Marker":100}]"#.to_string(),
        );

        let empty_batch = RecordBatch::new_empty(input_schema.clone());
        let empty_batch_with_metadata =
            enrich_batch_with_metadata(empty_batch, metadata.clone()).unwrap();

        ctx.register_batch("test_table", empty_batch_with_metadata)
            .unwrap();

        // Script that filters out all rows (returns null to filter out the row)
        let script = r#"
        function invoke(data) {
            // Return null to filter out the row (but input is already empty)
            return null;
        }
        "#;

        // Create output schema (different from input schema)
        let mut schema_map = BTreeMap::new();
        schema_map.insert("output_field1".to_string(), "int64".to_string());
        schema_map.insert("output_field2".to_string(), "string".to_string());
        schema_map.insert("output_field3".to_string(), "boolean".to_string());

        let wasm_node = WasmRunnerNode::new(
            ctx.table("test_table")
                .await
                .unwrap()
                .into_optimized_plan()
                .unwrap(),
            "javascript".to_string(),
            script.to_string(),
            None,
            1000,
            Some(schema_map),
        );

        let df = match ctx.execute_logical_plan(wasm_node.into()).await {
            Ok(df) => df,
            Err(e) => {
                eprintln!("Error executing logical plan: {:?}", e);
                panic!("Error executing logical plan: {:?}", e);
            }
        };
        let batches = match df.collect().await {
            Ok(batches) => batches,
            Err(e) => {
                eprintln!("Error collecting batches: {:?}", e);
                panic!("Error collecting batches: {:?}", e);
            }
        };

        // Verify we got at least one batch
        assert!(!batches.is_empty(), "Should have at least one batch");

        // Find the empty batch
        let empty_batch = batches
            .iter()
            .find(|b| b.num_rows() == 0)
            .expect("Should have an empty batch");

        // Verify the empty batch has the OUTPUT schema, not the input schema
        let output_schema = empty_batch.schema();
        assert_eq!(
            output_schema.fields().len(),
            4,
            "Empty batch should have 4 fields (3 user fields + _gs_op)"
        );

        // Verify output schema fields match the expected output schema
        assert!(
            output_schema.field_with_name("output_field1").is_ok(),
            "Empty batch should have output_field1"
        );
        assert!(
            output_schema.field_with_name("output_field2").is_ok(),
            "Empty batch should have output_field2"
        );
        assert!(
            output_schema.field_with_name("output_field3").is_ok(),
            "Empty batch should have output_field3"
        );
        assert!(
            output_schema.field_with_name("_gs_op").is_ok(),
            "Empty batch should have _gs_op"
        );

        // Verify it does NOT have input schema fields
        assert!(
            output_schema.field_with_name("input_field1").is_err(),
            "Empty batch should NOT have input_field1 (wrong schema)"
        );
        assert!(
            output_schema.field_with_name("input_field2").is_err(),
            "Empty batch should NOT have input_field2 (wrong schema)"
        );

        // Verify metadata propagation (checkpoint_messages should be removed)
        let output_metadata = output_schema.metadata();
        assert_eq!(
            output_metadata.get("custom_metadata"),
            Some(&"test_value".to_string()),
            "Custom metadata should be propagated"
        );
        assert_eq!(
            output_metadata.get("another_key"),
            Some(&"another_value".to_string()),
            "Another metadata key should be propagated"
        );
        // Verify the batch is actually empty
        assert_eq!(empty_batch.num_rows(), 0, "Batch should be empty");
        assert_eq!(
            empty_batch.num_columns(),
            4,
            "Empty batch should have 4 columns (3 user fields + _gs_op)"
        );
    }

    #[tokio::test]
    async fn test_wasm_pool_preserves_row_order() {
        // Test that parallel processing with parallelism > 1 preserves row order
        // This verifies the execute_with_pool and parallel_process_batch functions
        // maintain ordering when processing slices concurrently
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(StreamlingQueryPlanner::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);

        // Create input data with 100 rows to ensure parallel slicing kicks in
        let num_rows = 100;
        let input_data: Vec<String> = (0..num_rows)
            .map(|i| format!(r#"{{"id": {}, "name": "test{}"}}"#, i, i))
            .collect();

        // Create input schema
        let input_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
        ]));

        // Convert input JSON to Arrow batch
        let mut input_converter = JsonToArrowConverter::new(input_schema.clone(), true, None);
        for json_str in &input_data {
            input_converter.buffer(json_str.clone());
        }
        let input_batch = input_converter.convert_to_batch().unwrap();

        ctx.register_batch("test_table", input_batch).unwrap();

        // Script that returns id doubled to verify data integrity
        let script = r#"
        function invoke(data) {
            return {
                id: data.id,
                doubled: data.id * 2,
                name: data.name
            };
        }
        "#;

        let mut schema_map = BTreeMap::new();
        schema_map.insert("id".to_string(), "int64".to_string());
        schema_map.insert("doubled".to_string(), "int64".to_string());
        schema_map.insert("name".to_string(), "string".to_string());

        // Use parallelism = 8 to ensure parallel processing with multiple slices
        let wasm_node = WasmRunnerNode::with_parallelism(
            ctx.table("test_table")
                .await
                .unwrap()
                .into_optimized_plan()
                .unwrap(),
            "javascript".to_string(),
            script.to_string(),
            None,
            1000,
            Some(schema_map),
            8, // parallelism > 1 triggers parallel processing
        );

        let df = match ctx.execute_logical_plan(wasm_node.into()).await {
            Ok(df) => df,
            Err(e) => {
                eprintln!("Error executing logical plan: {:?}", e);
                panic!("Error executing logical plan: {:?}", e);
            }
        };
        let batches = match df.collect().await {
            Ok(batches) => batches,
            Err(e) => {
                eprintln!("Error collecting batches: {:?}", e);
                panic!("Error collecting batches: {:?}", e);
            }
        };

        let batch = &batches[0];

        // Verify the batch has all rows
        assert_eq!(
            batch.num_rows(),
            num_rows,
            "Merged batch should have {} rows",
            num_rows
        );

        // Extract columns for verification
        let id_col = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let doubled_col = batch
            .column_by_name("doubled")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Verify row order is preserved: IDs should be 0, 1, 2, ..., 99 in order
        for idx in 0..num_rows {
            let id = id_col.value(idx);
            assert_eq!(
                id, idx as i64,
                "Row {} should have id={}, but got id={}. Order not preserved!",
                idx, idx, id
            );
        }

        // Verify data integrity: doubled should be id * 2
        for idx in 0..num_rows {
            let id = id_col.value(idx);
            let doubled = doubled_col.value(idx);
            assert_eq!(
                doubled,
                id * 2,
                "Row {} should have doubled={}, but got doubled={}",
                idx,
                id * 2,
                doubled
            );
        }
    }
}
