//! Partition-aware plugins: the host runs one instance per physical stream,
//! and each instance learns which partition it is from its
//! `PluginInstanceContext`.

use abi_stable::std_types::RDuration;
use arrow::array::{Array, Int64Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use streamling_plugin::api::{
    PluginStateBackendFactory, STREAMLING_COLUMN_NAME_OP, SupportsGracefulShutdown,
};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::PluginMetricsRecorder;
use streamling_plugin::{
    CheckpointEpoch, InputPlacement, PartitionCount, PartitionedSinkPlugin,
    PartitionedSourcePlugin, PartitionedTransformPlugin, PluginError, PluginInitializationError,
    PluginInstanceContext, SinkDescription, SinkPlugin, SourceDescription, SourcePlugin,
    TransformDescription, TransformPlugin,
};
use tracing::info;

/// Rows each source partition emits before it completes.
const ROWS_OPTION: &str = "rows";
/// Partitions the source runs with when the topology sets no `parallelism`.
const PREFERRED_PARTITIONS_OPTION: &str = "preferred_partitions";
/// Directory the file sink writes one file per partition into.
const OUTPUT_DIR_OPTION: &str = "output_dir";
const DEFAULT_ROWS: usize = 100;
const ROWS_PER_BATCH: usize = 10;
/// Partition `p` emits ids `p * ID_STRIDE ..`, so ids never collide.
const ID_STRIDE: i64 = 1_000_000;
const ID_COLUMN: &str = "id";
const SOURCE_PARTITION_COLUMN: &str = "source_partition";
const TRANSFORM_PARTITION_COLUMN: &str = "transform_partition";

fn configuration_error(message: String) -> PluginInitializationError {
    PluginInitializationError::Configuration(message.into())
}

fn parse_option<T: std::str::FromStr>(
    options: &HashMap<String, String>,
    key: &str,
) -> Result<Option<T>, PluginInitializationError> {
    options
        .get(key)
        .map(|value| {
            value
                .parse()
                .map_err(|_| configuration_error(format!("option {key} is not a number: {value}")))
        })
        .transpose()
}

fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(ID_COLUMN, DataType::Int64, false),
        Field::new(SOURCE_PARTITION_COLUMN, DataType::UInt32, false),
        Field::new(STREAMLING_COLUMN_NAME_OP, DataType::Utf8, false),
    ]))
}

/// Emits `rows` rows per partition, tagged with the partition that emitted
/// them, then completes.
pub struct PartitionedSource {
    context: PluginInstanceContext,
    rows: usize,
    emitted: AtomicUsize,
    running: AtomicBool,
}

#[async_trait]
impl SupportsGracefulShutdown for PartitionedSource {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl SourcePlugin for PartitionedSource {
    async fn initialize(&self) -> Result<(), PluginError> {
        info!(
            "Source partition {} of {} emits {} rows",
            self.context.partition_index, self.context.partition_count, self.rows
        );
        Ok(())
    }

    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(source_schema())
    }

    async fn generate_batch(&self) -> Result<RecordBatch, PluginError> {
        let emitted = self.emitted.load(Ordering::SeqCst);
        let count = ROWS_PER_BATCH.min(self.rows - emitted);
        if count == 0 {
            self.running.store(false, Ordering::SeqCst);
            return Ok(RecordBatch::new_empty(source_schema()));
        }
        self.emitted.fetch_add(count, Ordering::SeqCst);
        let partition = self.context.partition_index;
        let ids: Vec<i64> = (emitted..emitted + count)
            .map(|n| i64::from(partition) * ID_STRIDE + n as i64)
            .collect();
        RecordBatch::try_new(
            source_schema(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(UInt32Array::from(vec![partition; count])),
                Arc::new(StringArray::from(vec!["i"; count])),
            ],
        )
        .map_err(PluginError::ArrowError)
    }

    async fn process_checkpoint_marker(&self, _: CheckpointEpoch) -> Result<(), PluginError> {
        Ok(())
    }

    async fn process_checkpoint_finalizer(&self, _: CheckpointEpoch) -> Result<(), PluginError> {
        Ok(())
    }
}

impl PartitionedSourcePlugin for PartitionedSource {
    fn describe(
        options: &HashMap<String, String>,
    ) -> Result<SourceDescription, PluginInitializationError> {
        Ok(SourceDescription {
            output_schema: source_schema(),
            labels: Vec::new(),
            partition_count: PartitionCount {
                preferred: parse_option(options, PREFERRED_PARTITIONS_OPTION)?,
                ..PartitionCount::default()
            },
        })
    }

    fn create(
        context: PluginInstanceContext,
        _: PluginAsyncRuntimeObj,
        _: PluginStateBackendFactory,
        _: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        Ok(PartitionedSource {
            context,
            rows: parse_option(&options, ROWS_OPTION)?.unwrap_or(DEFAULT_ROWS),
            emitted: AtomicUsize::new(0),
            running: AtomicBool::new(true),
        })
    }
}

/// Tags each row with the partition that processed it. Asks for its input
/// placed by primary key, so all rows of a key reach the same instance.
pub struct PartitionedTransform {
    context: PluginInstanceContext,
    output_schema: SchemaRef,
    running: AtomicBool,
}

fn transform_output_schema(input_schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Field> = input_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(
        TRANSFORM_PARTITION_COLUMN,
        DataType::UInt32,
        false,
    ));
    Arc::new(Schema::new(fields))
}

#[async_trait]
impl SupportsGracefulShutdown for PartitionedTransform {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl TransformPlugin for PartitionedTransform {
    async fn initialize(&self) -> Result<(), PluginError> {
        Ok(())
    }

    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(self.output_schema.clone())
    }

    async fn process_batch(&self, data: RecordBatch) -> Result<RecordBatch, PluginError> {
        let mut columns = data.columns().to_vec();
        columns.push(Arc::new(UInt32Array::from(vec![
            self.context
                .partition_index;
            data.num_rows()
        ])));
        RecordBatch::try_new(self.output_schema.clone(), columns).map_err(PluginError::ArrowError)
    }

    async fn process_checkpoint_marker(&self, _: CheckpointEpoch) -> Result<(), PluginError> {
        Ok(())
    }

    async fn process_checkpoint_finalizer(&self, _: CheckpointEpoch) -> Result<(), PluginError> {
        Ok(())
    }
}

impl PartitionedTransformPlugin for PartitionedTransform {
    fn describe(
        input_schema: SchemaRef,
        _: &HashMap<String, String>,
    ) -> Result<TransformDescription, PluginInitializationError> {
        Ok(TransformDescription {
            output_schema: transform_output_schema(&input_schema),
            labels: Vec::new(),
            input_placement: InputPlacement::ByPrimaryKey,
            partition_count: PartitionCount::default(),
        })
    }

    fn create(
        context: PluginInstanceContext,
        input_schema: SchemaRef,
        _: PluginAsyncRuntimeObj,
        _: PluginStateBackendFactory,
        _: PluginMetricsRecorder,
        _: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        Ok(PartitionedTransform {
            context,
            output_schema: transform_output_schema(&input_schema),
            running: AtomicBool::new(true),
        })
    }
}

/// Appends each row's id to `{output_dir}/{node}-{partition}.csv`, one file
/// per partition, and makes it durable on every checkpoint marker.
pub struct PartitionedFileSink {
    rt: PluginAsyncRuntimeObj,
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
    running: AtomicBool,
}

impl PartitionedFileSink {
    fn flush(&self) -> Result<(), PluginError> {
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| PluginError::Internal("file sink writer lock poisoned".to_string()))?;
        writer.flush().map_err(PluginError::IoError)?;
        writer.get_ref().sync_data().map_err(PluginError::IoError)
    }
}

#[async_trait]
impl SupportsGracefulShutdown for PartitionedFileSink {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::SeqCst);
        self.flush()
    }
}

#[async_trait]
impl SinkPlugin for PartitionedFileSink {
    async fn initialize(&self) -> Result<(), PluginError> {
        info!("Writing partition file {:?}", self.path);
        Ok(())
    }

    async fn process_batch(&self, data: RecordBatch) -> Result<(), PluginError> {
        let ids = data
            .column_by_name(ID_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| {
                PluginError::Execution(format!("input has no Int64 column '{ID_COLUMN}'"))
            })?;
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| PluginError::Internal("file sink writer lock poisoned".to_string()))?;
        for id in ids.iter().flatten() {
            writeln!(writer, "{id}").map_err(PluginError::IoError)?;
        }
        Ok(())
    }

    async fn process_checkpoint_marker(&self, _: CheckpointEpoch) -> Result<(), PluginError> {
        self.flush()?;
        // A little latency, so instances ack at different times.
        self.rt.sleep(RDuration::from_millis(10)).await;
        Ok(())
    }

    async fn process_checkpoint_finalizer(&self, _: CheckpointEpoch) -> Result<(), PluginError> {
        Ok(())
    }
}

impl PartitionedSinkPlugin for PartitionedFileSink {
    fn describe(
        input_schema: SchemaRef,
        options: &HashMap<String, String>,
    ) -> Result<SinkDescription, PluginInitializationError> {
        if !options.contains_key(OUTPUT_DIR_OPTION) {
            return Err(configuration_error(format!(
                "option {OUTPUT_DIR_OPTION} is required"
            )));
        }
        if input_schema.field_with_name(ID_COLUMN).is_err() {
            return Err(configuration_error(format!(
                "input must have an '{ID_COLUMN}' column"
            )));
        }
        Ok(SinkDescription {
            labels: Vec::new(),
            input_placement: InputPlacement::ByPrimaryKey,
            partition_count: PartitionCount::default(),
        })
    }

    fn create(
        context: PluginInstanceContext,
        _: SchemaRef,
        rt: PluginAsyncRuntimeObj,
        _: PluginStateBackendFactory,
        _: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        let output_dir = options.get(OUTPUT_DIR_OPTION).ok_or_else(|| {
            configuration_error(format!("option {OUTPUT_DIR_OPTION} is required"))
        })?;
        let path = PathBuf::from(output_dir).join(format!(
            "{}-{}.csv",
            context.reference_name, context.partition_index
        ));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| configuration_error(format!("cannot open {path:?}: {e}")))?;
        Ok(PartitionedFileSink {
            rt,
            path,
            writer: Mutex::new(BufWriter::new(file)),
            running: AtomicBool::new(true),
        })
    }
}
