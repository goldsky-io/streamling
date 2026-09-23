//! In-process plugins for host tests, registered through the same SDK macros
//! a plugin library uses. Options shape each node, and every instance records
//! what happened to it under its node's `node` option, so tests running in
//! parallel stay apart.

use crate::app_config::AppConfig;
use crate::checkpoints::checkpoint_management::{
    CheckpointMessage, enrich_batch_metadata_with_checkpoints, now_ms,
};
use crate::operators::coalesce::test_util::MarkerSourceExec;
use arrow::array::{Array, Int64Array, RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::physical_plan::ExecutionPlan;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Once};
use std::time::Duration;
use streamling_plugin::api::{PluginStateBackendFactory, SupportsGracefulShutdown};
use streamling_plugin::ffi::PluginMetricsRecorder;
use streamling_plugin::{
    CheckpointEpoch, InputPlacement, PartitionCount, PartitionedSinkPlugin,
    PartitionedSourcePlugin, PartitionedTransformPlugin, PluginError, PluginInstanceContext,
    PluginLabel, SinkDescription, SinkPlugin, SourceDescription, SourcePlugin,
    TransformDescription, TransformPlugin, init_plugin, partition_instance_name,
    register_partitioned_plugin_sink, register_partitioned_plugin_source,
    register_partitioned_plugin_transform, register_plugin_sink, register_plugin_transform,
};

pub(crate) const SOURCE: &str = "test.partitioned_source";
pub(crate) const TRANSFORM: &str = "test.partitioned_transform";
pub(crate) const SINK: &str = "test.partitioned_sink";
pub(crate) const LEGACY_TRANSFORM: &str = "test.legacy_transform";
pub(crate) const LEGACY_SINK: &str = "test.legacy_sink";

/// Name the node's instances record their observations under.
pub(crate) const NODE: &str = "node";
/// Rows each source partition emits before it completes. Without it, a
/// source emits empty batches until it is terminated.
pub(crate) const ROWS: &str = "rows";
pub(crate) const MINIMUM: &str = "minimum";
pub(crate) const MAXIMUM: &str = "maximum";
pub(crate) const PREFERRED: &str = "preferred";
/// `by_primary_key`, `round_robin`, or comma-separated column names.
pub(crate) const PLACEMENT: &str = "placement";
/// Partition index whose creation fails.
pub(crate) const FAIL_CREATE_AT: &str = "fail_create_at";
/// Partition index whose labels differ from the description.
pub(crate) const MISLABEL_AT: &str = "mislabel_at";
/// Partition index whose transform or sink `process_batch` fails.
pub(crate) const FAIL_BATCHES_AT: &str = "fail_batches_at";
/// When set, the checkpoint-marker hook of every instance fails.
pub(crate) const FAIL_MARKERS: &str = "fail_markers";
/// When set, the checkpoint-marker hook of every instance panics, which the
/// SDK does not report through `PluginMsg::Error`.
pub(crate) const PANIC_MARKERS: &str = "panic_markers";
/// Partition index whose sink instance delays each checkpoint ack.
pub(crate) const SLOW_ACK_AT: &str = "slow_ack_at";
/// How long the `slow_ack_at` instance delays each ack.
pub(crate) const ACK_DELAY_MS: &str = "ack_delay_ms";
/// When set, a sink instance records its partition index in its own state
/// and under `partition_{index}` in the node's shared state.
pub(crate) const WRITE_STATE: &str = "write_state";

/// Column holding the source partition that emitted a row.
pub(crate) const SOURCE_PARTITION_COLUMN: &str = "source_partition";
/// Column a transform appends with the partition that processed the row.
pub(crate) const TRANSFORM_PARTITION_COLUMN: &str = "transform_partition";
/// Source partition `p` emits ids `p * ID_STRIDE ..`.
pub(crate) const ID_STRIDE: i64 = 1_000_000;
const ROWS_PER_BATCH: usize = 10;
/// Pace of the empty batches an open-ended source emits.
const IDLE_BATCH_INTERVAL: Duration = Duration::from_millis(10);
const LABEL_KEY: &str = "fixture";
const LABEL_VALUE: &str = "described";

/// What one instance observed.
#[derive(Clone, Debug, Default)]
pub(crate) struct InstanceLog {
    pub context: Option<PluginInstanceContext>,
    pub initialized: bool,
    pub terminated: bool,
    pub ids: Vec<i64>,
    pub markers: Vec<u64>,
}

static LOGS: LazyLock<Mutex<HashMap<String, InstanceLog>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Observations of the instance named `instance` (`{node}[{i}]`, or `node`
/// for a single-stream plugin).
pub(crate) fn log(instance: &str) -> InstanceLog {
    LOGS.lock()
        .unwrap()
        .get(instance)
        .cloned()
        .unwrap_or_default()
}

fn record(instance: &str, update: impl FnOnce(&mut InstanceLog)) {
    update(
        LOGS.lock()
            .unwrap()
            .entry(instance.to_string())
            .or_default(),
    );
}

/// Registers the plugins above in the host's module registry, once.
pub(crate) fn install() {
    static INSTALLED: Once = Once::new();
    INSTALLED.call_once(|| {
        let app_config = AppConfig::load().expect("embedded config must load");
        super::initialize_plugin_module(get_module(), &app_config, "in-process test plugins")
            .expect("test plugins must register");
    });
}

struct TestOptions(HashMap<String, String>);

impl TestOptions {
    fn node(&self) -> String {
        self.0
            .get(NODE)
            .cloned()
            .expect("test plugins need a `node` option")
    }

    fn number<T: FromStr>(&self, key: &str) -> Option<T> {
        self.0.get(key).map(|v| {
            v.parse()
                .unwrap_or_else(|_| panic!("option {key} is not a number: {v}"))
        })
    }

    fn partition_count(&self) -> PartitionCount {
        PartitionCount {
            minimum: self.number(MINIMUM).unwrap_or(1),
            maximum: self.number(MAXIMUM),
            preferred: self.number(PREFERRED),
        }
    }

    fn placement(&self, default: InputPlacement) -> InputPlacement {
        match self.0.get(PLACEMENT).map(String::as_str) {
            None => default,
            Some("by_primary_key") => InputPlacement::ByPrimaryKey,
            Some("round_robin") => InputPlacement::RoundRobin,
            Some(columns) => {
                InputPlacement::ByColumns(columns.split(',').map(str::to_string).collect())
            }
        }
    }

    fn labels() -> Vec<PluginLabel> {
        vec![PluginLabel::new(LABEL_KEY, LABEL_VALUE)]
    }
}

/// What every test plugin instance shares: its identity and lifecycle.
struct Instance {
    name: String,
    context: Option<PluginInstanceContext>,
    labels: Vec<PluginLabel>,
    fail_markers: bool,
    panic_markers: bool,
    fail_batches: bool,
    running: AtomicBool,
}

impl Instance {
    fn create(
        context: Option<PluginInstanceContext>,
        options: &TestOptions,
    ) -> Result<Self, PluginInitializationError> {
        let index = context.as_ref().map(|c| c.partition_index);
        let partition_index = index.unwrap_or(0);
        if index.is_some() && index == options.number(FAIL_CREATE_AT) {
            return Err(PluginInitializationError::Configuration(
                "creation failed on purpose".into(),
            ));
        }
        let name = match index {
            Some(index) => partition_instance_name(&options.node(), index),
            None => options.node(),
        };
        let labels = if index.is_some() && index == options.number(MISLABEL_AT) {
            vec![PluginLabel::new(LABEL_KEY, "mislabeled")]
        } else {
            TestOptions::labels()
        };
        record(&name, |log| log.context = context.clone());
        Ok(Instance {
            name,
            context,
            labels,
            fail_markers: options.0.contains_key(FAIL_MARKERS),
            panic_markers: options.0.contains_key(PANIC_MARKERS),
            fail_batches: options.number(FAIL_BATCHES_AT) == Some(partition_index),
            running: AtomicBool::new(true),
        })
    }

    fn partition_index(&self) -> u32 {
        self.context.as_ref().map_or(0, |c| c.partition_index)
    }

    fn initialize(&self) {
        record(&self.name, |log| log.initialized = true);
    }

    fn terminate(&self) {
        self.running.store(false, Ordering::SeqCst);
        record(&self.name, |log| log.terminated = true);
    }

    fn checkpoint(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        if self.panic_markers {
            panic!("marker panicked on purpose");
        }
        if self.fail_markers {
            return Err(PluginError::Execution(
                "marker failed on purpose".to_string(),
            ));
        }
        record(&self.name, |log| log.markers.push(epoch.0));
        Ok(())
    }

    fn check_batch(&self) -> Result<(), PluginError> {
        if self.fail_batches {
            return Err(PluginError::Execution(
                "batch failed on purpose".to_string(),
            ));
        }
        Ok(())
    }

    fn record_ids(&self, batch: &RecordBatch) {
        let Some(ids) = batch.column_by_name("id") else {
            return;
        };
        let ids = ids
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        record(&self.name, |log| log.ids.extend(ids.values().iter()));
    }
}

pub(crate) fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(SOURCE_PARTITION_COLUMN, DataType::UInt32, false),
        Field::new(crate::data::COLUMN_NAME_OP, DataType::Utf8, false),
    ]))
}

/// A batch of the source schema carrying `messages` in its metadata, as a
/// batch flowing through a pipeline does.
pub(crate) fn batch(ids: &[i64], messages: &[CheckpointMessage]) -> RecordBatch {
    let mut metadata = HashMap::new();
    if !messages.is_empty() {
        enrich_batch_metadata_with_checkpoints(&mut metadata, messages);
    }
    let schema = Arc::new(source_schema().as_ref().clone().with_metadata(metadata));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(UInt32Array::from(vec![0; ids.len()])),
            Arc::new(arrow::array::StringArray::from(vec!["i"; ids.len()])),
        ],
    )
    .unwrap()
}

/// An input plan whose partition `i` yields `partitions[i]`.
pub(crate) fn input(partitions: Vec<Vec<RecordBatch>>) -> Arc<dyn ExecutionPlan> {
    Arc::new(MarkerSourceExec::with_schema(source_schema(), partitions))
}

pub(crate) fn marker(epoch: u64) -> CheckpointMessage {
    CheckpointMessage::Marker {
        epoch: crate::checkpoints::checkpoint_management::CheckpointEpoch(epoch),
        created_at_ms: now_ms(),
    }
}

/// Ids in a batch's `id` column.
pub(crate) fn ids(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column_by_name("id")
        .expect("batch has an id column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id is Int64")
        .values()
        .to_vec()
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

/// Emits `rows` rows per partition, then completes.
struct TestSource {
    instance: Instance,
    rows: Option<usize>,
    emitted: AtomicUsize,
}

#[async_trait]
impl SupportsGracefulShutdown for TestSource {
    fn is_running(&self) -> bool {
        self.instance.running.load(Ordering::SeqCst)
    }
    async fn terminate(&self) -> Result<(), PluginError> {
        self.instance.terminate();
        Ok(())
    }
}

#[async_trait]
impl SourcePlugin for TestSource {
    async fn initialize(&self) -> Result<(), PluginError> {
        self.instance.initialize();
        Ok(())
    }
    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(source_schema())
    }
    fn labels(&self) -> Vec<PluginLabel> {
        self.instance.labels.clone()
    }
    async fn generate_batch(&self) -> Result<RecordBatch, PluginError> {
        let Some(rows) = self.rows else {
            tokio::time::sleep(IDLE_BATCH_INTERVAL).await;
            return Ok(RecordBatch::new_empty(source_schema()));
        };
        let emitted = self.emitted.load(Ordering::SeqCst);
        let count = ROWS_PER_BATCH.min(rows - emitted);
        if count == 0 {
            self.instance.running.store(false, Ordering::SeqCst);
            return Ok(RecordBatch::new_empty(source_schema()));
        }
        self.emitted.fetch_add(count, Ordering::SeqCst);
        let partition = self.instance.partition_index();
        let ids: Vec<i64> = (emitted..emitted + count)
            .map(|n| i64::from(partition) * ID_STRIDE + n as i64)
            .collect();
        RecordBatch::try_new(
            source_schema(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(UInt32Array::from(vec![partition; count])),
                Arc::new(arrow::array::StringArray::from(vec!["i"; count])),
            ],
        )
        .map_err(PluginError::ArrowError)
    }
    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        self.instance.checkpoint(epoch)
    }
    async fn process_checkpoint_finalizer(&self, _: CheckpointEpoch) -> Result<(), PluginError> {
        Ok(())
    }
}

impl PartitionedSourcePlugin for TestSource {
    fn describe(
        options: &HashMap<String, String>,
    ) -> Result<SourceDescription, PluginInitializationError> {
        Ok(SourceDescription {
            output_schema: source_schema(),
            labels: TestOptions::labels(),
            partition_count: TestOptions(options.clone()).partition_count(),
        })
    }

    fn create(
        context: PluginInstanceContext,
        _: PluginAsyncRuntimeObj,
        _: PluginStateBackendFactory,
        _: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        let options = TestOptions(options);
        Ok(TestSource {
            instance: Instance::create(Some(context), &options)?,
            rows: options.number(ROWS),
            emitted: AtomicUsize::new(0),
        })
    }
}

/// Appends the partition that processed each row.
struct TestTransform {
    instance: Instance,
    output_schema: SchemaRef,
}

impl TestTransform {
    fn new(
        input_schema: SchemaRef,
        _: PluginAsyncRuntimeObj,
        _: PluginStateBackendFactory,
        _: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        Ok(TestTransform {
            instance: Instance::create(None, &TestOptions(options))?,
            output_schema: transform_output_schema(&input_schema),
        })
    }
}

#[async_trait]
impl SupportsGracefulShutdown for TestTransform {
    fn is_running(&self) -> bool {
        self.instance.running.load(Ordering::SeqCst)
    }
    async fn terminate(&self) -> Result<(), PluginError> {
        self.instance.terminate();
        Ok(())
    }
}

#[async_trait]
impl TransformPlugin for TestTransform {
    async fn initialize(&self) -> Result<(), PluginError> {
        self.instance.initialize();
        Ok(())
    }
    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(self.output_schema.clone())
    }
    fn labels(&self) -> Vec<PluginLabel> {
        self.instance.labels.clone()
    }
    async fn process_batch(&self, data: RecordBatch) -> Result<RecordBatch, PluginError> {
        self.instance.check_batch()?;
        self.instance.record_ids(&data);
        let mut columns = data.columns().to_vec();
        columns.push(Arc::new(UInt32Array::from(vec![
            self.instance
                .partition_index();
            data.num_rows()
        ])));
        RecordBatch::try_new(self.output_schema.clone(), columns).map_err(PluginError::ArrowError)
    }
    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        self.instance.checkpoint(epoch)
    }
    async fn process_checkpoint_finalizer(&self, _: CheckpointEpoch) -> Result<(), PluginError> {
        Ok(())
    }
}

impl PartitionedTransformPlugin for TestTransform {
    fn describe(
        input_schema: SchemaRef,
        options: &HashMap<String, String>,
    ) -> Result<TransformDescription, PluginInitializationError> {
        let options = TestOptions(options.clone());
        Ok(TransformDescription {
            output_schema: transform_output_schema(&input_schema),
            labels: TestOptions::labels(),
            input_placement: options.placement(InputPlacement::ByPrimaryKey),
            partition_count: options.partition_count(),
        })
    }

    fn create(
        context: PluginInstanceContext,
        input_schema: SchemaRef,
        _: PluginAsyncRuntimeObj,
        _: PluginStateBackendFactory,
        _: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        Ok(TestTransform {
            instance: Instance::create(Some(context), &TestOptions(options))?,
            output_schema: transform_output_schema(&input_schema),
        })
    }
}

/// Records the rows and markers it receives; the `slow_ack_at` instance acks
/// each marker only after `ack_delay_ms`.
struct TestSink {
    instance: Instance,
    ack_delay: Duration,
    state: Option<PluginStateBackendFactory>,
}

impl TestSink {
    fn new(
        _: SchemaRef,
        _: PluginAsyncRuntimeObj,
        state: PluginStateBackendFactory,
        _: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        Self::with_context(None, state, TestOptions(options))
    }

    fn with_context(
        context: Option<PluginInstanceContext>,
        state: PluginStateBackendFactory,
        options: TestOptions,
    ) -> Result<Self, PluginInitializationError> {
        let slow = context
            .as_ref()
            .is_some_and(|c| options.number(SLOW_ACK_AT) == Some(c.partition_index));
        let ack_delay = if slow {
            Duration::from_millis(options.number(ACK_DELAY_MS).unwrap_or(0))
        } else {
            Duration::ZERO
        };
        Ok(TestSink {
            instance: Instance::create(context, &options)?,
            ack_delay,
            state: options.0.contains_key(WRITE_STATE).then_some(state),
        })
    }
}

#[async_trait]
impl SupportsGracefulShutdown for TestSink {
    fn is_running(&self) -> bool {
        self.instance.running.load(Ordering::SeqCst)
    }
    async fn terminate(&self) -> Result<(), PluginError> {
        self.instance.terminate();
        Ok(())
    }
}

#[async_trait]
impl SinkPlugin for TestSink {
    async fn initialize(&self) -> Result<(), PluginError> {
        if let Some(state) = &self.state {
            let partition = self.instance.partition_index();
            state
                .create::<u32>()
                .put(partition)
                .await
                .map_err(PluginError::State)?;
            state
                .create_shared::<u32>()
                .put_kv(&format!("partition_{partition}"), partition)
                .await
                .map_err(PluginError::State)?;
        }
        self.instance.initialize();
        Ok(())
    }
    fn labels(&self) -> Vec<PluginLabel> {
        self.instance.labels.clone()
    }
    async fn process_batch(&self, data: RecordBatch) -> Result<(), PluginError> {
        self.instance.check_batch()?;
        self.instance.record_ids(&data);
        Ok(())
    }
    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        tokio::time::sleep(self.ack_delay).await;
        self.instance.checkpoint(epoch)
    }
    async fn process_checkpoint_finalizer(&self, _: CheckpointEpoch) -> Result<(), PluginError> {
        Ok(())
    }
}

impl PartitionedSinkPlugin for TestSink {
    fn describe(
        _: SchemaRef,
        options: &HashMap<String, String>,
    ) -> Result<SinkDescription, PluginInitializationError> {
        let options = TestOptions(options.clone());
        Ok(SinkDescription {
            labels: TestOptions::labels(),
            input_placement: options.placement(InputPlacement::RoundRobin),
            partition_count: options.partition_count(),
        })
    }

    fn create(
        context: PluginInstanceContext,
        _: SchemaRef,
        _: PluginAsyncRuntimeObj,
        state: PluginStateBackendFactory,
        _: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        Self::with_context(Some(context), state, TestOptions(options))
    }
}

register_partitioned_plugin_source!("test", "partitioned_source", TestSource);
register_partitioned_plugin_transform!("test", "partitioned_transform", TestTransform);
register_partitioned_plugin_sink!("test", "partitioned_sink", TestSink);
register_plugin_transform!("test", "legacy_transform", TestTransform);
register_plugin_sink!("test", "legacy_sink", TestSink);
init_plugin!();

/// Terminates every instance of `plugin` through the instance registry, the
/// way teardown does, and returns how each one exited.
pub(crate) async fn shut_down(
    plugin: &super::partitioned::PartitionedPlugin,
) -> Vec<(String, std::result::Result<(), String>)> {
    let futures = plugin.take_execution_futures();
    let entries = {
        let mut registry = super::PLUGIN_INSTANCE_REGISTRY.write().unwrap();
        futures
            .iter()
            .filter_map(|(key, _)| registry.remove(key).map(|channels| (key.clone(), channels)))
            .collect()
    };
    super::terminate_plugins(entries, None).unwrap();
    let mut exits = Vec::new();
    for (key, future) in futures {
        let exit = tokio::time::timeout(Duration::from_secs(5), future)
            .await
            .unwrap_or_else(|_| panic!("{key} must exit after Terminate"));
        exits.push((key, exit));
    }
    exits
}
