//! Capability and dispatch of the module `init_plugin!` generates when legacy
//! and partition-aware registrations share one library.

use std::collections::HashMap;
use std::sync::Arc;

use abi_stable::external_types::crossbeam_channel;
use abi_stable::nonexhaustive_enum::NonExhaustive;
use abi_stable::std_types::{RNone, RSome};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use streamling_plugin::api::{PluginStateBackendFactory, SupportsGracefulShutdown};
use streamling_plugin::r#async::DirectTokioProxy;
use streamling_plugin::ffi::{PluginMetricsChannel, PluginMetricsRecorder};
use streamling_plugin::{
    CheckpointEpoch, InputPlacement, PartitionCount, PartitionedSinkPlugin,
    PartitionedSourcePlugin, PartitionedTransformPlugin, PluginChannel, PluginError,
    PluginInputPlacement, PluginInstanceContext, PluginLabel, PluginMsg, SinkDescription,
    SinkPlugin, SourceDescription, SourcePlugin, TransformDescription, TransformPlugin,
    init_plugin, register_partitioned_plugin_sink, register_partitioned_plugin_source,
    register_partitioned_plugin_transform, register_plugin_source,
};

const LEGACY_SOURCE: &str = "test.legacy_source";
const PARTITIONED_SOURCE: &str = "test.partitioned_source";
const PARTITIONED_TRANSFORM: &str = "test.partitioned_transform";
const PARTITIONED_SINK: &str = "test.partitioned_sink";
const PARTITION_LABEL: &str = "partition";

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("_gs_op", DataType::Utf8, false),
    ]))
}

/// One type serves every registration here: it reports the partition it was
/// created for as a label, so the tests can see which context reached it.
struct TestPlugin {
    context: Option<PluginInstanceContext>,
}

impl TestPlugin {
    fn new(
        _: PluginAsyncRuntimeObj,
        _: PluginStateBackendFactory,
        _: PluginMetricsRecorder,
        _: HashMap<String, String>,
    ) -> Self {
        TestPlugin { context: None }
    }

    fn partition_labels(&self) -> Vec<PluginLabel> {
        self.context
            .iter()
            .map(|c| {
                PluginLabel::new(
                    PARTITION_LABEL,
                    format!("{}/{}", c.partition_index, c.partition_count),
                )
            })
            .collect()
    }
}

#[async_trait]
impl SupportsGracefulShutdown for TestPlugin {
    fn is_running(&self) -> bool {
        true
    }
    async fn terminate(&self) -> Result<(), PluginError> {
        Ok(())
    }
}

#[async_trait]
impl SourcePlugin for TestPlugin {
    async fn initialize(&self) -> Result<(), PluginError> {
        Ok(())
    }
    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(schema())
    }
    fn labels(&self) -> Vec<PluginLabel> {
        self.partition_labels()
    }
    async fn generate_batch(&self) -> Result<RecordBatch, PluginError> {
        Ok(RecordBatch::new_empty(schema()))
    }
    async fn process_checkpoint_marker(&self, _: CheckpointEpoch) -> Result<(), PluginError> {
        Ok(())
    }
    async fn process_checkpoint_finalizer(&self, _: CheckpointEpoch) -> Result<(), PluginError> {
        Ok(())
    }
}

#[async_trait]
impl TransformPlugin for TestPlugin {
    async fn initialize(&self) -> Result<(), PluginError> {
        Ok(())
    }
    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(schema())
    }
    fn labels(&self) -> Vec<PluginLabel> {
        self.partition_labels()
    }
    async fn process_batch(&self, data: RecordBatch) -> Result<RecordBatch, PluginError> {
        Ok(data)
    }
    async fn process_checkpoint_marker(&self, _: CheckpointEpoch) -> Result<(), PluginError> {
        Ok(())
    }
    async fn process_checkpoint_finalizer(&self, _: CheckpointEpoch) -> Result<(), PluginError> {
        Ok(())
    }
}

#[async_trait]
impl SinkPlugin for TestPlugin {
    async fn initialize(&self) -> Result<(), PluginError> {
        Ok(())
    }
    fn labels(&self) -> Vec<PluginLabel> {
        self.partition_labels()
    }
    async fn process_batch(&self, _: RecordBatch) -> Result<(), PluginError> {
        Ok(())
    }
    async fn process_checkpoint_marker(&self, _: CheckpointEpoch) -> Result<(), PluginError> {
        Ok(())
    }
    async fn process_checkpoint_finalizer(&self, _: CheckpointEpoch) -> Result<(), PluginError> {
        Ok(())
    }
}

impl PartitionedSourcePlugin for TestPlugin {
    fn describe(
        _: &HashMap<String, String>,
    ) -> Result<SourceDescription, streamling_plugin::PluginInitializationError> {
        Ok(SourceDescription {
            output_schema: schema(),
            labels: Vec::new(),
            partition_count: PartitionCount {
                preferred: Some(3),
                ..PartitionCount::default()
            },
        })
    }

    fn create(
        context: PluginInstanceContext,
        _: PluginAsyncRuntimeObj,
        _: PluginStateBackendFactory,
        _: PluginMetricsRecorder,
        _: HashMap<String, String>,
    ) -> Result<Self, streamling_plugin::PluginInitializationError> {
        Ok(TestPlugin {
            context: Some(context),
        })
    }
}

impl PartitionedTransformPlugin for TestPlugin {
    fn describe(
        input_schema: SchemaRef,
        _: &HashMap<String, String>,
    ) -> Result<TransformDescription, streamling_plugin::PluginInitializationError> {
        Ok(TransformDescription {
            output_schema: input_schema,
            labels: Vec::new(),
            input_placement: InputPlacement::ByPrimaryKey,
            partition_count: PartitionCount::default(),
        })
    }

    fn create(
        context: PluginInstanceContext,
        _: SchemaRef,
        _: PluginAsyncRuntimeObj,
        _: PluginStateBackendFactory,
        _: PluginMetricsRecorder,
        _: HashMap<String, String>,
    ) -> Result<Self, streamling_plugin::PluginInitializationError> {
        Ok(TestPlugin {
            context: Some(context),
        })
    }
}

impl PartitionedSinkPlugin for TestPlugin {
    fn describe(
        _: SchemaRef,
        _: &HashMap<String, String>,
    ) -> Result<SinkDescription, streamling_plugin::PluginInitializationError> {
        Ok(SinkDescription {
            labels: Vec::new(),
            input_placement: InputPlacement::RoundRobin,
            partition_count: PartitionCount::default(),
        })
    }

    fn create(
        context: PluginInstanceContext,
        _: SchemaRef,
        _: PluginAsyncRuntimeObj,
        _: PluginStateBackendFactory,
        _: PluginMetricsRecorder,
        _: HashMap<String, String>,
    ) -> Result<Self, streamling_plugin::PluginInitializationError> {
        Ok(TestPlugin {
            context: Some(context),
        })
    }
}

register_plugin_source!("test", "legacy_source", TestPlugin);
register_partitioned_plugin_source!("test", "partitioned_source", TestPlugin);
register_partitioned_plugin_transform!("test", "partitioned_transform", TestPlugin);
register_partitioned_plugin_sink!("test", "partitioned_sink", TestPlugin);
init_plugin!();

fn no_options() -> PluginOptions {
    PluginOptions::new(HashMap::new())
}

fn input_schema() -> ROption<SafeArrowSchema> {
    RSome(schema().into())
}

fn test_channels() -> PluginChannels {
    PluginChannels {
        input: PluginChannel::new(crossbeam_channel::bounded(8)),
        output: PluginChannel::new(crossbeam_channel::bounded(8)),
        metrics: PluginMetricsChannel::new(crossbeam_channel::bounded(64)),
    }
}

fn state_backend_config() -> PluginStateBackendConfig {
    PluginStateBackendConfig::new(
        "app".to_string(),
        "node".to_string(),
        r#"{"backend_type":"InMemory","postgres":null,"sqlite":null}"#.to_string(),
    )
}

fn context(partition_index: u32, partition_count: u32) -> PluginInstanceContext {
    PluginInstanceContext {
        reference_name: "node".into(),
        partition_index,
        partition_count,
    }
}

fn runtime() -> PluginAsyncRuntimeObj {
    DirectTokioProxy::new().into_async_runtime_obj()
}

/// Terminates the created instance and returns the partition it reported.
async fn reported_partition(
    result: RResult<PluginResult, PluginInitializationError>,
    channels: &PluginChannels,
) -> String {
    let result = result.unwrap();
    let partition = result
        .labels
        .iter()
        .find(|l| l.key.as_str() == PARTITION_LABEL)
        .map(|l| l.value.to_string())
        .expect("instance must report its partition");
    channels
        .input
        .sender
        .send(NonExhaustive::new(PluginMsg::Terminate))
        .unwrap();
    assert!(matches!(result.execution_future.await, RResult::ROk(())));
    partition
}

#[test]
fn legacy_and_unknown_ids_describe_as_single_stream() {
    let describe = get_module().describe_partitioned().unwrap();

    assert!(matches!(
        describe(LEGACY_SOURCE.into(), RNone, no_options()),
        RResult::ROk(RNone)
    ));
    assert!(matches!(
        describe("test.unknown".into(), RNone, no_options()),
        RResult::ROk(RNone)
    ));
}

#[test]
fn partitioned_ids_describe_their_capability() {
    let describe = get_module().describe_partitioned().unwrap();

    let source = describe(PARTITIONED_SOURCE.into(), RNone, no_options())
        .unwrap()
        .unwrap();
    assert_eq!(source.partition_count.preferred, RSome(3));
    assert!(source.input_placement.is_none());

    let transform = describe(PARTITIONED_TRANSFORM.into(), input_schema(), no_options())
        .unwrap()
        .unwrap();
    assert_eq!(
        transform.input_placement.unwrap().into_enum().unwrap(),
        PluginInputPlacement::ByPrimaryKey
    );

    let sink = describe(PARTITIONED_SINK.into(), input_schema(), no_options())
        .unwrap()
        .unwrap();
    assert_eq!(
        sink.input_placement.unwrap().into_enum().unwrap(),
        PluginInputPlacement::RoundRobin
    );
    assert!(sink.output_schema.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn create_partitioned_rejects_legacy_ids() {
    let create_partitioned = get_module().create_partitioned().unwrap();

    let result = create_partitioned(
        LEGACY_SOURCE.into(),
        RNone,
        no_options(),
        context(0, 2),
        runtime(),
        state_backend_config(),
        test_channels(),
    );

    assert!(matches!(
        result,
        RResult::RErr(PluginInitializationError::NotImplemented)
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn create_partitioned_dispatches_each_kind_with_its_context() {
    let create_partitioned = get_module().create_partitioned().unwrap();

    for (id, input) in [
        (PARTITIONED_SOURCE, RNone),
        (PARTITIONED_TRANSFORM, input_schema()),
        (PARTITIONED_SINK, input_schema()),
    ] {
        let channels = test_channels();
        let result = create_partitioned(
            RString::from(id),
            input,
            no_options(),
            context(1, 2),
            runtime(),
            state_backend_config(),
            channels.clone(),
        );
        assert_eq!(reported_partition(result, &channels).await, "1/2", "{id}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn create_refuses_partitioned_ids() {
    let create = get_module().create();

    for (id, input) in [
        (PARTITIONED_SOURCE, RNone),
        (PARTITIONED_TRANSFORM, input_schema()),
        (PARTITIONED_SINK, input_schema()),
    ] {
        let result = create(
            RString::from(id),
            input,
            no_options(),
            runtime(),
            state_backend_config(),
            test_channels(),
        );
        match result {
            RResult::RErr(PluginInitializationError::Configuration(message)) => {
                assert!(message.contains(id), "{message}")
            }
            RResult::RErr(other) => panic!("{id}: expected a configuration error, got {other:?}"),
            RResult::ROk(_) => panic!("{id} must only be created through `create_partitioned`"),
        }
    }
}

#[test]
fn init_lists_every_registration() {
    let configuration = (get_module().init())(PluginLogging::Plain).unwrap();
    let mut ids: Vec<String> = configuration
        .plugin_ids
        .iter()
        .map(|id| id.to_string())
        .collect();
    ids.sort();

    assert_eq!(
        ids,
        [
            LEGACY_SOURCE,
            PARTITIONED_SINK,
            PARTITIONED_SOURCE,
            PARTITIONED_TRANSFORM
        ]
    );
}
