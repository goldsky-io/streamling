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
const STATE_LABEL: &str = "state";
const MINIMUM_PARTITIONS_OPTION: &str = "minimum_partitions";
const PARALLELISM_OPTION: &str = "parallelism";

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("_gs_op", DataType::Utf8, false),
    ]))
}

/// One type serves every registration here: it reports the partition it was
/// created for, and the key of its `create()` state, as labels, so the tests
/// can see which context reached it.
struct TestPlugin {
    context: Option<PluginInstanceContext>,
    state_key: Option<String>,
}

impl TestPlugin {
    fn new(
        _: PluginAsyncRuntimeObj,
        _: PluginStateBackendFactory,
        _: PluginMetricsRecorder,
        _: HashMap<String, String>,
    ) -> Self {
        TestPlugin {
            context: None,
            state_key: None,
        }
    }

    fn partitioned(
        context: PluginInstanceContext,
        state: PluginStateBackendFactory,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        // A partitioned host never passes `parallelism`, so neither may the
        // single-stream fallback.
        if options.contains_key(PARALLELISM_OPTION) {
            return Err(PluginInitializationError::Configuration(
                "saw the parallelism option".into(),
            ));
        }
        Ok(TestPlugin {
            context: Some(context),
            state_key: Some(format!("{:?}", state.create::<u64>())),
        })
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
            .chain(
                self.state_key
                    .iter()
                    .map(|key| PluginLabel::new(STATE_LABEL, key.clone())),
            )
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
        options: &HashMap<String, String>,
    ) -> Result<SourceDescription, streamling_plugin::PluginInitializationError> {
        Ok(SourceDescription {
            output_schema: schema(),
            labels: Vec::new(),
            partition_count: PartitionCount {
                minimum: options
                    .get(MINIMUM_PARTITIONS_OPTION)
                    .map_or(1, |m| m.parse().unwrap()),
                preferred: Some(3),
                ..PartitionCount::default()
            },
        })
    }

    fn create(
        context: PluginInstanceContext,
        _: PluginAsyncRuntimeObj,
        state: PluginStateBackendFactory,
        _: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, streamling_plugin::PluginInitializationError> {
        TestPlugin::partitioned(context, state, options)
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
        state: PluginStateBackendFactory,
        _: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, streamling_plugin::PluginInitializationError> {
        TestPlugin::partitioned(context, state, options)
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
        state: PluginStateBackendFactory,
        _: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, streamling_plugin::PluginInitializationError> {
        TestPlugin::partitioned(context, state, options)
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

fn options(entries: &[(&str, &str)]) -> PluginOptions {
    PluginOptions::new(
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    )
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

/// Terminates the created instance and returns the labels it reported.
async fn reported_labels(
    result: RResult<PluginResult, PluginInitializationError>,
    channels: &PluginChannels,
) -> HashMap<String, String> {
    let result = result.unwrap();
    let labels = result
        .labels
        .iter()
        .map(|l| (l.key.to_string(), l.value.to_string()))
        .collect();
    channels
        .input
        .sender
        .send(NonExhaustive::new(PluginMsg::Terminate))
        .unwrap();
    assert!(matches!(result.execution_future.await, RResult::ROk(())));
    labels
}

/// The configuration error a creation failed with.
fn configuration_error(result: RResult<PluginResult, PluginInitializationError>) -> String {
    match result {
        RResult::RErr(PluginInitializationError::Configuration(message)) => message.to_string(),
        RResult::RErr(other) => panic!("expected a configuration error, got {other:?}"),
        RResult::ROk(_) => panic!("expected a configuration error, got an instance"),
    }
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
        assert_eq!(
            reported_labels(result, &channels).await[PARTITION_LABEL],
            "1/2",
            "{id}"
        );
    }
}

/// Only a host that predates partitioned plugins calls `create` for a
/// partition-aware id. It must get what a partitioned host runs at width 1,
/// down to the state keys, so neither engine moves the plugin's state.
#[tokio::test(flavor = "multi_thread")]
async fn create_runs_partitioned_ids_as_partition_0_of_1() {
    let module = get_module();
    let (create, create_partitioned) = (module.create(), module.create_partitioned().unwrap());

    for (id, has_input) in [
        (PARTITIONED_SOURCE, false),
        (PARTITIONED_TRANSFORM, true),
        (PARTITIONED_SINK, true),
    ] {
        let input = || if has_input { input_schema() } else { RNone };
        let channels = test_channels();
        let single_stream = reported_labels(
            create(
                RString::from(id),
                input(),
                no_options(),
                runtime(),
                state_backend_config(),
                channels.clone(),
            ),
            &channels,
        )
        .await;
        let channels = test_channels();
        let width_one = reported_labels(
            create_partitioned(
                RString::from(id),
                input(),
                no_options(),
                context(0, 1),
                runtime(),
                state_backend_config(),
                channels.clone(),
            ),
            &channels,
        )
        .await;

        assert_eq!(single_stream[PARTITION_LABEL], "0/1", "{id}");
        assert!(
            single_stream[STATE_LABEL].contains(r#"reference_name: "node[0]""#),
            "{id}: {single_stream:?}"
        );
        assert_eq!(single_stream, width_one, "{id}");
    }
}

/// `parallelism` reaches a plugin as an option only on a host that predates
/// partitioned plugins, so it means a width that host cannot run.
#[tokio::test(flavor = "multi_thread")]
async fn create_refuses_a_width_one_stream_cannot_honor() {
    let create = get_module().create();

    for width in ["2", "0", "many"] {
        let message = configuration_error(create(
            PARTITIONED_SOURCE.into(),
            RNone,
            options(&[(PARALLELISM_OPTION, width)]),
            runtime(),
            state_backend_config(),
            test_channels(),
        ));
        assert!(
            message.contains(PARTITIONED_SOURCE)
                && message.contains(&format!("parallelism {width}")),
            "{message}"
        );
    }

    let message = configuration_error(create(
        PARTITIONED_SOURCE.into(),
        RNone,
        options(&[(MINIMUM_PARTITIONS_OPTION, "2")]),
        runtime(),
        state_backend_config(),
        test_channels(),
    ));
    assert!(message.contains("at least 2 partitions"), "{message}");
}

#[tokio::test(flavor = "multi_thread")]
async fn create_accepts_parallelism_1_without_passing_it_on() {
    let channels = test_channels();
    let result = (get_module().create())(
        PARTITIONED_TRANSFORM.into(),
        input_schema(),
        options(&[(PARALLELISM_OPTION, "1")]),
        runtime(),
        state_backend_config(),
        channels.clone(),
    );

    assert_eq!(
        reported_labels(result, &channels).await[PARTITION_LABEL],
        "0/1"
    );
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
