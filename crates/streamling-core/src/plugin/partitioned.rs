//! Partition-capable plugins: described while the topology is built, then
//! instantiated once per physical stream during physical planning, when the
//! stream count is known.

use super::{
    COLUMN_NAME_OP, ExecutionFuture, InstanceExit, PLUGIN_INSTANCE_REGISTRY, PluginChannels,
    PluginId, PluginOptions, collect_labels, create_channels_for_plugin,
    create_plugin_async_runtime, create_plugin_state_backend_config, register_plugin_instance,
    require_plugin, terminate_plugins, track_exit,
};
use crate::app_config::AppConfig;
use crate::error::{Result, StreamlingError};
use crate::telemetry::provider::metric_key;
use crate::telemetry::recorder::merge_metadata_tags;
use crate::{streamling_err, streamling_user_bail};
use abi_stable::traits::{IntoReprC, IntoReprRust};
use arrow_schema::SchemaRef;
use futures::FutureExt;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
pub use streamling_plugin::InputPlacement;
use streamling_plugin::{
    PluginInputPlacement, PluginInputPlacement_NE, PluginInstanceContext, PluginPartitionCount,
    partition_instance_name,
};
use tokio::runtime::Handle;
use tracing::warn;

/// Bounds the wait for already-created instances to exit when a later
/// partition of the same node fails to construct. They were never sent
/// `Init`, so they exit as soon as they see `Terminate`.
const ROLLBACK_DRAIN_BOUND: Duration = Duration::from_secs(5);

/// The kind of node a plugin runs as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PluginKind {
    Source,
    Transform,
    Sink,
}

impl fmt::Display for PluginKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            PluginKind::Source => "source",
            PluginKind::Transform => "transform",
            PluginKind::Sink => "sink",
        })
    }
}

/// The validated partition-count constraints a plugin declared.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionRange {
    minimum: usize,
    maximum: Option<usize>,
    preferred: Option<usize>,
}

impl TryFrom<PluginPartitionCount> for PartitionRange {
    type Error = StreamlingError;

    fn try_from(count: PluginPartitionCount) -> Result<Self> {
        PartitionRange::new(
            count.minimum as usize,
            count.maximum.into_option().map(|m| m as usize),
            count.preferred.into_option().map(|p| p as usize),
        )
    }
}

impl PartitionRange {
    pub fn new(minimum: usize, maximum: Option<usize>, preferred: Option<usize>) -> Result<Self> {
        let range = PartitionRange {
            minimum,
            maximum,
            preferred,
        };
        if range.minimum == 0 {
            return Err(streamling_err!("the minimum partition count is zero"));
        }
        if range.maximum.is_some_and(|maximum| maximum < range.minimum) {
            return Err(streamling_err!(
                "the maximum partition count is below the minimum ({range})"
            ));
        }
        if let Some(preferred) = range.preferred
            && !range.supports(preferred)
        {
            return Err(streamling_err!(
                "the preferred partition count {preferred} is outside the supported range ({range})"
            ));
        }
        Ok(range)
    }
}

impl fmt::Display for PartitionRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.maximum {
            Some(maximum) if maximum == self.minimum => write!(f, "exactly {maximum}"),
            Some(maximum) => write!(f, "between {} and {maximum}", self.minimum),
            None => write!(f, "at least {}", self.minimum),
        }
    }
}

impl PartitionRange {
    pub fn supports(&self, partitions: usize) -> bool {
        partitions >= self.minimum && self.maximum.is_none_or(|maximum| partitions <= maximum)
    }

    /// Fails unless node `node` can run with `partitions` partitions.
    /// `reason` completes "but {reason}" in the error.
    pub fn check(&self, partitions: usize, node: &str, reason: &str) -> Result<()> {
        if !self.supports(partitions) {
            streamling_user_bail!("plugin '{node}' supports {self} partitions, but {reason}");
        }
        Ok(())
    }

    /// A source's width: its explicit `parallelism`, else the plugin's
    /// preferred count, else 1.
    pub fn source_width(&self, parallelism: Option<usize>, node: &str) -> Result<usize> {
        let (width, reason) = match (parallelism, self.preferred) {
            (Some(parallelism), _) => (parallelism, format!("its parallelism is {parallelism}")),
            (None, Some(preferred)) => (
                preferred,
                format!("its preferred partition count is {preferred}"),
            ),
            (None, None) => (1, "without `parallelism` it runs 1".to_string()),
        };
        self.check(width, node, &reason)?;
        Ok(width)
    }
}

/// One running instance of a plugin node.
#[derive(Clone, Debug)]
pub struct PluginInstance {
    /// Unique per instance: keys the instance registry (which delivers
    /// `Terminate`) and the shutdown diagnostics. `{reference_name}[{i}]`
    /// for a partition instance, the reference name for a single-stream one.
    pub key: String,
    pub channels: Arc<PluginChannels>,
    pub exit: InstanceExit,
}

/// The instances behind one plugin node.
#[derive(Clone, Debug)]
pub enum PluginInstances {
    /// A single-stream plugin, created while the topology was built. Its
    /// input is always narrowed to one stream.
    Single(PluginInstance),
    /// A partition-capable plugin, instantiated once its width is known.
    Partitioned(Arc<PartitionedPlugin>),
}

impl PluginInstances {
    /// The instances serving `partitions` streams: the single-stream plugin's
    /// one instance, or one instance per partition.
    pub async fn resolve(
        &self,
        partitions: usize,
        input_schema: Option<SchemaRef>,
    ) -> Result<Vec<PluginInstance>> {
        match self {
            PluginInstances::Single(instance) => Ok(vec![instance.clone()]),
            PluginInstances::Partitioned(plugin) => {
                plugin.instantiate(partitions, input_schema).await
            }
        }
    }
}

/// A partition-capable plugin node: what it declared at planning time, and
/// the instances created for it once its physical width was known.
pub struct PartitionedPlugin {
    kind: PluginKind,
    plugin_type: PluginId,
    reference_name: String,
    options: HashMap<String, String>,
    output_schema: Option<SchemaRef>,
    /// Sorted, so instances can be compared with the description.
    labels: Vec<(String, String)>,
    input_placement: Option<InputPlacement>,
    partitions: PartitionRange,
    app_config: AppConfig,
    /// Created at most once: shared scans and multiple consumers plan a node
    /// more than once, but it runs one instance per partition.
    instances: tokio::sync::Mutex<Option<Vec<PluginInstance>>>,
    /// Taken by the run loop so it can await every instance's dispatcher.
    execution_futures: Mutex<Vec<(String, ExecutionFuture)>>,
}

impl fmt::Debug for PartitionedPlugin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionedPlugin")
            .field("kind", &self.kind)
            .field("plugin_type", &self.plugin_type)
            .field("reference_name", &self.reference_name)
            .field("partitions", &self.partitions)
            .finish()
    }
}

fn input_placement(placement: PluginInputPlacement_NE) -> Result<InputPlacement> {
    match placement.as_enum() {
        Ok(PluginInputPlacement::ByPrimaryKey) => Ok(InputPlacement::ByPrimaryKey),
        Ok(PluginInputPlacement::ByColumns { columns }) => Ok(InputPlacement::ByColumns(
            columns.iter().map(|c| c.to_string()).collect(),
        )),
        Ok(PluginInputPlacement::RoundRobin) => Ok(InputPlacement::RoundRobin),
        Err(_) => Err(streamling_err!(
            "it declares an input placement this version of streamling does not support"
        )),
    }
}

impl PartitionedPlugin {
    /// Describes a plugin without creating it. `None` means the plugin only
    /// runs single-stream, through `create`: its library predates
    /// partitioning, or it was registered as a single-stream plugin.
    ///
    /// Merges the declared labels into the node's metric metadata, as
    /// creating a single-stream plugin does.
    pub fn describe(
        app_config: &AppConfig,
        reference_name: &str,
        plugin_type: &str,
        kind: PluginKind,
        input_schema: Option<SchemaRef>,
        options: HashMap<String, String>,
    ) -> Result<Option<Arc<Self>>> {
        let plugin_type: PluginId = plugin_type.to_string().into();
        let module = require_plugin(&plugin_type)?;
        let (Some(describe), Some(_)) =
            (module.describe_partitioned(), module.create_partitioned())
        else {
            return Ok(None);
        };

        let description = describe(
            plugin_type.to_string().into_c(),
            input_schema.map(Into::into).into_c(),
            PluginOptions::new(options.clone()),
        )
        .into_rust()
        .map_err(|e| streamling_err!("Plugin description failed: {:?}", e))?;
        let Some(description) = description.into_option() else {
            return Ok(None);
        };

        let invalid = |problem: StreamlingError| {
            streamling_err!(
                "{kind} plugin '{plugin_type}' returned an invalid description: {problem}"
            )
        };
        let output_schema: Option<SchemaRef> =
            description.output_schema.into_option().map(Into::into);
        match (kind, &output_schema) {
            (PluginKind::Source | PluginKind::Transform, None) => {
                return Err(invalid(streamling_err!("it has no output schema")));
            }
            (PluginKind::Sink, Some(_)) => {
                return Err(invalid(streamling_err!("a sink has no output schema")));
            }
            _ => {}
        }
        if let Some(schema) = &output_schema
            && schema.field_with_name(COLUMN_NAME_OP).is_err()
        {
            streamling_user_bail!("Output schema must contain the column '{}'", COLUMN_NAME_OP);
        }
        let input_placement = description
            .input_placement
            .into_option()
            .map(input_placement)
            .transpose()
            .map_err(invalid)?;
        match (kind, &input_placement) {
            (PluginKind::Source, Some(_)) => {
                return Err(invalid(streamling_err!("a source has no input to place")));
            }
            (PluginKind::Transform | PluginKind::Sink, None) => {
                return Err(invalid(streamling_err!("it has no input placement")));
            }
            (_, Some(InputPlacement::ByColumns(columns))) if columns.is_empty() => {
                return Err(invalid(streamling_err!(
                    "it places its input by an empty column list"
                )));
            }
            _ => {}
        }
        let partitions = PartitionRange::try_from(description.partition_count).map_err(invalid)?;
        let mut labels = collect_labels(description.labels);
        labels.sort();

        merge_metadata_tags(
            &metric_key(&app_config.application_id, reference_name),
            labels.clone(),
        );
        Ok(Some(Arc::new(PartitionedPlugin {
            kind,
            plugin_type,
            reference_name: reference_name.to_string(),
            options,
            output_schema,
            labels,
            input_placement,
            partitions,
            app_config: app_config.clone(),
            instances: tokio::sync::Mutex::new(None),
            execution_futures: Mutex::new(Vec::new()),
        })))
    }

    pub fn output_schema(&self) -> Option<SchemaRef> {
        self.output_schema.clone()
    }

    pub fn input_placement(&self) -> Option<&InputPlacement> {
        self.input_placement.as_ref()
    }

    pub fn partitions(&self) -> PartitionRange {
        self.partitions
    }

    /// Fails unless this node can run `partitions` partitions; `reason`
    /// completes "but {reason}" in the error.
    pub fn check_width(&self, partitions: usize, reason: &str) -> Result<()> {
        self.partitions
            .check(partitions, &self.reference_name, reason)
    }

    /// Creates one instance per partition, or returns the ones already
    /// created for this node. If any partition fails to construct or does not
    /// match the description, the instances created before it are terminated
    /// and drained, and the node has none.
    pub async fn instantiate(
        &self,
        partitions: usize,
        input_schema: Option<SchemaRef>,
    ) -> Result<Vec<PluginInstance>> {
        let mut created = self.instances.lock().await;
        if let Some(instances) = created.as_ref() {
            if instances.len() != partitions {
                return Err(streamling_err!(
                    "plugin '{}' already runs {} partitions; it cannot also run {}",
                    self.reference_name,
                    instances.len(),
                    partitions
                ));
            }
            return Ok(instances.clone());
        }
        self.check_width(partitions, &format!("it is planned to run {partitions}"))?;

        let mut instances = Vec::with_capacity(partitions);
        let mut futures = Vec::with_capacity(partitions);
        for index in 0..partitions {
            let result = self.create_instance(index, partitions, input_schema.clone());
            let created_instance = match result {
                Ok(created_instance) => created_instance,
                Err(e) => {
                    roll_back(instances, futures).await;
                    return Err(e);
                }
            };
            let mismatch = self.mismatch(&created_instance);
            instances.push(created_instance.instance);
            futures.push(created_instance.execution);
            if let Err(e) = mismatch {
                roll_back(instances, futures).await;
                return Err(e);
            }
        }

        self.execution_futures
            .lock()
            .expect("execution futures lock poisoned")
            .extend(futures);
        *created = Some(instances.clone());
        Ok(instances)
    }

    /// The execution futures of every instance created so far, each paired
    /// with its instance key. Taken once, by the run loop.
    pub fn take_execution_futures(&self) -> Vec<(String, ExecutionFuture)> {
        std::mem::take(
            &mut *self
                .execution_futures
                .lock()
                .expect("execution futures lock poisoned"),
        )
    }

    fn create_instance(
        &self,
        index: usize,
        partitions: usize,
        input_schema: Option<SchemaRef>,
    ) -> Result<CreatedInstance> {
        let key = partition_instance_name(&self.reference_name, index as u32);
        let create = require_plugin(&self.plugin_type)?
            .create_partitioned()
            .ok_or_else(|| {
                streamling_err!(
                    "plugin '{}' can no longer create partition instances",
                    self.plugin_type
                )
            })?;
        let channels = create_channels_for_plugin(&self.app_config, &self.plugin_type);
        let result = create(
            self.plugin_type.to_string().into_c(),
            input_schema.map(Into::into).into_c(),
            PluginOptions::new(self.options.clone()),
            PluginInstanceContext {
                reference_name: self.reference_name.as_str().into(),
                partition_index: index as u32,
                partition_count: partitions as u32,
            },
            create_plugin_async_runtime(Handle::current()),
            create_plugin_state_backend_config(&self.app_config, &self.reference_name),
            channels.clone(),
        )
        .into_rust()
        .map_err(|e| streamling_err!("Plugin creation failed for {key}: {:?}", e))?;

        register_plugin_instance(&key, channels.clone());
        let (execution, exit) = track_exit(Box::pin(
            result
                .execution_future
                .map(|r| r.into_rust().map_err(|msg| msg.into_string())),
        ));
        let mut labels = collect_labels(result.labels);
        labels.sort();
        Ok(CreatedInstance {
            instance: PluginInstance {
                key: key.clone(),
                channels: Arc::new(channels),
                exit,
            },
            execution: (key, execution),
            output_schema: result.output_schema.into_option().map(Into::into),
            labels,
        })
    }

    /// Every instance must produce what the node described: downstream nodes
    /// were planned against that schema, and its labels are already merged
    /// into the node's metrics.
    fn mismatch(&self, created: &CreatedInstance) -> Result<()> {
        let key = &created.instance.key;
        let fields = |schema: &Option<SchemaRef>| schema.as_ref().map(|s| s.fields().clone());
        if fields(&created.output_schema) != fields(&self.output_schema) {
            return Err(streamling_err!(
                "plugin instance {key} produces a different output schema than its node described"
            ));
        }
        if created.labels != self.labels {
            return Err(streamling_err!(
                "plugin instance {key} declares different labels ({:?}) than its node described ({:?})",
                created.labels,
                self.labels
            ));
        }
        Ok(())
    }
}

struct CreatedInstance {
    instance: PluginInstance,
    execution: (String, ExecutionFuture),
    output_schema: Option<SchemaRef>,
    labels: Vec<(String, String)>,
}

/// Terminates instances created before a failure and waits, bounded, for
/// them to exit.
async fn roll_back(instances: Vec<PluginInstance>, futures: Vec<(String, ExecutionFuture)>) {
    {
        let mut registry = PLUGIN_INSTANCE_REGISTRY
            .write()
            .expect("plugin instance registry lock poisoned");
        for instance in &instances {
            registry.remove(&instance.key);
        }
    }
    let to_terminate = instances
        .iter()
        .map(|i| (i.key.clone(), (*i.channels).clone()))
        .collect();
    if let Err(e) = terminate_plugins(to_terminate, Some(ROLLBACK_DRAIN_BOUND)) {
        warn!("Failed to terminate rolled-back plugin instances: {e}");
    }
    let drained = futures::future::join_all(futures.into_iter().map(|(key, future)| async move {
        if let Err(e) = future.await {
            warn!("Rolled-back plugin instance {key} exited with an error: {e}");
        }
    }));
    if tokio::time::timeout(ROLLBACK_DRAIN_BOUND, drained)
        .await
        .is_err()
    {
        warn!(
            "Rolled-back plugin instances did not exit within {:?}",
            ROLLBACK_DRAIN_BOUND
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::test_plugins::{
        self, FAIL_CREATE_AT, LEGACY_SINK, MAXIMUM, MINIMUM, MISLABEL_AT, NODE, PREFERRED, SINK,
        SOURCE, log,
    };
    use abi_stable::std_types::{RNone, RSome};
    use streamling_plugin::PluginPartitionCount;

    fn options(node: &str, extra: &[(&str, &str)]) -> HashMap<String, String> {
        extra
            .iter()
            .chain(&[(NODE, node)])
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn app_config() -> AppConfig {
        AppConfig::load().expect("embedded config must load")
    }

    fn describe_sink(node: &str, extra: &[(&str, &str)]) -> Arc<PartitionedPlugin> {
        test_plugins::install();
        PartitionedPlugin::describe(
            &app_config(),
            node,
            SINK,
            PluginKind::Sink,
            Some(test_plugins::source_schema()),
            options(node, extra),
        )
        .unwrap()
        .expect("the test sink is partition-capable")
    }

    fn keys(instances: &[PluginInstance]) -> Vec<String> {
        instances.iter().map(|i| i.key.clone()).collect()
    }

    fn registered(key: &str) -> bool {
        PLUGIN_INSTANCE_REGISTRY.read().unwrap().contains_key(key)
    }

    /// Sends Terminate through the instance registry, the way teardown does,
    /// and waits for every dispatcher to exit.
    async fn terminate_and_drain(plugin: &PartitionedPlugin, instances: &[PluginInstance]) {
        let entries: Vec<(String, PluginChannels)> = {
            let mut registry = PLUGIN_INSTANCE_REGISTRY.write().unwrap();
            instances
                .iter()
                .map(|i| (i.key.clone(), registry.remove(&i.key).unwrap()))
                .collect()
        };
        terminate_plugins(entries, None).unwrap();
        for (key, future) in plugin.take_execution_futures() {
            future
                .await
                .unwrap_or_else(|e| panic!("instance {key} failed: {e}"));
        }
    }

    fn range(minimum: u32, maximum: Option<u32>, preferred: Option<u32>) -> Result<PartitionRange> {
        PartitionRange::try_from(PluginPartitionCount {
            minimum,
            maximum: maximum.map_or(RNone, RSome),
            preferred: preferred.map_or(RNone, RSome),
        })
    }

    #[test]
    fn invalid_partition_counts_are_rejected() {
        assert!(range(0, None, None).is_err(), "zero minimum");
        assert!(range(4, Some(2), None).is_err(), "maximum below minimum");
        assert!(
            range(2, Some(4), Some(1)).is_err(),
            "preferred below minimum"
        );
        assert!(
            range(2, Some(4), Some(5)).is_err(),
            "preferred above maximum"
        );
        assert!(range(2, Some(4), Some(3)).is_ok());
    }

    #[test]
    fn source_width_prefers_explicit_then_preferred_then_one() {
        let preferring_three = range(1, Some(8), Some(3)).unwrap();
        assert_eq!(preferring_three.source_width(Some(5), "src").unwrap(), 5);
        assert_eq!(preferring_three.source_width(None, "src").unwrap(), 3);
        assert_eq!(
            range(1, None, None)
                .unwrap()
                .source_width(None, "src")
                .unwrap(),
            1
        );
    }

    #[test]
    fn source_width_outside_the_range_is_rejected() {
        let err = range(2, Some(4), None)
            .unwrap()
            .source_width(Some(8), "src")
            .unwrap_err();
        assert!(err.to_string().contains("between 2 and 4"), "{err}");

        let err = range(2, None, None)
            .unwrap()
            .source_width(None, "src")
            .unwrap_err();
        assert!(
            err.to_string().contains("at least 2"),
            "the fallback width of 1 must be validated too: {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn single_stream_ids_describe_as_none() {
        test_plugins::install();
        let described = PartitionedPlugin::describe(
            &app_config(),
            "legacy",
            LEGACY_SINK,
            PluginKind::Sink,
            Some(test_plugins::source_schema()),
            options("legacy", &[]),
        )
        .unwrap();
        assert!(described.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn description_reports_the_declared_constraints() {
        test_plugins::install();
        let source = PartitionedPlugin::describe(
            &app_config(),
            "described_source",
            SOURCE,
            PluginKind::Source,
            None,
            options("described_source", &[(PREFERRED, "3")]),
        )
        .unwrap()
        .unwrap();

        assert_eq!(source.partitions().source_width(None, "src").unwrap(), 3);
        assert!(source.input_placement().is_none());
        assert_eq!(
            source.output_schema().unwrap().fields(),
            test_plugins::source_schema().fields()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn creates_one_instance_per_partition() {
        let sink = describe_sink("one_per_partition", &[]);

        let instances = sink
            .instantiate(3, Some(test_plugins::source_schema()))
            .await
            .unwrap();

        assert_eq!(
            keys(&instances),
            [
                "one_per_partition[0]",
                "one_per_partition[1]",
                "one_per_partition[2]"
            ]
        );
        for (index, key) in keys(&instances).iter().enumerate() {
            let context = log(key).context.expect("instance was created");
            assert_eq!(context.reference_name.as_str(), "one_per_partition");
            assert_eq!(context.partition_index as usize, index);
            assert_eq!(context.partition_count, 3);
            assert!(registered(key), "{key} must be in the instance registry");
        }
        terminate_and_drain(&sink, &instances).await;
        for key in keys(&instances) {
            assert!(log(&key).terminated, "{key} must receive Terminate");
        }
    }

    /// Shared scans and multiple consumers plan a node more than once; the
    /// node must still run exactly one instance per partition.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn instances_are_created_once_per_node() {
        let sink = describe_sink("created_once", &[]);
        let schema = Some(test_plugins::source_schema());

        let first = sink.instantiate(2, schema.clone()).await.unwrap();
        let second = sink.instantiate(2, schema.clone()).await.unwrap();

        assert_eq!(keys(&first), keys(&second));
        assert_eq!(sink.take_execution_futures().len(), 2);
        assert!(
            sink.instantiate(3, schema).await.is_err(),
            "one node cannot run at two widths"
        );
        terminate_and_drain(&sink, &first).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn unsupported_width_is_rejected() {
        let sink = describe_sink("too_wide", &[(MINIMUM, "2"), (MAXIMUM, "2")]);

        let err = sink
            .instantiate(3, Some(test_plugins::source_schema()))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("exactly 2"), "{err}");
        assert!(sink.take_execution_futures().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn failed_partition_rolls_back_created_instances() {
        let sink = describe_sink("rolled_back", &[(FAIL_CREATE_AT, "2")]);

        let err = sink
            .instantiate(4, Some(test_plugins::source_schema()))
            .await
            .unwrap_err();

        assert!(
            err.to_string().contains("creation failed on purpose"),
            "{err}"
        );
        for key in ["rolled_back[0]", "rolled_back[1]"] {
            assert!(log(key).terminated, "{key} must be terminated");
            assert!(!registered(key), "{key} must leave the instance registry");
        }
        assert!(sink.take_execution_futures().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn instance_must_match_the_description() {
        let sink = describe_sink("mislabeled", &[(MISLABEL_AT, "1")]);

        let err = sink
            .instantiate(2, Some(test_plugins::source_schema()))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("labels"), "{err}");
        assert!(
            log("mislabeled[0]").terminated,
            "the first instance is rolled back"
        );
        assert!(
            log("mislabeled[1]").terminated,
            "the mismatched instance is rolled back"
        );
    }
}
