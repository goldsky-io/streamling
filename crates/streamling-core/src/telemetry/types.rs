use crate::node_context::NodeContext;
use crate::telemetry::provider::is_test_metrics_mode;
use std::fmt;
use std::fmt::Debug;

use datafusion::physical_plan::metrics::{Count, Gauge as DfGauge, MetricValue, Time};
use opentelemetry::KeyValue;
use serde_json::to_string;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};

use std::sync::OnceLock;
use std::sync::RwLock;
use std::time::Duration;

/// metric tags that will be applied to all metrics
static GLOBAL_METRIC_TAGS: OnceLock<RwLock<HashMap<String, Vec<KeyValue>>>> = OnceLock::new();

pub fn get_global_metric_tags(service_instance_id: &str) -> Vec<KeyValue> {
    let global_tags = GLOBAL_METRIC_TAGS
        .get_or_init(|| RwLock::new(HashMap::new()))
        .read()
        .unwrap();
    match global_tags.get(service_instance_id) {
        Some(tags) => tags.clone(),
        None => vec![],
    }
}
pub fn set_global_metric_tags(service_instance_id: &str, attributes: Vec<KeyValue>) {
    let mut global_tags = GLOBAL_METRIC_TAGS
        .get_or_init(|| RwLock::new(HashMap::new()))
        .write()
        .unwrap();
    global_tags.insert(service_instance_id.to_string(), attributes);
}

pub use crate::node_context::TopologyNodeType;

pub fn create_time_from_duration(duration: Duration) -> Time {
    let time = Time::new();
    time.add_duration(duration);
    time
}

pub fn create_count_with_value(value: usize) -> Count {
    let count = Count::new();
    count.add(value);
    count
}

pub fn create_gauge_with_value(value: usize) -> DfGauge {
    let gauge = DfGauge::new();
    gauge.set(value);
    gauge
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Hash, Eq)]
pub struct PipelineMetricMetadata {
    pub node_context: NodeContext,
    pub service_instance_id: String,
    // BTreeMap because this struct is embedded in LogicalNodes like ExternalHandlerNode
    // that implement traits like PartialEq, PartialOrd which HashMap cannot satisfy
    pub additional_tags: BTreeMap<String, String>,
    pub children_metadata_ids: Vec<String>,
}

impl fmt::Display for PipelineMetricMetadata {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}.{}.{}",
            self.node_context.node_type,
            self.node_context.operator_type,
            to_string(&self.additional_tags).unwrap()
        )
    }
}

#[derive(Debug, Clone)]
pub struct MetricData<'a> {
    pub tags: Cow<'a, HashMap<String, String>>,
    pub metric_values: Vec<MetricValue>,
}

impl<'a> MetricData<'a> {
    pub fn new(metric_values: Vec<MetricValue>, tags: Cow<'a, HashMap<String, String>>) -> Self {
        Self {
            tags,
            metric_values,
        }
    }

    pub fn new_with_borrowed_tags(
        metric_values: Vec<MetricValue>,
        tags: &'a HashMap<String, String>,
    ) -> Self {
        Self {
            tags: Cow::Borrowed(tags),
            metric_values,
        }
    }

    pub fn new_with_owned_tags(
        metric_values: Vec<MetricValue>,
        tags: HashMap<String, String>,
    ) -> Self {
        Self {
            tags: Cow::Owned(tags),
            metric_values,
        }
    }

    pub fn new_with_metric_value_and_tags(
        metric_value: MetricValue,
        tags: &'a HashMap<String, String>,
    ) -> Self {
        Self {
            tags: Cow::Borrowed(tags),
            metric_values: vec![metric_value],
        }
    }

    pub fn new_with_metric_value_additional_tags(
        metric_value: MetricValue,
        metric_metadata_tags: &'a HashMap<String, String>,
        additional_tags: Vec<(&str, &str)>,
    ) -> MetricData<'a> {
        if additional_tags.is_empty() {
            MetricData::new_with_borrowed_tags(vec![metric_value], metric_metadata_tags)
        } else {
            let mut all_tags = metric_metadata_tags.clone();
            additional_tags.iter().for_each(|tag| {
                all_tags.insert(String::from(tag.0), String::from(tag.1));
            });
            MetricData::new_with_owned_tags(vec![metric_value], all_tags)
        }
    }

    pub fn new_empty() -> Self {
        Self {
            tags: Cow::Owned(HashMap::new()),
            metric_values: vec![],
        }
    }
}

impl PipelineMetricMetadata {
    pub fn to_tags(&self) -> HashMap<String, String> {
        let mut tags = HashMap::new();
        tags.insert(String::from("id"), self.node_context.reference_name.clone());
        // In tests, include service_instance_id in tags (shared provider path).
        if is_test_metrics_mode() {
            tags.insert(
                String::from("service_instance_id"),
                self.service_instance_id.clone(),
            );
        }
        tags.insert(
            String::from("topology_node_type"),
            self.node_context.node_type.to_string(),
        );
        tags.insert(
            String::from("operator_type"),
            self.node_context.operator_type.clone(),
        );
        for (k, v) in &self.additional_tags {
            tags.insert(String::from(k), String::from(v));
        }
        tags
    }
}

#[derive(PartialEq, Clone)]
pub enum RowCountMeasurementType {
    OutputRowCount,
    InputRowCount,
}

#[derive(Debug, Clone)]
pub enum TelemetryIOInfo {
    Input { count: u64 },
    Output { count: u64 },
    ElapsedTime { time_elapsed_ms: u64 },
}

impl fmt::Display for TelemetryIOInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TelemetryIOInfo::Input { count } => {
                write!(f, "input_rows_count: {}", count)
            }
            TelemetryIOInfo::Output { count } => {
                write!(f, "output_rows_count: {}", count)
            }
            TelemetryIOInfo::ElapsedTime { time_elapsed_ms } => {
                write!(f, "time_elapsed_ms: {}", time_elapsed_ms)
            }
        }
    }
}
