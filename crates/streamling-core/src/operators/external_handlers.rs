use crate::data::RowKind;
use crate::error::StreamlingError;
use crate::formats::json::{FromArrowToJsonConverter, JsonToArrowConverter};
use crate::formats::{FromArrowConverter, ToArrowConverter};
use crate::retry::retry_if_retriable;
use crate::utils::batch::enrich_batch_with_metadata;
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::DFSchemaRef;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties, Statistics,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use futures::StreamExt;
use reqwest::header::HeaderMap;
use serde_derive::Serialize;
use serde_json::value::RawValue;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

use crate::telemetry::recorder::get_metrics_recorder;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct ExternalHandlerConfig {
    pub url: String,
    pub headers: Option<BTreeMap<String, String>>,
    pub one_row_per_request: Option<bool>,
    pub payload_version: Option<u32>,
    pub trigger_max_count: u32,
    pub operator_timeout_sec: u32,
    pub schema_override: Option<BTreeMap<String, Option<String>>>,
    pub buffer_size: u32,
    pub metric_metadata_id: String,
}

#[derive(PartialEq, Eq, PartialOrd, Hash)]
pub struct ExternalHandlerNode {
    input: LogicalPlan,
    config: ExternalHandlerConfig,
}

impl ExternalHandlerNode {
    pub fn new(input: LogicalPlan, config: ExternalHandlerConfig) -> Self {
        Self { input, config }
    }
}

impl Debug for ExternalHandlerNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for ExternalHandlerNode {
    fn name(&self) -> &str {
        "ExternalHandler"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExternalHandler: url={}", self.config.url)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        let config = self.config.clone();

        Ok(Self {
            input: inputs.swap_remove(0),
            config,
        })
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }
}

pub(crate) struct ExternalHandlerExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for ExternalHandlerExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(
            if let Some(node) = node.as_any().downcast_ref::<ExternalHandlerNode>() {
                assert_eq!(logical_inputs.len(), 1, "Inconsistent number of inputs");
                assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");
                let external_handler_exec = Arc::new(ExternalHandlerExec::new(
                    physical_inputs[0].clone(),
                    node.config.clone(),
                ));
                //wrap with telemetry exec to export metrics
                Some(external_handler_exec)
            } else {
                None
            },
        )
    }
}

struct ExternalHandlerExec {
    input: Arc<dyn ExecutionPlan>,
    config: ExternalHandlerConfig,
    cache: Arc<PlanProperties>,
}

impl ExternalHandlerExec {
    fn new(input: Arc<dyn ExecutionPlan>, config: ExternalHandlerConfig) -> Self {
        let cache = Self::compute_properties(&input);
        Self {
            input,
            config,
            cache: Arc::new(cache),
        }
    }

    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            // The input's width, but not its `Hash` claim: the endpoint returns
            // the rows it wants, so any column — the primary key included — may
            // come back rewritten. Inheriting the claim would elide a downstream
            // keyed sink's exchange on a placement that no longer holds.
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        )
    }
}

impl Debug for ExternalHandlerExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExternalHandlerExec")
    }
}

impl DisplayAs for ExternalHandlerExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "ExternalHandlerExec: url={}, partitions={}",
                    self.config.url,
                    self.properties().output_partitioning().partition_count()
                )
            }
        }
    }
}

impl ExecutionPlan for ExternalHandlerExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // Every input partition gets its own request stream and its own client.
        vec![Distribution::UnspecifiedDistribution]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ExternalHandlerExec::new(
            children[0].clone(),
            self.config.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // One client per partition, deliberately: its `JsonToArrowConverter`
        // buffers the responses it is about to convert, so streams sharing one
        // would interleave into each other's output batches.
        let client = ExternalHandlerClient::new(
            self.schema(),
            self.config.url.clone(),
            self.config.headers.clone(),
            self.config.one_row_per_request,
            self.config.payload_version,
            self.config.trigger_max_count,
            self.config.operator_timeout_sec,
            self.config.schema_override.clone(),
            false,
            true,
            Some(self.config.metric_metadata_id.clone()),
        );

        let input_stream = self.input.execute(partition, context)?;

        let buffer_size = self.config.buffer_size as usize;

        let mut builder = RecordBatchReceiverStreamBuilder::new(self.schema(), buffer_size);
        let tx = builder.tx();

        builder.spawn(async move {
            let mut buffered_input = input_stream.ready_chunks(buffer_size);

            while let Some(batches) = buffered_input.next().await {
                let batches_futures =
                    futures::future::join_all(batches.into_iter().map(|item| async {
                        let data: Result<Option<RecordBatch>> = match item {
                            Ok(batch) => {
                                let input_metadata = batch.schema().metadata().clone();

                                if batch.num_rows() > 0 {
                                    client.execute(batch).await
                                } else {
                                    // For empty batches, manually propagate metadata
                                    let enriched_batch =
                                        enrich_batch_with_metadata(batch, input_metadata)?;
                                    Ok(Some(enriched_batch))
                                }
                            }
                            Err(e) => Err(e),
                        };

                        data
                    }));

                let batches = batches_futures.await;
                for item in batches {
                    match item {
                        Ok(Some(modified_batch)) => {
                            tx.send(Ok(modified_batch)).await.unwrap();
                        }
                        Err(e) => {
                            tx.send(Err(e)).await.unwrap();
                        }
                        _ => {}
                    }
                }
            }

            Ok(())
        });

        Ok(builder.build())
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema())))
    }
}

/// Whether an HTTP status code is retriable.
///
/// - 408 Request Timeout and 429 Too Many Requests are always retriable.
/// - All 5xx server errors are retriable.
/// - Other 4xx client errors are non-retriable (bad request, auth failures, etc.).
fn is_retriable_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::REQUEST_TIMEOUT
        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

fn http_status_class(status: reqwest::StatusCode) -> &'static str {
    if status.is_informational() {
        "1xx"
    } else if status.is_success() {
        "2xx"
    } else if status.is_redirection() {
        "3xx"
    } else if status.is_client_error() {
        "4xx"
    } else if status.is_server_error() {
        "5xx"
    } else {
        "unknown"
    }
}

static DEFAULT_ONE_ROW_PER_REQUEST: bool = false;

static DEFAULT_PAYLOAD_VERSION: u32 = 0;

#[derive(Serialize, Debug)]
struct PayloadEnvelopeMetadata {
    op: String,
}

#[derive(Serialize, Debug)]
struct PayloadEnvelope<'a> {
    metadata: PayloadEnvelopeMetadata,
    data: &'a RawValue, // using RawValue since data is already serialized using FromArrowToJsonConverter
}

#[allow(dead_code)]
pub struct ExternalHandlerClient {
    schema: SchemaRef,
    url: String,
    client: reqwest::Client,
    headers: HashMap<String, String>,
    one_row_per_request: bool,
    payload_version: u32,
    trigger_max_count: u32,
    operator_timeout_sec: u32,
    skip_on_error: bool,
    from_json_converter: FromArrowToJsonConverter,
    to_json_converter: Arc<Mutex<JsonToArrowConverter>>,
    is_output_expected: bool,
    metric_metadata_id: Option<String>,
}

pub struct ExternalHandlerExecutionOutcome {
    pub batch: Option<RecordBatch>,
    pub delivered_rows: u64,
    pub skipped_rows: u64,
}

enum HttpSendOutcome {
    Delivered(String),
    Skipped {
        status: reqwest::StatusCode,
        response_body: String,
    },
}

impl ExternalHandlerClient {
    fn record_http_request_metric(
        &self,
        metrics_recorder: Option<&Arc<crate::telemetry::recorder::MetricsRecorder>>,
        outcome: &'static str,
        status_class: &'static str,
        request_started_at: Instant,
    ) {
        if let (Some(metric_metadata_id), Some(metrics_recorder)) =
            (&self.metric_metadata_id, metrics_recorder)
        {
            metrics_recorder.record_count_w_tags(
                "http_requests",
                1,
                vec![("outcome", outcome), ("status_class", status_class)],
                metric_metadata_id,
            );
            metrics_recorder.record_time_w_tags(
                "http_request_latency",
                request_started_at.elapsed(),
                vec![("outcome", outcome), ("status_class", status_class)],
                metric_metadata_id,
            );
        }
    }

    fn record_http_retry_metric(
        &self,
        metrics_recorder: Option<&Arc<crate::telemetry::recorder::MetricsRecorder>>,
        status_class: &'static str,
    ) {
        if let (Some(metric_metadata_id), Some(metrics_recorder)) =
            (&self.metric_metadata_id, metrics_recorder)
        {
            metrics_recorder.record_count_w_tags(
                "http_retries",
                1,
                vec![("status_class", status_class)],
                metric_metadata_id,
            );
        }
    }

    pub fn new(
        schema: SchemaRef,
        url: String,
        headers: Option<BTreeMap<String, String>>,
        one_row_per_request: Option<bool>,
        payload_version: Option<u32>,
        trigger_max_count: u32,
        operator_timeout_sec: u32,
        schema_override: Option<BTreeMap<String, Option<String>>>,
        skip_on_error: bool,
        is_output_expected: bool,
        metric_metadata_id: Option<String>,
    ) -> Self {
        let one_row_per_request = one_row_per_request.unwrap_or(DEFAULT_ONE_ROW_PER_REQUEST);
        let payload_version = payload_version.unwrap_or(DEFAULT_PAYLOAD_VERSION);

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(operator_timeout_sec as u64))
            .connect_timeout(Duration::from_secs(10))
            .pool_idle_timeout(Duration::from_secs(20))
            .pool_max_idle_per_host(4)
            .tcp_keepalive(Duration::from_secs(15))
            .build()
            .expect("failed to build HTTP client");

        let mut headers = if let Some(headers) = &headers {
            // Converting to HashMap since reqwest expects it
            let headers: HashMap<String, String> = headers
                .clone()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            headers
        } else {
            HashMap::new()
        };

        // Default to JSON content type
        if !headers.contains_key("Content-Type") {
            headers.insert("Content-Type".to_string(), "application/json".to_string());
        }

        debug!("Schema before overrides: {:?}", schema);

        let schema = Self::apply_schema_overrides(schema, schema_override);

        debug!("Schema after overrides: {:?}", schema);

        let from_json_converter = FromArrowToJsonConverter::new();
        let to_json_converter = Arc::new(Mutex::new(JsonToArrowConverter::new(
            schema.clone(),
            one_row_per_request,
            Self::unapply_envelope_field_to_extract(payload_version),
        )));

        debug_assert!(
            !(skip_on_error && is_output_expected),
            "skip_on_error is not compatible with output-producing handlers"
        );

        Self {
            schema,
            url,
            client,
            headers,
            one_row_per_request,
            payload_version,
            trigger_max_count,
            operator_timeout_sec,
            skip_on_error,
            from_json_converter,
            to_json_converter,
            is_output_expected,
            metric_metadata_id,
        }
    }

    fn create_request_builder(&self) -> reqwest::RequestBuilder {
        let headers: HeaderMap = (&self.headers).try_into().expect("invalid HTTP headers");

        self.client.post(&self.url).headers(headers.clone())
    }

    /// Send an HTTP request with retry semantics via `retry_if_retriable`.
    ///
    /// Retriable conditions (network errors, timeouts, 408, 429, 5xx) are retried
    /// indefinitely with exponential backoff unless `skip_on_error` is enabled.
    /// In skip mode, response errors are acknowledged immediately while transport
    /// failures still retry.
    async fn send_with_retry(
        &self,
        body: String,
        operation_name: &str,
    ) -> crate::error::Result<HttpSendOutcome> {
        let metrics_recorder = self
            .metric_metadata_id
            .as_ref()
            .map(|_| get_metrics_recorder());
        let pending_retry_status_class = parking_lot::Mutex::new(None::<&'static str>);
        retry_if_retriable(
            || {
                let body = body.clone();
                let metrics_recorder = metrics_recorder.clone();
                let prc = &pending_retry_status_class;
                async move {
                    if let Some(status_class) = prc.lock().take() {
                        self.record_http_retry_metric(metrics_recorder.as_ref(), status_class);
                    }

                    let request_started_at = Instant::now();
                    let response = self
                        .create_request_builder()
                        .body(body)
                        .send()
                        .await
                        .map_err(|e| {
                            let status_class = if e.is_timeout() {
                                "timeout"
                            } else {
                                "network_error"
                            };
                            self.record_http_request_metric(
                                metrics_recorder.as_ref(),
                                "retriable_error",
                                status_class,
                                request_started_at,
                            );
                            *prc.lock() = Some(status_class);
                            StreamlingError::retriable_with_cause(
                                format!("{operation_name}: error sending request"),
                                e,
                            )
                        })?;

                    let status = response.status();
                    if status.is_success() {
                        let status_class = http_status_class(status);
                        let response_body = response.text().await.map_err(|e| {
                            // The HTTP transaction succeeded (2xx) but reading the body
                            // failed. Use a distinct status_class so dashboards don't
                            // conflate these failures with successful 2xx requests.
                            self.record_http_request_metric(
                                metrics_recorder.as_ref(),
                                "retriable_error",
                                "body_read_error",
                                request_started_at,
                            );
                            *prc.lock() = Some("body_read_error");
                            StreamlingError::retriable_with_cause(
                                format!("{operation_name}: failed to read response body"),
                                e,
                            )
                        })?;
                        self.record_http_request_metric(
                            metrics_recorder.as_ref(),
                            "success",
                            status_class,
                            request_started_at,
                        );
                        return Ok(HttpSendOutcome::Delivered(response_body));
                    }

                    let response_body = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "<failed to read body>".to_string());

                    if is_retriable_status(status) {
                        let status_class = http_status_class(status);
                        self.record_http_request_metric(
                            metrics_recorder.as_ref(),
                            "retriable_error",
                            status_class,
                            request_started_at,
                        );
                        if self.skip_on_error {
                            Ok(HttpSendOutcome::Skipped {
                                status,
                                response_body,
                            })
                        } else {
                            *prc.lock() = Some(status_class);
                            Err(StreamlingError::retriable(format!(
                                "{operation_name}: HTTP {status} - {response_body}"
                            )))
                        }
                    } else {
                        let status_class = http_status_class(status);
                        self.record_http_request_metric(
                            metrics_recorder.as_ref(),
                            "non_retriable_error",
                            status_class,
                            request_started_at,
                        );
                        if self.skip_on_error {
                            Ok(HttpSendOutcome::Skipped {
                                status,
                                response_body,
                            })
                        } else {
                            Err(StreamlingError::new(format!(
                                "{operation_name}: non-retriable HTTP {status} - {response_body}"
                            )))
                        }
                    }
                }
            },
            operation_name,
        )
        .await
    }

    fn apply_schema_overrides(
        schema: SchemaRef,
        overrides: Option<BTreeMap<String, Option<String>>>,
    ) -> SchemaRef {
        match overrides {
            Some(overrides) => {
                let mut new_fields: Vec<FieldRef> = schema
                    .fields()
                    .into_iter()
                    // first, filter out fields that have null values in the overrides map
                    .filter(|field| {
                        let name = field.name();
                        // no overrides configured
                        !overrides.contains_key(name) ||
                            // or an override configured and not null
                            overrides.get(name).unwrap().is_some()
                    })
                    // second, change types of the existing fields
                    .map(|field| {
                        let name = field.name();
                        let new_field: FieldRef = match overrides.get(name) {
                            Some(Some(new_field_type)) => {
                                let new_field_type: DataType = new_field_type.parse().unwrap();
                                Arc::new(Field::new(name, new_field_type, field.is_nullable()))
                            }
                            _ => field.clone(),
                        };
                        new_field
                    })
                    .collect();
                // finally, add new fields that are not present in the original schema
                for (name, new_field_type) in overrides.iter() {
                    if schema.field_with_name(name).is_err() && new_field_type.is_some() {
                        let new_field_type: DataType =
                            new_field_type.clone().unwrap().parse().unwrap();
                        new_fields.push(Arc::new(Field::new(name, new_field_type, true)));
                    }
                }
                Arc::new(Schema::new(new_fields))
            }
            None => schema,
        }
    }

    fn apply_envelope(&self, data: String, op: Option<String>) -> String {
        match self.payload_version {
            0 => data, // no envelope
            1 => {
                // This shouldn't perform any parsing
                let raw_data = serde_json::from_str(data.as_str()).unwrap();
                let envelope = PayloadEnvelope {
                    metadata: PayloadEnvelopeMetadata {
                        op: op.expect("Operation must be defined for this payload version"),
                    },
                    data: raw_data,
                };
                serde_json::to_string(&envelope).expect("Failed to serialize payload envelope")
            }
            _ => panic!("Unsupported payload version: {}", self.payload_version),
        }
    }

    fn unapply_envelope_field_to_extract(payload_version: u32) -> Option<String> {
        match payload_version {
            0 => None,
            1 => Some("data".to_string()), // matches the envelope from `apply_envelope`
            _ => panic!("Unsupported payload version: {}", payload_version),
        }
    }

    pub async fn execute_with_stats(
        &self,
        batch: RecordBatch,
    ) -> Result<ExternalHandlerExecutionOutcome> {
        let mut responses: Vec<String> = vec![];
        let mut delivered_rows: u64 = 0;
        let mut skipped_rows: u64 = 0;

        let input_metadata = batch.schema().metadata().clone();

        let row_kinds = RowKind::extract_row_kinds_from_batch(&batch);
        let rows = self.from_json_converter.convert_from_batch(&batch)?;

        if self.one_row_per_request {
            for (i, row) in rows.into_iter().enumerate() {
                let row_kind = row_kinds.get(i).expect("row_kind index out of bounds");

                let json_row = String::from_utf8(row)
                    .map_err(|e| StreamlingError::new(format!("invalid UTF-8 in row: {}", e)))?;
                let json_row: String = self.apply_envelope(json_row, Some(row_kind.to_str()));

                match self
                    .send_with_retry(json_row, &format!("HTTP handler POST {}", self.url))
                    .await
                {
                    Ok(HttpSendOutcome::Delivered(response)) => {
                        responses.push(response);
                        delivered_rows += 1;
                    }
                    Ok(HttpSendOutcome::Skipped {
                        status,
                        response_body,
                    }) => {
                        warn!(
                            status = %status,
                            response_body = %response_body,
                            row_index = i,
                            "skipping failed webhook delivery"
                        );
                        skipped_rows += 1;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        } else if !rows.is_empty() {
            for (i, chunk) in rows.chunks(self.trigger_max_count as usize).enumerate() {
                let json_rows: Vec<String> = chunk
                    .iter()
                    .map(|r| {
                        String::from_utf8(r.clone()).map_err(|e| {
                            StreamlingError::new(format!("invalid UTF-8 in row: {}", e))
                        })
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let json_rows = json_rows
                    .into_iter()
                    .enumerate()
                    .map(|(j, row)| {
                        let row_kind_index = i * self.trigger_max_count as usize + j;
                        self.apply_envelope(
                            row,
                            Some(
                                row_kinds
                                    .get(row_kind_index)
                                    .expect("row_kind index out of bounds")
                                    .to_str(),
                            ),
                        )
                    })
                    .collect::<Vec<String>>();
                let json_rows = json_rows.join(",\n");
                let json_rows = format!("[{}]\n", json_rows);

                match self
                    .send_with_retry(json_rows, &format!("HTTP handler POST {}", self.url))
                    .await
                {
                    Ok(HttpSendOutcome::Delivered(response)) => {
                        responses.push(response);
                        delivered_rows += chunk.len() as u64;
                    }
                    Ok(HttpSendOutcome::Skipped {
                        status,
                        response_body,
                    }) => {
                        warn!(
                            status = %status,
                            response_body = %response_body,
                            batch_rows = chunk.len(),
                            "skipping failed webhook delivery"
                        );
                        skipped_rows += chunk.len() as u64;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }

        let batch = if self.is_output_expected {
            let mut to_json_converter_guard = self
                .to_json_converter
                .lock()
                .expect("JSON converter lock poisoned");

            for response in responses {
                (*to_json_converter_guard).buffer(response);
            }
            match (*to_json_converter_guard).convert_to_batch() {
                Ok(output_batch) => {
                    let enriched_batch = enrich_batch_with_metadata(output_batch, input_metadata)?;
                    Some(enriched_batch)
                }
                Err(e) => return Err(e),
            }
        } else {
            None
        };

        Ok(ExternalHandlerExecutionOutcome {
            batch,
            delivered_rows,
            skipped_rows,
        })
    }

    pub async fn execute(&self, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        self.execute_with_stats(batch)
            .await
            .map(|outcome| outcome.batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
    use std::collections::BTreeMap;
    use std::net::TcpListener;
    use std::sync::Arc;

    const DEFAULT_TRIGGER_MAX_COUNT: u32 = 2000;
    const DEFAULT_OPERATOR_TIMEOUT_SEC: u32 = 300;

    #[test]
    fn test_apply_schema_overrides() {
        let original_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Utf8, false),
            Field::new("d", DataType::Int64, false),
        ]));

        let mut overrides: BTreeMap<String, Option<String>> = BTreeMap::new();
        // new field
        overrides.insert("x".to_string(), Some("Int32".to_string()));
        // change field type
        overrides.insert("a".to_string(), Some("Int64".to_string()));
        // remove field
        overrides.insert("b".to_string(), None);

        let new_schema =
            ExternalHandlerClient::apply_schema_overrides(original_schema, Some(overrides));

        assert_eq!(new_schema.fields().len(), 4);
        assert_eq!(
            new_schema.field_with_name("a").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            new_schema.field_with_name("c").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            new_schema.field_with_name("d").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            new_schema.field_with_name("x").unwrap().data_type(),
            &DataType::Int32
        );
    }

    #[test]
    fn test_is_retriable_status() {
        use reqwest::StatusCode;

        assert!(is_retriable_status(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(is_retriable_status(StatusCode::BAD_GATEWAY));
        assert!(is_retriable_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(is_retriable_status(StatusCode::GATEWAY_TIMEOUT));
        assert!(is_retriable_status(StatusCode::REQUEST_TIMEOUT));
        assert!(is_retriable_status(StatusCode::TOO_MANY_REQUESTS));

        assert!(!is_retriable_status(StatusCode::OK));
        assert!(!is_retriable_status(StatusCode::CREATED));
        assert!(!is_retriable_status(StatusCode::BAD_REQUEST));
        assert!(!is_retriable_status(StatusCode::UNAUTHORIZED));
        assert!(!is_retriable_status(StatusCode::FORBIDDEN));
        assert!(!is_retriable_status(StatusCode::NOT_FOUND));
        assert!(!is_retriable_status(StatusCode::CONFLICT));
        assert!(!is_retriable_status(StatusCode::UNPROCESSABLE_ENTITY));
    }

    #[test]
    fn test_http_status_class() {
        use reqwest::StatusCode;

        assert_eq!(http_status_class(StatusCode::OK), "2xx");
        assert_eq!(http_status_class(StatusCode::TEMPORARY_REDIRECT), "3xx");
        assert_eq!(http_status_class(StatusCode::BAD_REQUEST), "4xx");
        assert_eq!(http_status_class(StatusCode::SERVICE_UNAVAILABLE), "5xx");
    }

    mod test_helpers {
        use super::*;
        use axum::extract::State;
        use axum::http::StatusCode;
        use axum::routing::post;
        use axum::{Json, Router};
        use serde::{Deserialize, Serialize};
        use std::sync::Mutex;

        #[derive(Serialize, Deserialize, Debug)]
        pub struct TestMessage {
            pub block: u64,
            pub id: String,
            pub data: String,
            pub timestamp: u64,
            #[serde(rename = "_gs_op")]
            pub _gs_op: String,
        }

        impl TestMessage {
            pub fn schema() -> SchemaRef {
                Arc::new(Schema::new(vec![
                    Field::new("block", DataType::UInt64, false),
                    Field::new("id", DataType::Utf8, false),
                    Field::new("data", DataType::Utf8, false),
                    Field::new("timestamp", DataType::UInt64, false),
                    Field::new("_gs_op", DataType::Utf8, false),
                ]))
            }
        }

        #[derive(Serialize, Deserialize, Debug)]
        pub struct ModifiedTestMessage {
            pub block: u32,
            pub id: String,
            pub data: String,
            #[serde(rename = "_gs_op")]
            pub _gs_op: String,
        }

        #[derive(Serialize, Deserialize, Debug)]
        pub struct SlimTestMessage {
            pub id: String,
            pub data: String,
            #[serde(rename = "_gs_op")]
            pub _gs_op: String,
        }

        impl SlimTestMessage {
            pub fn schema() -> SchemaRef {
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("data", DataType::Utf8, false),
                    Field::new("_gs_op", DataType::Utf8, false),
                ]))
            }
        }

        #[derive(Serialize, Deserialize, Debug)]
        pub struct EnvelopeMetadata {
            pub op: String,
        }

        #[derive(Serialize, Deserialize, Debug)]
        pub struct SlimTestMessageEnvelope {
            pub metadata: EnvelopeMetadata,
            pub data: SlimTestMessage,
        }

        pub struct HttpServerAddress(pub String);

        #[derive(Clone)]
        pub struct HttpAppState {
            request_counter: Arc<Mutex<u64>>,
        }

        impl HttpAppState {
            fn new() -> Self {
                HttpAppState {
                    request_counter: Arc::new(Mutex::new(0)),
                }
            }

            pub fn inc_requests(&self) {
                let mut counter = self.request_counter.lock().unwrap();
                *counter += 1;
            }

            pub fn total_requests(&self) -> u64 {
                let counter = self.request_counter.lock().unwrap();
                *counter
            }
        }

        fn find_available_port() -> std::io::Result<u16> {
            let listener = TcpListener::bind("127.0.0.1:0")?;
            let local_addr = listener.local_addr()?;
            Ok(local_addr.port())
        }

        pub async fn start_http_server() -> (HttpServerAddress, HttpAppState) {
            let state = HttpAppState::new();

            let app = Router::new()
                .route(
                    "/handler_slim_test_message",
                    post(http_handler_slim_test_message),
                )
                .route(
                    "/handler_slim_test_message_with_envelope",
                    post(http_handler_slim_test_message_with_envelope),
                )
                .route(
                    "/handler_slim_test_message_batch",
                    post(http_handler_slim_test_message_batch),
                )
                .route(
                    "/handler_slim_test_message_batch_with_envelope",
                    post(http_handler_slim_test_message_batch_with_envelope),
                )
                .route(
                    "/handler_schema_override",
                    post(http_handler_schema_override),
                )
                .route(
                    "/handler_fail_then_succeed",
                    post(http_handler_fail_then_succeed),
                )
                .route(
                    "/handler_non_retriable_error",
                    post(http_handler_non_retriable_error),
                )
                .route(
                    "/handler_retriable_error",
                    post(http_handler_retriable_error),
                )
                .with_state(state.clone());

            let port = find_available_port().unwrap();
            let address = format!("127.0.0.1:{port}");

            let listener = tokio::net::TcpListener::bind(address.clone())
                .await
                .unwrap();
            tokio::spawn(async {
                axum::serve(listener, app).await.unwrap();
            });

            (HttpServerAddress(address), state)
        }

        /// Returns 503 for the first 2 requests, then 200 on the third.
        async fn http_handler_fail_then_succeed(
            State(state): State<HttpAppState>,
            Json(payload): Json<SlimTestMessage>,
        ) -> (StatusCode, Json<SlimTestMessage>) {
            state.inc_requests();
            let count = state.total_requests();
            if count <= 2 {
                return (StatusCode::SERVICE_UNAVAILABLE, Json(payload));
            }
            let updated_payload = SlimTestMessage {
                id: payload.id,
                data: format!("updated-{}", payload.data),
                _gs_op: payload._gs_op,
            };
            (StatusCode::OK, Json(updated_payload))
        }

        /// Always returns 400 Bad Request (non-retriable).
        async fn http_handler_non_retriable_error(
            State(state): State<HttpAppState>,
            body: String,
        ) -> (StatusCode, String) {
            state.inc_requests();
            let _ = body;
            (StatusCode::BAD_REQUEST, "bad request".to_string())
        }

        /// Always returns 503 Service Unavailable (retriable).
        async fn http_handler_retriable_error(
            State(state): State<HttpAppState>,
            body: String,
        ) -> (StatusCode, String) {
            state.inc_requests();
            let _ = body;
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "service unavailable".to_string(),
            )
        }

        async fn http_handler_slim_test_message(
            State(state): State<HttpAppState>,
            Json(payload): Json<SlimTestMessage>,
        ) -> (StatusCode, Json<SlimTestMessage>) {
            state.inc_requests();
            let updated_payload = SlimTestMessage {
                id: payload.id,
                data: format!("updated-{}", payload.data),
                _gs_op: payload._gs_op,
            };
            (StatusCode::OK, Json(updated_payload))
        }

        async fn http_handler_slim_test_message_with_envelope(
            State(state): State<HttpAppState>,
            Json(payload): Json<SlimTestMessageEnvelope>,
        ) -> (StatusCode, Json<SlimTestMessageEnvelope>) {
            state.inc_requests();
            let updated_payload = SlimTestMessageEnvelope {
                metadata: payload.metadata,
                data: SlimTestMessage {
                    id: payload.data.id,
                    data: format!("updated-{}", payload.data.data),
                    _gs_op: payload.data._gs_op,
                },
            };
            (StatusCode::OK, Json(updated_payload))
        }

        async fn http_handler_slim_test_message_batch(
            State(state): State<HttpAppState>,
            Json(payloads): Json<Vec<SlimTestMessage>>,
        ) -> (StatusCode, Json<Vec<SlimTestMessage>>) {
            state.inc_requests();
            let updated_payloads = payloads
                .into_iter()
                .map(|payload| SlimTestMessage {
                    id: payload.id,
                    data: format!("updated-{}", payload.data),
                    _gs_op: payload._gs_op,
                })
                .collect();
            (StatusCode::OK, Json(updated_payloads))
        }

        async fn http_handler_slim_test_message_batch_with_envelope(
            State(state): State<HttpAppState>,
            Json(payloads): Json<Vec<SlimTestMessageEnvelope>>,
        ) -> (StatusCode, Json<Vec<SlimTestMessageEnvelope>>) {
            state.inc_requests();
            let updated_payloads = payloads
                .into_iter()
                .map(|payload| SlimTestMessageEnvelope {
                    metadata: payload.metadata,
                    data: SlimTestMessage {
                        id: payload.data.id,
                        data: format!("updated-{}", payload.data.data),
                        _gs_op: payload.data._gs_op,
                    },
                })
                .collect();
            (StatusCode::OK, Json(updated_payloads))
        }

        async fn http_handler_schema_override(
            State(state): State<HttpAppState>,
            Json(payload): Json<TestMessage>,
        ) -> (StatusCode, Json<ModifiedTestMessage>) {
            state.inc_requests();
            let updated_payload = ModifiedTestMessage {
                block: payload.block as u32,
                id: payload.id,
                data: format!("updated-{}", payload.data),
                _gs_op: payload._gs_op,
            };
            (StatusCode::OK, Json(updated_payload))
        }
    }

    use test_helpers::*;

    #[tokio::test]
    async fn test_external_handlers_single_row_envelope_zero() {
        let (http_server_address, http_server_state) = start_http_server().await;

        let client = ExternalHandlerClient::new(
            SlimTestMessage::schema(),
            format!("http://{}/handler_slim_test_message", http_server_address.0),
            None,
            Some(true),
            Some(0),
            DEFAULT_TRIGGER_MAX_COUNT,
            DEFAULT_OPERATOR_TIMEOUT_SEC,
            None,
            false,
            true,
            None,
        );

        let batch = RecordBatch::try_new(
            SlimTestMessage::schema(),
            vec![
                Arc::new(StringArray::from(vec!["1", "2", "3"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["i", "u", "i"])) as ArrayRef,
            ],
        )
        .unwrap();

        let updated_batch = client.execute(batch).await.unwrap().unwrap();

        assert_eq!(updated_batch.num_columns(), 3);
        assert_eq!(updated_batch.num_rows(), 3);

        let updated_data = updated_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            updated_data,
            &StringArray::from(vec!["updated-alpha", "updated-beta", "updated-gamma"])
        );

        assert_eq!(http_server_state.total_requests(), 3);
    }

    #[tokio::test]
    async fn test_external_handlers_single_row_envelope_one() {
        let (http_server_address, http_server_state) = start_http_server().await;

        let client = ExternalHandlerClient::new(
            SlimTestMessage::schema(),
            format!(
                "http://{}/handler_slim_test_message_with_envelope",
                http_server_address.0
            ),
            None,
            Some(true),
            Some(1),
            DEFAULT_TRIGGER_MAX_COUNT,
            DEFAULT_OPERATOR_TIMEOUT_SEC,
            None,
            false,
            true,
            None,
        );

        let batch = RecordBatch::try_new(
            SlimTestMessage::schema(),
            vec![
                Arc::new(StringArray::from(vec!["1", "2", "3"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["i", "u", "i"])) as ArrayRef,
            ],
        )
        .unwrap();

        let updated_batch = client.execute(batch).await.unwrap().unwrap();

        assert_eq!(updated_batch.num_columns(), 3);
        assert_eq!(updated_batch.num_rows(), 3);

        let updated_data = updated_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            updated_data,
            &StringArray::from(vec!["updated-alpha", "updated-beta", "updated-gamma"])
        );

        assert_eq!(http_server_state.total_requests(), 3);
    }

    #[tokio::test]
    async fn test_external_handlers_batch_envelope_zero() {
        let (http_server_address, http_server_state) = start_http_server().await;

        let client = ExternalHandlerClient::new(
            SlimTestMessage::schema(),
            format!(
                "http://{}/handler_slim_test_message_batch",
                http_server_address.0
            ),
            None,
            Some(false),
            Some(0),
            DEFAULT_TRIGGER_MAX_COUNT,
            DEFAULT_OPERATOR_TIMEOUT_SEC,
            None,
            false,
            true,
            None,
        );

        let batch = RecordBatch::try_new(
            SlimTestMessage::schema(),
            vec![
                Arc::new(StringArray::from(vec!["1", "2", "3"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["i", "u", "i"])) as ArrayRef,
            ],
        )
        .unwrap();

        let updated_batch = client.execute(batch).await.unwrap().unwrap();

        assert_eq!(updated_batch.num_columns(), 3);
        assert_eq!(updated_batch.num_rows(), 3);

        let updated_data = updated_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            updated_data,
            &StringArray::from(vec!["updated-alpha", "updated-beta", "updated-gamma"])
        );

        assert_eq!(http_server_state.total_requests(), 1);
    }

    #[tokio::test]
    async fn test_external_handlers_batch_envelope_one() {
        let (http_server_address, http_server_state) = start_http_server().await;

        let client = ExternalHandlerClient::new(
            SlimTestMessage::schema(),
            format!(
                "http://{}/handler_slim_test_message_batch_with_envelope",
                http_server_address.0
            ),
            None,
            Some(false),
            Some(1),
            DEFAULT_TRIGGER_MAX_COUNT,
            DEFAULT_OPERATOR_TIMEOUT_SEC,
            None,
            false,
            true,
            None,
        );

        let batch = RecordBatch::try_new(
            SlimTestMessage::schema(),
            vec![
                Arc::new(StringArray::from(vec!["1", "2", "3"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["i", "u", "i"])) as ArrayRef,
            ],
        )
        .unwrap();

        let updated_batch = client.execute(batch).await.unwrap().unwrap();

        assert_eq!(updated_batch.num_columns(), 3);
        assert_eq!(updated_batch.num_rows(), 3);

        let updated_data = updated_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            updated_data,
            &StringArray::from(vec!["updated-alpha", "updated-beta", "updated-gamma"])
        );

        assert_eq!(http_server_state.total_requests(), 1);
    }

    #[tokio::test]
    async fn test_external_handlers_schema_override() {
        let (http_server_address, http_server_state) = start_http_server().await;

        let client = ExternalHandlerClient::new(
            TestMessage::schema(),
            format!("http://{}/handler_schema_override", http_server_address.0),
            None,
            Some(true),
            Some(0),
            DEFAULT_TRIGGER_MAX_COUNT,
            DEFAULT_OPERATOR_TIMEOUT_SEC,
            // change block's type to u32, drop timestamp
            Some(BTreeMap::from([
                ("block".to_string(), Some("Int32".to_string())),
                ("timestamp".to_string(), None),
            ])),
            false,
            true,
            None,
        );

        let batch = RecordBatch::try_new(
            TestMessage::schema(),
            vec![
                Arc::new(UInt64Array::from(vec![10, 20, 30])) as ArrayRef,
                Arc::new(StringArray::from(vec!["1", "2", "3"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![123, 234, 345])) as ArrayRef,
                Arc::new(StringArray::from(vec!["i", "u", "i"])) as ArrayRef,
            ],
        )
        .unwrap();

        let updated_batch = client.execute(batch).await.unwrap().unwrap();

        assert_eq!(updated_batch.num_columns(), 4); // timestamp dropped
        assert_eq!(updated_batch.num_rows(), 3);

        let updated_data = updated_batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            updated_data,
            &StringArray::from(vec!["updated-alpha", "updated-beta", "updated-gamma"])
        );

        assert_eq!(http_server_state.total_requests(), 3);
    }

    #[tokio::test]
    async fn test_retry_on_server_error_then_succeed() {
        let (http_server_address, http_server_state) = start_http_server().await;

        let client = ExternalHandlerClient::new(
            SlimTestMessage::schema(),
            format!("http://{}/handler_fail_then_succeed", http_server_address.0),
            None,
            Some(true),
            Some(0),
            DEFAULT_TRIGGER_MAX_COUNT,
            DEFAULT_OPERATOR_TIMEOUT_SEC,
            None,
            false,
            true,
            None,
        );

        let batch = RecordBatch::try_new(
            SlimTestMessage::schema(),
            vec![
                Arc::new(StringArray::from(vec!["1"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["alpha"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["i"])) as ArrayRef,
            ],
        )
        .unwrap();

        let updated_batch = client.execute(batch).await.unwrap().unwrap();

        assert_eq!(updated_batch.num_columns(), 3);
        assert_eq!(updated_batch.num_rows(), 1);

        let updated_data = updated_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(updated_data, &StringArray::from(vec!["updated-alpha"]));

        // 2 failed attempts + 1 success = 3 total requests
        assert_eq!(http_server_state.total_requests(), 3);
    }

    #[tokio::test]
    async fn test_non_retriable_error_fails_immediately() {
        let (http_server_address, http_server_state) = start_http_server().await;

        let client = ExternalHandlerClient::new(
            SlimTestMessage::schema(),
            format!(
                "http://{}/handler_non_retriable_error",
                http_server_address.0
            ),
            None,
            Some(true),
            Some(0),
            DEFAULT_TRIGGER_MAX_COUNT,
            DEFAULT_OPERATOR_TIMEOUT_SEC,
            None,
            false,
            false,
            None,
        );

        let batch = RecordBatch::try_new(
            SlimTestMessage::schema(),
            vec![
                Arc::new(StringArray::from(vec!["1"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["alpha"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["i"])) as ArrayRef,
            ],
        )
        .unwrap();

        let result = client.execute(batch).await;
        assert!(result.is_err());

        let df_err = result.unwrap_err();
        let err_msg = df_err.to_string();
        assert!(
            err_msg.contains("non-retriable HTTP 400"),
            "expected non-retriable error, got: {}",
            err_msg
        );

        // Recover StreamlingError from DataFusionError to verify flags
        if let datafusion::error::DataFusionError::External(boxed) = df_err {
            let se = boxed
                .downcast::<StreamlingError>()
                .expect("should be a StreamlingError");
            assert!(
                !se.is_retriable(),
                "non-retriable 4xx should not be retriable"
            );
            assert!(se.is_internal(), "should be an internal error");
        } else {
            panic!("expected DataFusionError::External, got: {df_err:?}");
        }

        // Only 1 request — no retries for 400
        assert_eq!(http_server_state.total_requests(), 1);
    }

    #[tokio::test]
    async fn test_skip_on_error_skips_on_http_error_response() {
        let (http_server_address, http_server_state) = start_http_server().await;

        let client = ExternalHandlerClient::new(
            SlimTestMessage::schema(),
            format!("http://{}/handler_retriable_error", http_server_address.0),
            None,
            Some(true),
            Some(0),
            DEFAULT_TRIGGER_MAX_COUNT,
            DEFAULT_OPERATOR_TIMEOUT_SEC,
            None,
            true,
            false,
            None,
        );

        let batch = RecordBatch::try_new(
            SlimTestMessage::schema(),
            vec![
                Arc::new(StringArray::from(vec!["1"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["alpha"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["i"])) as ArrayRef,
            ],
        )
        .unwrap();

        let outcome = client.execute_with_stats(batch).await.unwrap();
        assert_eq!(outcome.delivered_rows, 0);
        assert_eq!(outcome.skipped_rows, 1);
        assert!(outcome.batch.is_none());

        assert_eq!(http_server_state.total_requests(), 1);
    }

    #[tokio::test]
    async fn test_skip_on_error_continues_after_failed_row_in_batch() {
        let (http_server_address, http_server_state) = start_http_server().await;

        let client = ExternalHandlerClient::new(
            SlimTestMessage::schema(),
            format!("http://{}/handler_fail_then_succeed", http_server_address.0),
            None,
            Some(true),
            Some(0),
            DEFAULT_TRIGGER_MAX_COUNT,
            DEFAULT_OPERATOR_TIMEOUT_SEC,
            None,
            true,
            false,
            None,
        );

        let batch = RecordBatch::try_new(
            SlimTestMessage::schema(),
            vec![
                Arc::new(StringArray::from(vec!["1", "2", "3"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["i", "u", "i"])) as ArrayRef,
            ],
        )
        .unwrap();

        let outcome = client.execute_with_stats(batch).await.unwrap();
        assert_eq!(outcome.delivered_rows, 1);
        assert_eq!(outcome.skipped_rows, 2);
        assert!(outcome.batch.is_none());

        assert_eq!(http_server_state.total_requests(), 3);
    }

    #[tokio::test]
    async fn test_skip_on_error_retries_on_connection_refused() {
        let client = ExternalHandlerClient::new(
            SlimTestMessage::schema(),
            "http://127.0.0.1:1/unreachable".to_string(),
            None,
            Some(true),
            Some(0),
            DEFAULT_TRIGGER_MAX_COUNT,
            2, // short timeout
            None,
            true,
            false,
            None,
        );

        let batch = RecordBatch::try_new(
            SlimTestMessage::schema(),
            vec![
                Arc::new(StringArray::from(vec!["1"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["alpha"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["i"])) as ArrayRef,
            ],
        )
        .unwrap();

        let result =
            tokio::time::timeout(Duration::from_secs(2), client.execute_with_stats(batch)).await;

        assert!(
            result.is_err(),
            "expected timeout since retries should continue on connection errors"
        );
    }

    #[tokio::test]
    async fn test_retry_on_connection_refused() {
        // Use a port that nothing is listening on
        let client = ExternalHandlerClient::new(
            SlimTestMessage::schema(),
            "http://127.0.0.1:1/unreachable".to_string(),
            None,
            Some(true),
            Some(0),
            DEFAULT_TRIGGER_MAX_COUNT,
            2, // short timeout
            None,
            false,
            false,
            None,
        );

        let batch = RecordBatch::try_new(
            SlimTestMessage::schema(),
            vec![
                Arc::new(StringArray::from(vec!["1"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["alpha"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["i"])) as ArrayRef,
            ],
        )
        .unwrap();

        // The retry loop will retry forever, so we use a timeout to verify retries happen
        let result = tokio::time::timeout(Duration::from_secs(2), client.execute(batch)).await;

        // Should timeout because it retries forever on network errors
        assert!(
            result.is_err(),
            "expected timeout since retries should continue forever on connection errors"
        );
    }
}
