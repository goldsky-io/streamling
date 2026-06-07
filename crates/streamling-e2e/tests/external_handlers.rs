//! External handler e2e tests.
//!
//! These tests verify that the handler transform correctly sends data to an external
//! HTTP endpoint and processes the response.
//!
//! Ported from crates/streamling/tests/external_handlers.rs

use serde::Serialize;
use streamling_e2e::resources::ExternalHandlerResource;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

// ============================================================================
// Test Record Types
// ============================================================================

/// Simple test record matching the slim test message format
#[derive(Debug, Clone, Serialize)]
struct SlimTestRecord {
    id: String,
    data: String,
}

const SLIM_SCHEMA: &str = r#"{
    "type": "record",
    "name": "SlimTestRecord",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "data", "type": "string"}
    ]
}"#;

// ============================================================================
// External Handler Tests
// ============================================================================

/// Test external handler with single row per request and envelope version 0.
///
/// This test verifies that when `one_row_per_request: true` and `payload_version: 0`,
/// the handler receives one HTTP request per row with flat JSON format.
///
/// Ported from: test_external_handlers_single_row_envelope_zero
#[tokio::test]
async fn test_external_handler_single_row_envelope_zero() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // Start external handler server
    let handler = ExternalHandlerResource::new()
        .await
        .expect("Failed to start handler server");

    // Register schema
    ctx.kafka
        .register_schema(SLIM_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Create test records
    let records: Vec<SlimTestRecord> = vec![
        SlimTestRecord {
            id: "1".to_string(),
            data: "alpha".to_string(),
        },
        SlimTestRecord {
            id: "2".to_string(),
            data: "beta".to_string(),
        },
        SlimTestRecord {
            id: "3".to_string(),
            data: "gamma".to_string(),
        },
    ];

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Pipeline with handler transform - single row, envelope v0
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    primary_key: id

transforms:
  handler_transform:
    type: handler
    from: kafka_source
    url: {handler_url}
    one_row_per_request: true
    payload_version: 0
    primary_key: id

sinks:
  print_sink:
    type: print
    from: handler_transform
    sample_every: 1
"#,
        topic = ctx.kafka_topic,
        handler_url = handler.slim_handler_url()
    );

    let output = ctx
        .run_pipeline_with_capture(&pipeline, PipelineOpts::new().record_limit(3))
        .await
        .expect("Pipeline execution failed");

    // Verify that we got 3 records with updated data
    assert_eq!(output.rows().len(), 3, "Expected 3 output rows");

    // Verify the handler was called once per row (single row mode)
    assert_eq!(
        handler.request_count(),
        3,
        "Handler should receive 3 requests (one per row)"
    );

    // Verify the data was updated by the handler
    let data_values: Vec<&str> = output
        .rows()
        .iter()
        .filter_map(|r| r.data.get("data").and_then(|v| v.as_str()))
        .collect();

    assert!(
        data_values.iter().all(|d| d.starts_with("updated-")),
        "All data values should be prefixed with 'updated-': {:?}",
        data_values
    );
}

/// Test external handler with single row per request and envelope version 1.
///
/// This test verifies that when `one_row_per_request: true` and `payload_version: 1`,
/// the handler receives one HTTP request per row with envelope wrapper.
///
/// Ported from: test_external_handlers_single_row_envelope_one
#[tokio::test]
async fn test_external_handler_single_row_envelope_one() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // Start external handler server
    let handler = ExternalHandlerResource::new()
        .await
        .expect("Failed to start handler server");

    // Register schema
    ctx.kafka
        .register_schema(SLIM_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Create test records
    let records: Vec<SlimTestRecord> = vec![
        SlimTestRecord {
            id: "1".to_string(),
            data: "alpha".to_string(),
        },
        SlimTestRecord {
            id: "2".to_string(),
            data: "beta".to_string(),
        },
        SlimTestRecord {
            id: "3".to_string(),
            data: "gamma".to_string(),
        },
    ];

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Pipeline with handler transform - single row, envelope v1
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    primary_key: id

transforms:
  handler_transform:
    type: handler
    from: kafka_source
    url: {handler_url}
    one_row_per_request: true
    payload_version: 1
    primary_key: id

sinks:
  print_sink:
    type: print
    from: handler_transform
    sample_every: 1
"#,
        topic = ctx.kafka_topic,
        handler_url = handler.slim_handler_envelope_url()
    );

    let output = ctx
        .run_pipeline_with_capture(&pipeline, PipelineOpts::new().record_limit(3))
        .await
        .expect("Pipeline execution failed");

    // Verify that we got 3 records with updated data
    assert_eq!(output.rows().len(), 3, "Expected 3 output rows");

    // Verify the handler was called once per row (single row mode)
    assert_eq!(
        handler.request_count(),
        3,
        "Handler should receive 3 requests (one per row)"
    );

    // Verify the data was updated by the handler
    let data_values: Vec<&str> = output
        .rows()
        .iter()
        .filter_map(|r| r.data.get("data").and_then(|v| v.as_str()))
        .collect();

    assert!(
        data_values.iter().all(|d| d.starts_with("updated-")),
        "All data values should be prefixed with 'updated-': {:?}",
        data_values
    );
}

/// Test external handler with batch requests and envelope version 0.
///
/// This test verifies that when `one_row_per_request: false` and `payload_version: 0`,
/// the handler receives batched records in a single HTTP request.
///
/// Ported from: test_external_handlers_batch_envelope_zero
#[tokio::test]
async fn test_external_handler_batch_envelope_zero() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // Start external handler server
    let handler = ExternalHandlerResource::new()
        .await
        .expect("Failed to start handler server");

    // Register schema
    ctx.kafka
        .register_schema(SLIM_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Create test records
    let records: Vec<SlimTestRecord> = vec![
        SlimTestRecord {
            id: "1".to_string(),
            data: "alpha".to_string(),
        },
        SlimTestRecord {
            id: "2".to_string(),
            data: "beta".to_string(),
        },
        SlimTestRecord {
            id: "3".to_string(),
            data: "gamma".to_string(),
        },
    ];

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Pipeline with handler transform - batch mode, envelope v0
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    primary_key: id

transforms:
  handler_transform:
    type: handler
    from: kafka_source
    url: {handler_url}
    one_row_per_request: false
    payload_version: 0
    primary_key: id

sinks:
  print_sink:
    type: print
    from: handler_transform
    sample_every: 1
"#,
        topic = ctx.kafka_topic,
        handler_url = handler.slim_batch_handler_url()
    );

    let output = ctx
        .run_pipeline_with_capture(&pipeline, PipelineOpts::new().record_limit(3))
        .await
        .expect("Pipeline execution failed");

    // Verify that we got 3 records with updated data
    assert_eq!(output.rows().len(), 3, "Expected 3 output rows");

    // In batch mode, handler should receive fewer requests (ideally 1 for all rows)
    assert!(
        handler.request_count() <= 3,
        "Handler should receive at most 3 requests in batch mode"
    );

    // Verify the data was updated by the handler
    let data_values: Vec<&str> = output
        .rows()
        .iter()
        .filter_map(|r| r.data.get("data").and_then(|v| v.as_str()))
        .collect();

    assert!(
        data_values.iter().all(|d| d.starts_with("updated-")),
        "All data values should be prefixed with 'updated-': {:?}",
        data_values
    );
}

/// Test external handler with batch requests and envelope version 1.
///
/// This test verifies that when `one_row_per_request: false` and `payload_version: 1`,
/// the handler receives batched records with envelope wrapper.
///
/// Ported from: test_external_handlers_batch_envelope_one
#[tokio::test]
async fn test_external_handler_batch_envelope_one() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // Start external handler server
    let handler = ExternalHandlerResource::new()
        .await
        .expect("Failed to start handler server");

    // Register schema
    ctx.kafka
        .register_schema(SLIM_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Create test records
    let records: Vec<SlimTestRecord> = vec![
        SlimTestRecord {
            id: "1".to_string(),
            data: "alpha".to_string(),
        },
        SlimTestRecord {
            id: "2".to_string(),
            data: "beta".to_string(),
        },
        SlimTestRecord {
            id: "3".to_string(),
            data: "gamma".to_string(),
        },
    ];

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Pipeline with handler transform - batch mode, envelope v1
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    primary_key: id

transforms:
  handler_transform:
    type: handler
    from: kafka_source
    url: {handler_url}
    one_row_per_request: false
    payload_version: 1
    primary_key: id

sinks:
  print_sink:
    type: print
    from: handler_transform
    sample_every: 1
"#,
        topic = ctx.kafka_topic,
        handler_url = handler.slim_batch_handler_envelope_url()
    );

    let output = ctx
        .run_pipeline_with_capture(&pipeline, PipelineOpts::new().record_limit(3))
        .await
        .expect("Pipeline execution failed");

    // Verify that we got 3 records with updated data
    assert_eq!(output.rows().len(), 3, "Expected 3 output rows");

    // In batch mode, handler should receive fewer requests (ideally 1 for all rows)
    assert!(
        handler.request_count() <= 3,
        "Handler should receive at most 3 requests in batch mode"
    );

    // Verify the data was updated by the handler
    let data_values: Vec<&str> = output
        .rows()
        .iter()
        .filter_map(|r| r.data.get("data").and_then(|v| v.as_str()))
        .collect();

    assert!(
        data_values.iter().all(|d| d.starts_with("updated-")),
        "All data values should be prefixed with 'updated-': {:?}",
        data_values
    );
}

/// Test external handler request capture.
///
/// This test verifies that the handler correctly captures incoming requests
/// that can be inspected for verification.
#[tokio::test]
async fn test_external_handler_request_capture() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // Start external handler server
    let handler = ExternalHandlerResource::new()
        .await
        .expect("Failed to start handler server");

    // Register schema
    ctx.kafka
        .register_schema(SLIM_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Create test records
    let records: Vec<SlimTestRecord> = vec![SlimTestRecord {
        id: "1".to_string(),
        data: "test_data".to_string(),
    }];

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Pipeline with passthrough handler
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    primary_key: id

transforms:
  handler_transform:
    type: handler
    from: kafka_source
    url: {handler_url}
    one_row_per_request: true
    payload_version: 0
    primary_key: id

sinks:
  print_sink:
    type: print
    from: handler_transform
    sample_every: 1
"#,
        topic = ctx.kafka_topic,
        handler_url = handler.passthrough_handler_url()
    );

    let _ = ctx
        .run_pipeline_with_opts(&pipeline, PipelineOpts::new().record_limit(1))
        .await
        .expect("Pipeline execution failed");

    // Verify the handler captured the request
    assert_eq!(
        handler.request_count(),
        1,
        "Handler should have captured 1 request"
    );

    let requests = handler.get_requests();
    assert!(!requests.is_empty(), "Should have captured requests");
    assert_eq!(requests[0].endpoint, "handler_passthrough");
    assert!(
        requests[0].body.contains("test_data"),
        "Request body should contain test_data"
    );
}

// ============================================================================
// Generic batch accumulation on handler transform
// ============================================================================

/// Test that batch_size on a handler transform controls the number of rows
/// sent per HTTP request when one_row_per_request is false.
///
/// Forces the Kafka source to emit one-row batches (RECORD_BATCH_SIZE=1),
/// then configures batch_size=5 on the handler transform. The wrapping layer
/// should accumulate rows and send them to the HTTP handler in batches of 5.
#[tokio::test]
async fn test_handler_transform_batch_accumulation() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    let handler = ExternalHandlerResource::new()
        .await
        .expect("Failed to start handler server");

    ctx.kafka
        .register_schema(SLIM_SCHEMA)
        .await
        .expect("Failed to register schema");

    let total_records = 20;
    let records: Vec<SlimTestRecord> = (1..=total_records)
        .map(|i| SlimTestRecord {
            id: i.to_string(),
            data: format!("item_{}", i),
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let batch_size = 5;
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    primary_key: id
    batch_size: 1

transforms:
  handler_transform:
    type: handler
    from: kafka_source
    url: {handler_url}
    one_row_per_request: false
    payload_version: 0
    primary_key: id
    batch_size: {batch_size}
    batch_flush_interval: 5s

sinks:
  print_sink:
    type: print
    from: handler_transform
    sample_every: 1
"#,
        topic = ctx.kafka_topic,
        handler_url = handler.slim_batch_handler_url(),
        batch_size = batch_size,
    );

    let output = ctx
        .run_pipeline_with_capture(
            &pipeline,
            PipelineOpts::new().record_limit(total_records as u64),
        )
        .await
        .expect("Pipeline execution failed");

    // All records should pass through the handler and reach the print sink
    assert_eq!(
        output.rows().len(),
        total_records,
        "All {} records should reach the print sink",
        total_records
    );

    // Verify the handler received batched requests, not one-per-row
    let requests = handler.get_requests();
    assert!(
        !requests.is_empty(),
        "Handler should have received at least one request"
    );

    // Each request body is a JSON array. Parse and check sizes.
    let request_sizes: Vec<usize> = requests
        .iter()
        .map(|r| {
            let parsed: Vec<serde_json::Value> =
                serde_json::from_str(&r.body).expect("Request body should be a JSON array");
            parsed.len()
        })
        .collect();

    let total_rows_received: usize = request_sizes.iter().sum();
    assert_eq!(
        total_rows_received, total_records,
        "Handler should have received all {} records across all requests",
        total_records
    );

    // With batch_size=5 and 20 records, we expect around 4 requests of 5 rows each.
    // Due to timing, the last batch may be smaller, but no batch should exceed batch_size.
    for (i, size) in request_sizes.iter().enumerate() {
        assert!(
            *size <= batch_size,
            "Request {} had {} rows, which exceeds batch_size={}",
            i,
            size,
            batch_size
        );
    }

    // Without batching, we'd get as many requests as input batches (20 with RECORD_BATCH_SIZE=1).
    // With batch_size=5, we should get significantly fewer.
    assert!(
        requests.len() <= total_records / batch_size + 1,
        "Expected at most {} requests with batch_size={}, got {}",
        total_records / batch_size + 1,
        batch_size,
        requests.len()
    );
}
