use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use futures::future::try_join_all;
use schema_registry_converter::async_impl::schema_registry::{
    SrSettings, get_all_versions, perform_sr_call,
};
use schema_registry_converter::error::SRCError;
use schema_registry_converter::schema_registry_common::SrCall;
use streamling_core::error::StreamlingError;
use streamling_core::retry::retry_if_retriable;
use streamling_core::streamling_err;
use tokio::sync::OnceCell;

/// `(schema_id -> subject-local version)` map for a single subject.
type IdToVersionMap = Arc<HashMap<u32, u32>>;

/// Per-subject cache slot. The `OnceCell` doubles as a single-flight primitive so concurrent
/// callers that miss for the same subject share one HTTP fetch instead of stampeding the registry.
type SubjectCacheSlot = Arc<OnceCell<IdToVersionMap>>;

/// Read-only wrapper over the schema registry that resolves a `(subject, schema_id)` pair to its
/// subject-local version number. Used to decide whether a writer schema is newer than a reader
/// schema for the same subject, without depending on monotonically-increasing global IDs.
///
/// The mapping `(subject, id) -> version` is immutable in the schema registry: once a schema is
/// registered, neither its id nor its position in the subject's version list ever changes. Newly
/// registered schemas only add new pairs. The cache therefore needs no invalidation for known ids.
///
/// Schemas registered *after* the cache populated will not be present; `version_for_id` returns
/// `Ok(None)` in that case so callers can decide what to do (`is_writer_schema_ahead` treats an
/// unknown writer id as semantically newer than the reader, which produces the same fail-fast
/// "restart to refetch" path on-call already expects during a schema rollout).
pub struct SubjectVersionsClient {
    settings: SrSettings,
    cache: DashMap<String, SubjectCacheSlot>,
}

impl SubjectVersionsClient {
    pub fn new(settings: SrSettings) -> Self {
        Self {
            settings,
            cache: DashMap::new(),
        }
    }

    /// Returns the subject-local version for a schema id, or `Ok(None)` when the id is not in the
    /// subject's version list. The `None` case happens for schemas registered after the cache was
    /// populated; callers decide whether that's an error or a signal (see `is_writer_schema_ahead`).
    pub async fn version_for_id(
        &self,
        subject: &str,
        id: u32,
    ) -> streamling_core::error::Result<Option<u32>> {
        let cell = if let Some(slot) = self.cache.get(subject) {
            slot.clone()
        } else {
            self.cache.entry(subject.to_owned()).or_default().clone()
        };

        let map = cell
            .get_or_try_init(|| async { Self::fetch_subject_map(&self.settings, subject).await })
            .await?;

        Ok(map.get(&id).copied())
    }

    async fn fetch_subject_map(
        settings: &SrSettings,
        subject: &str,
    ) -> streamling_core::error::Result<IdToVersionMap> {
        let versions = retry_registry_call(
            format!("schema-registry get_all_versions(subject='{subject}')"),
            || get_all_versions(settings, subject.to_owned()),
        )
        .await?;

        // Fetch all (id, version) pairs concurrently; with N versions this becomes one round-trip
        // bound by the slowest call instead of N sequential round-trips. Each per-version call is
        // independently retried so a single transient blip doesn't fail the whole batch.
        let raw_schemas = try_join_all(versions.iter().map(|&version| {
            retry_registry_call(
                format!(
                    "schema-registry get_by_subject_and_version(subject='{subject}', version={version})"
                ),
                move || perform_sr_call(settings, SrCall::GetBySubjectAndVersion(subject, version)),
            )
        }))
        .await?;

        let mut map = HashMap::with_capacity(raw_schemas.len());
        for raw in raw_schemas {
            if let (Some(id), Some(v)) = (raw.id, raw.version) {
                map.insert(id, v);
            }
        }
        Ok(Arc::new(map))
    }

    /// Pre-populate the cache for a subject. For tests; bypasses HTTP entirely.
    #[cfg(test)]
    pub fn with_seeded(subject: impl Into<String>, id_to_version: HashMap<u32, u32>) -> Self {
        let cell: SubjectCacheSlot = Arc::new(OnceCell::new());
        cell.set(Arc::new(id_to_version)).ok();
        let cache = DashMap::new();
        cache.insert(subject.into(), cell);
        Self {
            // any url; we never call out to it because the cell is pre-populated.
            settings: SrSettings::new("http://test.invalid".to_string()),
            cache,
        }
    }
}

/// Run a schema-registry call through `streamling_core::retry::retry_if_retriable`, preserving
/// `SRCError`'s own `retriable` flag (which the crate already sets for HTTP-layer failures and
/// clears for non-retryable cases like parse errors). The retry helper applies exponential
/// backoff with jitter and stops as soon as a non-retriable error surfaces.
pub(crate) async fn retry_registry_call<F, Fut, T>(
    op_name: String,
    op: F,
) -> streamling_core::error::Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, SRCError>>,
{
    retry_if_retriable(
        || async {
            op().await.map_err(|e| {
                if e.retriable {
                    StreamlingError::retriable_with_cause(op_name.clone(), e)
                } else {
                    streamling_err!("{}: {}", op_name, e)
                }
            })
        },
        &op_name,
    )
    .await
}


/// Returns true when the writer schema is registered after the reader schema for the same subject.
///
/// Compares the writer's and reader's *subject-local versions* (not the global schema IDs), which
/// works correctly regardless of whether the registry assigns monotonically increasing IDs. The
/// `(subject, id) → version` mapping is immutable in the registry, so the
/// `SubjectVersionsClient` cache amortizes lookups to a single fetch per subject.
///
/// Unknown-id handling follows the *signal vs noise* split:
///
/// - *Writer id missing* is a positive signal — the schema was registered after the cache
///   populated, so the writer is ahead. Return `Ok(true)`.
/// - *Reader id missing* is registry weirdness (subject recreated, soft-delete, partial response).
///   It says nothing about the message itself, and the reader schema bytes are still cached
///   locally. Return `Ok(false)` so the caller falls through to Avro resolution against the cached
///   reader schema — same outcome the pre-version-comparison code path took for everything.
pub async fn is_writer_schema_ahead(
    versions_client: &SubjectVersionsClient,
    subject: &str,
    writer_schema_id: u32,
    reader_schema_id: u32,
) -> streamling_core::error::Result<bool> {
    if writer_schema_id == reader_schema_id {
        return Ok(false);
    }
    let writer_v = match versions_client
        .version_for_id(subject, writer_schema_id)
        .await?
    {
        Some(v) => v,
        None => return Ok(true),
    };
    let reader_v = match versions_client
        .version_for_id(subject, reader_schema_id)
        .await?
    {
        Some(v) => v,
        None => {
            tracing::warn!(
                subject,
                reader_schema_id,
                writer_schema_id,
                "reader schema id not found in subject version list — falling through to Avro \
                 resolution against the cached reader schema. Registry state may be inconsistent \
                 (subject recreated, soft-deleted, or partial /versions response)."
            );
            return Ok(false);
        }
    };
    Ok(writer_v > reader_v)
}

/// Rewrites the schema ID in a Confluent wire-format payload using a writer-id → override-id map.
///
/// Confluent wire format:
/// - Byte 0: Magic byte (0x00)
/// - Bytes 1-4: Schema ID (big-endian u32)
/// - Bytes 5+: Avro binary payload
///
/// Returns `Some(patched_bytes)` when the payload's writer schema ID is a key in `overrides`;
/// `None` otherwise (empty map, missing magic byte, payload shorter than 5 bytes, or writer id
/// not in the map).
pub fn patch_schema_id_with_overrides(
    bytes: Option<&[u8]>,
    overrides: &HashMap<u32, u32>,
) -> Option<Vec<u8>> {
    if overrides.is_empty() {
        return None;
    }
    let b = bytes?;
    if b.len() < 5 || b[0] != 0 {
        tracing::debug!(
            bytes_len = b.len(),
            magic_byte = b.first().copied(),
            "patch_schema_id_with_overrides: invalid format (too short or wrong magic byte)"
        );
        return None;
    }
    let id = u32::from_be_bytes([b[1], b[2], b[3], b[4]]);
    let &to_id = overrides.get(&id)?;
    let mut patched = b.to_vec();
    patched[1..5].copy_from_slice(&to_id.to_be_bytes());
    tracing::debug!(
        from_id = id,
        to_id,
        bytes_len = b.len(),
        "patch_schema_id_with_overrides: patched schema ID via overrides map"
    );
    Some(patched)
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::types::Value;
    use mockito::Server;

    fn mk_settings(server: &Server) -> SrSettings {
        SrSettings::new(server.url())
    }

    fn version_response(id: u32, version: u32, subject: &str) -> String {
        format!(r#"{{"subject":"{subject}","id":{id},"version":{version},"schema":"\"string\""}}"#)
    }

    #[tokio::test]
    async fn version_for_id_returns_version_and_caches() {
        let mut server = Server::new_async().await;
        let subject = "topic-value";

        let m_versions = server
            .mock("GET", format!("/subjects/{subject}/versions").as_str())
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body("[1,2]")
            .expect(1)
            .create_async()
            .await;

        let m_v1 = server
            .mock("GET", format!("/subjects/{subject}/versions/1").as_str())
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(version_response(100, 1, subject))
            .expect(1)
            .create_async()
            .await;

        let m_v2 = server
            .mock("GET", format!("/subjects/{subject}/versions/2").as_str())
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(version_response(200, 2, subject))
            .expect(1)
            .create_async()
            .await;

        let client = SubjectVersionsClient::new(mk_settings(&server));

        assert_eq!(
            client.version_for_id(subject, 100).await.unwrap().unwrap(),
            1
        );
        assert_eq!(
            client.version_for_id(subject, 200).await.unwrap().unwrap(),
            2
        );
        // Repeat — cache hit, no additional HTTP.
        assert_eq!(
            client.version_for_id(subject, 100).await.unwrap().unwrap(),
            1
        );
        assert_eq!(
            client.version_for_id(subject, 200).await.unwrap().unwrap(),
            2
        );

        m_versions.assert_async().await;
        m_v1.assert_async().await;
        m_v2.assert_async().await;
    }

    #[tokio::test]
    async fn version_for_id_handles_non_monotonic_ids() {
        // Versions are sequential (1, 2, 3) but ids deliberately out of order.
        let mut server = Server::new_async().await;
        let subject = "topic-value";

        server
            .mock("GET", format!("/subjects/{subject}/versions").as_str())
            .with_status(200)
            .with_body("[1,2,3]")
            .create_async()
            .await;
        server
            .mock("GET", format!("/subjects/{subject}/versions/1").as_str())
            .with_status(200)
            .with_body(version_response(500, 1, subject))
            .create_async()
            .await;
        server
            .mock("GET", format!("/subjects/{subject}/versions/2").as_str())
            .with_status(200)
            .with_body(version_response(100, 2, subject))
            .create_async()
            .await;
        server
            .mock("GET", format!("/subjects/{subject}/versions/3").as_str())
            .with_status(200)
            .with_body(version_response(700, 3, subject))
            .create_async()
            .await;

        let client = SubjectVersionsClient::new(mk_settings(&server));
        assert_eq!(
            client.version_for_id(subject, 500).await.unwrap().unwrap(),
            1
        );
        assert_eq!(
            client.version_for_id(subject, 100).await.unwrap().unwrap(),
            2
        );
        assert_eq!(
            client.version_for_id(subject, 700).await.unwrap().unwrap(),
            3
        );
    }

    #[tokio::test]
    async fn version_for_id_returns_error_for_unknown_id() {
        let mut server = Server::new_async().await;
        let subject = "topic-value";

        server
            .mock("GET", format!("/subjects/{subject}/versions").as_str())
            .with_status(200)
            .with_body("[1]")
            .create_async()
            .await;
        server
            .mock("GET", format!("/subjects/{subject}/versions/1").as_str())
            .with_status(200)
            .with_body(version_response(100, 1, subject))
            .create_async()
            .await;

        let client = SubjectVersionsClient::new(mk_settings(&server));
        // Unknown id → Ok(None); the caller decides what to do (see is_writer_schema_ahead).
        assert_eq!(client.version_for_id(subject, 999).await.unwrap(), None);
    }

    #[tokio::test]
    async fn is_writer_schema_ahead_treats_unknown_writer_as_ahead() {
        // Reader id is in the cache; writer id is not (e.g. registered after pipeline started).
        // is_writer_schema_ahead must return Ok(true) so the consumer fails fast and refetches.
        let mut id_to_version = HashMap::new();
        id_to_version.insert(100u32, 1u32);
        let client = SubjectVersionsClient::with_seeded("topic-value", id_to_version);

        let ahead = is_writer_schema_ahead(&client, "topic-value", 999, 100)
            .await
            .unwrap();
        assert!(ahead, "unknown writer id should be reported as ahead");
    }

    #[tokio::test]
    async fn is_writer_schema_ahead_falls_through_on_unknown_reader() {
        // Reader id missing is registry-state noise, not a message-level signal. We don't crash
        // the pod over it — return Ok(false) so the caller proceeds to Avro resolution against
        // the cached reader schema bytes.
        let mut id_to_version = HashMap::new();
        id_to_version.insert(999u32, 5u32);
        let client = SubjectVersionsClient::with_seeded("topic-value", id_to_version);

        let ahead = is_writer_schema_ahead(&client, "topic-value", 999, 100)
            .await
            .unwrap();
        assert!(
            !ahead,
            "unknown reader id should fall through to Avro resolution, not be treated as ahead"
        );
    }

    #[tokio::test]
    async fn retry_registry_call_does_not_retry_non_retriable_errors() {
        // 500 from /subjects/{subject}/versions makes reqwest succeed with a non-JSON body, which
        // SRCError classifies as non-retriable (it's a parse failure, not a transport blip). The
        // wrapper must surface the error after exactly one attempt — no infinite retry loop.
        let mut server = Server::new_async().await;
        let subject = "topic-value";

        let m = server
            .mock("GET", format!("/subjects/{subject}/versions").as_str())
            .with_status(500)
            .with_body("upstream blip")
            .expect(1)
            .create_async()
            .await;

        let client = SubjectVersionsClient::new(mk_settings(&server));
        let err = client.version_for_id(subject, 100).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("could not parse to list of versions"),
            "expected SRCError parse-failure message, got: {err}"
        );

        m.assert_async().await;
    }

    #[tokio::test]
    async fn retry_registry_call_retries_on_retriable_srcerror() {
        // Drive `retry_registry_call` directly with a fake SRCError that is *retriable*. This
        // mirrors what happens in production when the schema_registry_converter crate returns a
        // network-layer failure: SRCError::retryable_with_cause sets retriable=true.
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};

        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let result: streamling_core::error::Result<u32> =
            retry_registry_call("test_op".to_string(), move || {
                let attempts = attempts_clone.clone();
                async move {
                    let n = attempts.fetch_add(1, Ordering::SeqCst);
                    if n < 2 {
                        Err(SRCError::retryable_with_cause(
                            "transient",
                            "fake network blip",
                        ))
                    } else {
                        Ok(42u32)
                    }
                }
            })
            .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            3,
            "expected 2 retries before success"
        );
    }

    #[tokio::test]
    async fn with_seeded_bypasses_http() {
        let mut id_to_version = HashMap::new();
        id_to_version.insert(1293, 2);
        id_to_version.insert(4605, 5);
        let client = SubjectVersionsClient::with_seeded("topic-value", id_to_version);

        assert_eq!(
            client
                .version_for_id("topic-value", 1293)
                .await
                .unwrap()
                .unwrap(),
            2
        );
        assert_eq!(
            client
                .version_for_id("topic-value", 4605)
                .await
                .unwrap()
                .unwrap(),
            5
        );
    }

    fn make_payload(schema_id: u32) -> Vec<u8> {
        let mut bytes = vec![0x00];
        bytes.extend_from_slice(&schema_id.to_be_bytes());
        bytes.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        bytes
    }

    fn read_schema_id(bytes: &[u8]) -> u32 {
        u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]])
    }

    #[test]
    fn patch_schema_id_with_overrides_empty_map_returns_none() {
        let payload = make_payload(10);
        let overrides: HashMap<u32, u32> = HashMap::new();
        assert!(patch_schema_id_with_overrides(Some(&payload), &overrides).is_none());
    }

    #[test]
    fn patch_schema_id_with_overrides_patches_when_id_matches() {
        let payload = make_payload(10);
        let overrides: HashMap<u32, u32> = [(10, 20)].into_iter().collect();
        let patched = patch_schema_id_with_overrides(Some(&payload), &overrides).unwrap();
        assert_eq!(read_schema_id(&patched), 20);
        assert_eq!(&patched[5..], &payload[5..]);
    }

    #[test]
    fn patch_schema_id_with_overrides_multiple_entries() {
        let overrides: HashMap<u32, u32> = [(1, 100), (2, 200), (3, 300)].into_iter().collect();
        for (from, to) in [(1u32, 100u32), (2, 200), (3, 300)] {
            let payload = make_payload(from);
            let patched = patch_schema_id_with_overrides(Some(&payload), &overrides).unwrap();
            assert_eq!(read_schema_id(&patched), to);
        }
    }

    #[test]
    fn patch_schema_id_with_overrides_returns_none_when_id_not_in_map() {
        let payload = make_payload(999);
        let overrides: HashMap<u32, u32> = [(10, 20)].into_iter().collect();
        assert!(patch_schema_id_with_overrides(Some(&payload), &overrides).is_none());
    }

    #[test]
    fn patch_schema_id_with_overrides_rejects_short_payload() {
        let short = vec![0x00, 0x00, 0x00];
        let overrides: HashMap<u32, u32> = [(0, 1)].into_iter().collect();
        assert!(patch_schema_id_with_overrides(Some(&short), &overrides).is_none());
    }

    #[test]
    fn patch_schema_id_with_overrides_rejects_wrong_magic_byte() {
        let mut payload = make_payload(10);
        payload[0] = 0x01;
        let overrides: HashMap<u32, u32> = [(10, 20)].into_iter().collect();
        assert!(patch_schema_id_with_overrides(Some(&payload), &overrides).is_none());
    }

    #[test]
    fn patch_schema_id_with_overrides_handles_none_input() {
        let overrides: HashMap<u32, u32> = [(1, 2)].into_iter().collect();
        assert!(patch_schema_id_with_overrides(None, &overrides).is_none());
    }

    /// Integration tests for patch_schema_id with mocked schema registry
    mod patch_schema_id_integration {
        use super::*;
        use schema_registry_converter::async_impl::avro::AvroDecoder;
        use std::sync::atomic::{AtomicU32, Ordering};

        // Use a counter to generate unique schema IDs for each test
        // This avoids cache conflicts since schema_registry_converter caches by schema ID
        static SCHEMA_ID_COUNTER: AtomicU32 = AtomicU32::new(100_000);

        fn next_unique_schema_id() -> u32 {
            SCHEMA_ID_COUNTER.fetch_add(1, Ordering::SeqCst)
        }

        /// Load a test schema from the testdata/avro-schema directory
        fn load_test_schema(filename: &str) -> String {
            let path = format!(
                "{}/testdata/avro-schema/{}",
                env!("CARGO_MANIFEST_DIR"),
                filename
            );
            std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("Failed to load schema from {}: {}", path, e))
        }

        /// Load a binary payload from the testdata/avro-payload directory
        fn load_test_payload(filename: &str) -> Vec<u8> {
            let path = format!(
                "{}/testdata/avro-payload/{}",
                env!("CARGO_MANIFEST_DIR"),
                filename
            );
            std::fs::read(&path)
                .unwrap_or_else(|e| panic!("Failed to load payload from {}: {}", path, e))
        }

        /// Shared helper to decode and verify a payload with schema ID patching
        async fn decode_and_verify_payload(
            payload_file: &str,
            schema_file: &str,
            original_schema_id: u32,
        ) {
            let mut server = mockito::Server::new_async().await;

            // Use a unique target schema ID to avoid cache conflicts
            let target_schema_id = next_unique_schema_id();

            let schema = load_test_schema(schema_file);
            let response_body = serde_json::json!({
                "id": target_schema_id,
                "schema": schema
            });

            let mock_target = server
                .mock(
                    "GET",
                    format!("/schemas/ids/{}?deleted=true", target_schema_id).as_str(),
                )
                .with_status(200)
                .with_header("content-type", "application/vnd.schemaregistry.v1+json")
                .with_body(serde_json::to_string(&response_body).unwrap())
                .expect_at_least(1)
                .create_async()
                .await;

            let sr_settings = SrSettings::new(server.url());
            let decoder = AvroDecoder::new(sr_settings);

            // Load real payload
            let payload = load_test_payload(payload_file);

            // Verify the payload has the expected schema ID
            let actual_id = u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]);
            assert_eq!(
                actual_id, original_schema_id,
                "Payload {} should have schema ID {}",
                payload_file, original_schema_id
            );

            // Patch the schema ID to target
            let overrides: HashMap<u32, u32> = [(original_schema_id, target_schema_id)]
                .into_iter()
                .collect();
            let patched = patch_schema_id_with_overrides(Some(&payload), &overrides);
            assert!(
                patched.is_some(),
                "Schema ID {} should be patched for {}",
                original_schema_id,
                payload_file
            );
            let patched_payload = patched.unwrap();

            // Verify the patched schema ID
            let patched_id = u32::from_be_bytes([
                patched_payload[1],
                patched_payload[2],
                patched_payload[3],
                patched_payload[4],
            ]);
            assert_eq!(patched_id, target_schema_id);

            // Decode the patched payload
            let decoded = decoder
                .decode_with_schema(Some(&patched_payload))
                .await
                .unwrap_or_else(|e| panic!("Decoding {} should succeed: {:?}", payload_file, e))
                .unwrap_or_else(|| panic!("Decoded value for {} should not be None", payload_file));

            assert_eq!(decoded.schema.id, target_schema_id);

            if let Value::Union(_, inner) = decoded.value {
                if let Value::Record(fields) = *inner {
                    let field_names: Vec<&str> =
                        fields.iter().map(|(name, _)| name.as_str()).collect();
                    assert!(
                        field_names.contains(&"id"),
                        "{} should have 'id' field",
                        payload_file
                    );
                    assert!(
                        field_names.contains(&"block_number"),
                        "{} should have 'block_number' field",
                        payload_file
                    );
                    assert!(
                        field_names.contains(&"transaction_hash"),
                        "{} should have 'transaction_hash' field",
                        payload_file
                    );
                } else {
                    panic!("Expected Record inside Union for {}", payload_file);
                }
            } else {
                panic!("Expected Union value for {}", payload_file);
            }

            mock_target.assert_async().await;
        }

        #[tokio::test]
        async fn test_decode_real_payload_v1_1271() {
            for payload_file in [
                "arbitrum-one.raw.traces-1271.bin",
                "arbitrum-one.raw.traces-1271-p0-o366.bin",
            ] {
                decode_and_verify_payload(payload_file, "traces-value-v1-1271.json", 1271).await;
            }
        }

        #[tokio::test]
        async fn test_decode_real_payload_v2_1293() {
            decode_and_verify_payload(
                "arbitrum-one.raw.traces-1293.bin",
                "traces-value-v5-4605.json",
                1293,
            )
            .await;
        }
    }
}
