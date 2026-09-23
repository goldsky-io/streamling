//! Listing a source's files and their Hive partition values.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::helpers::parse_partitions_for_path;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::error::Result as DataFusionResult;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use tracing::debug;

/// Where a source's files live and how to recognize them.
#[derive(Clone)]
pub(super) struct FileDiscovery {
    pub(super) table_url: ListingTableUrl,
    pub(super) file_extension: String,
    /// Hive partition columns inferred from the path (empty for flat layouts).
    pub(super) partition_cols: Vec<(String, DataType)>,
}

impl FileDiscovery {
    /// Lists the objects under the URL that match its glob (if any) and pass
    /// `keep`, with their Hive partition values. Files outside the partition
    /// layout are skipped.
    pub(super) async fn list(
        &self,
        object_store: &dyn ObjectStore,
        keep: impl Fn(&ObjectMeta) -> bool,
    ) -> DataFusionResult<Vec<PartitionedFile>> {
        let mut files = Vec::new();
        // A HEADed single object flows through the same checks as a listed one.
        let mut candidates = list_candidates(object_store, &self.table_url).await;
        while let Some(meta) = candidates.next().await {
            let meta = meta?;
            if !self.table_url.contains(&meta.location, false) || !keep(&meta) {
                debug!(
                    "Skipping file {}, last modified at {}",
                    meta.location, meta.last_modified
                );
                continue;
            }
            let Some(partition_values) =
                partition_values_for(&self.table_url, &meta.location, &self.partition_cols)?
            else {
                debug!("Skipping file outside partition layout: {}", meta.location);
                continue;
            };
            files.push(PartitionedFile {
                object_meta: meta,
                partition_values,
                range: None,
                statistics: None,
                ordering: None,
                extensions: Default::default(),
                metadata_size_hint: None,
                table_reference: None,
            });
        }
        Ok(files)
    }

    pub(super) fn has_extension(&self, meta: &ObjectMeta) -> bool {
        meta.location.extension() == Some(self.file_extension.as_str())
    }
}

/// The candidate objects for one listing.
///
/// A non-collection URL names one exact object, which a prefix listing never
/// returns (object_store matches prefixes on whole path segments), so it is
/// HEADed instead. A `NotFound` falls back to a prefix listing, mirroring
/// DataFusion's `ListingTableUrl::list_prefixed_files`: a remote directory URL
/// without a trailing slash is also non-collection, and schema inference
/// resolves it through that same fallback, so a source configured that way would
/// otherwise start and then tear down on its first poll. Other errors ride the
/// stream, keeping one error path in the caller.
async fn list_candidates(
    object_store: &dyn ObjectStore,
    table_url: &ListingTableUrl,
) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
    if table_url.is_collection() {
        return object_store.list(Some(table_url.prefix()));
    }
    match object_store.head(table_url.prefix()).await {
        Err(object_store::Error::NotFound { .. }) => object_store.list(Some(table_url.prefix())),
        result => futures::stream::iter([result]).boxed(),
    }
}

/// Detects Hive partition column names from a sample of file paths: the leading
/// run of `key=value` parent directory segments, required to be consistent across
/// the sample. Plain (non `key=value`) directories yield no partition columns, so
/// arbitrary nested subfolders are read without spurious partition columns.
///
/// This replaces DataFusion's `infer_partitions_from_path`, which treats every
/// parent directory as a partition level and so rejects plain nested subfolders.
fn detect_partition_columns(
    table_url: &ListingTableUrl,
    sample_paths: &[object_store::path::Path],
) -> Vec<String> {
    let per_file: Vec<Vec<String>> = sample_paths
        .iter()
        .filter_map(|path| {
            let segments: Vec<&str> = table_url.strip_prefix(path)?.collect();
            // Parent directories only (drop the filename), then the leading run of
            // `key=value` segments.
            let parents = segments
                .split_last()
                .map(|(_, parents)| parents)
                .unwrap_or(&[]);
            let keys = parents
                .iter()
                .take_while(|segment| segment.contains('='))
                .map(|segment| segment.split('=').next().unwrap_or(segment).to_string())
                .collect();
            Some(keys)
        })
        .collect();

    match per_file.split_first() {
        Some((first, rest)) if rest.iter().all(|keys| keys == first) => first.clone(),
        _ => Vec::new(),
    }
}

/// Lists a sample of files under the prefix and detects the Hive partition
/// columns, typed as DataFusion types inferred partitions. Empty for flat /
/// plain-subfolder layouts.
pub(super) async fn infer_partition_columns(
    table_url: &ListingTableUrl,
    file_extension: &str,
    object_store: &dyn object_store::ObjectStore,
    sample_size: usize,
) -> Vec<(String, DataType)> {
    let sample: Vec<object_store::path::Path> = object_store
        .list(Some(table_url.prefix()))
        .filter_map(|meta| {
            let extension = file_extension.to_string();
            async move {
                meta.ok()
                    .filter(|object| object.location.extension() == Some(extension.as_str()))
                    .map(|object| object.location)
            }
        })
        .take(sample_size)
        .collect()
        .await;
    detect_partition_columns(table_url, &sample)
        .into_iter()
        .map(|name| {
            (
                name,
                DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
            )
        })
        .collect()
}

/// Parses Hive partition values for a file path against the configured partition
/// columns (typed as DataFusion's `infer_partitions_from_path` produced them).
/// `Ok(None)` means the file is not under the expected `key=value` layout and
/// should be skipped; `Ok(Some(vec![]))` is returned for flat / plain-subfolder
/// layouts that have no partition columns.
fn partition_values_for(
    table_url: &ListingTableUrl,
    file_path: &object_store::path::Path,
    partition_cols: &[(String, DataType)],
) -> DataFusionResult<Option<Vec<ScalarValue>>> {
    if partition_cols.is_empty() {
        return Ok(Some(vec![]));
    }
    match parse_partitions_for_path(
        table_url,
        file_path,
        partition_cols.iter().map(|(name, _)| name.as_str()),
    ) {
        None => Ok(None),
        Some(values) => values
            .into_iter()
            .zip(partition_cols)
            .map(|(value, (_, datatype))| ScalarValue::try_from_string(value.to_string(), datatype))
            .collect::<DataFusionResult<Vec<_>>>()
            .map(Some),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_partition_columns_handles_hive_and_plain() {
        let url = ListingTableUrl::parse("file:///t/").unwrap();

        let hive = vec![
            object_store::path::Path::from("t/dt=2024-01-01/a.parquet"),
            object_store::path::Path::from("t/dt=2024-01-02/b.parquet"),
        ];
        assert_eq!(
            detect_partition_columns(&url, &hive),
            vec!["dt".to_string()]
        );

        let multi = vec![
            object_store::path::Path::from("t/dt=2024-01-01/region=us/a.parquet"),
            object_store::path::Path::from("t/dt=2024-01-02/region=eu/b.parquet"),
        ];
        assert_eq!(
            detect_partition_columns(&url, &multi),
            vec!["dt".to_string(), "region".to_string()]
        );

        // Plain nested subfolders (no `key=value`) → no partition columns, even at
        // mixed depths.
        let plain = vec![
            object_store::path::Path::from("t/a/x.parquet"),
            object_store::path::Path::from("t/b/c/y.parquet"),
        ];
        assert!(detect_partition_columns(&url, &plain).is_empty());

        // Flat layout → no partition columns.
        let flat = vec![object_store::path::Path::from("t/x.parquet")];
        assert!(detect_partition_columns(&url, &flat).is_empty());
    }

    #[test]
    fn partition_values_for_parses_hive_layout() {
        let table_url = ListingTableUrl::parse("file:///tablepath/").unwrap();
        let cols = vec![
            ("dt".to_string(), DataType::Utf8),
            ("region".to_string(), DataType::Utf8),
        ];

        // A file under a `dt=…/region=…/` layout yields its partition values.
        let path = object_store::path::Path::from("tablepath/dt=2024-01-01/region=us/f.parquet");
        let values = partition_values_for(&table_url, &path, &cols)
            .unwrap()
            .unwrap();
        assert_eq!(
            values,
            vec![ScalarValue::from("2024-01-01"), ScalarValue::from("us")]
        );

        // No partition columns → empty values (flat / plain-subfolder layout).
        assert_eq!(
            partition_values_for(&table_url, &path, &[]).unwrap(),
            Some(vec![])
        );

        // A file not under the expected partition layout is skipped.
        let flat = object_store::path::Path::from("tablepath/f.parquet");
        assert_eq!(
            partition_values_for(&table_url, &flat, &cols).unwrap(),
            None
        );
    }

    /// A remote directory URL without a trailing slash is not a collection, so
    /// the listing HEADs it and gets NotFound. Schema inference resolves such a URL
    /// by falling back to a prefix listing, so the source must agree — otherwise
    /// it starts and dies on its first poll.
    #[tokio::test]
    async fn poll_candidates_fall_back_to_listing_when_head_misses() {
        use futures::TryStreamExt;
        use object_store::memory::InMemory;
        use object_store::path::Path;

        let store = InMemory::new();
        store
            .put(&Path::from("data/1.csv"), "id\n1".into())
            .await
            .unwrap();

        let table_url = ListingTableUrl::parse("memory:///data").unwrap();
        assert!(!table_url.is_collection(), "no trailing slash");

        let found: Vec<ObjectMeta> = list_candidates(&store, &table_url)
            .await
            .try_collect()
            .await
            .unwrap();
        let locations: Vec<&str> = found.iter().map(|meta| meta.location.as_ref()).collect();
        assert_eq!(locations, vec!["data/1.csv"]);
    }
}
