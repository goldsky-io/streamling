//! Fixtures shared by the file source's unit tests.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use futures::StreamExt;
use streamling_config::FileSourceConfig;
use streamling_core::dynamic_table::DynamicTableRegistry;
use streamling_core::session::SessionManager;
use streamling_core::topology::FileSourceFormat;
use streamling_state::in_memory::InMemoryStateOperatorBackendFactory;
use streamling_state::{StateOperatorBackend, StateOperatorBackendFactory};

use super::bounded::BoundedProgress;
use super::continuous::FileWatermark;
use super::provider::{FileSourceReadMode, FileSourceTableProvider};

pub(super) fn session_manager() -> SessionManager {
    SessionManager::new(100, 10, DynamicTableRegistry::new(), 1).unwrap()
}

/// A fresh temp directory holding `files` as `(name, contents)`.
pub(super) fn temp_dir_with(name: &str, files: &[(String, String)]) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("streamling_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    for (file, contents) in files {
        std::fs::write(dir.join(file), contents).unwrap();
    }
    dir
}

/// `count` CSV files named `{n}.csv`, each with three rows whose ids are
/// `{n}0`, `{n}1`, `{n}2`.
pub(super) fn csv_files(count: usize) -> Vec<(String, String)> {
    (0..count)
        .map(|file| {
            (
                format!("{file}.csv"),
                format!("id,name\n{file}0,alice\n{file}1,bob\n{file}2,carol"),
            )
        })
        .collect()
}

pub(super) async fn continuous_provider(
    path: &str,
    name: &str,
    parallelism: usize,
) -> (SessionManager, Arc<FileSourceTableProvider>) {
    let session_manager = session_manager();
    let provider = FileSourceTableProvider::try_new(
        name,
        path,
        FileSourceFormat::Csv,
        FileSourceReadMode::Continuous {
            poll_interval: Duration::from_millis(100),
            parallelism,
            state_backend: InMemoryStateOperatorBackendFactory::new()
                .unwrap()
                .create::<FileWatermark>(name),
        },
        &session_manager,
        None,
        10,
        &FileSourceConfig::default(),
    )
    .await
    .unwrap();
    (session_manager, provider)
}

pub(super) async fn bounded_provider(
    dir: &std::path::Path,
    name: &str,
    parallelism: Option<usize>,
    state_backend: Arc<dyn StateOperatorBackend<BoundedProgress>>,
    num_records_before_stop: Option<u64>,
) -> (SessionManager, Arc<FileSourceTableProvider>) {
    let session_manager = session_manager();
    let provider = FileSourceTableProvider::try_new(
        name,
        &format!("{}/", dir.to_str().unwrap()),
        FileSourceFormat::Csv,
        FileSourceReadMode::Bounded {
            parallelism,
            state_backend,
            checkpoint_control: None,
        },
        &session_manager,
        num_records_before_stop,
        10,
        &FileSourceConfig::default(),
    )
    .await
    .unwrap();
    (session_manager, provider)
}

pub(super) fn bounded_backend(namespace: &str) -> Arc<dyn StateOperatorBackend<BoundedProgress>> {
    InMemoryStateOperatorBackendFactory::new()
        .unwrap()
        .create::<BoundedProgress>(namespace)
}

/// Reads every partition of `plan` to the end of its stream.
pub(super) async fn collect_partitions(
    plan: &Arc<dyn ExecutionPlan>,
    session_manager: &SessionManager,
) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    for partition in 0..plan.output_partitioning().partition_count() {
        let mut stream = plan
            .execute(partition, session_manager.session_context().task_ctx())
            .unwrap();
        while let Some(batch) = stream.next().await {
            batches.push(batch.unwrap());
        }
    }
    batches
}

pub(super) fn int64_values(batches: &[RecordBatch], column: &str) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}
