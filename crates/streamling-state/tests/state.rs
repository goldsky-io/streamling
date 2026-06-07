use crate::common::create_logger;
use serde_derive::{Deserialize, Serialize};
use serial_test::serial;
use std::sync::Arc;
use streamling_state::sqlite::SqliteStateOperatorBackendFactory;
use streamling_state::{
    StateBackendError, StateKey, StateOperatorBackend, StateOperatorBackendFactory,
};

pub mod common;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
struct TestValue {
    field1: String,
    field2: i32,
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_state_backend_sqlite() -> Result<(), StateBackendError> {
    let _ = create_logger().try_init();

    let db_file = "./test_state.db";

    let factory = SqliteStateOperatorBackendFactory::new(db_file.to_string(), None, None).await?;
    let backend: Arc<dyn StateOperatorBackend<TestValue>> = factory.create("test_ns");

    let key = StateKey::from("test_key");
    let value = TestValue {
        field1: "test_value".to_string(),
        field2: 42,
    };

    backend.put(key.clone(), value.clone()).await?;
    assert_eq!(backend.get(key.clone()).await?, Some(value.clone()));

    backend.remove(key.clone()).await?;
    assert_eq!(backend.get(key).await?, None);

    std::fs::remove_file(db_file).unwrap_or_else(|_| {
        eprintln!("Failed to delete the SQLite database file.");
    });

    Ok(())
}
