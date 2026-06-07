//! Test inspection utilities for debugging e2e tests.

use crate::resources::{ClickHouseResource, KafkaResource, PostgresResource};
use crate::{E2eConfig, E2eError, Result};
use tracing::info;

/// Inspect resources for a specific test UUID
pub async fn inspect_test(test_uuid: &str) -> Result<()> {
    // Extract UUID part (remove 'test_' prefix if present)
    let uuid = test_uuid.strip_prefix("test_").unwrap_or(test_uuid);

    println!("\n=== Inspecting resources for test UUID: {} ===\n", uuid);

    let config = E2eConfig::from_env()?;

    // ============================================================================
    // Kafka Topic Inspection
    // ============================================================================

    println!("=== Kafka Topic: test_{}_topic ===", uuid);
    let topic = format!("test_{}_topic", uuid);

    // Inspect messages in the topic (without creating it)
    info!("Checking topic and fetching messages...");
    let (messages, highest_offset) = KafkaResource::inspect_topic_messages(
        &config.kafka_broker,
        &config.schema_registry_url,
        &topic,
        100,
        20,
    )
    .await?;

    if messages.is_empty() {
        println!("[WARN] Topic {} does not exist or has no messages", topic);
    } else {
        for (offset, key, id_str) in &messages {
            println!("{}\t{}\t{}", offset, key, id_str);
        }
        if let Some(max_offset) = highest_offset {
            println!("[INFO] Highest offset: {}", max_offset);
        }
    }

    // ============================================================================
    // PostgreSQL Database Inspection
    // ============================================================================

    println!("\n=== PostgreSQL Database: test_{} ===", uuid);
    let pg_db = format!("test_{}", uuid);

    // Try to connect to the database
    // E2E_POSTGRES_URL format: postgres://user:pass@host:port/db?sslmode=disable
    // We need to extract the base URL and connect to 'postgres' database first
    let parsed_url = url::Url::parse(&config.postgres_url)
        .map_err(|e| E2eError::Postgres(sqlx::Error::Configuration(e.to_string().into())))?;

    // Build admin URL pointing to 'postgres' database
    let mut admin_url = parsed_url.clone();
    admin_url.set_path("/postgres");
    let admin_url_str = admin_url.as_str();

    match PostgresResource::connect_existing(admin_url_str, &pg_db).await {
        Ok(postgres) => {
            info!("Database exists. Listing tables...");

            let tables = postgres.list_tables().await?;

            if tables.is_empty() {
                println!("[WARN] No tables found in database {}", pg_db);
            } else {
                for table in &tables {
                    println!("\nTable: {}", table);
                    let count = postgres
                        .count(&format!("SELECT COUNT(*) FROM public.\"{}\"", table))
                        .await?;
                    println!("Row count: {}", count);

                    if count > 0 {
                        println!("Sample data (first 5 rows):");
                        let columns = postgres.get_column_names(table).await?;
                        println!("{}", columns.join("\t"));

                        let sample_data = postgres.get_sample_data(table, 5).await?;
                        for row in sample_data {
                            println!("{}", row.join("\t"));
                        }
                    }
                }
            }
        }
        Err(e) => {
            println!("[WARN] PostgreSQL database {} does not exist: {}", pg_db, e);
        }
    }

    // ============================================================================
    // ClickHouse Database Inspection
    // ============================================================================

    println!("\n=== ClickHouse Database: test_{} ===", uuid);
    let ch_db = format!("test_{}", uuid);

    match ClickHouseResource::connect_existing(&config.clickhouse_url, &ch_db).await {
        Ok(clickhouse) => {
            info!("Database exists. Listing tables...");

            let tables = clickhouse.list_tables().await?;

            if tables.is_empty() {
                println!("[WARN] No tables found in database {}", ch_db);
            } else {
                for table in &tables {
                    println!("\nTable: {}", table);
                    let count = clickhouse
                        .count(&format!("SELECT COUNT(*) FROM {}", table))
                        .await?;
                    println!("Row count: {}", count);

                    if count > 0 {
                        println!("Sample data (first 5 rows):");
                        match clickhouse.get_sample_data_formatted(table, 5).await {
                            Ok(formatted) => println!("{}", formatted),
                            Err(e) => println!("[WARN] Could not fetch sample data: {}", e),
                        }
                    }
                }
            }
        }
        Err(e) => {
            println!("[WARN] ClickHouse database {} does not exist: {}", ch_db, e);
        }
    }

    println!("\n=== Inspection Complete ===");
    Ok(())
}
