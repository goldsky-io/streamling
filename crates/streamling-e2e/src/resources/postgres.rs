//! PostgreSQL resource manager for creating isolated databases per test.

use crate::{E2eError, Result};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use std::time::Duration;
use tracing::info;

/// PostgreSQL resource manager
pub struct PostgresResource {
    /// Connection pool to the isolated database
    pool: PgPool,
    /// Connection pool to the admin database (for cleanup)
    admin_pool: PgPool,
    /// Name of the isolated database
    pub database: String,
    /// Host
    pub host: String,
    /// Port
    pub port: u16,
    /// User
    pub user: String,
    /// Password
    pub password: String,
    /// Base URL without database
    base_url: String,
    /// Query string (e.g., ?sslmode=disable)
    query_string: String,
    /// Whether to drop the database on cleanup
    should_drop: bool,
}

impl PostgresResource {
    /// Connect to an existing PostgreSQL database (for inspection, does not drop on cleanup)
    pub async fn connect_existing(admin_url: &str, database: &str) -> Result<Self> {
        // Parse the admin URL to extract components
        let parsed = url::Url::parse(admin_url)
            .map_err(|e| E2eError::Postgres(sqlx::Error::Configuration(e.to_string().into())))?;

        let host = parsed.host_str().unwrap_or("localhost").to_string();
        let port = parsed.port().unwrap_or(5432);
        let user = parsed.username().to_string();
        let password = parsed.password().unwrap_or("").to_string();
        let query_string = parsed
            .query()
            .map(|q| format!("?{}", q))
            .unwrap_or_default();

        // Build base URL without database (but preserve query params)
        let base_url = format!("postgres://{}:{}@{}:{}", user, password, host, port);

        // Connect to admin database (for compatibility, but won't be used for dropping)
        let admin_pool = PgPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_secs(30))
            .connect(admin_url)
            .await?;

        // Connect to the existing database (preserve query params like sslmode)
        let db_url = format!("{}/{}{}", base_url, database, query_string);
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(30))
            .connect(&db_url)
            .await?;

        info!("Connected to existing PostgreSQL database: {}", database);

        Ok(Self {
            pool,
            admin_pool,
            database: database.to_string(),
            host,
            port,
            user,
            password,
            base_url,
            query_string,
            should_drop: false, // Don't drop when connecting to existing database
        })
    }

    /// Create a new PostgreSQL resource with an isolated database
    pub async fn new(admin_url: &str, database: &str) -> Result<Self> {
        // Parse the admin URL to extract components
        let parsed = url::Url::parse(admin_url)
            .map_err(|e| E2eError::Postgres(sqlx::Error::Configuration(e.to_string().into())))?;

        let host = parsed.host_str().unwrap_or("localhost").to_string();
        let port = parsed.port().unwrap_or(5432);
        let user = parsed.username().to_string();
        let password = parsed.password().unwrap_or("").to_string();
        let query_string = parsed
            .query()
            .map(|q| format!("?{}", q))
            .unwrap_or_default();

        // Build base URL without database (but preserve query params)
        let base_url = format!("postgres://{}:{}@{}:{}", user, password, host, port);

        // Connect to admin database
        let admin_pool = PgPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_secs(30))
            .connect(admin_url)
            .await?;

        // Create the isolated database
        // Use IF NOT EXISTS to handle race conditions in parallel tests
        sqlx::query(&format!(
            "CREATE DATABASE \"{}\"",
            database.replace('"', "\"\"")
        ))
        .execute(&admin_pool)
        .await
        .ok(); // Ignore error if database already exists

        // Connect to the isolated database (preserve query params like sslmode)
        let db_url = format!("{}/{}{}", base_url, database, query_string);
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(30))
            .connect(&db_url)
            .await?;

        info!("Connected to PostgreSQL database: {}", database);

        Ok(Self {
            pool,
            admin_pool,
            database: database.to_string(),
            host,
            port,
            user,
            password,
            base_url,
            query_string,
            should_drop: true, // Drop when created by test
        })
    }

    /// Get the connection string for the isolated database
    pub fn connection_string(&self) -> String {
        format!("{}/{}{}", self.base_url, self.database, self.query_string)
    }

    /// Get the connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Execute a SQL statement
    pub async fn execute(&self, sql: &str) -> Result<()> {
        sqlx::query(sql).execute(&self.pool).await?;
        Ok(())
    }

    /// Execute SQL on the cluster-level admin connection (the `postgres` DB),
    /// not on the isolated test database. Used for cross-database operations like
    /// `ALTER DATABASE ... SET default_transaction_read_only = on` and
    /// `pg_terminate_backend(...)` in failover tests.
    pub async fn admin_execute(&self, sql: &str) -> Result<()> {
        sqlx::query(sql).execute(&self.admin_pool).await?;
        Ok(())
    }

    /// Execute a count query and return the result
    pub async fn count(&self, query: &str) -> Result<i64> {
        let row = sqlx::query(query).fetch_one(&self.pool).await?;
        let count: i64 = row.try_get(0)?;
        Ok(count)
    }

    /// Execute a query and return typed results
    pub async fn query<T>(&self, query: &str) -> Result<Vec<T>>
    where
        T: for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> + Send + Unpin,
    {
        let rows = sqlx::query_as::<_, T>(query).fetch_all(&self.pool).await?;
        Ok(rows)
    }

    /// List all tables in the database
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        #[derive(sqlx::FromRow)]
        struct TableRow {
            tablename: String,
        }

        let tables: Vec<TableRow> = self
            .query("SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename")
            .await?;

        Ok(tables.into_iter().map(|t| t.tablename).collect())
    }

    /// Get sample data from a table (first N rows)
    pub async fn get_sample_data(&self, table: &str, limit: usize) -> Result<Vec<Vec<String>>> {
        let rows = sqlx::query(&format!(
            "SELECT * FROM public.\"{}\" LIMIT {}",
            table, limit
        ))
        .fetch_all(&self.pool)
        .await?;

        let mut results = Vec::new();
        for row in rows {
            let values: Vec<String> = (0..row.len())
                .map(|i| {
                    let val: Option<String> = row.try_get(i).ok();
                    val.unwrap_or_else(|| "NULL".to_string())
                })
                .collect();
            results.push(values);
        }

        Ok(results)
    }

    /// Get column names for a table
    pub async fn get_column_names(&self, table: &str) -> Result<Vec<String>> {
        let rows = sqlx::query(&format!("SELECT * FROM public.\"{}\" LIMIT 1", table))
            .fetch_all(&self.pool)
            .await?;

        if let Some(row) = rows.first() {
            use sqlx::Column;
            Ok(row.columns().iter().map(|c| c.name().to_string()).collect())
        } else {
            // If no rows, query the schema directly
            #[derive(sqlx::FromRow)]
            struct ColumnRow {
                column_name: String,
            }

            let columns: Vec<ColumnRow> = self
                .query(&format!(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{}' ORDER BY ordinal_position",
                    table
                ))
                .await?;

            Ok(columns.into_iter().map(|c| c.column_name).collect())
        }
    }

    /// Clean up the database (can be called explicitly if needed)
    #[allow(dead_code)]
    pub async fn cleanup(&self) -> Result<()> {
        // Close the pool connection first
        self.pool.close().await;

        // Terminate all connections to the database
        let terminate_sql = format!(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}' AND pid <> pg_backend_pid()",
            self.database.replace('\'', "''")
        );
        let _ = sqlx::query(&terminate_sql).execute(&self.admin_pool).await;

        // Small delay to ensure connections are terminated
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Drop the database
        let drop_sql = format!(
            "DROP DATABASE IF EXISTS \"{}\"",
            self.database.replace('"', "\"\"")
        );
        sqlx::query(&drop_sql).execute(&self.admin_pool).await?;

        info!("Dropped PostgreSQL database: {}", self.database);
        Ok(())
    }
}

impl Drop for PostgresResource {
    fn drop(&mut self) {
        // Only drop if this resource was created (not connected to existing)
        if !self.should_drop {
            return;
        }

        // Cleanup is best-effort since Drop is synchronous but cleanup is async
        // For reliable cleanup, call cleanup() explicitly before dropping
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let database = self.database.clone();
            let admin_pool = self.admin_pool.clone();
            let pool = self.pool.clone();

            // Spawn cleanup task (best-effort - may not complete if runtime shuts down)
            handle.spawn(async move {
                // Close the pool first to release connections
                pool.close().await;

                // Terminate any remaining connections to the database
                let terminate_sql = format!(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}' AND pid <> pg_backend_pid()",
                    database.replace('\'', "''")
                );
                let _ = sqlx::query(&terminate_sql).execute(&admin_pool).await;

                // Small delay to ensure connections are terminated
                tokio::time::sleep(Duration::from_millis(200)).await;

                // Drop the database
                let drop_sql = format!(
                    "DROP DATABASE IF EXISTS \"{}\"",
                    database.replace('"', "\"\"")
                );
                if let Err(e) = sqlx::query(&drop_sql).execute(&admin_pool).await {
                    tracing::warn!("Failed to drop database {}: {}", database, e);
                } else {
                    info!("Dropped PostgreSQL database: {}", database);
                }

                // Close admin pool
                admin_pool.close().await;
            });
        }
    }
}
