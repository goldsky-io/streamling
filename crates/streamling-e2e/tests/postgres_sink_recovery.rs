//! PostgreSQL sink recovery tests.
//!
//! Verifies that the sink survives a database connection that dies *silently*
//! (no RST/FIN — the peer just stops responding, as happens when a NAT or
//! firewall drops an established flow). Without a client-side statement bound
//! this wedges the sink forever with clean logs; with it, the sink must log,
//! discard the dead connection, and recover on a fresh one.

use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, Clone, Serialize)]
struct TestRecord {
    id: i64,
    value: String,
    timestamp: i64,
}

const TEST_SCHEMA: &str = r#"{
    "type": "record",
    "name": "TestRecord",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "value", "type": "string"},
        {"name": "timestamp", "type": "long"}
    ]
}"#;

/// TCP proxy that can simulate a middlebox silently killing connections.
///
/// Two failure injections, both leaving the client with a socket that accepts
/// writes but never delivers a response (the "silently dead" state):
/// - `kill_established()` poisons every currently-open link; new connections
///   pass through normally (models a NAT flushing its flow table).
/// - `arm_insert_trap()` poisons the first link that carries an INSERT
///   statement after arming — this catches a *fresh* connection mid-statement,
///   which is the exact state a server-side `statement_timeout` can never
///   cancel.
struct FlakyProxy {
    port: u16,
    /// Bumped by kill_established(); links remember the value at birth.
    generation: Arc<AtomicU64>,
    /// 1 = armed, 2 = fired. One-shot.
    insert_trap: Arc<AtomicU64>,
}

impl FlakyProxy {
    async fn start(upstream: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("proxy bind");
        let port = listener.local_addr().expect("proxy addr").port();
        let generation = Arc::new(AtomicU64::new(0));
        let insert_trap = Arc::new(AtomicU64::new(0));

        let generation_accept = generation.clone();
        let insert_trap_accept = insert_trap.clone();
        tokio::spawn(async move {
            loop {
                let Ok((client, _)) = listener.accept().await else {
                    break;
                };
                let upstream = upstream.clone();
                let generation = generation_accept.clone();
                let insert_trap = insert_trap_accept.clone();
                tokio::spawn(async move {
                    let born = generation.load(Ordering::SeqCst);
                    let Ok(server) = TcpStream::connect(&upstream).await else {
                        return;
                    };
                    let (mut client_read, mut client_write) = client.into_split();
                    let (mut server_read, mut server_write) = server.into_split();

                    // Per-link poison flag shared by both directions.
                    let poisoned = Arc::new(AtomicU64::new(0));

                    let c2s = {
                        let generation = generation.clone();
                        let insert_trap = insert_trap.clone();
                        let poisoned = poisoned.clone();
                        async move {
                            let mut buf = [0u8; 8192];
                            loop {
                                let n = match client_read.read(&mut buf).await {
                                    Ok(0) | Err(_) => break,
                                    Ok(n) => n,
                                };
                                if insert_trap.load(Ordering::SeqCst) == 1
                                    && buf[..n].windows(11).any(|w| w == b"INSERT INTO")
                                    && insert_trap
                                        .compare_exchange(1, 2, Ordering::SeqCst, Ordering::SeqCst)
                                        .is_ok()
                                {
                                    poisoned.store(1, Ordering::SeqCst);
                                }
                                let dead = poisoned.load(Ordering::SeqCst) == 1
                                    || generation.load(Ordering::SeqCst) != born;
                                // A dead link keeps reading (the client's writes
                                // succeed and get ACKed) but forwards nothing.
                                if !dead && server_write.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                        }
                    };
                    let s2c = {
                        let generation = generation.clone();
                        let poisoned = poisoned.clone();
                        async move {
                            let mut buf = [0u8; 8192];
                            loop {
                                let n = match server_read.read(&mut buf).await {
                                    Ok(0) | Err(_) => break,
                                    Ok(n) => n,
                                };
                                let dead = poisoned.load(Ordering::SeqCst) == 1
                                    || generation.load(Ordering::SeqCst) != born;
                                if !dead && client_write.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                        }
                    };
                    tokio::join!(c2s, s2c);
                });
            }
        });

        Self {
            port,
            generation,
            insert_trap,
        }
    }

    /// Silently kill every currently-established link. New connections pass.
    fn kill_established(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    /// Arm a one-shot trap: the next link that carries an INSERT statement
    /// goes silent from that statement on.
    fn arm_insert_trap(&self) {
        self.insert_trap.store(1, Ordering::SeqCst);
    }

    fn trap_fired(&self) -> bool {
        self.insert_trap.load(Ordering::SeqCst) == 2
    }
}

/// The sink must recover when its connection dies silently: first a poisoned
/// idle connection (caught at acquire), then a fresh connection that goes
/// silent mid-INSERT (caught by the client-side statement bound). Before the
/// client-side bound existed, the second case hung the pipeline forever with
/// no logs.
#[tokio::test]
async fn test_postgres_sink_recovers_from_silently_dead_connection() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let proxy = FlakyProxy::start(format!("{}:{}", ctx.postgres.host, ctx.postgres.port)).await;

    let first_batch: Vec<TestRecord> = (1..=5)
        .map(|i| TestRecord {
            id: i,
            value: format!("value_{}", i),
            timestamp: 1000 + i,
        })
        .collect();
    ctx.kafka
        .produce_avro_records(&first_batch)
        .await
        .expect("Failed to produce first batch");

    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: kafka_source
    table: test_recovery
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
"#,
        topic = ctx.kafka_topic,
    );

    let opts = PipelineOpts::new()
        .record_limit(10)
        .timeout(Duration::from_secs(90))
        // Route the sink through the proxy; the test asserts via a direct pool.
        .env("STREAMLING__POSTGRES_SINK__HOST", "127.0.0.1")
        .env("STREAMLING__POSTGRES_SINK__PORT", proxy.port.to_string())
        // 1s server-side timeout => 2s client-side bound; keeps the test fast.
        .env("STREAMLING__POSTGRES_SINK__STATEMENT_TIMEOUT_SECS", "1")
        // Poisoned idle connections fail the pre-acquire ping at this deadline.
        .env("STREAMLING__POSTGRES_SINK__POOL_ACQUIRE_TIMEOUT_SECS", "2")
        .env("STREAMLING__RECORD_BATCH_SIZE", "1");

    let run = ctx.run_pipeline_raw(&pipeline, opts);
    tokio::pin!(run);

    let mut wedged = false;
    let output = loop {
        tokio::select! {
            out = &mut run => break out.expect("pipeline run failed"),
            _ = tokio::time::sleep(Duration::from_millis(250)) => {
                if !wedged {
                    let count = ctx
                        .postgres
                        .count("SELECT COUNT(*) FROM public.test_recovery")
                        .await
                        .unwrap_or(0);
                    if count >= 5 {
                        // Phase 2: kill the connections the sink is holding and
                        // make the next fresh connection go silent mid-INSERT.
                        proxy.kill_established();
                        proxy.arm_insert_trap();
                        let second_batch: Vec<TestRecord> = (6..=10)
                            .map(|i| TestRecord {
                                id: i,
                                value: format!("value_{}", i),
                                timestamp: 1000 + i,
                            })
                            .collect();
                        ctx.kafka
                            .produce_avro_records(&second_batch)
                            .await
                            .expect("Failed to produce second batch");
                        wedged = true;
                    }
                }
            }
        }
    };

    assert!(wedged, "test never reached the failure-injection phase");
    assert!(proxy.trap_fired(), "the mid-INSERT trap never fired");
    assert!(
        output.status.success(),
        "pipeline must recover and exit cleanly; stderr:\n{}",
        output.stderr
    );

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.test_recovery")
        .await
        .expect("Failed to query count");
    assert_eq!(
        count, 10,
        "all records must land despite the dead connection"
    );

    assert!(
        output.stderr.contains("did not complete within"),
        "expected the client-side statement timeout to be logged; stderr:\n{}",
        output.stderr
    );
}
