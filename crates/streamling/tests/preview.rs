//! End-to-end test for the preview server: real binary, CSV file source, SSE out.
//!
//! Spawns the real `streamling` binary in `--preview-server` mode, then:
//! 1. Submits a broken config and expects HTTP 422.
//! 2. Submits a valid sink-less CSV pipeline and expects HTTP 200 with
//!    `text/event-stream` content-type and at least one SSE frame.

use futures::StreamExt;
use std::time::Duration;

/// Binds a random port and returns it. The port is released before returning,
/// so there is a small race window — acceptable for tests.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Polls until a TCP connection to `port` succeeds, or panics after ~15 s.
async fn wait_port(port: u16) {
    for _ in 0..150 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("preview server port {port} never came up");
}

#[tokio::test]
async fn preview_validates_runs_and_tears_down() {
    let port = free_port();

    // Spawn the real binary in preview-server mode. The server does not require
    // an external config.yaml — AppConfig falls back to embedded defaults.
    let mut server = tokio::process::Command::new(env!("CARGO_BIN_EXE_streamling"))
        .arg("--preview-server")
        .arg("--preview-server-port")
        .arg(port.to_string())
        .kill_on_drop(true)
        .spawn()
        .expect("failed to spawn preview server binary");

    wait_port(port).await;

    let client = reqwest::Client::new();

    // ── 1. Broken config → 422 ──────────────────────────────────────────────
    //
    // A transform that references a non-existent source table fails validation.
    let broken = "transforms:\n  t:\n    type: sql\n    sql: select * from nope\n    primary_key: id\n";
    let bad = client
        .post(format!("http://127.0.0.1:{port}/preview"))
        .header("content-type", "text/yaml")
        .body(broken)
        .send()
        .await
        .expect("POST broken config");
    assert_eq!(
        bad.status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY,
        "broken config must return 422"
    );

    // ── 2. Valid sink-less CSV pipeline → 200 SSE ───────────────────────────
    //
    // Write 20 000 rows so the bounded file-source pipeline stays alive long
    // enough for the preview server to proxy at least one SSE frame before EOF.
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("csv_data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let mut csv = String::with_capacity(20_001 * 12);
    csv.push_str("id,name\n");
    for i in 0..20_000_u32 {
        csv.push_str(&format!("{i},name{i}\n"));
    }
    std::fs::write(data_dir.join("data.csv"), &csv).unwrap();

    // No sinks: the preview rewriter appends a blackhole for every terminal node.
    let config = format!(
        "sources:\n  events:\n    type: file\n    path: {path}/\n    format: csv\n    primary_key: id\ntransforms:\n  passthrough:\n    type: sql\n    sql: select * from events\n    primary_key: id\n",
        path = data_dir.display()
    );

    let resp = client
        .post(format!(
            "http://127.0.0.1:{port}/preview?duration_seconds=5"
        ))
        .header("content-type", "text/yaml")
        .body(config)
        .send()
        .await
        .expect("POST valid preview config");

    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "valid CSV pipeline must return 200; body: {}",
        resp.status()
    );
    assert_eq!(
        resp.headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("text/event-stream"),
        "response must be SSE (text/event-stream)"
    );

    // Consume the stream until we see at least one SSE frame or time out.
    let mut stream = resp.bytes_stream();
    let mut saw_frame = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
            Ok(Some(Ok(bytes))) => {
                let text = String::from_utf8_lossy(&bytes);
                if text.contains("data:") || text.contains("event: end") {
                    saw_frame = true;
                    break;
                }
            }
            // Stream ended cleanly or with an error — break either way.
            Ok(Some(Err(_))) | Ok(None) => break,
            // Inner timeout: no chunk yet, keep waiting.
            Err(_) => {}
        }
    }

    assert!(
        saw_frame,
        "expected at least one SSE frame (data: or event: end) from the preview stream"
    );

    // Tear down the preview server.
    let _ = server.start_kill();
    let _ = server.wait().await;
}
