pub mod rewrite;
pub mod server;

use crate::error::{Result, ResultExt};

/// Runs the preview HTTP server on `0.0.0.0:{port}` until the process exits.
/// Serves the stateless `POST /rewrite` config-rewrite endpoint.
pub async fn run_preview_server(port: u16) -> Result<()> {
    let app = server::build_router();
    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .streamling_context("failed to bind preview server")?;
    tracing::info!("Preview rewrite server listening on {addr} (POST /rewrite)");
    axum::serve(listener, app)
        .await
        .streamling_context("preview server error")?;
    Ok(())
}
