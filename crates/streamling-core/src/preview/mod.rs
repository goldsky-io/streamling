pub mod child;
pub mod duration;
pub mod rewrite;
pub mod server;

use crate::error::{Result, ResultExt};

/// Runs the preview HTTP server on `0.0.0.0:{port}` until the process exits.
pub async fn run_preview_server(port: u16) -> Result<()> {
    let app = server::build_router(server::PreviewState::new());
    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .streamling_context("failed to bind preview server")?;
    tracing::info!("Preview server listening on {addr}");
    axum::serve(listener, app)
        .await
        .streamling_context("preview server error")?;
    Ok(())
}
