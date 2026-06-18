//! Axum server exposing `POST /rewrite`: stateless sink-to-blackhole config rewrite.

use axum::{
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Router,
};

/// Builds the axum router for the preview rewrite server.
pub fn build_router() -> Router {
    Router::new().route("/rewrite", post(rewrite_handler))
}

/// Handles `POST /rewrite`: rewrites every sink in the submitted YAML to a
/// blackhole sink and returns the modified YAML.
///
/// - `200 application/yaml` on success.
/// - `422 Unprocessable Entity` when the YAML is invalid or rewriting fails.
async fn rewrite_handler(body: String) -> Response {
    match crate::preview::rewrite::rewrite_sinks_to_blackhole(&body) {
        Ok(yaml) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/yaml")],
            yaml,
        )
            .into_response(),
        Err(e) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            [(header::CONTENT_TYPE, "text/plain")],
            format!("{e}"),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::build_router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt; // oneshot

    /// A minimal source + transform + postgres-sink config for the happy path.
    const VALID_YAML: &str = r#"
sources:
  src:
    type: kafka
    topic: events
    primary_key: id
transforms:
  t:
    type: sql
    sql: select * from src
    primary_key: id
sinks:
  out:
    type: postgres
    from: t
    table: events
    primary_key: id
"#;

    #[tokio::test]
    async fn valid_yaml_returns_200_with_blackhole() {
        let app = build_router();
        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rewrite")
                    .header("content-type", "text/yaml")
                    .body(Body::from(VALID_YAML))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);

        let ct = res
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(ct.contains("application/yaml"), "expected application/yaml, got {ct}");

        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        assert!(body.contains("blackhole"), "response should contain 'blackhole'");
        assert!(!body.contains("postgres"), "response must not contain 'postgres'");
    }

    #[tokio::test]
    async fn invalid_yaml_returns_422() {
        let app = build_router();
        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rewrite")
                    .header("content-type", "text/yaml")
                    .body(Body::from("::: not yaml :::"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }
}
