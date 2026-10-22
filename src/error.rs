use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    #[error("invalid request: {0}")]
    BadRequest(String),

    #[error("upstream request failed: {0}")]
    UpstreamRequest(#[from] reqwest::Error),

    #[error("upstream returned {status}: {body}")]
    UpstreamStatus { status: StatusCode, body: String },
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        let (status, error_type, message) = match &self {
            ProxyError::BadRequest(m) => {
                (StatusCode::BAD_REQUEST, "invalid_request_error", m.clone())
            }
            ProxyError::UpstreamRequest(e) => (StatusCode::BAD_GATEWAY, "api_error", e.to_string()),
            ProxyError::UpstreamStatus { status, body } => {
                (*status, "upstream_error", body.clone())
            }
        };

        let body = json!({
            "error": {
                "message": message,
                "type": error_type,
                "code": status.as_u16(),
            }
        });

        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use serde_json::Value;

    #[tokio::test]
    async fn error_renders_openai_shape() {
        let err = ProxyError::BadRequest("bad json".into());
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = to_bytes(response.into_body(), 1024).await.unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["error"]["type"], "invalid_request_error");
        assert_eq!(value["error"]["message"], "bad json");
    }
}
