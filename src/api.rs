//! Complete HTTP API composition. Authentication wraps every `/v1` and `/api`
//! route once, so future transports can reuse the same verified context contract.
use axum::{
    Extension, Router,
    body::{Body, Bytes},
    extract::State,
    http::HeaderMap,
    middleware::from_fn_with_state,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use std::sync::Arc;
use tower_http::trace::TraceLayer;

use crate::{handlers, state::AppState};

#[path = "mcp_server.rs"]
pub(crate) mod mcp_server;

pub fn routes(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/v1/chat/completions", post(admitted_chat_completions))
        .route("/v1/models", get(handlers::list_models))
        .merge(crate::web::routes())
        .merge(crate::auth::admin_routes())
        .merge(crate::async_ops::routes())
        .merge(crate::management::routes())
        .merge(mcp_server::routes())
        // Auth runs before the web read-model cache so cached bodies can be
        // partitioned by authenticated key/workforce context.
        .layer(TraceLayer::new_for_http())
        .layer(from_fn_with_state(state.clone(), crate::auth::middleware))
        .with_state(state)
}

async fn admitted_chat_completions(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<crate::auth::AuthContext>>,
    lifecycle: Option<Extension<Arc<crate::health::Lifecycle>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let admission = match lifecycle {
        Some(Extension(lifecycle)) => match lifecycle.try_admit_new_work() {
            Ok(permit) => Some(permit),
            Err(_) => return crate::management::draining_response(),
        },
        None => None,
    };
    let response = match handlers::chat_completions(State(state), auth, headers, body).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    };
    hold_admission_until_body(response, admission)
}

fn hold_admission_until_body(
    response: Response,
    admission: Option<crate::health::AdmissionPermit>,
) -> Response {
    use futures::StreamExt;

    let (parts, body) = response.into_parts();
    let mut body = Box::pin(body.into_data_stream());
    let stream = async_stream::stream! {
        let _admission = admission;
        while let Some(frame) = body.next().await {
            yield frame;
        }
    };
    Response::from_parts(parts, Body::from_stream(stream))
}
