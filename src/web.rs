//! Same-origin web workspace and memory-first replay APIs. Native chat uses the same
//! Foreman pipeline as external API clients, not a second orchestration path.
use crate::{observe::now_ms, state::AppState};
use axum::{
    Json, Router,
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{
        Html, IntoResponse, Sse,
        sse::{Event, KeepAlive},
    },
    routing::{get, post},
};
use rusqlite::{OptionalExtension, params};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{convert::Infallible, sync::Arc};

type ApiError = (StatusCode, Json<Value>);
fn failure(error: impl ToString) -> ApiError {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error":error.to_string()})),
    )
}
fn bad(error: impl ToString) -> ApiError {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"error":error.to_string()})),
    )
}

fn admission_error(error: crate::health::AdmissionError) -> ApiError {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error":{
            "message":error.to_string(),
            "type":"service_unavailable",
            "code":error.code(),
            "retryable":error.is_retryable()
        }})),
    )
}

fn admit_new_work(
    lifecycle: Option<Extension<Arc<crate::health::Lifecycle>>>,
) -> Result<Option<crate::health::AdmissionPermit>, ApiError> {
    lifecycle
        .map(|Extension(lifecycle)| lifecycle.try_admit_new_work().map_err(admission_error))
        .transpose()
}

fn admit_existing_work(
    lifecycle: Option<Extension<Arc<crate::health::Lifecycle>>>,
) -> Result<Option<crate::health::AdmissionPermit>, ApiError> {
    lifecycle
        .map(|Extension(lifecycle)| lifecycle.try_admit_existing_work().map_err(admission_error))
        .transpose()
}

fn snapshot(state: Arc<AppState>) -> Result<Arc<AppState>, ApiError> {
    Ok(Arc::new(AppState {
        config: state.config.clone(),
        providers: state.providers.clone(),
        http: state.http.clone(),
        portfolio: state.portfolio.read_view().map_err(failure)?,
        external_api_enabled: state.external_api_enabled,
        auth: state.auth.clone(),
    }))
}

fn authenticated(
    state: Arc<AppState>,
    auth: Option<Extension<crate::auth::AuthContext>>,
) -> Arc<AppState> {
    auth.as_ref()
        .map(|Extension(auth)| state.with_auth(auth))
        .unwrap_or(state)
}

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/",
            get(|| async { Html(include_str!("../web/index.html")) }),
        )
        .route(
            "/assets/style.css",
            get(|| async {
                (
                    [("content-type", "text/css; charset=utf-8")],
                    include_str!("../web/style.css"),
                )
            }),
        )
        .route(
            "/assets/app.js",
            get(|| async {
                (
                    [("content-type", "text/javascript; charset=utf-8")],
                    include_str!("../web/app.js"),
                )
            }),
        )
        .route(
            "/assets/state.js",
            get(|| async {
                (
                    [("content-type", "text/javascript; charset=utf-8")],
                    include_str!("../web/state.js"),
                )
            }),
        )
        .route("/api/runs", get(runs))
        .route("/api/runs/{id}", get(detail))
        .route("/api/runs/{id}/control", post(control))
        .route("/api/runs/{id}/otlp", get(otlp))
        .route("/api/runs/{id}/improvements", get(improvements))
        .route("/api/improvements", get(improvement_suggestions))
        .route(
            "/api/improvements/{id}/apply",
            post(apply_improvement_suggestion),
        )
        .route(
            "/api/improvements/{id}/declassify",
            post(declassify_improvement_suggestion),
        )
        .route("/api/events", get(events))
        .route("/api/events/page", get(event_page))
        .route("/api/definitions/{id}", get(lineage))
        .route("/api/statistics", get(statistics))
        .route("/api/workforce", get(workforce))
        .route("/api/conversations", get(conversations))
        .route("/api/conversations/{id}", get(conversation))
        .route("/api/chat", post(chat))
        .route("/api/memory", get(memory_status))
        .layer(axum::middleware::from_fn(crate::dashboard_cache::layer))
}

async fn memory_status(
    State(state): State<Arc<AppState>>,
    _auth: Option<Extension<crate::auth::AuthContext>>,
) -> Json<Value> {
    Json(json!({"budget_mib":state.config.memory.budget_mib,
        "database":state.portfolio.storage.as_ref().map(|s|s.metrics()),
        "delivery":{"bytes":state.portfolio.delivery_budget.used(),"limit_bytes":state.portfolio.delivery_budget.limit()},
        "dashboard":crate::dashboard_cache::metrics(),"journal":crate::traffic::metrics()}))
}

#[derive(Deserialize, Default)]
struct Paging {
    #[serde(default)]
    offset: u32,
}
async fn runs(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<crate::auth::AuthContext>>,
    Query(query): Query<Paging>,
) -> Result<Json<Value>, ApiError> {
    let state = snapshot(authenticated(state, auth))?;
    let conn = state.portfolio.conn.lock().unwrap();
    let mut stmt=conn.prepare("SELECT id,conversation_id,source,title,status,created_ms,updated_ms FROM trace_runs WHERE (?1 IS NULL OR workforce_id=?1) AND (?2 IS NULL OR owner_key_id=?2) ORDER BY created_ms DESC,rowid DESC LIMIT 100 OFFSET ?3").map_err(failure)?;
    let rows=stmt.query_map(rusqlite::params![state.portfolio.workforce_scope(),state.portfolio.key_scope(),query.offset],|r|Ok(json!({"id":r.get::<_,String>(0)?,"conversation_id":r.get::<_,Option<String>>(1)?,"source":r.get::<_,String>(2)?,"title":r.get::<_,String>(3)?,"status":r.get::<_,String>(4)?,"created_ms":r.get::<_,i64>(5)?,"updated_ms":r.get::<_,i64>(6)?}))).map_err(failure)?.collect::<Result<Vec<_>,_>>().map_err(failure)?;
    drop(stmt);
    drop(conn);
    let cursor = state.portfolio.trace_cursor().map_err(failure)?;
    Ok(Json(
        json!({"runs":rows,"cursor":cursor,"next_offset":if rows.len()==100{Some(query.offset+100)}else{None}}),
    ))
}

async fn detail(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<crate::auth::AuthContext>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let state = snapshot(authenticated(state, auth))?;
    Ok(Json(state.portfolio.trace_detail(&id).map_err(failure)?))
}
async fn otlp(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<crate::auth::AuthContext>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let state = snapshot(authenticated(state, auth))?;
    Ok(Json(state.portfolio.otlp(&id, None).map_err(failure)?))
}
async fn statistics(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<crate::auth::AuthContext>>,
) -> Result<Json<Value>, ApiError> {
    let state = snapshot(authenticated(state, auth))?;
    Ok(Json(state.portfolio.statistics().map_err(failure)?))
}

#[derive(Deserialize)]
struct Control {
    #[serde(default = "all")]
    target: String,
    action: String,
    #[serde(default)]
    message: String,
}
fn all() -> String {
    "*".into()
}
async fn control(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<crate::auth::AuthContext>>,
    lifecycle: Option<Extension<Arc<crate::health::Lifecycle>>>,
    Path(id): Path<String>,
    Json(body): Json<Control>,
) -> Result<Json<Value>, ApiError> {
    let _admission = admit_existing_work(lifecycle)?;
    let state = authenticated(state, auth);
    state
        .portfolio
        .control(&id, &body.target, &body.action, &body.message)
        .map(Json)
        .map_err(|e| (StatusCode::CONFLICT, Json(json!({"error":e}))))
}

#[derive(Deserialize, Default)]
struct EventQuery {
    #[serde(default)]
    after: i64,
    run: Option<String>,
}
async fn event_page(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<crate::auth::AuthContext>>,
    Query(query): Query<EventQuery>,
) -> Result<Json<Value>, ApiError> {
    let state = snapshot(authenticated(state, auth))?;
    Ok(Json(
        json!({"events":state.portfolio.trace_events(query.after.max(0),query.run.as_deref()).map_err(failure)?}),
    ))
}
async fn events(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<crate::auth::AuthContext>>,
    Query(query): Query<EventQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let state = authenticated(state, auth);
    let last = headers.get("last-event-id").and_then(|v| v.to_str().ok());
    let epoch = state.portfolio.trace_epoch.clone();
    let different_epoch = last
        .and_then(|s| s.split_once(':'))
        .is_some_and(|(previous, _)| previous != epoch);
    let mut cursor = last
        .and_then(|s| s.rsplit(':').next().unwrap_or(s).parse::<i64>().ok())
        .unwrap_or(query.after)
        .max(0);
    let mut notices = state.portfolio.trace_bus.subscribe();
    let stream = async_stream::stream! {
        let reader = state.portfolio.clone();
        let head = tokio::task::spawn_blocking(move || reader.trace_cursor()).await;
        if let Ok(Ok(head)) = head {
            if different_epoch || cursor > head {
                cursor = head;
                yield Ok::<_,Infallible>(Event::default().id(format!("{epoch}:{cursor}")).event("trace_reset")
                    .data(json!({"cursor":cursor,"reason":"memory_history_reset","epoch":epoch}).to_string()));
            }
        }
        loop {
            let reader = state.portfolio.clone();
            let run = query.run.clone();
            let permit = crate::dashboard_cache::read_permit().await;
            let result = tokio::task::spawn_blocking(move || reader.read_view()?.trace_events(cursor,run.as_deref())).await.unwrap_or_else(|e|Err(e.to_string()));
            drop(permit);
            let rows=match result {
                Ok(rows)=>rows,
                Err(error)=>{yield Ok::<_,Infallible>(Event::default().event("trace_error").data(json!({"error":error}).to_string()));break;}
            };
            let full=rows.len()==500;
            for row in rows {
                cursor=row["seq"].as_i64().unwrap();
                yield Ok(Event::default().id(format!("{epoch}:{cursor}")).event("trace").data(row.to_string()));
            }
            if !full {
                tokio::select! {
                    _=notices.recv()=>{},
                    _=tokio::time::sleep(std::time::Duration::from_secs(1))=>{},
                }
            }
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn workforce(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<crate::auth::AuthContext>>,
) -> Result<Json<Value>, ApiError> {
    let state = snapshot(authenticated(state, auth))?;
    Ok(Json(state.portfolio.workforce(now_ms()).map_err(failure)?))
}

async fn improvements(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<crate::auth::AuthContext>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let state = snapshot(authenticated(state, auth))?;
    Ok(Json(state.portfolio.improvements(&id).map_err(failure)?))
}

async fn improvement_suggestions(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<crate::auth::AuthContext>>,
) -> Result<Json<Value>, ApiError> {
    let state = authenticated(state, auth);
    // The auth middleware allows roster_read or improvement_contribute for the
    // key-private view. Portfolio adds cross-key items only when both roster_read
    // and roster_write are present.
    Ok(Json(state.portfolio.suggestions().map_err(failure)?))
}

async fn apply_improvement_suggestion(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<crate::auth::AuthContext>>,
    lifecycle: Option<Extension<Arc<crate::health::Lifecycle>>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let state = authenticated(state, auth);
    if state
        .auth
        .as_ref()
        .is_some_and(|auth| !auth.permissions.roster_write)
    {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({"error":"roster write permission required"})),
        ));
    }
    let _admission = admit_new_work(lifecycle)?;
    let request = uuid::Uuid::new_v4().to_string();
    // This is a writer-owned application trace, not an extension of the
    // contributor's private conversation or checkpoint trace.
    let payload = json!({"messages":[{"role":"user","content":"Apply shared workforce improvement proposal"}]});
    let run = state
        .portfolio
        .trace_begin(&request, &payload, "improvement")
        .map_err(bad)?;
    match crate::improvement::apply_suggestion_async_with_authority(
        &state,
        &id,
        &request,
        &crate::improvement::ApplicationAuthority::Explicit,
    )
    .await
    {
        Ok(result) => {
            state
                .portfolio
                .trace_finish_request(&request, "completed", &result)
                .map_err(failure)?;
            Ok(Json(result))
        }
        Err(error) => {
            let _ =
                state
                    .portfolio
                    .trace_finish_request(&request, "error", &json!({"error":error}));
            let status = if error.contains("unknown suggestion") {
                StatusCode::NOT_FOUND
            } else if error.contains("stale") || error.contains("already claimed") {
                StatusCode::CONFLICT
            } else {
                StatusCode::BAD_REQUEST
            };
            let _ = run;
            Err((status, Json(json!({"error":error}))))
        }
    }
}

async fn declassify_improvement_suggestion(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<crate::auth::AuthContext>>,
    lifecycle: Option<Extension<Arc<crate::health::Lifecycle>>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let state = authenticated(state, auth);
    if state
        .auth
        .as_ref()
        .is_none_or(|auth| !auth.permissions.improvement_contribute)
    {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({"error":"improvement contribution permission required"})),
        ));
    }
    let _admission = admit_new_work(lifecycle)?;

    let request = uuid::Uuid::new_v4().to_string();
    let payload = json!({"messages":[{"role":"user","content":"Release generated improvement revision to shared workforce writers"}]});
    state
        .portfolio
        .trace_begin(&request, &payload, "improvement")
        .map_err(bad)?;
    match crate::improvement::declassify_suggestion(&state, &id, &request) {
        Ok(result) => {
            state
                .portfolio
                .trace_finish_request(&request, "completed", &result)
                .map_err(failure)?;
            Ok(Json(result))
        }
        Err(error) => {
            let _ =
                state
                    .portfolio
                    .trace_finish_request(&request, "error", &json!({"error":error}));
            // Keep wrong-owner and missing IDs indistinguishable so this route
            // cannot be used to probe another key's private suggestions.
            let status = if error.contains("unknown, non-pending, or already released") {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::BAD_REQUEST
            };
            Err((status, Json(json!({"error":error}))))
        }
    }
}

async fn lineage(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<crate::auth::AuthContext>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let state = snapshot(authenticated(state, auth))?;
    let conn = state.portfolio.conn.lock().unwrap();
    let mut stmt=conn.prepare("SELECT id,kind,definition,created_ms FROM definition_versions WHERE entity_id=?1 AND (?2 IS NULL OR workforce_id=?2) ORDER BY created_ms,rowid").map_err(failure)?;
    let versions=stmt.query_map(rusqlite::params![&id,state.portfolio.workforce_scope()],|r|Ok(json!({"id":r.get::<_,String>(0)?,"kind":r.get::<_,String>(1)?,"definition":serde_json::from_str::<Value>(&r.get::<_,String>(2)?).unwrap_or(Value::Null),"created_ms":r.get::<_,i64>(3)?}))).map_err(failure)?.collect::<Result<Vec<_>,_>>().map_err(failure)?;
    let mut stmt=conn.prepare("SELECT before_version,after_version,action,evidence,applied,run_id,span_id,at_ms FROM definition_changes WHERE entity_id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3) ORDER BY id").map_err(failure)?;
    let changes=stmt.query_map(rusqlite::params![&id,state.portfolio.workforce_scope(),state.portfolio.key_scope()],|r|Ok(json!({"before_version":r.get::<_,Option<String>>(0)?,"after_version":r.get::<_,Option<String>>(1)?,"action":r.get::<_,String>(2)?,"evidence":serde_json::from_str::<Value>(&r.get::<_,String>(3)?).unwrap_or(Value::Null),"applied":r.get::<_,bool>(4)?,"run_id":r.get::<_,Option<String>>(5)?,"span_id":r.get::<_,Option<String>>(6)?,"at_ms":r.get::<_,i64>(7)?}))).map_err(failure)?.collect::<Result<Vec<_>,_>>().map_err(failure)?;
    let mut stmt=conn.prepare("SELECT request_id,verdict,before_definition,after_definition,applied,created_at FROM evaluations WHERE entity_id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3) ORDER BY id").map_err(failure)?;
    let legacy=stmt.query_map(rusqlite::params![&id,state.portfolio.workforce_scope(),state.portfolio.key_scope()],|r|Ok(json!({"request_id":r.get::<_,String>(0)?,"verdict":r.get::<_,String>(1)?,"before":r.get::<_,String>(2)?,"after":r.get::<_,Option<String>>(3)?,"applied":r.get::<_,bool>(4)?,"created_at":r.get::<_,i64>(5)?}))).map_err(failure)?.collect::<Result<Vec<_>,_>>().map_err(failure)?;
    Ok(Json(
        json!({"entity_id":id,"versions":versions,"changes":changes,"evaluations":legacy}),
    ))
}

async fn conversations(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<crate::auth::AuthContext>>,
) -> Result<Json<Value>, ApiError> {
    let state = snapshot(authenticated(state, auth))?;
    let conn = state.portfolio.conn.lock().unwrap();
    let mut stmt = conn
        .prepare("SELECT id,title,created_ms FROM conversations WHERE (?1 IS NULL OR workforce_id=?1) AND (?2 IS NULL OR owner_key_id=?2) ORDER BY created_ms DESC")
        .map_err(failure)?;
    let rows=stmt.query_map(rusqlite::params![state.portfolio.workforce_scope(),state.portfolio.key_scope()],|r|Ok(json!({"id":r.get::<_,String>(0)?,"title":r.get::<_,String>(1)?,"created_ms":r.get::<_,i64>(2)?}))).map_err(failure)?.collect::<Result<Vec<_>,_>>().map_err(failure)?;
    Ok(Json(json!(rows)))
}

fn messages(state: &AppState, id: &str) -> Result<Vec<Value>, ApiError> {
    let conn = state.portfolio.conn.lock().unwrap();
    let mut stmt = conn
        .prepare("SELECT message FROM conversation_messages WHERE conversation_id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3) ORDER BY id")
        .map_err(failure)?;
    let rows = stmt
        .query_map(
            rusqlite::params![
                id,
                state.portfolio.workforce_scope(),
                state.portfolio.key_scope()
            ],
            |r| r.get::<_, String>(0),
        )
        .map_err(failure)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(failure)?;
    rows.iter()
        .map(|s| serde_json::from_str(s).map_err(failure))
        .collect()
}
async fn conversation(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<crate::auth::AuthContext>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let state = snapshot(authenticated(state, auth))?;
    let exists: bool = state.portfolio.conn.lock().unwrap().query_row(
        "SELECT EXISTS(SELECT 1 FROM conversations WHERE id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3))",
        rusqlite::params![&id,state.portfolio.workforce_scope(),state.portfolio.key_scope()],
        |row| row.get(0),
    ).map_err(failure)?;
    if !exists {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error":"unknown conversation"})),
        ));
    }
    Ok(Json(json!({"id":id,"messages":messages(&state,&id)?})))
}

#[derive(Deserialize)]
struct Submit {
    conversation_id: Option<String>,
    message: String,
}
async fn chat(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<crate::auth::AuthContext>>,
    lifecycle: Option<Extension<Arc<crate::health::Lifecycle>>>,
    Json(body): Json<Submit>,
) -> Result<Json<Value>, ApiError> {
    let state = authenticated(state, auth);
    if body.message.trim().is_empty() {
        return Err(bad("message is empty"));
    }
    let worker_lifecycle = lifecycle.as_ref().map(|Extension(value)| value.clone());
    let admission = match lifecycle {
        Some(Extension(lifecycle)) => Some(lifecycle.try_admit_new_work().map_err(|error| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error":{
                    "message":error.to_string(),
                    "type":"service_unavailable",
                    "code":error.code(),
                    "retryable":error.is_retryable()
                }})),
            )
        })?),
        None => None,
    };
    let conversation = match body.conversation_id {
        Some(id) => {
            let exists: Option<String> = state
                .portfolio
                .conn
                .lock()
                .unwrap()
                .query_row("SELECT id FROM conversations WHERE id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)", rusqlite::params![&id,state.portfolio.workforce_scope(),state.portfolio.key_scope()], |r| {
                    r.get(0)
                })
                .optional()
                .map_err(failure)?;
            if exists.is_none() {
                return Err(bad("unknown native conversation"));
            }
            id
        }
        None => {
            let id = uuid::Uuid::new_v4().to_string();
            state
                .portfolio
                .conn
                .lock()
                .unwrap()
                .execute(
                    "INSERT INTO conversations(id,title,created_ms,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4,?5)",
                    params![
                        id,
                        body.message.chars().take(80).collect::<String>(),
                        now_ms(),
                        state.portfolio.workforce_scope(),
                        state.portfolio.key_scope()
                    ],
                )
                .map_err(failure)?;
            id
        }
    };
    let mut history = messages(&state, &conversation)?;
    // Presentation history is not part of the next task's instructions/evidence.
    for message in &mut history {
        if let Some(object) = message.as_object_mut() {
            object.remove("ccm_narration");
        }
    }
    let user = json!({"role":"user","content":body.message});
    history.push(user.clone());
    let payload = json!({"model":state.config.model,"stream":false,"messages":history,"metadata":{"ccm_source":"web","ccm_conversation_id":conversation}});
    let request = uuid::Uuid::new_v4().to_string();
    let run = state
        .portfolio
        .trace_begin(&request, &payload, "web")
        .map_err(bad)?;
    state.portfolio.conn.lock().unwrap().execute("INSERT INTO conversation_messages(conversation_id,run_id,message,created_ms,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4,?5,?6)",params![conversation,run,user.to_string(),now_ms(),state.portfolio.workforce_scope(),state.portfolio.key_scope()]).map_err(failure)?;
    let worker_state = state.clone();
    let worker_conversation = conversation.clone();
    let worker_run = run.clone();
    let worker_request = request.clone();
    tokio::spawn(async move {
        // The route returns the conversation/run receipt before this worker is
        // done; keep admission counted through execution and both publications.
        let _admission = admission;
        let mut gate = crate::improvement::ResponseGate::new(
            worker_state.portfolio.clone(),
            worker_request.clone(),
            worker_state.config.improvement.clone(),
        );
        let result = async {
            let response = tokio::select! {
                response = crate::handlers::native_chat(worker_state.clone(), payload, worker_request.clone()) => response,
                _ = async {
                    match &worker_lifecycle {
                        Some(lifecycle) => lifecycle.wait_until_sealed().await,
                        None => std::future::pending::<()>().await,
                    }
                } => Err(crate::error::ProxyError::BadRequest(
                    "service shutdown cutoff".into(),
                )),
            }
            .map_err(|e| e.to_string())?;
            let status = response.status();
            let bytes = axum::body::to_bytes(response.into_body(), 16 * 1024 * 1024)
                .await
                .map_err(|e| e.to_string())?;
            let value: Value = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
            if !status.is_success() {
                return Err(value.to_string());
            }
            value
                .pointer("/choices/0/message")
                .cloned()
                .ok_or_else(|| "missing response message".to_string())
        }
        .await;
        let success = result.is_ok();
        let message = match result {
            Ok(message) => message,
            Err(error) => {
                let _ = worker_state.portfolio.trace_finish_request(
                    &worker_request,
                    "error",
                    &json!({"error":error}),
                );
                json!({"role":"assistant","content":format!("CCM error: {error}"),"ccm_error":true})
            }
        };
        if let Err(error)=worker_state.portfolio.conn.lock().unwrap().execute("INSERT INTO conversation_messages(conversation_id,run_id,message,created_ms,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4,?5,?6)",params![worker_conversation,worker_run,message.to_string(),now_ms(),worker_state.portfolio.workforce_scope(),worker_state.portfolio.key_scope()]){tracing::error!(%error,"saving native chat response failed");return;}
        let published = worker_state.portfolio.trace_emit(
            &worker_run,
            None,
            "conversation.message",
            &json!({"conversation_id":worker_conversation,"message":message}),
        );
        if success
            && published.is_ok()
            && message["tool_calls"]
                .as_array()
                .is_none_or(|calls| calls.is_empty())
        {
            gate.complete();
        }
    });
    Ok(Json(
        json!({"conversation_id":conversation,"run_id":run,"request_id":request}),
    ))
}
