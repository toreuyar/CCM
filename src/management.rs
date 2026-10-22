//! Shared Foreman management operations and authenticated dashboard projections.
//!
//! HTTP, MCP, and conversational adapters call the same workforce operations so
//! creation, retirement, evaluation, execution, scope, and receipts do not drift.

use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use rusqlite::OptionalExtension;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;

type HttpError = (StatusCode, Json<Value>);

const DRAIN_ERROR: &str = "service is draining; retry after the service is ready";

pub fn draining_response() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error":{
            "message":DRAIN_ERROR,
            "type":"service_unavailable",
            "code":"service_draining",
            "retryable":true
        }})),
    )
        .into_response()
}

pub fn routes() -> Router<Arc<crate::state::AppState>> {
    Router::new()
        .route("/api/session", get(session))
        .route("/api/runtime", get(runtime_snapshot))
        .route(
            "/api/admin/settings",
            get(global_settings).put(update_global_settings),
        )
        .route(
            "/api/settings/overrides",
            get(client_overrides).put(update_client_overrides),
        )
        .route("/api/admin/service/status", get(service_status))
        .route("/api/admin/service/drain", post(service_drain))
        .route("/api/management/definitions", post(create_definition_http))
        .route(
            "/api/management/definitions/{kind}/{id}/retire",
            post(retire_definition_http),
        )
        .route(
            "/api/management/definitions/{kind}/{id}/evaluate",
            post(evaluate_definition_http),
        )
        .route(
            "/api/management/definitions/{kind}/{id}/run",
            post(run_definition_http),
        )
}

fn auth_error(error: impl ToString) -> HttpError {
    (
        StatusCode::FORBIDDEN,
        Json(json!({"error":error.to_string()})),
    )
}

fn bad_request(error: impl ToString) -> HttpError {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"error":error.to_string()})),
    )
}

fn conflict(revision: &str) -> HttpError {
    (
        StatusCode::CONFLICT,
        Json(json!({"error":"revision_conflict","current_revision":revision})),
    )
}

fn server_error(error: impl ToString) -> HttpError {
    tracing::error!(error = %error.to_string(), "management operation failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error":"management operation failed"})),
    )
}

fn require(
    auth: &crate::auth::AuthContext,
    permission: crate::auth::RequiredPermission,
) -> Result<(), HttpError> {
    auth.require(permission).map_err(auth_error)
}

fn admission_error(error: crate::health::AdmissionError) -> HttpError {
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
    lifecycle: Option<&Arc<crate::health::Lifecycle>>,
) -> Result<Option<crate::health::AdmissionPermit>, HttpError> {
    lifecycle
        .map(|lifecycle| lifecycle.try_admit_new_work().map_err(admission_error))
        .transpose()
}

fn read_if_match(headers: &HeaderMap) -> Result<String, HttpError> {
    let Some(value) = headers.get(header::IF_MATCH) else {
        return Err((
            StatusCode::PRECONDITION_REQUIRED,
            Json(json!({"error":"If-Match revision is required"})),
        ));
    };
    let text = value
        .to_str()
        .map_err(|_| bad_request("If-Match must contain a revision"))?
        .trim();
    let revision = text
        .strip_prefix('"')
        .and_then(|text| text.strip_suffix('"'))
        .filter(|text| !text.is_empty() && !text.contains(','))
        .ok_or_else(|| bad_request("If-Match must be one quoted revision"))?;
    Ok(revision.to_owned())
}

fn etag(revision: &str) -> HeaderValue {
    HeaderValue::from_str(&format!("\"{revision}\""))
        .expect("hex revisions are valid header values")
}

async fn session(
    State(state): State<Arc<crate::state::AppState>>,
    Extension(auth): Extension<crate::auth::AuthContext>,
) -> Result<(HeaderMap, Json<Value>), HttpError> {
    let identity: Option<(String, String, String, String)> = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT k.label,w.name,w.visibility,k.id FROM api_keys k
             JOIN workforces w ON w.id=k.workforce_id
             WHERE k.id=?1 AND k.workforce_id=?2 AND k.revoked_ms IS NULL",
            rusqlite::params![auth.key_id, auth.workforce_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(server_error)?;
    let Some((key_label, workforce_name, visibility, key_id)) = identity else {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"invalid or missing bearer key"})),
        ));
    };
    let mut supported = Vec::new();
    if auth.permissions.roster_read {
        // Operation status/list/result routes require roster_read; advertise the
        // capability only where the matching HTTP permission is actually held.
        supported.extend([
            "workforce.read",
            "runs.read",
            "runtime.read",
            "operations.read",
        ]);
    }
    if auth.permissions.roster_use {
        supported.extend([
            "foreman.chat",
            "workforce.run",
            "settings.overrides.read",
            "settings.overrides.write",
            "operations.continue",
            "tool_approvals.manage",
        ]);
    }
    if auth.permissions.roster_write {
        supported.extend([
            "workforce.create",
            "workforce.retire",
            "workforce.evaluate",
            "improvements.apply",
        ]);
    }
    if auth.permissions.improvement_contribute {
        supported.extend(["improvements.read_own", "improvements.declassify"]);
    }
    if auth.permissions.bootstrap_admin {
        supported.extend([
            "settings.global.read",
            "settings.global.write",
            "service.status",
            "service.drain",
            "async_limits.read_write",
        ]);
    }
    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok((
        headers,
        Json(json!({
            "identity":{"key_id":key_id,"label":key_label},
            "workforce":{"id":auth.workforce_id,"name":workforce_name,"visibility":visibility},
            "permissions":auth.permissions,
            "supported_operations":supported
        })),
    ))
}

async fn runtime_snapshot(
    State(state): State<Arc<crate::state::AppState>>,
    Extension(auth): Extension<crate::auth::AuthContext>,
    lifecycle: Option<Extension<Arc<crate::health::Lifecycle>>>,
) -> Result<Json<Value>, HttpError> {
    if !auth.permissions.roster_read && !auth.permissions.bootstrap_admin {
        return Err(auth_error("bearer key lacks the required permission"));
    }
    let scoped = state.with_auth(&auth);
    let admin = auth.permissions.bootstrap_admin;
    let generated_ms = crate::observe::now_ms();
    let lifecycle_status = lifecycle.map(|Extension(value)| value.safe_snapshot());
    if !admin {
        let counts = scoped
            .portfolio
            .async_ops
            .scoped_counts(&scoped.portfolio)
            .await
            .map_err(|_| auth_error("bearer key lacks the required permission"))?;
        return Ok(Json(json!({
            "generated_ms":generated_ms,
            "scope":{"type":"key_workforce","key_id":auth.key_id,"workforce_id":auth.workforce_id},
            "providers":null,
            "provider_scope":"bootstrap_admin_only",
            "async_supervisor":serde_json::to_value(counts).map_err(server_error)?,
            "service":lifecycle_status
        })));
    }

    let mut providers = Vec::new();
    for (provider_id, provider) in &state.config.routing.providers {
        let configured_model_ids = state
            .config
            .routing
            .models
            .keys()
            .filter(|model_id| {
                state
                    .config
                    .select_model(Some(model_id))
                    .is_ok_and(|model| model.routing.provider == *provider_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        let gate = state.providers.get(provider_id, provider).snapshot();
        providers.push(json!({
            "provider_id":provider_id,
            "configured_model_ids":configured_model_ids,
            "max_concurrent_requests":provider.max_concurrent_requests,
            "active_requests":gate.active_requests,
            "waiting_requests":gate.waiting_requests,
            "circuit_state":gate.circuit_state,
            "cooldown_remaining_ms":gate.cooldown_remaining_ms
        }));
    }
    let raw_async = state.portfolio.async_ops.settings().await;
    let async_supervisor = admin_async_projection(&raw_async);
    Ok(Json(json!({
        "generated_ms":generated_ms,
        "scope":{"type":"server"},
        "providers":providers,
        "async_supervisor":async_supervisor,
        "native_tool_sandbox":state.sandbox_runtime_status(),
        "service":lifecycle_status
    })))
}

fn admin_async_projection(settings: &Value) -> Value {
    let server = settings.get("server").cloned().unwrap_or(Value::Null);
    let mut workforces = serde_json::Map::new();
    if let Some(configured) = settings.get("workforces").and_then(Value::as_object) {
        for (id, limits) in configured {
            workforces.insert(
                id.clone(),
                json!({
                    "configured":limits,
                    "effective":effective_limits(&server,limits)
                }),
            );
        }
    }
    json!({
        "scope":"server",
        "availability":"live",
        "source":"in_process_supervisor",
        "server_limits":server,
        "active_root_operations":settings["active_root_operations"],
        "active_child_workers":settings["active_child_workers"],
        "queued_roots":settings["queued_roots"],
        "waiting_client_tool_handoffs":settings["outstanding_client_tool_handoffs"],
        "reserved_client_tool_handoff_slots":settings["reserved_client_tool_handoff_slots"],
        "workforces":workforces
    })
}

fn effective_limits(server: &Value, workforce: &Value) -> Value {
    let minimum = |key: &str| -> Value {
        match (server[key].as_u64(), workforce[key].as_u64()) {
            (Some(server), Some(workforce)) => json!(server.min(workforce)),
            (Some(server), None) => json!(server),
            (None, Some(workforce)) => json!(workforce),
            _ => Value::Null,
        }
    };
    json!({
        "max_root_operations":minimum("max_root_operations"),
        "max_child_workers":minimum("max_child_workers"),
        "max_queued_roots":minimum("max_queued_roots"),
        "max_queue_wait_seconds":minimum("max_queue_wait_seconds")
    })
}

async fn service_status(
    State(state): State<Arc<crate::state::AppState>>,
    lifecycle: Extension<Arc<crate::health::Lifecycle>>,
) -> Json<Value> {
    let mut status = lifecycle.0.safe_snapshot();
    let admission = lifecycle.0.active_admissions();
    let supervisor = state.portfolio.async_ops.drain_status().await;
    let persistence = persistence_status(&state);
    let mut blockers = Vec::new();
    if status["accepting"] == true {
        blockers.push("service_not_draining");
    }
    if status["phase"] == "starting" {
        blockers.push("service_starting");
    }
    if admission > 0 {
        blockers.push("admission_active");
    }
    if supervisor.active_roots > 0 {
        blockers.push("active_root_operations");
    }
    if supervisor.active_child_workers > 0 {
        blockers.push("active_child_workers");
    }
    if supervisor.queued_roots > 0 {
        blockers.push("queued_root_operations");
    }
    if persistence["error_present"] == true {
        blockers.push("persistence_error");
    }
    if persistence["write_pending"] == true {
        blockers.push("persistence_pending");
    }
    status["admission"] = json!({
        "active":admission,
        "accepting":status["accepting"]
    });
    status["async_supervisor"] = serde_json::to_value(supervisor).unwrap_or(Value::Null);
    status["persistence"] = persistence;
    // Held client-tool handoffs and pending approvals are durably recoverable and
    // deliberately do not block a planned restart. They are still reported.
    status["planned_restart"] = json!({
        "safe_to_stop":blockers.is_empty(),
        "blockers":blockers,
        "recoverable_after_restart":["waiting_client_tool_handoffs","pending_tool_approvals"],
        "requires":"drain, then poll this status until safe_to_stop before sending TERM"
    });
    Json(status)
}

fn persistence_status(state: &crate::state::AppState) -> Value {
    let Some(storage) = &state.portfolio.storage else {
        return json!({"availability":"not_configured","healthy":true,"write_pending":false});
    };
    let mut metrics = storage.metrics();
    let error_present = !metrics["error"].is_null();
    let write_pending = metrics["pending_pages"].as_u64().unwrap_or_default() > 0
        || metrics["flushing"].as_bool().unwrap_or(false)
        || metrics["commit_generation"].as_u64().unwrap_or_default()
            > metrics["persisted_generation"].as_u64().unwrap_or_default();
    if let Some(fields) = metrics.as_object_mut() {
        // Persistence error strings can contain machine-specific paths or other
        // details. Report health without disclosing the raw diagnostic.
        fields.remove("error");
    }
    json!({
        "availability":"available",
        "healthy":!error_present,
        "error_present":error_present,
        "write_pending":write_pending,
        "metrics":metrics
    })
}

async fn service_drain(
    lifecycle: Extension<Arc<crate::health::Lifecycle>>,
) -> (StatusCode, Json<Value>) {
    lifecycle.0.begin_drain();
    (
        StatusCode::ACCEPTED,
        Json(json!({"accepted":true,"service":lifecycle.0.safe_snapshot()})),
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SettingsPatch {
    changes: Value,
}

async fn global_settings(
    State(state): State<Arc<crate::state::AppState>>,
    Extension(auth): Extension<crate::auth::AuthContext>,
) -> Result<(HeaderMap, Json<Value>), HttpError> {
    require(&auth, crate::auth::RequiredPermission::BootstrapAdmin)?;
    let value = global_settings_value(&state)?;
    let revision = value["revision"].as_str().unwrap_or_default();
    let mut headers = HeaderMap::new();
    headers.insert(header::ETAG, etag(revision));
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok((headers, Json(value)))
}

async fn update_global_settings(
    State(state): State<Arc<crate::state::AppState>>,
    Extension(auth): Extension<crate::auth::AuthContext>,
    lifecycle: Option<Extension<Arc<crate::health::Lifecycle>>>,
    headers: HeaderMap,
    Json(body): Json<SettingsPatch>,
) -> Result<(HeaderMap, Json<Value>), HttpError> {
    require(&auth, crate::auth::RequiredPermission::BootstrapAdmin)?;
    let _admission = admit_new_work(lifecycle.as_ref().map(|Extension(value)| value))?;
    let expected = read_if_match(&headers)?;
    let value = update_global_settings_value(&state, &expected, &body.changes)?;
    let revision = value["revision"].as_str().unwrap_or_default();
    let mut response_headers = HeaderMap::new();
    response_headers.insert(header::ETAG, etag(revision));
    response_headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok((response_headers, Json(value)))
}

fn global_settings_value(state: &crate::state::AppState) -> Result<Value, HttpError> {
    let path = config_path();
    let _guard = global_config_lock().lock().unwrap();
    let current_bytes = std::fs::read(&path).unwrap_or_default();
    let revision = digest(&current_bytes);
    let saved = read_config_value(&current_bytes)?;
    settings_projection(state, &saved, &revision)
}

fn update_global_settings_value(
    state: &crate::state::AppState,
    expected: &str,
    changes: &Value,
) -> Result<Value, HttpError> {
    let path = config_path();
    let _guard = global_config_lock().lock().unwrap();
    let current_bytes = std::fs::read(&path).unwrap_or_default();
    let current_revision = digest(&current_bytes);
    if current_revision != expected {
        return Err(conflict(&current_revision));
    }
    let mut candidate = read_config_value(&current_bytes)?;
    apply_settings_patch(&mut candidate, changes)?;
    persist_validated_config(&path, &candidate, &state)?;
    let bytes = std::fs::read(&path).map_err(server_error)?;
    let revision = digest(&bytes);
    let saved = read_config_value(&bytes)?;
    settings_projection(state, &saved, &revision)
}

fn config_path() -> std::path::PathBuf {
    #[cfg(test)]
    if let Some(path) = TEST_CONFIG_PATH
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .unwrap()
        .clone()
    {
        return path;
    }
    std::env::var_os("CCM_CONFIG")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("ccm.json"))
}

#[cfg(test)]
static TEST_CONFIG_PATH: std::sync::OnceLock<std::sync::Mutex<Option<std::path::PathBuf>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
pub(crate) fn set_test_config_path(path: Option<std::path::PathBuf>) {
    *TEST_CONFIG_PATH
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .unwrap() = path;
}

fn global_config_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

fn digest(bytes: &[u8]) -> String {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn read_config_value(bytes: &[u8]) -> Result<Value, HttpError> {
    if bytes.is_empty() {
        return Ok(json!({}));
    }
    let value: Value = serde_json::from_slice(bytes).map_err(bad_request)?;
    if !value.is_object() {
        return Err(bad_request("configuration root must be an object"));
    }
    Ok(value)
}

fn policy_value(value: &Value, key: &str) -> Result<String, HttpError> {
    let policy: crate::policy::CreationPolicy =
        serde_json::from_value(value.clone()).map_err(|_| bad_request(format!("invalid {key}")))?;
    serde_json::to_value(policy)
        .map_err(server_error)?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| bad_request(format!("invalid {key}")))
}

fn personality_value(value: &Value) -> Result<String, HttpError> {
    let mode: crate::config::PersonalityTransparency = serde_json::from_value(value.clone())
        .map_err(|_| {
            bad_request("personality_transparency must be introvert, extrovert, or flexible")
        })?;
    serde_json::to_value(mode)
        .map_err(server_error)?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| bad_request("invalid personality_transparency"))
}

fn validate_timeout(name: &str, value: &Value) -> Result<u64, HttpError> {
    let minimum = value
        .as_u64()
        .ok_or_else(|| bad_request(format!("upstream.{name} must be a positive integer")))?;
    if minimum == 0 || minimum > 86_400_000 {
        return Err(bad_request(format!(
            "upstream.{name} must be between 1 and 86400000"
        )));
    }
    Ok(minimum)
}

fn sandbox_mode(value: &Value) -> Result<Option<bool>, HttpError> {
    match value.as_str() {
        Some("platform_default" | "inherit") => Ok(None),
        Some("on") => Ok(Some(true)),
        Some("off") => Ok(Some(false)),
        _ => Err(bad_request(
            "sandbox mode must be platform_default, inherit, on, or off",
        )),
    }
}

fn apply_settings_patch(config: &mut Value, changes: &Value) -> Result<(), HttpError> {
    let Some(changes) = changes.as_object() else {
        return Err(bad_request("changes must be an object"));
    };
    if changes.is_empty() {
        return Err(bad_request("changes must not be empty"));
    }
    for (key, value) in changes {
        match key.as_str() {
            "creation_policy" => {
                let policy = policy_value(value, key)?;
                config["creation_policy"] = json!(policy);
            }
            "personality_transparency" => {
                config["personality_transparency"] = json!(personality_value(value)?);
            }
            "upstream" => {
                let patch = value
                    .as_object()
                    .ok_or_else(|| bad_request("upstream must be an object"))?;
                if patch.is_empty() {
                    return Err(bad_request("upstream patch must not be empty"));
                }
                let allowed = [
                    "connect_timeout_ms",
                    "first_response_timeout_ms",
                    "idle_timeout_ms",
                    "total_timeout_ms",
                    "max_attempts",
                    "retry_base_ms",
                ];
                for (field, value) in patch {
                    if !allowed.contains(&field.as_str()) {
                        return Err(bad_request(format!("upstream.{field} is not configurable")));
                    }
                    let number = validate_timeout(field, value)?;
                    if field == "max_attempts" && number > 10 {
                        return Err(bad_request(
                            "upstream.max_attempts must be between 1 and 10",
                        ));
                    }
                    if field == "total_timeout_ms" && number > 1_800_000 {
                        return Err(bad_request(
                            "upstream.total_timeout_ms exceeds the 30-minute cap",
                        ));
                    }
                    config["upstream"][field] = json!(number);
                }
            }
            "memory" => {
                let patch = value
                    .as_object()
                    .ok_or_else(|| bad_request("memory must be an object"))?;
                if patch.len() != 1 || !patch.contains_key("budget_mib") {
                    return Err(bad_request("memory may contain only budget_mib"));
                }
                let budget = patch["budget_mib"]
                    .as_u64()
                    .ok_or_else(|| bad_request("memory.budget_mib must be an integer"))?;
                if !(32..=16_384).contains(&budget) {
                    return Err(bad_request(
                        "memory.budget_mib must be between 32 and 16384",
                    ));
                }
                config["memory"]["budget_mib"] = json!(budget);
            }
            "sandbox" => apply_sandbox_patch(config, value)?,
            _ => return Err(bad_request(format!("{key} is not configurable"))),
        }
    }
    if let Some(total) = config["upstream"]["total_timeout_ms"].as_u64()
        && total > 1_800_000
    {
        return Err(bad_request(
            "upstream.total_timeout_ms exceeds the 30-minute cap",
        ));
    }
    Ok(())
}

fn apply_sandbox_patch(config: &mut Value, value: &Value) -> Result<(), HttpError> {
    let patch = value
        .as_object()
        .ok_or_else(|| bad_request("sandbox must be an object"))?;
    if patch.is_empty()
        || patch
            .keys()
            .any(|key| !["global_mode", "mcp_stdio_modes"].contains(&key.as_str()))
    {
        return Err(bad_request(
            "sandbox accepts only global_mode and mcp_stdio_modes",
        ));
    }
    if let Some(mode) = patch.get("global_mode") {
        let enabled = sandbox_mode(mode)?;
        let tools = config["tools"]
            .as_object_mut()
            .ok_or_else(|| bad_request("tools configuration must be an object"))?;
        match enabled {
            Some(enabled) => {
                tools.insert("sandbox_enabled".into(), json!(enabled));
            }
            None => {
                tools.remove("sandbox_enabled");
            }
        }
    }
    if let Some(modes) = patch.get("mcp_stdio_modes") {
        let modes = modes
            .as_object()
            .ok_or_else(|| bad_request("sandbox.mcp_stdio_modes must be an object"))?;
        let servers = config["tools"]["mcp_servers"]
            .as_object_mut()
            .ok_or_else(|| bad_request("tools.mcp_servers must be an object"))?;
        for (server_id, mode) in modes {
            let server = servers
                .get_mut(server_id)
                .ok_or_else(|| bad_request(format!("unknown MCP server id {server_id}")))?;
            if server
                .pointer("/transport/transport")
                .and_then(Value::as_str)
                != Some("stdio")
            {
                return Err(bad_request(format!("MCP server {server_id} is not stdio")));
            }
            let enabled = sandbox_mode(mode)?;
            let transport = server
                .get_mut("transport")
                .and_then(Value::as_object_mut)
                .ok_or_else(|| bad_request("invalid MCP transport configuration"))?;
            let sandbox = transport.entry("sandbox").or_insert_with(|| json!({}));
            let sandbox = sandbox
                .as_object_mut()
                .ok_or_else(|| bad_request("MCP sandbox configuration must be an object"))?;
            match enabled {
                Some(enabled) => {
                    sandbox.insert("enabled".into(), json!(enabled));
                }
                None => {
                    sandbox.remove("enabled");
                }
            }
        }
    }
    Ok(())
}

fn persist_validated_config(
    path: &std::path::Path,
    candidate: &Value,
    state: &crate::state::AppState,
) -> Result<(), HttpError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    std::fs::create_dir_all(parent).map_err(server_error)?;
    let file_name = path.file_name().unwrap_or_default().to_string_lossy();
    let temporary = parent.join(format!(
        ".{file_name}.ccm-{}.tmp",
        uuid::Uuid::new_v4().simple()
    ));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let serialized = serde_json::to_vec_pretty(candidate).map_err(server_error)?;
    let result = (|| -> Result<(), HttpError> {
        use std::io::Write;
        let mut file = options.open(&temporary).map_err(server_error)?;
        file.write_all(&serialized).map_err(server_error)?;
        file.write_all(b"\n").map_err(server_error)?;
        file.sync_all().map_err(server_error)?;
        drop(file);
        let loaded = crate::config::ServerConfig::load(&temporary, true).map_err(bad_request)?;
        let mut effective = (*state.config).clone();
        effective.creation = loaded.creation;
        effective.improvement = loaded.improvement;
        effective.personality_transparency = loaded.personality_transparency;
        effective.memory = loaded.memory;
        effective.routing = loaded.routing;
        loaded
            .operational
            .apply(&mut effective)
            .map_err(bad_request)?;
        effective.initialize_models().map_err(bad_request)?;
        std::fs::rename(&temporary, path).map_err(server_error)?;
        if let Ok(directory) = std::fs::File::open(parent) {
            let _ = directory.sync_all();
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

fn saved_upstream(file: &Value) -> Value {
    let defaults = crate::config::UpstreamConfig::default();
    let raw = file.get("upstream").unwrap_or(&Value::Null);
    json!({
        "connect_timeout_ms":raw.get("connect_timeout_ms").and_then(Value::as_u64).unwrap_or(defaults.connect_timeout_ms),
        "first_response_timeout_ms":raw.get("first_response_timeout_ms").and_then(Value::as_u64).unwrap_or(defaults.first_response_timeout_ms),
        "idle_timeout_ms":raw.get("idle_timeout_ms").and_then(Value::as_u64).unwrap_or(defaults.idle_timeout_ms),
        "total_timeout_ms":raw.get("total_timeout_ms").and_then(Value::as_u64).unwrap_or(defaults.total_timeout_ms),
        "max_attempts":raw.get("max_attempts").and_then(Value::as_u64).unwrap_or(defaults.max_attempts as u64),
        "retry_base_ms":raw.get("retry_base_ms").and_then(Value::as_u64).unwrap_or(defaults.retry_base_ms)
    })
}

fn saved_sandbox(file: &Value) -> Value {
    let tools = file.get("tools").unwrap_or(&Value::Null);
    let global = match tools.get("sandbox_enabled") {
        Some(Value::Bool(true)) => "on",
        Some(Value::Bool(false)) => "off",
        _ => "platform_default",
    };
    let mut stdio = serde_json::Map::new();
    if let Some(servers) = tools.get("mcp_servers").and_then(Value::as_object) {
        for (id, server) in servers {
            if server
                .pointer("/transport/transport")
                .and_then(Value::as_str)
                != Some("stdio")
            {
                continue;
            }
            let mode = match server.pointer("/transport/sandbox/enabled") {
                Some(Value::Bool(true)) => "on",
                Some(Value::Bool(false)) => "off",
                _ => "inherit",
            };
            stdio.insert(id.clone(), json!(mode));
        }
    }
    json!({"global_mode":global,"mcp_stdio_modes":stdio})
}

fn source_value(file: &Value) -> Value {
    let field_source = |root: &str, name: &str, env: &str| {
        if file.pointer(&format!("/{root}/{name}")).is_some() {
            "config_file"
        } else if std::env::var_os(env).is_some() {
            "environment"
        } else {
            "default"
        }
    };
    let mut timeout_sources = serde_json::Map::new();
    for (name, env) in [
        ("connect_timeout_ms", "CCM_UPSTREAM_CONNECT_TIMEOUT_MS"),
        (
            "first_response_timeout_ms",
            "CCM_UPSTREAM_FIRST_RESPONSE_TIMEOUT_MS",
        ),
        ("idle_timeout_ms", "CCM_UPSTREAM_IDLE_TIMEOUT_MS"),
        ("total_timeout_ms", "CCM_UPSTREAM_TOTAL_TIMEOUT_MS"),
        ("max_attempts", "CCM_UPSTREAM_MAX_ATTEMPTS"),
        ("retry_base_ms", "CCM_UPSTREAM_RETRY_BASE_MS"),
    ] {
        timeout_sources.insert(name.into(), json!(field_source("upstream", name, env)));
    }
    let sandbox = saved_sandbox(file);
    let mut stdio_source = serde_json::Map::new();
    if let Some(modes) = sandbox["mcp_stdio_modes"].as_object() {
        for (id, _) in modes {
            stdio_source.insert(id.clone(), json!("config_file"));
        }
    }
    json!({
        "creation_policy":if file.get("creation_policy").is_some(){"config_file"}else{"default"},
        "personality_transparency":if file.get("personality_transparency").is_some(){"config_file"}else{"default"},
        "upstream":timeout_sources,
        "memory":if file.pointer("/memory/budget_mib").is_some(){"config_file"}else{"default"},
        "sandbox":{"global_mode":if file.pointer("/tools/sandbox_enabled").is_some(){"config_file"}else{"platform_default"},"mcp_stdio_modes":stdio_source}
    })
}

fn settings_projection(
    state: &crate::state::AppState,
    file: &Value,
    revision: &str,
) -> Result<Value, HttpError> {
    let default_policy = file.get("creation_policy").cloned().unwrap_or_else(|| {
        serde_json::to_value(crate::policy::CreationPolicy::default()).unwrap_or(json!("Dynamic"))
    });
    let saved_personality = file
        .get("personality_transparency")
        .cloned()
        .unwrap_or_else(|| json!("introvert"));
    let saved_memory = file
        .pointer("/memory/budget_mib")
        .cloned()
        .unwrap_or_else(|| json!(crate::config::MemoryConfig::default().budget_mib));
    let effective_sandbox = state.sandbox_runtime_status();
    let saved = json!({
        "creation_policy":default_policy,
        "personality_transparency":saved_personality,
        "upstream":saved_upstream(file),
        "memory":{"budget_mib":saved_memory},
        "sandbox":saved_sandbox(file)
    });
    let effective = json!({
        "creation_policy":state.config.creation.policy,
        "personality_transparency":state.config.personality_transparency,
        "upstream":state.config.upstream,
        "memory":{"budget_mib":state.config.memory.budget_mib},
        "sandbox":effective_sandbox
    });
    Ok(json!({
        "scope":"global",
        "revision":revision,
        "saved":saved,
        "effective":effective,
        "source":source_value(file),
        "application_timing":{
            "creation_policy":"restart_required",
            "personality_transparency":"restart_required",
            "upstream":"restart_required",
            "memory":"restart_required",
            "sandbox":"restart_required"
        }
    }))
}

fn client_identity(state: &crate::state::AppState, headers: &HeaderMap) -> String {
    let identity = state
        .auth
        .as_ref()
        .map(|auth| format!("key:{}", auth.key_id))
        .unwrap_or_else(|| crate::policy::identity(headers, false));
    state.config.client_scope(&identity)
}

fn client_override_snapshot(
    state: &crate::state::AppState,
    client: &str,
) -> Result<(Value, String), HttpError> {
    let conn = state.portfolio.conn.lock().unwrap();
    let policy: Option<String> = conn
        .query_row(
            "SELECT policy FROM client_creation_policy WHERE client=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)",
            rusqlite::params![client,state.portfolio.workforce_scope(),state.portfolio.key_scope()],
            |row| row.get(0),
        )
        .optional()
        .map_err(server_error)?;
    let personality: Option<String> = conn
        .query_row(
            "SELECT mode FROM client_personality WHERE client=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)",
            rusqlite::params![client,state.portfolio.workforce_scope(),state.portfolio.key_scope()],
            |row| row.get(0),
        )
        .optional()
        .map_err(server_error)?;
    drop(conn);
    let saved_policy: Option<Value> = policy
        .as_ref()
        .map(|text| serde_json::from_str(text).map_err(server_error))
        .transpose()?;
    let effective_policy = saved_policy.clone().unwrap_or_else(|| {
        serde_json::to_value(state.config.creation.policy).unwrap_or(json!("Dynamic"))
    });
    let saved_personality = personality.as_ref().map(|mode| json!(mode));
    let effective_personality = if state.config.personality_transparency
        == crate::config::PersonalityTransparency::Flexible
    {
        saved_personality
            .clone()
            .unwrap_or_else(|| json!("introvert"))
    } else {
        serde_json::to_value(state.config.personality_transparency).unwrap_or(json!("introvert"))
    };
    let value = json!({
        "scope":"current_key",
        "saved":{"creation_policy":saved_policy,"personality_transparency":saved_personality},
        "effective":{"creation_policy":effective_policy,"personality_transparency":effective_personality},
        "server_effective":{
            "upstream":state.config.upstream,
            "memory":{"budget_mib":state.config.memory.budget_mib}
        },
        "source":{
            "creation_policy":if policy.is_some(){"current_key_override"}else{"server_default"},
            "personality_transparency":if state.config.personality_transparency != crate::config::PersonalityTransparency::Flexible {"server_fixed"}else if personality.is_some(){"current_key_override"}else{"flexible_default"}
        },
        "application_timing":{
            "creation_policy":"next_request",
            "personality_transparency":"next_request_if_server_mode_flexible",
            "upstream":"read_only_here_restart_required_global",
            "memory":"read_only_here_restart_required_global"
        }
    });
    let revision = digest(value.to_string().as_bytes());
    Ok((value, revision))
}

/// Current-key settings projection shared by management tools and MCP
/// `config.get`. It includes only the current key's overrides plus read-only
/// server operational values that are safe for that key; global configuration and
/// server-wide async limits are not included.
pub fn current_key_settings_value(state: &crate::state::AppState) -> Result<Value, String> {
    let client = client_identity(state, &HeaderMap::new());
    let (mut value, revision) =
        client_override_snapshot(state, &client).map_err(http_error_message)?;
    value["revision"] = json!(revision);
    Ok(value)
}

fn has_permission(
    state: &crate::state::AppState,
    permission: crate::auth::RequiredPermission,
) -> bool {
    state
        .auth
        .as_ref()
        .is_none_or(|auth| auth.allows(permission))
}

/// Foreman-only tools are capability-filtered before the routing model sees
/// them, then checked again at execution. They are intentionally separate from
/// automatic creation tools so explicit management never inherits timing policy.
pub fn foreman_tool_specs(state: &crate::state::AppState) -> Vec<Value> {
    use crate::auth::RequiredPermission as P;

    let mut tools = vec![json!({
        "name":"management.server_context",
        "description":"Answer a question about Foreman’s identity and safe, authoritative runtime facts. Does not delegate workspace work.",
        "arguments":{}
    })];
    if has_permission(state, P::RosterRead) {
        tools.extend([
            json!({"name":"management.inspect_definition","description":"Inspect one active or retained agent/workflow definition by its exact id.","arguments":{"kind":"agent|workflow","id":"exact definition id","include_retired":"boolean"}}),
            json!({"name":"management.inspect_history","description":"Inspect the retained definition versions, evaluator assessments and change lineage for one exact agent/workflow id.","arguments":{"kind":"agent|workflow","id":"exact definition id"}}),
        ]);
    }
    if has_permission(state, P::RosterWrite) {
        tools.extend([
            json!({"name":"management.create_definition","description":"Explicitly create/register an agent or workflow through its dedicated generator immediately, independent of automatic creation policy. This operation does not run the new definition. Include an exact quote from the latest user instruction.","arguments":{"kind":"agent|workflow","requirements":"explicit reusable definition requirements","user_instruction":"exact quote from the latest user instruction"}}),
            json!({"name":"management.retire_definition","description":"Retire an active agent/workflow while preserving its history; retirement is not permanent deletion. Include an exact quote from the latest user instruction.","arguments":{"kind":"agent|workflow","id":"exact definition id","user_instruction":"exact quote from the latest user instruction"}}),
            json!({"name":"management.evaluate_definition","description":"Ask the evaluator to assess an active definition against supplied task/result evidence. The evaluator owns any revision or retirement decision. Include an exact quote from the latest user instruction and only supplied evidence.","arguments":{"kind":"agent|workflow","id":"exact definition id","task":"observed task","result":"observed result","user_instruction":"exact quote from the latest user instruction"}}),
        ]);
    }
    if has_permission(state, P::RosterUse) {
        tools.extend([
            json!({"name":"management.get_settings","description":"Read settings for the current bearer key only; global settings require bootstrap administration.","arguments":{"scope":"current_key"}}),
            json!({"name":"management.update_settings","description":"Update only current-key creation-policy/personality overrides. Changes require the exact current revision, an exact quote from the latest user instruction and apply to subsequent requests.","arguments":{"scope":"current_key","revision":"exact revision from the current snapshot","changes":"allowlisted current-key changes","user_instruction":"exact quote from the latest user instruction"}}),
        ]);
    }
    if has_permission(state, P::BootstrapAdmin) {
        tools.extend([
            json!({"name":"management.get_settings","description":"Read saved/effective global server settings, their revision, source and application timing.","arguments":{"scope":"global"}}),
            json!({"name":"management.update_settings","description":"Update allowlisted global server settings using compare-and-swap against the exact revision. Global writes are bootstrap-admin-only; settings requiring restart remain saved but are not claimed effective until restart. Include an exact quote from the latest user instruction.","arguments":{"scope":"global","revision":"exact revision from the current snapshot","changes":"allowlisted global changes","user_instruction":"exact quote from the latest user instruction"}}),
        ]);
    }
    tools
}

/// Safe authoritative facts provided to Foreman before routing. Global settings,
/// provider capacity and server-wide async counts are included only for admins;
/// ordinary keys receive their own settings and exact-key operation counts.
pub async fn foreman_context(state: &crate::state::AppState) -> Result<Value, String> {
    let tools = foreman_tool_specs(state)
        .into_iter()
        .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
        .collect::<Vec<_>>();
    let auth = state.auth.as_ref();
    let scope = auth.map_or_else(
        || json!({"type":"local"}),
        |auth| {
            json!({"type":"key_workforce","key_id":auth.key_id,"workforce_id":auth.workforce_id})
        },
    );
    let mut context = json!({
        "identity":"Foreman",
        "role":"The service spokesperson and orchestrator; dedicated generators create definitions and the evaluator owns quality assessments.",
        "scope":scope,
        "available_management_operations":tools,
        "global_settings":"bootstrap_admin_only"
    });
    if auth.is_some_and(|auth| auth.permissions.roster_use) {
        let client = client_identity(state, &HeaderMap::new());
        let (mut settings, revision) =
            client_override_snapshot(state, &client).map_err(http_error_message)?;
        settings["revision"] = json!(revision);
        context["current_key_settings"] = settings;
    }
    if let Some(auth) = auth {
        if auth.permissions.bootstrap_admin {
            context["runtime"] = json!({
                "scope":"server",
                "async_supervisor":state.portfolio.async_ops.drain_status().await,
                "providers":safe_provider_context(state),
                "native_tool_sandbox":state.sandbox_runtime_status()
            });
        } else if auth.permissions.roster_read
            && let Ok(counts) = state
                .portfolio
                .async_ops
                .scoped_counts(&state.portfolio)
                .await
        {
            context["runtime"] = json!({
                "scope":"key_workforce",
                "async_supervisor":counts
            });
        }
        if auth.permissions.bootstrap_admin {
            context["global_settings"] =
                global_settings_value(state).map_err(http_error_message)?;
        }
    }
    Ok(context)
}

fn safe_provider_context(state: &crate::state::AppState) -> Vec<Value> {
    state
        .config
        .routing
        .providers
        .iter()
        .map(|(provider_id, provider)| {
            let models = state
                .config
                .routing
                .models
                .iter()
                .filter(|(model_id, _)| {
                    state
                        .config
                        .select_model(Some(model_id))
                        .is_ok_and(|model| model.routing.provider == *provider_id)
                })
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            let gate = state.providers.get(provider_id, provider).snapshot();
            json!({
                "provider_id":provider_id,
                "configured_model_ids":models,
                "max_concurrent_requests":provider.max_concurrent_requests,
                "active_requests":gate.active_requests,
                "waiting_requests":gate.waiting_requests,
                "circuit_state":gate.circuit_state,
                "cooldown_remaining_ms":gate.cooldown_remaining_ms
            })
        })
        .collect()
}

fn http_error_message(error: HttpError) -> String {
    let (_, Json(body)) = error;
    match body.get("error") {
        Some(Value::String(message)) => message.clone(),
        Some(Value::Object(value)) if value.get("current_revision").is_some() => {
            format!(
                "revision conflict; current revision is {}",
                value["current_revision"]
            )
        }
        Some(value) => value.to_string(),
        None => "management operation failed".into(),
    }
}

pub fn is_foreman_management_tool(name: &str) -> bool {
    matches!(
        name,
        "management.server_context"
            | "management.inspect_definition"
            | "management.inspect_history"
            | "management.create_definition"
            | "management.retire_definition"
            | "management.evaluate_definition"
            | "management.get_settings"
            | "management.update_settings"
    )
}

pub async fn execute_foreman_tool(
    state: &Arc<crate::state::AppState>,
    request: &str,
    name: &str,
    arguments: &Value,
    payload: &Value,
    generation: u64,
) -> Result<Value, String> {
    use crate::auth::RequiredPermission as P;

    match name {
        "management.server_context" => foreman_context(state).await,
        "management.inspect_definition" => {
            require_state_permission(state, P::RosterRead)?;
            let kind = required_management_arg(arguments, "kind")?;
            let id = required_management_arg(arguments, "id")?;
            validate_kind(kind)?;
            let include_retired = arguments["include_retired"].as_bool().unwrap_or(false);
            let definition = current_definition(state, kind, id, include_retired)?
                .ok_or_else(|| format!("unknown {kind}"))?;
            Ok(json!({
                "status":"inspected",
                "resource":{"kind":kind,"id":id},
                "definition":definition
            }))
        }
        "management.inspect_history" => {
            require_state_permission(state, P::RosterRead)?;
            inspect_definition_history(
                state,
                required_management_arg(arguments, "kind")?,
                required_management_arg(arguments, "id")?,
            )
        }
        "management.create_definition" => {
            require_state_permission(state, P::RosterWrite)?;
            require_management_quote(payload, arguments)?;
            let kind = required_management_arg(arguments, "kind")?;
            let requirements = required_management_arg(arguments, "requirements")?;
            validate_kind(kind)?;
            if requirements.len() > 32 * 1024 {
                return Err("requirements exceed the 32768-byte limit".into());
            }
            let run = run_id_for_request(state, request)?;
            create_definition_for_request(state, request, &run, kind, requirements, generation)
                .await
        }
        "management.retire_definition" => {
            require_state_permission(state, P::RosterWrite)?;
            require_management_quote(payload, arguments)?;
            let kind = required_management_arg(arguments, "kind")?;
            let id = required_management_arg(arguments, "id")?;
            validate_kind(kind)?;
            let run = run_id_for_request(state, request)?;
            retire_definition_for_request(state, request, &run, kind, id)
        }
        "management.evaluate_definition" => {
            require_state_permission(state, P::RosterWrite)?;
            require_management_quote(payload, arguments)?;
            let kind = required_management_arg(arguments, "kind")?;
            let id = required_management_arg(arguments, "id")?;
            let task = required_management_arg(arguments, "task")?;
            let result = required_management_arg(arguments, "result")?;
            validate_kind(kind)?;
            let run = run_id_for_request(state, request)?;
            evaluate_definition_for_request(
                state, request, &run, kind, id, task, result, generation,
            )
            .await
        }
        "management.get_settings" => match required_management_arg(arguments, "scope")? {
            "current_key" => {
                require_state_permission(state, P::RosterUse)?;
                let client = client_identity(state, &HeaderMap::new());
                let (mut value, revision) =
                    client_override_snapshot(state, &client).map_err(http_error_message)?;
                value["revision"] = json!(revision);
                Ok(value)
            }
            "global" => {
                require_state_permission(state, P::BootstrapAdmin)?;
                global_settings_value(state).map_err(http_error_message)
            }
            _ => Err("settings scope must be current_key or global".into()),
        },
        "management.update_settings" => {
            require_management_quote(payload, arguments)?;
            let scope = required_management_arg(arguments, "scope")?;
            let revision = required_management_arg(arguments, "revision")?;
            let changes = arguments.get("changes").ok_or("missing changes")?;
            match scope {
                "current_key" => {
                    require_state_permission(state, P::RosterUse)?;
                    let client = client_identity(state, &HeaderMap::new());
                    update_client_override_value(state, &client, revision, changes)
                        .map_err(http_error_message)
                }
                "global" => {
                    require_state_permission(state, P::BootstrapAdmin)?;
                    update_global_settings_value(state, revision, changes)
                        .map_err(http_error_message)
                }
                _ => Err("settings scope must be current_key or global".into()),
            }
        }
        _ => Err(format!("unknown Foreman management operation {name}")),
    }
}

fn require_management_quote(payload: &Value, arguments: &Value) -> Result<(), String> {
    let quote = required_management_arg(arguments, "user_instruction")?;
    if crate::async_ops::quote_is_from_current_user_message(payload, quote) {
        Ok(())
    } else {
        Err("management operation requires an exact quote from the latest user instruction".into())
    }
}

fn require_state_permission(
    state: &crate::state::AppState,
    permission: crate::auth::RequiredPermission,
) -> Result<(), String> {
    if state
        .auth
        .as_ref()
        .is_some_and(|auth| !auth.allows(permission))
    {
        Err("bearer key lacks the required permission".into())
    } else {
        Ok(())
    }
}

fn required_management_arg<'a>(arguments: &'a Value, name: &str) -> Result<&'a str, String> {
    arguments
        .get(name)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing or empty {name}"))
}

fn inspect_definition_history(
    state: &crate::state::AppState,
    kind: &str,
    id: &str,
) -> Result<Value, String> {
    validate_kind(kind)?;
    let definition =
        current_definition(state, kind, id, true)?.ok_or_else(|| format!("unknown {kind}"))?;
    let conn = state.portfolio.conn.lock().unwrap();
    let mut statement = conn
        .prepare(
            "SELECT id,kind,definition,created_ms FROM definition_versions
         WHERE entity_id=?1 AND (?2 IS NULL OR workforce_id=?2) ORDER BY created_ms,rowid",
        )
        .map_err(|error| error.to_string())?;
    let versions = statement
        .query_map(
            rusqlite::params![id, state.portfolio.workforce_scope()],
            |row| {
                let text: String = row.get(2)?;
                Ok(json!({
                    "id":row.get::<_,String>(0)?,
                    "kind":row.get::<_,String>(1)?,
                    "definition":serde_json::from_str::<Value>(&text).unwrap_or(Value::Null),
                    "created_ms":row.get::<_,i64>(3)?
                }))
            },
        )
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let mut statement = conn
        .prepare(
            "SELECT before_version,after_version,action,evidence,applied,run_id,span_id,at_ms
         FROM definition_changes WHERE entity_id=?1 AND (?2 IS NULL OR workforce_id=?2)
         AND (?3 IS NULL OR owner_key_id=?3) ORDER BY id",
        )
        .map_err(|error| error.to_string())?;
    let changes = statement
        .query_map(
            rusqlite::params![
                id,
                state.portfolio.workforce_scope(),
                state.portfolio.key_scope()
            ],
            |row| {
                let evidence: String = row.get(3)?;
                Ok(json!({
                    "before_version":row.get::<_,Option<String>>(0)?,
                    "after_version":row.get::<_,Option<String>>(1)?,
                    "action":row.get::<_,String>(2)?,
                    "evidence":serde_json::from_str::<Value>(&evidence).unwrap_or(Value::Null),
                    "applied":row.get::<_,bool>(4)?,
                    "run_id":row.get::<_,Option<String>>(5)?,
                    "span_id":row.get::<_,Option<String>>(6)?,
                    "at_ms":row.get::<_,i64>(7)?
                }))
            },
        )
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let mut statement = conn
        .prepare(
            "SELECT request_id,verdict,before_definition,after_definition,applied,created_at
         FROM evaluations WHERE entity_id=?1 AND (?2 IS NULL OR workforce_id=?2)
         AND (?3 IS NULL OR owner_key_id=?3) ORDER BY id",
        )
        .map_err(|error| error.to_string())?;
    let evaluations = statement
        .query_map(
            rusqlite::params![
                id,
                state.portfolio.workforce_scope(),
                state.portfolio.key_scope()
            ],
            |row| {
                Ok(json!({
                    "request_id":row.get::<_,String>(0)?,
                    "verdict":row.get::<_,String>(1)?,
                    "before":row.get::<_,String>(2)?,
                    "after":row.get::<_,Option<String>>(3)?,
                    "applied":row.get::<_,bool>(4)?,
                    "created_at":row.get::<_,i64>(5)?
                }))
            },
        )
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    Ok(json!({
        "status":"inspected",
        "resource":{"kind":kind,"id":id,"definition":definition},
        "versions":versions,
        "changes":changes,
        "evaluations":evaluations
    }))
}

async fn client_overrides(
    State(state): State<Arc<crate::state::AppState>>,
    Extension(auth): Extension<crate::auth::AuthContext>,
    headers: HeaderMap,
) -> Result<(HeaderMap, Json<Value>), HttpError> {
    require(&auth, crate::auth::RequiredPermission::RosterUse)?;
    let scoped = state.with_auth(&auth);
    let client = client_identity(&scoped, &headers);
    let (mut value, revision) = client_override_snapshot(&scoped, &client)?;
    value["revision"] = json!(revision);
    let mut response_headers = HeaderMap::new();
    response_headers.insert(header::ETAG, etag(&revision));
    response_headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok((response_headers, Json(value)))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientOverridePatch {
    changes: Value,
}

async fn update_client_overrides(
    State(state): State<Arc<crate::state::AppState>>,
    Extension(auth): Extension<crate::auth::AuthContext>,
    lifecycle: Option<Extension<Arc<crate::health::Lifecycle>>>,
    headers: HeaderMap,
    Json(body): Json<ClientOverridePatch>,
) -> Result<(HeaderMap, Json<Value>), HttpError> {
    require(&auth, crate::auth::RequiredPermission::RosterUse)?;
    let expected = read_if_match(&headers)?;
    let scoped = state.with_auth(&auth);
    let client = client_identity(&scoped, &headers);
    let patch = body
        .changes
        .as_object()
        .ok_or_else(|| bad_request("changes must be an object"))?;
    if patch.is_empty()
        || patch
            .keys()
            .any(|key| !["creation_policy", "personality_transparency"].contains(&key.as_str()))
    {
        return Err(bad_request(
            "current-key overrides accept only creation_policy and personality_transparency",
        ));
    }
    let _admission = admit_new_work(lifecycle.as_ref().map(|Extension(value)| value))?;
    let value = update_client_override_value(&scoped, &client, &expected, &body.changes)?;
    let revision = value["revision"].as_str().unwrap_or_default();
    let mut response_headers = HeaderMap::new();
    response_headers.insert(header::ETAG, etag(revision));
    response_headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok((response_headers, Json(value)))
}

fn update_client_override_value(
    scoped: &crate::state::AppState,
    client: &str,
    expected: &str,
    changes: &Value,
) -> Result<Value, HttpError> {
    let patch = changes
        .as_object()
        .ok_or_else(|| bad_request("changes must be an object"))?;
    if patch.is_empty()
        || patch
            .keys()
            .any(|key| !["creation_policy", "personality_transparency"].contains(&key.as_str()))
    {
        return Err(bad_request(
            "current-key overrides accept only creation_policy and personality_transparency",
        ));
    }
    let revision_guard = global_config_lock().lock().unwrap();
    let (_, current_revision) = client_override_snapshot(scoped, client)?;
    if current_revision != expected {
        return Err(conflict(&current_revision));
    }
    let now = crate::observe::now_ms();
    {
        let mut conn = scoped.portfolio.conn.lock().unwrap();
        let tx = conn.transaction().map_err(server_error)?;
        if let Some(value) = patch.get("creation_policy") {
            if value.is_null() {
                tx.execute(
                    "DELETE FROM client_creation_policy WHERE client=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)",
                    rusqlite::params![client,scoped.portfolio.workforce_scope(),scoped.portfolio.key_scope()],
                ).map_err(server_error)?;
            } else {
                let policy = policy_value(value, "creation_policy")?;
                tx.execute(
                    "INSERT INTO client_creation_policy(client,policy,updated_ms,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4,?5)
                     ON CONFLICT(client) DO UPDATE SET policy=excluded.policy,updated_ms=excluded.updated_ms,workforce_id=excluded.workforce_id,owner_key_id=excluded.owner_key_id",
                    rusqlite::params![client,json!(policy).to_string(),now,scoped.portfolio.workforce_scope(),scoped.portfolio.key_scope()],
                ).map_err(server_error)?;
            }
        }
        if let Some(value) = patch.get("personality_transparency") {
            if value.is_null() {
                tx.execute(
                    "DELETE FROM client_personality WHERE client=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)",
                    rusqlite::params![client,scoped.portfolio.workforce_scope(),scoped.portfolio.key_scope()],
                ).map_err(server_error)?;
            } else {
                let mode = value
                    .as_str()
                    .filter(|mode| ["introvert", "extrovert"].contains(mode))
                    .ok_or_else(|| {
                        bad_request(
                            "personality_transparency override must be introvert or extrovert",
                        )
                    })?;
                tx.execute(
                    "INSERT INTO client_personality(client,mode,updated_ms,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4,?5)
                     ON CONFLICT(client) DO UPDATE SET mode=excluded.mode,updated_ms=excluded.updated_ms,workforce_id=excluded.workforce_id,owner_key_id=excluded.owner_key_id",
                    rusqlite::params![client,mode,now,scoped.portfolio.workforce_scope(),scoped.portfolio.key_scope()],
                ).map_err(server_error)?;
            }
        }
        tx.commit().map_err(server_error)?;
    }
    drop(revision_guard);
    let (mut value, revision) = client_override_snapshot(scoped, client)?;
    value["revision"] = json!(revision);
    Ok(value)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateDefinitionRequest {
    kind: String,
    requirements: String,
}

async fn create_definition_http(
    State(state): State<Arc<crate::state::AppState>>,
    Extension(auth): Extension<crate::auth::AuthContext>,
    lifecycle: Option<Extension<Arc<crate::health::Lifecycle>>>,
    Json(body): Json<CreateDefinitionRequest>,
) -> Result<(StatusCode, Json<Value>), HttpError> {
    require(&auth, crate::auth::RequiredPermission::RosterWrite)?;
    let _admission = admit_new_work(lifecycle.as_ref().map(|Extension(value)| value))?;
    validate_kind(&body.kind).map_err(bad_request)?;
    if body.requirements.trim().is_empty() || body.requirements.len() > 32 * 1024 {
        return Err(bad_request("requirements must contain 1 to 32768 bytes"));
    }
    create_definition(&state.with_auth(&auth), &body.kind, &body.requirements)
        .await
        .map(|value| (StatusCode::OK, Json(value)))
        .map_err(operation_http_error)
}

async fn retire_definition_http(
    State(state): State<Arc<crate::state::AppState>>,
    Extension(auth): Extension<crate::auth::AuthContext>,
    lifecycle: Option<Extension<Arc<crate::health::Lifecycle>>>,
    Path((kind, id)): Path<(String, String)>,
) -> Result<Json<Value>, HttpError> {
    require(&auth, crate::auth::RequiredPermission::RosterWrite)?;
    let _admission = admit_new_work(lifecycle.as_ref().map(|Extension(value)| value))?;
    validate_kind(&kind).map_err(bad_request)?;
    retire_definition(&state.with_auth(&auth), &kind, &id)
        .map(Json)
        .map_err(operation_http_error)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EvaluateRequest {
    task: String,
    result: String,
}

async fn evaluate_definition_http(
    State(state): State<Arc<crate::state::AppState>>,
    Extension(auth): Extension<crate::auth::AuthContext>,
    lifecycle: Option<Extension<Arc<crate::health::Lifecycle>>>,
    Path((kind, id)): Path<(String, String)>,
    Json(body): Json<EvaluateRequest>,
) -> Result<Json<Value>, HttpError> {
    require(&auth, crate::auth::RequiredPermission::RosterWrite)?;
    let _admission = admit_new_work(lifecycle.as_ref().map(|Extension(value)| value))?;
    validate_kind(&kind).map_err(bad_request)?;
    if body.task.trim().is_empty() || body.result.trim().is_empty() {
        return Err(bad_request("task and result must not be empty"));
    }
    evaluate_definition(
        &state.with_auth(&auth),
        &kind,
        &id,
        &body.task,
        &body.result,
    )
    .await
    .map(Json)
    .map_err(operation_http_error)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunDefinitionRequest {
    message: String,
    #[serde(default)]
    conversation_id: Option<String>,
    #[serde(default)]
    tools: Option<Value>,
    #[serde(default)]
    tool_choice: Option<Value>,
    #[serde(default)]
    response_format: Option<Value>,
}

async fn run_definition_http(
    State(state): State<Arc<crate::state::AppState>>,
    Extension(auth): Extension<crate::auth::AuthContext>,
    lifecycle: Option<Extension<Arc<crate::health::Lifecycle>>>,
    Path((kind, id)): Path<(String, String)>,
    Json(body): Json<RunDefinitionRequest>,
) -> Response {
    if let Err(error) = require(&auth, crate::auth::RequiredPermission::RosterUse) {
        return error.into_response();
    }
    let admission = match admit_new_work(lifecycle.as_ref().map(|Extension(value)| value)) {
        Ok(admission) => admission,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = validate_kind(&kind) {
        return bad_request(error).into_response();
    }
    if body.message.trim().is_empty() {
        return bad_request("message must not be empty").into_response();
    }
    let mut args = json!({"id":id,"message":body.message});
    if let Some(value) = body.conversation_id {
        args["conversation_id"] = json!(value);
    }
    if let Some(value) = body.tools {
        args["tools"] = value;
    }
    if let Some(value) = body.tool_choice {
        args["tool_choice"] = value;
    }
    if let Some(value) = body.response_format {
        args["response_format"] = value;
    }
    crate::api::mcp_server::run_selected_http(state.with_auth(&auth), &kind, &args, admission).await
}

fn validate_kind(kind: &str) -> Result<(), String> {
    if ["agent", "workflow"].contains(&kind) {
        Ok(())
    } else {
        Err("kind must be agent or workflow".into())
    }
}

fn operation_http_error(error: String) -> HttpError {
    if error.contains("unknown active")
        || error.contains("unknown agent")
        || error.contains("unknown workflow")
    {
        (StatusCode::NOT_FOUND, Json(json!({"error":error})))
    } else if error.contains("changed") || error.contains("conflict") || error.contains("retired") {
        (StatusCode::CONFLICT, Json(json!({"error":error})))
    } else if error.contains("permission") {
        auth_error(error)
    } else {
        (StatusCode::BAD_REQUEST, Json(json!({"error":error})))
    }
}

fn require_write(state: &crate::state::AppState) -> Result<(), String> {
    if state
        .auth
        .as_ref()
        .is_some_and(|auth| !auth.permissions.roster_write)
    {
        Err("roster write permission required".into())
    } else {
        Ok(())
    }
}

fn new_management_trace(
    state: &crate::state::AppState,
    message: &str,
    source: &str,
) -> Result<(String, String), String> {
    let request = uuid::Uuid::new_v4().to_string();
    let payload = json!({"messages":[{"role":"user","content":message}]});
    let run = state.portfolio.trace_begin(&request, &payload, source)?;
    let client = state
        .auth
        .as_ref()
        .map(|auth| format!("key:{}", auth.key_id))
        .unwrap_or_else(|| source.to_owned());
    state
        .portfolio
        .bind_policy(&request, &run, &client, state.config.creation.policy)?;
    Ok((request, run))
}

fn run_id_for_request(state: &crate::state::AppState, request: &str) -> Result<String, String> {
    state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT q.run_id FROM trace_requests q
         JOIN trace_runs r ON r.id=q.run_id
         WHERE q.id=?1 AND (?2 IS NULL OR r.workforce_id=?2)
         AND (?3 IS NULL OR r.owner_key_id=?3)",
            rusqlite::params![
                request,
                state.portfolio.workforce_scope(),
                state.portfolio.key_scope()
            ],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())
}

fn definition_change_action(
    state: &crate::state::AppState,
    run: &str,
    id: &str,
) -> Result<String, String> {
    state.portfolio.conn.lock().unwrap().query_row(
        "SELECT action FROM definition_changes WHERE run_id=?1 AND entity_id=?2 AND (?3 IS NULL OR workforce_id=?3) AND (?4 IS NULL OR owner_key_id=?4) ORDER BY id DESC LIMIT 1",
        rusqlite::params![run,id,state.portfolio.workforce_scope(),state.portfolio.key_scope()],
        |row| row.get(0),
    ).map_err(|error| error.to_string())
}

pub async fn create_definition(
    state: &Arc<crate::state::AppState>,
    kind: &str,
    requirements: &str,
) -> Result<Value, String> {
    validate_kind(kind)?;
    require_write(state)?;
    let (request, run) = new_management_trace(state, requirements, "management")?;
    let value = create_definition_for_request(
        state,
        &request,
        &run,
        kind,
        requirements,
        state.portfolio.generation(),
    )
    .await;
    match value {
        Ok(result) => {
            state
                .portfolio
                .trace_finish_request(&request, "completed", &result)?;
            Ok(result)
        }
        Err(error) => {
            let _ =
                state
                    .portfolio
                    .trace_finish_request(&request, "error", &json!({"error":error}));
            Err(error)
        }
    }
}

pub async fn create_definition_for_request(
    state: &Arc<crate::state::AppState>,
    request: &str,
    run: &str,
    kind: &str,
    requirements: &str,
    generation: u64,
) -> Result<Value, String> {
    validate_kind(kind)?;
    require_write(state)?;
    if requirements.trim().is_empty() {
        return Err("requirements must not be empty".into());
    }
    let definition = if kind == "agent" {
        serde_json::to_value(
            crate::meta::create_agent(state, request, requirements, generation).await?,
        )
        .map_err(|error| error.to_string())?
    } else {
        serde_json::to_value(
            crate::meta::create_workflow(state, request, requirements, generation).await?,
        )
        .map_err(|error| error.to_string())?
    };
    let id = definition["id"]
        .as_str()
        .ok_or("generated definition has no id")?;
    let action = definition_change_action(state, run, id).unwrap_or_else(|_| "create".into());
    let status = if action == "reuse" {
        "reused"
    } else {
        "created"
    };
    let result = json!({
        "status":status,
        "resource":{"kind":kind,"id":id,"name":definition["name"],"definition":definition},
        "request_id":request,
        "run_id":run,
        "operation_id":Value::Null,
        "receipt":{"action":action,"resource_id":id,"run_id":run,"applied":true}
    });
    state
        .portfolio
        .trace_event(request, "management.definition_created", &result["receipt"])?;
    Ok(result)
}

pub async fn evaluate_definition(
    state: &Arc<crate::state::AppState>,
    kind: &str,
    id: &str,
    task: &str,
    result: &str,
) -> Result<Value, String> {
    validate_kind(kind)?;
    require_write(state)?;
    let (request, run) = new_management_trace(state, task, "management")?;
    let evaluation = evaluate_definition_for_request(
        state,
        &request,
        &run,
        kind,
        id,
        task,
        result,
        state.portfolio.generation(),
    )
    .await;
    match evaluation {
        Ok(value) => {
            state
                .portfolio
                .trace_finish_request(&request, "completed", &value)?;
            Ok(value)
        }
        Err(error) => {
            let _ =
                state
                    .portfolio
                    .trace_finish_request(&request, "error", &json!({"error":error}));
            Err(error)
        }
    }
}

pub async fn evaluate_definition_for_request(
    state: &Arc<crate::state::AppState>,
    request: &str,
    run: &str,
    kind: &str,
    id: &str,
    task: &str,
    result: &str,
    generation: u64,
) -> Result<Value, String> {
    validate_kind(kind)?;
    require_write(state)?;
    let before = current_definition(state, kind, id, false)?
        .ok_or_else(|| format!("unknown active {kind}"))?;
    let assessment =
        crate::meta::evaluate(state, request, kind, id, &before, task, result, generation).await?;
    let action = assessment["verdict"]["action"].as_str().unwrap_or("keep");
    let applied = assessment["applied"].as_bool().unwrap_or(false);
    let value = json!({
        "status":"evaluated",
        "resource":{"kind":kind,"id":id},
        "verdict":assessment["verdict"],
        "applied":applied,
        "request_id":request,
        "run_id":run,
        "operation_id":Value::Null,
        "receipt":{"action":action,"resource_id":id,"run_id":run,"applied":applied}
    });
    state.portfolio.trace_event(
        request,
        "management.definition_evaluated",
        &value["receipt"],
    )?;
    Ok(value)
}

fn current_definition(
    state: &crate::state::AppState,
    kind: &str,
    id: &str,
    include_retired: bool,
) -> Result<Option<Value>, String> {
    let conn = state.portfolio.conn.lock().unwrap();
    if kind == "agent" {
        conn.query_row(
            "SELECT id,name,purpose,system_prompt,model,provider,retired,retired_ms FROM agents WHERE id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 OR retired=0)",
            rusqlite::params![id,state.portfolio.workforce_scope(),include_retired],
            |row| Ok(json!({"id":row.get::<_,String>(0)?,"name":row.get::<_,String>(1)?,"purpose":row.get::<_,String>(2)?,"system_prompt":row.get::<_,String>(3)?,"model":row.get::<_,String>(4)?,"provider":row.get::<_,String>(5)?,"retired":row.get::<_,bool>(6)?,"retired_ms":row.get::<_,Option<i64>>(7)?})),
        ).optional().map_err(|error|error.to_string())
    } else {
        conn.query_row(
            "SELECT definition,retired FROM workflows WHERE id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 OR retired=0)",
            rusqlite::params![id,state.portfolio.workforce_scope(),include_retired],
            |row| {
                let text: String = row.get(0)?;
                let mut value: Value = serde_json::from_str(&text).map_err(|error|rusqlite::Error::FromSqlConversionFailure(0,rusqlite::types::Type::Text,Box::new(error)))?;
                value["retired"] = json!(row.get::<_,bool>(1)?);
                Ok(value)
            },
        ).optional().map_err(|error|error.to_string())
    }
}

pub fn retire_definition(
    state: &Arc<crate::state::AppState>,
    kind: &str,
    id: &str,
) -> Result<Value, String> {
    let (request, run) = new_management_trace(state, &format!("Retire {kind} {id}"), "management")?;
    match retire_definition_for_request(state, &request, &run, kind, id) {
        Ok(value) => {
            state
                .portfolio
                .trace_finish_request(&request, "completed", &value)?;
            Ok(value)
        }
        Err(error) => {
            let _ =
                state
                    .portfolio
                    .trace_finish_request(&request, "error", &json!({"error":error}));
            Err(error)
        }
    }
}

pub fn retire_definition_for_request(
    state: &Arc<crate::state::AppState>,
    request: &str,
    run: &str,
    kind: &str,
    id: &str,
) -> Result<Value, String> {
    validate_kind(kind)?;
    require_write(state)?;
    let before = match current_definition(state, kind, id, true)? {
        Some(value) => value,
        None => return Err(format!("unknown {kind}")),
    };
    if before["retired"] == true {
        let value = json!({"status":"already_retired","resource":{"kind":kind,"id":id,"name":before["name"]},"request_id":request,"run_id":run,"operation_id":Value::Null,"receipt":{"action":"already_retired","resource_id":id,"run_id":run,"applied":false}});
        return Ok(value);
    }
    let mut conn = state.portfolio.conn.lock().unwrap();
    let tx = conn.transaction().map_err(|error| error.to_string())?;
    crate::observe::archive_definitions(&tx).map_err(|error| error.to_string())?;
    let changed = if kind == "agent" {
        tx.execute("UPDATE agents SET retired=1 WHERE id=?1 AND retired=0 AND (?2 IS NULL OR workforce_id=?2)",rusqlite::params![id,state.portfolio.workforce_scope()])
    } else {
        tx.execute("UPDATE workflows SET retired=1 WHERE id=?1 AND retired=0 AND (?2 IS NULL OR workforce_id=?2)",rusqlite::params![id,state.portfolio.workforce_scope()])
    }.map_err(|error|error.to_string())?;
    if changed != 1 {
        return Err("definition changed during retirement".into());
    }
    tx.commit().map_err(|error| error.to_string())?;
    drop(conn);
    state.portfolio.trace_changes(
        request,
        id,
        kind,
        Some(&before),
        None,
        "retire",
        &json!({"authority":"explicit_management_request"}),
        true,
    )?;
    let value = json!({
        "status":"retired",
        "resource":{"kind":kind,"id":id,"name":before["name"]},
        "request_id":request,
        "run_id":run,
        "operation_id":Value::Null,
        "receipt":{"action":"retire","resource_id":id,"run_id":run,"applied":true}
    });
    state
        .portfolio
        .trace_event(request, "management.definition_retired", &value["receipt"])?;
    Ok(value)
}

pub async fn execute_selected_definition(
    state: Arc<crate::state::AppState>,
    kind: &str,
    id: &str,
    payload: Value,
    request: &str,
) -> Result<crate::handlers::Execution, String> {
    validate_kind(kind)?;
    current_definition(&state, kind, id, false)?.ok_or_else(|| format!("unknown active {kind}"))?;
    let mut route = if kind == "agent" {
        json!({"decision":"use","agent_id":id,"note":"Explicit management selection"})
    } else {
        json!({"decision":"use_workflow","workflow_id":id,"note":"Explicit management selection"})
    };
    route["execution_mode"] = json!("sync");
    crate::handlers::execute_seeded(state, payload, request.to_owned(), route, true).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sandbox_patch_distinguishes_platform_default_explicit_off_and_mcp_inherit() {
        let mut file = json!({"tools":{"sandbox_enabled":true,"mcp_servers":{"stdio":{"transport":{"transport":"stdio","command":"/bin/true","sandbox":{"enabled":true}}}}}});
        apply_settings_patch(&mut file, &json!({"sandbox":{"global_mode":"platform_default","mcp_stdio_modes":{"stdio":"inherit"}}})).unwrap();
        assert!(file.pointer("/tools/sandbox_enabled").is_none());
        assert!(
            file.pointer("/tools/mcp_servers/stdio/transport/sandbox/enabled")
                .is_none()
        );
        apply_settings_patch(
            &mut file,
            &json!({"sandbox":{"global_mode":"off","mcp_stdio_modes":{"stdio":"on"}}}),
        )
        .unwrap();
        assert_eq!(file["tools"]["sandbox_enabled"], false);
        assert_eq!(
            file["tools"]["mcp_servers"]["stdio"]["transport"]["sandbox"]["enabled"],
            true
        );
    }

    #[test]
    fn global_settings_patch_is_allowlisted_and_validates_bounds() {
        let mut file = json!({});
        apply_settings_patch(&mut file, &json!({"creation_policy":"Dynamic","memory":{"budget_mib":512},"upstream":{"total_timeout_ms":900000}})).unwrap();
        assert_eq!(file["creation_policy"], "Dynamic");
        assert_eq!(file["memory"]["budget_mib"], 512);
        assert!(apply_settings_patch(&mut file, &json!({"api_key":"no"})).is_err());
        assert!(apply_settings_patch(&mut file, &json!({"memory":{"budget_mib":16385}})).is_err());
        assert!(
            apply_settings_patch(
                &mut file,
                &json!({"sandbox":{"global_mode":"off","shell_enabled":true}})
            )
            .is_err()
        );
    }

    #[test]
    fn runtime_effective_caps_are_the_minimum_of_server_and_override() {
        let server = json!({"max_root_operations":4,"max_child_workers":4,"max_queued_roots":8,"max_queue_wait_seconds":900});
        let overrides = json!({"max_root_operations":2,"max_child_workers":null,"max_queued_roots":16,"max_queue_wait_seconds":60});
        assert_eq!(
            effective_limits(&server, &overrides),
            json!({"max_root_operations":2,"max_child_workers":4,"max_queued_roots":8,"max_queue_wait_seconds":60})
        );
    }
}
