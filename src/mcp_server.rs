//! Authenticated Streamable HTTP MCP adapter for CCM's existing operations.
//!
//! The adapter owns protocol/session behavior only. Execution, workforce
//! mutation, improvement evaluation, observation and async supervision stay on
//! their existing CCM paths and retain their normal key/workforce scopes.

use axum::{
    Extension, Json, Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response, Sse, sse::KeepAlive},
    routing::post,
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

const SERVER_NAME: &str = "ccm";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
const PROTOCOL_VERSIONS: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26"];
const SESSION_TTL: Duration = Duration::from_secs(8 * 60 * 60);
const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;
const MAX_TOOL_RESULT_BYTES: usize = 1024 * 1024;
const MAX_PAGE_SIZE: u32 = 100;
const DEFAULT_PAGE_SIZE: u32 = 50;
const MAX_PAGE_OFFSET: u32 = 1_000_000;

static SESSIONS: OnceLock<Mutex<HashMap<String, McpSession>>> = OnceLock::new();

#[derive(Clone)]
struct McpSession {
    key_id: String,
    workforce_id: String,
    protocol_version: String,
    touched: Instant,
}

type McpPublicationSlot = Arc<Mutex<Option<McpPublication>>>;

struct McpPublication {
    request_id: String,
    run_id: String,
    message: McpPublicationMessage,
    action: McpPublicationAction,
    retryable: bool,
}

enum McpPublicationAction {
    Handoff,
    Complete,
}

enum McpPublicationMessage {
    Conversation {
        conversation_id: String,
        assistant: Value,
    },
    ApprovalResult(Value),
}

#[derive(Clone)]
struct ToolSpec {
    name: &'static str,
    description: &'static str,
    permission: crate::auth::RequiredPermission,
    input_schema: Value,
}

fn sessions() -> &'static Mutex<HashMap<String, McpSession>> {
    SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn routes() -> Router<Arc<crate::state::AppState>> {
    Router::new().route("/mcp", post(post_mcp).get(get_mcp).delete(delete_mcp))
}

fn string_schema() -> Value {
    json!({"type":"string"})
}

fn nonempty_string_schema() -> Value {
    json!({"type":"string","minLength":1})
}

fn boolean_schema() -> Value {
    json!({"type":"boolean"})
}

fn integer_schema(minimum: i64) -> Value {
    json!({"type":"integer","minimum":minimum})
}

fn bounded_integer_schema(minimum: i64, maximum: i64) -> Value {
    json!({"type":"integer","minimum":minimum,"maximum":maximum})
}

fn bounded_string_schema(minimum: u64, maximum: u64) -> Value {
    json!({"type":"string","minLength":minimum,"maxLength":maximum})
}

fn array_schema(items: Value) -> Value {
    json!({"type":"array","items":items})
}

fn object_schema(properties: Value, required: &[&str]) -> Value {
    json!({
        "type":"object",
        "properties":properties,
        "required":required,
        "additionalProperties":false
    })
}

fn generic_object_schema() -> Value {
    json!({"type":"object","additionalProperties":true})
}

fn tool_specs() -> Vec<ToolSpec> {
    use crate::auth::RequiredPermission as P;

    let arbitrary_tools = array_schema(generic_object_schema());
    let tool_result = object_schema(
        json!({"tool_call_id":nonempty_string_schema(),"content":string_schema()}),
        &["tool_call_id", "content"],
    );
    let tool_results = array_schema(tool_result);
    let permissions = object_schema(
        json!({
            "roster_use":boolean_schema(),
            "roster_read":boolean_schema(),
            "roster_write":boolean_schema(),
            "improvement_contribute":boolean_schema(),
            "bootstrap_admin":boolean_schema()
        }),
        &[],
    );
    let list_paging = json!({
        "offset":integer_schema(0),
        "limit":integer_schema(1),
        "kind":{"type":"string","enum":["agent","workflow"]},
        "query":string_schema(),
        "include_retired":boolean_schema()
    });
    let run_messages = array_schema(generic_object_schema());
    let mut specs = vec![];
    let mut add = |name, description, permission, input_schema| {
        specs.push(ToolSpec {
            name,
            description,
            permission,
            input_schema,
        });
    };

    add(
        "foreman.chat",
        "Send a message through Foreman or return exact pending client-tool results. If an internal tool needs one-time approval, returns status waiting_approval and handles instead of an assistant answer; use tool_approvals.* to resolve it. Conversations and handoffs are private to this bearer key.",
        P::RosterUse,
        object_schema(
            json!({
                "message":string_schema(),
                "conversation_id":string_schema(),
                "tool_results":tool_results,
                "tools":arbitrary_tools,
                "tool_choice":{},
                "response_format":{}
            }),
            &[],
        ),
    );
    add(
        "foreman.conversations",
        "List this key's saved Foreman conversations with bounded pagination.",
        P::RosterRead,
        object_schema(
            json!({"offset":integer_schema(0),"limit":integer_schema(1)}),
            &[],
        ),
    );
    add(
        "foreman.conversation",
        "Read a bounded page of messages from a conversation owned by this key.",
        P::RosterRead,
        object_schema(
            json!({
                "id":nonempty_string_schema(),
                "offset":integer_schema(0),
                "limit":integer_schema(1)
            }),
            &["id"],
        ),
    );
    add(
        "foreman.start_async",
        "Explicitly start a background Foreman request. The returned operation uses CCM's async supervisor and must be polled with operations.status/result.",
        P::RosterUse,
        object_schema(
            json!({
                "message":nonempty_string_schema(),
                "tools":arbitrary_tools,
                "tool_choice":{},
                "response_format":{}
            }),
            &["message"],
        ),
    );
    add(
        "workforce.list",
        "List the authenticated workforce's agent/workflow roster. Definitions and usage details are omitted; retired entries are opt-in.",
        P::RosterRead,
        object_schema(list_paging.clone(), &[]),
    );
    add(
        "workforce.get",
        "Get one active definition, or a retained retired definition when include_retired is true.",
        P::RosterRead,
        object_schema(
            json!({
                "id":nonempty_string_schema(),
                "kind":{"type":"string","enum":["agent","workflow"]},
                "include_retired":boolean_schema()
            }),
            &["id"],
        ),
    );
    add(
        "workforce.create_agent",
        "Create/reuse an agent through CCM's dedicated agent-generator meta-agent.",
        P::RosterWrite,
        object_schema(
            json!({"requirements":nonempty_string_schema()}),
            &["requirements"],
        ),
    );
    add(
        "workforce.create_workflow",
        "Create/reuse a workflow through CCM's dedicated workflow-generator meta-agent.",
        P::RosterWrite,
        object_schema(
            json!({"requirements":nonempty_string_schema()}),
            &["requirements"],
        ),
    );
    add(
        "workforce.evaluate",
        "Ask CCM's Evaluator to keep, revise, or retire an active definition using supplied task/result evidence. Mutations are Evaluator-owned; retirement removes an item from selection while retaining history.",
        P::RosterWrite,
        object_schema(
            json!({
                "kind":{"type":"string","enum":["agent","workflow"]},
                "id":nonempty_string_schema(),
                "task":nonempty_string_schema(),
                "result":nonempty_string_schema()
            }),
            &["kind", "id", "task", "result"],
        ),
    );
    for (name, kind) in [
        ("workforce.run_agent", "agent"),
        ("workforce.run_workflow", "workflow"),
    ] {
        add(
            name,
            if kind == "agent" {
                "Run the explicitly selected active agent, then have Foreman compose from its report. If an internal tool needs one-time approval, returns status waiting_approval and handles while the same run is paused; use tool_approvals.* to resolve it. Optional client tools use foreman.chat."
            } else {
                "Run the explicitly selected active workflow, then have Foreman compose from its report. If an internal tool needs one-time approval, returns status waiting_approval and handles while the same run is paused; use tool_approvals.* to resolve it. Optional client tools use foreman.chat."
            },
            P::RosterUse,
            object_schema(
                json!({
                    "id":nonempty_string_schema(),
                    "message":nonempty_string_schema(),
                    "conversation_id":string_schema(),
                    "tools":arbitrary_tools,
                    "tool_choice":{},
                    "response_format":{}
                }),
                &["id", "message"],
            ),
        );
    }
    add(
        "operations.status",
        "Read the status and budgets of an async operation owned by this key.",
        P::RosterRead,
        object_schema(json!({"id":nonempty_string_schema()}), &["id"]),
    );
    add(
        "operations.result",
        "Read the current result/error of an async operation owned by this key.",
        P::RosterRead,
        object_schema(json!({"id":nonempty_string_schema()}), &["id"]),
    );
    add(
        "operations.cancel",
        "Cancel an async operation owned by this key.",
        P::RosterWrite,
        object_schema(json!({"id":nonempty_string_schema()}), &["id"]),
    );
    add(
        "operations.continue",
        "Resume an async operation after returning every pending client-tool result. Supply the full role-labelled message array, including the pending assistant tool_calls and matching tool messages.",
        P::RosterUse,
        object_schema(
            json!({
                "id":nonempty_string_schema(),
                "messages":run_messages,
                "tools":arbitrary_tools,
                "tool_choice":{},
                "response_format":{}
            }),
            &["id", "messages"],
        ),
    );
    let approval_id = bounded_string_schema(1, 128);
    add(
        "tool_approvals.status",
        "Inspect one pending or resolved tool approval owned by this exact bearer key and workforce. Approval is bound to its original request, run, tool and arguments.",
        P::RosterUse,
        object_schema(json!({"id":approval_id}), &["id"]),
    );
    add(
        "tool_approvals.approve",
        "Approve this invoker's exact pending tool operation once. This resumes the paused original run and never changes stored tool policy.",
        P::RosterUse,
        object_schema(json!({"id":approval_id}), &["id"]),
    );
    add(
        "tool_approvals.deny",
        "Deny this invoker's exact pending tool operation once. The paused run resumes with a denial observation; the operation is not performed and stored tool policy is unchanged.",
        P::RosterUse,
        object_schema(json!({"id":approval_id}), &["id"]),
    );
    add(
        "tool_approvals.cancel",
        "Cancel this invoker's exact pending approval and its paused run. Requires roster_use, not roster_write; it cannot cancel another key's work.",
        P::RosterUse,
        object_schema(json!({"id":approval_id}), &["id"]),
    );
    add(
        "tool_approvals.result",
        "Read the original paused run's current status or resumed result after an approval decision. Results are bounded; async work can also be read with operations.result.",
        P::RosterUse,
        object_schema(json!({"id":approval_id}), &["id"]),
    );
    add(
        "observability.runs",
        "List this key's recent runs with status/source filtering and bounded pagination.",
        P::RosterRead,
        object_schema(
            json!({
                "offset":integer_schema(0),
                "limit":integer_schema(1),
                "status":string_schema(),
                "source":string_schema()
            }),
            &[],
        ),
    );
    add(
        "observability.run",
        "Read a scoped run summary and one bounded page of compact spans/events.",
        P::RosterRead,
        object_schema(
            json!({
                "id":nonempty_string_schema(),
                "offset":integer_schema(0),
                "limit":integer_schema(1),
                "span_kind":string_schema(),
                "event_kind":string_schema()
            }),
            &["id"],
        ),
    );
    add(
        "observability.events",
        "Read this key's events after a sequence cursor, optionally filtered by run/kind. Results are bounded and secret fields are redacted.",
        P::RosterRead,
        object_schema(
            json!({
                "after":integer_schema(0),
                "limit":integer_schema(1),
                "run_id":string_schema(),
                "kind":string_schema()
            }),
            &[],
        ),
    );
    add(
        "observability.statistics",
        "Read bounded pages of the existing scoped aggregate statistics, optionally filtered by kind/entity/status.",
        P::RosterRead,
        object_schema(
            json!({
                "offset":integer_schema(0),
                "limit":integer_schema(1),
                "kind":string_schema(),
                "entity_id":string_schema(),
                "status":string_schema()
            }),
            &[],
        ),
    );
    add(
        "observability.improvements",
        "Read a bounded page of improvement-job history for one scoped run.",
        P::RosterRead,
        object_schema(
            json!({
                "run_id":nonempty_string_schema(),
                "offset":integer_schema(0),
                "limit":integer_schema(1)
            }),
            &["run_id"],
        ),
    );
    add(
        "improvements.suggestions",
        "Read this key's private improvement suggestions. Shared projections appear only when the key has roster_read and roster_write, matching CCM's existing policy.",
        P::ImprovementSuggestionsRead,
        object_schema(
            json!({
                "offset":integer_schema(0),
                "limit":integer_schema(1),
                "status":string_schema()
            }),
            &[],
        ),
    );
    add(
        "improvements.declassify",
        "Explicitly release this source key's generated replacement suggestion to eligible same-workforce roster writers. This records consent but does not apply the suggestion or return its private content.",
        P::ImprovementContribute,
        object_schema(json!({"id":nonempty_string_schema()}), &["id"]),
    );
    add(
        "improvements.apply",
        "Apply a pending, explicitly applicable suggestion through CCM's existing conflict-checked operation.",
        P::RosterWrite,
        object_schema(json!({"id":nonempty_string_schema()}), &["id"]),
    );
    add(
        "config.get",
        "Read a safe projection of effective CCM configuration and live async supervisor limits. Credentials and environment secret values are not exposed.",
        P::RosterRead,
        object_schema(json!({}), &[]),
    );
    add(
        "config.async_limits.get",
        "Read the live async supervisor's server limits, all workforce overrides, active counts and queue depth. This bounded configuration subset uses CCM's bootstrap-admin control plane; values are in-memory and are not persisted to ccm.json.",
        P::BootstrapAdmin,
        object_schema(json!({}), &[]),
    );
    add(
        "config.async_limits.set_server",
        "Replace the live async supervisor's four server limits using CCM's existing bootstrap-admin operation. Bounds: roots and child workers 1-256, queued roots 0-4096, queue wait 1-86400 seconds. Changes are in-memory, not ccm.json writes.",
        P::BootstrapAdmin,
        object_schema(
            json!({
                "max_root_operations":bounded_integer_schema(1,256),
                "max_child_workers":bounded_integer_schema(1,256),
                "max_queued_roots":bounded_integer_schema(0,4096),
                "max_queue_wait_seconds":bounded_integer_schema(1,86400)
            }),
            &[
                "max_root_operations",
                "max_child_workers",
                "max_queued_roots",
                "max_queue_wait_seconds",
            ],
        ),
    );
    add(
        "config.async_limits.set_workforce",
        "Set bounded live async limits for an existing workforce using CCM's bootstrap-admin operation. Omitted fields clear those overrides; configured values must also fit within the current server limits. Changes are in-memory, not ccm.json writes.",
        P::BootstrapAdmin,
        object_schema(
            json!({
                "workforce_id":bounded_string_schema(1,128),
                "limits":object_schema(
                    json!({
                        "max_root_operations":bounded_integer_schema(1,256),
                        "max_child_workers":bounded_integer_schema(1,256),
                        "max_queued_roots":bounded_integer_schema(0,4096),
                        "max_queue_wait_seconds":bounded_integer_schema(1,86400)
                    }),
                    &[]
                )
            }),
            &["workforce_id", "limits"],
        ),
    );
    add(
        "admin.workforces.list",
        "List workforces using CCM's bootstrap-admin workforce API.",
        P::BootstrapAdmin,
        object_schema(json!({}), &[]),
    );
    add(
        "admin.workforces.create",
        "Create a private or shared workforce using CCM's bootstrap-admin workforce API.",
        P::BootstrapAdmin,
        object_schema(
            json!({
                "name":nonempty_string_schema(),
                "visibility":{"type":"string","enum":["private","shared"]}
            }),
            &["name"],
        ),
    );
    add(
        "admin.keys.list",
        "List key metadata and permissions; bearer tokens are never returned.",
        P::BootstrapAdmin,
        object_schema(json!({}), &[]),
    );
    add(
        "admin.keys.create",
        "Create a bearer key through CCM's bootstrap-admin API. The new token is returned once, as required to provision that credential.",
        P::BootstrapAdmin,
        object_schema(
            json!({
                "label":nonempty_string_schema(),
                "workforce_id":string_schema(),
                "workforce_name":string_schema(),
                "permissions":permissions
            }),
            &["label"],
        ),
    );
    add(
        "admin.keys.revoke",
        "Revoke a key through CCM's bootstrap-admin API. The configured bootstrap credential cannot be revoked here.",
        P::BootstrapAdmin,
        object_schema(json!({"id":nonempty_string_schema()}), &["id"]),
    );
    drop(add);
    specs
}

async fn post_mcp(
    State(state): State<Arc<crate::state::AppState>>,
    Extension(auth): Extension<crate::auth::AuthContext>,
    lifecycle: Option<Extension<Arc<crate::health::Lifecycle>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if body.len() > MAX_BODY_BYTES {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            "MCP request body exceeds 2 MiB",
        )
            .into_response();
    }
    if !accepts_json_or_sse(&headers) {
        return StatusCode::NOT_ACCEPTABLE.into_response();
    }
    if !headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(';')
                .next()
                .is_some_and(|mime| mime.trim() == "application/json")
        })
    {
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "MCP POST requires application/json",
        )
            .into_response();
    }
    let request: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => {
            return json_rpc_response(
                StatusCode::BAD_REQUEST,
                json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}),
                None,
                None,
            );
        }
    };
    if !request.is_object() {
        return json_rpc_response(
            StatusCode::BAD_REQUEST,
            json!({"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"Invalid Request"}}),
            None,
            None,
        );
    }
    let id = request.get("id").cloned();
    let valid_id = id
        .as_ref()
        .is_none_or(|value| value.is_string() || value.is_number());
    if request["jsonrpc"] != "2.0"
        || request["method"].as_str().is_none_or(str::is_empty)
        || !valid_id
        || request
            .get("params")
            .is_some_and(|params| !params.is_object())
    {
        return json_rpc_response(
            StatusCode::BAD_REQUEST,
            rpc_error(if valid_id { id } else { None }, -32600, "Invalid Request"),
            None,
            None,
        );
    }
    let method = request["method"].as_str().unwrap();
    if method == "initialize" {
        if id.is_none() {
            return json_rpc_response(
                StatusCode::BAD_REQUEST,
                rpc_error(None, -32600, "initialize must be a request"),
                None,
                None,
            );
        }
        if headers.get(session_header()).is_some() {
            return json_rpc_response(
                StatusCode::BAD_REQUEST,
                rpc_error(
                    id,
                    -32600,
                    "initialize cannot include an existing MCP session",
                ),
                None,
                None,
            );
        }
        let params = request.get("params").unwrap_or(&Value::Null);
        let Some(version) = params["protocolVersion"].as_str() else {
            return json_rpc_response(
                StatusCode::BAD_REQUEST,
                rpc_error(id, -32602, "initialize.params.protocolVersion is required"),
                None,
                None,
            );
        };
        if !PROTOCOL_VERSIONS.contains(&version)
            || !params["capabilities"].is_object()
            || !params["clientInfo"].is_object()
        {
            return json_rpc_response(
                StatusCode::BAD_REQUEST,
                rpc_error(
                    id,
                    -32602,
                    "unsupported protocol version or invalid initialize parameters",
                ),
                None,
                None,
            );
        }
        let session_id = create_session(&auth, version);
        let result = json!({
            "protocolVersion":version,
            "capabilities":{"tools":{"listChanged":false}},
            "serverInfo":{"name":SERVER_NAME,"version":SERVER_VERSION},
            "instructions":"CCM exposes only operations permitted by the authenticated bearer key. Tool results and observability are workforce/key scoped."
        });
        return json_rpc_response(
            StatusCode::OK,
            rpc_success(id.unwrap(), result),
            Some(&session_id),
            Some(version),
        );
    }

    let (Some(session_id), Some(version)) = (
        headers.get(session_header()).and_then(|v| v.to_str().ok()),
        headers.get(protocol_header()).and_then(|v| v.to_str().ok()),
    ) else {
        return (
            StatusCode::BAD_REQUEST,
            "MCP session and protocol-version headers are required",
        )
            .into_response();
    };
    if let Err(status) = touch_session(session_id, version, &auth) {
        return status.into_response();
    }
    if method == "notifications/initialized" {
        if id.is_some() {
            return json_rpc_response(
                StatusCode::OK,
                rpc_error(
                    id,
                    -32600,
                    "notifications/initialized must not include an id",
                ),
                Some(session_id),
                Some(version),
            );
        }
        return notification_response(Some(session_id), Some(version));
    }
    let Some(request_id) = id else {
        // JSON-RPC notifications do not receive JSON-RPC response bodies.
        return notification_response(Some(session_id), Some(version));
    };
    let params = request.get("params").unwrap_or(&Value::Null);
    let mut publication_slot: Option<McpPublicationSlot> = None;
    let mut publication_valid = true;
    let mut admission_permit: Option<crate::health::AdmissionPermit> = None;
    let result = match method {
        "ping" => Some(json!({})),
        "tools/list" => Some(json!({"tools":visible_tools(&auth)})),
        "tools/call" => {
            let Some(name) = params["name"].as_str() else {
                return json_rpc_response(
                    StatusCode::OK,
                    rpc_error(Some(request_id), -32602, "tools/call requires params.name"),
                    Some(session_id),
                    Some(version),
                );
            };
            let args = params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let admission = mcp_admission_class(name, &args);
            if let Some(Extension(lifecycle)) = lifecycle.as_ref() {
                let permit = match admission {
                    McpAdmissionClass::NewWork | McpAdmissionClass::Mutation => {
                        lifecycle.try_admit_new_work().map(Some)
                    }
                    McpAdmissionClass::ExistingWork => {
                        lifecycle.try_admit_existing_work().map(Some)
                    }
                    McpAdmissionClass::ReadOnly => Ok(None),
                };
                match permit {
                    Ok(permit) => admission_permit = permit,
                    Err(_) => return crate::management::draining_response(),
                }
            }
            let scoped_state = state.with_auth(&auth);
            let slot = Arc::new(Mutex::new(None));
            let tool_result = execute_tool(&scoped_state, &auth, name, &args, &slot).await;
            let (value, publishable) = tool_result_value_checked(name, tool_result);
            publication_slot = Some(slot);
            publication_valid = publishable;
            Some(value)
        }
        other => {
            return json_rpc_response(
                StatusCode::OK,
                rpc_error(
                    Some(request_id),
                    -32601,
                    &format!("Method not found: {other}"),
                ),
                Some(session_id),
                Some(version),
            );
        }
    };
    let response = match result {
        Some(result) => {
            let response_value = rpc_success(request_id, result);
            if let Some(slot) = publication_slot
                && let Some(publication) = slot.lock().unwrap().take()
            {
                mcp_publication_response(
                    StatusCode::OK,
                    response_value,
                    Some(session_id),
                    Some(version),
                    state.with_auth(&auth),
                    publication,
                    publication_valid,
                    admission_permit.take(),
                )
            } else {
                json_rpc_response(
                    StatusCode::OK,
                    response_value,
                    Some(session_id),
                    Some(version),
                )
            }
        }
        None => notification_response(Some(session_id), Some(version)),
    };
    response
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum McpAdmissionClass {
    NewWork,
    ExistingWork,
    Mutation,
    ReadOnly,
}

fn mcp_admission_class(name: &str, arguments: &Value) -> McpAdmissionClass {
    match name {
        "foreman.chat"
            if arguments
                .get("tool_results")
                .and_then(Value::as_array)
                .is_some_and(|results| !results.is_empty()) =>
        {
            McpAdmissionClass::ExistingWork
        }
        "foreman.chat"
        | "foreman.start_async"
        | "workforce.create_agent"
        | "workforce.create_workflow"
        | "workforce.evaluate"
        | "workforce.run_agent"
        | "workforce.run_workflow"
        | "improvements.apply" => McpAdmissionClass::NewWork,
        "operations.continue"
        | "operations.cancel"
        | "tool_approvals.result"
        | "tool_approvals.approve"
        | "tool_approvals.deny"
        | "tool_approvals.cancel" => McpAdmissionClass::ExistingWork,
        "improvements.declassify"
        | "config.async_limits.set_server"
        | "config.async_limits.set_workforce"
        | "admin.workforces.create"
        | "admin.keys.create"
        | "admin.keys.revoke" => McpAdmissionClass::Mutation,
        _ => McpAdmissionClass::ReadOnly,
    }
}

async fn get_mcp(
    Extension(auth): Extension<crate::auth::AuthContext>,
    headers: HeaderMap,
) -> Response {
    let Some((session_id, version)) = request_session(&headers) else {
        return (
            StatusCode::BAD_REQUEST,
            "MCP session and protocol-version headers are required",
        )
            .into_response();
    };
    if let Err(status) = touch_session(session_id, version, &auth) {
        return status.into_response();
    }
    let accepts_sse = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|item| item.trim().starts_with("text/event-stream"))
        });
    if !accepts_sse {
        return StatusCode::NOT_ACCEPTABLE.into_response();
    }
    let stream = async_stream::stream! {
        loop {
            tokio::time::sleep(Duration::from_secs(15)).await;
            yield Ok::<_, Infallible>(axum::response::sse::Event::default().comment("CCM MCP session active"));
        }
    };
    let mut response = Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response();
    add_protocol_headers(&mut response, Some(session_id), Some(version));
    response
}

async fn delete_mcp(
    Extension(auth): Extension<crate::auth::AuthContext>,
    headers: HeaderMap,
) -> Response {
    let Some((session_id, version)) = request_session(&headers) else {
        return (
            StatusCode::BAD_REQUEST,
            "MCP session and protocol-version headers are required",
        )
            .into_response();
    };
    let mut sessions = sessions().lock().unwrap();
    prune_sessions(&mut sessions);
    let Some(session) = sessions.get(session_id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if session.key_id != auth.key_id
        || session.workforce_id != auth.workforce_id
        || session.touched.elapsed() >= SESSION_TTL
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    if session.protocol_version != version {
        return StatusCode::BAD_REQUEST.into_response();
    }
    sessions.remove(session_id);
    StatusCode::NO_CONTENT.into_response()
}

fn session_header() -> &'static HeaderName {
    static NAME: OnceLock<HeaderName> = OnceLock::new();
    NAME.get_or_init(|| HeaderName::from_static("mcp-session-id"))
}

fn protocol_header() -> &'static HeaderName {
    static NAME: OnceLock<HeaderName> = OnceLock::new();
    NAME.get_or_init(|| HeaderName::from_static("mcp-protocol-version"))
}

fn request_session<'a>(headers: &'a HeaderMap) -> Option<(&'a str, &'a str)> {
    Some((
        headers.get(session_header())?.to_str().ok()?,
        headers.get(protocol_header())?.to_str().ok()?,
    ))
}

fn create_session(auth: &crate::auth::AuthContext, version: &str) -> String {
    let mut sessions = sessions().lock().unwrap();
    prune_sessions(&mut sessions);
    let id = uuid::Uuid::new_v4().simple().to_string();
    sessions.insert(
        id.clone(),
        McpSession {
            key_id: auth.key_id.clone(),
            workforce_id: auth.workforce_id.clone(),
            protocol_version: version.to_string(),
            touched: Instant::now(),
        },
    );
    id
}

fn prune_sessions(sessions: &mut HashMap<String, McpSession>) {
    sessions.retain(|_, session| session.touched.elapsed() < SESSION_TTL);
}

fn touch_session(
    id: &str,
    version: &str,
    auth: &crate::auth::AuthContext,
) -> Result<(), StatusCode> {
    let mut sessions = sessions().lock().unwrap();
    prune_sessions(&mut sessions);
    let Some(session) = sessions.get_mut(id) else {
        return Err(StatusCode::NOT_FOUND);
    };
    if session.key_id != auth.key_id || session.workforce_id != auth.workforce_id {
        return Err(StatusCode::NOT_FOUND);
    }
    if session.protocol_version != version {
        return Err(StatusCode::BAD_REQUEST);
    }
    session.touched = Instant::now();
    Ok(())
}

fn accepts_json_or_sse(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_none_or(|value| {
            value.contains("*/*")
                || value.split(',').any(|item| {
                    item.trim().starts_with("application/json")
                        || item.trim().starts_with("text/event-stream")
                })
        })
}

fn json_rpc_response(
    status: StatusCode,
    value: Value,
    session: Option<&str>,
    version: Option<&str>,
) -> Response {
    let mut response = (status, Json(value)).into_response();
    add_protocol_headers(&mut response, session, version);
    response
}

fn mcp_publication_response(
    status: StatusCode,
    value: Value,
    session: Option<&str>,
    version: Option<&str>,
    state: Arc<crate::state::AppState>,
    publication: McpPublication,
    publishable: bool,
    admission: Option<crate::health::AdmissionPermit>,
) -> Response {
    let (body, publishable) = bounded_publication_body(&value, publishable);
    let mut guard = McpPublicationGuard {
        state,
        publication,
        publishable,
        _admission: admission,
        finished: false,
    };
    let stream = async_stream::stream! {
        yield Ok::<_, Infallible>(Bytes::from(body));
        guard.finish_after_delivery();
    };
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    add_protocol_headers(&mut response, session, version);
    response
}

fn bounded_publication_body(value: &Value, publishable: bool) -> (Vec<u8>, bool) {
    let body = match serde_json::to_vec(value) {
        Ok(body) => body,
        Err(error) => {
            tracing::error!(%error,"serializing final MCP publication failed");
            return (
                b"{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32603,\"message\":\"Internal error\"}}"
                    .to_vec(),
                false,
            );
        }
    };
    if body.len() <= MAX_TOOL_RESULT_BYTES {
        return (body, publishable);
    }

    let message = "MCP response exceeds the 1 MiB response limit";
    let rejected = rpc_success(
        value.get("id").cloned().unwrap_or(Value::Null),
        json!({
            "content":[{"type":"text","text":message}],
            "structuredContent":{"error":message},
            "isError":true
        }),
    );
    let body = serde_json::to_vec(&rejected).unwrap_or_else(|error| {
        tracing::error!(%error,"serializing rejected MCP publication failed");
        b"{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32603,\"message\":\"Internal error\"}}".to_vec()
    });
    (body, false)
}

struct McpPublicationGuard {
    state: Arc<crate::state::AppState>,
    publication: McpPublication,
    publishable: bool,
    /// Held until the response body is delivered and publication effects have
    /// completed, not merely until the response object is constructed.
    _admission: Option<crate::health::AdmissionPermit>,
    finished: bool,
}

impl McpPublicationGuard {
    fn finish_after_delivery(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;

        if !self.publishable {
            if !self.publication.retryable
                && let Err(error) = crate::handlers::cancel_run_response_gates(
                    &self.state.portfolio,
                    &self.publication.run_id,
                    &self.publication.request_id,
                )
            {
                tracing::error!(%error,"cancelling rejected MCP run publication failed");
            }
            return;
        }

        let result = persist_mcp_publication(&self.state, &self.publication).and_then(|()| {
            match self.publication.action {
                McpPublicationAction::Handoff => Ok(()),
                McpPublicationAction::Complete => crate::handlers::complete_run_response_gates(
                    &self.state,
                    &self.publication.run_id,
                    &self.publication.request_id,
                ),
            }
        });
        if let Err(error) = result {
            tracing::error!(%error,"completing delivered MCP run publication failed");
            if !self.publication.retryable
                && let Err(cancel_error) = crate::handlers::cancel_run_response_gates(
                    &self.state.portfolio,
                    &self.publication.run_id,
                    &self.publication.request_id,
                )
            {
                tracing::error!(%cancel_error,"cancelling failed MCP run publication failed");
            }
        }
    }
}

impl Drop for McpPublicationGuard {
    fn drop(&mut self) {
        if !self.finished && !self.publication.retryable {
            if let Err(error) = crate::handlers::cancel_run_response_gates(
                &self.state.portfolio,
                &self.publication.run_id,
                &self.publication.request_id,
            ) {
                tracing::error!(%error,"cancelling abandoned MCP run publication failed");
            }
        }
    }
}

fn persist_mcp_publication(
    state: &crate::state::AppState,
    publication: &McpPublication,
) -> Result<(), String> {
    match &publication.message {
        McpPublicationMessage::Conversation {
            conversation_id,
            assistant,
        } => {
            save_conversation_message(state, conversation_id, &publication.run_id, assistant)?;
            state.portfolio.trace_emit(
                &publication.run_id,
                None,
                "conversation.message",
                &json!({"conversation_id":conversation_id,"message":assistant}),
            )?;
        }
        McpPublicationMessage::ApprovalResult(result) => {
            persist_mcp_approval_message(state, &publication.run_id, result)?;
        }
    }
    Ok(())
}

fn notification_response(session: Option<&str>, version: Option<&str>) -> Response {
    let mut response = StatusCode::ACCEPTED.into_response();
    add_protocol_headers(&mut response, session, version);
    response
}

fn add_protocol_headers(response: &mut Response, session: Option<&str>, version: Option<&str>) {
    if let Some(session) = session.and_then(|value| HeaderValue::from_str(value).ok()) {
        response
            .headers_mut()
            .insert(session_header().clone(), session);
    }
    if let Some(version) = version.and_then(|value| HeaderValue::from_str(value).ok()) {
        response
            .headers_mut()
            .insert(protocol_header().clone(), version);
    }
}

fn rpc_success(id: Value, result: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"result":result})
}

fn rpc_error(id: Option<Value>, code: i64, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id.unwrap_or(Value::Null),"error":{"code":code,"message":message}})
}

fn visible_tools(auth: &crate::auth::AuthContext) -> Vec<Value> {
    tool_specs()
        .into_iter()
        .filter(|tool| auth.allows(tool.permission))
        .map(|tool| {
            json!({
                "name":tool.name,
                "description":tool.description,
                "inputSchema":tool.input_schema
            })
        })
        .collect()
}

async fn execute_tool(
    state: &Arc<crate::state::AppState>,
    auth: &crate::auth::AuthContext,
    name: &str,
    args: &Value,
    publication: &McpPublicationSlot,
) -> Result<Value, String> {
    let spec = tool_specs()
        .into_iter()
        .find(|tool| tool.name == name)
        .ok_or_else(|| format!("unknown MCP tool: {name}"))?;
    auth.require(spec.permission).map_err(str::to_string)?;
    validate_tool_arguments(args, &spec.input_schema, "arguments")?;
    if !args.is_object() {
        return Err("tool arguments must be an object".into());
    }

    match name {
        "foreman.chat" => foreman_chat(state.clone(), args, publication).await,
        "foreman.conversations" => foreman_conversations(state, args),
        "foreman.conversation" => foreman_conversation(state, args),
        "foreman.start_async" => foreman_start_async(state.clone(), auth, args).await,
        "workforce.list" => workforce_list(state, args),
        "workforce.get" => workforce_get(state, args),
        "workforce.create_agent" => {
            crate::management::create_definition(
                state,
                "agent",
                required_str(args, "requirements")?,
            )
            .await
        }
        "workforce.create_workflow" => {
            crate::management::create_definition(
                state,
                "workflow",
                required_str(args, "requirements")?,
            )
            .await
        }
        "workforce.evaluate" => {
            crate::management::evaluate_definition(
                state,
                required_str(args, "kind")?,
                required_str(args, "id")?,
                required_str(args, "task")?,
                required_str(args, "result")?,
            )
            .await
        }
        "workforce.run_agent" => run_selected(state, args, "agent", publication.clone()).await,
        "workforce.run_workflow" => {
            run_selected(state, args, "workflow", publication.clone()).await
        }
        "operations.status" => {
            let snapshot = state
                .portfolio
                .async_ops
                .status(&state.portfolio, required_str(args, "id")?)
                .await?;
            serde_json::to_value(snapshot).map_err(|error| error.to_string())
        }
        "operations.result" => {
            let (status, result, error) = state
                .portfolio
                .async_ops
                .result(&state.portfolio, required_str(args, "id")?)
                .await?;
            Ok(json!({"status":status,"result":result,"error":error}))
        }
        "operations.cancel" => {
            let snapshot = state
                .portfolio
                .async_ops
                .cancel(&state.portfolio, required_str(args, "id")?)
                .await?;
            serde_json::to_value(snapshot).map_err(|error| error.to_string())
        }
        "operations.continue" => continue_async(state.clone(), auth, args).await,
        "tool_approvals.status" => {
            state
                .portfolio
                .async_ops
                .tool_approval_status(&state.portfolio, required_str(args, "id")?)
                .await
        }
        "tool_approvals.approve" => {
            state
                .portfolio
                .async_ops
                .decide_tool_approval(
                    &state.portfolio,
                    required_str(args, "id")?,
                    crate::async_ops::ToolApprovalDecision::Approved,
                )
                .await
        }
        "tool_approvals.deny" => {
            state
                .portfolio
                .async_ops
                .decide_tool_approval(
                    &state.portfolio,
                    required_str(args, "id")?,
                    crate::async_ops::ToolApprovalDecision::Denied,
                )
                .await
        }
        "tool_approvals.cancel" => {
            state
                .portfolio
                .async_ops
                .cancel_tool_approval(&state.portfolio, required_str(args, "id")?)
                .await
        }
        "tool_approvals.result" => mcp_tool_approval_result(state, args, publication).await,
        "observability.runs" => observability_runs(state, args),
        "observability.run" => observability_run(state, args),
        "observability.events" => observability_events(state, args),
        "observability.statistics" => observability_statistics(state, args),
        "observability.improvements" => improvement_jobs(state, args),
        "improvements.suggestions" => improvement_suggestions(state, args),
        "improvements.declassify" => declassify_suggestion(state, args),
        "improvements.apply" => apply_suggestion(state, args).await,
        "config.get" => config_projection(state).await,
        "config.async_limits.get" => Ok(state.portfolio.async_ops.settings().await),
        "config.async_limits.set_server" => set_server_async_limits(state, args).await,
        "config.async_limits.set_workforce" => set_workforce_async_limits(state, args).await,
        "admin.workforces.list" => state.portfolio.list_workforces(),
        "admin.workforces.create" => create_workforce(state, args),
        "admin.keys.list" => state.portfolio.list_keys(),
        "admin.keys.create" => create_key(state, args),
        "admin.keys.revoke" => {
            state.portfolio.revoke_key(required_str(args, "id")?)?;
            Ok(json!({"revoked":true,"id":args["id"]}))
        }
        _ => Err(format!("unknown MCP tool: {name}")),
    }
}

fn tool_result_value(name: &str, result: Result<Value, String>) -> Value {
    tool_result_value_checked(name, result).0
}

fn tool_result_value_checked(name: &str, result: Result<Value, String>) -> (Value, bool) {
    let (value, is_error) = match result {
        Ok(value) => (value, false),
        Err(error) => (json!({"error":error}), true),
    };
    // The existing admin API deliberately reveals a newly created key exactly
    // once. Preserve that one credential-provisioning field while redacting all
    // other secret-like fields from MCP output.
    let one_time_token = (name == "admin.keys.create")
        .then(|| value["token"].as_str().map(str::to_owned))
        .flatten();
    let mut safe = redact_mcp(&value);
    if let Some(token) = one_time_token {
        safe["token"] = json!(token);
    }
    let result_too_large = safe.to_string().len() > MAX_TOOL_RESULT_BYTES;
    if result_too_large {
        safe = json!({"error":"MCP tool result exceeds the 1 MiB response limit"});
    }
    let text = safe.to_string();
    let mut result = json!({
        "content":[{"type":"text","text":text}],
        "structuredContent":safe
    });
    if is_error || result_too_large {
        result["isError"] = json!(true);
    }
    (result, !is_error && !result_too_large)
}

async fn mcp_tool_approval_result(
    state: &Arc<crate::state::AppState>,
    args: &Value,
    publication: &McpPublicationSlot,
) -> Result<Value, String> {
    let id = required_str(args, "id")?;
    let approval = state
        .portfolio
        .async_ops
        .tool_approval_status(&state.portfolio, id)
        .await?;
    let operation_id = approval["flow"]["operation_id"].as_str();

    let (status, request_id, run_id, result, error, pending_approvals) =
        if let Some(operation_id) = operation_id {
            // Approval result access is scoped to the exact invoker through the
            // approval handle, then to the same owned async operation. Do not
            // require roster_read for the invoker's own approval result.
            let mut receiver = state
                .portfolio
                .async_ops
                .subscribe_internal(&state.portfolio, operation_id)
                .await?;
            let snapshot = receiver.borrow_and_update().clone();
            let result = snapshot
                .result
                .as_ref()
                .and_then(|result| result.get("completion"))
                .cloned();
            (
                snapshot.status,
                snapshot.root_request_id,
                approval["flow"]["run_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                result,
                snapshot.error,
                snapshot.pending_approvals,
            )
        } else {
            let (status, result, error) = state
                .portfolio
                .async_ops
                .tool_approval_result(&state.portfolio, id)
                .await?;
            (
                status,
                approval["flow"]["request_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                approval["flow"]["run_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                result,
                error,
                Vec::new(),
            )
        };

    let has_assistant_response = result
        .as_ref()
        .is_some_and(|result| approval_result_assistant(result).is_some());
    if status == "completed" {
        let mut gate = crate::improvement::ResponseGate::new(
            state.portfolio.clone(),
            request_id.clone(),
            state.config.improvement.clone(),
        );
        gate.retain_on_drop();
        if has_assistant_response {
            if let Some(result) = &result {
                *publication.lock().unwrap() = Some(McpPublication {
                    request_id: request_id.clone(),
                    run_id: run_id.clone(),
                    message: McpPublicationMessage::ApprovalResult(result.clone()),
                    action: if approval_result_waiting_for_client_tools(result) {
                        McpPublicationAction::Handoff
                    } else {
                        McpPublicationAction::Complete
                    },
                    retryable: true,
                });
            }
        }
        gate.defer();
    }

    Ok(json!({
        "status":status,
        "request_id":request_id,
        "run_id":run_id,
        "operation_id":operation_id,
        "pending_approvals":pending_approvals,
        "result":result,
        "error":error
    }))
}

fn approval_result_waiting_for_client_tools(result: &Value) -> bool {
    result["waiting_for_tools"] == true
        || result["assistant"]["tool_calls"]
            .as_array()
            .is_some_and(|calls| !calls.is_empty())
        || result
            .pointer("/choices/0/finish_reason")
            .and_then(Value::as_str)
            == Some("tool_calls")
        || result
            .pointer("/choices/0/message/tool_calls")
            .and_then(Value::as_array)
            .is_some_and(|calls| !calls.is_empty())
}

fn persist_mcp_approval_message(
    state: &crate::state::AppState,
    run: &str,
    result: &Value,
) -> Result<(), String> {
    let Some(conversation) = state.portfolio.trace_conversation(run)? else {
        return Ok(());
    };
    let Some(assistant) = approval_result_assistant(result) else {
        return Ok(());
    };
    let existing_messages = {
        let conn = state.portfolio.conn.lock().unwrap();
        let mut statement = conn
            .prepare(
                "SELECT message FROM conversation_messages WHERE conversation_id=?1 AND run_id=?2
                 AND (?3 IS NULL OR workforce_id=?3) AND (?4 IS NULL OR owner_key_id=?4)",
            )
            .map_err(|error| error.to_string())?;
        statement
            .query_map(
                rusqlite::params![
                    conversation,
                    run,
                    state.portfolio.workforce_scope(),
                    state.portfolio.key_scope()
                ],
                |row| row.get::<_, String>(0),
            )
            .map_err(|error| error.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?
    };
    let already_saved = existing_messages.iter().any(|message| {
        serde_json::from_str::<Value>(message)
            .ok()
            .is_some_and(|message| message["role"] == "assistant")
    });
    if !already_saved {
        save_conversation_message(state, &conversation, run, assistant)?;
        state.portfolio.trace_emit(
            run,
            None,
            "conversation.message",
            &json!({"conversation_id":conversation,"message":assistant}),
        )?;
    }
    Ok(())
}

fn approval_result_assistant(result: &Value) -> Option<&Value> {
    result
        .pointer("/choices/0/message")
        .or_else(|| result.get("assistant"))
        .filter(|message| message["role"] == "assistant")
}

fn redact_mcp(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| {
                    let normalized = key
                        .chars()
                        .filter(|character| character.is_ascii_alphanumeric())
                        .flat_map(char::to_lowercase)
                        .collect::<String>();
                    let secret = matches!(
                        normalized.as_str(),
                        "authorization"
                            | "apikey"
                            | "token"
                            | "accesstoken"
                            | "refreshtoken"
                            | "password"
                            | "secret"
                            | "clientsecret"
                            | "credential"
                            | "credentials"
                            | "privatekey"
                    );
                    (
                        key.clone(),
                        if secret {
                            json!("[redacted]")
                        } else {
                            redact_mcp(value)
                        },
                    )
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.iter().map(redact_mcp).collect()),
        _ => value.clone(),
    }
}

fn validate_tool_arguments(value: &Value, schema: &Value, path: &str) -> Result<(), String> {
    if schema.is_null() || schema.as_object().is_some_and(|object| object.is_empty()) {
        return Ok(());
    }
    if let Some(allowed) = schema["enum"].as_array()
        && !allowed.contains(value)
    {
        return Err(format!("{path} is not an allowed value"));
    }
    match schema["type"].as_str() {
        Some("object") => {
            let object = value
                .as_object()
                .ok_or_else(|| format!("{path} must be an object"))?;
            for field in schema["required"].as_array().into_iter().flatten() {
                let field = field.as_str().ok_or("invalid tool schema")?;
                if !object.contains_key(field) {
                    return Err(format!("{path}.{field} is required"));
                }
            }
            let properties = schema["properties"].as_object();
            for (field, field_value) in object {
                let field_schema = properties.and_then(|properties| properties.get(field));
                if let Some(field_schema) = field_schema {
                    validate_tool_arguments(field_value, field_schema, &format!("{path}.{field}"))?;
                } else if schema["additionalProperties"] != true {
                    return Err(format!("{path}.{field} is not permitted"));
                }
            }
        }
        Some("array") => {
            let values = value
                .as_array()
                .ok_or_else(|| format!("{path} must be an array"))?;
            if let Some(items) = schema.get("items") {
                for (index, item) in values.iter().enumerate() {
                    validate_tool_arguments(item, items, &format!("{path}[{index}]"))?;
                }
            }
        }
        Some("string") => {
            let text = value
                .as_str()
                .ok_or_else(|| format!("{path} must be a string"))?;
            if schema["minLength"]
                .as_u64()
                .is_some_and(|minimum| text.trim().chars().count() < minimum as usize)
            {
                return Err(format!("{path} must not be empty"));
            }
            if schema["maxLength"]
                .as_u64()
                .is_some_and(|maximum| text.chars().count() > maximum as usize)
            {
                return Err(format!("{path} exceeds the allowed length"));
            }
        }
        Some("boolean") if !value.is_boolean() => {
            return Err(format!("{path} must be a boolean"));
        }
        Some("integer") => {
            let integer = value
                .as_i64()
                .ok_or_else(|| format!("{path} must be an integer"))?;
            if schema["minimum"]
                .as_i64()
                .is_some_and(|minimum| integer < minimum)
            {
                return Err(format!("{path} is below the allowed minimum"));
            }
            if schema["maximum"]
                .as_i64()
                .is_some_and(|maximum| integer > maximum)
            {
                return Err(format!("{path} exceeds the allowed maximum"));
            }
        }
        _ => {}
    }
    Ok(())
}

fn required_str<'a>(args: &'a Value, key: &str) -> Result<&'a str, String> {
    args.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing or empty {key}"))
}

fn optional_str<'a>(args: &'a Value, key: &str) -> Option<&'a str> {
    args.get(key).and_then(Value::as_str)
}

fn paging(args: &Value) -> Result<(u32, u32), String> {
    let offset = args["offset"].as_u64().unwrap_or(0);
    if offset > MAX_PAGE_OFFSET as u64 {
        return Err(format!("offset must not exceed {MAX_PAGE_OFFSET}"));
    }
    let limit = args["limit"].as_u64().unwrap_or(DEFAULT_PAGE_SIZE as u64);
    if !(1..=MAX_PAGE_SIZE as u64).contains(&limit) {
        return Err(format!("limit must be between 1 and {MAX_PAGE_SIZE}"));
    }
    Ok((offset as u32, limit as u32))
}

fn page(items: Vec<Value>, offset: u32, limit: u32) -> Value {
    let total = items.len();
    let selected: Vec<Value> = items
        .into_iter()
        .skip(offset as usize)
        .take(limit as usize)
        .collect();
    let next_offset =
        (offset as usize + selected.len() < total).then_some(offset + selected.len() as u32);
    json!({"items":selected,"total":total,"offset":offset,"next_offset":next_offset})
}

fn workforce_list(state: &Arc<crate::state::AppState>, args: &Value) -> Result<Value, String> {
    let (offset, limit) = paging(args)?;
    let include_retired = args["include_retired"].as_bool().unwrap_or(false);
    let kind = optional_str(args, "kind");
    let query = optional_str(args, "query").map(str::to_lowercase);
    let roster = state.portfolio.workforce(crate::observe::now_ms())?;
    let entities = roster["entities"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|entity| include_retired || entity["retired"] != true)
        .filter(|entity| kind.is_none_or(|kind| entity["kind"] == kind))
        .filter(|entity| {
            query.as_ref().is_none_or(|query| {
                ["id", "name", "purpose"].iter().any(|field| {
                    entity[*field]
                        .as_str()
                        .is_some_and(|value| value.to_lowercase().contains(query))
                })
            })
        })
        .map(|entity| {
            json!({
                "id":entity["id"],
                "kind":entity["kind"],
                "name":entity["name"],
                "purpose":entity["definition"]["purpose"],
                "retired":entity["retired"],
                "retired_ms":entity["retired_ms"],
                "created_ms":entity["created_ms"],
                "consolidated_into":entity["consolidated_into"],
                "usage":entity["usage"],
                "last_assignment_ms":entity["last_assignment_ms"],
                "last_model_call_ms":entity["last_model_call_ms"]
            })
        })
        .collect::<Vec<_>>();
    let mut result = page(entities, offset, limit);
    result["as_of_ms"] = roster["as_of_ms"].clone();
    result["tracking_since_ms"] = roster["tracking_since_ms"].clone();
    Ok(result)
}

fn workforce_get(state: &Arc<crate::state::AppState>, args: &Value) -> Result<Value, String> {
    let id = required_str(args, "id")?;
    let kind = optional_str(args, "kind");
    let include_retired = args["include_retired"].as_bool().unwrap_or(false);
    let roster = state.portfolio.workforce(crate::observe::now_ms())?;
    let entity = roster["entities"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|entity| {
            entity["id"] == id
                && kind.is_none_or(|kind| entity["kind"] == kind)
                && (include_retired || entity["retired"] != true)
        })
        .ok_or("unknown active workforce definition")?;
    Ok(json!({
        "id":entity["id"],
        "kind":entity["kind"],
        "name":entity["name"],
        "retired":entity["retired"],
        "retired_ms":entity["retired_ms"],
        "definition":entity["definition"],
        "usage":entity["usage"],
        "consolidated_into":entity["consolidated_into"]
    }))
}

fn begin_request(
    state: &crate::state::AppState,
    message: &str,
    conversation: Option<&str>,
) -> Result<(String, String), String> {
    let request = uuid::Uuid::new_v4().to_string();
    let mut payload = json!({"messages":[{"role":"user","content":message}]});
    if let Some(conversation) = conversation {
        payload["metadata"]["ccm_conversation_id"] = json!(conversation);
    }
    let run = state.portfolio.trace_begin(&request, &payload, "mcp")?;
    let client = state
        .auth
        .as_ref()
        .map(|auth| format!("key:{}", auth.key_id))
        .unwrap_or_else(|| "mcp".to_string());
    state
        .portfolio
        .bind_policy(&request, &run, &client, state.config.creation.policy)?;
    Ok((request, run))
}

fn foreman_conversations(state: &crate::state::AppState, args: &Value) -> Result<Value, String> {
    let (offset, limit) = paging(args)?;
    let conn = state.portfolio.conn.lock().unwrap();
    let mut statement = conn
        .prepare(
            "SELECT id,title,created_ms FROM conversations
             WHERE (?1 IS NULL OR workforce_id=?1) AND (?2 IS NULL OR owner_key_id=?2)
             ORDER BY created_ms DESC LIMIT ?3 OFFSET ?4",
        )
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map(
            rusqlite::params![
                state.portfolio.workforce_scope(),
                state.portfolio.key_scope(),
                (limit + 1) as i64,
                offset as i64
            ],
            |row| {
                Ok(json!({
                    "id":row.get::<_,String>(0)?,
                    "title":row.get::<_,String>(1)?,
                    "created_ms":row.get::<_,i64>(2)?
                }))
            },
        )
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let has_more = rows.len() > limit as usize;
    let conversations = rows.into_iter().take(limit as usize).collect::<Vec<_>>();
    Ok(json!({
        "conversations":conversations,
        "offset":offset,
        "limit":limit,
        "next_offset":has_more.then_some(offset + limit)
    }))
}

fn foreman_conversation(state: &crate::state::AppState, args: &Value) -> Result<Value, String> {
    let id = required_str(args, "id")?;
    let (offset, limit) = paging(args)?;
    let conn = state.portfolio.conn.lock().unwrap();
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM conversations WHERE id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3))",
            rusqlite::params![id,state.portfolio.workforce_scope(),state.portfolio.key_scope()],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    if !exists {
        return Err("unknown conversation for this bearer key".into());
    }
    let total: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM conversation_messages WHERE conversation_id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)",
            rusqlite::params![id,state.portfolio.workforce_scope(),state.portfolio.key_scope()],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    let mut statement = conn
        .prepare(
            "SELECT message FROM conversation_messages WHERE conversation_id=?1
             AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)
             ORDER BY id LIMIT ?4 OFFSET ?5",
        )
        .map_err(|error| error.to_string())?;
    let mut messages = statement
        .query_map(
            rusqlite::params![
                id,
                state.portfolio.workforce_scope(),
                state.portfolio.key_scope(),
                limit as i64,
                offset as i64
            ],
            |row| row.get::<_, String>(0),
        )
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|message| serde_json::from_str::<Value>(&message).map_err(|error| error.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    for message in &mut messages {
        if let Some(object) = message.as_object_mut() {
            object.remove("ccm_narration");
        }
    }
    let next_offset =
        (offset as i64 + (messages.len() as i64) < total).then_some(offset + messages.len() as u32);
    Ok(json!({
        "id":id,
        "messages":messages,
        "total":total,
        "offset":offset,
        "next_offset":next_offset
    }))
}

fn conversation_history(
    state: &crate::state::AppState,
    requested_id: Option<&str>,
) -> Result<(String, Vec<Value>), String> {
    let id = requested_id
        .map(str::to_owned)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    if id.trim().is_empty() || id.len() > 200 {
        return Err("conversation_id must contain 1 to 200 bytes".into());
    }
    let conn = state.portfolio.conn.lock().unwrap();
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM conversations WHERE id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3))",
            rusqlite::params![id,state.portfolio.workforce_scope(),state.portfolio.key_scope()],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    if requested_id.is_some() && !exists {
        return Err("unknown conversation for this bearer key".into());
    }
    let mut statement = conn
        .prepare("SELECT message FROM conversation_messages WHERE conversation_id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3) ORDER BY id")
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map(
            rusqlite::params![
                id,
                state.portfolio.workforce_scope(),
                state.portfolio.key_scope()
            ],
            |row| row.get::<_, String>(0),
        )
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let mut history = rows
        .iter()
        .map(|row| serde_json::from_str::<Value>(row).map_err(|error| error.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    for message in &mut history {
        if let Some(message) = message.as_object_mut() {
            message.remove("ccm_narration");
        }
    }
    Ok((id, history))
}

fn conversation_has_active_run(
    state: &crate::state::AppState,
    conversation: &str,
) -> Result<bool, String> {
    state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM trace_runs WHERE conversation_id=?1 AND status IN ('running','paused','waiting_tools') AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3))",
            rusqlite::params![conversation,state.portfolio.workforce_scope(),state.portfolio.key_scope()],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())
}

fn save_conversation_message(
    state: &crate::state::AppState,
    conversation: &str,
    run: &str,
    message: &Value,
) -> Result<(), String> {
    state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO conversation_messages(conversation_id,run_id,message,created_ms,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4,?5,?6)",
            rusqlite::params![conversation,run,message.to_string(),crate::observe::now_ms(),state.portfolio.workforce_scope(),state.portfolio.key_scope()],
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn conversation_result(
    conversation: &str,
    request: &str,
    run: &str,
    assistant: Value,
    usage: Option<Value>,
    defer_until_result: bool,
    gate: &mut crate::improvement::ResponseGate,
    publication: &McpPublicationSlot,
) -> Result<Value, String> {
    let waiting = assistant["tool_calls"]
        .as_array()
        .is_some_and(|calls| !calls.is_empty());
    if defer_until_result {
        gate.defer();
    } else {
        gate.defer();
        *publication.lock().unwrap() = Some(McpPublication {
            request_id: request.to_owned(),
            run_id: run.to_owned(),
            message: McpPublicationMessage::Conversation {
                conversation_id: conversation.to_owned(),
                assistant: assistant.clone(),
            },
            action: if waiting {
                McpPublicationAction::Handoff
            } else {
                McpPublicationAction::Complete
            },
            retryable: false,
        });
    }
    Ok(json!({
        "conversation_id":conversation,
        "request_id":request,
        "run_id":run,
        "waiting_for_tools":waiting,
        "assistant":assistant,
        "usage":usage
    }))
}

fn pending_approval_content(assistant: &Value) -> Option<Value> {
    let content = assistant.get("content")?;
    let value = match content {
        Value::String(text) => serde_json::from_str::<Value>(text).ok()?,
        Value::Object(_) => content.clone(),
        _ => return None,
    };
    (value["status"] == "waiting_approval" && value["approvals"].is_array()).then_some(value)
}

fn mcp_pending_approval_response(
    pending: &Value,
    conversation: &str,
    request: &str,
    run: &str,
) -> Value {
    let approvals = pending
        .get("approvals")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let approval_ids = approvals
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|approval| approval["id"].as_str())
        .collect::<Vec<_>>();
    json!({
        "status":"waiting_approval",
        "conversation_id":conversation,
        "request_id":request,
        "run_id":run,
        "approvals":approvals,
        "approval_ids":approval_ids,
        "message":pending.get("message").and_then(Value::as_str).unwrap_or("No tool side effect has run. Resolve this one-time approval to resume the original run."),
        "result_tool":"tool_approvals.result"
    })
}

async fn foreman_chat(
    state: Arc<crate::state::AppState>,
    args: &Value,
    publication: &McpPublicationSlot,
) -> Result<Value, String> {
    let conversation_arg = optional_str(args, "conversation_id");
    let (conversation, mut history) = conversation_history(&state, conversation_arg)?;
    let tool_results = args.get("tool_results").and_then(Value::as_array);
    let appended = if let Some(results) = tool_results {
        if results.is_empty() || args.get("message").is_some() {
            return Err("supply either a message or a nonempty tool_results batch".into());
        }
        if conversation_arg.is_none() {
            return Err("tool-result continuation requires conversation_id".into());
        }
        if results.len() > 64 {
            return Err("tool result batch exceeds 64 items".into());
        }
        results
            .iter()
            .map(|result| {
                Ok(json!({
                    "role":"tool",
                    "tool_call_id":required_str(result,"tool_call_id")?,
                    "content":result.get("content").and_then(Value::as_str).ok_or("tool result content must be a string")?
                }))
            })
            .collect::<Result<Vec<_>, String>>()?
    } else {
        let message = required_str(args, "message")?;
        if conversation_has_active_run(&state, &conversation)? {
            return Err("conversation has an active run; return its complete tool-result batch or wait for it to finish".into());
        }
        vec![json!({"role":"user","content":message})]
    };
    history.extend(appended.iter().cloned());
    if history.len() > 1000 {
        return Err("conversation exceeds the 1000-message MCP context limit".into());
    }
    let request = uuid::Uuid::new_v4().to_string();
    let mut payload = json!({
        "model":state.config.model,
        "stream":false,
        "messages":history,
        "metadata":{"ccm_source":"mcp","ccm_conversation_id":conversation}
    });
    for key in ["tools", "tool_choice", "response_format"] {
        if let Some(value) = args.get(key) {
            payload[key] = value.clone();
        }
    }
    let run = state.portfolio.trace_begin(&request, &payload, "web")?;
    for message in &appended {
        save_conversation_message(&state, &conversation, &run, message)?;
    }
    let mut gate = crate::improvement::ResponseGate::new(
        state.portfolio.clone(),
        request.clone(),
        state.config.improvement.clone(),
    );
    let response = match crate::handlers::native_chat(state.clone(), payload, request.clone()).await
    {
        Ok(response) => response,
        Err(error) => {
            let message = error.to_string();
            let _ =
                state
                    .portfolio
                    .trace_finish_request(&request, "error", &json!({"error":message}));
            return Err(message);
        }
    };
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 16 * 1024 * 1024)
        .await
        .map_err(|error| error.to_string())?;
    let body: Value = serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    if !status.is_success() {
        return Err(body
            .pointer("/error/message")
            .and_then(Value::as_str)
            .unwrap_or("CCM Foreman request failed")
            .to_string());
    }
    let assistant = body
        .pointer("/choices/0/message")
        .cloned()
        .ok_or("Foreman response is missing its assistant message")?;
    if let Some(pending) = pending_approval_content(&assistant) {
        gate.defer();
        return Ok(mcp_pending_approval_response(
            &pending,
            &conversation,
            &request,
            &run,
        ));
    }
    conversation_result(
        &conversation,
        &request,
        &run,
        assistant,
        body.get("usage").cloned(),
        false,
        &mut gate,
        publication,
    )
}

async fn foreman_start_async(
    state: Arc<crate::state::AppState>,
    auth: &crate::auth::AuthContext,
    args: &Value,
) -> Result<Value, String> {
    let message = required_str(args, "message")?;
    let mut payload = json!({
        "model":state.config.model,
        "stream":false,
        "messages":[{"role":"user","content":message}],
        "metadata":{"ccm_async":true}
    });
    for key in ["tools", "tool_choice", "response_format"] {
        if let Some(value) = args.get(key) {
            payload[key] = value.clone();
        }
    }
    let response = crate::handlers::chat_completions(
        State(state),
        Some(Extension(auth.clone())),
        HeaderMap::new(),
        Bytes::from(payload.to_string()),
    )
    .await
    .map_err(|error| error.to_string())?;
    let status = response.status();
    let request_id = response
        .headers()
        .get("x-ccm-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let run_id = response
        .headers()
        .get("x-ccm-run-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .map_err(|error| error.to_string())?;
    let body: Value = serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    if !status.is_success() {
        return Err(body
            .pointer("/error/message")
            .or_else(|| body.get("error"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| body.to_string()));
    }
    Ok(json!({
        "status":"accepted",
        "operation":body["operation"],
        "request_id":request_id,
        "run_id":run_id
    }))
}

async fn continue_async(
    state: Arc<crate::state::AppState>,
    auth: &crate::auth::AuthContext,
    args: &Value,
) -> Result<Value, String> {
    let id = required_str(args, "id")?;
    let messages = args["messages"]
        .as_array()
        .filter(|messages| !messages.is_empty())
        .ok_or("messages must be a nonempty role-labelled array")?;
    let mut payload = json!({
        "model":state.config.model,
        "stream":false,
        "messages":messages,
        "metadata":{"ccm_operation_id":id}
    });
    for key in ["tools", "tool_choice", "response_format"] {
        if let Some(value) = args.get(key) {
            payload[key] = value.clone();
        }
    }
    let response = crate::handlers::chat_completions(
        State(state),
        Some(Extension(auth.clone())),
        HeaderMap::new(),
        Bytes::from(payload.to_string()),
    )
    .await
    .map_err(|error| error.to_string())?;
    let status = response.status();
    let request_id = response
        .headers()
        .get("x-ccm-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .map_err(|error| error.to_string())?;
    let body: Value = serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    if !status.is_success() {
        return Err(body
            .pointer("/error/message")
            .and_then(Value::as_str)
            .unwrap_or("async continuation failed")
            .to_string());
    }
    Ok(json!({"http_status":status.as_u16(),"response":body,"request_id":request_id}))
}

pub(crate) async fn run_selected_http(
    state: Arc<crate::state::AppState>,
    kind: &str,
    arguments: &Value,
    mut admission: Option<crate::health::AdmissionPermit>,
) -> Response {
    let slot = Arc::new(Mutex::new(None));
    let result = match run_selected(&state, arguments, kind, slot.clone()).await {
        Ok(result) => result,
        Err(error) => {
            return (StatusCode::BAD_REQUEST, Json(json!({"error":error}))).into_response();
        }
    };
    let status = if result["status"] == "waiting_approval" {
        "waiting_approval"
    } else if result["waiting_for_tools"] == true {
        "waiting_tools"
    } else {
        "completed"
    };
    let id = arguments["id"].as_str().unwrap_or_default();
    let normalized = json!({
        "status":status,
        "resource":{"kind":kind,"id":id},
        "request_id":result["request_id"],
        "run_id":result["run_id"],
        "operation_id":Value::Null,
        "result":result,
        "receipt":{"action":"run","resource_id":id,"run_id":result["run_id"],"completed":status=="completed"}
    });
    if let Some(publication) = slot.lock().unwrap().take() {
        mcp_publication_response(
            StatusCode::OK,
            normalized,
            None,
            None,
            state,
            publication,
            true,
            admission.take(),
        )
    } else {
        Json(normalized).into_response()
    }
}

async fn run_selected(
    state: &Arc<crate::state::AppState>,
    args: &Value,
    kind: &str,
    publication: McpPublicationSlot,
) -> Result<Value, String> {
    let id = required_str(args, "id")?;
    let message = required_str(args, "message")?;
    if kind == "agent" {
        state
            .portfolio
            .get(id)
            .map_err(|error| error.to_string())?
            .ok_or("unknown active agent")?;
    } else {
        state
            .portfolio
            .workflow_get(id)?
            .ok_or("unknown active workflow")?;
    }
    let (conversation, mut history) =
        conversation_history(state, optional_str(args, "conversation_id"))?;
    if conversation_has_active_run(state, &conversation)? {
        return Err(
            "conversation has an active run; wait for its client-tool handoff to be resolved"
                .into(),
        );
    }
    let user = json!({"role":"user","content":message});
    history.push(user.clone());
    let mut payload = json!({
        "model":state.config.model,
        "stream":false,
        "messages":history,
        "metadata":{"ccm_source":"mcp","ccm_conversation_id":conversation}
    });
    for key in ["tools", "tool_choice", "response_format"] {
        if let Some(value) = args.get(key) {
            payload[key] = value.clone();
        }
    }
    let request = uuid::Uuid::new_v4().to_string();
    let run = state.portfolio.trace_begin(&request, &payload, "web")?;
    let client = state
        .auth
        .as_ref()
        .map(|auth| format!("key:{}", auth.key_id))
        .unwrap_or_else(|| "mcp".to_string());
    let client = state.config.client_scope(&client);
    let state = crate::handlers::prepare_run_context(state, &run, &request, &client)
        .map_err(|error| error.to_string())?;
    state
        .portfolio
        .trace_event(&request, "mcp.selected_run", &json!({"kind":kind,"id":id}))?;
    save_conversation_message(&state, &conversation, &run, &user)?;
    let generation = state.portfolio.generation();

    let worker_state = state.clone();
    let worker_request = request.clone();
    let worker_run = run.clone();
    let worker_id = id.to_owned();
    let worker_kind = kind.to_owned();
    let worker_conversation = conversation.clone();
    let worker_publication = publication.clone();
    let mut worker = tokio::spawn(async move {
        finish_selected_run(
            worker_state,
            worker_id,
            worker_kind,
            worker_conversation,
            payload,
            worker_request,
            worker_run,
            generation,
            worker_publication,
        )
        .await
    });
    tokio::select! {
        joined = &mut worker => match joined {
            Ok(result) => result,
            Err(error) => Err(format!("selected definition worker failed: {error}")),
        },
        approvals = state.portfolio.async_ops.wait_for_request_approval(&state.portfolio, &request) => {
            match approvals {
                Err(error) => Err(error),
                Ok(approvals) if approvals.is_empty() => match worker.await {
                    Ok(result) => result,
                    Err(error) => Err(format!("selected definition worker failed: {error}")),
                },
                Ok(approvals) => {
                    let pending = json!({
                        "approvals":approvals,
                        "message":"No tool side effect has run. Resolve the one-time approval to resume this exact definition run."
                    });
                    let mut response = mcp_pending_approval_response(
                        &pending,
                        &conversation,
                        &request,
                        &run,
                    );
                    response["kind"] = json!(kind);
                    response["definition_id"] = json!(id);
                    Ok(response)
                }
            }
        },
        _ = state.portfolio.async_ops.wait_until_sealed() => {
            worker.abort();
            let _ = worker.await;
            Err("service shutdown cutoff".into())
        }
    }
}

async fn finish_selected_run(
    state: Arc<crate::state::AppState>,
    id: String,
    kind: String,
    conversation: String,
    payload: Value,
    request: String,
    run: String,
    generation: u64,
    publication: McpPublicationSlot,
) -> Result<Value, String> {
    let mut gate = crate::improvement::ResponseGate::new(
        state.portfolio.clone(),
        request.clone(),
        state.config.improvement.clone(),
    );
    let result = async {
        let output = match crate::management::execute_selected_definition(
            state.clone(),
            &kind,
            &id,
            payload,
            &request,
        )
        .await
        {
            Ok(output) => output,
            Err(error) => {
                let _ = state.portfolio.trace_finish_request(
                    &request,
                    "error",
                    &json!({"error":error}),
                );
                return Err(error);
            }
        };
        let assistant = output.message;
        state.portfolio.trace_finish_request(
            &request,
            if output.finish == "tool_calls" {
                "waiting_tools"
            } else {
                "completed"
            },
            &assistant,
        )?;
        let defer_until_result = state
            .portfolio
            .async_ops
            .has_tool_approval_flow(&request)
            .await;
        conversation_result(
            &conversation,
            &request,
            &run,
            assistant,
            output.usage,
            defer_until_result,
            &mut gate,
            &publication,
        )
    }
    .await;
    let result = result.map(|mut value| {
        value["kind"] = json!(kind);
        value["definition_id"] = json!(id);
        value["generation"] = json!(generation);
        value
    });

    if state
        .portfolio
        .async_ops
        .has_tool_approval_flow(&request)
        .await
    {
        match &result {
            Ok(value) => {
                state
                    .portfolio
                    .async_ops
                    .finish_tool_approval_flow(
                        &state.portfolio,
                        &request,
                        Some(value.clone()),
                        None,
                    )
                    .await;
            }
            Err(error) => {
                state
                    .portfolio
                    .async_ops
                    .finish_tool_approval_flow(
                        &state.portfolio,
                        &request,
                        None,
                        Some(error.clone()),
                    )
                    .await;
            }
        }
    }
    result
}

fn create_workforce(state: &crate::state::AppState, args: &Value) -> Result<Value, String> {
    let name = required_str(args, "name")?;
    if name.len() > 120 {
        return Err("name must contain 1 to 120 characters".into());
    }
    let visibility = optional_str(args, "visibility").unwrap_or("private");
    let id = state.portfolio.create_workforce(name, visibility)?;
    Ok(json!({"id":id,"name":name,"visibility":visibility}))
}

fn create_key(state: &crate::state::AppState, args: &Value) -> Result<Value, String> {
    let label = required_str(args, "label")?;
    if label.len() > 120 {
        return Err("label must contain 1 to 120 characters".into());
    }
    let workforce_name = optional_str(args, "workforce_name").unwrap_or(label);
    if workforce_name.trim().is_empty() || workforce_name.len() > 120 {
        return Err("workforce_name must contain 1 to 120 characters".into());
    }
    let permissions = args
        .get("permissions")
        .cloned()
        .map(serde_json::from_value::<crate::auth::KeyPermissions>)
        .transpose()
        .map_err(|error| error.to_string())?
        .unwrap_or_default();
    let (context, token) = state.portfolio.create_key(
        label,
        optional_str(args, "workforce_id"),
        workforce_name,
        &permissions,
    )?;
    Ok(json!({
        "id":context.key_id,
        "workforce_id":context.workforce_id,
        "label":label,
        "permissions":context.permissions,
        "token":token,
        "shown_once":true
    }))
}

fn observability_runs(state: &crate::state::AppState, args: &Value) -> Result<Value, String> {
    let (offset, limit) = paging(args)?;
    let conn = state.portfolio.conn.lock().unwrap();
    let mut statement = conn
        .prepare(
            "SELECT id,conversation_id,source,title,status,created_ms,updated_ms
             FROM trace_runs WHERE (?1 IS NULL OR workforce_id=?1) AND (?2 IS NULL OR owner_key_id=?2)
             AND (?3 IS NULL OR status=?3) AND (?4 IS NULL OR source=?4)
             ORDER BY created_ms DESC,rowid DESC LIMIT ?5 OFFSET ?6",
        )
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map(
            rusqlite::params![
                state.portfolio.workforce_scope(),
                state.portfolio.key_scope(),
                optional_str(args, "status"),
                optional_str(args, "source"),
                (limit + 1) as i64,
                offset as i64
            ],
            |row| {
                Ok(json!({
                    "id":row.get::<_,String>(0)?,
                    "conversation_id":row.get::<_,Option<String>>(1)?,
                    "source":row.get::<_,String>(2)?,
                    "title":row.get::<_,String>(3)?,
                    "status":row.get::<_,String>(4)?,
                    "created_ms":row.get::<_,i64>(5)?,
                    "updated_ms":row.get::<_,i64>(6)?
                }))
            },
        )
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let has_more = rows.len() > limit as usize;
    let rows = rows.into_iter().take(limit as usize).collect::<Vec<_>>();
    drop(statement);
    drop(conn);
    let next = has_more.then_some(offset + limit);
    Ok(json!({
        "runs":rows,
        "offset":offset,
        "limit":limit,
        "next_offset":next,
        "cursor":state.portfolio.trace_cursor()?
    }))
}

fn observability_run(state: &crate::state::AppState, args: &Value) -> Result<Value, String> {
    let id = required_str(args, "id")?;
    let (offset, limit) = paging(args)?;
    let detail = state.portfolio.trace_detail(id)?;
    let span_kind = optional_str(args, "span_kind");
    let event_kind = optional_str(args, "event_kind");
    let spans = detail["spans"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|span| span_kind.is_none_or(|kind| span["kind"] == kind))
        .map(|span| {
            json!({
                "id":span["id"],
                "parent_id":span["parent_id"],
                "kind":span["kind"],
                "entity_id":span["entity_id"],
                "status":span["status"],
                "started_ms":span["started_ms"],
                "ended_ms":span["ended_ms"],
                "version_id":span["version_id"],
                "usage":span["usage"]
            })
        })
        .collect::<Vec<_>>();
    let events = detail["events"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|event| event_kind.is_none_or(|kind| event["kind"] == kind))
        .cloned()
        .collect::<Vec<_>>();
    let controls = detail["controls"].as_array().cloned().unwrap_or_default();
    let changes = detail["changes"].as_array().cloned().unwrap_or_default();
    let improvements = detail["improvements"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    Ok(json!({
        "run_id":detail["run_id"],
        "creation_policy":detail["creation_policy"],
        "cursor":detail["cursor"],
        "spans":page(spans,offset,limit),
        "events":page(events,offset,limit),
        "controls":page(controls,offset,limit),
        "changes":page(changes,offset,limit),
        "improvements":page(improvements,offset,limit)
    }))
}

fn observability_events(state: &crate::state::AppState, args: &Value) -> Result<Value, String> {
    let after = args["after"].as_i64().unwrap_or(0).max(0);
    let (_, limit) = paging(args)?;
    let conn = state.portfolio.conn.lock().unwrap();
    let mut statement = conn
        .prepare(
            "SELECT seq,run_id,span_id,kind,at_ms,data FROM trace_events
             WHERE seq>?1 AND (?2 IS NULL OR run_id=?2) AND (?3 IS NULL OR kind=?3)
             AND (?4 IS NULL OR workforce_id=?4) AND (owner_key_id IS NULL OR owner_key_id=?5)
             ORDER BY seq LIMIT ?6",
        )
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map(
            rusqlite::params![
                after,
                optional_str(args, "run_id"),
                optional_str(args, "kind"),
                state.portfolio.workforce_scope(),
                state.portfolio.key_scope(),
                (limit + 1) as i64
            ],
            |row| {
                Ok(json!({
                    "seq":row.get::<_,i64>(0)?,
                    "run_id":row.get::<_,String>(1)?,
                    "span_id":row.get::<_,Option<String>>(2)?,
                    "kind":row.get::<_,String>(3)?,
                    "at_ms":row.get::<_,i64>(4)?,
                    "data":serde_json::from_str::<Value>(&row.get::<_,String>(5)?).unwrap_or(Value::Null)
                }))
            },
        )
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let has_more = rows.len() > limit as usize;
    let events = rows.into_iter().take(limit as usize).collect::<Vec<_>>();
    let next_after = has_more
        .then(|| events.last().and_then(|event| event["seq"].as_i64()))
        .flatten();
    Ok(json!({"events":events,"after":after,"next_after":next_after,"has_more":has_more}))
}

fn observability_statistics(state: &crate::state::AppState, args: &Value) -> Result<Value, String> {
    let (offset, limit) = paging(args)?;
    let stats = state.portfolio.statistics()?;
    let groups = stats["groups"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|group| optional_str(args, "kind").is_none_or(|kind| group["kind"] == kind))
        .filter(|group| optional_str(args, "entity_id").is_none_or(|id| group["entity_id"] == id))
        .filter(|group| optional_str(args, "status").is_none_or(|status| group["status"] == status))
        .cloned()
        .collect::<Vec<_>>();
    Ok(json!({
        "groups":page(groups,offset,limit),
        "token_usage":stats["token_usage"]
    }))
}

fn improvement_jobs(state: &crate::state::AppState, args: &Value) -> Result<Value, String> {
    let run_id = required_str(args, "run_id")?;
    let (offset, limit) = paging(args)?;
    let conn = state.portfolio.conn.lock().unwrap();
    let value = crate::improvement::list_scoped(
        &conn,
        run_id,
        state.portfolio.workforce_scope(),
        state.portfolio.key_scope(),
    )?;
    let items = value.as_array().cloned().unwrap_or_default();
    Ok(page(items, offset, limit))
}

fn improvement_suggestions(state: &crate::state::AppState, args: &Value) -> Result<Value, String> {
    let (offset, limit) = paging(args)?;
    let status = optional_str(args, "status");
    let all = state.portfolio.suggestions()?;
    let items = all
        .as_array()
        .into_iter()
        .flatten()
        .filter(|suggestion| status.is_none_or(|status| suggestion["status"] == status))
        .cloned()
        .collect::<Vec<_>>();
    Ok(page(items, offset, limit))
}

fn declassify_suggestion(
    state: &Arc<crate::state::AppState>,
    args: &Value,
) -> Result<Value, String> {
    let id = required_str(args, "id")?;
    let (request, _) = begin_request(
        state,
        "Release a generated improvement replacement to same-workforce roster writers",
        None,
    )?;
    let result = match crate::improvement::declassify_suggestion(state, id, &request) {
        Ok(result) => result,
        Err(error) => {
            let _ =
                state
                    .portfolio
                    .trace_finish_request(&request, "error", &json!({"error":error}));
            return Err(error);
        }
    };
    state
        .portfolio
        .trace_finish_request(&request, "completed", &result)?;
    Ok(json!({"result":result,"request_id":request}))
}

async fn apply_suggestion(
    state: &Arc<crate::state::AppState>,
    args: &Value,
) -> Result<Value, String> {
    let id = required_str(args, "id")?;
    let (request, _) = begin_request(state, "Apply an evaluated improvement suggestion", None)?;
    let result = match crate::improvement::apply_suggestion_async_with_authority(
        state,
        id,
        &request,
        &crate::improvement::ApplicationAuthority::Explicit,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            let _ =
                state
                    .portfolio
                    .trace_finish_request(&request, "error", &json!({"error":error}));
            return Err(error);
        }
    };
    state
        .portfolio
        .trace_finish_request(&request, "completed", &result)?;
    Ok(json!({"result":result,"request_id":request}))
}

async fn config_projection(state: &crate::state::AppState) -> Result<Value, String> {
    let settings = state.portfolio.async_ops.settings().await;
    let admin = state
        .auth
        .as_ref()
        .is_none_or(|auth| auth.permissions.bootstrap_admin);
    if !admin {
        let workforce_id = state
            .auth
            .as_ref()
            .map(|auth| auth.workforce_id.clone())
            .unwrap_or_default();
        let workforce_limits = settings["workforces"].get(&workforce_id).cloned();
        return Ok(json!({
            "scope":"key_workforce",
            "workforce_id":workforce_id,
            "model":state.config.model,
            "settings":crate::management::current_key_settings_value(state)?,
            "native_tool_sandbox":state.sandbox_runtime_status(),
            "async_supervisor":{
                "server":Value::Null,
                "server_scope":"bootstrap_admin_only",
                "workforce":workforce_limits
            }
        }));
    }
    Ok(json!({
        "scope":"server",
        "model":state.config.model,
        "provider":state.config.routing.provider,
        "inference_model":state.config.routing.inference_model,
        "max_iterations":state.config.max_iterations,
        "creation_policy":state.config.creation.policy,
        "improvement":state.config.improvement,
        "personality_transparency":state.config.personality_transparency,
        "memory_budget_mib":state.config.memory.budget_mib,
        "native_tool_sandbox":state.sandbox_runtime_status(),
        "upstream_timeouts":{
            "connect_timeout_ms":state.config.upstream.connect_timeout_ms,
            "first_response_timeout_ms":state.config.upstream.first_response_timeout_ms,
            "idle_timeout_ms":state.config.upstream.idle_timeout_ms,
            "total_timeout_ms":state.config.upstream.total_timeout_ms,
            "max_attempts":state.config.upstream.max_attempts,
            "retry_base_ms":state.config.upstream.retry_base_ms
        },
        "async_supervisor":{"server":settings["server"],"workforce":Value::Null}
    }))
}

async fn set_server_async_limits(
    state: &crate::state::AppState,
    args: &Value,
) -> Result<Value, String> {
    let limits = serde_json::from_value::<crate::async_ops::ServerLimits>(args.clone())
        .map_err(|_| "invalid server async limits".to_string())?;
    state.portfolio.async_ops.set_server_limits(limits).await
}

async fn set_workforce_async_limits(
    state: &crate::state::AppState,
    args: &Value,
) -> Result<Value, String> {
    let workforce_id = required_str(args, "workforce_id")?;
    let workforces = state.portfolio.list_workforces()?;
    let exists = workforces.as_array().is_some_and(|workforces| {
        workforces
            .iter()
            .any(|workforce| workforce["id"] == workforce_id)
    });
    if !exists {
        return Err("unknown workforce".into());
    }
    let limits =
        serde_json::from_value::<crate::async_ops::WorkforceLimits>(args["limits"].clone())
            .map_err(|_| "invalid workforce async limits".to_string())?;
    state
        .portfolio
        .async_ops
        .set_workforce_limits(workforce_id.to_string(), limits)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::{Client, Response};
    use serde_json::Value;
    use std::{collections::VecDeque, sync::Mutex, time::Duration};

    #[test]
    fn mcp_admission_keeps_new_work_mutations_and_existing_controls_distinct() {
        use McpAdmissionClass::{ExistingWork, Mutation, NewWork, ReadOnly};

        assert_eq!(mcp_admission_class("foreman.chat", &json!({})), NewWork);
        assert_eq!(
            mcp_admission_class(
                "foreman.chat",
                &json!({"tool_results":[{"tool_call_id":"call-1","content":"ok"}]})
            ),
            ExistingWork
        );
        for name in [
            "foreman.start_async",
            "workforce.create_agent",
            "workforce.create_workflow",
            "workforce.evaluate",
            "workforce.run_agent",
            "workforce.run_workflow",
            "improvements.apply",
        ] {
            assert_eq!(mcp_admission_class(name, &json!({})), NewWork, "{name}");
        }
        for name in [
            "operations.continue",
            "operations.cancel",
            "tool_approvals.result",
            "tool_approvals.approve",
            "tool_approvals.deny",
            "tool_approvals.cancel",
        ] {
            assert_eq!(
                mcp_admission_class(name, &json!({})),
                ExistingWork,
                "{name}"
            );
        }
        for name in [
            "improvements.declassify",
            "config.async_limits.set_server",
            "config.async_limits.set_workforce",
            "admin.workforces.create",
            "admin.keys.create",
            "admin.keys.revoke",
        ] {
            assert_eq!(mcp_admission_class(name, &json!({})), Mutation, "{name}");
        }
        for name in [
            "operations.status",
            "operations.result",
            "tool_approvals.status",
            "workforce.list",
            "config.get",
        ] {
            assert_eq!(mcp_admission_class(name, &json!({})), ReadOnly, "{name}");
        }
    }

    struct TestServer {
        url: String,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn test_state() -> Arc<crate::state::AppState> {
        test_state_at("http://127.0.0.1:1")
    }

    fn test_state_at(upstream_base_url: &str) -> Arc<crate::state::AppState> {
        let providers = std::collections::BTreeMap::new();
        Arc::new(crate::state::AppState {
            config: Arc::new(crate::config::Config {
                api_key: "not-for-mcp".into(),
                upstream_base_url: upstream_base_url.into(),
                model: "test-model".into(),
                host: "127.0.0.1".into(),
                port: 0,
                upstream: Default::default(),
                portfolio_db: ":memory:".into(),
                max_iterations: 2,
                debug_enabled: false,
                creation: Default::default(),
                improvement: Default::default(),
                personality_transparency: Default::default(),
                memory: Default::default(),
                shutdown: Default::default(),
                routing: Default::default(),
            }),
            http: reqwest::Client::new(),
            portfolio: Arc::new(crate::portfolio::Portfolio::open(":memory:").unwrap()),
            providers: Arc::new(crate::provider_gate::Registry::new(&providers)),
            external_api_enabled: true,
            auth: None,
        })
    }

    type MockScript = Arc<Mutex<(VecDeque<Value>, Vec<Value>)>>;

    async fn mock_upstream(
        axum::extract::State(script): axum::extract::State<MockScript>,
        axum::Json(body): axum::Json<Value>,
    ) -> axum::Json<Value> {
        let mut script = script.lock().unwrap();
        script.1.push(body);
        axum::Json(script.0.pop_front().expect("unexpected upstream request"))
    }

    async fn serve_mock_upstream(responses: Vec<Value>) -> (TestServer, MockScript) {
        let script = Arc::new(Mutex::new((responses.into(), Vec::new())));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = axum::Router::new()
            .route("/chat/completions", axum::routing::post(mock_upstream))
            .with_state(script.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (TestServer { url, task }, script)
    }

    fn assistant_response(content: &str) -> Value {
        json!({"choices":[{"message":{"role":"assistant","content":content},"finish_reason":"stop"}]})
    }

    fn file_write_call(path: &str, content: &str, call_id: &str) -> Value {
        json!({
            "choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{
                "id":call_id,"type":"function","function":{
                    "name":"ccm_file_write",
                    "arguments":json!({"path":path,"content":content}).to_string()
                }
            }]},"finish_reason":"tool_calls"}]
        })
    }

    fn client_tool_call(call_id: &str) -> Value {
        json!({
            "choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{
                "id":call_id,
                "type":"function",
                "function":{"name":"client_lookup","arguments":"{}"}
            }]},"finish_reason":"tool_calls"}]
        })
    }

    fn client_tools() -> Value {
        json!([{
            "type":"function",
            "function":{
                "name":"client_lookup",
                "description":"Look up a record in the caller's system",
                "parameters":{"type":"object","properties":{},"additionalProperties":false}
            }
        }])
    }

    fn permissions(
        roster_use: bool,
        roster_read: bool,
        roster_write: bool,
        improvement_contribute: bool,
        bootstrap_admin: bool,
    ) -> crate::auth::KeyPermissions {
        crate::auth::KeyPermissions {
            roster_use,
            roster_read,
            roster_write,
            improvement_contribute,
            bootstrap_admin,
        }
    }

    fn issue(
        state: &crate::state::AppState,
        permissions: crate::auth::KeyPermissions,
    ) -> (crate::auth::AuthContext, String) {
        state
            .portfolio
            .create_key("MCP test", None, "MCP test workforce", &permissions)
            .unwrap()
    }

    async fn serve(state: Arc<crate::state::AppState>) -> TestServer {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, crate::api::routes(state))
                .await
                .unwrap();
        });
        TestServer { url, task }
    }

    async fn serve_with_lifecycle(
        state: Arc<crate::state::AppState>,
        lifecycle: Arc<crate::health::Lifecycle>,
    ) -> TestServer {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = crate::api::routes(state).layer(Extension(lifecycle));
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        TestServer { url, task }
    }

    async fn initialize(client: &Client, server: &TestServer, token: &str) -> (String, Value) {
        let response = client
            .post(format!("{}/mcp", server.url))
            .bearer_auth(token)
            .json(&json!({
                "jsonrpc":"2.0",
                "id":"init",
                "method":"initialize",
                "params":{
                    "protocolVersion":"2025-11-25",
                    "capabilities":{},
                    "clientInfo":{"name":"ccm-mcp-test","version":"1"}
                }
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[protocol_header()], "2025-11-25");
        let session = response.headers()[session_header()]
            .to_str()
            .unwrap()
            .to_owned();
        (session, response.json().await.unwrap())
    }

    async fn rpc(
        client: &Client,
        server: &TestServer,
        token: &str,
        session: &str,
        method: &str,
        id: Value,
        params: Value,
    ) -> Response {
        client
            .post(format!("{}/mcp", server.url))
            .bearer_auth(token)
            .header(session_header().clone(), session)
            .header(protocol_header().clone(), "2025-11-25")
            .json(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
            .send()
            .await
            .unwrap()
    }

    async fn call_tool(
        client: &Client,
        server: &TestServer,
        token: &str,
        session: &str,
        name: &str,
        arguments: Value,
    ) -> Value {
        rpc(
            client,
            server,
            token,
            session,
            "tools/call",
            json!("call"),
            json!({"name":name,"arguments":arguments}),
        )
        .await
        .json::<Value>()
        .await
        .unwrap()["result"]["structuredContent"]
            .clone()
    }

    #[tokio::test]
    async fn mcp_drain_blocks_new_work_and_mutations_but_keeps_reads_and_controls_open() {
        let state = test_state();
        let (_, token) = issue(&state, permissions(true, true, true, true, true));
        let lifecycle = Arc::new(crate::health::Lifecycle::new());
        lifecycle.mark_ready();
        let server = serve_with_lifecycle(state.clone(), lifecycle.clone()).await;
        let client = Client::new();
        let (session, _) = initialize(&client, &server, &token).await;

        lifecycle.begin_drain();

        let read = call_tool(&client, &server, &token, &session, "config.get", json!({})).await;
        assert_eq!(read["model"], "test-model");

        let continuation_control = rpc(
            &client,
            &server,
            &token,
            &session,
            "tools/call",
            json!("drain-cancel-control"),
            json!({"name":"operations.cancel","arguments":{"id":"missing-operation"}}),
        )
        .await;
        assert_eq!(continuation_control.status(), StatusCode::OK);

        for (id, name, arguments) in [
            (
                "drain-new-work",
                "workforce.create_agent",
                json!({"requirements":"must not generate while draining"}),
            ),
            (
                "drain-settings-mutation",
                "config.async_limits.set_server",
                json!({"max_root_operations":1,"max_child_workers":1,"max_queued_roots":0,"max_queue_wait_seconds":60}),
            ),
        ] {
            let response = rpc(
                &client,
                &server,
                &token,
                &session,
                "tools/call",
                json!(id),
                json!({"name":name,"arguments":arguments}),
            )
            .await;
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{name}");
            assert_eq!(
                response.json::<Value>().await.unwrap()["error"]["code"],
                "service_draining"
            );
        }
        assert_eq!(lifecycle.active_admissions(), 0);
    }

    fn rpc_error(value: &Value) -> i64 {
        value["error"]["code"].as_i64().unwrap()
    }

    #[tokio::test]
    async fn selected_mcp_and_http_runs_share_snapshot_checkpoint_and_mandatory_completion() {
        let (upstream, script) = serve_mock_upstream(vec![]).await;
        let mut state = test_state_at(&upstream.url);
        let mut config = (*state.config).clone();
        config
            .routing
            .models
            .insert("test-model".into(), crate::model_config::Model::default());
        config.routing.models.insert(
            "other-exposed-model".into(),
            crate::model_config::Model::default(),
        );
        config.routing.default_model = "test-model".into();
        config.creation.policy = crate::policy::CreationPolicy::AlwaysPostExecution;
        Arc::get_mut(&mut state).unwrap().config = Arc::new(config);
        let (owner, token) = issue(&state, permissions(true, true, false, true, false));
        let scoped = state.with_auth(&owner);
        let agent = scoped
            .portfolio
            .insert(&crate::portfolio::NewAgent {
                provider: String::new(),
                name: "selected MCP writer".into(),
                purpose: "writes a report using the caller's lookup tool".into(),
                system_prompt: "Use the caller's lookup tool when useful, then report findings"
                    .into(),
                model: "test-model".into(),
            })
            .unwrap();
        let tools = client_tools();
        script.lock().unwrap().0 = vec![
            client_tool_call("selected-client-call"),
            assistant_response("Selected specialist report after client continuation."),
            assistant_response("Selected MCP answer."),
            assistant_response(
                &json!({"decision":"use","agent_id":agent.id,"note":"the selected specialist fits"}).to_string(),
            ),
            assistant_response("HTTP specialist report."),
            assistant_response("HTTP answer."),
        ]
        .into();

        let server = serve(state.clone()).await;
        let client = Client::new();
        let (session, _) = initialize(&client, &server, &token).await;
        let pending = call_tool(
            &client,
            &server,
            &token,
            &session,
            "workforce.run_agent",
            json!({
                "id":agent.id,
                "message":"Look up the selected lifecycle record",
                "tools":tools
            }),
        )
        .await;
        assert_eq!(
            pending["waiting_for_tools"], true,
            "MCP returned: {pending:#}"
        );
        let mcp_conversation = pending["conversation_id"].as_str().unwrap().to_string();
        let pending_call_id = pending["assistant"]["tool_calls"][0]["id"]
            .as_str()
            .unwrap()
            .to_string();
        let continued = call_tool(
            &client,
            &server,
            &token,
            &session,
            "foreman.chat",
            json!({
                "conversation_id":mcp_conversation,
                "tools":tools,
                "tool_results":[{
                    "tool_call_id":pending_call_id,
                    "content":"The caller's system returned the selected lifecycle record."
                }]
            }),
        )
        .await;
        assert_eq!(continued["waiting_for_tools"], false);
        assert_eq!(continued["assistant"]["content"], "Selected MCP answer.");
        assert_eq!(continued["run_id"], pending["run_id"]);

        let mcp_run = pending["run_id"].as_str().unwrap();
        let selected_request = pending["request_id"].as_str().unwrap();
        let continued_request = continued["request_id"].as_str().unwrap();
        let snapshot = scoped.portfolio.model_snapshot(mcp_run).unwrap().unwrap();
        assert_eq!(snapshot.model, "test-model");
        let policy = scoped.portfolio.policy_context(continued_request).unwrap();
        let expected_client = json!([format!("key:{}", owner.key_id), "test-model"]).to_string();
        assert_eq!(policy.client, expected_client);
        assert_eq!(
            policy.policy,
            crate::policy::CreationPolicy::AlwaysPostExecution
        );
        assert_eq!(policy.phase.as_deref(), Some("post"));
        assert!(scoped.portfolio.policy_context(selected_request).is_ok());
        assert_captured_and_mandatory_work(
            &scoped,
            mcp_run,
            "Look up the selected lifecycle record",
        );

        let http_conversation = "http-selected-lifecycle";
        let http_response = client
            .post(format!("{}/v1/chat/completions", server.url))
            .bearer_auth(&token)
            .json(&json!({
                "model":"test-model",
                "stream":false,
                "messages":[{"role":"user","content":"Write the HTTP lifecycle report"}],
                "metadata":{"ccm_conversation_id":http_conversation}
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(http_response.status(), StatusCode::OK);
        let http_run = http_response.headers()["x-ccm-run-id"]
            .to_str()
            .unwrap()
            .to_string();
        let http_result: Value = http_response.json().await.unwrap();
        assert_eq!(
            http_result["choices"][0]["message"]["content"],
            "HTTP answer."
        );
        let http_state = state.with_auth(&owner);
        assert_eq!(
            http_state
                .portfolio
                .model_snapshot(&http_run)
                .unwrap()
                .unwrap()
                .model,
            "test-model"
        );
        let http_request = http_result["id"]
            .as_str()
            .and_then(|id| id.strip_prefix("chatcmpl-"))
            .unwrap();
        let http_policy = http_state.portfolio.policy_context(http_request).unwrap();
        assert_eq!(http_policy.client, expected_client);
        assert_eq!(http_policy.phase.as_deref(), Some("post"));
        assert_captured_and_mandatory_work(
            &http_state,
            &http_run,
            "Write the HTTP lifecycle report",
        );
    }

    fn assert_captured_and_mandatory_work(
        state: &crate::state::AppState,
        run: &str,
        expected_prompt: &str,
    ) {
        let captured: Vec<(String, String)> = {
            let conn = state.portfolio.conn.lock().unwrap();
            let mut statement = conn
                .prepare("SELECT prompt,status FROM improvement_turns WHERE run_id=?1 ORDER BY id")
                .unwrap();
            statement
                .query_map([run], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap()
        };
        assert!(
            captured
                .iter()
                .any(|(prompt, status)| { prompt == expected_prompt && status == "released" })
        );
        let jobs = state.portfolio.improvements(run).unwrap();
        assert!(
            jobs.as_array()
                .unwrap()
                .iter()
                .any(|job| { job["status"] == "queued" && job["kind"] == "workforce" })
        );
        assert!(!expected_prompt.is_empty());
    }

    #[tokio::test]
    async fn authenticated_streamable_protocol_and_tool_permissions() {
        let state = test_state();
        let (_, token) = issue(&state, permissions(true, true, false, false, false));
        let server = serve(state).await;
        let client = Client::new();

        let missing = client
            .post(format!("{}/mcp", server.url))
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}))
            .send()
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

        let (session, initialized) = initialize(&client, &server, &token).await;
        assert_eq!(initialized["result"]["protocolVersion"], "2025-11-25");
        assert_eq!(
            initialized["result"]["capabilities"]["tools"]["listChanged"],
            false
        );
        let initialized_notification = client
            .post(format!("{}/mcp", server.url))
            .bearer_auth(&token)
            .header(session_header().clone(), &session)
            .header(protocol_header().clone(), "2025-11-25")
            .json(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .send()
            .await
            .unwrap();
        assert_eq!(initialized_notification.status(), StatusCode::ACCEPTED);

        let event_stream = client
            .get(format!("{}/mcp", server.url))
            .bearer_auth(&token)
            .header(session_header().clone(), &session)
            .header(protocol_header().clone(), "2025-11-25")
            .header(header::ACCEPT, "text/event-stream")
            .send()
            .await
            .unwrap();
        assert_eq!(event_stream.status(), StatusCode::OK);
        assert_eq!(
            event_stream.headers()[header::CONTENT_TYPE],
            "text/event-stream"
        );

        let list = rpc(
            &client,
            &server,
            &token,
            &session,
            "tools/list",
            json!(2),
            json!({}),
        )
        .await;
        assert_eq!(list.status(), StatusCode::OK);
        let list: Value = list.json().await.unwrap();
        let names = list["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|tool| tool["name"].as_str())
            .collect::<Vec<_>>();
        assert!(names.contains(&"foreman.chat"));
        assert!(names.contains(&"workforce.list"));
        assert!(!names.contains(&"workforce.create_agent"));
        assert!(!names.contains(&"admin.keys.list"));

        let denied = call_tool(
            &client,
            &server,
            &token,
            &session,
            "admin.keys.list",
            json!({}),
        )
        .await;
        assert!(denied["error"].as_str().unwrap().contains("permission"));
        let permission_result = rpc(
            &client,
            &server,
            &token,
            &session,
            "tools/call",
            json!(3),
            json!({"name":"workforce.create_agent","arguments":{"requirements":"x"}}),
        )
        .await
        .json::<Value>()
        .await
        .unwrap();
        assert_eq!(permission_result["result"]["isError"], true);
        let unknown_method = rpc(
            &client,
            &server,
            &token,
            &session,
            "mcp/unknown",
            json!(8),
            json!({}),
        )
        .await
        .json::<Value>()
        .await
        .unwrap();
        assert_eq!(rpc_error(&unknown_method), -32601);

        let wrong_version = client
            .post(format!("{}/mcp", server.url))
            .bearer_auth(&token)
            .header(session_header().clone(), &session)
            .header(protocol_header().clone(), "2025-03-26")
            .json(&json!({"jsonrpc":"2.0","id":4,"method":"ping","params":{}}))
            .send()
            .await
            .unwrap();
        assert_eq!(wrong_version.status(), StatusCode::BAD_REQUEST);

        let no_session = client
            .post(format!("{}/mcp", server.url))
            .bearer_auth(&token)
            .json(&json!({"jsonrpc":"2.0","id":5,"method":"ping","params":{}}))
            .send()
            .await
            .unwrap();
        assert_eq!(no_session.status(), StatusCode::BAD_REQUEST);

        let deleted = client
            .delete(format!("{}/mcp", server.url))
            .bearer_auth(&token)
            .header(session_header().clone(), &session)
            .header(protocol_header().clone(), "2025-11-25")
            .send()
            .await
            .unwrap();
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
        let expired = rpc(
            &client,
            &server,
            &token,
            &session,
            "ping",
            json!(6),
            json!({}),
        )
        .await;
        assert_eq!(expired.status(), StatusCode::NOT_FOUND);

        let unknown = rpc(
            &client,
            &server,
            &token,
            "no-such-session",
            "does/not/exist",
            json!(7),
            json!({}),
        )
        .await;
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn session_is_bound_to_key_and_private_observability_is_scoped() {
        let state = test_state();
        let (first, first_token) = issue(&state, permissions(true, true, false, false, false));
        let (second, second_token) = issue(&state, permissions(true, true, false, false, false));
        for (auth, title) in [(&first, "private-one"), (&second, "private-two")] {
            let scoped = state.portfolio.with_scope(auth);
            let request = uuid::Uuid::new_v4().to_string();
            let payload = json!({"messages":[{"role":"user","content":title}]});
            scoped.trace_begin(&request, &payload, "mcp").unwrap();
            scoped
                .trace_finish_request(&request, "completed", &json!({"ok":true}))
                .unwrap();
        }
        let server = serve(state).await;
        let client = Client::new();
        let (session, _) = initialize(&client, &server, &first_token).await;

        let hijack = client
            .post(format!("{}/mcp", server.url))
            .bearer_auth(&second_token)
            .header(session_header().clone(), &session)
            .header(protocol_header().clone(), "2025-11-25")
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}))
            .send()
            .await
            .unwrap();
        assert_eq!(hijack.status(), StatusCode::NOT_FOUND);

        let owned = rpc(
            &client,
            &server,
            &first_token,
            &session,
            "tools/call",
            json!(2),
            json!({"name":"observability.runs","arguments":{"limit":100}}),
        )
        .await
        .json::<Value>()
        .await
        .unwrap();
        let items = owned["result"]["structuredContent"]["runs"]
            .as_array()
            .unwrap();
        assert_eq!(items.len(), 1);
        assert!(items[0]["title"].as_str().unwrap().contains("private-one"));
        assert!(!owned.to_string().contains("private-two"));

        let missing_version = client
            .post(format!("{}/mcp", server.url))
            .bearer_auth(&first_token)
            .header(session_header().clone(), &session)
            .json(&json!({"jsonrpc":"2.0","id":3,"method":"ping","params":{}}))
            .send()
            .await
            .unwrap();
        assert_eq!(missing_version.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn foreman_conversation_tools_are_private_and_paginated() {
        let state = test_state();
        let (owner, owner_token) = issue(&state, permissions(true, true, false, false, false));
        let (peer, peer_token) = issue(&state, permissions(true, true, false, false, false));
        {
            let conn = state.portfolio.conn.lock().unwrap();
            for (auth, id, title, created_ms) in [
                (&owner, "owner-conversation", "owner title", 2_i64),
                (&peer, "peer-conversation", "peer title", 1_i64),
            ] {
                conn.execute(
                    "INSERT INTO conversations(id,title,created_ms,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4,?5)",
                    rusqlite::params![id,title,created_ms,auth.workforce_id,auth.key_id],
                )
                .unwrap();
            }
            for index in 0..3 {
                let message = json!({
                    "role":"user",
                    "content":format!("owner message {index}"),
                    "ccm_narration":"private narration",
                    "authorization":"Bearer private-secret"
                });
                conn.execute(
                    "INSERT INTO conversation_messages(conversation_id,run_id,message,created_ms,workforce_id,owner_key_id) VALUES(?1,NULL,?2,?3,?4,?5)",
                    rusqlite::params!["owner-conversation",message.to_string(),index,owner.workforce_id,owner.key_id],
                )
                .unwrap();
            }
        }

        let server = serve(state).await;
        let client = Client::new();
        let (owner_session, _) = initialize(&client, &server, &owner_token).await;
        let (peer_session, _) = initialize(&client, &server, &peer_token).await;

        let listed = call_tool(
            &client,
            &server,
            &owner_token,
            &owner_session,
            "foreman.conversations",
            json!({"limit":1}),
        )
        .await;
        assert_eq!(listed["conversations"].as_array().unwrap().len(), 1);
        assert_eq!(listed["conversations"][0]["id"], "owner-conversation");
        assert_eq!(listed["next_offset"], Value::Null);

        let page = call_tool(
            &client,
            &server,
            &owner_token,
            &owner_session,
            "foreman.conversation",
            json!({"id":"owner-conversation","offset":1,"limit":1}),
        )
        .await;
        assert_eq!(page["messages"].as_array().unwrap().len(), 1);
        assert_eq!(page["total"], 3);
        assert_eq!(page["next_offset"], 2);
        assert!(page["messages"][0].get("ccm_narration").is_none());
        assert_eq!(page["messages"][0]["authorization"], "[redacted]");

        let foreign = call_tool(
            &client,
            &server,
            &owner_token,
            &owner_session,
            "foreman.conversation",
            json!({"id":"peer-conversation"}),
        )
        .await;
        assert_eq!(foreign["error"], "unknown conversation for this bearer key");

        let peer_list = call_tool(
            &client,
            &server,
            &peer_token,
            &peer_session,
            "foreman.conversations",
            json!({}),
        )
        .await;
        assert_eq!(peer_list["conversations"].as_array().unwrap().len(), 1);
        assert_eq!(peer_list["conversations"][0]["id"], "peer-conversation");
    }

    #[tokio::test]
    async fn mcp_tool_approval_handoffs_are_private_one_time_and_resume_the_original_run() {
        let _config_guard = crate::config::lock_tooling_config_for_test().await;
        let root = std::env::temp_dir().join(format!("ccm-mcp-approval-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join("plugins")).unwrap();
        let (upstream, script) = serve_mock_upstream(vec![]).await;
        let state = test_state_at(&upstream.url);
        let (owner, owner_token) = issue(&state, permissions(true, true, false, false, false));
        let (_peer, peer_token) = state
            .portfolio
            .create_key(
                "same-workforce peer",
                Some(&owner.workforce_id),
                "MCP test workforce",
                &permissions(true, true, false, false, false),
            )
            .unwrap();
        let owner_state = state.with_auth(&owner);
        let agent = owner_state
            .portfolio
            .insert(&crate::portfolio::NewAgent {
                provider: String::new(),
                name: "MCP approval writer".into(),
                purpose: "writes approved files".into(),
                system_prompt: "Write only when the supplied tool is approved".into(),
                model: "test-model".into(),
            })
            .unwrap();
        crate::config::set_test_tooling_config(crate::config::ToolingConfig {
            plugin_dir: root.join("plugins").to_string_lossy().into_owned(),
            workspace_root: root.to_string_lossy().into_owned(),
            workforces: std::collections::BTreeMap::from([(
                owner.workforce_id.clone(),
                crate::config::WorkforceToolPolicy {
                    default: crate::config::ToolPermission::Deny,
                    grants: vec![crate::config::ToolGrant {
                        tool: "file.write".into(),
                        decision: crate::config::ToolPermission::Ask,
                        agent_id: Some(agent.id.clone()),
                        stage_id: None,
                    }],
                },
            )]),
            ..Default::default()
        });
        script.lock().unwrap().0 = vec![
            assistant_response(
                &json!({"decision":"use","agent_id":agent.id,"note":"fits"}).to_string(),
            ),
            file_write_call("approved.txt", "approved exactly once", "mcp-approved"),
            assistant_response("The approved file was written."),
            assistant_response("Composed answer after approval."),
            file_write_call("denied.txt", "must not be written", "mcp-denied"),
            assistant_response("The requested write was denied."),
            assistant_response("Composed answer after denial."),
            assistant_response(
                &json!({"decision":"use","agent_id":agent.id,"note":"fits"}).to_string(),
            ),
            file_write_call("cancelled.txt", "must not be written", "mcp-cancelled"),
        ]
        .into();

        let server = serve(state.clone()).await;
        let client = Client::new();
        let (owner_session, _) = initialize(&client, &server, &owner_token).await;
        let (peer_session, _) = initialize(&client, &server, &peer_token).await;
        let listed: Value = rpc(
            &client,
            &server,
            &owner_token,
            &owner_session,
            "tools/list",
            json!(1),
            json!({}),
        )
        .await
        .json()
        .await
        .unwrap();
        for name in [
            "tool_approvals.status",
            "tool_approvals.approve",
            "tool_approvals.deny",
            "tool_approvals.cancel",
            "tool_approvals.result",
        ] {
            assert!(
                listed["result"]["tools"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|tool| tool["name"] == name)
            );
        }
        assert!(!listed.to_string().contains("operations.cancel"));

        let pending = call_tool(
            &client,
            &server,
            &owner_token,
            &owner_session,
            "foreman.chat",
            json!({"message":"Write approved.txt"}),
        )
        .await;
        assert_eq!(pending["status"], "waiting_approval");
        assert_eq!(pending["approvals"].as_array().unwrap().len(), 1);
        assert!(pending.get("assistant").is_none());
        let approval = pending["approvals"][0].clone();
        assert_eq!(approval["requested_by"]["key_id"], owner.key_id);
        assert_eq!(approval["requested_by"]["workforce_id"], owner.workforce_id);
        assert_eq!(approval["operation"]["tool"], "file.write");
        assert_eq!(approval["operation"]["arguments"]["path"], "approved.txt");
        assert!(!root.join("approved.txt").exists());

        let hijack = rpc(
            &client,
            &server,
            &peer_token,
            &owner_session,
            "tools/call",
            json!("hijack"),
            json!({"name":"tool_approvals.status","arguments":{"id":approval["id"]}}),
        )
        .await;
        assert_eq!(hijack.status(), StatusCode::NOT_FOUND);
        let peer_status = call_tool(
            &client,
            &server,
            &peer_token,
            &peer_session,
            "tool_approvals.status",
            json!({"id":approval["id"]}),
        )
        .await;
        assert_eq!(peer_status["error"], "unknown tool approval");

        let inspected = call_tool(
            &client,
            &server,
            &owner_token,
            &owner_session,
            "tool_approvals.status",
            json!({"id":approval["id"]}),
        )
        .await;
        assert_eq!(inspected["status"], "pending");
        assert_eq!(inspected["flow"]["run_id"], approval["run_id"]);
        let approved = call_tool(
            &client,
            &server,
            &owner_token,
            &owner_session,
            "tool_approvals.approve",
            json!({"id":approval["id"]}),
        )
        .await;
        assert_eq!(approved["status"], "approved");
        let replay = call_tool(
            &client,
            &server,
            &owner_token,
            &owner_session,
            "tool_approvals.approve",
            json!({"id":approval["id"]}),
        )
        .await;
        assert!(
            replay["error"]
                .as_str()
                .unwrap()
                .contains("already resolved")
        );
        let resumed = wait_approval_result(
            &client,
            &server,
            &owner_token,
            &owner_session,
            approval["id"].as_str().unwrap(),
            "completed",
        )
        .await;
        assert_eq!(resumed["request_id"], approval["request_id"]);
        assert_eq!(resumed["run_id"], approval["run_id"]);
        assert_eq!(
            resumed["result"]["choices"][0]["message"]["content"],
            "Composed answer after approval."
        );
        assert_eq!(
            std::fs::read_to_string(root.join("approved.txt")).unwrap(),
            "approved exactly once"
        );
        assert_eq!(
            crate::runtime::tools::permission_for(
                &crate::config::tooling_config(),
                &owner.workforce_id,
                &agent.id,
                None,
                "file.write"
            ),
            crate::config::ToolPermission::Ask,
            "approval must not mutate the stored policy"
        );

        let denied_pending = call_tool(
            &client,
            &server,
            &owner_token,
            &owner_session,
            "workforce.run_agent",
            json!({"id":agent.id,"message":"Write denied.txt"}),
        )
        .await;
        assert_eq!(denied_pending["status"], "waiting_approval");
        let denied_handle = denied_pending["approvals"][0].clone();
        assert!(!root.join("denied.txt").exists());
        let denied = call_tool(
            &client,
            &server,
            &owner_token,
            &owner_session,
            "tool_approvals.deny",
            json!({"id":denied_handle["id"]}),
        )
        .await;
        assert_eq!(denied["status"], "denied");
        let denial_result = wait_approval_result(
            &client,
            &server,
            &owner_token,
            &owner_session,
            denied_handle["id"].as_str().unwrap(),
            "completed",
        )
        .await;
        assert_eq!(denial_result["request_id"], denied_handle["request_id"]);
        assert_eq!(denial_result["run_id"], denied_handle["run_id"]);
        assert!(!root.join("denied.txt").exists());
        assert_eq!(
            denial_result["result"]["assistant"]["content"],
            "Composed answer after denial."
        );

        let cancel_pending = call_tool(
            &client,
            &server,
            &owner_token,
            &owner_session,
            "foreman.chat",
            json!({"message":"Write cancelled.txt"}),
        )
        .await;
        assert_eq!(cancel_pending["status"], "waiting_approval");
        let cancelled_handle = cancel_pending["approvals"][0].clone();
        let cancelled = call_tool(
            &client,
            &server,
            &owner_token,
            &owner_session,
            "tool_approvals.cancel",
            json!({"id":cancelled_handle["id"]}),
        )
        .await;
        assert_eq!(cancelled["status"], "cancelled");
        assert!(!root.join("cancelled.txt").exists());
        let _ = wait_approval_result(
            &client,
            &server,
            &owner_token,
            &owner_session,
            cancelled_handle["id"].as_str().unwrap(),
            "cancelled",
        )
        .await;

        // REST cancellation shares the same current-invoker rule; a caller with
        // roster_use and no roster_write can cancel only its own approval.
        let direct_request = uuid::Uuid::new_v4().to_string();
        let direct_payload = json!({"messages":[{"role":"user","content":"cancel my approval"}]});
        let direct_run = owner_state
            .portfolio
            .trace_begin(&direct_request, &direct_payload, "mcp")
            .unwrap();
        owner_state
            .portfolio
            .bind_policy(
                &direct_request,
                &direct_run,
                &format!("key:{}", owner.key_id),
                state.config.creation.policy,
            )
            .unwrap();
        let direct_pending = owner_state
            .portfolio
            .async_ops
            .register_tool_approvals(
                &owner_state.portfolio,
                &direct_request,
                &direct_run,
                vec![crate::async_ops::ToolApprovalRequest {
                    target_name: "ccm_file_write".into(),
                    target: crate::runtime::tools::builtin_target("file.write").unwrap(),
                    tool: "file.write".into(),
                    arguments: json!({"path":"rest-cancel.txt","content":"no"}),
                    agent_id: agent.id.clone(),
                    stage_id: None,
                }],
            )
            .await
            .unwrap()
            .remove(0);
        let rest_cancel = client
            .delete(format!(
                "{}{}",
                server.url, direct_pending.handle.cancel_url
            ))
            .bearer_auth(&owner_token)
            .send()
            .await
            .unwrap();
        assert_eq!(rest_cancel.status(), StatusCode::OK);
        assert_eq!(
            rest_cancel.json::<Value>().await.unwrap()["status"],
            "cancelled"
        );

        let too_long_id = call_tool(
            &client,
            &server,
            &owner_token,
            &owner_session,
            "tool_approvals.status",
            json!({"id":"i".repeat(129)}),
        )
        .await;
        assert!(
            too_long_id["error"]
                .as_str()
                .unwrap()
                .contains("allowed length")
        );
        assert_eq!(script.lock().unwrap().1.len(), 9);

        std::fs::remove_file(root.join("approved.txt")).unwrap();
        std::fs::remove_dir(root.join("plugins")).unwrap();
        std::fs::remove_dir(root).unwrap();
    }

    async fn wait_approval_result(
        client: &Client,
        server: &TestServer,
        token: &str,
        session: &str,
        id: &str,
        expected: &str,
    ) -> Value {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let result = call_tool(
                    client,
                    server,
                    token,
                    session,
                    "tool_approvals.result",
                    json!({"id":id}),
                )
                .await;
                if result["status"] == expected {
                    return result;
                }
                assert!(
                    matches!(
                        result["status"].as_str(),
                        Some("pending" | "running" | "waiting_approval" | "cancelling")
                    ),
                    "approval reached unexpected result: {result}"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("tool approval did not reach expected result")
    }

    struct McpApprovalHandoff {
        state: Arc<crate::state::AppState>,
        owner: crate::auth::AuthContext,
        root: std::path::PathBuf,
        _upstream: TestServer,
        session: String,
        conversation: String,
        original_request: String,
        run: String,
        call_id: String,
        tools: Value,
    }

    async fn start_mcp_approval_handoff(final_reply: Value, label: &str) -> McpApprovalHandoff {
        let root = std::env::temp_dir().join(format!(
            "ccm-mcp-publication-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(root.join("plugins")).unwrap();
        let (upstream, _) = serve_mock_upstream(vec![
            file_write_call(
                &format!("{label}.txt"),
                "one approved write",
                &format!("{label}-approval-write"),
            ),
            client_tool_call(&format!("{label}-client-call")),
            assistant_response("Final specialist report after client continuation."),
            final_reply,
        ])
        .await;
        let state = test_state_at(&upstream.url);
        let (owner, token) = issue(&state, permissions(true, true, false, true, false));
        let scoped = state.with_auth(&owner);
        let agent = scoped
            .portfolio
            .insert(&crate::portfolio::NewAgent {
                provider: String::new(),
                name: format!("MCP {label} writer"),
                purpose: "executes an approved write before asking for a client lookup".into(),
                system_prompt: "Complete the supplied task in order".into(),
                model: "test-model".into(),
            })
            .unwrap();
        crate::config::set_test_tooling_config(crate::config::ToolingConfig {
            plugin_dir: root.join("plugins").to_string_lossy().into_owned(),
            workspace_root: root.to_string_lossy().into_owned(),
            workforces: std::collections::BTreeMap::from([(
                owner.workforce_id.clone(),
                crate::config::WorkforceToolPolicy {
                    default: crate::config::ToolPermission::Deny,
                    grants: vec![crate::config::ToolGrant {
                        tool: "file.write".into(),
                        decision: crate::config::ToolPermission::Ask,
                        agent_id: Some(agent.id.clone()),
                        stage_id: None,
                    }],
                },
            )]),
            ..Default::default()
        });

        let server = serve(state.clone()).await;
        let client = Client::new();
        let (session, _) = initialize(&client, &server, &token).await;
        let tools = client_tools();
        let pending = call_tool(
            &client,
            &server,
            &token,
            &session,
            "workforce.run_agent",
            json!({
                "id":agent.id,
                "message":format!("Write {label}.txt, then look up the lifecycle record"),
                "tools":tools.clone()
            }),
        )
        .await;
        assert_eq!(pending["status"], "waiting_approval");
        let original_request = pending["request_id"].as_str().unwrap().to_owned();
        scoped
            .portfolio
            .queue_improvement(
                &original_request,
                "agent",
                &agent.id,
                &json!(agent),
                "Assess the approved file-writing stage",
                "The stage produced a report after its authorized file write.",
                scoped.portfolio.generation(),
            )
            .unwrap();
        let approval_id = pending["approvals"][0]["id"].as_str().unwrap();
        let approved = call_tool(
            &client,
            &server,
            &token,
            &session,
            "tool_approvals.approve",
            json!({"id":approval_id}),
        )
        .await;
        assert_eq!(approved["status"], "approved");
        let approval_result =
            wait_approval_result(&client, &server, &token, &session, approval_id, "completed")
                .await;
        assert_eq!(approval_result["result"]["waiting_for_tools"], true);
        assert_eq!(
            approval_result["result"]["assistant"]["tool_calls"][0]["function"]["name"],
            "client_lookup"
        );
        assert_eq!(
            std::fs::read_to_string(root.join(format!("{label}.txt"))).unwrap(),
            "one approved write"
        );
        let call_id = approval_result["result"]["assistant"]["tool_calls"][0]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let run = pending["run_id"].as_str().unwrap().to_owned();
        let conversation = pending["conversation_id"].as_str().unwrap().to_owned();
        let held: bool = scoped
            .portfolio
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM improvement_jobs
                 WHERE origin_request=?1 AND status='held' AND entity_kind='agent')",
                [&original_request],
                |row| row.get(0),
            )
            .unwrap();
        assert!(held, "fixture must carry real held assessment work");

        McpApprovalHandoff {
            state,
            owner,
            root,
            _upstream: upstream,
            session,
            conversation,
            original_request,
            run,
            call_id,
            tools,
        }
    }

    fn cleanup_mcp_approval_handoff(fixture: &McpApprovalHandoff, label: &str) {
        let _ = std::fs::remove_file(fixture.root.join(format!("{label}.txt")));
        let _ = std::fs::remove_dir(fixture.root.join("plugins"));
        let _ = std::fs::remove_dir(&fixture.root);
    }

    async fn direct_mcp_tool_call(
        fixture: &McpApprovalHandoff,
        id: &str,
        name: &str,
        arguments: Value,
    ) -> axum::response::Response {
        let mut headers = HeaderMap::new();
        headers.insert(header::ACCEPT, HeaderValue::from_static("application/json"));
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        headers.insert(
            session_header().clone(),
            HeaderValue::from_str(&fixture.session).unwrap(),
        );
        headers.insert(
            protocol_header().clone(),
            HeaderValue::from_static("2025-11-25"),
        );
        let request = json!({
            "jsonrpc":"2.0",
            "id":id,
            "method":"tools/call",
            "params":{"name":name,"arguments":arguments}
        });
        post_mcp(
            State(fixture.state.clone()),
            Extension(fixture.owner.clone()),
            None,
            headers,
            Bytes::from(request.to_string()),
        )
        .await
    }

    fn held_job_status(state: &crate::state::AppState, request: &str) -> (i64, i64) {
        let conn = state.portfolio.conn.lock().unwrap();
        conn.query_row(
            "SELECT
                COUNT(*) FILTER (WHERE status='held'),
                COUNT(*) FILTER (WHERE status='cancelled')
             FROM improvement_jobs WHERE origin_request=?1 AND entity_kind='agent'",
            [request],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
    }

    fn conversation_assistant_message_count(fixture: &McpApprovalHandoff) -> i64 {
        let conn = fixture.state.portfolio.conn.lock().unwrap();
        let mut statement = conn
            .prepare(
                "SELECT message FROM conversation_messages
                 WHERE conversation_id=?1 AND run_id=?2",
            )
            .unwrap();
        statement
            .query_map(
                rusqlite::params![fixture.conversation, fixture.run],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
            .filter_map(Result::ok)
            .filter(|message| {
                serde_json::from_str::<Value>(message)
                    .ok()
                    .is_some_and(|message| message["role"] == "assistant")
            })
            .count() as i64
    }

    fn assert_run_response_cancelled(fixture: &McpApprovalHandoff) {
        let (held, cancelled) = held_job_status(&fixture.state, &fixture.original_request);
        assert_eq!(held, 0, "abandoned run must not leave held assessment work");
        assert!(
            cancelled > 0,
            "the real held assessment job must be cancelled"
        );
        let status: String = fixture
            .state
            .portfolio
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT status FROM improvement_responses WHERE request_id=?1",
                [&fixture.original_request],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "cancelled");
    }

    #[tokio::test]
    async fn dropped_final_mcp_body_cancels_run_holds_without_persisting_message() {
        let _config_guard = crate::config::lock_tooling_config_for_test().await;
        let fixture = start_mcp_approval_handoff(
            assistant_response("Final answer whose public MCP body will be dropped."),
            "drop-final",
        )
        .await;
        let messages_before = conversation_assistant_message_count(&fixture);
        assert!(held_job_status(&fixture.state, &fixture.original_request).0 > 0);

        let response = direct_mcp_tool_call(
            &fixture,
            "drop-final-continuation",
            "foreman.chat",
            json!({
                "conversation_id":fixture.conversation,
                "tools":fixture.tools,
                "tool_results":[{
                    "tool_call_id":fixture.call_id,
                    "content":"The caller returned the requested lifecycle record."
                }]
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        drop(response);

        assert_run_response_cancelled(&fixture);
        assert_eq!(
            conversation_assistant_message_count(&fixture),
            messages_before,
            "an abandoned final message must not be saved as part of the conversation"
        );
        cleanup_mcp_approval_handoff(&fixture, "drop-final");
    }

    #[tokio::test]
    async fn oversized_public_mcp_wrapper_is_rejected_and_cancels_run_holds() {
        let _config_guard = crate::config::lock_tooling_config_for_test().await;
        let large_answer = assistant_response(&"x".repeat(600 * 1024));
        let fixture = start_mcp_approval_handoff(large_answer, "oversized-final").await;
        let messages_before = conversation_assistant_message_count(&fixture);
        assert!(held_job_status(&fixture.state, &fixture.original_request).0 > 0);

        let response = direct_mcp_tool_call(
            &fixture,
            "oversized-final-continuation",
            "foreman.chat",
            json!({
                "conversation_id":fixture.conversation,
                "tools":fixture.tools,
                "tool_results":[{
                    "tool_call_id":fixture.call_id,
                    "content":"The caller returned the requested lifecycle record."
                }]
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), MAX_TOOL_RESULT_BYTES + 1024)
            .await
            .unwrap();
        assert!(body.len() <= MAX_TOOL_RESULT_BYTES);
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["result"]["isError"], true);
        assert_eq!(
            value["result"]["structuredContent"]["error"],
            "MCP response exceeds the 1 MiB response limit"
        );

        assert_run_response_cancelled(&fixture);
        assert_eq!(
            conversation_assistant_message_count(&fixture),
            messages_before,
            "an oversized final answer must not be persisted as a published message"
        );
        cleanup_mcp_approval_handoff(&fixture, "oversized-final");
    }

    #[tokio::test]
    async fn mcp_approval_client_tool_handoff_resolves_original_assessment_at_final_continuation() {
        let _config_guard = crate::config::lock_tooling_config_for_test().await;
        let root = std::env::temp_dir().join(format!(
            "ccm-mcp-approval-client-tool-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(root.join("plugins")).unwrap();
        let (upstream, script) = serve_mock_upstream(vec![]).await;
        let state = test_state_at(&upstream.url);
        let (owner, owner_token) = issue(&state, permissions(true, true, false, true, false));
        let scoped = state.with_auth(&owner);
        let agent = scoped
            .portfolio
            .insert(&crate::portfolio::NewAgent {
                provider: String::new(),
                name: "MCP approval then client tool".into(),
                purpose: "executes the approved write, then requests a client lookup".into(),
                system_prompt: "Complete the supplied task in order".into(),
                model: "test-model".into(),
            })
            .unwrap();
        crate::config::set_test_tooling_config(crate::config::ToolingConfig {
            plugin_dir: root.join("plugins").to_string_lossy().into_owned(),
            workspace_root: root.to_string_lossy().into_owned(),
            workforces: std::collections::BTreeMap::from([(
                owner.workforce_id.clone(),
                crate::config::WorkforceToolPolicy {
                    default: crate::config::ToolPermission::Deny,
                    grants: vec![crate::config::ToolGrant {
                        tool: "file.write".into(),
                        decision: crate::config::ToolPermission::Ask,
                        agent_id: Some(agent.id.clone()),
                        stage_id: None,
                    }],
                },
            )]),
            ..Default::default()
        });
        script.lock().unwrap().0 = vec![
            file_write_call(
                "approved-before-client-tool.txt",
                "write once",
                "approval-write",
            ),
            client_tool_call("lookup-after-approval"),
            assistant_response("Final specialist report after client continuation."),
            assistant_response("Final answer after approval and client lookup."),
        ]
        .into();

        let server = serve(state.clone()).await;
        let client = Client::new();
        let (session, _) = initialize(&client, &server, &owner_token).await;
        let tools = client_tools();
        let pending = call_tool(
            &client,
            &server,
            &owner_token,
            &session,
            "workforce.run_agent",
            json!({
                "id":agent.id,
                "message":"Write the approved file, then look up the lifecycle record",
                "tools":tools.clone()
            }),
        )
        .await;
        assert_eq!(pending["status"], "waiting_approval");
        let approval_request = pending["request_id"].as_str().unwrap();
        scoped
            .portfolio
            .queue_improvement(
                approval_request,
                "agent",
                &agent.id,
                &json!(agent),
                "Assess the approved file-writing stage",
                "The stage produced a report after its authorized file write.",
                scoped.portfolio.generation(),
            )
            .unwrap();
        let approval = pending["approvals"][0]["id"].as_str().unwrap();
        let approved = call_tool(
            &client,
            &server,
            &owner_token,
            &session,
            "tool_approvals.approve",
            json!({"id":approval}),
        )
        .await;
        assert_eq!(approved["status"], "approved");
        let handoff = wait_approval_result(
            &client,
            &server,
            &owner_token,
            &session,
            approval,
            "completed",
        )
        .await;
        assert_eq!(handoff["result"]["waiting_for_tools"], true);
        assert_eq!(
            handoff["result"]["assistant"]["tool_calls"][0]["function"]["name"],
            "client_lookup"
        );
        assert_eq!(
            std::fs::read_to_string(root.join("approved-before-client-tool.txt")).unwrap(),
            "write once"
        );
        let (prematurely_released, held_assessments): (bool, Vec<String>) = {
            let conn = state.portfolio.conn.lock().unwrap();
            let released = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM improvement_responses
                     WHERE request_id=?1 AND status='released')",
                    [approval_request],
                    |row| row.get(0),
                )
                .unwrap();
            let mut statement = conn
                .prepare(
                    "SELECT entity_kind FROM improvement_jobs
                     WHERE origin_request=?1 AND status='held' ORDER BY entity_kind",
                )
                .unwrap();
            let held = statement
                .query_map([approval_request], |row| row.get(0))
                .unwrap()
                .collect::<Result<Vec<String>, _>>()
                .unwrap();
            (released, held)
        };
        assert!(
            !prematurely_released,
            "approval completion must preserve the deferred response gate while client tools are pending"
        );
        assert!(
            !held_assessments.is_empty(),
            "the original request must have a real held stage-assessment job"
        );
        assert!(held_assessments.iter().any(|kind| kind == "agent"));

        let conversation = pending["conversation_id"].as_str().unwrap();
        let call_id = handoff["result"]["assistant"]["tool_calls"][0]["id"]
            .as_str()
            .unwrap();
        let continued = call_tool(
            &client,
            &server,
            &owner_token,
            &session,
            "foreman.chat",
            json!({
                "conversation_id":conversation,
                "tools":tools,
                "tool_results":[{
                    "tool_call_id":call_id,
                    "content":"The caller returned the requested lifecycle record."
                }]
            }),
        )
        .await;
        assert_eq!(
            continued["waiting_for_tools"], false,
            "continued: {continued}"
        );
        assert_eq!(
            continued["assistant"]["content"],
            "Final answer after approval and client lookup."
        );
        assert_eq!(continued["run_id"], pending["run_id"]);
        let final_request = continued["request_id"].as_str().unwrap();
        let (original_released, final_released, held_after_final): (bool, bool, i64) = {
            let conn = state.portfolio.conn.lock().unwrap();
            let original_released = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM improvement_responses
                     WHERE request_id=?1 AND status='released')",
                    [approval_request],
                    |row| row.get(0),
                )
                .unwrap();
            let final_released = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM improvement_responses
                     WHERE request_id=?1 AND status='released')",
                    [final_request],
                    |row| row.get(0),
                )
                .unwrap();
            let held = conn
                .query_row(
                    "SELECT COUNT(*) FROM improvement_jobs
                     WHERE origin_request=?1 AND status='held'",
                    [approval_request],
                    |row| row.get(0),
                )
                .unwrap();
            (original_released, final_released, held)
        };
        assert!(
            original_released,
            "the original approval gate must be resolved"
        );
        assert!(
            final_released,
            "the final continuation gate must be resolved"
        );
        assert_eq!(
            held_after_final, 0,
            "all original stage-assessment rows must leave held state after final publication"
        );

        std::fs::remove_file(root.join("approved-before-client-tool.txt")).unwrap();
        std::fs::remove_dir(root.join("plugins")).unwrap();
        std::fs::remove_dir(root).unwrap();
    }

    #[tokio::test]
    async fn mcp_async_approval_resumes_and_reports_the_original_operation() {
        let _config_guard = crate::config::lock_tooling_config_for_test().await;
        let root =
            std::env::temp_dir().join(format!("ccm-mcp-async-approval-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join("plugins")).unwrap();
        let (upstream, script) = serve_mock_upstream(vec![]).await;
        let state = test_state_at(&upstream.url);
        let (owner, owner_token) = issue(&state, permissions(true, true, false, false, false));
        let scoped = state.with_auth(&owner);
        let agent = scoped
            .portfolio
            .insert(&crate::portfolio::NewAgent {
                provider: String::new(),
                name: "MCP async writer".into(),
                purpose: "writes after approval".into(),
                system_prompt: "Use the supplied file tool when requested".into(),
                model: "test-model".into(),
            })
            .unwrap();
        crate::config::set_test_tooling_config(crate::config::ToolingConfig {
            plugin_dir: root.join("plugins").to_string_lossy().into_owned(),
            workspace_root: root.to_string_lossy().into_owned(),
            workforces: std::collections::BTreeMap::from([(
                owner.workforce_id.clone(),
                crate::config::WorkforceToolPolicy {
                    default: crate::config::ToolPermission::Deny,
                    grants: vec![crate::config::ToolGrant {
                        tool: "file.write".into(),
                        decision: crate::config::ToolPermission::Ask,
                        agent_id: Some(agent.id.clone()),
                        stage_id: None,
                    }],
                },
            )]),
            ..Default::default()
        });
        script.lock().unwrap().0 = vec![
            assistant_response(
                &json!({"decision":"use","agent_id":agent.id,"note":"fits"}).to_string(),
            ),
            file_write_call("async-approved.txt", "same operation", "mcp-async-approved"),
            assistant_response("Async specialist report."),
            assistant_response("Async composition result."),
        ]
        .into();

        let server = serve(state).await;
        let client = Client::new();
        let (session, _) = initialize(&client, &server, &owner_token).await;
        let accepted = call_tool(
            &client,
            &server,
            &owner_token,
            &session,
            "foreman.start_async",
            json!({"message":"Write async-approved.txt"}),
        )
        .await;
        assert_eq!(accepted["status"], "accepted");
        let operation_id = accepted["operation"]["id"].as_str().unwrap().to_owned();

        let waiting = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let status = call_tool(
                    &client,
                    &server,
                    &owner_token,
                    &session,
                    "operations.status",
                    json!({"id":operation_id}),
                )
                .await;
                if status["status"] == "waiting_approval" {
                    break status;
                }
                assert!(
                    !matches!(
                        status["status"].as_str(),
                        Some("completed" | "error" | "cancelled" | "expired")
                    ),
                    "async run ended before approval: {status}"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("MCP async operation did not reach approval wait");
        let approval = waiting["pending_approvals"][0].clone();
        assert_eq!(approval["operation_id"], operation_id);
        assert!(!root.join("async-approved.txt").exists());

        let approved = call_tool(
            &client,
            &server,
            &owner_token,
            &session,
            "tool_approvals.approve",
            json!({"id":approval["id"]}),
        )
        .await;
        assert_eq!(approved["status"], "approved");
        let resumed = wait_approval_result(
            &client,
            &server,
            &owner_token,
            &session,
            approval["id"].as_str().unwrap(),
            "completed",
        )
        .await;
        assert_eq!(resumed["operation_id"], operation_id);
        assert_eq!(
            resumed["result"]["choices"][0]["message"]["content"],
            "Async composition result."
        );
        let operation_result = call_tool(
            &client,
            &server,
            &owner_token,
            &session,
            "operations.result",
            json!({"id":operation_id}),
        )
        .await;
        assert_eq!(operation_result["status"], "completed");
        assert_eq!(
            std::fs::read_to_string(root.join("async-approved.txt")).unwrap(),
            "same operation"
        );
        assert_eq!(script.lock().unwrap().1.len(), 4);

        std::fs::remove_file(root.join("async-approved.txt")).unwrap();
        std::fs::remove_dir(root.join("plugins")).unwrap();
        std::fs::remove_dir(root).unwrap();
    }

    #[tokio::test]
    async fn bootstrap_admin_tools_are_separate_and_key_tokens_are_one_time() {
        let state = test_state();
        state
            .portfolio
            .seed_bootstrap_key("mcp-bootstrap-secret")
            .unwrap();
        let (_, ordinary) = issue(&state, permissions(true, true, true, false, false));
        let server = serve(state.clone()).await;
        let client = Client::new();
        let (ordinary_session, _) = initialize(&client, &server, &ordinary).await;
        let ordinary_tools: Value = rpc(
            &client,
            &server,
            &ordinary,
            &ordinary_session,
            "tools/list",
            json!(1),
            json!({}),
        )
        .await
        .json()
        .await
        .unwrap();
        assert!(!ordinary_tools.to_string().contains("admin.keys.create"));

        let (admin_session, _) = initialize(&client, &server, "mcp-bootstrap-secret").await;
        let admin_tools: Value = rpc(
            &client,
            &server,
            "mcp-bootstrap-secret",
            &admin_session,
            "tools/list",
            json!(2),
            json!({}),
        )
        .await
        .json()
        .await
        .unwrap();
        let tools = admin_tools["result"]["tools"].as_array().unwrap();
        assert!(tools.iter().any(|tool| tool["name"] == "admin.keys.create"));
        assert!(!tools.iter().any(|tool| tool["name"] == "foreman.chat"));

        let created = call_tool(
            &client,
            &server,
            "mcp-bootstrap-secret",
            &admin_session,
            "admin.keys.create",
            json!({"label":"new MCP key","permissions":{"roster_read":true}}),
        )
        .await;
        let token = created["token"].as_str().unwrap();
        assert!(token.starts_with("ccm_"));
        let listed = call_tool(
            &client,
            &server,
            "mcp-bootstrap-secret",
            &admin_session,
            "admin.keys.list",
            json!({}),
        )
        .await;
        assert!(!listed.to_string().contains(token));

        let workforce = call_tool(
            &client,
            &server,
            "mcp-bootstrap-secret",
            &admin_session,
            "admin.workforces.create",
            json!({"name":"shared MCP workforce","visibility":"shared"}),
        )
        .await;
        assert_eq!(workforce["visibility"], "shared");
    }

    #[tokio::test]
    async fn live_async_config_tools_are_bootstrap_only_bounded_and_mutable() {
        let state = test_state();
        state
            .portfolio
            .seed_bootstrap_key("mcp-config-admin-secret")
            .unwrap();
        let (_, ordinary) = issue(&state, permissions(true, true, true, false, false));
        let server = serve(state.clone()).await;
        let client = Client::new();
        let (ordinary_session, _) = initialize(&client, &server, &ordinary).await;
        let (admin_session, _) = initialize(&client, &server, "mcp-config-admin-secret").await;

        let ordinary_tools: Value = rpc(
            &client,
            &server,
            &ordinary,
            &ordinary_session,
            "tools/list",
            json!(1),
            json!({}),
        )
        .await
        .json()
        .await
        .unwrap();
        let ordinary_names = ordinary_tools["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|tool| tool["name"].as_str())
            .collect::<Vec<_>>();
        assert!(ordinary_names.contains(&"config.get"));
        assert!(!ordinary_names.contains(&"config.async_limits.get"));
        assert!(!ordinary_names.contains(&"config.async_limits.set_server"));
        assert!(!ordinary_names.contains(&"config.async_limits.set_workforce"));

        let projection = call_tool(
            &client,
            &server,
            &ordinary,
            &ordinary_session,
            "config.get",
            json!({}),
        )
        .await;
        assert_eq!(projection["scope"], "key_workforce");
        assert_eq!(projection["model"], "test-model");
        assert_eq!(
            projection["async_supervisor"]["server_scope"],
            "bootstrap_admin_only"
        );
        assert!(projection["async_supervisor"]["server"].is_null());
        assert!(projection.get("upstream_timeouts").is_none());
        assert!(projection.get("memory_budget_mib").is_none());
        assert!(projection.get("improvement").is_none());
        assert!(projection["settings"]["revision"].is_string());
        assert!(projection["native_tool_sandbox"]["shell"]["mode"].is_string());
        assert!(projection["native_tool_sandbox"]["shell"]["backend"].is_string());
        assert!(!projection.to_string().contains("not-for-mcp"));
        assert!(!projection.to_string().contains("/private/provider"));

        let denied = call_tool(
            &client,
            &server,
            &ordinary,
            &ordinary_session,
            "config.async_limits.get",
            json!({}),
        )
        .await;
        assert!(denied["error"].as_str().unwrap().contains("permission"));
        let denied_write = call_tool(
            &client,
            &server,
            &ordinary,
            &ordinary_session,
            "config.async_limits.set_server",
            json!({
                "max_root_operations":8,
                "max_child_workers":6,
                "max_queued_roots":10,
                "max_queue_wait_seconds":180
            }),
        )
        .await;
        assert!(
            denied_write["error"]
                .as_str()
                .unwrap()
                .contains("permission")
        );

        let admin_tools: Value = rpc(
            &client,
            &server,
            "mcp-config-admin-secret",
            &admin_session,
            "tools/list",
            json!(2),
            json!({}),
        )
        .await
        .json()
        .await
        .unwrap();
        for name in [
            "config.async_limits.get",
            "config.async_limits.set_server",
            "config.async_limits.set_workforce",
        ] {
            assert!(
                admin_tools["result"]["tools"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|tool| tool["name"] == name)
            );
        }
        assert!(!admin_tools.to_string().contains("config.get"));

        let initial = call_tool(
            &client,
            &server,
            "mcp-config-admin-secret",
            &admin_session,
            "config.async_limits.get",
            json!({}),
        )
        .await;
        assert_eq!(initial["server"]["max_root_operations"], 4);

        let server_limits = json!({
            "max_root_operations":8,
            "max_child_workers":6,
            "max_queued_roots":10,
            "max_queue_wait_seconds":180
        });
        let updated = call_tool(
            &client,
            &server,
            "mcp-config-admin-secret",
            &admin_session,
            "config.async_limits.set_server",
            server_limits.clone(),
        )
        .await;
        assert_eq!(updated["server"], server_limits);

        let out_of_bounds = call_tool(
            &client,
            &server,
            "mcp-config-admin-secret",
            &admin_session,
            "config.async_limits.set_server",
            json!({
                "max_root_operations":257,
                "max_child_workers":6,
                "max_queued_roots":10,
                "max_queue_wait_seconds":180
            }),
        )
        .await;
        assert!(out_of_bounds["error"].as_str().unwrap().contains("maximum"));
        let unknown_field = call_tool(
            &client,
            &server,
            "mcp-config-admin-secret",
            &admin_session,
            "config.async_limits.set_server",
            json!({
                "max_root_operations":8,
                "max_child_workers":6,
                "max_queued_roots":10,
                "max_queue_wait_seconds":180,
                "ccm_json_path":"/tmp/config.json"
            }),
        )
        .await;
        assert!(
            unknown_field["error"]
                .as_str()
                .unwrap()
                .contains("not permitted")
        );

        let workforce_limits = json!({
            "max_root_operations":3,
            "max_child_workers":4,
            "max_queued_roots":5,
            "max_queue_wait_seconds":60
        });
        let workforce = call_tool(
            &client,
            &server,
            "mcp-config-admin-secret",
            &admin_session,
            "config.async_limits.set_workforce",
            json!({
                "workforce_id":"bootstrap-admin-workforce",
                "limits":workforce_limits
            }),
        )
        .await;
        assert_eq!(
            workforce["configured"],
            json!({
                "max_root_operations":3,
                "max_child_workers":4,
                "max_queued_roots":5,
                "max_queue_wait_seconds":60
            })
        );

        let partial_workforce = call_tool(
            &client,
            &server,
            "mcp-config-admin-secret",
            &admin_session,
            "config.async_limits.set_workforce",
            json!({
                "workforce_id":"bootstrap-admin-workforce",
                "limits":{"max_root_operations":2}
            }),
        )
        .await;
        assert_eq!(partial_workforce["configured"]["max_root_operations"], 2);
        assert_eq!(
            partial_workforce["configured"]["max_child_workers"],
            Value::Null
        );

        let too_large_for_server = call_tool(
            &client,
            &server,
            "mcp-config-admin-secret",
            &admin_session,
            "config.async_limits.set_workforce",
            json!({
                "workforce_id":"bootstrap-admin-workforce",
                "limits":{"max_root_operations":9}
            }),
        )
        .await;
        assert!(
            too_large_for_server["error"]
                .as_str()
                .unwrap()
                .contains("server limit")
        );

        let unknown_workforce = call_tool(
            &client,
            &server,
            "mcp-config-admin-secret",
            &admin_session,
            "config.async_limits.set_workforce",
            json!({
                "workforce_id":"no-such-workforce",
                "limits":{"max_root_operations":2}
            }),
        )
        .await;
        assert_eq!(unknown_workforce["error"], "unknown workforce");

        let oversized_id = call_tool(
            &client,
            &server,
            "mcp-config-admin-secret",
            &admin_session,
            "config.async_limits.set_workforce",
            json!({
                "workforce_id":"w".repeat(129),
                "limits":{}
            }),
        )
        .await;
        assert!(
            oversized_id["error"]
                .as_str()
                .unwrap()
                .contains("allowed length")
        );

        let current = call_tool(
            &client,
            &server,
            "mcp-config-admin-secret",
            &admin_session,
            "config.async_limits.get",
            json!({}),
        )
        .await;
        assert_eq!(current["server"], server_limits);
        assert_eq!(
            current["workforces"]["bootstrap-admin-workforce"]["max_root_operations"],
            2
        );
        assert_eq!(
            current["workforces"]["bootstrap-admin-workforce"]["max_child_workers"],
            Value::Null
        );
    }

    #[tokio::test]
    async fn async_operation_tools_enforce_key_ownership() {
        let state = test_state();
        let (owner, owner_token) = issue(&state, permissions(true, true, true, false, false));
        let (_, peer_token) = issue(&state, permissions(true, true, true, false, false));
        let scoped = state.with_auth(&owner);
        let (handle, mut operation) = scoped
            .portfolio
            .async_ops
            .start_root(
                scoped.portfolio.clone(),
                "mcp-operation-test".into(),
                0,
                async { Ok(json!({"private_result":"owner-only"})) },
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if matches!(
                    operation.borrow().status.as_str(),
                    "completed" | "error" | "cancelled" | "expired"
                ) {
                    break;
                }
                operation.changed().await.unwrap();
            }
        })
        .await
        .unwrap();

        let server = serve(state).await;
        let client = Client::new();
        let (owner_session, _) = initialize(&client, &server, &owner_token).await;
        let (peer_session, _) = initialize(&client, &server, &peer_token).await;
        let owned = call_tool(
            &client,
            &server,
            &owner_token,
            &owner_session,
            "operations.result",
            json!({"id":handle.id}),
        )
        .await;
        assert_eq!(owned["status"], "completed");
        assert_eq!(owned["result"]["private_result"], "owner-only");

        let foreign = call_tool(
            &client,
            &server,
            &peer_token,
            &peer_session,
            "operations.result",
            json!({"id":handle.id}),
        )
        .await;
        assert_eq!(foreign["error"], "unknown operation");
        let cancelled = call_tool(
            &client,
            &server,
            &owner_token,
            &owner_session,
            "operations.cancel",
            json!({"id":handle.id}),
        )
        .await;
        assert_eq!(cancelled["status"], "completed");
    }

    #[test]
    fn redaction_and_argument_validation_are_recursive_and_fail_closed() {
        let safe = redact_mcp(&json!({
            "nested":{"api_key":"do-not-show","authorization":"Bearer x"},
            "items":[{"secret":"also-secret","description":"safe"}]
        }));
        assert_eq!(safe["nested"]["api_key"], "[redacted]");
        assert_eq!(safe["items"][0]["secret"], "[redacted]");
        assert_eq!(safe["items"][0]["description"], "safe");

        let schema = object_schema(
            json!({"name":nonempty_string_schema(),"enabled":boolean_schema()}),
            &["name"],
        );
        assert!(validate_tool_arguments(&json!({"name":"ok"}), &schema, "args").is_ok());
        assert!(validate_tool_arguments(&json!({"name":" "}), &schema, "args").is_err());
        assert!(
            validate_tool_arguments(&json!({"name":"ok","admin":true}), &schema, "args").is_err()
        );
        assert!(validate_tool_arguments(&json!([]), &schema, "args").is_err());

        let oversized = tool_result_value(
            "test",
            Ok(json!({"payload":"x".repeat(MAX_TOOL_RESULT_BYTES)})),
        );
        assert_eq!(oversized["isError"], true);
        assert_eq!(
            oversized["structuredContent"]["error"],
            "MCP tool result exceeds the 1 MiB response limit"
        );
    }

    #[test]
    fn advertised_tools_have_permission_scoped_schemas() {
        let read = crate::auth::AuthContext {
            key_id: "read".into(),
            workforce_id: "wf".into(),
            permissions: permissions(true, true, false, true, false),
        };
        let tools = visible_tools(&read);
        assert!(tools.iter().any(|tool| tool["name"] == "workforce.get"));
        assert!(
            tools
                .iter()
                .any(|tool| tool["name"] == "improvements.suggestions")
        );
        assert!(
            tools
                .iter()
                .any(|tool| tool["name"] == "improvements.declassify")
        );
        assert!(
            !tools
                .iter()
                .any(|tool| tool["name"] == "workforce.evaluate")
        );
        assert!(!tools.iter().any(|tool| tool["name"] == "admin.keys.list"));
        assert!(
            tools
                .iter()
                .all(|tool| tool["inputSchema"]["type"] == "object")
        );
    }

    #[tokio::test]
    async fn mcp_suggestion_declassification_is_source_only_audited_and_does_not_apply() {
        let state = test_state();
        let (writer, writer_token) = state
            .portfolio
            .create_key(
                "MCP suggestion writer",
                None,
                "MCP suggestion workforce",
                &permissions(true, true, true, false, false),
            )
            .unwrap();
        let (source, source_token) = state
            .portfolio
            .create_key(
                "MCP suggestion source",
                Some(&writer.workforce_id),
                "MCP suggestion workforce",
                &permissions(true, false, false, true, false),
            )
            .unwrap();
        let (_other_source, other_source_token) = state
            .portfolio
            .create_key(
                "MCP other suggestion source",
                Some(&writer.workforce_id),
                "MCP suggestion workforce",
                &permissions(true, false, false, true, false),
            )
            .unwrap();
        let (_reader, reader_token) = state
            .portfolio
            .create_key(
                "MCP suggestion reader",
                Some(&writer.workforce_id),
                "MCP suggestion workforce",
                &permissions(true, true, false, false, false),
            )
            .unwrap();
        let writer_state = state.with_auth(&writer);
        let source_state = state.with_auth(&source);
        let agent = writer_state
            .portfolio
            .insert(&crate::portfolio::NewAgent {
                provider: String::new(),
                name: "suggestion target".into(),
                purpose: "writes accurate reports".into(),
                system_prompt: "original writer prompt".into(),
                model: "test-model".into(),
            })
            .unwrap();
        let suggestion_id = "mcp-private-revision";
        let share_id = "mcp-opaque-share-handle";
        let private_marker = "mcp-private-generated-system-prompt";
        let private_reason = "mcp-private-evaluator-reason";
        let private_evidence = "mcp-private-evaluator-evidence";
        source_state
            .portfolio
            .conn
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO improvement_suggestions(id,job_id,checkpoint_id,conversation_id,workforce_id,
                 owner_key_id,entity_kind,entity_id,action,proposal,before_definition,status,created_ms,share_id)
                 VALUES(?1,'private-job','private-checkpoint','private-conversation',?2,?3,'agent',?4,
                 'revise',?5,?6,'pending',?7,?8)",
                rusqlite::params![
                    suggestion_id,
                    source.workforce_id,
                    source.key_id,
                    agent.id,
                    json!({"action":"revise","reason":private_reason,"evidence":private_evidence,
                        "replacement":{"provider":"","name":"released revision","purpose":"write accurately",
                        "system_prompt":private_marker,"model":"test-model"}}).to_string(),
                    json!(agent).to_string(),
                    crate::observe::now_ms(),
                    share_id
                ],
            )
            .unwrap();

        let server = serve(state.clone()).await;
        let client = Client::new();
        let (source_session, _) = initialize(&client, &server, &source_token).await;
        let (writer_session, _) = initialize(&client, &server, &writer_token).await;
        let (other_source_session, _) = initialize(&client, &server, &other_source_token).await;
        let (reader_session, _) = initialize(&client, &server, &reader_token).await;

        let writer_tools: Value = rpc(
            &client,
            &server,
            &writer_token,
            &writer_session,
            "tools/list",
            json!("writer-tools"),
            json!({}),
        )
        .await
        .json()
        .await
        .unwrap();
        assert!(
            writer_tools["result"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .all(|tool| tool["name"] != "improvements.declassify")
        );
        let reader_tools: Value = rpc(
            &client,
            &server,
            &reader_token,
            &reader_session,
            "tools/list",
            json!("reader-tools"),
            json!({}),
        )
        .await
        .json()
        .await
        .unwrap();
        assert!(
            reader_tools["result"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .all(|tool| tool["name"] != "improvements.declassify")
        );

        let source_suggestions = call_tool(
            &client,
            &server,
            &source_token,
            &source_session,
            "improvements.suggestions",
            json!({}),
        )
        .await;
        assert_eq!(source_suggestions["items"][0]["id"], suggestion_id);
        assert_eq!(
            source_suggestions["items"][0]["proposal"]["replacement"]["system_prompt"],
            private_marker
        );
        let source_apply_denied = call_tool(
            &client,
            &server,
            &source_token,
            &source_session,
            "improvements.apply",
            json!({"id":suggestion_id}),
        )
        .await;
        assert_eq!(
            source_apply_denied["error"],
            "bearer key lacks the required permission"
        );
        assert!(!source_apply_denied.to_string().contains(private_marker));
        assert!(!source_apply_denied.to_string().contains(private_reason));
        assert!(!source_apply_denied.to_string().contains(private_evidence));
        let suggestion_status: String = source_state
            .portfolio
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT status FROM improvement_suggestions WHERE id=?1",
                [suggestion_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(suggestion_status, "pending");

        let writer_before = call_tool(
            &client,
            &server,
            &writer_token,
            &writer_session,
            "improvements.suggestions",
            json!({}),
        )
        .await;
        assert_eq!(writer_before["items"].as_array().unwrap().len(), 1);
        assert_eq!(writer_before["items"][0]["id"], share_id);
        assert_eq!(
            writer_before["items"][0]["proposed_definition"],
            Value::Null
        );
        assert_eq!(writer_before["items"][0]["declassification_required"], true);
        assert!(!writer_before.to_string().contains(private_marker));
        assert!(!writer_before.to_string().contains(private_reason));
        assert!(!writer_before.to_string().contains(private_evidence));

        let writer_denied = call_tool(
            &client,
            &server,
            &writer_token,
            &writer_session,
            "improvements.declassify",
            json!({"id":suggestion_id}),
        )
        .await;
        assert_eq!(
            writer_denied["error"],
            "bearer key lacks the required permission"
        );
        let other_source_denied = call_tool(
            &client,
            &server,
            &other_source_token,
            &other_source_session,
            "improvements.declassify",
            json!({"id":suggestion_id}),
        )
        .await;
        assert_eq!(
            other_source_denied["error"],
            "unknown, non-pending, or already released suggestion"
        );
        assert!(!other_source_denied.to_string().contains(private_marker));

        let released = call_tool(
            &client,
            &server,
            &source_token,
            &source_session,
            "improvements.declassify",
            json!({"id":suggestion_id}),
        )
        .await;
        assert_eq!(released["result"]["id"], suggestion_id);
        assert_eq!(released["result"]["status"], "pending");
        assert_eq!(released["result"]["declassified"], true);
        assert!(released["request_id"].as_str().is_some());
        assert!(!released.to_string().contains(private_marker));
        assert!(!released.to_string().contains(private_reason));
        assert!(!released.to_string().contains(private_evidence));

        let request = released["request_id"].as_str().unwrap();
        let (run, _) = source_state
            .portfolio
            .trace_context(request)
            .unwrap()
            .unwrap();
        let source_events = source_state.portfolio.trace_events(0, Some(&run)).unwrap();
        let audit = source_events
            .iter()
            .find(|event| event["kind"] == "improvement.suggestion_declassified")
            .expect("source release should be audited");
        assert_eq!(audit["data"]["suggestion_id"], suggestion_id);
        assert_eq!(audit["data"]["scope"], "same-workforce roster writers");
        assert!(!audit.to_string().contains(private_marker));
        assert!(!audit.to_string().contains(private_reason));
        assert!(!audit.to_string().contains(private_evidence));
        assert!(
            writer_state
                .portfolio
                .trace_events(0, Some(&run))
                .unwrap()
                .iter()
                .all(|event| event["kind"] != "improvement.suggestion_declassified")
        );

        let writer_after = call_tool(
            &client,
            &server,
            &writer_token,
            &writer_session,
            "improvements.suggestions",
            json!({}),
        )
        .await;
        assert_eq!(
            writer_after["items"][0]["proposed_definition"]["system_prompt"],
            private_marker
        );
        assert_eq!(writer_after["items"][0]["declassification_required"], false);
        assert_eq!(writer_after["items"][0]["status"], "pending");
        assert!(!writer_after.to_string().contains(private_reason));
        assert!(!writer_after.to_string().contains(private_evidence));
        let reader_suggestions = call_tool(
            &client,
            &server,
            &reader_token,
            &reader_session,
            "improvements.suggestions",
            json!({}),
        )
        .await;
        assert!(reader_suggestions["items"].as_array().unwrap().is_empty());
        assert_eq!(
            writer_state
                .portfolio
                .get(&agent.id)
                .unwrap()
                .unwrap()
                .system_prompt,
            "original writer prompt"
        );
    }

    #[tokio::test]
    async fn mcp_create_suggestion_apply_requires_release_and_replays_source_receipt() {
        let generated = json!({"name":"mcp_generated_writer","purpose":"writes reusable reports",
            "provisions":{"identity":"A writing specialist","mission":"Write accurately",
            "method":"Check claims against supplied evidence","boundaries":"Do not fabricate",
            "task_context":"Use the caller's task","capabilities":"Only supplied tools",
            "output_contract":"Return a concise report"}});
        let (upstream, script) =
            serve_mock_upstream(vec![assistant_response(&generated.to_string())]).await;
        let state = test_state_at(&upstream.url);
        let (writer, writer_token) = state
            .portfolio
            .create_key(
                "MCP create apply writer",
                None,
                "MCP create apply workforce",
                &permissions(true, true, true, false, false),
            )
            .unwrap();
        let (source, source_token) = state
            .portfolio
            .create_key(
                "MCP create apply source",
                Some(&writer.workforce_id),
                "MCP create apply workforce",
                &permissions(true, false, false, true, false),
            )
            .unwrap();
        let job_id = "mcp-create-apply-job";
        let suggestion_id = "mcp-private-create-suggestion";
        let share_id = "mcp-opaque-create-handle";
        let conversation_id = "mcp-private-create-conversation";
        let checkpoint_id = "mcp-private-create-checkpoint";
        let requirements = "MCP_PRIVATE_CREATE_REQUIREMENTS_CANARY reusable writing agent";
        let proposal = json!({"action":"create","kind":"agent","requirements":requirements});
        let now = crate::observe::now_ms();
        let epoch: i64 = state
            .portfolio
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT value FROM improvement_epoch WHERE id=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        {
            let conn = state.portfolio.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO improvement_jobs(id,origin_request,origin_run,entity_kind,entity_id,
                 definition,task,result,epoch,status,created_ms,updated_ms,workforce_id,owner_key_id,
                 auto_apply_private,conversation_id,checkpoint_id,job_type)
                 VALUES(?1,'mcp-create-origin-request','mcp-create-origin-run','agent','observed-agent',
                 '{}','task','result',?2,'suggested',?3,?3,?4,?5,0,?6,?7,'response')",
                rusqlite::params![
                    job_id,
                    epoch,
                    now,
                    source.workforce_id,
                    source.key_id,
                    conversation_id,
                    checkpoint_id
                ],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO workforce_plans(job_id,plan,workforce_id,owner_key_id)
                 VALUES(?1,?2,?3,?4)",
                rusqlite::params![
                    job_id,
                    json!([{"action":"create","kind":"agent","requirements":requirements}])
                        .to_string(),
                    source.workforce_id,
                    source.key_id
                ],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO improvement_suggestions(id,job_id,checkpoint_id,conversation_id,
                 workforce_id,owner_key_id,entity_kind,entity_id,action,proposal,before_definition,
                 status,created_ms,share_id,idempotency_key)
                 VALUES(?1,?2,?3,?4,?5,?6,'agent','','create',?7,'null','pending',?8,?9,?10)",
                rusqlite::params![
                    suggestion_id,
                    job_id,
                    checkpoint_id,
                    conversation_id,
                    source.workforce_id,
                    source.key_id,
                    proposal.to_string(),
                    now,
                    share_id,
                    format!("{job_id}:create:agent:0")
                ],
            )
            .unwrap();
        }

        let server = serve(state.clone()).await;
        let client = Client::new();
        let (writer_session, _) = initialize(&client, &server, &writer_token).await;
        let (source_session, _) = initialize(&client, &server, &source_token).await;

        let shared_before = call_tool(
            &client,
            &server,
            &writer_token,
            &writer_session,
            "improvements.suggestions",
            json!({}),
        )
        .await;
        assert_eq!(shared_before["items"][0]["id"], share_id);
        assert_eq!(shared_before["items"][0]["requirements"], Value::Null);
        assert!(!shared_before.to_string().contains(requirements));

        let denied: Value = rpc(
            &client,
            &server,
            &writer_token,
            &writer_session,
            "tools/call",
            json!("unreleased-create-apply"),
            json!({"name":"improvements.apply","arguments":{"id":share_id}}),
        )
        .await
        .json()
        .await
        .unwrap();
        assert_eq!(denied["result"]["isError"], true);
        assert_eq!(
            denied["result"]["structuredContent"]["error"],
            "source key release is required before cross-key creation apply"
        );
        for private in [
            requirements,
            suggestion_id,
            job_id,
            conversation_id,
            checkpoint_id,
        ] {
            assert!(!denied.to_string().contains(private));
        }
        assert!(script.lock().unwrap().1.is_empty());

        let released = call_tool(
            &client,
            &server,
            &source_token,
            &source_session,
            "improvements.declassify",
            json!({"id":suggestion_id}),
        )
        .await;
        assert_eq!(released["result"]["declassified"], true);
        assert!(!released.to_string().contains(requirements));

        let shared_after = call_tool(
            &client,
            &server,
            &writer_token,
            &writer_session,
            "improvements.suggestions",
            json!({}),
        )
        .await;
        assert_eq!(shared_after["items"][0]["requirements"], requirements);
        assert_eq!(shared_after["items"][0]["declassification_required"], false);

        let applied_rpc: Value = rpc(
            &client,
            &server,
            &writer_token,
            &writer_session,
            "tools/call",
            json!("released-create-apply"),
            json!({"name":"improvements.apply","arguments":{"id":share_id}}),
        )
        .await
        .json()
        .await
        .unwrap();
        let applied = &applied_rpc["result"]["structuredContent"]["result"];
        let initial_fields = applied
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            initial_fields,
            [
                "id",
                "status",
                "action",
                "kind",
                "entity_id",
                "applied",
                "receipt"
            ]
            .into_iter()
            .collect()
        );
        assert_eq!(applied["id"], share_id);
        assert_eq!(applied["status"], "applied");
        assert_eq!(applied["action"], "create");
        assert_eq!(applied["applied"], true);
        assert_eq!(applied["receipt"], true);
        assert!(applied.get("suggestion_id").is_none());
        assert!(applied.get("job_status").is_none());
        for private in [
            suggestion_id,
            job_id,
            conversation_id,
            checkpoint_id,
            "mcp-create-origin-request",
            "mcp-create-origin-run",
            source.key_id.as_str(),
        ] {
            assert!(!applied_rpc.to_string().contains(private));
        }
        let entity_id = applied["entity_id"].as_str().unwrap();
        let provider_requests = script.lock().unwrap().1.clone();
        assert_eq!(provider_requests.len(), 1);
        let provider_request = provider_requests[0].to_string();
        assert!(provider_request.contains(requirements));
        assert!(!provider_request.contains(job_id));
        assert!(!provider_request.contains(conversation_id));
        assert!(!provider_request.contains(checkpoint_id));

        let replay_rpc: Value = rpc(
            &client,
            &server,
            &writer_token,
            &writer_session,
            "tools/call",
            json!("released-create-replay"),
            json!({"name":"improvements.apply","arguments":{"id":share_id}}),
        )
        .await
        .json()
        .await
        .unwrap();
        let replay = &replay_rpc["result"]["structuredContent"]["result"];
        let replay_fields = replay
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            replay_fields,
            [
                "id",
                "status",
                "action",
                "kind",
                "entity_id",
                "applied",
                "receipt"
            ]
            .into_iter()
            .collect()
        );
        assert_eq!(replay["id"], share_id);
        assert_eq!(replay["receipt"], true);
        assert_eq!(replay["entity_id"], entity_id);
        assert!(replay.get("suggestion_id").is_none());
        assert!(replay.get("job_status").is_none());
        for private in [
            suggestion_id,
            job_id,
            conversation_id,
            checkpoint_id,
            "mcp-create-origin-request",
            "mcp-create-origin-run",
            source.key_id.as_str(),
        ] {
            assert!(!replay_rpc.to_string().contains(private));
        }
        assert_eq!(script.lock().unwrap().1.len(), 1);

        let writer_after = call_tool(
            &client,
            &server,
            &writer_token,
            &writer_session,
            "improvements.suggestions",
            json!({}),
        )
        .await;
        assert!(writer_after["items"].as_array().unwrap().is_empty());
        assert!(!writer_after.to_string().contains(suggestion_id));
        let source_after = call_tool(
            &client,
            &server,
            &source_token,
            &source_session,
            "improvements.suggestions",
            json!({}),
        )
        .await;
        assert_eq!(source_after["items"][0]["id"], suggestion_id);
        assert_eq!(source_after["items"][0]["status"], "applied");

        let conn = state.portfolio.conn.lock().unwrap();
        let (receipt_owner, receipt_workforce, receipt_id, suggestion_status, job_status, count): (
            String,
            String,
            String,
            String,
            String,
            i64,
        ) = conn
            .query_row(
                "SELECT r.owner_key_id,r.workforce_id,r.entity_id,s.status,j.status,
                 (SELECT COUNT(*) FROM creation_receipts WHERE job_id=?1)
                 FROM creation_receipts r
                 JOIN improvement_suggestions s ON s.job_id=r.job_id
                 JOIN improvement_jobs j ON j.id=r.job_id
                 WHERE r.job_id=?1 AND r.order_index=0 AND s.id=?2",
                rusqlite::params![job_id, suggestion_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(receipt_owner, source.key_id);
        assert_eq!(receipt_workforce, source.workforce_id);
        assert_eq!(receipt_id, entity_id);
        assert_eq!(suggestion_status, "applied");
        assert_eq!(job_status, "completed");
        assert_eq!(count, 1);
    }
}
