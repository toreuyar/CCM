//! Isolated HTTP scenarios: no live provider, user database, or server restart.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::{
    Json, Router,
    body::Body,
    extract::State,
    response::{IntoResponse, Response},
    routing::post,
};
#[path = "scenarios_async.rs"]
mod async_execution;
#[path = "scenarios_auth.rs"]
mod auth;
#[path = "scenarios_contracts.rs"]
mod contracts;
#[path = "scenarios_foreman_protocol.rs"]
mod foreman_protocol;
#[path = "scenarios_improvement.rs"]
mod improvement;
#[path = "scenarios_lifecycle.rs"]
mod lifecycle;
#[path = "scenarios_models.rs"]
mod model_routing;
#[path = "scenarios_more.rs"]
mod more;
#[path = "scenarios_observe.rs"]
mod observe;
#[path = "scenarios_policy.rs"]
mod policy;
#[path = "scenarios_transparency.rs"]
mod transparency;
#[path = "scenarios_upstream.rs"]
mod upstream_reliability;
#[path = "scenarios_workforce.rs"]
mod workforce;
use serde_json::{Value, json};

use crate::{
    config::Config,
    handlers,
    portfolio::{NewAgent, Portfolio},
    state::AppState,
};

struct Server {
    url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(router: Router) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    Server { url, task }
}

type Script = Arc<Mutex<(VecDeque<Value>, Vec<Value>)>>;

async fn scripted(State(script): State<Script>, Json(body): Json<Value>) -> Response {
    let mut script = script.lock().unwrap();
    script.1.push(body);
    let response = script.0.pop_front().expect("unexpected upstream call");
    if response["__stream_delay"] == true {
        let stream = async_stream::stream! {
            yield Ok::<_,std::io::Error>(bytes::Bytes::from_static(b"data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n"));
            tokio::time::sleep(Duration::from_secs(5)).await;
            yield Ok(bytes::Bytes::from_static(b"data: [DONE]\n\n"));
        };
        return (
            [("content-type", "text/event-stream")],
            Body::from_stream(stream),
        )
            .into_response();
    }
    if let Some(text) = response.as_str() {
        return ([("content-type", "text/event-stream")], text.to_string()).into_response();
    }
    if let Some(status) = response["__status"].as_u64() {
        return (
            axum::http::StatusCode::from_u16(status as u16).unwrap(),
            Json(response),
        )
            .into_response();
    }
    Json(response).into_response()
}

fn message(content: &str) -> Value {
    json!({"choices":[{"message":{"role":"assistant","content":content},"finish_reason":"stop"}]})
}

fn keep() -> Value {
    message(
        r#"{"action":"keep","reason":"meets requirements","evidence":"observed result satisfies task"}"#,
    )
}
fn definition() -> Value {
    json!({"name":"writer","purpose":"writes","provisions":{
        "identity":"Writing specialist","mission":"Write accurately","method":"write carefully",
        "boundaries":"Do not fabricate; explain uncertainty","task_context":"Read runtime conversation",
        "capabilities":"Only supplied tools","output_contract":"Return a report"}})
}

async fn upstream(responses: Vec<Value>) -> (Server, Script) {
    let script = Arc::new(Mutex::new((responses.into(), vec![])));
    let server = serve(
        Router::new()
            .route("/chat/completions", post(scripted))
            .with_state(script.clone()),
    )
    .await;
    (server, script)
}

fn state(url: &str, debug: bool) -> Arc<AppState> {
    Arc::new(AppState {
        providers: Arc::new(crate::provider_gate::Registry::new(
            &std::collections::BTreeMap::from([(
                String::new(),
                crate::model_config::Provider {
                    recovery: crate::provider_gate::Recovery {
                        cooldown_ms: 1,
                        max_cooldown_ms: 4,
                    },
                    ..Default::default()
                },
            )]),
        )),
        config: Arc::new(Config {
            routing: Default::default(),
            api_key: "test".into(),
            upstream_base_url: url.into(),
            model: "test-model".into(),
            host: "127.0.0.1".into(),
            port: 0,
            upstream: crate::config::UpstreamConfig {
                first_response_timeout_ms: 400,
                idle_timeout_ms: 400,
                total_timeout_ms: 1000,
                max_attempts: 1,
                ..Default::default()
            },
            portfolio_db: ":memory:".into(),
            max_iterations: 2,
            debug_enabled: debug,
            personality_transparency: crate::config::PersonalityTransparency::default(),
            memory: crate::config::MemoryConfig::default(),
            shutdown: crate::config::ShutdownConfig::default(),
            creation: crate::config::CreationConfig {
                policy: crate::policy::CreationPolicy::AlwaysPreExecution,
                ..Default::default()
            },
            improvement: crate::improvement::ImprovementConfig::default(),
        }),
        http: reqwest::Client::new(),
        portfolio: Arc::new(Portfolio::open(":memory:").unwrap()),
        external_api_enabled: true,
        auth: None,
    })
}

async fn proxy(state: Arc<AppState>) -> Server {
    serve(
        Router::new()
            .route("/v1/chat/completions", post(handlers::chat_completions))
            .route("/v1/models", axum::routing::get(handlers::list_models))
            .with_state(state),
    )
    .await
}

async fn request(server: &Server, content: &str, stream: bool) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{}/v1/chat/completions", server.url))
        .json(
            &json!({"model":"test-model","stream":stream,"messages":[{"role":"user","content":content}]}),
        )
        .send()
        .await
        .unwrap()
}

fn insert(state: &AppState) -> crate::portfolio::Agent {
    state
        .portfolio
        .insert(&NewAgent {
            provider: String::new(),
            name: "writer".into(),
            purpose: "writes".into(),
            system_prompt: "Specialist instructions with scope evidence".into(),
            model: "test-model".into(),
        })
        .unwrap()
}

#[tokio::test]
async fn existing_agent_report_returns_to_composition() {
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    script.lock().unwrap().0 = vec![
        message(&json!({"decision":"use","agent_id":agent.id,"note":"fits"}).to_string()),
        message("Specialist report"),
        message("Composed result"),
    ]
    .into();
    let proxy = proxy(state).await;
    let response = request(&proxy, "write", false).await;
    assert!(response.status().is_success());
    assert_eq!(response.headers()["x-ccm-route"], "dispatch");
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "Composed result");
    let script = script.lock().unwrap();
    assert_eq!(script.1.len(), 3);
    assert!(
        script.1[0]["messages"][0]["content"]
            .as_str()
            .unwrap()
            .contains("scope evidence")
    );
    let composition = script.1[2]["messages"].as_array().unwrap().last().unwrap()["content"]
        .as_str()
        .unwrap();
    assert!(composition.contains("Specialist report"));
    assert!(composition.contains("do not evaluate or second-guess"));
}

#[tokio::test]
async fn new_agent_is_stored_and_used() {
    let (upstream, script) = upstream(vec![
        message(r#"{"decision":"create","requirements":"writing specialist","note":"new"}"#),
        message(&definition().to_string()),
        message("report"),
        message("answer"),
    ])
    .await;
    let state = state(&upstream.url, false);
    let proxy = proxy(state.clone()).await;
    assert!(request(&proxy, "write", false).await.status().is_success());
    assert_eq!(state.portfolio.list().unwrap().len(), 1);
    assert!(
        script.lock().unwrap().1[2]["messages"][0]["content"]
            .as_str()
            .unwrap()
            .contains("write carefully")
    );
}

#[tokio::test]
async fn unknown_agent_fails_without_direct_answer() {
    let (upstream, script) = upstream(vec![
        message(r#"{"decision":"use","agent_id":"missing"}"#);
        3
    ])
    .await;
    let proxy = proxy(state(&upstream.url, false)).await;
    assert_eq!(request(&proxy, "write", false).await.status(), 503);
    assert_eq!(script.lock().unwrap().1.len(), 3);
}

#[tokio::test]
async fn debug_disabled_dispatches_without_deleting() {
    let (upstream, script) = upstream(vec![message("invalid decision"); 3]).await;
    let state = state(&upstream.url, false);
    insert(&state);
    let proxy = proxy(state.clone()).await;
    assert_eq!(request(&proxy, "debug-delete", false).await.status(), 503);
    assert_eq!(state.portfolio.list().unwrap().len(), 1);
    assert_eq!(script.lock().unwrap().1.len(), 3);
}

#[tokio::test]
async fn debug_nonstream_returns_programmatic_completion() {
    let state = state("http://127.0.0.1:1", true);
    insert(&state);
    let proxy = proxy(state.clone()).await;
    let response = request(&proxy, "debug-delete", false).await;
    assert_eq!(response.headers()["x-ccm-route"], "debug");
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["object"], "chat.completion");
    let result: Value =
        serde_json::from_str(body["choices"][0]["message"]["content"].as_str().unwrap()).unwrap();
    assert_eq!(result["deleted"]["agents"], 1);
    assert!(state.portfolio.list().unwrap().is_empty());
}

#[tokio::test]
async fn debug_stream_returns_content_stop_and_done() {
    let state = state("http://127.0.0.1:1", true);
    insert(&state);
    let proxy = proxy(state).await;
    let response = request(&proxy, "debug-delete", true).await;
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    let body = response.text().await.unwrap();
    let frames: Vec<_> = body.split("\n\n").filter(|f| !f.is_empty()).collect();
    assert_eq!(frames.len(), 3);
    let chunk: Value = serde_json::from_str(frames[0].strip_prefix("data: ").unwrap()).unwrap();
    let result: Value =
        serde_json::from_str(chunk["choices"][0]["delta"]["content"].as_str().unwrap()).unwrap();
    assert_eq!(result["deleted"]["agents"], 1);
    let stop: Value = serde_json::from_str(frames[1].strip_prefix("data: ").unwrap()).unwrap();
    assert_eq!(stop["choices"][0]["finish_reason"], "stop");
    assert_eq!(frames[2], "data: [DONE]");
}

async fn stalled_body() -> Response {
    let stream = async_stream::stream! {
        yield Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"{"));
        tokio::time::sleep(Duration::from_secs(5)).await;
        yield Ok(bytes::Bytes::from_static(b"}"));
    };
    Response::new(Body::from_stream(stream))
}

#[tokio::test]
async fn foreman_budget_includes_response_body() {
    let upstream = serve(Router::new().route("/chat/completions", post(stalled_body))).await;
    let state = state(&upstream.url, false);
    let started = Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        crate::foreman::foreman_call(&state, "empty", "task"),
    )
    .await;
    assert!(
        result
            .expect("response body escaped the request budget")
            .is_err()
    );
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn specialist_budgets_include_response_body() {
    let upstream = serve(Router::new().route("/chat/completions", post(stalled_body))).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    let report = tokio::time::timeout(
        Duration::from_secs(2),
        crate::loop_runner::agent_call(&state, &agent, &[], &json!({}), "timeout-test"),
    )
    .await;
    assert!(report.expect("report body escaped budget").is_err());
}

fn tool_payload() -> Value {
    json!({"model":"test-model","messages":[{"role":"user","content":"look it up"}],"tools":[
        {"type":"function","function":{"name":"lookup","description":"client tool","parameters":{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}}}
    ]})
}

fn tool_message() -> Value {
    json!({"choices":[{"message":{"role":"assistant","content":null,"tool_calls":[
        {"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"rust\"}"}}
    ]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":11,"completion_tokens":7}})
}

fn workflow(agent: &str) -> crate::workflow::Workflow {
    serde_json::from_value(json!({"id":uuid::Uuid::new_v4().to_string(),"name":"review flow","purpose":"produce and check",
        "entry":"work","max_steps":4,"stages":[
            {"id":"work","agent_id":agent,"instruction":"produce JSON with done field","branches":[{"pointer":"/done","equals":true,"next":"finish"}],"next":"work"},
            {"id":"finish","agent_id":agent,"instruction":"summarize previous outputs","next":null}
        ]})).unwrap()
}

#[tokio::test]
async fn client_tools_roundtrip_through_specialist_only() {
    let _tooling_config_guard = crate::config::lock_tooling_config_for_test().await;
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    crate::config::set_test_tooling_config(crate::config::ToolingConfig {
        workforces: std::collections::BTreeMap::from([(
            "anonymous".into(),
            crate::config::WorkforceToolPolicy {
                default: crate::config::ToolPermission::Deny,
                grants: vec![crate::config::ToolGrant {
                    tool: "file.read".into(),
                    decision: crate::config::ToolPermission::Allow,
                    agent_id: Some(agent.id.clone()),
                    stage_id: None,
                }],
            },
        )]),
        ..Default::default()
    });
    script.lock().unwrap().0 = vec![
        message(&json!({"decision":"use","agent_id":agent.id}).to_string()),
        tool_message(),
        message("result after tool observation"),
        message("user result"),
    ]
    .into();
    let proxy = proxy(state.clone()).await;
    let client = reqwest::Client::new();
    let mut payload = tool_payload();
    let first: Value = client
        .post(format!("{}/v1/chat/completions", proxy.url))
        .json(&payload)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(first["choices"][0]["finish_reason"], "tool_calls");
    let assistant = first["choices"][0]["message"].clone();
    let id = assistant["tool_calls"][0]["id"].clone();
    payload["messages"].as_array_mut().unwrap().extend([
        assistant,
        json!({"role":"tool","tool_call_id":id,"content":"tool observation"}),
    ]);
    let response = client
        .post(format!("{}/v1/chat/completions", proxy.url))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "user result");
    let script = script.lock().unwrap();
    assert_eq!(script.1.len(), 4);
    assert_eq!(script.1[1]["tools"][0], payload["tools"][0]);
    assert!(script.1[1]["tools"].as_array().unwrap().iter().any(|tool| {
        tool.pointer("/function/name").and_then(Value::as_str) == Some("ccm_file_read")
    }));
    assert_eq!(
        script.1[2]["messages"].as_array().unwrap().last().unwrap()["content"],
        "tool observation"
    );
    assert!(script.1[3].get("tools").is_none());
    let stats = state.portfolio.statistics().unwrap();
    assert!(
        stats["groups"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["kind"] == "agent_call" && r["prompt_tokens"] == 11)
    );
}

#[tokio::test]
async fn tool_calls_stream_with_protocol_indexes() {
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    script.lock().unwrap().0 = vec![
        message(&json!({"decision":"use","agent_id":agent.id}).to_string()),
        tool_message(),
    ]
    .into();
    let proxy = proxy(state).await;
    let mut payload = tool_payload();
    payload["stream"] = json!(true);
    let response = reqwest::Client::new()
        .post(format!("{}/v1/chat/completions", proxy.url))
        .json(&payload)
        .send()
        .await
        .unwrap();
    let text = response.text().await.unwrap();
    let chunk = text
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter_map(|raw| serde_json::from_str::<Value>(raw).ok())
        .find(|chunk| chunk.pointer("/choices/0/delta/tool_calls").is_some())
        .unwrap();
    assert_eq!(chunk["choices"][0]["delta"]["tool_calls"][0]["index"], 0);
    assert!(text.contains("\"finish_reason\":\"tool_calls\""));
    assert!(text.ends_with("data: [DONE]\n\n"));
}

#[tokio::test]
async fn undeclared_tool_is_rejected() {
    let (upstream, _) = upstream(vec![tool_message()]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    assert!(
        crate::loop_runner::agent_call(&state, &agent, &[], &json!({}), "test")
            .await
            .unwrap_err()
            .contains("undeclared tool lookup")
    );
}

#[tokio::test]
async fn workflow_branches_loops_and_passes_outputs() {
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    let workflow = workflow(&agent.id);
    state.portfolio.workflow_insert(&workflow, 0).unwrap();
    script.lock().unwrap().0 = vec![
        message(&json!({"decision":"use_workflow","workflow_id":workflow.id}).to_string()),
        message(r#"{"done":false,"finding":"try again"}"#),
        message(r#"{"done":true}"#),
        message("final stage report"),
    ]
    .into();
    let output = crate::runtime::dispatch(&state, &tool_payload(), "flow")
        .await
        .unwrap();
    assert!(output.message.is_none());
    assert!(output.report.contains("final stage report"));
    let snapshot = state.portfolio.workforce(crate::observe::now_ms()).unwrap();
    for (id, assignments) in [(&agent.id, 3), (&workflow.id, 1)] {
        let entity = snapshot["entities"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["id"] == *id)
            .unwrap();
        assert_eq!(entity["usage"]["1h"]["assignment"], assignments);
        assert_eq!(entity["usage"]["1h"]["model_call"], 3);
    }
    let script = script.lock().unwrap();
    assert!(
        script.1[2]["messages"].as_array().unwrap().last().unwrap()["content"]
            .as_str()
            .unwrap()
            .contains("try again")
    );
    assert!(
        state
            .portfolio
            .evaluations()
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty()
    );
    let (run, _) = state.portfolio.trace_context("flow").unwrap().unwrap();
    assert_eq!(
        state
            .portfolio
            .improvements(&run)
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        4
    );
}

#[tokio::test]
async fn workflow_cycle_stops_at_budget() {
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    let mut workflow = workflow(&agent.id);
    workflow.max_steps = 2;
    state.portfolio.workflow_insert(&workflow, 0).unwrap();
    script.lock().unwrap().0 = vec![
        message(&json!({"decision":"use_workflow","workflow_id":workflow.id}).to_string()),
        message(r#"{"done":false}"#),
        message(r#"{"done":false}"#),
    ]
    .into();
    let error = crate::runtime::dispatch(&state, &tool_payload(), "bounded")
        .await
        .err()
        .unwrap();
    assert!(error.contains("step budget"));
    assert_eq!(script.lock().unwrap().1.len(), 3);
    assert!(
        state
            .portfolio
            .statistics()
            .unwrap()
            .to_string()
            .contains("error")
    );
}

#[tokio::test]
async fn workflow_tool_pause_resumes_without_foreman() {
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    let mut w = workflow(&agent.id);
    w.stages.truncate(1);
    w.stages[0].branches.clear();
    w.stages[0].next = None;
    state.portfolio.workflow_insert(&w, 0).unwrap();
    script.lock().unwrap().0 = vec![
        message(&json!({"decision":"use_workflow","workflow_id":w.id}).to_string()),
        tool_message(),
        message("observed tool result"),
    ]
    .into();
    let mut payload = tool_payload();
    let first = crate::runtime::dispatch(&state, &payload, "pause")
        .await
        .unwrap();
    let assistant = first.message.unwrap();
    let id = assistant["tool_calls"][0]["id"].clone();
    payload["messages"].as_array_mut().unwrap().extend([
        assistant,
        json!({"role":"tool","tool_call_id":id,"content":"external observation"}),
    ]);
    let second = crate::runtime::dispatch(&state, &payload, "resume")
        .await
        .unwrap();
    assert!(second.report.contains("observed tool result"));
    let snapshot = state.portfolio.workforce(crate::observe::now_ms()).unwrap();
    for id in [&agent.id, &w.id] {
        let entity = snapshot["entities"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["id"] == *id)
            .unwrap();
        assert_eq!(entity["usage"]["1h"]["assignment"], 1);
        assert_eq!(entity["usage"]["1h"]["model_call"], 2);
    }
    assert_eq!(script.lock().unwrap().1.len(), 3);
    // A duplicate result cannot execute the completed workflow again.
    assert!(
        crate::runtime::dispatch(&state, &payload, "duplicate")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn workflow_generator_output_is_validated_and_invoked() {
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    let mut w = workflow(&agent.id);
    w.stages.truncate(1);
    w.stages[0].branches.clear();
    w.stages[0].next = None;
    script.lock().unwrap().0 = vec![
        message(r#"{"decision":"create_workflow","requirements":"single explicit stage"}"#),
        message(&json!(w).to_string()),
        message("stage report"),
    ]
    .into();
    let output = crate::runtime::dispatch(&state, &tool_payload(), "generate-flow")
        .await
        .unwrap();
    assert!(output.report.contains("stage report"));
    assert_eq!(state.portfolio.workflow_list().unwrap().len(), 1);
    let script = script.lock().unwrap();
    let prompt = script.1[1]["messages"][0]["content"].as_str().unwrap();
    for framework in ["ReAct", "PDCA", "OODA", "MAPE-K"] {
        assert!(prompt.contains(framework));
    }
}

#[tokio::test]
async fn malformed_generated_agent_is_not_stored() {
    let (upstream, _) = upstream(vec![message(
        r#"{"name":"a","purpose":"p","provisions":{}}"#,
    )])
    .await;
    let state = state(&upstream.url, false);
    assert!(
        crate::meta::create_agent(&state, "invalid", "write", 0)
            .await
            .is_err()
    );
    assert!(state.portfolio.list().unwrap().is_empty());
}

#[tokio::test]
async fn generated_workflow_cannot_invent_agent_ids() {
    let w = workflow("invented");
    let (upstream, _) = upstream(vec![message(&json!(w).to_string())]).await;
    let state = state(&upstream.url, false);
    assert!(
        crate::meta::create_workflow(&state, "invalid", "flow", 0)
            .await
            .is_err()
    );
    assert!(state.portfolio.workflow_list().unwrap().is_empty());
}

#[tokio::test]
async fn evaluator_revises_agent_and_records_history() {
    let replacement = definition();
    let (upstream,_)=upstream(vec![message(&json!({"action":"revise","reason":"scope incomplete","evidence":"observed omission","replacement":replacement}).to_string())]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    let result = crate::meta::evaluate(
        &state,
        "review",
        "agent",
        &agent.id,
        &json!(agent),
        "task",
        "result",
        0,
    )
    .await
    .unwrap();
    assert_eq!(result["applied"], true);
    assert!(
        state
            .portfolio
            .get(&agent.id)
            .unwrap()
            .unwrap()
            .system_prompt
            .contains("write carefully")
    );
    let history = state.portfolio.evaluations().unwrap();
    assert_eq!(history[0]["applied"], true);
    assert!(
        history[0]["before"]
            .as_str()
            .unwrap()
            .contains("scope evidence")
    );
}

#[tokio::test]
async fn evaluator_retires_agent_from_selection() {
    let (upstream,_)=upstream(vec![message(r#"{"action":"retire","reason":"obsolete","evidence":"definition no longer meets requested scope"}"#)]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    crate::meta::evaluate(
        &state,
        "retire",
        "agent",
        &agent.id,
        &json!(agent),
        "task",
        "result",
        0,
    )
    .await
    .unwrap();
    assert!(state.portfolio.get(&agent.id).unwrap().is_none());
    assert!(state.portfolio.list().unwrap().is_empty());
}

#[tokio::test]
async fn invalid_evaluation_does_not_mutate_definition() {
    let (upstream, _) = upstream(vec![message(
        r#"{"action":"revise","reason":"r","evidence":"e","replacement":{}}"#,
    )])
    .await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    assert!(
        crate::meta::evaluate(
            &state,
            "invalid",
            "agent",
            &agent.id,
            &json!(agent),
            "task",
            "result",
            0
        )
        .await
        .is_err()
    );
    assert_eq!(
        state
            .portfolio
            .get(&agent.id)
            .unwrap()
            .unwrap()
            .system_prompt,
        agent.system_prompt
    );
}

#[tokio::test]
async fn foreman_inspects_internal_tools_before_dispatch() {
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    let mut responses = vec![];
    for name in [
        "list_agents",
        "list_workflows",
        "get_statistics",
        "get_evaluations",
    ] {
        responses.push(message(
            &json!({"decision":"tool","name":name,"arguments":{}}).to_string(),
        ));
    }
    responses.extend([
        message(&json!({"decision":"use","agent_id":agent.id}).to_string()),
        message("report"),
    ]);
    script.lock().unwrap().0 = responses.into();
    let output = crate::runtime::dispatch(&state, &tool_payload(), "inspect")
        .await
        .unwrap();
    assert_eq!(output.report, "report");
    let script = script.lock().unwrap();
    let context = script.1[4]["messages"][1]["content"].as_str().unwrap();
    assert!(context.contains("agent-generator"));
    assert!(context.contains("get_statistics"));
    assert!(context.contains("get_evaluations"));
    let specialist_context = script.1[5]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|message| message["content"].as_str())
        .find(|content| content.starts_with("CCM internal tool observations"))
        .expect("specialist must receive internal tool observations");
    for name in [
        "list_agents",
        "list_workflows",
        "get_statistics",
        "get_evaluations",
    ] {
        assert!(specialist_context.contains(name));
    }
}

#[tokio::test]
async fn debug_reset_clears_generated_state_and_retains_evidence() {
    let state = state("http://127.0.0.1:1", true);
    let agent = insert(&state);
    let w = workflow(&agent.id);
    state.portfolio.workflow_insert(&w, 0).unwrap();
    state.portfolio.save_run("run", &json!({}), 0).unwrap();
    state
        .portfolio
        .event("test", "request", "agent", "ok", 3, None, "")
        .unwrap();
    let proxy = proxy(state.clone()).await;
    let response: Value = request(&proxy, "debug-delete", false)
        .await
        .json()
        .await
        .unwrap();
    let result: Value = serde_json::from_str(
        response["choices"][0]["message"]["content"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(result["deleted"]["workflows"], 1);
    assert_eq!(result["deleted"]["workflow_runs"], 1);
    assert_eq!(result["history_retained"], true);
    assert!(state.portfolio.workflow_list().unwrap().is_empty());
    assert!(state.portfolio.claim_run("run").is_err());
    assert_eq!(
        state.portfolio.statistics().unwrap()["groups"][0]["count"],
        1
    );
}
