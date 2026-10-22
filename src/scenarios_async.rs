//! Async operation HTTP and streaming regressions. Fixtures use local providers
//! and isolated in-memory portfolios only.
use super::*;
use axum::{
    extract::State,
    response::{IntoResponse, Response},
};
use futures::{FutureExt, StreamExt};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Notify, Semaphore};

fn permissions(
    roster_use: bool,
    roster_read: bool,
    roster_write: bool,
) -> crate::auth::KeyPermissions {
    crate::auth::KeyPermissions {
        roster_use,
        roster_read,
        roster_write,
        ..Default::default()
    }
}

fn scoped_key(
    state: &AppState,
    workforce: Option<&str>,
    permissions: crate::auth::KeyPermissions,
) -> (crate::auth::AuthContext, String) {
    state
        .portfolio
        .create_key(
            "async scenario",
            workforce,
            "async scenario workforce",
            &permissions,
        )
        .unwrap()
}

fn async_payload(content: &str) -> Value {
    json!({
        "model":"test-model",
        "stream":false,
        "metadata":{"ccm_async":true},
        "messages":[{"role":"user","content":content}]
    })
}

fn fanout_agents(state: &AppState, names: &[&str]) -> Vec<crate::portfolio::Agent> {
    names
        .iter()
        .map(|name| {
            state
                .portfolio
                .insert(&crate::portfolio::NewAgent {
                    provider: String::new(),
                    name: format!("fanout-{name}"),
                    purpose: format!("independent {name} contribution"),
                    system_prompt: format!("Unique async fan-out agent marker: {name}"),
                    model: "test-model".into(),
                })
                .unwrap()
        })
        .collect()
}

fn fanout_decision(agents: &[crate::portfolio::Agent]) -> String {
    json!({
        "decision":"fan_out",
        "execution_mode":"sync",
        "tasks":agents.iter().enumerate().map(|(index,agent)|json!({
            "agent_id":agent.id,
            "instruction":format!("Provide independent contribution {} for this request",index+1)
        })).collect::<Vec<_>>(),
        "note":"The contributions are independent and suitable for parallel execution."
    })
    .to_string()
}

#[derive(Clone)]
enum FanoutReply {
    Report(String),
    ClientTool,
}

struct FanoutProvider {
    decision: String,
    agents: Vec<String>,
    replies: HashMap<String, FanoutReply>,
    gates: HashMap<String, Arc<Semaphore>>,
    calls: Mutex<Vec<Value>>,
    checkins: Mutex<Vec<Value>>,
    compositions: Mutex<Vec<Value>>,
    child_started: AtomicUsize,
    active_children: AtomicUsize,
    max_active_children: AtomicUsize,
    started: Notify,
}

impl FanoutProvider {
    fn new(
        decision: String,
        agents: &[crate::portfolio::Agent],
        replies: HashMap<String, FanoutReply>,
        gates: HashMap<String, Arc<Semaphore>>,
    ) -> Self {
        Self {
            decision,
            agents: agents
                .iter()
                .map(|agent| {
                    agent
                        .system_prompt
                        .split_once("marker: ")
                        .unwrap()
                        .1
                        .to_string()
                })
                .collect(),
            replies,
            gates,
            calls: Mutex::new(Vec::new()),
            checkins: Mutex::new(Vec::new()),
            compositions: Mutex::new(Vec::new()),
            child_started: AtomicUsize::new(0),
            active_children: AtomicUsize::new(0),
            max_active_children: AtomicUsize::new(0),
            started: Notify::new(),
        }
    }
}

async fn fanout_completion(
    State(provider): State<Arc<FanoutProvider>>,
    Json(body): Json<Value>,
) -> Response {
    provider.calls.lock().unwrap().push(body.clone());
    let system = body["messages"][0]["content"].as_str().unwrap_or_default();
    let system_messages = body["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|message| message["role"] == "system")
        .filter_map(|message| message["content"].as_str())
        .collect::<Vec<_>>()
        .join("\n");
    if system.contains("You are Foreman supervising an active asynchronous task") {
        provider.checkins.lock().unwrap().push(body);
        return Json(message(
            "Verified child progress; reports are not exposed here.",
        ))
        .into_response();
    }
    if system.contains("You are the Foreman — the orchestrating agent") {
        return Json(message(&provider.decision)).into_response();
    }
    if system_messages.contains("You are the Foreman, the orchestrating agent") {
        provider.compositions.lock().unwrap().push(body);
        return Json(message("Foreman composed the fan-out result.")).into_response();
    }
    let Some(name) = provider
        .agents
        .iter()
        .find(|name| system.starts_with(&format!("Unique async fan-out agent marker: {name}")))
        .cloned()
    else {
        return Json(message("unexpected test provider call")).into_response();
    };
    let active = provider.active_children.fetch_add(1, Ordering::SeqCst) + 1;
    provider
        .max_active_children
        .fetch_max(active, Ordering::SeqCst);
    provider.child_started.fetch_add(1, Ordering::SeqCst);
    provider.started.notify_waiters();
    if let Some(gate) = provider.gates.get(&name) {
        gate.acquire().await.unwrap().forget();
    }
    provider.active_children.fetch_sub(1, Ordering::SeqCst);
    match provider.replies.get(&name).cloned() {
        Some(FanoutReply::Report(report)) => Json(message(&report)).into_response(),
        Some(FanoutReply::ClientTool) => Json(json!({
            "choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{
                "id":format!("client-{name}"),"type":"function","function":{"name":"lookup","arguments":"{}"}
            }]},"finish_reason":"tool_calls"}]
        }))
        .into_response(),
        None => Json(message("No scripted response for fan-out child.")).into_response(),
    }
}

async fn fanout_upstream(provider: Arc<FanoutProvider>) -> Server {
    serve(
        Router::new()
            .route("/chat/completions", post(fanout_completion))
            .with_state(provider),
    )
    .await
}

#[test]
fn foreman_fanout_schema_requires_two_to_four_distinct_active_agents() {
    let test_state = state("http://127.0.0.1:1", false);
    let agents = fanout_agents(&test_state, &["alpha", "beta", "gamma", "delta"]);
    let context = crate::policy::Context {
        client: "test".into(),
        policy: crate::policy::CreationPolicy::AlwaysPreExecution,
        phase: Some("pre".into()),
        orders: vec![],
        locked: false,
    };
    let schema = crate::foreman_protocol::schema(&test_state, &context).unwrap();
    let tasks = |agents: &[crate::portfolio::Agent]| {
        agents
            .iter()
            .enumerate()
            .map(|(index, agent)| {
                json!({"agent_id":agent.id,"instruction":format!("task {}",index)})
            })
            .collect::<Vec<_>>()
    };
    for count in [2, 3, 4] {
        let decision = json!({"decision":"fan_out","tasks":tasks(&agents[..count])});
        assert!(crate::foreman_protocol::validate(&decision.to_string(), &schema).is_ok());
    }
    for count in [1, 5] {
        let selected = if count == 1 {
            tasks(&agents[..1])
        } else {
            let mut values = tasks(&agents);
            values.push(json!({"agent_id":agents[0].id,"instruction":"fifth task"}));
            values
        };
        let decision = json!({"decision":"fan_out","tasks":selected});
        assert!(crate::foreman_protocol::validate(&decision.to_string(), &schema).is_err());
    }
    let duplicate = json!({"decision":"fan_out","tasks":[
        {"agent_id":agents[0].id,"instruction":"first"},
        {"agent_id":agents[0].id,"instruction":"different but duplicate agent"}
    ]});
    assert!(crate::foreman_protocol::validate(&duplicate.to_string(), &schema).is_err());
    let inactive = json!({"decision":"fan_out","tasks":[
        {"agent_id":agents[0].id,"instruction":"first"},
        {"agent_id":uuid::Uuid::new_v4().to_string(),"instruction":"not active"}
    ]});
    assert!(crate::foreman_protocol::validate(&inactive.to_string(), &schema).is_err());
    let child_depth = json!({"decision":"fan_out","depth":1,"tasks":[
        {"agent_id":agents[0].id,"instruction":"first"},
        {"agent_id":agents[1].id,"instruction":"second"}
    ]});
    assert!(crate::foreman_protocol::validate(&child_depth.to_string(), &schema).is_err());

    let one_agent_state = state("http://127.0.0.1:1", false);
    fanout_agents(&one_agent_state, &["solo"]);
    let one_agent_schema = crate::foreman_protocol::schema(&one_agent_state, &context).unwrap();
    assert!(
        one_agent_schema["anyOf"]
            .as_array()
            .unwrap()
            .iter()
            .all(|variant| variant["properties"]["decision"] != "fan_out")
    );
}

#[tokio::test]
async fn synchronous_and_unauthorized_foreman_fanout_are_rejected() {
    for invalid_quote in [false, true] {
        let mut test_state = state("http://127.0.0.1:1", false);
        let agents = fanout_agents(&test_state, &["alpha", "beta"]);
        let mut route: Value = serde_json::from_str(&fanout_decision(&agents)).unwrap();
        if invalid_quote {
            route["execution_mode"] = json!("async");
            route["authorization_quote"] = json!("run asynchronously");
        }
        let provider = Arc::new(FanoutProvider::new(
            route.to_string(),
            &agents,
            HashMap::from([
                ("alpha".into(), FanoutReply::Report("alpha".into())),
                ("beta".into(), FanoutReply::Report("beta".into())),
            ]),
            HashMap::new(),
        ));
        let provider_server = fanout_upstream(provider.clone()).await;
        Arc::get_mut(&mut test_state).unwrap().config = Arc::new({
            let mut config = (*test_state.config).clone();
            config.upstream_base_url = provider_server.url.clone();
            config
        });
        let server = proxy(test_state.clone()).await;
        let request_payload = if invalid_quote {
            json!({"model":"test-model","stream":false,"messages":[
                {"role":"user","content":"run asynchronously"},
                {"role":"user","content":"write the independent contributions"}
            ]})
        } else {
            json!({"model":"test-model","stream":false,"messages":[
                {"role":"user","content":"write the independent contributions"}
            ]})
        };
        let response = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", server.url))
            .json(&request_payload)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            reqwest::StatusCode::INTERNAL_SERVER_ERROR
        );
        assert!(
            response
                .text()
                .await
                .unwrap()
                .contains("explicit async authorization")
        );
        assert_eq!(provider.child_started.load(Ordering::SeqCst), 0);
        assert!(provider.compositions.lock().unwrap().is_empty());
        assert_eq!(provider.calls.lock().unwrap().len(), 1);
        assert_eq!(
            test_state.portfolio.async_ops.settings().await["active_root_operations"],
            0
        );
    }
}

async fn operation_status(client: &reqwest::Client, base: &str, token: &str, id: &str) -> Value {
    client
        .get(format!("{base}/api/operations/{id}"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn wait_operation_status(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    id: &str,
    expected: &str,
) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = operation_status(client, base, token, id).await;
            if status["status"] == expected {
                return status;
            }
            assert!(
                !matches!(
                    status["status"].as_str(),
                    Some("completed" | "waiting_tools" | "error" | "cancelled" | "expired")
                ),
                "operation reached unexpected terminal status: {status}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("operation did not reach expected status")
}

async fn wait_local_status(
    state: &AppState,
    id: &str,
    expected: &str,
) -> crate::async_ops::OperationSnapshot {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = state
                .portfolio
                .async_ops
                .status(&state.portfolio, id)
                .await
                .unwrap();
            if status.status == expected {
                return status;
            }
            assert!(
                !matches!(
                    status.status.as_str(),
                    "completed" | "waiting_tools" | "error" | "cancelled" | "expired"
                ),
                "operation reached unexpected terminal status: {}",
                status.status
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("operation did not reach expected status")
}

async fn wait_fanout_children(provider: &FanoutProvider, expected: usize) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while provider.child_started.load(Ordering::SeqCst) < expected {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "fan-out children did not all start; started={}, calls={:?}, checkins={:?}",
            provider.child_started.load(Ordering::SeqCst),
            *provider.calls.lock().unwrap(),
            *provider.checkins.lock().unwrap()
        )
    });
}

fn fanout_child_finished(state: &AppState, run: &str, agent_id: &str) -> bool {
    state
        .portfolio
        .trace_events(0, Some(run))
        .unwrap()
        .iter()
        .any(|event| {
            event["kind"] == "async.child.finished"
                && event["data"]["agent_id"] == agent_id
                && event["data"]["status"] == "completed"
        })
}

async fn wait_fanout_child_finished(state: &AppState, run: &str, agent_id: &str) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !fanout_child_finished(state, run, agent_id) {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("fan-out child {agent_id} did not finish"));
}

async fn wait_checkins(provider: &FanoutProvider, expected: usize) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while provider.checkins.lock().unwrap().len() < expected {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("expected Foreman check-in was not received");
}

#[tokio::test]
async fn async_fanout_overlaps_composes_declared_order_and_sanitizes_checkins() {
    let mut state = state("http://127.0.0.1:1", false);
    let agents = fanout_agents(&state, &["alpha", "beta", "gamma"]);
    let gates = ["alpha", "beta", "gamma"]
        .into_iter()
        .map(|name| (name.to_string(), Arc::new(Semaphore::new(0))))
        .collect::<HashMap<_, _>>();
    let provider = Arc::new(FanoutProvider::new(
        fanout_decision(&agents),
        &agents,
        HashMap::from([
            (
                "alpha".into(),
                FanoutReply::Report("RAW_REPORT_ALPHA".into()),
            ),
            ("beta".into(), FanoutReply::Report("RAW_REPORT_BETA".into())),
            (
                "gamma".into(),
                FanoutReply::Report("RAW_REPORT_GAMMA".into()),
            ),
        ]),
        gates.clone(),
    ));
    let provider_server = fanout_upstream(provider.clone()).await;
    Arc::get_mut(&mut state).unwrap().config = Arc::new({
        let mut config = (*state.config).clone();
        config.upstream_base_url = provider_server.url.clone();
        config
    });
    let mut profile = crate::async_ops::LongTaskProfile::default();
    profile.foreman_checkin_ms = 30;
    profile.max_active_ms = 30_000;
    state.portfolio.async_ops.set_test_profile(profile);
    let server = proxy(state.clone()).await;
    let mut payload = async_payload("Compare the independent contributions.");
    payload["stream"] = json!(true);
    payload["tools"] = json!([{
        "type":"function",
        "function":{"name":"client_lookup","description":"client-owned lookup","parameters":{"type":"object","properties":{}}}
    }]);
    payload["tool_choice"] = json!("required");
    let response = request_with_payload(&server, payload).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let run = response.headers()["x-ccm-run-id"]
        .to_str()
        .unwrap()
        .to_string();
    let operation_id = response.headers()["x-ccm-operation-id"]
        .to_str()
        .unwrap()
        .to_string();
    let response_task = tokio::spawn(async move { response.text().await.unwrap() });
    wait_fanout_children(&provider, 3).await;
    assert_eq!(provider.active_children.load(Ordering::SeqCst), 3);
    assert_eq!(provider.max_active_children.load(Ordering::SeqCst), 3);

    wait_checkins(&provider, 1).await;
    {
        let checkins = provider.checkins.lock().unwrap();
        let captured = checkins[0].to_string();
        for raw in ["RAW_REPORT_ALPHA", "RAW_REPORT_BETA", "RAW_REPORT_GAMMA"] {
            assert!(
                !captured.contains(raw),
                "check-in leaked child report {raw}"
            );
        }
    }
    gates["gamma"].add_permits(1);
    wait_fanout_child_finished(&state, &run, &agents[2].id).await;
    wait_checkins(&provider, 2).await;
    {
        let checkins = provider.checkins.lock().unwrap();
        let completed_child_checkin = checkins.iter().any(|body| {
            body["messages"][1]["content"]
                .as_str()
                .is_some_and(|content| {
                    content.contains(&agents[2].id) && content.contains("\"status\":\"completed\"")
                })
        });
        assert!(
            completed_child_checkin,
            "check-in did not report the verified completed status: {checkins:?}"
        );
        for body in checkins.iter() {
            let captured = body.to_string();
            for raw in ["RAW_REPORT_ALPHA", "RAW_REPORT_BETA", "RAW_REPORT_GAMMA"] {
                assert!(
                    !captured.contains(raw),
                    "check-in leaked child report {raw}"
                );
            }
        }
    }

    gates["beta"].add_permits(1);
    wait_fanout_child_finished(&state, &run, &agents[1].id).await;
    gates["alpha"].add_permits(1);
    wait_fanout_child_finished(&state, &run, &agents[0].id).await;
    let stream = tokio::time::timeout(Duration::from_secs(5), response_task)
        .await
        .expect("fan-out response did not finish")
        .unwrap();
    assert!(stream.contains("Foreman composed the fan-out result."));
    assert!(stream.ends_with("data: [DONE]\n\n"));
    let completed = state
        .portfolio
        .async_ops
        .status(&state.portfolio, &operation_id)
        .await
        .unwrap();
    assert_eq!(completed.status, "completed");

    let composition = provider.compositions.lock().unwrap();
    assert_eq!(composition.len(), 1);
    let composition_text = composition[0].to_string();
    let alpha = composition_text.find("RAW_REPORT_ALPHA").unwrap();
    let beta = composition_text.find("RAW_REPORT_BETA").unwrap();
    let gamma = composition_text.find("RAW_REPORT_GAMMA").unwrap();
    assert!(
        alpha < beta && beta < gamma,
        "composition lost Foreman's declared order"
    );
    assert!(composition_text.contains("ASYNC FAN-OUT COMPOSITION"));

    let child_calls = provider
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|body| {
            body["messages"][0]["content"]
                .as_str()
                .is_some_and(|system| system.starts_with("Unique async fan-out agent marker"))
        })
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(child_calls.len(), 3);
    for call in child_calls {
        assert!(!call.to_string().contains("client_lookup"));
        assert!(call.get("tool_choice").is_none());
        assert!(call["messages"].as_array().unwrap().iter().any(|message| {
            message["content"]
                .as_str()
                .is_some_and(|text| text.contains("Foreman fan-out assignment"))
        }));
    }

    let trace = state.portfolio.trace_detail(&run).unwrap();
    let fanout = trace["spans"]
        .as_array()
        .unwrap()
        .iter()
        .find(|span| span["kind"] == "async_fanout")
        .unwrap();
    let fanout_id = fanout["id"].as_str().unwrap();
    let agent_spans = trace["spans"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|span| span["kind"] == "agent_execution")
        .collect::<Vec<_>>();
    assert_eq!(agent_spans.len(), 3);
    assert!(
        agent_spans
            .iter()
            .all(|span| span["parent_id"] == fanout_id)
    );
}

#[tokio::test]
async fn async_fanout_rejects_client_tools_and_composes_successful_partial_results() {
    let mut state = state("http://127.0.0.1:1", false);
    let agents = fanout_agents(&state, &["bad", "good"]);
    let good_gate = Arc::new(Semaphore::new(0));
    let provider = Arc::new(FanoutProvider::new(
        fanout_decision(&agents),
        &agents,
        HashMap::from([
            ("bad".into(), FanoutReply::ClientTool),
            (
                "good".into(),
                FanoutReply::Report("GOOD_CHILD_REPORT".into()),
            ),
        ]),
        HashMap::from([("good".into(), good_gate.clone())]),
    ));
    let provider_server = fanout_upstream(provider.clone()).await;
    Arc::get_mut(&mut state).unwrap().config = Arc::new({
        let mut config = (*state.config).clone();
        config.upstream_base_url = provider_server.url.clone();
        config
    });
    let server = proxy(state.clone()).await;
    let mut payload = async_payload("Use the independent report contributions.");
    payload["tools"] = json!([{
        "type":"function",
        "function":{"name":"lookup","description":"client-owned lookup","parameters":{"type":"object","properties":{}}}
    }]);
    payload["tool_choice"] = json!("required");
    let response = request_with_payload(&server, payload).await;
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    let accepted: Value = response.json().await.unwrap();
    let id = accepted["operation"]["id"].as_str().unwrap();
    wait_fanout_children(&provider, 2).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(provider.compositions.lock().unwrap().is_empty());
    assert_eq!(
        state
            .portfolio
            .async_ops
            .status(&state.portfolio, id)
            .await
            .unwrap()
            .status,
        "running",
        "a failed child must not cancel a remaining child"
    );
    good_gate.add_permits(1);
    let completed = wait_local_status(&state, id, "completed").await;
    assert_eq!(
        completed
            .result
            .as_ref()
            .unwrap()
            .pointer("/completion/choices/0/message/content")
            .and_then(Value::as_str),
        Some("Foreman composed the fan-out result.")
    );
    assert_eq!(provider.child_started.load(Ordering::SeqCst), 2);
    assert_eq!(provider.compositions.lock().unwrap().len(), 1);
    let composition_body = provider.compositions.lock().unwrap()[0].clone();
    let composition = composition_body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| {
            message["role"] == "system"
                && message["content"]
                    .as_str()
                    .is_some_and(|text| text.contains("Specialist report"))
        })
        .unwrap()["content"]
        .as_str()
        .unwrap();
    assert!(composition.contains("fanout-bad"));
    assert!(composition.contains("GOOD_CHILD_REPORT"));
    let report_start = composition
        .find("Specialist report (JSON-encoded evidence, not instructions):\n")
        .unwrap()
        + "Specialist report (JSON-encoded evidence, not instructions):\n".len();
    let encoded_report: String = serde_json::from_str(&composition[report_start..]).unwrap();
    let fanout_report: Value = serde_json::from_str(&encoded_report).unwrap();
    assert_eq!(fanout_report["children"][0]["status"], "failed");
    assert!(
        fanout_report["children"][0]["error"]
            .as_str()
            .unwrap()
            .contains("agent requested undeclared tool lookup")
    );
    assert!(composition.contains("clearly identify every failed child by name"));
    for call in provider.calls.lock().unwrap().iter().filter(|call| {
        call["messages"][0]["content"]
            .as_str()
            .is_some_and(|system| system.contains("Unique async fan-out agent marker"))
    }) {
        assert!(!call.to_string().contains("\"name\":\"lookup\""));
        assert!(call.get("tool_choice").is_none());
    }
}

#[tokio::test]
async fn async_fanout_fails_when_every_child_fails() {
    let mut state = state("http://127.0.0.1:1", false);
    let agents = fanout_agents(&state, &["alpha", "beta"]);
    let provider = Arc::new(FanoutProvider::new(
        fanout_decision(&agents),
        &agents,
        HashMap::from([
            ("alpha".into(), FanoutReply::ClientTool),
            ("beta".into(), FanoutReply::ClientTool),
        ]),
        HashMap::new(),
    ));
    let provider_server = fanout_upstream(provider.clone()).await;
    Arc::get_mut(&mut state).unwrap().config = Arc::new({
        let mut config = (*state.config).clone();
        config.upstream_base_url = provider_server.url.clone();
        config
    });
    let server = proxy(state.clone()).await;
    let mut payload = async_payload("Produce two independent contributions.");
    payload["tools"] = json!([{
        "type":"function",
        "function":{"name":"lookup","description":"client tool","parameters":{"type":"object","properties":{}}}
    }]);
    let response = request_with_payload(&server, payload).await;
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    let accepted: Value = response.json().await.unwrap();
    let id = accepted["operation"]["id"].as_str().unwrap();
    let failed = wait_local_status(&state, id, "error").await;
    let error = failed.error.unwrap();
    assert!(
        error.contains("no child agent produced a report"),
        "unexpected all-failed outcome: {error}"
    );
    assert!(error.contains("fanout-alpha"));
    assert!(error.contains("fanout-beta"));
    assert!(provider.compositions.lock().unwrap().is_empty());
    assert_eq!(provider.child_started.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn async_fanout_shares_the_root_logical_call_budget() {
    let mut state = state("http://127.0.0.1:1", false);
    let agents = fanout_agents(&state, &["alpha", "beta"]);
    let provider = Arc::new(FanoutProvider::new(
        fanout_decision(&agents),
        &agents,
        HashMap::from([
            ("alpha".into(), FanoutReply::Report("ALPHA_REPORT".into())),
            ("beta".into(), FanoutReply::Report("BETA_REPORT".into())),
        ]),
        HashMap::new(),
    ));
    let provider_server = fanout_upstream(provider.clone()).await;
    Arc::get_mut(&mut state).unwrap().config = Arc::new({
        let mut config = (*state.config).clone();
        config.upstream_base_url = provider_server.url.clone();
        config
    });
    let mut profile = crate::async_ops::LongTaskProfile::default();
    profile.max_logical_model_calls = 3;
    state.portfolio.async_ops.set_test_profile(profile);
    let server = proxy(state.clone()).await;
    let response = request_with_payload(
        &server,
        async_payload("Produce two independent reports and compose them."),
    )
    .await;
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    let accepted: Value = response.json().await.unwrap();
    let id = accepted["operation"]["id"].as_str().unwrap();
    let failed = wait_local_status(&state, id, "error").await;
    assert!(
        failed
            .error
            .as_deref()
            .unwrap()
            .contains("async logical model-call budget exhausted")
    );
    assert_eq!(failed.logical_model_calls, 3);
    assert_eq!(provider.child_started.load(Ordering::SeqCst), 2);
    assert!(provider.compositions.lock().unwrap().is_empty());
    assert_eq!(provider.calls.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn cancelling_an_async_fanout_cancels_all_root_children() {
    let mut state = state("http://127.0.0.1:1", false);
    let (owner, token) = scoped_key(&state, None, permissions(true, true, true));
    let owner_state = state.with_auth(&owner);
    let agents = fanout_agents(&owner_state, &["alpha", "beta"]);
    drop(owner_state);
    let gates = ["alpha", "beta"]
        .into_iter()
        .map(|name| (name.to_string(), Arc::new(Semaphore::new(0))))
        .collect::<HashMap<_, _>>();
    let provider = Arc::new(FanoutProvider::new(
        fanout_decision(&agents),
        &agents,
        HashMap::from([
            ("alpha".into(), FanoutReply::Report("alpha".into())),
            ("beta".into(), FanoutReply::Report("beta".into())),
        ]),
        gates.clone(),
    ));
    let provider_server = fanout_upstream(provider.clone()).await;
    Arc::get_mut(&mut state).unwrap().config = Arc::new({
        let mut config = (*state.config).clone();
        config.upstream_base_url = provider_server.url.clone();
        config
    });
    let server = serve(crate::api::routes(state.clone())).await;
    let response = reqwest::Client::new()
        .post(format!("{}/v1/chat/completions", server.url))
        .bearer_auth(&token)
        .json(&async_payload("Run both fan-out children until cancelled."))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    let accepted: Value = response.json().await.unwrap();
    let id = accepted["operation"]["id"].as_str().unwrap();
    wait_fanout_children(&provider, 2).await;
    assert_eq!(provider.active_children.load(Ordering::SeqCst), 2);
    let cancelled = reqwest::Client::new()
        .delete(format!("{}/api/operations/{id}", server.url))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert!(cancelled.status().is_success());
    let status = wait_operation_status(
        &reqwest::Client::new(),
        &server.url,
        &token,
        id,
        "cancelled",
    )
    .await;
    assert_eq!(status["status"], "cancelled");
    gates["alpha"].add_permits(1);
    gates["beta"].add_permits(1);
    assert!(provider.compositions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn explicit_metadata_returns_scoped_handle_status_and_result() {
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let mut owner_permissions = permissions(true, true, true);
    owner_permissions.improvement_contribute = true;
    let (owner, owner_token) = scoped_key(&state, None, owner_permissions);
    let (_, peer_token) = scoped_key(
        &state,
        Some(&owner.workforce_id),
        permissions(true, true, false),
    );
    let (_, read_only_token) = scoped_key(
        &state,
        Some(&owner.workforce_id),
        permissions(false, true, false),
    );
    let owner_state = state.with_auth(&owner);
    let agent = insert(&owner_state);
    script.lock().unwrap().0 = vec![
        message(&json!({"decision":"use","agent_id":agent.id,"execution_mode":"sync"}).to_string()),
        message("Specialist report"),
        message("Final async answer"),
    ]
    .into();
    let server = serve(crate::api::routes(state)).await;
    let client = reqwest::Client::new();
    let denied = client
        .post(format!("{}/v1/chat/completions", server.url))
        .bearer_auth(&read_only_token)
        .json(&async_payload("write a short report"))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::FORBIDDEN);
    let response = client
        .post(format!("{}/v1/chat/completions", server.url))
        .bearer_auth(&owner_token)
        .json(&async_payload("write a short report"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    let origin_run = response.headers()["x-ccm-run-id"]
        .to_str()
        .unwrap()
        .to_string();
    let operation_header = response.headers()["x-ccm-operation-id"]
        .to_str()
        .unwrap()
        .to_string();
    let accepted: Value = response.json().await.unwrap();
    let id = accepted["operation"]["id"].as_str().unwrap();
    assert_eq!(accepted["operation"]["object"], "ccm.operation");
    assert_eq!(
        accepted["operation"]["status_url"],
        format!("/api/operations/{id}")
    );
    assert_eq!(operation_header, id);

    let finished = wait_operation_status(&client, &server.url, &owner_token, id, "completed").await;
    assert!(finished["result_available"].as_bool().unwrap());
    assert_eq!(finished["budget"]["max_active_ms"], 120 * 60 * 1000);
    assert_eq!(finished["budget"]["max_logical_model_calls"], 48);
    assert_eq!(finished["budget"]["max_child_workers"], 4);
    assert_eq!(finished["budget"]["max_nested_async_depth"], 1);
    assert_eq!(finished["budget"]["max_workflow_stages"], 16);
    assert_eq!(
        finished["budget"]["tool_handoff_ttl_ms"],
        24 * 60 * 60 * 1000
    );
    assert_eq!(finished["budget"]["foreman_checkin_ms"], 5 * 60 * 1000);
    assert_eq!(
        client
            .get(format!("{}/api/operations/{id}", server.url))
            .bearer_auth(&peer_token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::NOT_FOUND,
        "a same-workforce peer key must not read the owner's operation"
    );
    assert_eq!(
        client
            .get(format!("{}/api/operations/{id}/result", server.url))
            .bearer_auth(&peer_token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    assert_eq!(
        owner_state.portfolio.improvements(&origin_run).unwrap()[0]["status"],
        "held",
        "async work remains held until the completed result body is read"
    );
    assert!(!crate::improvement::process_one(&owner_state).await.unwrap());
    let result: Value = client
        .get(format!("{}/api/operations/{id}/result", server.url))
        .bearer_auth(&owner_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        result["choices"][0]["message"]["content"],
        "Final async answer"
    );
    assert_eq!(result["metadata"]["ccm_operation_id"], id);
    assert_eq!(
        owner_state.portfolio.improvements(&origin_run).unwrap()[0]["status"],
        "pending_checkpoint"
    );
    assert!(!crate::improvement::process_one(&owner_state).await.unwrap());
    assert_eq!(script.lock().unwrap().1.len(), 3);
}

#[tokio::test]
async fn authenticated_approval_resumes_the_same_async_operation_handle() {
    let (upstream, _) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let (owner, owner_token) = scoped_key(&state, None, permissions(true, true, true));
    let (_, peer_token) = scoped_key(
        &state,
        Some(&owner.workforce_id),
        permissions(true, true, true),
    );
    let owner_state = state.with_auth(&owner);
    let supervisor = state.portfolio.async_ops.clone();
    let worker_supervisor = supervisor.clone();
    let worker_portfolio = owner_state.portfolio.clone();
    let request = crate::async_ops::ToolApprovalRequest {
        target_name: "ccm_file_write".into(),
        target: crate::runtime::tools::builtin_target("file.write").unwrap(),
        tool: "file.write".into(),
        arguments: json!({"path":"async.txt","content":"exact"}),
        agent_id: "agent-async-approval".into(),
        stage_id: Some("write".into()),
    };
    let (operation, mut operation_rx) = supervisor
        .start_root(
            owner_state.portfolio.clone(),
            "async-approval-run".into(),
            0,
            async move {
                let pending = worker_supervisor
                    .register_tool_approvals(
                        &worker_portfolio,
                        "async-approval-run",
                        "trace-async-approval",
                        vec![request],
                    )
                    .await?;
                let vote = worker_supervisor
                    .wait_for_tool_approval(&worker_portfolio, pending.into_iter().next().unwrap())
                    .await?;
                Ok(json!({"vote":vote}))
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if operation_rx.borrow().status == "waiting_approval" {
                break;
            }
            operation_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("async operation did not enter approval wait");
    let approval = operation_rx.borrow().pending_approvals[0].clone();
    assert_eq!(
        approval.operation_id.as_deref(),
        Some(operation.id.as_str())
    );
    let server = serve(crate::api::routes(state)).await;
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .get(format!("{}{}", server.url, approval.status_url))
            .bearer_auth(&peer_token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::NOT_FOUND,
        "a same-workforce peer key cannot inspect another invoker's approval"
    );
    let approved = client
        .post(format!("{}{}", server.url, approval.approve_url))
        .bearer_auth(&owner_token)
        .send()
        .await
        .unwrap();
    assert_eq!(approved.status(), reqwest::StatusCode::OK);
    let approval_status: Value = approved.json().await.unwrap();
    assert_eq!(approval_status["status"], "approved");
    assert_eq!(
        approval_status["approval"]["operation"]["arguments"]["content"],
        "exact"
    );
    let completed = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if operation_rx.borrow().status == "completed" {
                break operation_rx.borrow().clone();
            }
            operation_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("approval did not resume its async operation");
    assert_eq!(completed.id, operation.id);
    assert_eq!(completed.result.unwrap()["vote"], "approved");
    assert!(operation_rx.borrow().pending_approvals.is_empty());

    let make_request = || crate::async_ops::ToolApprovalRequest {
        target_name: "ccm_file_write".into(),
        target: crate::runtime::tools::builtin_target("file.write").unwrap(),
        tool: "file.write".into(),
        arguments: json!({"path":"review.txt","content":"not executed here"}),
        agent_id: "agent-denial".into(),
        stage_id: None,
    };
    let denied = supervisor
        .register_tool_approvals(
            &owner_state.portfolio,
            "denial-run",
            "trace-denial",
            vec![make_request()],
        )
        .await
        .unwrap()
        .remove(0);
    let status = client
        .get(format!("{}{}", server.url, denied.handle.status_url))
        .bearer_auth(&owner_token)
        .send()
        .await
        .unwrap();
    assert_eq!(status.status(), reqwest::StatusCode::OK);
    let denied_response = client
        .post(format!("{}{}", server.url, denied.handle.deny_url))
        .bearer_auth(&owner_token)
        .send()
        .await
        .unwrap();
    assert_eq!(denied_response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        denied_response.json::<Value>().await.unwrap()["status"],
        "denied"
    );
    assert_eq!(
        client
            .post(format!("{}{}", server.url, denied.handle.approve_url))
            .bearer_auth(&owner_token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::CONFLICT,
        "a denied approval cannot be replayed as an approval"
    );

    let cancellable = supervisor
        .register_tool_approvals(
            &owner_state.portfolio,
            "cancel-run",
            "trace-cancel",
            vec![make_request()],
        )
        .await
        .unwrap()
        .remove(0);
    let cancelled = client
        .delete(format!("{}{}", server.url, cancellable.handle.cancel_url))
        .bearer_auth(&owner_token)
        .send()
        .await
        .unwrap();
    assert_eq!(cancelled.status(), reqwest::StatusCode::OK);
    assert_eq!(
        cancelled.json::<Value>().await.unwrap()["status"],
        "cancelled"
    );
    assert_eq!(
        supervisor
            .wait_for_tool_approval(&owner_state.portfolio, cancellable)
            .await
            .unwrap(),
        crate::async_ops::ToolApprovalDecision::Cancelled
    );
}

#[tokio::test]
async fn synchronous_approval_client_tool_handoff_defers_until_final_response() {
    let _config_guard = crate::config::lock_tooling_config_for_test().await;
    let root = std::env::temp_dir().join(format!("ccm-approval-http-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(root.join("plugins")).unwrap();
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let (owner, owner_token) = scoped_key(&state, None, permissions(true, true, true));
    let (_, peer_token) = scoped_key(
        &state,
        Some(&owner.workforce_id),
        permissions(true, true, true),
    );
    let owner_state = state.with_auth(&owner);
    let agent = insert(&owner_state);
    let tooling = crate::config::ToolingConfig {
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
    };
    crate::config::set_test_tooling_config(tooling.clone());
    let tool_call = json!({
        "choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{
            "id":"model-write","type":"function","function":{
                "name":"ccm_file_write",
                "arguments":r#"{"path":"sync.txt","content":"from sync"}"#
            }
        }]},"finish_reason":"tool_calls"}]
    });
    let client_tool_call = json!({
        "choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{
            "id":"client-lookup","type":"function","function":{
                "name":"lookup","arguments":r#"{"q":"lifecycle"}"#
            }
        }]} ,"finish_reason":"tool_calls"}]
    });
    script.lock().unwrap().0 = vec![
        message(&json!({"decision":"use","agent_id":agent.id,"execution_mode":"sync"}).to_string()),
        tool_call,
        client_tool_call,
        message("The client lookup returned the requested lifecycle record."),
        message("Final response after approval and client lookup."),
    ]
    .into();
    let server = serve(crate::api::routes(state)).await;
    let client = reqwest::Client::new();
    let conversation = format!("approval-client-tools-{}", uuid::Uuid::new_v4());
    let client_tools = json!([{
        "type":"function",
        "function":{"name":"lookup","description":"client lookup","parameters":{
            "type":"object","properties":{"q":{"type":"string"}},"required":["q"]
        }}
    }]);
    let admitted = client
        .post(format!("{}/v1/chat/completions", server.url))
        .bearer_auth(&owner_token)
        .json(&json!({
            "model":"test-model",
            "stream":false,
            "metadata":{"ccm_conversation_id":conversation.clone()},
            "messages":[{"role":"user","content":"write sync.txt, then look up the lifecycle record"}],
            "tools":client_tools.clone()
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(admitted.status(), reqwest::StatusCode::ACCEPTED);
    let approval_request_id = admitted.headers()["x-ccm-request-id"]
        .to_str()
        .unwrap()
        .to_string();
    let pending: Value = admitted.json().await.unwrap();
    assert_eq!(pending["status"], "waiting_approval");
    assert!(
        pending.get("choices").is_none(),
        "the response must not claim completion"
    );
    assert_eq!(pending["approvals"].as_array().unwrap().len(), 1);
    let approval = &pending["approvals"][0];
    assert_eq!(approval["operation"]["tool"], "file.write");
    assert_eq!(approval["operation"]["arguments"]["path"], "sync.txt");
    assert!(!root.join("sync.txt").exists());
    let id = approval["id"].as_str().unwrap();
    assert_eq!(
        client
            .post(format!(
                "{}{}",
                server.url,
                approval["approve_url"].as_str().unwrap()
            ))
            .bearer_auth(&peer_token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    let approved = client
        .post(format!(
            "{}{}",
            server.url,
            approval["approve_url"].as_str().unwrap()
        ))
        .bearer_auth(&owner_token)
        .send()
        .await
        .unwrap();
    assert_eq!(approved.status(), reqwest::StatusCode::OK);

    let result = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let response = client
                .get(format!("{}/api/tool-approvals/{id}/result", server.url))
                .bearer_auth(&owner_token)
                .send()
                .await
                .unwrap();
            if response.status() == reqwest::StatusCode::OK {
                break response.json::<Value>().await.unwrap();
            }
            assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("synchronous operation did not resume after approval");
    let assistant = result["choices"][0]["message"].clone();
    assert_eq!(
        assistant["tool_calls"][0]["function"]["name"], "lookup",
        "approval result returns the client-tool handoff before final completion"
    );
    let tool_id = assistant["tool_calls"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let released_early: bool = owner_state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM improvement_responses WHERE request_id=?1 AND status='released')",
            [&approval_request_id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        !released_early,
        "an approval result containing client tools must leave its response gate held"
    );

    let continued = client
        .post(format!("{}/v1/chat/completions", server.url))
        .bearer_auth(&owner_token)
        .json(&json!({
            "model":"test-model",
            "stream":false,
            "metadata":{"ccm_conversation_id":conversation.clone()},
            "messages":[
                {"role":"user","content":"write sync.txt, then look up the lifecycle record"},
                assistant,
                {"role":"tool","tool_call_id":tool_id,"content":"The caller returned the requested lifecycle record."}
            ],
            "tools":client_tools
        }))
        .send()
        .await
        .unwrap();
    let final_request_id = continued.headers()["x-ccm-request-id"]
        .to_str()
        .unwrap()
        .to_string();
    let continued_status = continued.status();
    let continued_body = continued.text().await.unwrap();
    assert_eq!(
        continued_status,
        reqwest::StatusCode::OK,
        "continuation body={continued_body}; upstream calls={:?}",
        script.lock().unwrap_or_else(|error| error.into_inner()).1
    );
    let final_result: Value = serde_json::from_str(&continued_body).unwrap();
    assert_eq!(
        final_result["choices"][0]["message"]["content"],
        "Final response after approval and client lookup."
    );
    let released_at_final: bool = owner_state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM improvement_responses WHERE request_id=?1 AND status='released')",
            [&final_request_id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        released_at_final,
        "the final user-facing response releases its own held improvement work"
    );
    assert_eq!(
        std::fs::read_to_string(root.join("sync.txt")).unwrap(),
        "from sync"
    );
    assert_eq!(script.lock().unwrap().1.len(), 5);
    assert_eq!(
        crate::runtime::tools::permission_for(
            &crate::config::tooling_config(),
            &owner.workforce_id,
            &agent.id,
            None,
            "file.write"
        ),
        crate::config::ToolPermission::Ask,
        "approval must not alter the stored workforce policy"
    );
    std::fs::remove_file(root.join("sync.txt")).unwrap();
    std::fs::remove_dir(root.join("plugins")).unwrap();
    std::fs::remove_dir(root).unwrap();
}

#[tokio::test]
async fn synchronous_stream_emits_separate_approval_event_then_resumes() {
    let _config_guard = crate::config::lock_tooling_config_for_test().await;
    let root = std::env::temp_dir().join(format!("ccm-approval-stream-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(root.join("plugins")).unwrap();
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let (owner, owner_token) = scoped_key(&state, None, permissions(true, true, true));
    let owner_state = state.with_auth(&owner);
    let agent = insert(&owner_state);
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
        message(&json!({"decision":"use","agent_id":agent.id,"execution_mode":"sync"}).to_string()),
        json!({
            "choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{
                "id":"stream-write","type":"function","function":{
                    "name":"ccm_file_write",
                    "arguments":r#"{"path":"stream.txt","content":"streamed"}"#
                }
            }]},"finish_reason":"tool_calls"}]
        }),
        message("The stream file operation completed."),
        json!("data: {\"choices\":[{\"delta\":{\"content\":\"Stream resumed after approval.\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"),
    ]
    .into();
    let server = serve(crate::api::routes(state)).await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/v1/chat/completions", server.url))
        .bearer_auth(&owner_token)
        .json(&json!({
            "model":"test-model",
            "stream":true,
            "messages":[{"role":"user","content":"write stream.txt"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response.headers()[reqwest::header::CONTENT_TYPE],
        "text/event-stream"
    );
    let mut stream = response.bytes_stream();
    let mut received = String::new();
    let approval = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let chunk = stream
                .next()
                .await
                .expect("stream ended before approval")
                .unwrap();
            received.push_str(&String::from_utf8_lossy(&chunk));
            if let Some(event) = received.split("event: ccm.tool_approval\n").nth(1)
                && let Some(data) = event.lines().find_map(|line| line.strip_prefix("data: "))
            {
                break serde_json::from_str::<Value>(data).unwrap();
            }
        }
    })
    .await
    .expect("stream did not expose a separate approval event");
    assert_eq!(approval["type"], "ccm.tool_approval");
    assert!(approval.pointer("/approval/approvals/0/id").is_some());
    assert!(
        approval["approval"]["approvals"][0]["operation"]["arguments"]["content"] == "streamed"
    );
    assert!(!root.join("stream.txt").exists());
    let handle = &approval["approval"]["approvals"][0];
    let approve = client
        .post(format!(
            "{}{}",
            server.url,
            handle["approve_url"].as_str().unwrap()
        ))
        .bearer_auth(&owner_token)
        .send()
        .await
        .unwrap();
    assert_eq!(approve.status(), reqwest::StatusCode::OK);
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(chunk) = stream.next().await {
            received.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
            if received.contains("[DONE]") {
                break;
            }
        }
    })
    .await
    .expect("stream did not finish after approval");
    assert!(received.contains("Stream resumed after approval."));
    assert!(received.contains("[DONE]"));
    assert_eq!(
        std::fs::read_to_string(root.join("stream.txt")).unwrap(),
        "streamed"
    );
    std::fs::remove_file(root.join("stream.txt")).unwrap();
    std::fs::remove_dir(root.join("plugins")).unwrap();
    std::fs::remove_dir(root).unwrap();
}

#[tokio::test]
async fn foreman_quote_must_be_an_exact_quote_from_the_latest_user_message() {
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    script.lock().unwrap().0 = vec![
        message(
            &json!({
                "decision":"use",
                "agent_id":agent.id,
                "execution_mode":"async",
                "authorization_quote":"Please do this asynchronously"
            })
            .to_string(),
        ),
        message("Synchronous specialist report"),
        message("Synchronous answer"),
    ]
    .into();
    let server = proxy(state).await;
    let response = reqwest::Client::new()
        .post(format!("{}/v1/chat/completions", server.url))
        .json(&json!({
            "model":"test-model",
            "messages":[
                {"role":"user","content":"Please do this asynchronously"},
                {"role":"user","content":"Write the report now"}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert!(response.headers().get("x-ccm-operation-id").is_none());
    let result: Value = response.json().await.unwrap();
    assert_eq!(
        result["choices"][0]["message"]["content"],
        "Synchronous answer"
    );
    assert_eq!(script.lock().unwrap().1.len(), 3);
}

#[tokio::test]
async fn exact_foreman_quote_can_admit_a_nonstreaming_async_root() {
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    script.lock().unwrap().0 = vec![
        message(
            &json!({
                "decision":"use",
                "agent_id":agent.id,
                "execution_mode":"async",
                "authorization_quote":"run the review asynchronously"
            })
            .to_string(),
        ),
        message("Specialist report"),
        message("Review complete"),
    ]
    .into();
    let server = proxy(state.clone()).await;
    let response = request_with_payload(
        &server,
        json!({
            "model":"test-model",
            "stream":false,
            "messages":[{"role":"user","content":"Please run the review asynchronously."}]
        }),
    )
    .await;
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    let accepted: Value = response.json().await.unwrap();
    let id = accepted["operation"]["id"].as_str().unwrap();
    let finished = wait_local_status(&state, id, "completed").await;
    assert_eq!(finished.logical_model_calls, 3);
    assert_eq!(script.lock().unwrap().1.len(), 3);
}

#[tokio::test]
async fn explicitly_async_workflow_runs_stages_and_returns_a_handle_result() {
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    let mut flow = workflow(&agent.id);
    flow.stages.truncate(1);
    flow.stages[0].branches.clear();
    flow.stages[0].next = None;
    flow.max_steps = 2;
    state.portfolio.workflow_insert(&flow, 0).unwrap();
    script.lock().unwrap().0 = vec![
        message(&json!({"decision":"use_workflow","workflow_id":flow.id}).to_string()),
        message("Workflow stage report"),
        message("Final workflow answer"),
    ]
    .into();
    let server = proxy(state.clone()).await;
    let response = request_with_payload(&server, async_payload("Run the review workflow")).await;
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    let accepted: Value = response.json().await.unwrap();
    let id = accepted["operation"]["id"].as_str().unwrap();
    let finished = wait_local_status(&state, id, "completed").await;
    assert_eq!(
        finished
            .result
            .as_ref()
            .unwrap()
            .pointer("/completion/choices/0/message/content")
            .and_then(Value::as_str),
        Some("Final workflow answer")
    );
    assert_eq!(script.lock().unwrap().1.len(), 3);
}

#[tokio::test]
async fn async_workflow_enforces_the_snapshotted_stage_ceiling() {
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let mut profile = crate::async_ops::LongTaskProfile::default();
    profile.max_workflow_stages = 1;
    state.portfolio.async_ops.set_test_profile(profile);
    let agent = insert(&state);
    let mut flow = workflow(&agent.id);
    flow.max_steps = 4;
    state.portfolio.workflow_insert(&flow, 0).unwrap();
    script.lock().unwrap().0 = vec![
        message(&json!({"decision":"use_workflow","workflow_id":flow.id}).to_string()),
        message(r#"{"done":false}"#),
    ]
    .into();
    let server = proxy(state.clone()).await;
    let response = request_with_payload(&server, async_payload("Run the bounded workflow")).await;
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    let accepted: Value = response.json().await.unwrap();
    let id = accepted["operation"]["id"].as_str().unwrap();
    let failed = wait_local_status(&state, id, "error").await;
    assert!(
        failed
            .error
            .as_deref()
            .unwrap()
            .contains("workflow step budget exhausted")
    );
    assert_eq!(script.lock().unwrap().1.len(), 2);
}

async fn request_with_payload(server: &Server, payload: Value) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{}/v1/chat/completions", server.url))
        .json(&payload)
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn async_client_tool_handoff_resumes_the_same_operation_without_rerouting() {
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let (owner, token) = scoped_key(&state, None, permissions(true, true, true));
    let owner_state = state.with_auth(&owner);
    let agent = insert(&owner_state);
    script.lock().unwrap().0 = vec![
        message(&json!({"decision":"use","agent_id":agent.id}).to_string()),
        tool_message(),
        message("Tool observation received"),
        message("Final answer after tool"),
    ]
    .into();
    let server = serve(crate::api::routes(state)).await;
    let client = reqwest::Client::new();
    let mut payload = async_payload("Look up the requested information");
    payload["tools"] = json!([{
        "type":"function",
        "function":{"name":"lookup","description":"client lookup","parameters":{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}}
    }]);
    let first = client
        .post(format!("{}/v1/chat/completions", server.url))
        .bearer_auth(&token)
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), reqwest::StatusCode::ACCEPTED);
    let accepted: Value = first.json().await.unwrap();
    let id = accepted["operation"]["id"].as_str().unwrap().to_string();
    wait_operation_status(&client, &server.url, &token, &id, "waiting_tools").await;
    let pending: Value = client
        .get(format!("{}/api/operations/{id}/result", server.url))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let assistant = pending["choices"][0]["message"].clone();
    let tool_id = assistant["tool_calls"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(tool_id.starts_with("ccm-async-agent:"));
    assert!(tool_id.ends_with(&id));
    payload["metadata"]["ccm_operation_id"] = json!(id);
    payload["messages"].as_array_mut().unwrap().extend([
        assistant,
        json!({"role":"tool","tool_call_id":tool_id,"content":"client observation"}),
    ]);
    let resumed = client
        .post(format!("{}/v1/chat/completions", server.url))
        .bearer_auth(&token)
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resumed.status(), reqwest::StatusCode::ACCEPTED);
    let resumed_handle: Value = resumed.json().await.unwrap();
    assert_eq!(resumed_handle["operation"]["id"], id);
    wait_operation_status(&client, &server.url, &token, &id, "completed").await;
    let result: Value = client
        .get(format!("{}/api/operations/{id}/result", server.url))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        result["choices"][0]["message"]["content"],
        "Final answer after tool"
    );
    let calls = script.lock().unwrap();
    assert_eq!(
        calls.1.len(),
        4,
        "resume must not run Foreman routing again"
    );
    assert!(
        calls.1[2]["messages"].as_array().unwrap().last().unwrap()["content"]
            == "client observation"
    );
}

struct GatedProvider {
    agent_id: String,
    calls: Mutex<Vec<Value>>,
    agent_started: Notify,
    release_agent: Semaphore,
    checkin_seen: Notify,
    checkin_count: AtomicUsize,
}

async fn gated_completion(
    State(provider): State<Arc<GatedProvider>>,
    Json(body): Json<Value>,
) -> Response {
    let system = body["messages"][0]["content"].as_str().unwrap_or_default();
    if system.contains("You are Foreman supervising an active asynchronous task") {
        provider.checkin_count.fetch_add(1, Ordering::SeqCst);
        provider.checkin_seen.notify_one();
        return Json(message(
            "Verified progress: the specialist is still working.",
        ))
        .into_response();
    }
    let call = {
        let mut calls = provider.calls.lock().unwrap();
        calls.push(body);
        calls.len() - 1
    };
    match call {
        0 => Json(message(
            &json!({"decision":"use","agent_id":provider.agent_id}).to_string(),
        ))
        .into_response(),
        1 => {
            provider.agent_started.notify_one();
            provider.release_agent.acquire().await.unwrap().forget();
            Json(message("Specialist report after release")).into_response()
        }
        _ => Json(message("Final answer after release")).into_response(),
    }
}

async fn gated_upstream(provider: Arc<GatedProvider>) -> Server {
    serve(
        Router::new()
            .route("/chat/completions", post(gated_completion))
            .with_state(provider),
    )
    .await
}

fn short_checkin_profile() -> crate::async_ops::LongTaskProfile {
    let mut profile = crate::async_ops::LongTaskProfile::default();
    profile.foreman_checkin_ms = 25;
    profile.max_active_ms = 30_000;
    profile
}

async fn wait_for_notify(notify: &Notify, label: &str) {
    tokio::time::timeout(Duration::from_secs(3), notify.notified())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {label}"));
}

async fn wait_receiver_status(
    receiver: &mut tokio::sync::watch::Receiver<crate::async_ops::OperationSnapshot>,
    expected: &str,
) -> crate::async_ops::OperationSnapshot {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let snapshot = receiver.borrow().clone();
            if snapshot.status == expected {
                return snapshot;
            }
            receiver.changed().await.unwrap();
        }
    })
    .await
    .unwrap_or_else(|_| panic!("operation did not reach {expected}"))
}

async fn start_waiting_handoff(
    portfolio: Arc<crate::portfolio::Portfolio>,
    request_id: String,
) -> Result<(String, String), String> {
    let supervisor = portfolio.async_ops.clone();
    let (id_tx, id_rx) = tokio::sync::oneshot::channel();
    let future = async move {
        let operation_id = id_rx.await.map_err(|error| error.to_string())?;
        let entity_id = uuid::Uuid::new_v4().simple().to_string();
        let tool_id = format!("ccm-async-agent:{entity_id}:batch-0:{operation_id}");
        Ok(json!({
            "__ccm_operation_status":"waiting_tools",
            "result":{"completion":{"choices":[{"message":{
                "role":"assistant",
                "content":null,
                "tool_calls":[{"id":tool_id,"type":"function","function":{
                    "name":"lookup","arguments":"{}"
                }}]
            }}]}}
        }))
    }
    .boxed();
    let (handle, mut receiver) = supervisor
        .start_root_boxed(portfolio, request_id, 0, future)
        .await?;
    id_tx
        .send(handle.id.clone())
        .map_err(|error| error.to_string())?;
    let snapshot = wait_receiver_status(&mut receiver, "waiting_tools").await;
    let tool_id = snapshot
        .result
        .as_ref()
        .and_then(|result| result.pointer("/completion/choices/0/message/tool_calls/0/id"))
        .and_then(Value::as_str)
        .ok_or("waiting operation did not retain its tool call")?
        .to_string();
    Ok((handle.id, tool_id))
}

fn handoff_payload(tool_id: &str) -> Value {
    json!({"messages":[
        {"role":"assistant","tool_calls":[{"id":tool_id,"type":"function","function":{"name":"lookup","arguments":"{}"}}]},
        {"role":"tool","tool_call_id":tool_id,"content":"client observation"}
    ]})
}

#[tokio::test]
async fn streamed_async_root_checks_in_and_wakes_on_completion() {
    let state = state("http://127.0.0.1:1", false);
    let (owner, token) = scoped_key(&state, None, permissions(true, true, true));
    let owner_state = state.with_auth(&owner);
    let agent = insert(&owner_state);
    let provider = Arc::new(GatedProvider {
        agent_id: agent.id,
        calls: Mutex::new(Vec::new()),
        agent_started: Notify::new(),
        release_agent: Semaphore::new(0),
        checkin_seen: Notify::new(),
        checkin_count: AtomicUsize::new(0),
    });
    let upstream = gated_upstream(provider.clone()).await;
    let mut state = state;
    Arc::get_mut(&mut state).unwrap().config = Arc::new({
        let mut config = (*state.config).clone();
        config.upstream_base_url = upstream.url.clone();
        config
    });
    state
        .portfolio
        .async_ops
        .set_test_profile(short_checkin_profile());
    let server = serve(crate::api::routes(state)).await;
    let mut payload = async_payload("Write a progress-monitored report");
    payload["stream"] = json!(true);
    let response = reqwest::Client::new()
        .post(format!("{}/v1/chat/completions", server.url))
        .bearer_auth(&token)
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let operation_id = response.headers()["x-ccm-operation-id"]
        .to_str()
        .unwrap()
        .to_string();
    let body_task = tokio::spawn(async move { response.text().await.unwrap() });
    wait_for_notify(&provider.agent_started, "specialist start").await;
    wait_for_notify(&provider.checkin_seen, "Foreman check-in").await;
    assert!(provider.checkin_count.load(Ordering::SeqCst) >= 1);
    provider.release_agent.add_permits(1);
    let body = tokio::time::timeout(Duration::from_secs(5), body_task)
        .await
        .expect("stream did not finish after operation completion")
        .unwrap();
    assert!(body.contains("Verified progress: the specialist is still working."));
    assert!(body.contains("Final answer after release"));
    assert!(body.contains(": ccm.operation "));
    assert!(body.contains(&operation_id));
    assert!(body.ends_with("data: [DONE]\n\n"));
}

#[tokio::test]
async fn dropping_an_active_async_stream_cancels_its_root() {
    let state = state("http://127.0.0.1:1", false);
    let (owner, token) = scoped_key(&state, None, permissions(true, true, true));
    let owner_state = state.with_auth(&owner);
    let agent = insert(&owner_state);
    let provider = Arc::new(GatedProvider {
        agent_id: agent.id,
        calls: Mutex::new(Vec::new()),
        agent_started: Notify::new(),
        release_agent: Semaphore::new(0),
        checkin_seen: Notify::new(),
        checkin_count: AtomicUsize::new(0),
    });
    let upstream = gated_upstream(provider.clone()).await;
    let mut state = state;
    Arc::get_mut(&mut state).unwrap().config = Arc::new({
        let mut config = (*state.config).clone();
        config.upstream_base_url = upstream.url.clone();
        config
    });
    let server = serve(crate::api::routes(state)).await;
    let mut payload = async_payload("Write a cancellable report");
    payload["stream"] = json!(true);
    let response = reqwest::Client::new()
        .post(format!("{}/v1/chat/completions", server.url))
        .bearer_auth(&token)
        .json(&payload)
        .send()
        .await
        .unwrap();
    let id = response.headers()["x-ccm-operation-id"]
        .to_str()
        .unwrap()
        .to_string();
    let mut body = response.bytes_stream();
    let first = tokio::time::timeout(Duration::from_secs(2), body.next())
        .await
        .expect("stream did not open")
        .expect("stream closed before its opening frame")
        .unwrap();
    assert!(!first.is_empty());
    wait_for_notify(&provider.agent_started, "active specialist request").await;
    drop(body);
    provider.release_agent.add_permits(1);
    let cancelled = wait_operation_status(
        &reqwest::Client::new(),
        &server.url,
        &token,
        &id,
        "cancelled",
    )
    .await;
    assert_eq!(cancelled["status"], "cancelled");
}

#[tokio::test]
async fn explicit_cancel_stops_a_nonstreaming_operation() {
    let state = state("http://127.0.0.1:1", false);
    let (owner, token) = scoped_key(&state, None, permissions(true, true, true));
    let owner_state = state.with_auth(&owner);
    let agent = insert(&owner_state);
    let provider = Arc::new(GatedProvider {
        agent_id: agent.id,
        calls: Mutex::new(Vec::new()),
        agent_started: Notify::new(),
        release_agent: Semaphore::new(0),
        checkin_seen: Notify::new(),
        checkin_count: AtomicUsize::new(0),
    });
    let upstream = gated_upstream(provider.clone()).await;
    let mut state = state;
    Arc::get_mut(&mut state).unwrap().config = Arc::new({
        let mut config = (*state.config).clone();
        config.upstream_base_url = upstream.url.clone();
        config
    });
    let server = serve(crate::api::routes(state)).await;
    let response = reqwest::Client::new()
        .post(format!("{}/v1/chat/completions", server.url))
        .bearer_auth(&token)
        .json(&async_payload("Write a cancellable report"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    let accepted: Value = response.json().await.unwrap();
    let id = accepted["operation"]["id"].as_str().unwrap();
    wait_for_notify(&provider.agent_started, "active specialist request").await;
    let cancelled = reqwest::Client::new()
        .delete(format!("{}/api/operations/{id}", server.url))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert!(cancelled.status().is_success());
    provider.release_agent.add_permits(1);
    let status = wait_operation_status(
        &reqwest::Client::new(),
        &server.url,
        &token,
        id,
        "cancelled",
    )
    .await;
    assert_eq!(status["status"], "cancelled");
}

#[tokio::test]
async fn async_client_tool_handoffs_have_global_and_per_workforce_bounds_without_eviction() {
    let base = state("http://127.0.0.1:1", false);
    let supervisor = base.portfolio.async_ops.clone();
    let limits = supervisor.settings().await;
    let global_limit = limits["max_outstanding_client_tool_handoffs"]
        .as_u64()
        .unwrap() as usize;
    let workforce_limit = limits["max_outstanding_client_tool_handoffs_per_workforce"]
        .as_u64()
        .unwrap() as usize;
    let workforce_count = global_limit.div_ceil(workforce_limit) + 1;
    let mut portfolios = Vec::with_capacity(workforce_count);
    for index in 0..workforce_count {
        let (owner, _) = scoped_key(&base, None, permissions(true, true, true));
        let scoped = base.with_auth(&owner);
        portfolios.push((
            owner,
            scoped.portfolio.clone(),
            format!("handoff-bound-{index}"),
        ));
    }

    let mut first_handoff = None;
    let mut accepted = 0usize;
    for slot in 0..workforce_limit {
        let (id, tool_id) =
            start_waiting_handoff(portfolios[0].1.clone(), format!("handoff-bound-a-{slot}"))
                .await
                .unwrap();
        if first_handoff.is_none() {
            first_handoff = Some((id, tool_id));
        }
        accepted += 1;
    }
    assert_eq!(
        start_waiting_handoff(portfolios[0].1.clone(), "handoff-bound-a-over-cap".into())
            .await
            .unwrap_err(),
        "async queue full",
        "one workforce cannot exceed its live handoff quota"
    );

    // A full workforce quota must not consume another workforce's independent
    // allowance while the process-wide handoff cap still has room.
    let (id, _) = start_waiting_handoff(portfolios[1].1.clone(), "handoff-bound-b-0".into())
        .await
        .unwrap();
    assert!(!id.is_empty());
    accepted += 1;
    for slot in 1..workforce_limit {
        start_waiting_handoff(portfolios[1].1.clone(), format!("handoff-bound-b-{slot}"))
            .await
            .unwrap();
        accepted += 1;
    }
    for (_, portfolio, _) in portfolios.iter().skip(2) {
        for _ in 0..workforce_limit.min(global_limit - accepted) {
            start_waiting_handoff(portfolio.clone(), format!("handoff-bound-fill-{accepted}"))
                .await
                .unwrap();
            accepted += 1;
        }
        if accepted == global_limit {
            break;
        }
    }
    assert_eq!(accepted, global_limit);
    assert_eq!(
        supervisor.settings().await["outstanding_client_tool_handoffs"],
        json!(global_limit)
    );

    let (first_id, first_tool_id) = first_handoff.unwrap();
    let first_status = supervisor
        .status(&portfolios[0].1, &first_id)
        .await
        .unwrap();
    assert_eq!(first_status.status, "waiting_tools");
    assert!(first_status.result_available);
    assert_eq!(
        supervisor
            .validate_tool_handoff(
                &portfolios[0].1,
                &handoff_payload(&first_tool_id),
                Some(&first_id),
            )
            .await
            .unwrap()
            .as_deref(),
        Some(first_id.as_str()),
        "filling the quota must preserve accepted live continuation ownership"
    );
    assert_eq!(
        start_waiting_handoff(
            portfolios.last().unwrap().1.clone(),
            "handoff-bound-global-over-cap".into()
        )
        .await
        .unwrap_err(),
        "async queue full",
        "the global handoff quota rejects new work instead of evicting live state"
    );
}

#[tokio::test]
async fn handoff_expiry_uses_one_shared_worker_and_stops_when_idle() {
    let base = state("http://127.0.0.1:1", false);
    let supervisor = base.portfolio.async_ops.clone();
    let mut profile = crate::async_ops::LongTaskProfile::default();
    profile.tool_handoff_ttl_ms = 300;
    supervisor.set_test_profile(profile);

    let mut receivers = Vec::new();
    for index in 0..3 {
        let (id, _) =
            start_waiting_handoff(base.portfolio.clone(), format!("shared-expiry-{index}"))
                .await
                .unwrap();
        receivers.push(
            supervisor
                .subscribe_internal(&base.portfolio, &id)
                .await
                .unwrap(),
        );
    }
    assert_eq!(
        supervisor.settings().await["handoff_expiry_worker_running"],
        true,
        "all handoffs share one expiry worker rather than spawning a sleeper each"
    );
    for receiver in &mut receivers {
        let expired = wait_receiver_status(receiver, "expired").await;
        assert_eq!(
            expired.error.as_deref(),
            Some("client tool handoff expired")
        );
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if supervisor.settings().await["handoff_expiry_worker_running"] == false {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("shared expiry worker did not stop after the last handoff expired");
    assert_eq!(
        supervisor.settings().await["outstanding_client_tool_handoffs"],
        0
    );
}

#[tokio::test]
async fn handoff_expiry_worker_stops_after_the_last_handoff_is_cancelled() {
    let base = state("http://127.0.0.1:1", false);
    let supervisor = base.portfolio.async_ops.clone();
    let (id, _) = start_waiting_handoff(base.portfolio.clone(), "cancel-expiry-worker".into())
        .await
        .unwrap();
    assert_eq!(
        supervisor.settings().await["handoff_expiry_worker_running"],
        true
    );

    let cancelled = supervisor.cancel(&base.portfolio, &id).await.unwrap();
    assert_eq!(cancelled.status, "cancelled");
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if supervisor.settings().await["handoff_expiry_worker_running"] == false {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("cancellation did not wake and stop the idle expiry worker");
    assert_eq!(
        supervisor.settings().await["outstanding_client_tool_handoffs"],
        0
    );
}

#[tokio::test]
async fn handoff_reservations_cover_active_and_queued_roots_and_release_on_cancel() {
    let base = state("http://127.0.0.1:1", false);
    let (owner, _) = scoped_key(&base, None, permissions(true, true, true));
    let scoped = base.with_auth(&owner);
    let supervisor = base.portfolio.async_ops.clone();
    let workforce_limit =
        supervisor.settings().await["max_outstanding_client_tool_handoffs_per_workforce"]
            .as_u64()
            .unwrap() as usize;
    let live_handoffs = workforce_limit - 4;
    for index in 0..live_handoffs {
        start_waiting_handoff(
            scoped.portfolio.clone(),
            format!("reservation-live-{index}"),
        )
        .await
        .unwrap();
    }
    supervisor
        .set_workforce_limits(
            owner.workforce_id.clone(),
            crate::async_ops::WorkforceLimits {
                max_root_operations: Some(1),
                max_queued_roots: Some(3),
                max_queue_wait_seconds: Some(30),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let active_gate = Arc::new(Semaphore::new(0));
    let active_gate_worker = active_gate.clone();
    let (active, mut active_rx) = supervisor
        .start_root(
            scoped.portfolio.clone(),
            "reservation-active".into(),
            0,
            async move {
                active_gate_worker.acquire().await.unwrap().forget();
                Ok(json!({"ok":true}))
            },
        )
        .await
        .unwrap();
    assert_eq!(active.status, "running");

    let mut queued = Vec::new();
    for index in 0..3 {
        let gate = Arc::new(Semaphore::new(0));
        let gate_worker = gate.clone();
        let (handle, receiver) = supervisor
            .start_root(
                scoped.portfolio.clone(),
                format!("reservation-queued-{index}"),
                0,
                async move {
                    gate_worker.acquire().await.unwrap().forget();
                    Ok(json!({"ok":true}))
                },
            )
            .await
            .unwrap();
        assert_eq!(handle.status, "queued");
        queued.push((handle, receiver));
    }
    let settings = supervisor.settings().await;
    assert_eq!(
        settings["outstanding_client_tool_handoffs"],
        json!(live_handoffs)
    );
    assert_eq!(settings["reserved_client_tool_handoff_slots"], 4);
    assert_eq!(
        settings["client_tool_handoff_capacity_in_use"],
        json!(workforce_limit)
    );
    assert_eq!(
        supervisor
            .start_root(
                scoped.portfolio.clone(),
                "reservation-over-cap".into(),
                0,
                async { Ok(json!({"unexpected":"started"})) }
            )
            .await
            .unwrap_err(),
        "async queue full",
        "active and queued candidates consume their handoff capacity before completion"
    );

    supervisor
        .cancel(&scoped.portfolio, &active.id)
        .await
        .unwrap();
    wait_receiver_status(&mut active_rx, "cancelled").await;
    for (handle, receiver) in &mut queued {
        supervisor
            .cancel(&scoped.portfolio, &handle.id)
            .await
            .unwrap();
        wait_receiver_status(receiver, "cancelled").await;
    }
    assert_eq!(
        supervisor.settings().await["reserved_client_tool_handoff_slots"],
        0,
        "cancellation releases candidate reservations"
    );
}

#[tokio::test]
async fn partial_async_tool_batches_do_not_consume_the_original_handoff() {
    let base = state("http://127.0.0.1:1", false);
    let supervisor = base.portfolio.async_ops.clone();
    let (id_tx, id_rx) = tokio::sync::oneshot::channel();
    let future = async move {
        let operation_id = id_rx.await.map_err(|error| error.to_string())?;
        let entity_id = uuid::Uuid::new_v4().simple().to_string();
        let calls = (0..2)
            .map(|index| {
                json!({
                    "id":format!("ccm-async-agent:{entity_id}:batch-{index}:{operation_id}"),
                    "type":"function",
                    "function":{"name":"lookup","arguments":"{}"}
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({
            "__ccm_operation_status":"waiting_tools",
            "result":{"completion":{"choices":[{"message":{
                "role":"assistant","content":null,"tool_calls":calls
            }}]}}
        }))
    };
    let (handle, mut receiver) = supervisor
        .start_root(base.portfolio.clone(), "partial-handoff".into(), 0, future)
        .await
        .unwrap();
    id_tx.send(handle.id.clone()).unwrap();
    let waiting = wait_receiver_status(&mut receiver, "waiting_tools").await;
    let calls = waiting
        .result
        .as_ref()
        .unwrap()
        .pointer("/completion/choices/0/message/tool_calls")
        .and_then(Value::as_array)
        .unwrap();
    let tool_ids = calls
        .iter()
        .map(|call| call["id"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    let partial = json!({"messages":[
        {"role":"assistant","tool_calls":[calls[0].clone()]},
        {"role":"tool","tool_call_id":tool_ids[0],"content":"only one result"}
    ]});
    let rejected = supervisor
        .validate_tool_handoff(&base.portfolio, &partial, Some(&handle.id))
        .await;
    assert_eq!(
        rejected.unwrap_err(),
        "tool results do not exactly cover the async handoff"
    );
    assert_eq!(
        supervisor
            .status(&base.portfolio, &handle.id)
            .await
            .unwrap()
            .status,
        "waiting_tools",
        "a partial result batch cannot consume the continuation"
    );

    let complete = json!({"messages":[
        {"role":"assistant","tool_calls":calls.clone()},
        {"role":"tool","tool_call_id":tool_ids[0],"content":"first result"},
        {"role":"tool","tool_call_id":tool_ids[1],"content":"second result"}
    ]});
    assert_eq!(
        supervisor
            .validate_tool_handoff(&base.portfolio, &complete, Some(&handle.id))
            .await
            .unwrap()
            .as_deref(),
        Some(handle.id.as_str())
    );
}

#[tokio::test]
async fn async_retained_results_reclaim_old_completions_with_explicit_expiry() {
    let portfolio =
        Arc::new(crate::portfolio::Portfolio::open_budget(":memory:", 2 * 1024 * 1024).unwrap());
    let (owner, _) = portfolio
        .create_key(
            "small result budget",
            None,
            "small result budget",
            &crate::auth::KeyPermissions {
                roster_use: true,
                roster_read: true,
                roster_write: true,
                ..Default::default()
            },
        )
        .unwrap();
    let scoped = portfolio.with_scope(&owner);
    let supervisor = scoped.async_ops.clone();
    let result_limit = scoped.delivery_budget.limit() / 2;
    let mut payload = "x".repeat((result_limit - 512) / 4);
    let (large_result, result_cost) = loop {
        let result = json!({"payload":payload.clone()});
        let cost = result.to_string().len() * 4 + 512;
        if cost > result_limit {
            payload.pop();
        } else if result_limit - cost >= 512 {
            payload.push('x');
        } else {
            break (result, cost);
        }
    };
    assert!(result_cost <= result_limit);
    assert!(result_limit - result_cost < 512);

    let (first, mut receiver) = supervisor
        .start_root(
            scoped.clone(),
            "large-retained-result".into(),
            0,
            async move { Ok(large_result) },
        )
        .await
        .unwrap();
    let completed = wait_receiver_status(&mut receiver, "completed").await;
    assert!(completed.result_available);
    assert_eq!(
        supervisor.settings().await["retained_async_result_bytes"],
        json!(result_cost)
    );
    assert_eq!(
        supervisor.settings().await["retained_async_result_limit_bytes"],
        json!(result_limit)
    );

    let (live_handoff_id, _) =
        start_waiting_handoff(scoped.clone(), "result-budget-reclaim-live-handoff".into())
            .await
            .unwrap();

    let reclaimed = supervisor.status(&scoped, &first.id).await.unwrap();
    assert_eq!(reclaimed.status, "result_expired");
    assert!(!reclaimed.result_available);
    assert!(
        reclaimed
            .error
            .as_deref()
            .is_some_and(|error| error.contains("retained-memory pressure")),
        "expired results report why they are no longer available"
    );
    let retained_bytes = supervisor.settings().await["retained_async_result_bytes"]
        .as_u64()
        .unwrap();
    assert!(retained_bytes < result_limit as u64);
    let live_handoff = supervisor.status(&scoped, &live_handoff_id).await.unwrap();
    assert_eq!(live_handoff.status, "waiting_tools");
    assert!(live_handoff.result_available);

    let (_, mut second_rx) = supervisor
        .start_root(
            scoped.clone(),
            "result-budget-after-reclaim".into(),
            0,
            async { Ok(json!({"small":"result"})) },
        )
        .await
        .unwrap();
    let second_result = wait_receiver_status(&mut second_rx, "completed").await;
    assert!(second_result.result_available);

    let oversized = json!({"payload":"x".repeat(result_limit)});
    let (_, mut oversized_rx) = supervisor
        .start_root(
            scoped.clone(),
            "single-result-too-large".into(),
            0,
            async move { Ok(oversized) },
        )
        .await
        .unwrap();
    let failed = wait_receiver_status(&mut oversized_rx, "error").await;
    assert!(
        failed
            .error
            .as_deref()
            .is_some_and(|error| error.contains("retained result memory budget full")),
        "a single result beyond the byte ceiling fails explicitly"
    );
    assert_eq!(second_result.status, "completed");
}

#[tokio::test]
async fn resumed_approval_result_expiry_clears_all_flows_and_cancels_held_work() {
    let portfolio =
        Arc::new(crate::portfolio::Portfolio::open_budget(":memory:", 2 * 1024 * 1024).unwrap());
    let (owner, _) = portfolio
        .create_key(
            "approval result reclamation",
            None,
            "approval result reclamation workforce",
            &crate::auth::KeyPermissions {
                roster_use: true,
                roster_read: true,
                roster_write: true,
                improvement_contribute: true,
                ..Default::default()
            },
        )
        .unwrap();
    let scoped = portfolio.with_scope(&owner);
    let supervisor = scoped.async_ops.clone();

    let (operation_id_tx, operation_id_rx) = tokio::sync::oneshot::channel();
    let (root_approval_tx, root_approval_rx) = tokio::sync::oneshot::channel();
    let root_supervisor = supervisor.clone();
    let root_portfolio = scoped.clone();
    let root_request = "reclaim-approval-root".to_string();
    let conversation = "reclaim-result-conversation";
    let root_payload = json!({
        "metadata":{"ccm_conversation_id":conversation},
        "messages":[{"role":"user","content":"finish the approved request"}]
    });
    let root_run = scoped
        .trace_begin(&root_request, &root_payload, "external")
        .unwrap();
    let root_generation = scoped.generation();
    scoped
        .queue_improvement(
            &root_request,
            "agent",
            &uuid::Uuid::new_v4().to_string(),
            &json!({"kind":"agent"}),
            "begin the approved request",
            "root response",
            root_generation,
        )
        .unwrap();
    scoped
        .queue_improvement(
            &root_request,
            "workforce",
            &owner.workforce_id,
            &json!({"kind":"workforce"}),
            "review the root response policy",
            "root policy",
            root_generation,
        )
        .unwrap();
    let root_request_worker = root_request.clone();
    let root_future = async move {
        let operation_id = operation_id_rx.await.map_err(|error| error.to_string())?;
        let pending = root_supervisor
            .register_tool_approvals(
                &root_portfolio,
                &root_request_worker,
                &root_run,
                vec![crate::async_ops::ToolApprovalRequest {
                    target_name: "ccm_file_write".into(),
                    target: crate::runtime::tools::builtin_target("file.write").unwrap(),
                    tool: "file.write".into(),
                    arguments: json!({"path":"root.txt","content":"approved"}),
                    agent_id: "reclaim-agent".into(),
                    stage_id: None,
                }],
            )
            .await?;
        let approval_id = pending[0].handle.id.clone();
        root_supervisor
            .decide_tool_approval(
                &root_portfolio,
                &approval_id,
                crate::async_ops::ToolApprovalDecision::Approved,
            )
            .await?;
        root_approval_tx
            .send(approval_id)
            .map_err(|error| error.to_string())?;

        let entity_id = uuid::Uuid::new_v4().simple().to_string();
        let tool_id = format!("ccm-async-agent:{entity_id}:batch-0:{operation_id}");
        let tool_call = json!({
            "id":tool_id,"type":"function","function":{"name":"lookup","arguments":"{}"}
        });
        let assistant_message = json!({"role":"assistant","content":null,"tool_calls":[tool_call]});
        root_portfolio.trace_bind_tools(&root_request_worker, &assistant_message)?;
        Ok(json!({
            "__ccm_operation_status":"waiting_tools",
            "result":{"completion":{"choices":[{"message":{
                "role":"assistant","content":null,"tool_calls":[tool_call]
            }}]}}
        }))
    };
    let (operation, mut operation_rx) = supervisor
        .start_root(scoped.clone(), root_request.clone(), 0, root_future)
        .await
        .unwrap();
    operation_id_tx.send(operation.id.clone()).unwrap();
    let waiting = wait_receiver_status(&mut operation_rx, "waiting_tools").await;
    let root_approval_id = root_approval_rx.await.unwrap();
    let tool_id = waiting
        .result
        .as_ref()
        .and_then(|result| result.pointer("/completion/choices/0/message/tool_calls/0/id"))
        .and_then(Value::as_str)
        .unwrap()
        .to_string();
    let root_flow = supervisor
        .tool_approval_result(&scoped, &root_approval_id)
        .await
        .unwrap();
    assert_eq!(root_flow.0, "completed");
    assert!(
        root_flow.1.is_none(),
        "async flows do not retain an unbudgeted result clone"
    );

    let result_limit = scoped.delivery_budget.limit() / 2;
    let mut payload = "x".repeat((result_limit - 512) / 4);
    let (large_result, result_cost) = loop {
        let result = json!({"payload":payload.clone()});
        let cost = result.to_string().len() * 4 + 512;
        if cost > result_limit {
            payload.pop();
        } else if result_limit - cost >= 512 {
            payload.push('x');
        } else {
            break (result, cost);
        }
    };

    let resume_request = "reclaim-approval-resume".to_string();
    let resume_payload = json!({
        "metadata":{"ccm_conversation_id":conversation},
        "messages":[
            {"role":"user","content":"finish the approved request"},
            {"role":"assistant","tool_calls":[{
                "id":tool_id,"type":"function","function":{"name":"lookup","arguments":"{}"}
            }]},
            {"role":"tool","tool_call_id":tool_id,"content":"client observation"}
        ]
    });
    let resume_supervisor = supervisor.clone();
    let resume_portfolio = scoped.clone();
    let resume_request_worker = resume_request.clone();
    let resume_payload_worker = resume_payload.clone();
    let (resume_approval_tx, resume_approval_rx) = tokio::sync::oneshot::channel();
    let resume_future = async move {
        let run_id = resume_portfolio
            .trace_begin(&resume_request_worker, &resume_payload_worker, "external")
            .map_err(|error| error.to_string())?;
        let generation = resume_portfolio.generation();
        resume_portfolio.queue_improvement(
            &resume_request_worker,
            "agent",
            &uuid::Uuid::new_v4().to_string(),
            &json!({"kind":"agent"}),
            "complete the approved request",
            "completed response",
            generation,
        )?;
        resume_portfolio.queue_improvement(
            &resume_request_worker,
            "workforce",
            &owner.workforce_id,
            &json!({"kind":"workforce"}),
            "review policy after the response",
            "completed response",
            generation,
        )?;
        resume_portfolio
            .capture_conversation_turn(&resume_request_worker, &resume_payload_worker)?;

        let pending = resume_supervisor
            .register_tool_approvals(
                &resume_portfolio,
                &resume_request_worker,
                &run_id,
                vec![crate::async_ops::ToolApprovalRequest {
                    target_name: "ccm_file_write".into(),
                    target: crate::runtime::tools::builtin_target("file.write").unwrap(),
                    tool: "file.write".into(),
                    arguments: json!({"path":"resumed.txt","content":"approved"}),
                    agent_id: "reclaim-resumed-agent".into(),
                    stage_id: Some("resume".into()),
                }],
            )
            .await?;
        let approval_id = pending[0].handle.id.clone();
        resume_supervisor
            .decide_tool_approval(
                &resume_portfolio,
                &approval_id,
                crate::async_ops::ToolApprovalDecision::Approved,
            )
            .await?;
        resume_approval_tx
            .send(approval_id)
            .map_err(|error| error.to_string())?;

        let mut gate = crate::improvement::ResponseGate::new(
            resume_portfolio.clone(),
            resume_request_worker,
            crate::improvement::ImprovementConfig::default(),
        );
        gate.defer();
        drop(gate);
        Ok(large_result)
    };
    let (resumed, mut resumed_rx) = supervisor
        .resume_root_boxed(
            scoped.clone(),
            &operation.id,
            resume_request.clone(),
            &resume_payload,
            resume_future.boxed(),
        )
        .await
        .unwrap();
    assert_eq!(resumed.status, "running");
    let completed = wait_receiver_status(&mut resumed_rx, "completed").await;
    assert!(completed.result_available);
    assert_eq!(
        supervisor.settings().await["retained_async_result_bytes"],
        json!(result_cost)
    );
    let resume_approval_id = resume_approval_rx.await.unwrap();
    let resume_flow = supervisor
        .tool_approval_result(&scoped, &resume_approval_id)
        .await
        .unwrap();
    assert_eq!(resume_flow.0, "completed");
    assert!(resume_flow.1.is_none());

    let (_, mut pressure_rx) = supervisor
        .start_root(scoped.clone(), "reclaim-result-pressure".into(), 0, async {
            Ok(json!({"small":"result"}))
        })
        .await
        .unwrap();
    wait_receiver_status(&mut pressure_rx, "completed").await;
    assert_eq!(
        supervisor
            .status(&scoped, &operation.id)
            .await
            .unwrap()
            .status,
        "result_expired"
    );

    for approval_id in [&root_approval_id, &resume_approval_id] {
        let status = supervisor
            .tool_approval_status(&scoped, approval_id)
            .await
            .unwrap();
        assert_eq!(status["flow"]["status"], "result_expired");
        assert!(
            status["flow"]["error"]
                .as_str()
                .is_some_and(|error| error.contains("retained-memory pressure"))
        );
        assert!(
            supervisor
                .tool_approval_result(&scoped, approval_id)
                .await
                .unwrap()
                .1
                .is_none()
        );
    }

    let conn = scoped.conn.lock().unwrap();
    let cancelled_jobs: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM improvement_jobs WHERE origin_request IN (?1,?2) AND status='cancelled' AND job_type IN ('response','policy')",
            rusqlite::params![root_request, resume_request],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        cancelled_jobs, 4,
        "response and policy jobs from both requests in the run are cancelled on expiry"
    );
    let turn_status: String = conn
        .query_row(
            "SELECT status FROM improvement_turns WHERE request_id=?1",
            [&resume_request],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        turn_status, "cancelled",
        "held turn state is cancelled on expiry"
    );
    let response_status: String = conn
        .query_row(
            "SELECT status FROM improvement_responses WHERE request_id=?1",
            [&resume_request],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(response_status, "cancelled");
    let root_response_status: String = conn
        .query_row(
            "SELECT status FROM improvement_responses WHERE request_id=?1",
            [&root_request],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        root_response_status, "cancelled",
        "result expiry also resolves the original request gate"
    );
}

#[tokio::test]
async fn async_approval_client_tool_result_read_retries_before_releasing_run_holds() {
    let base = state("http://127.0.0.1:1", false);
    let (owner, _) = scoped_key(
        &base,
        None,
        crate::auth::KeyPermissions {
            roster_use: true,
            roster_read: true,
            roster_write: true,
            improvement_contribute: true,
            ..Default::default()
        },
    );
    let scoped = base.with_auth(&owner);
    let supervisor = scoped.portfolio.async_ops.clone();
    let conversation = format!("async-result-gates-{}", uuid::Uuid::new_v4());
    let root_request = format!("async-result-root-{}", uuid::Uuid::new_v4());
    let root_payload = json!({
        "metadata":{"ccm_conversation_id":conversation.clone()},
        "messages":[{"role":"user","content":"approve the lookup and report the result"}]
    });
    let run_id = scoped
        .portfolio
        .trace_begin(&root_request, &root_payload, "external")
        .unwrap();
    let generation = scoped.portfolio.generation();
    scoped
        .portfolio
        .queue_improvement(
            &root_request,
            "agent",
            &uuid::Uuid::new_v4().to_string(),
            &json!({"kind":"agent"}),
            "approve the lookup and report the result",
            "root assessment",
            generation,
        )
        .unwrap();
    scoped
        .portfolio
        .queue_improvement(
            &root_request,
            "workforce",
            &owner.workforce_id,
            &json!({"kind":"workforce"}),
            "review the root policy response",
            "root policy assessment",
            generation,
        )
        .unwrap();
    scoped
        .portfolio
        .capture_conversation_turn(&root_request, &root_payload)
        .unwrap();

    let (operation_id_tx, operation_id_rx) = tokio::sync::oneshot::channel();
    let (approval_id_tx, approval_id_rx) = tokio::sync::oneshot::channel();
    let root_supervisor = supervisor.clone();
    let root_portfolio = scoped.portfolio.clone();
    let root_request_worker = root_request.clone();
    let root_future = async move {
        let operation_id = operation_id_rx.await.map_err(|error| error.to_string())?;
        let pending = root_supervisor
            .register_tool_approvals(
                &root_portfolio,
                &root_request_worker,
                &run_id,
                vec![crate::async_ops::ToolApprovalRequest {
                    target_name: "ccm_file_write".into(),
                    target: crate::runtime::tools::builtin_target("file.write").unwrap(),
                    tool: "file.write".into(),
                    arguments: json!({"path":"approved-before-lookup.txt","content":"approved"}),
                    agent_id: "async-result-gate-agent".into(),
                    stage_id: Some("approve".into()),
                }],
            )
            .await?;
        let pending = pending.into_iter().next().unwrap();
        let approval_id = pending.handle.id.clone();
        root_supervisor
            .decide_tool_approval(
                &root_portfolio,
                &approval_id,
                crate::async_ops::ToolApprovalDecision::Approved,
            )
            .await?;
        if root_supervisor
            .wait_for_tool_approval(&root_portfolio, pending)
            .await?
            != crate::async_ops::ToolApprovalDecision::Approved
        {
            return Err("expected approval before client-tool handoff".into());
        }
        approval_id_tx
            .send(approval_id)
            .map_err(|error| error.to_string())?;

        let entity_id = uuid::Uuid::new_v4().simple().to_string();
        let tool_id = format!("ccm-async-agent:{entity_id}:batch-0:{operation_id}");
        let tool_call = json!({
            "id":tool_id,"type":"function","function":{"name":"lookup","arguments":"{}"}
        });
        let assistant_message = json!({"role":"assistant","content":null,"tool_calls":[tool_call]});
        root_portfolio.trace_bind_tools(&root_request_worker, &assistant_message)?;
        Ok(json!({
            "__ccm_operation_status":"waiting_tools",
            "result":{"completion":{"choices":[{"message":{
                "role":"assistant","content":null,"tool_calls":[tool_call]
            },"finish_reason":"tool_calls"}]}}
        }))
    };
    let (operation, mut operation_rx) = supervisor
        .start_root(
            scoped.portfolio.clone(),
            root_request.clone(),
            0,
            root_future,
        )
        .await
        .unwrap();
    operation_id_tx.send(operation.id.clone()).unwrap();
    let waiting = wait_receiver_status(&mut operation_rx, "waiting_tools").await;
    let approval_id = approval_id_rx.await.unwrap();
    let approval = supervisor
        .tool_approval_status(&scoped.portfolio, &approval_id)
        .await
        .unwrap();
    assert_eq!(approval["flow"]["operation_id"], operation.id);
    assert_eq!(approval["flow"]["status"], "completed");

    let tool_id = waiting
        .result
        .as_ref()
        .and_then(|result| result.pointer("/completion/choices/0/message/tool_calls/0/id"))
        .and_then(Value::as_str)
        .unwrap()
        .to_string();
    let resume_request = format!("async-result-resume-{}", uuid::Uuid::new_v4());
    let workforce_id = owner.workforce_id.clone();
    let resume_payload = json!({
        "metadata":{"ccm_conversation_id":conversation.clone()},
        "messages":[
            {"role":"user","content":"approve the lookup and report the result"},
            {"role":"assistant","tool_calls":[{
                "id":tool_id,"type":"function","function":{"name":"lookup","arguments":"{}"}
            }]},
            {"role":"tool","tool_call_id":tool_id,"content":"the record is ready"}
        ]
    });
    let resume_portfolio = scoped.portfolio.clone();
    let resume_payload_worker = resume_payload.clone();
    let resume_request_worker = resume_request.clone();
    let resume_future = async move {
        resume_portfolio
            .trace_begin(&resume_request_worker, &resume_payload_worker, "external")
            .map_err(|error| error.to_string())?;
        let generation = resume_portfolio.generation();
        resume_portfolio.queue_improvement(
            &resume_request_worker,
            "agent",
            &uuid::Uuid::new_v4().to_string(),
            &json!({"kind":"agent"}),
            "continue after client tool result",
            "resumed response assessment",
            generation,
        )?;
        resume_portfolio.queue_improvement(
            &resume_request_worker,
            "workforce",
            &workforce_id,
            &json!({"kind":"workforce"}),
            "review the resumed policy response",
            "resumed policy assessment",
            generation,
        )?;
        Ok(json!({"completion":{"choices":[{"message":{
            "role":"assistant","content":"Final response after approval and client lookup."
        },"finish_reason":"stop"}]}}))
    };
    let (resumed, mut resumed_rx) = supervisor
        .resume_root_boxed(
            scoped.portfolio.clone(),
            &operation.id,
            resume_request.clone(),
            &resume_payload,
            resume_future.boxed(),
        )
        .await
        .unwrap();
    assert_eq!(resumed.status, "running");
    let completed = wait_receiver_status(&mut resumed_rx, "completed").await;
    assert!(completed.result_available);
    let (delivery_snapshot, latest_result_request) = supervisor
        .result_delivery(&scoped.portfolio, &operation.id)
        .await
        .unwrap();
    assert_eq!(delivery_snapshot.status, "completed");
    assert_eq!(
        latest_result_request.as_deref(),
        Some(resume_request.as_str())
    );
    let root_trace = scoped
        .portfolio
        .trace_context(&delivery_snapshot.root_request_id)
        .unwrap()
        .unwrap()
        .0;
    let latest_trace = scoped
        .portfolio
        .trace_context(latest_result_request.as_deref().unwrap())
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(
        root_trace, latest_trace,
        "the latest request belongs to the root run"
    );

    let held_count = |request: &str| -> i64 {
        scoped
            .portfolio
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM improvement_jobs WHERE origin_request=?1 AND status='held'",
                [request],
                |row| row.get(0),
            )
            .unwrap()
    };
    assert_eq!(held_count(&root_request), 2);
    assert_eq!(held_count(&resume_request), 2);

    let abandoned = crate::async_ops::get_result(
        State(base.clone()),
        axum::Extension(owner.clone()),
        axum::extract::Path(operation.id.clone()),
    )
    .await
    .unwrap()
    .into_response();
    assert_eq!(abandoned.status(), reqwest::StatusCode::OK);
    let mut abandoned_body = abandoned.into_body().into_data_stream();
    let first_chunk = abandoned_body
        .next()
        .await
        .expect("result body should yield the completion")
        .unwrap();
    assert!(String::from_utf8_lossy(&first_chunk).contains("Final response after approval"));
    drop(abandoned_body);
    assert_eq!(
        held_count(&root_request),
        2,
        "abandoning the final body retains root holds"
    );
    assert_eq!(
        held_count(&resume_request),
        2,
        "abandoning the final body retains continuation holds for retry"
    );

    let retry = crate::async_ops::get_result(
        State(base),
        axum::Extension(owner),
        axum::extract::Path(operation.id),
    )
    .await
    .unwrap()
    .into_response();
    let mut retry_body = retry.into_body().into_data_stream();
    let mut retry_bytes = Vec::new();
    while let Some(chunk) = retry_body.next().await {
        retry_bytes.extend_from_slice(&chunk.unwrap());
    }
    let final_completion: Value = serde_json::from_slice(&retry_bytes).unwrap();
    assert_eq!(
        final_completion["choices"][0]["message"]["content"],
        "Final response after approval and client lookup."
    );
    assert_eq!(
        held_count(&root_request),
        0,
        "consumed retry releases root holds"
    );
    assert_eq!(
        held_count(&resume_request),
        0,
        "consumed retry releases continuation holds in the same trace run"
    );

    let response_rows: i64 = scoped
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM improvement_responses WHERE request_id IN (?1,?2) AND status='released'",
            rusqlite::params![root_request, resume_request],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        response_rows, 2,
        "each request in the resumed run is released"
    );
}

#[tokio::test]
async fn synchronous_approval_final_result_releases_run_gates_after_body_consumption() {
    let base = state("http://127.0.0.1:1", false);
    let (owner, _) = scoped_key(
        &base,
        None,
        crate::auth::KeyPermissions {
            roster_use: true,
            roster_read: true,
            roster_write: true,
            improvement_contribute: true,
            ..Default::default()
        },
    );
    let scoped = base.with_auth(&owner);
    let supervisor = scoped.portfolio.async_ops.clone();
    let conversation = format!("approval-result-gates-{}", uuid::Uuid::new_v4());
    let root_request = format!("approval-result-root-{}", uuid::Uuid::new_v4());
    let continuation_request = format!("approval-result-continuation-{}", uuid::Uuid::new_v4());
    let user_message = json!({"role":"user","content":"look up and approve the answer"});
    let root_payload = json!({
        "metadata":{"ccm_conversation_id":conversation.clone()},
        "messages":[user_message.clone()]
    });
    let run_id = scoped
        .portfolio
        .trace_begin(&root_request, &root_payload, "external")
        .unwrap();
    let generation = scoped.portfolio.generation();
    for (kind, entity, task) in [
        (
            "agent",
            uuid::Uuid::new_v4().to_string(),
            "hold root response work",
        ),
        (
            "workforce",
            owner.workforce_id.clone(),
            "hold root policy work",
        ),
    ] {
        scoped
            .portfolio
            .queue_improvement(
                &root_request,
                kind,
                &entity,
                &json!({"kind":kind}),
                task,
                "root result",
                generation,
            )
            .unwrap();
    }

    let tool_id = format!("approval-client-call-{}", uuid::Uuid::new_v4());
    let assistant_message = json!({
        "role":"assistant",
        "tool_calls":[{"id":tool_id,"type":"function","function":{"name":"lookup","arguments":"{}"}}]
    });
    scoped
        .portfolio
        .trace_bind_tools(&root_request, &assistant_message)
        .unwrap();
    scoped
        .portfolio
        .trace_finish_request(&root_request, "waiting_tools", &assistant_message)
        .unwrap();

    let continuation_payload = json!({
        "metadata":{"ccm_conversation_id":conversation},
        "messages":[
            user_message,
            assistant_message,
            {"role":"tool","tool_call_id":tool_id,"content":"the answer is ready"}
        ]
    });
    assert_eq!(
        scoped
            .portfolio
            .trace_begin(&continuation_request, &continuation_payload, "external")
            .unwrap(),
        run_id,
        "the approval continuation must stay in the original trace run"
    );
    for (kind, entity, task) in [
        (
            "agent",
            uuid::Uuid::new_v4().to_string(),
            "hold continuation response work",
        ),
        (
            "workforce",
            owner.workforce_id.clone(),
            "hold continuation policy work",
        ),
    ] {
        scoped
            .portfolio
            .queue_improvement(
                &continuation_request,
                kind,
                &entity,
                &json!({"kind":kind}),
                task,
                "continuation result",
                generation,
            )
            .unwrap();
    }

    let pending = supervisor
        .register_tool_approvals(
            &scoped.portfolio,
            &continuation_request,
            &run_id,
            vec![crate::async_ops::ToolApprovalRequest {
                target_name: "ccm_file_write".into(),
                target: crate::runtime::tools::builtin_target("file.write").unwrap(),
                tool: "file.write".into(),
                arguments: json!({"path":"approval-result.txt","content":"approved"}),
                agent_id: "approval-result-agent".into(),
                stage_id: None,
            }],
        )
        .await
        .unwrap()
        .remove(0);
    let approval_id = pending.handle.id.clone();
    supervisor
        .decide_tool_approval(
            &scoped.portfolio,
            &approval_id,
            crate::async_ops::ToolApprovalDecision::Approved,
        )
        .await
        .unwrap();
    assert_eq!(
        supervisor
            .wait_for_tool_approval(&scoped.portfolio, pending)
            .await
            .unwrap(),
        crate::async_ops::ToolApprovalDecision::Approved
    );
    let final_result = json!({"completion":{"choices":[{"message":{
        "role":"assistant","content":"Final response after synchronous approval."
    },"finish_reason":"stop"}]}});
    scoped
        .portfolio
        .trace_finish_request(&continuation_request, "completed", &final_result)
        .unwrap();
    supervisor
        .finish_tool_approval_flow(
            &scoped.portfolio,
            &continuation_request,
            Some(final_result),
            None,
        )
        .await;

    let held_count = |request: &str| -> i64 {
        scoped
            .portfolio
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM improvement_jobs WHERE origin_request=?1 AND status='held'",
                [request],
                |row| row.get(0),
            )
            .unwrap()
    };
    assert_eq!(held_count(&root_request), 2);
    assert_eq!(held_count(&continuation_request), 2);

    let abandoned = crate::async_ops::get_tool_approval_result(
        State(base.clone()),
        axum::Extension(owner.clone()),
        axum::extract::Path(approval_id.clone()),
    )
    .await
    .unwrap()
    .into_response();
    let mut abandoned_body = abandoned.into_body().into_data_stream();
    assert!(abandoned_body.next().await.unwrap().unwrap().len() > 0);
    drop(abandoned_body);
    assert_eq!(held_count(&root_request), 2);
    assert_eq!(held_count(&continuation_request), 2);

    let retry = crate::async_ops::get_tool_approval_result(
        State(base),
        axum::Extension(owner),
        axum::extract::Path(approval_id),
    )
    .await
    .unwrap()
    .into_response();
    let mut retry_body = retry.into_body().into_data_stream();
    let mut result_bytes = Vec::new();
    while let Some(chunk) = retry_body.next().await {
        result_bytes.extend_from_slice(&chunk.unwrap());
    }
    let result: Value = serde_json::from_slice(&result_bytes).unwrap();
    assert_eq!(
        result["completion"]["choices"][0]["message"]["content"],
        "Final response after synchronous approval."
    );
    assert_eq!(held_count(&root_request), 0);
    assert_eq!(held_count(&continuation_request), 0);
}

#[tokio::test]
async fn workforce_root_queue_and_resume_admission_use_local_and_global_counts_separately() {
    let base = state("http://127.0.0.1:1", false);
    let (owner_a, _) = scoped_key(&base, None, permissions(true, true, true));
    let (owner_b, _) = scoped_key(&base, None, permissions(true, true, true));
    let state_a = base.with_auth(&owner_a);
    let state_b = base.with_auth(&owner_b);
    let supervisor = base.portfolio.async_ops.clone();
    supervisor
        .set_server_limits(crate::async_ops::ServerLimits {
            max_root_operations: 2,
            max_child_workers: 2,
            max_queued_roots: 8,
            max_queue_wait_seconds: 30,
        })
        .await
        .unwrap();
    supervisor
        .set_workforce_limits(
            owner_a.workforce_id.clone(),
            crate::async_ops::WorkforceLimits {
                max_root_operations: Some(1),
                max_child_workers: Some(1),
                max_queued_roots: Some(4),
                max_queue_wait_seconds: Some(30),
            },
        )
        .await
        .unwrap();
    supervisor
        .set_workforce_limits(
            owner_b.workforce_id.clone(),
            crate::async_ops::WorkforceLimits {
                max_root_operations: Some(1),
                max_child_workers: Some(1),
                max_queued_roots: Some(1),
                max_queue_wait_seconds: Some(30),
            },
        )
        .await
        .unwrap();

    let a_gate = Arc::new(Semaphore::new(0));
    let a_gate_worker = a_gate.clone();
    let (a_active, mut a_active_rx) = supervisor
        .start_root(
            state_a.portfolio.clone(),
            "local-cap-a-active".into(),
            0,
            async move {
                a_gate_worker.acquire().await.unwrap().forget();
                Ok(json!({"ok":true}))
            },
        )
        .await
        .unwrap();
    let b_gate = Arc::new(Semaphore::new(0));
    let b_gate_worker = b_gate.clone();
    let (b_active, mut b_active_rx) = supervisor
        .start_root(
            state_b.portfolio.clone(),
            "local-cap-b-active".into(),
            0,
            async move {
                b_gate_worker.acquire().await.unwrap().forget();
                Ok(json!({"ok":true}))
            },
        )
        .await
        .unwrap();
    assert_eq!(a_active.status, "running");
    assert_eq!(
        b_active.status, "running",
        "A's local root cap must not consume B's free slot"
    );

    let a_queued_gate = Arc::new(Semaphore::new(0));
    let a_queued_worker = a_queued_gate.clone();
    let (a_queued, mut a_queued_rx) = supervisor
        .start_root(
            state_a.portfolio.clone(),
            "local-cap-a-queued".into(),
            0,
            async move {
                a_queued_worker.acquire().await.unwrap().forget();
                Ok(json!({"ok":true}))
            },
        )
        .await
        .unwrap();
    let b_queued_gate = Arc::new(Semaphore::new(0));
    let b_queued_worker = b_queued_gate.clone();
    let (b_queued, mut b_queued_rx) = supervisor
        .start_root(
            state_b.portfolio.clone(),
            "local-cap-b-queued".into(),
            0,
            async move {
                b_queued_worker.acquire().await.unwrap().forget();
                Ok(json!({"ok":true}))
            },
        )
        .await
        .unwrap();
    assert_eq!(a_queued.status, "queued");
    assert_eq!(
        b_queued.status, "queued",
        "A's queued count must not consume B's workforce queue allowance"
    );

    b_gate.add_permits(1);
    wait_receiver_status(&mut b_active_rx, "completed").await;
    wait_receiver_status(&mut b_queued_rx, "running").await;
    b_queued_gate.add_permits(1);
    wait_receiver_status(&mut b_queued_rx, "completed").await;

    // A's queued root is first but cannot dispatch while A already occupies its
    // workforce slot. B's queue was able to dispatch above despite that earlier
    // blocked entry, preserving cross-workforce progress.
    a_gate.add_permits(1);
    wait_receiver_status(&mut a_active_rx, "completed").await;
    wait_receiver_status(&mut a_queued_rx, "running").await;
    a_queued_gate.add_permits(1);
    wait_receiver_status(&mut a_queued_rx, "completed").await;

    // Resume also uses the same split accounting: B occupies one of two global
    // roots, while the waiting A handoff has an unused local root slot.
    let retained_before_handoff = supervisor.settings().await["retained_async_result_bytes"]
        .as_u64()
        .unwrap();
    let (handoff_id, tool_id) =
        start_waiting_handoff(state_a.portfolio.clone(), "local-cap-resume-handoff".into())
            .await
            .unwrap();
    let retained_with_handoff = supervisor.settings().await["retained_async_result_bytes"]
        .as_u64()
        .unwrap();
    assert!(retained_with_handoff > retained_before_handoff);
    let hold_b = Arc::new(Semaphore::new(0));
    let hold_b_worker = hold_b.clone();
    let (b_root, _b_root_rx) = supervisor
        .start_root(
            state_b.portfolio.clone(),
            "local-cap-resume-b".into(),
            0,
            async move {
                hold_b_worker.acquire().await.unwrap().forget();
                Ok(json!({"ok":true}))
            },
        )
        .await
        .unwrap();
    assert_eq!(b_root.status, "running");
    let capacity_before_resume = supervisor.settings().await["client_tool_handoff_capacity_in_use"]
        .as_u64()
        .unwrap();
    let hold_a = Arc::new(Semaphore::new(0));
    let hold_a_worker = hold_a.clone();
    let (resumed, mut resumed_rx) = supervisor
        .resume_root_boxed(
            state_a.portfolio.clone(),
            &handoff_id,
            "local-cap-resume-a".into(),
            &handoff_payload(&tool_id),
            async move {
                hold_a_worker.acquire().await.unwrap().forget();
                Ok(json!({"ok":true}))
            }
            .boxed(),
        )
        .await
        .unwrap();
    assert_eq!(resumed.status, "running");
    assert_eq!(supervisor.settings().await["active_root_operations"], 2);
    assert_eq!(
        supervisor.settings().await["outstanding_client_tool_handoffs"],
        0
    );
    assert_eq!(
        supervisor.settings().await["reserved_client_tool_handoff_slots"],
        2,
        "resume moves the existing handoff slot to its in-flight candidate"
    );
    assert_eq!(
        supervisor.settings().await["client_tool_handoff_capacity_in_use"],
        capacity_before_resume,
        "resume does not double-charge the same continuation"
    );
    assert_eq!(
        supervisor.settings().await["retained_async_result_bytes"],
        json!(retained_before_handoff),
        "resume releases the handoff result but preserves other retained results"
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if supervisor.settings().await["handoff_expiry_worker_running"] == false {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("resume did not wake and stop the idle expiry worker");
    hold_a.add_permits(1);
    wait_receiver_status(&mut resumed_rx, "completed").await;
    hold_b.add_permits(1);
}

#[tokio::test]
async fn workforce_child_caps_do_not_compare_other_workforces_usage_to_the_local_cap() {
    let base = state("http://127.0.0.1:1", false);
    let (owner_a, _) = scoped_key(&base, None, permissions(true, true, true));
    let (owner_b, _) = scoped_key(&base, None, permissions(true, true, true));
    let state_a = base.with_auth(&owner_a);
    let state_b = base.with_auth(&owner_b);
    let supervisor = base.portfolio.async_ops.clone();
    supervisor
        .set_server_limits(crate::async_ops::ServerLimits {
            max_root_operations: 2,
            max_child_workers: 2,
            max_queued_roots: 0,
            max_queue_wait_seconds: 30,
        })
        .await
        .unwrap();
    for owner in [&owner_a, &owner_b] {
        supervisor
            .set_workforce_limits(
                owner.workforce_id.clone(),
                crate::async_ops::WorkforceLimits {
                    max_root_operations: Some(1),
                    max_child_workers: Some(1),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    let root_gate_a = Arc::new(Semaphore::new(0));
    let root_worker_a = root_gate_a.clone();
    let (root_a, _) = supervisor
        .start_root(
            state_a.portfolio.clone(),
            "child-cap-root-a".into(),
            0,
            async move {
                root_worker_a.acquire().await.unwrap().forget();
                Ok(json!({"ok":true}))
            },
        )
        .await
        .unwrap();
    let root_gate_b = Arc::new(Semaphore::new(0));
    let root_worker_b = root_gate_b.clone();
    let (root_b, _) = supervisor
        .start_root(
            state_b.portfolio.clone(),
            "child-cap-root-b".into(),
            0,
            async move {
                root_worker_b.acquire().await.unwrap().forget();
                Ok(json!({"ok":true}))
            },
        )
        .await
        .unwrap();
    assert_eq!(root_a.status, "running");
    assert_eq!(root_b.status, "running");

    let child_gate_a = Arc::new(Semaphore::new(0));
    let child_worker_a = child_gate_a.clone();
    let child_a_started = Arc::new(Notify::new());
    let child_a_started_worker = child_a_started.clone();
    let child_a = tokio::spawn({
        let supervisor = supervisor.clone();
        let root_id = root_a.id.clone();
        async move {
            supervisor
                .run_child(&root_id, 1, async move {
                    child_a_started_worker.notify_one();
                    child_worker_a.acquire().await.unwrap().forget();
                    Ok(())
                })
                .await
        }
    });
    wait_for_notify(&child_a_started, "workforce A child admission").await;

    let child_gate_b = Arc::new(Semaphore::new(0));
    let child_worker_b = child_gate_b.clone();
    let child_b_started = Arc::new(Notify::new());
    let child_b_started_worker = child_b_started.clone();
    let child_b = tokio::spawn({
        let supervisor = supervisor.clone();
        let root_id = root_b.id.clone();
        async move {
            supervisor
                .run_child(&root_id, 1, async move {
                    child_b_started_worker.notify_one();
                    child_worker_b.acquire().await.unwrap().forget();
                    Ok(())
                })
                .await
        }
    });
    wait_for_notify(&child_b_started, "workforce B child admission").await;
    assert_eq!(
        supervisor.settings().await["active_child_workers"],
        2,
        "B has one local slot and the process still has a spare global child slot"
    );
    child_gate_b.add_permits(1);
    child_gate_a.add_permits(1);
    child_a.await.unwrap().unwrap();
    child_b.await.unwrap().unwrap();
    root_gate_a.add_permits(1);
    root_gate_b.add_permits(1);
}

#[tokio::test]
async fn full_async_root_admission_returns_too_many_requests() {
    let state = state("http://127.0.0.1:1", false);
    let (owner, token) = scoped_key(&state, None, permissions(true, true, true));
    let owner_state = state.with_auth(&owner);
    let agent = insert(&owner_state);
    let provider = Arc::new(GatedProvider {
        agent_id: agent.id,
        calls: Mutex::new(Vec::new()),
        agent_started: Notify::new(),
        release_agent: Semaphore::new(0),
        checkin_seen: Notify::new(),
        checkin_count: AtomicUsize::new(0),
    });
    let upstream = gated_upstream(provider.clone()).await;
    let mut state = state;
    Arc::get_mut(&mut state).unwrap().config = Arc::new({
        let mut config = (*state.config).clone();
        config.upstream_base_url = upstream.url.clone();
        config
    });
    state
        .portfolio
        .async_ops
        .set_server_limits(crate::async_ops::ServerLimits {
            max_root_operations: 1,
            max_child_workers: 1,
            max_queued_roots: 0,
            max_queue_wait_seconds: 30,
        })
        .await
        .unwrap();
    let server = serve(crate::api::routes(state)).await;
    let client = reqwest::Client::new();
    let first = client
        .post(format!("{}/v1/chat/completions", server.url))
        .bearer_auth(&token)
        .json(&async_payload("First bounded task"))
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), reqwest::StatusCode::ACCEPTED);
    let accepted: Value = first.json().await.unwrap();
    let id = accepted["operation"]["id"].as_str().unwrap();
    wait_for_notify(&provider.agent_started, "first root's agent worker").await;

    let second = client
        .post(format!("{}/v1/chat/completions", server.url))
        .bearer_auth(&token)
        .json(&async_payload("Second bounded task"))
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    client
        .delete(format!("{}/api/operations/{id}", server.url))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    provider.release_agent.add_permits(1);
    wait_operation_status(&client, &server.url, &token, id, "cancelled").await;
}
