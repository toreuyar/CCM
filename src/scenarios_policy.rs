//! Creation timing, prompt transparency, durable orders and atomic consolidation.
//! Authored for the next user-authorized verification cycle.
use super::*;
use crate::policy::{Context, CreationPolicy as Policy};

fn configured(url: &str, policy: Policy) -> Arc<AppState> {
    let mut state = state(url, false);
    Arc::make_mut(&mut Arc::get_mut(&mut state).unwrap().config)
        .creation
        .policy = policy;
    state
}
fn context(policy: Policy) -> Context {
    Context {
        client: "anonymous".into(),
        policy,
        phase: policy.initial_phase().map(str::to_string),
        orders: vec![],
        locked: false,
    }
}
fn begin(state: &AppState, request: &str, client: &str, policy: Policy) -> String {
    let run = state
        .portfolio
        .trace_begin(request, &json!({"messages":[]}), "external")
        .unwrap();
    state
        .portfolio
        .bind_policy(request, &run, client, policy)
        .unwrap();
    run
}
fn running_job(state: &AppState, request: &str) -> String {
    state
        .portfolio
        .queue_improvement(
            request,
            "workforce",
            "workforce",
            &json!(context(Policy::AlwaysPreExecution)),
            "task",
            "result",
            state.portfolio.generation(),
        )
        .unwrap();
    state.portfolio.release_improvements(request).unwrap();
    let conn = state.portfolio.conn.lock().unwrap();
    conn.execute(
        "UPDATE improvement_jobs SET status='running' WHERE origin_request=?1",
        [request],
    )
    .unwrap();
    conn.query_row(
        "SELECT id FROM improvement_jobs WHERE origin_request=?1",
        [request],
        |r| r.get(0),
    )
    .unwrap()
}

#[test]
fn five_modes_control_capability_and_persistent_flips() {
    for policy in [
        Policy::AlwaysPreExecution,
        Policy::DefaultPreExecution,
        Policy::Dynamic,
        Policy::DefaultPostExecution,
        Policy::AlwaysPostExecution,
    ] {
        let state = configured("http://unused", policy);
        begin(&state, "r", "client-a", policy);
        let initial = state.portfolio.policy_context("r").unwrap();
        let tools = crate::policy::tools(&initial);
        assert_eq!(
            tools.iter().any(|t| t["name"] == "create_agent"),
            initial.phase.as_deref() == Some("pre")
        );
        assert_eq!(
            tools.iter().any(|t| t["name"] == "set_creation_timing"),
            policy.mutable()
        );
        let prompt =
            crate::foreman::policy_prompt("empty", &initial, "UNIQUE_SHARED_CREATION_INSTRUCTIONS");
        assert_eq!(
            prompt.contains("UNIQUE_SHARED_CREATION_INSTRUCTIONS"),
            initial.phase.as_deref() == Some("pre")
        );
        if policy.is_default() {
            assert!(
                state
                    .portfolio
                    .change_timing(
                        "r",
                        "post",
                        "",
                        &json!({"messages":[{"role":"user","content":"write a story"}]})
                    )
                    .is_err()
            );
            let phase = if policy == Policy::DefaultPreExecution {
                "post"
            } else {
                "pre"
            };
            let quote = format!("Switch creation to {phase}-execution");
            let payload = json!({"messages":[{"role":"user","content":quote}]});
            state
                .portfolio
                .change_timing("r", phase, &quote, &payload)
                .unwrap();
            begin(&state, "next", "client-a", Policy::Dynamic);
            assert_eq!(
                state
                    .portfolio
                    .policy_context("next")
                    .unwrap()
                    .phase
                    .as_deref(),
                Some(phase)
            );
            begin(&state, "other", "client-b", Policy::Dynamic);
            assert_eq!(
                state.portfolio.policy_context("other").unwrap().policy,
                Policy::Dynamic
            );
        } else if policy == Policy::Dynamic {
            state
                .portfolio
                .change_timing("r", "post", "", &json!({"messages":[]}))
                .unwrap();
            state
                .portfolio
                .defer_creation("r", "workflow", "reusable staged process")
                .unwrap();
            assert!(state.portfolio.authorize_pre_creation("r").is_err());
            begin(&state, "next", "client-a", Policy::Dynamic);
            assert!(
                state
                    .portfolio
                    .policy_context("next")
                    .unwrap()
                    .phase
                    .is_none()
            );
        } else {
            assert!(
                state
                    .portfolio
                    .change_timing(
                        "r",
                        "post",
                        "switch",
                        &json!({"messages":[{"role":"user","content":"switch"}]})
                    )
                    .is_err()
            );
        }
    }
}

#[test]
fn identities_and_overrides_survive_reopen_without_raw_credentials() {
    let path = std::env::temp_dir().join(format!("ccm-policy-{}.db", uuid::Uuid::new_v4()));
    let mut headers = axum::http::HeaderMap::new();
    headers.insert("authorization", "Bearer client-secret".parse().unwrap());
    let client = crate::policy::identity(&headers, false);
    assert!(!client.contains("client-secret"));
    assert_eq!(crate::policy::identity(&headers, true), "native");
    assert_eq!(
        crate::policy::identity(&axum::http::HeaderMap::new(), false),
        "anonymous"
    );
    {
        let p = Portfolio::open(path.to_str().unwrap()).unwrap();
        let run = p
            .trace_begin("first", &json!({"messages":[]}), "external")
            .unwrap();
        p.bind_policy("first", &run, &client, Policy::DefaultPreExecution)
            .unwrap();
        p.change_timing(
            "first",
            "post",
            "create after responding",
            &json!({"messages":[{"role":"user","content":"create after responding"}]}),
        )
        .unwrap();
    }
    let p = Portfolio::open(path.to_str().unwrap()).unwrap();
    let run = p
        .trace_begin("second", &json!({"messages":[]}), "external")
        .unwrap();
    assert_eq!(
        p.bind_policy("second", &run, &client, Policy::AlwaysPreExecution)
            .unwrap()
            .policy,
        Policy::DefaultPostExecution
    );
    drop(p);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn config_defaults_and_relative_prompt_files() {
    let dir = std::env::temp_dir().join(format!("ccm-config-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("ccm.json");
    assert_eq!(
        crate::config::CreationConfig::load(&path, false)
            .unwrap()
            .policy,
        Policy::Dynamic
    );
    assert!(crate::config::CreationConfig::load(&path, true).is_err());
    std::fs::write(dir.join("custom.md"), "shared creation instruction").unwrap();
    std::fs::write(
        &path,
        r#"{"creation_policy":"DefaultPostExecution","creation_prompt":"custom.md"}"#,
    )
    .unwrap();
    let config = crate::config::CreationConfig::load(&path, true).unwrap();
    assert_eq!(config.policy, Policy::DefaultPostExecution);
    assert_eq!(config.prompt, "shared creation instruction");
    std::fs::write(&path, r#"{"creation_policy":"not-a-policy"}"#).unwrap();
    assert!(crate::config::CreationConfig::load(&path, true).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn fallback_has_exact_client_messages_and_defers_creation_until_body_completion() {
    let (provider, script) = upstream(vec![
        message(r#"{"decision":"fallback"}"#),
        message("report"),
        message("answer"),
        message(r#"{"creations":[]}"#),
    ])
    .await;
    let state = configured(&provider.url, Policy::AlwaysPostExecution);
    let payload = json!({"messages":[{"role":"system","content":"Exact client system prompt"},{"role":"developer","content":"Exact developer instruction"},{"role":"user","content":"task"}],"response_format":{"type":"json_object"}});
    let response = handlers::chat_completions(
        State(state.clone()),
        None,
        axum::http::HeaderMap::new(),
        axum::body::Bytes::from(payload.to_string()),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), 200);
    let run = response.headers()["x-ccm-run-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(script.lock().unwrap().1[1]["messages"], payload["messages"]);
    assert_eq!(
        script.lock().unwrap().1[1]["response_format"],
        payload["response_format"]
    );
    assert!(!crate::improvement::process_one(&state).await.unwrap());
    axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    assert!(crate::improvement::process_one(&state).await.unwrap());
    assert_eq!(
        state.portfolio.improvements(&run).unwrap()[0]["status"],
        "completed"
    );
    assert!(state.portfolio.list().unwrap().is_empty());
    assert!(
        script.lock().unwrap().1[3]["messages"][0]["content"]
            .as_str()
            .unwrap()
            .contains("Shared agent and workflow creation capability")
    );
}

#[tokio::test]
async fn dynamic_post_order_creates_even_when_evaluator_proposes_no_creations() {
    let (provider,script)=upstream(vec![
        message(r#"{"decision":"tool","name":"set_creation_timing","arguments":{"timing":"post"}}"#),
        message(r#"{"decision":"tool","name":"defer_creation","arguments":{"kind":"agent","requirements":"Reusable writing specialist"}}"#),
        message(r#"{"decision":"fallback"}"#),message("report"),message("answer"),
        message(r#"{"dependencies":[],"creations":[]}"#),message(&definition().to_string()),
    ]).await;
    let state = configured(&provider.url, Policy::Dynamic);
    let server = proxy(state.clone()).await;
    let response = request(&server, "write", false).await;
    let run = response.headers()["x-ccm-run-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        response.json::<Value>().await.unwrap()["choices"][0]["message"]["content"],
        "answer"
    );
    assert!(state.portfolio.list().unwrap().is_empty());
    assert!(crate::improvement::process_one(&state).await.unwrap());
    assert_eq!(state.portfolio.list().unwrap().len(), 1);
    let jobs = state.portfolio.improvements(&run).unwrap();
    assert_eq!(jobs[0]["status"], "completed");
    assert_eq!(jobs[0]["outcome"]["fulfilled"].as_array().unwrap().len(), 1);
    assert_eq!(script.lock().unwrap().1.len(), 7);
}

#[tokio::test]
async fn fallback_tool_continuation_preserves_orders_policy_and_client_ownership() {
    let (provider,script)=upstream(vec![
        message(r#"{"decision":"tool","name":"set_creation_timing","arguments":{"timing":"post"}}"#),
        message(r#"{"decision":"tool","name":"defer_creation","arguments":{"kind":"agent","requirements":"Research with client lookup tools"}}"#),
        message(r#"{"decision":"fallback"}"#),tool_message(),message("lookup report"),message("answer"),
    ]).await;
    let state = configured(&provider.url, Policy::Dynamic);
    let server = proxy(state.clone()).await;
    let client = reqwest::Client::new();
    let url = format!("{}/v1/chat/completions", server.url);
    let mut payload = tool_payload();
    payload["messages"].as_array_mut().unwrap().insert(
        0,
        json!({"role":"system","content":"Use the client's lookup tool"}),
    );
    let first = client
        .post(&url)
        .bearer_auth("owner")
        .json(&payload)
        .send()
        .await
        .unwrap();
    let run = first.headers()["x-ccm-run-id"].to_str().unwrap().to_owned();
    let first = first.json::<Value>().await.unwrap();
    let assistant = first["choices"][0]["message"].clone();
    let id = assistant["tool_calls"][0]["id"].clone();
    payload["messages"].as_array_mut().unwrap().extend([
        assistant,
        json!({"role":"tool","tool_call_id":id,"content":"lookup result"}),
    ]);
    assert!(!crate::improvement::process_one(&state).await.unwrap());
    let wrong = client
        .post(&url)
        .bearer_auth("other-client")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), 400);
    assert_eq!(script.lock().unwrap().1.len(), 4);
    let response = client
        .post(&url)
        .bearer_auth("owner")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["x-ccm-run-id"].to_str().unwrap(), run);
    response.json::<Value>().await.unwrap();
    let detail = state.portfolio.trace_detail(&run).unwrap();
    assert_eq!(detail["creation_policy"]["phase"], "post");
    assert_eq!(
        detail["creation_policy"]["orders"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(detail["improvements"].as_array().unwrap().len(), 1);
    assert_eq!(script.lock().unwrap().1[4]["messages"], payload["messages"]);
}

#[tokio::test]
async fn mandatory_workflow_order_is_satisfied_by_equivalent_existing_workflow() {
    let (provider, script) = upstream(vec![]).await;
    let state = state(&provider.url, false);
    let agent = insert(&state);
    let workflow = workflow(&agent.id);
    state.portfolio.workflow_insert(&workflow, 0).unwrap();
    let run = begin(&state, "origin", "anonymous", Policy::Dynamic);
    let mut policy = context(Policy::Dynamic);
    policy.phase = Some("post".into());
    policy
        .orders
        .push(json!({"kind":"workflow","requirements":"produce and check"}));
    state
        .portfolio
        .queue_improvement(
            "origin",
            "workforce",
            "workforce",
            &json!(policy),
            "task",
            "answer",
            0,
        )
        .unwrap();
    state.portfolio.release_improvements("origin").unwrap();
    script.lock().unwrap().0=vec![message(r#"{"dependencies":[]}"#),message(&json!({"reuse_id":workflow.id,"reason":"equivalent process","evidence":"same branch, loop bound, stages and output"}).to_string())].into();
    assert!(crate::improvement::process_one(&state).await.unwrap());
    let jobs = state.portfolio.improvements(&run).unwrap();
    assert_eq!(jobs[0]["status"], "completed");
    assert_eq!(jobs[0]["outcome"]["fulfilled"][0]["id"], workflow.id);
    assert_eq!(state.portfolio.workflow_list().unwrap().len(), 1);
}

#[tokio::test]
async fn runtime_rejects_creation_bypassing_post_policy() {
    for decision in [
        json!({"decision":"create","requirements":"x"}),
        json!({"decision":"create_workflow","requirements":"x"}),
    ] {
        let (provider, script) = upstream(vec![message(&decision.to_string()); 3]).await;
        let state = configured(&provider.url, Policy::AlwaysPostExecution);
        assert!(
            crate::runtime::dispatch(
                &state,
                &json!({"messages":[{"role":"user","content":"task"}]}),
                "r"
            )
            .await
            .err()
            .unwrap()
            .contains("unavailable under the current")
        );
        assert_eq!(script.lock().unwrap().1.len(), 3);
        assert!(state.portfolio.list().unwrap().is_empty());
    }
}

#[tokio::test]
async fn fallback_keeps_internal_lookup_evidence_without_injecting_a_system_prompt() {
    let (provider, script) = upstream(vec![
        message(r#"{"decision":"tool","name":"list_agents","arguments":{}}"#),
        message(r#"{"decision":"fallback"}"#),
        message("directory report"),
        message("directory answer"),
    ])
    .await;
    let state = configured(&provider.url, Policy::AlwaysPostExecution);
    let agent = insert(&state);
    let server = proxy(state).await;
    let payload = json!({"messages":[{"role":"system","content":"Report the actual available agents"},{"role":"user","content":"List agents"}]});
    let response = reqwest::Client::new()
        .post(format!("{}/v1/chat/completions", server.url))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    response.json::<Value>().await.unwrap();
    let calls = script.lock().unwrap();
    let messages = calls.1[2]["messages"].as_array().unwrap();
    assert_eq!(messages[0], payload["messages"][0]);
    assert_eq!(messages.iter().filter(|m| m["role"] == "system").count(), 1);
    assert!(
        messages.last().unwrap()["content"]
            .as_str()
            .unwrap()
            .contains(&agent.id)
    );
}

#[tokio::test]
async fn concurrent_generation_rechecks_roster_and_reuses_winner() {
    #[derive(Clone)]
    struct Race {
        barrier: Arc<tokio::sync::Barrier>,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }
    async fn racing(State(race): State<Race>, Json(body): Json<Value>) -> Json<Value> {
        let index = race.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if index < 2 {
            race.barrier.wait().await;
        }
        let input: Value =
            serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
        let agents = input["registry"]["agents"].as_array().unwrap();
        Json(if let Some(agent) = agents.first() {
            message(&json!({"reuse_id":agent["id"],"reason":"equivalent writer","evidence":"same writing mission, boundaries and report contract"}).to_string())
        } else {
            message(&definition().to_string())
        })
    }
    let race = Race {
        barrier: Arc::new(tokio::sync::Barrier::new(2)),
        calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    };
    let provider = serve(
        Router::new()
            .route("/chat/completions", post(racing))
            .with_state(race.clone()),
    )
    .await;
    let state = state(&provider.url, false);
    let (a, b) = tokio::join!(
        crate::meta::create_agent(&state, "a", "writing", 0),
        crate::meta::create_agent(&state, "b", "writing", 0)
    );
    assert_eq!(a.unwrap().id, b.unwrap().id);
    assert_eq!(state.portfolio.list().unwrap().len(), 1);
    assert_eq!(race.calls.load(std::sync::atomic::Ordering::SeqCst), 3);
}

#[test]
fn consolidation_checks_revision_updates_future_references_and_preserves_pending_snapshots() {
    let state = state("http://unused", false);
    let survivor = insert(&state);
    let duplicate = insert(&state);
    let workflow:crate::workflow::Workflow=serde_json::from_value(json!({"id":uuid::Uuid::new_v4().to_string(),"name":"w","purpose":"write","entry":"s","max_steps":1,"stages":[{"id":"s","agent_id":duplicate.id,"instruction":"write","next":null}]})).unwrap();
    state.portfolio.workflow_insert(&workflow, 0).unwrap();
    state
        .portfolio
        .save_run(
            "pending",
            &json!({"workflow":workflow,"pending_agent":duplicate}),
            0,
        )
        .unwrap();
    begin(&state, "r", "anonymous", Policy::AlwaysPreExecution);
    let job = running_job(&state, "r");
    let (stale, snapshot) = state.portfolio.registry_snapshot().unwrap();
    state.portfolio.record_usage(&survivor.id);
    assert_eq!(
        state.portfolio.registry_snapshot().unwrap().0,
        stale,
        "usage changes must not invalidate definition decisions"
    );
    let review = json!({"duplicates":[{"kind":"agent","duplicate_id":duplicate.id,"survivor_id":survivor.id,"reason":"equivalent","evidence":"identical full provisions"}]});
    let extra = insert(&state);
    assert!(
        crate::registry::apply_consolidation(
            &state.portfolio,
            "r",
            &job,
            stale,
            &snapshot,
            &review,
            &json!([]),
            0
        )
        .unwrap()
        .is_none()
    );
    assert!(state.portfolio.get(&duplicate.id).unwrap().is_some());
    let (version, snapshot) = state.portfolio.registry_snapshot().unwrap();
    assert!(
        crate::registry::apply_consolidation(
            &state.portfolio,
            "r",
            &job,
            version,
            &snapshot,
            &review,
            &json!([]),
            0
        )
        .unwrap()
        .is_some()
    );
    assert!(state.portfolio.get(&duplicate.id).unwrap().is_none());
    assert!(state.portfolio.get(&extra.id).unwrap().is_some());
    assert_eq!(
        state
            .portfolio
            .workflow_get(&workflow.id)
            .unwrap()
            .unwrap()
            .stages[0]
            .agent_id,
        survivor.id
    );
    let pending = state.portfolio.claim_run("pending").unwrap();
    assert_eq!(pending["workflow"]["stages"][0]["agent_id"], duplicate.id);
    assert_eq!(
        pending["agents"][&duplicate.id]["system_prompt"],
        duplicate.system_prompt
    );
    assert_eq!(state.portfolio.evaluations().unwrap()[0]["applied"], true);
    assert!(
        crate::registry::apply_consolidation(
            &state.portfolio,
            "r",
            &job,
            version,
            &snapshot,
            &review,
            &json!([]),
            0
        )
        .is_err()
    );
}

#[tokio::test]
async fn committed_creation_receipt_prevents_replay_after_reopen() {
    let (provider, script) = upstream(vec![message(&definition().to_string())]).await;
    let path = std::env::temp_dir().join(format!("ccm-receipt-{}.db", uuid::Uuid::new_v4()));
    let mut state = state(&provider.url, false);
    Arc::get_mut(&mut state).unwrap().portfolio =
        Arc::new(Portfolio::open(path.to_str().unwrap()).unwrap());
    begin(&state, "r", "anonymous", Policy::Dynamic);
    let job = running_job(&state, "r");
    let stored = crate::registry::create(&state, "r", "agent", "writer", 0, Some((&job, 0)))
        .await
        .unwrap();
    let config = state.config.clone();
    drop(state);
    let state = Arc::new(AppState {
        providers: Default::default(),
        config,
        http: reqwest::Client::new(),
        portfolio: Arc::new(Portfolio::open(path.to_str().unwrap()).unwrap()),
        external_api_enabled: true,
        auth: None,
    });
    let replay = crate::registry::create(&state, "new", "agent", "writer", 0, Some((&job, 0)))
        .await
        .unwrap();
    assert_eq!(stored["id"], replay["id"]);
    assert_eq!(script.lock().unwrap().1.len(), 1);
    assert_eq!(state.portfolio.list().unwrap().len(), 1);
    drop(state);
    std::fs::remove_file(path).unwrap();
}
