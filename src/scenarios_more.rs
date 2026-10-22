use super::*;

#[tokio::test]
async fn disconnected_stream_records_cancellation() {
    use futures::StreamExt;
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    script.lock().unwrap().0 = vec![
        message(&json!({"decision":"use","agent_id":agent.id}).to_string()),
        message("report"),
        json!({"__stream_delay":true}),
    ]
    .into();
    let server = proxy(state.clone()).await;
    let response = request(&server, "task", true).await;
    let mut body = response.bytes_stream();
    // The initial role frame now precedes dispatch. Wait for actual composition
    // output before testing cancellation of that particular model call.
    let mut received = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            received.extend_from_slice(&body.next().await.unwrap().unwrap());
            if String::from_utf8_lossy(&received).contains("\"content\":\"first\"") {
                break;
            }
        }
    })
    .await
    .unwrap();
    drop(body);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let stats = state.portfolio.statistics().unwrap();
            if stats["groups"]
                .as_array()
                .unwrap()
                .iter()
                .any(|g| g["kind"] == "composition" && g["status"] == "cancelled")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("stream cancellation was not recorded");
}

#[tokio::test]
async fn client_tool_failure_is_an_observation_not_fabricated_success() {
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    script.lock().unwrap().0 = vec![
        message(&json!({"decision":"use","agent_id":agent.id}).to_string()),
        tool_message(),
        message("lookup failed; unable to verify"),
    ]
    .into();
    let mut payload = tool_payload();
    let first = crate::runtime::dispatch(&state, &payload, "first")
        .await
        .unwrap();
    let assistant = first.message.unwrap();
    let id = assistant["tool_calls"][0]["id"].clone();
    payload["messages"].as_array_mut().unwrap().extend([
        assistant,
        json!({"role":"tool","tool_call_id":id,"content":"ERROR: lookup unavailable"}),
    ]);
    let output = crate::runtime::dispatch(&state, &payload, "resume")
        .await
        .unwrap();
    assert_eq!(output.report, "lookup failed; unable to verify");
    assert_eq!(
        script.lock().unwrap().1[2]["messages"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()["content"],
        "ERROR: lookup unavailable"
    );
}

#[tokio::test]
async fn model_route_returns_configured_model() {
    let state = state("http://127.0.0.1:1", false);
    let server = serve(
        Router::new()
            .route("/v1/models", axum::routing::get(handlers::list_models))
            .with_state(state),
    )
    .await;
    let body: Value = reqwest::get(format!("{}/v1/models", server.url))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["object"], "list");
    assert_eq!(body["data"][0]["id"], "test-model");
}

#[tokio::test]
async fn failed_debug_reset_rolls_back_without_success() {
    let state = state("http://127.0.0.1:1", true);
    insert(&state);
    state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute("DROP TABLE workflows", [])
        .unwrap();
    let server = proxy(state.clone()).await;
    let body: Value = request(&server, "debug-delete", false)
        .await
        .json()
        .await
        .unwrap();
    let result: Value =
        serde_json::from_str(body["choices"][0]["message"]["content"].as_str().unwrap()).unwrap();
    assert_eq!(result["status"], "error");
    assert_eq!(state.portfolio.list().unwrap().len(), 1);
}

#[tokio::test]
async fn exhausted_tool_budget_never_returns_another_call() {
    let (upstream, script) = upstream(vec![]).await;
    let mut state = state(&upstream.url, false);
    Arc::make_mut(&mut Arc::make_mut(&mut state).config).max_iterations = 0;
    let agent = insert(&state);
    script.lock().unwrap().0 = vec![
        message(&json!({"decision":"use","agent_id":agent.id}).to_string()),
        tool_message(),
    ]
    .into();
    let error = crate::runtime::dispatch(&state, &tool_payload(), "budget")
        .await
        .err()
        .unwrap();
    assert!(error.contains("tool-round budget"));
}

#[test]
fn legacy_agent_database_is_migrated_without_rewriting_prompts() {
    let path = std::env::temp_dir()
        .join(format!("ccm-legacy-{}.db", uuid::Uuid::new_v4()))
        .to_string_lossy()
        .into_owned();
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE agents(id TEXT PRIMARY KEY,name TEXT NOT NULL,purpose TEXT NOT NULL,system_prompt TEXT NOT NULL,model TEXT NOT NULL,created_at INTEGER NOT NULL,usage_count INTEGER NOT NULL DEFAULT 0,last_used_at INTEGER);
            INSERT INTO agents(id,name,purpose,system_prompt,model,created_at) VALUES ('legacy','legacy','p','original prompt','m',0);").unwrap();
    }
    {
        let portfolio = Portfolio::open(&path).unwrap();
        assert_eq!(
            portfolio.get("legacy").unwrap().unwrap().system_prompt,
            "original prompt"
        );
        assert!(portfolio.workflow_list().unwrap().is_empty());
    }
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn composition_stream_records_provider_usage() {
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    script.lock().unwrap().0=vec![message(&json!({"decision":"use","agent_id":agent.id}).to_string()),message("report"),
        json!("data: {\"choices\":[{\"delta\":{\"content\":\"answer\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":13,\"completion_tokens\":5}}\n\ndata: [DONE]\n\n")].into();
    let proxy = proxy(state.clone()).await;
    let response = request(&proxy, "write", true).await;
    let body = response.text().await.unwrap();
    assert!(body.contains("answer"));
    assert!(body.ends_with("data: [DONE]\n\n"));
    let stats = state.portfolio.statistics().unwrap();
    assert!(
        stats["groups"]
            .as_array()
            .unwrap()
            .iter()
            .any(|g| g["kind"] == "composition" && g["prompt_tokens"] == 13)
    );
}

#[tokio::test]
async fn parallel_tool_results_preserve_each_id() {
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    let mut calls = tool_message();
    let mut second = calls["choices"][0]["message"]["tool_calls"][0].clone();
    second["id"] = json!("call_2");
    calls["choices"][0]["message"]["tool_calls"]
        .as_array_mut()
        .unwrap()
        .push(second);
    script.lock().unwrap().0 = vec![
        message(&json!({"decision":"use","agent_id":agent.id}).to_string()),
        calls,
        message("both observed"),
    ]
    .into();
    let mut payload = tool_payload();
    let first = crate::runtime::dispatch(&state, &payload, "first")
        .await
        .unwrap();
    let assistant = first.message.unwrap();
    let ids: Vec<_> = assistant["tool_calls"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["id"].clone())
        .collect();
    assert_ne!(ids[0], ids[1]);
    payload["messages"].as_array_mut().unwrap().extend([
        assistant,
        json!({"role":"tool","tool_call_id":ids[1],"content":"second"}),
        json!({"role":"tool","tool_call_id":ids[0],"content":"first"}),
    ]);
    let result = crate::runtime::dispatch(&state, &payload, "second")
        .await
        .unwrap();
    assert_eq!(result.report, "both observed");
}

#[tokio::test]
async fn evaluator_revises_and_retires_workflow() {
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    let w = workflow(&agent.id);
    state.portfolio.workflow_insert(&w, 0).unwrap();
    let mut replacement = w.clone();
    replacement.name = "improved flow".into();
    script.lock().unwrap().0=vec![message(&json!({"action":"revise","reason":"better guidance","evidence":"observed failure","replacement":replacement}).to_string()),
        message(r#"{"action":"retire","reason":"obsolete","evidence":"superseded definition"}"#)].into();
    let outcome = crate::meta::evaluate(
        &state,
        "revise",
        "workflow",
        &w.id,
        &json!(w),
        "task",
        "result",
        0,
    )
    .await
    .unwrap();
    assert_eq!(outcome["applied"], true);
    let current = state.portfolio.workflow_get(&w.id).unwrap().unwrap();
    assert_eq!(current.name, "improved flow");
    crate::meta::evaluate(
        &state,
        "retire",
        "workflow",
        &w.id,
        &json!(current),
        "task",
        "result",
        0,
    )
    .await
    .unwrap();
    assert!(state.portfolio.workflow_get(&w.id).unwrap().is_none());
    assert_eq!(
        state
            .portfolio
            .evaluations()
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn stale_evaluation_cannot_overwrite_newer_agent() {
    let verdict=message(&json!({"action":"revise","reason":"improve","evidence":"observed gap","replacement":definition()}).to_string());
    let (upstream, _) = upstream(vec![verdict.clone(), verdict]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    assert_eq!(
        crate::meta::evaluate(
            &state,
            "first",
            "agent",
            &agent.id,
            &json!(agent),
            "task",
            "result",
            0
        )
        .await
        .unwrap()["applied"],
        true
    );
    assert_eq!(
        crate::meta::evaluate(
            &state,
            "stale",
            "agent",
            &agent.id,
            &json!(agent),
            "task",
            "result",
            0
        )
        .await
        .unwrap()["applied"],
        false
    );
}

#[tokio::test]
async fn foreman_can_invoke_creation_and_evaluation_tools() {
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    script.lock().unwrap().0=vec![
        message(&json!({"decision":"tool","name":"create_agent","arguments":{"requirements":"writing"}}).to_string()),message(&definition().to_string()),
        message(&json!({"decision":"tool","name":"evaluate_agent","arguments":{"id":agent.id,"task":"inspect","result":"observed work"}}).to_string()),keep(),
        message(&json!({"decision":"use","agent_id":agent.id}).to_string()),message("report")
    ].into();
    crate::runtime::dispatch(&state, &tool_payload(), "internal")
        .await
        .unwrap();
    assert_eq!(state.portfolio.list().unwrap().len(), 2);
    assert_eq!(
        state
            .portfolio
            .evaluations()
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn explicit_foreman_management_creation_is_immediate_create_only_and_policy_independent() {
    let quote = "Create a reusable writing agent that drafts reports.";
    let (upstream, script) = upstream(vec![
        message(
            &json!({
                "decision":"tool",
                "name":"management.create_definition",
                "arguments":{
                    "kind":"agent",
                    "requirements":"A reusable writing specialist that drafts accurate reports.",
                    "user_instruction":quote
                }
            })
            .to_string(),
        ),
        message(&definition().to_string()),
        message("The writing agent has been created and registered. It has not been run."),
    ])
    .await;
    let mut state = state(&upstream.url, false);
    let mut config = (*state.config).clone();
    config.creation.policy = crate::policy::CreationPolicy::AlwaysPostExecution;
    Arc::get_mut(&mut state).unwrap().config = Arc::new(config);
    let (identity, token) = state
        .portfolio
        .create_key(
            "Foreman management test",
            None,
            "Foreman management workforce",
            &crate::auth::KeyPermissions {
                roster_use: true,
                roster_read: true,
                roster_write: true,
                ..Default::default()
            },
        )
        .unwrap();
    let lifecycle = Arc::new(crate::health::Lifecycle::new());
    lifecycle.mark_ready();
    let app = crate::api::routes(state.clone()).layer(axum::Extension(lifecycle.clone()));
    let server = super::serve(app).await;
    let response = reqwest::Client::new()
        .post(format!("{}/v1/chat/completions", server.url))
        .bearer_auth(token)
        .json(&json!({
            "model":"test-model",
            "messages":[{"role":"user","content":quote}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let answer = response.json::<Value>().await.unwrap();
    assert_eq!(
        answer["choices"][0]["message"]["content"],
        "The writing agent has been created and registered. It has not been run."
    );
    let agents = state.portfolio.list().unwrap();
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0].name, "writer");
    assert_eq!(
        script.lock().unwrap().1.len(),
        3,
        "routing, generator, composition only"
    );
    assert_eq!(lifecycle.active_admissions(), 0);
    let request_id = answer["id"].as_str().unwrap_or_default();
    let jobs: i64 = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM improvement_jobs WHERE origin_request=?1 AND job_type='response'",
            [request_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        jobs, 0,
        "management completion must not trigger automatic evaluation"
    );
    assert_eq!(identity.permissions.roster_write, true);
}

#[tokio::test]
async fn foreman_workflow_tools_create_and_evaluate() {
    let (upstream, script) = upstream(vec![]).await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    let w = workflow(&agent.id);
    state.portfolio.workflow_insert(&w, 0).unwrap();
    script.lock().unwrap().0=vec![
        message(&json!({"decision":"tool","name":"create_workflow","arguments":{"requirements":"workflow"}}).to_string()),message(&json!(w).to_string()),
        message(&json!({"decision":"tool","name":"evaluate_workflow","arguments":{"id":w.id,"task":"inspect","result":"observed work"}}).to_string()),keep(),
        message(&json!({"decision":"use","agent_id":agent.id}).to_string()),message("report")
    ].into();
    crate::runtime::dispatch(&state, &tool_payload(), "internal-flow")
        .await
        .unwrap();
    assert_eq!(state.portfolio.workflow_list().unwrap().len(), 2);
    assert_eq!(
        state
            .portfolio
            .evaluations()
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn malformed_requests_never_call_upstream() {
    let state = state("http://127.0.0.1:1", false);
    let proxy = proxy(state).await;
    for body in [
        "not JSON",
        "[]",
        r#"{"messages":[]}"#,
        r#"{"messages":[{"role":"unknown"}]}"#,
    ] {
        let response = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", proxy.url))
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
    }
}

#[tokio::test]
async fn upstream_failure_is_recorded_as_failure() {
    let (upstream, _) = upstream(vec![json!({"__status":502,"error":"provider failed"})]).await;
    let state = state(&upstream.url, false);
    let proxy = proxy(state.clone()).await;
    assert_eq!(request(&proxy, "task", false).await.status(), 503);
    let stats = state.portfolio.statistics().unwrap();
    assert!(
        stats["groups"]
            .as_array()
            .unwrap()
            .iter()
            .any(|g| g["kind"] == "foreman_decision" && g["status"] == "error")
    );
}

#[test]
fn definitions_statistics_and_paused_runs_survive_reopen() {
    let path = std::env::temp_dir()
        .join(format!("ccm-test-{}.db", uuid::Uuid::new_v4()))
        .to_string_lossy()
        .into_owned();
    let id;
    {
        let p = Portfolio::open(&path).unwrap();
        let agent = p
            .insert(&NewAgent {
                provider: String::new(),
                name: "a".into(),
                purpose: "p".into(),
                system_prompt: "s".into(),
                model: "m".into(),
            })
            .unwrap();
        id = agent.id.clone();
        p.workflow_insert(&workflow(&id), 0).unwrap();
        p.record_usage(&id);
        p.event(
            "r",
            "agent_call",
            &id,
            "ok",
            42,
            Some(&json!({"prompt_tokens":2,"completion_tokens":3})),
            "",
        )
        .unwrap();
        p.save_run("paused", &json!({"stage":"work"}), 0).unwrap();
    }
    {
        let p = Portfolio::open(&path).unwrap();
        assert!(p.get(&id).unwrap().is_some());
        assert_eq!(p.workflow_list().unwrap().len(), 1);
        assert_eq!(p.statistics().unwrap()["groups"][0]["duration_ms"], 42);
        assert_eq!(p.claim_run("paused").unwrap()["stage"], "work");
        assert!(p.claim_run("paused").is_err());
    }
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn workflow_runs_do_not_share_outputs() {
    async fn echo(State(workflow_id): State<String>, Json(body): Json<Value>) -> Json<Value> {
        let messages = body["messages"].as_array().unwrap();
        let system = messages[0]["content"].as_str().unwrap();
        if system.contains("You are the Foreman") {
            return Json(message(
                &json!({"decision":"use_workflow","workflow_id":workflow_id}).to_string(),
            ));
        }
        if system.contains("meta-agent") {
            return Json(keep());
        }
        let content = messages.iter().find(|m| m["role"] == "user").unwrap()["content"]
            .as_str()
            .unwrap();
        Json(message(content))
    }
    let wid = uuid::Uuid::new_v4().to_string();
    let upstream = serve(
        Router::new()
            .route("/chat/completions", post(echo))
            .with_state(wid.clone()),
    )
    .await;
    let state = state(&upstream.url, false);
    let agent = insert(&state);
    let mut w = workflow(&agent.id);
    w.id = wid;
    w.stages.truncate(1);
    w.stages[0].branches.clear();
    w.stages[0].next = None;
    state.portfolio.workflow_insert(&w, 0).unwrap();
    let p1 = json!({"messages":[{"role":"user","content":"first marker"}]});
    let p2 = json!({"messages":[{"role":"user","content":"second marker"}]});
    let (one, two) = tokio::join!(
        crate::runtime::dispatch(&state, &p1, "one"),
        crate::runtime::dispatch(&state, &p2, "two")
    );
    let one = one.unwrap().report;
    let two = two.unwrap().report;
    assert!(one.contains("first marker"));
    assert!(!one.contains("second marker"));
    assert!(two.contains("second marker"));
    assert!(!two.contains("first marker"));
}
