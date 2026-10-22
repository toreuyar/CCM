//! Approved personality/output-channel contracts; authored, not run automatically.
use super::*;
use crate::config::{PersonalityTransparency, ServerConfig};
use tokio::sync::{Semaphore, mpsc};

fn extrovert(url: &str) -> Arc<AppState> {
    let mut s = state(url, false);
    let config = Arc::make_mut(&mut Arc::make_mut(&mut s).config);
    config.personality_transparency = PersonalityTransparency::Extrovert;
    config.upstream.total_timeout_ms = 10_000;
    config.upstream.first_response_timeout_ms = 5000;
    config.upstream.idle_timeout_ms = 5000;
    s
}

fn thought(content: &str, reasoning: &str) -> Value {
    let mut value = message(content);
    value["choices"][0]["message"]["reasoning"] = json!(reasoning);
    value
}

#[tokio::test]
async fn flexible_switch_is_client_scoped_and_starts_on_next_request() {
    let (provider, script) = upstream(vec![]).await;
    let mut s = extrovert(&provider.url);
    Arc::make_mut(&mut Arc::make_mut(&mut s).config).personality_transparency =
        PersonalityTransparency::Flexible;
    let agent = insert(&s);
    let server = proxy(s.clone()).await;
    for (key, switch, expected_thinking) in [
        ("alice", Some("extrovert"), false),
        ("bob", None, false),
        ("alice", None, true),
        ("alice", Some("introvert"), true),
        ("alice", None, false),
    ] {
        let prompt = switch
            .map(|mode| format!("Switch to {mode}."))
            .unwrap_or("Answer the task.".into());
        let mut replies = vec![];
        if let Some(mode) = switch {
            replies.push(thought(&json!({"decision":"tool","name":"set_personality","arguments":{"mode":mode,"user_instruction":prompt},"chat_narration_allowed":true}).to_string(), "SWITCH_THOUGHT"));
        }
        replies.extend([
            routing(&agent.id, true),
            message("report"),
            thought("FINAL", "COMPOSITION_THOUGHT"),
        ]);
        script.lock().unwrap().0 = replies.into();
        let response = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", server.url))
            .bearer_auth(key)
            .json(&json!({"stream":true,"messages":[{"role":"user","content":prompt}]}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let text = response.text().await.unwrap();
        let (content, thinking) = output(&text);
        assert!(content.ends_with("FINAL"), "{text}");
        assert_eq!(!thinking.is_empty(), expected_thinking, "{text}");
        assert_eq!(content != "FINAL", expected_thinking, "{text}");
    }
}

#[test]
fn personality_switch_requires_flexible_valid_mode_and_latest_user_quote() {
    let mut s = state("http://unused", false);
    let payload = json!({"messages":[{"role":"user","content":"Switch to extrovert."},{"role":"user","content":"Switch to introvert."}]});
    let run = s
        .portfolio
        .trace_begin("switch", &payload, "external")
        .unwrap();
    s.portfolio
        .bind_policy("switch", &run, "alice", s.config.creation.policy)
        .unwrap();
    let args = json!({"mode":"introvert","user_instruction":"Switch to introvert."});
    assert!(crate::transparency::switch_personality(&s, "switch", &args, &payload).is_err());
    Arc::make_mut(&mut Arc::make_mut(&mut s).config).personality_transparency =
        PersonalityTransparency::Flexible;
    for invalid in [
        json!({"mode":"extrovert","user_instruction":"Switch to extrovert."}),
        json!({"mode":"flexible","user_instruction":"Switch to introvert."}),
        json!({"mode":"introvert","user_instruction":""}),
    ] {
        assert!(crate::transparency::switch_personality(&s, "switch", &invalid, &payload).is_err());
    }
    assert_eq!(
        crate::transparency::switch_personality(&s, "switch", &args, &payload).unwrap()["effective_from"],
        "next_request"
    );
    s.portfolio
        .set_client_personality("alice", "extrovert")
        .unwrap();
    Arc::make_mut(&mut Arc::make_mut(&mut s).config).personality_transparency =
        PersonalityTransparency::Introvert;
    assert!(
        crate::transparency::personality_contract(&s, "switch")
            .unwrap()
            .contains("effective for this response: \"introvert\"")
    );
}

#[test]
fn personality_preferences_survive_database_reopen() {
    let path = std::env::temp_dir().join(format!("personality-{}.db", uuid::Uuid::new_v4()));
    {
        let db = Portfolio::open(path.to_str().unwrap()).unwrap();
        db.set_client_personality("alice", "extrovert").unwrap();
        db.storage.as_ref().unwrap().flush().unwrap();
    }
    {
        let db = Portfolio::open(path.to_str().unwrap()).unwrap();
        assert_eq!(
            db.client_personality("alice").unwrap(),
            PersonalityTransparency::Extrovert
        );
        assert_eq!(
            db.client_personality("bob").unwrap(),
            PersonalityTransparency::Introvert
        );
    }
    std::fs::remove_file(&path).unwrap();
    let _ = std::fs::remove_file(format!("{}.ccm-lock", path.display()));
}

fn routing(agent: &str, allowed: bool) -> Value {
    thought(&json!({"decision":"use","agent_id":agent,"note":"Its scope fits this task.","chat_narration_allowed":allowed}).to_string(), "ROUTING_THOUGHT")
}

fn frames(text: &str) -> Vec<Value> {
    text.lines()
        .filter_map(|s| s.strip_prefix("data: "))
        .filter(|s| *s != "[DONE]")
        .map(|s| serde_json::from_str(s).unwrap())
        .collect()
}

fn output(text: &str) -> (String, String) {
    let mut content = String::new();
    let mut thinking = String::new();
    for frame in frames(text) {
        let delta = &frame["choices"][0]["delta"];
        if let Some(s) = delta["content"].as_str() {
            content.push_str(s);
        }
        if let Some(s) = delta["reasoning_content"].as_str() {
            thinking.push_str(s);
        }
        assert!(
            delta.get("reasoning").is_none(),
            "client uses canonical reasoning_content"
        );
    }
    (content, thinking)
}

#[test]
fn server_enum_defaults_to_introvert_and_accepts_deployment_extrovert() {
    let path = std::env::temp_dir().join(format!("personality-{}.json", uuid::Uuid::new_v4()));
    assert_eq!(
        ServerConfig::load(&path, false)
            .unwrap()
            .personality_transparency,
        PersonalityTransparency::Introvert
    );
    std::fs::write(&path, "{}").unwrap();
    assert_eq!(
        ServerConfig::load(&path, true)
            .unwrap()
            .personality_transparency,
        PersonalityTransparency::Introvert
    );
    std::fs::write(&path, r#"{"personality_transparency":"extrovert"}"#).unwrap();
    assert_eq!(
        ServerConfig::load(&path, true)
            .unwrap()
            .personality_transparency,
        PersonalityTransparency::Extrovert
    );
    std::fs::write(&path, r#"{"personality_transparency":"ambivert"}"#).unwrap();
    assert!(ServerConfig::load(&path, true).is_err());
    std::fs::write(&path, r#"{"personality_transparency":"flexible"}"#).unwrap();
    assert_eq!(
        ServerConfig::load(&path, true)
            .unwrap()
            .personality_transparency,
        PersonalityTransparency::Flexible
    );
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn strict_chat_contracts_leave_thinking_available_in_both_response_modes() {
    for stream in [false, true] {
        for explicit_format in [false, true] {
            let (provider, script) = upstream(vec![]).await;
            let s = extrovert(&provider.url);
            let agent = insert(&s);
            // Deliberately allow in the model when response_format must override it.
            script.lock().unwrap().0 = vec![
                routing(&agent.id, explicit_format),
                message("report"),
                thought(r#"{"answer":42}"#, "COMPOSITION_THOUGHT"),
            ]
            .into();
            let server = proxy(s.clone()).await;
            let mut body = json!({"stream":stream,"messages":[{"role":"system","content":"Output only JSON, with no extra text."},{"role":"user","content":"answer"}]});
            if explicit_format {
                body["response_format"] = json!({"type":"json_schema","json_schema":{"name":"answer","schema":{"type":"object"}}});
            }
            let response = reqwest::Client::new()
                .post(format!("{}/v1/chat/completions", server.url))
                .json(&body)
                .send()
                .await
                .unwrap();
            let (content, thinking) = if stream {
                output(&response.text().await.unwrap())
            } else {
                let value: Value = response.json().await.unwrap();
                let message = &value["choices"][0]["message"];
                assert!(message.get("ccm_narration").is_none());
                (
                    message["content"].as_str().unwrap().into(),
                    message["reasoning_content"].as_str().unwrap().into(),
                )
            };
            assert_eq!(content, r#"{"answer":42}"#);
            assert!(thinking.contains("ROUTING_THOUGHT"));
            assert!(thinking.contains("COMPOSITION_THOUGHT"));
            let request = &script.lock().unwrap().1[0];
            assert!(
                request["messages"][0]["content"]
                    .as_str()
                    .unwrap()
                    .contains("Output only JSON")
            );
        }
    }
}

#[tokio::test]
async fn introvert_hides_internal_channels_and_request_cannot_override_server() {
    for stream in [false, true] {
        let (provider, script) = upstream(vec![]).await;
        let s = state(&provider.url, false);
        let agent = insert(&s);
        script.lock().unwrap().0 = vec![
            routing(&agent.id, true),
            thought("report", "SPECIALIST_THOUGHT"),
            thought("ANSWER", "COMPOSITION_THOUGHT"),
        ]
        .into();
        let server = proxy(s).await;
        let response = reqwest::Client::new().post(format!("{}/v1/chat/completions",server.url)).json(&json!({"stream":stream,"personality_transparency":"extrovert","messages":[{"role":"user","content":"task"}]})).send().await.unwrap();
        if stream {
            assert_eq!(
                output(&response.text().await.unwrap()),
                ("ANSWER".into(), String::new())
            );
        } else {
            let value: Value = response.json().await.unwrap();
            assert_eq!(
                value["choices"][0]["message"],
                json!({"role":"assistant","content":"ANSWER"})
            );
        }
    }
}

#[derive(Clone)]
struct Gate {
    script: Script,
    started: mpsc::Sender<usize>,
    release: Arc<Semaphore>,
}
async fn gated(State(gate): State<Gate>, Json(body): Json<Value>) -> Json<Value> {
    let (index, response) = {
        let mut s = gate.script.lock().unwrap();
        let index = s.1.len();
        s.1.push(body);
        (index, s.0.pop_front().unwrap())
    };
    gate.started.send(index).await.unwrap();
    if index == 1 {
        gate.release.acquire().await.unwrap().forget();
    }
    Json(response)
}

#[tokio::test]
async fn routing_narration_is_live_and_precedes_specialist_work() {
    let script: Script = Arc::new(Mutex::new((VecDeque::new(), vec![])));
    let (started, mut calls) = mpsc::channel(8);
    let release = Arc::new(Semaphore::new(0));
    let provider = serve(
        Router::new()
            .route("/chat/completions", post(gated))
            .with_state(Gate {
                script: script.clone(),
                started,
                release: release.clone(),
            }),
    )
    .await;
    let s = extrovert(&provider.url);
    let agent = insert(&s);
    script.lock().unwrap().0 = vec![
        routing(&agent.id, true),
        message("report"),
        thought("FINAL ANSWER", "COMPOSITION_THOUGHT"),
    ]
    .into();
    let server = proxy(s.clone()).await;
    let mut response = request(&server, "task", true).await;
    let run = response.headers()["x-ccm-run-id"]
        .to_str()
        .unwrap()
        .to_string();
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !String::from_utf8_lossy(&bytes).contains("Delegating to specialist") {
            bytes.extend_from_slice(&response.chunk().await.unwrap().unwrap());
        }
        assert_eq!(calls.recv().await, Some(0));
        assert_eq!(calls.recv().await, Some(1));
    })
    .await
    .unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("FINAL ANSWER"));
    let events = s.portfolio.trace_events(0, Some(&run)).unwrap();
    let announcement = events
        .iter()
        .find(|e| {
            e["kind"] == "response.delta"
                && e["data"]["text"]
                    .as_str()
                    .is_some_and(|t| t.contains("Delegating to specialist"))
        })
        .unwrap()["seq"]
        .as_i64()
        .unwrap();
    let acting = events
        .iter()
        .find(|e| e["kind"] == "span.started" && e["data"]["kind"] == "agent_call")
        .unwrap()["seq"]
        .as_i64()
        .unwrap();
    assert!(announcement < acting);
    release.add_permits(1);
    while let Some(chunk) = response.chunk().await.unwrap() {
        bytes.extend_from_slice(&chunk);
    }
    let text = String::from_utf8(bytes).unwrap();
    let (content, thinking) = output(&text);
    assert!(content.ends_with("FINAL ANSWER"));
    assert_eq!(content.matches("FINAL ANSWER").count(), 1);
    assert!(thinking.contains("ROUTING_THOUGHT"));
    assert!(!content.contains("ROUTING_THOUGHT"));
    assert!(text.ends_with("data: [DONE]\n\n"));
}

#[tokio::test]
async fn internal_tools_are_observations_in_thinking_not_client_tool_calls() {
    let (provider, script) = upstream(vec![]).await;
    let s = extrovert(&provider.url);
    let agent = insert(&s);
    script.lock().unwrap().0=vec![thought(r#"{"decision":"tool","name":"get_statistics","arguments":{},"chat_narration_allowed":true}"#,"TOOL_THOUGHT"),routing(&agent.id,true),message("report"),message("FINAL")].into();
    let server = proxy(s).await;
    let text = request(&server, "task", true).await.text().await.unwrap();
    let (content, thinking) = output(&text);
    assert!(content.contains("Calling internal operation 'get_statistics'"));
    assert!(thinking.contains("Foreman internal tool call"));
    assert!(thinking.contains("Foreman internal tool result"));
    assert!(!content.contains("\"arguments\""));
    assert!(
        frames(&text)
            .iter()
            .all(|f| f.pointer("/choices/0/delta/tool_calls").is_none())
    );
}

#[tokio::test]
async fn no_narration_or_thinking_is_appended_after_answer_content() {
    let (provider, script) = upstream(vec![]).await;
    let s = extrovert(&provider.url);
    let agent = insert(&s);
    let composition = json!(format!(
        "data: {}\n\ndata: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"delta":{"reasoning":"BEFORE"}}]}),
        json!({"choices":[{"delta":{"content":"FINAL"}}]}),
        json!({"choices":[{"delta":{"reasoning":"AFTER"},"finish_reason":"stop"}]})
    ));
    script.lock().unwrap().0 =
        vec![routing(&agent.id, true), message("report"), composition].into();
    let server = proxy(s).await;
    let text = request(&server, "task", true).await.text().await.unwrap();
    let (content, thinking) = output(&text);
    assert!(content.ends_with("FINAL"));
    assert!(thinking.contains("BEFORE"));
    assert!(!thinking.contains("AFTER"));
    let chunks = frames(&text);
    let start = chunks
        .iter()
        .position(|f| f["choices"][0]["delta"]["content"] == "FINAL")
        .unwrap();
    assert!(
        chunks[start + 1..]
            .iter()
            .all(|f| f["choices"][0]["delta"]["content"].is_null()
                && f["choices"][0]["delta"]["reasoning_content"].is_null())
    );
}

#[tokio::test]
async fn unvalidated_routing_thinking_is_never_exposed_even_when_transport_retries() {
    let (provider, script) = upstream(vec![
        json!(
            "data: {\"choices\":[{\"delta\":{\"reasoning\":\"PARTIAL_THOUGHT\"}}]}\n\n"
        );
        3
    ])
    .await;
    let mut s = extrovert(&provider.url);
    Arc::make_mut(&mut Arc::make_mut(&mut s).config)
        .upstream
        .max_attempts = 3;
    let server = proxy(s).await;
    let text = request(&server, "task", true).await.text().await.unwrap();
    assert!(!text.contains("PARTIAL_THOUGHT"));
    assert!(text.contains("premature_eof"));
    assert_eq!(script.lock().unwrap().1.len(), 3);
}

#[tokio::test]
async fn buffered_failed_reasoning_is_discarded_on_retry() {
    let (provider, script) = upstream(vec![]).await;
    let mut s = extrovert(&provider.url);
    let config = Arc::make_mut(&mut Arc::make_mut(&mut s).config);
    config.upstream.max_attempts = 2;
    config.upstream.retry_base_ms = 1;
    let agent = insert(&s);
    script.lock().unwrap().0 = vec![
        json!("data: {\"choices\":[{\"delta\":{\"reasoning\":\"DISCARD_ME\"}}]}\n\n"),
        routing(&agent.id, true),
        message("report"),
        message("FINAL"),
    ]
    .into();
    let server = proxy(s).await;
    let value: Value = request(&server, "task", false).await.json().await.unwrap();
    assert_eq!(value["choices"][0]["message"]["content"], "FINAL");
    assert!(
        !value["choices"][0]["message"]["reasoning_content"]
            .as_str()
            .unwrap()
            .contains("DISCARD_ME")
    );
}

#[tokio::test]
async fn native_progress_is_live_replayable_and_separate_from_final_message() {
    let script: Script = Arc::new(Mutex::new((VecDeque::new(), vec![])));
    let (started, mut calls) = mpsc::channel(8);
    let release = Arc::new(Semaphore::new(0));
    let provider = serve(
        Router::new()
            .route("/chat/completions", post(gated))
            .with_state(Gate {
                script: script.clone(),
                started,
                release: release.clone(),
            }),
    )
    .await;
    let s = extrovert(&provider.url);
    let agent = insert(&s);
    script.lock().unwrap().0 = vec![
        routing(&agent.id, true),
        message("report"),
        message("FINAL"),
    ]
    .into();
    let server = serve(crate::web::routes().with_state(s.clone())).await;
    let client = reqwest::Client::new();
    let accepted: Value = client
        .post(format!("{}/api/chat", server.url))
        .json(&json!({"message":"task"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        assert_eq!(calls.recv().await, Some(0));
        assert_eq!(calls.recv().await, Some(1));
    })
    .await
    .unwrap();
    let detail = s
        .portfolio
        .trace_detail(accepted["run_id"].as_str().unwrap())
        .unwrap();
    let p = &detail["presentations"][accepted["request_id"].as_str().unwrap()];
    assert!(p["narration"].as_str().unwrap().contains("Delegating"));
    assert!(p["reasoning"].as_str().unwrap().contains("ROUTING_THOUGHT"));
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let conversation: Value = client
                .get(format!(
                    "{}/api/conversations/{}",
                    server.url,
                    accepted["conversation_id"].as_str().unwrap()
                ))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if let Some(message) = conversation["messages"]
                .as_array()
                .unwrap()
                .iter()
                .find(|m| m["role"] == "assistant")
            {
                assert_eq!(message["content"], "FINAL");
                assert!(message["ccm_narration"].is_string());
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn client_tools_stay_executable_and_resume_rechecks_content_contract() {
    let (provider, script) = upstream(vec![]).await;
    let s = extrovert(&provider.url);
    let agent = insert(&s);
    script.lock().unwrap().0 = vec![
        routing(&agent.id, true),
        tool_message(),
        thought(
            r#"{"chat_narration_allowed":false}"#,
            "RESUME_CONTRACT_THOUGHT",
        ),
        message("report"),
        message("EXACT"),
    ]
    .into();
    let server = proxy(s).await;
    let mut body = tool_payload();
    body["stream"] = json!(true);
    let client = reqwest::Client::new();
    let first = client
        .post(format!("{}/v1/chat/completions", server.url))
        .json(&body)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let mut collector = crate::upstream::Collector::default();
    for frame in frames(&first) {
        collector.feed(&frame).unwrap();
    }
    let assistant = collector.finish()["choices"][0]["message"].clone();
    let calls = assistant["tool_calls"].as_array().unwrap();
    assert_eq!(calls.len(), 1);
    assert!(
        calls
            .iter()
            .all(|c| c["id"].as_str().unwrap().starts_with("ccm-agent:"))
    );
    let messages = body["messages"].as_array_mut().unwrap();
    messages.insert(0,json!({"role":"system","content":"On this continuation output only EXACT. No extra chat text."}));
    messages.push(assistant.clone());
    for call in calls {
        messages
            .push(json!({"role":"tool","tool_call_id":call["id"],"content":"client observation"}));
    }
    let text = client
        .post(format!("{}/v1/chat/completions", server.url))
        .json(&body)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let (content, thinking) = output(&text);
    assert_eq!(content, "EXACT");
    assert!(thinking.contains("RESUME_CONTRACT_THOUGHT"));
    let requests = &script.lock().unwrap().1;
    assert_eq!(requests.len(), 5);
    assert!(
        requests[2]["messages"][0]["content"]
            .as_str()
            .unwrap()
            .contains("On this continuation output only EXACT")
    );
    assert!(
        requests[3]["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["role"] == "tool")
    );
}

#[tokio::test]
async fn workflow_selection_and_creation_announce_before_their_operations() {
    for create in [false, true] {
        let (provider, script) = upstream(vec![]).await;
        let s = extrovert(&provider.url);
        let agent = insert(&s);
        let mut w = workflow(&agent.id);
        w.stages.truncate(1);
        w.stages[0].branches.clear();
        w.stages[0].next = None;
        let mut responses = if create {
            vec![message(&json!({"decision":"create_workflow","requirements":"one stage","note":"Reusable staged task.","chat_narration_allowed":true}).to_string()),message(&json!(w).to_string())]
        } else {
            s.portfolio.workflow_insert(&w, 0).unwrap();
            vec![message(&json!({"decision":"use_workflow","workflow_id":w.id,"note":"Fits.","chat_narration_allowed":true}).to_string())]
        };
        responses.extend([message("stage report"), message("FINAL")]);
        script.lock().unwrap().0 = responses.into();
        let server = proxy(s.clone()).await;
        let response = request(&server, "task", true).await;
        let run = response.headers()["x-ccm-run-id"]
            .to_str()
            .unwrap()
            .to_string();
        let text = response.text().await.unwrap();
        let (content, _) = output(&text);
        assert!(content.contains(if create {
            "Requesting a new workflow"
        } else {
            "I found workflow"
        }));
        assert!(content.ends_with("FINAL"));
        let events = s.portfolio.trace_events(0, Some(&run)).unwrap();
        let announce = events
            .iter()
            .find(|e| {
                e["kind"] == "response.delta"
                    && e["data"]["text"].as_str().is_some_and(|s| {
                        s.contains(if create {
                            "Requesting a new workflow"
                        } else {
                            "I found workflow"
                        })
                    })
            })
            .unwrap()["seq"]
            .as_i64()
            .unwrap();
        let action = events
            .iter()
            .find(|e| {
                e["kind"] == "span.started"
                    && e["data"]["kind"]
                        == if create {
                            "definition_creation"
                        } else {
                            "workflow_execution"
                        }
            })
            .unwrap()["seq"]
            .as_i64()
            .unwrap();
        assert!(announce < action);
    }
}

#[tokio::test]
async fn missing_narration_verdict_fails_quiet_without_hiding_thinking() {
    let (provider, script) = upstream(vec![]).await;
    let s = extrovert(&provider.url);
    let agent = insert(&s);
    script.lock().unwrap().0 = vec![
        thought(
            &json!({"decision":"use","agent_id":agent.id}).to_string(),
            "VISIBLE_THOUGHT",
        ),
        message("report"),
        message("FINAL"),
    ]
    .into();
    let server = proxy(s).await;
    let (content, thinking) = output(&request(&server, "task", true).await.text().await.unwrap());
    assert_eq!(content, "FINAL");
    assert!(thinking.contains("VISIBLE_THOUGHT"));
}

#[tokio::test]
async fn final_cutoff_and_spawned_background_work_cannot_emit_narration() {
    let s = extrovert("http://unused");
    let request = "isolated";
    s.portfolio
        .trace_begin(request, &json!({"messages":[]}), "web")
        .unwrap();
    let (tx, mut rx) = mpsc::channel(8);
    let p = crate::transparency::Presentation::new(
        s.clone(),
        request,
        &json!({"messages":[]}),
        Some(tx),
        false,
    )
    .unwrap();
    p.scope(async {
        crate::transparency::resolve_contract(request, &json!({"chat_narration_allowed":true}))
            .await
            .unwrap();
        s.announce(request, "BEFORE").await.unwrap();
        let background = s.clone();
        tokio::spawn(async move {
            background.announce(request, "BACKGROUND").await.unwrap();
        })
        .await
        .unwrap();
        crate::transparency::begin_final(request).await;
        s.announce(request, "DURING_FINAL").await.unwrap();
        p.finish(&mut json!({"role":"assistant","content":"FINAL"}))
            .await
            .unwrap();
        s.announce(request, "AFTER").await.unwrap();
        assert!(
            !crate::transparency::thinking(request, "late", &json!("AFTER"))
                .await
                .unwrap()
        );
    })
    .await;
    assert_eq!(
        rx.try_recv().unwrap()["choices"][0]["delta"]["content"],
        "BEFORE\n\n"
    );
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn concurrent_presentations_keep_permissions_and_channels_request_local() {
    let s = extrovert("http://unused");
    for request in ["a", "b"] {
        s.portfolio
            .trace_begin(request, &json!({"messages":[]}), "external")
            .unwrap();
    }
    let (tx_a, mut rx_a) = mpsc::channel(4);
    let (tx_b, mut rx_b) = mpsc::channel(4);
    let a = crate::transparency::Presentation::new(
        s.clone(),
        "a",
        &json!({"messages":[]}),
        Some(tx_a),
        false,
    )
    .unwrap();
    let b = crate::transparency::Presentation::new(
        s.clone(),
        "b",
        &json!({"messages":[],"response_format":{"type":"json_object"}}),
        Some(tx_b),
        false,
    )
    .unwrap();
    tokio::join!(
        a.scope(async {
            crate::transparency::resolve_contract("a", &json!({"chat_narration_allowed":true}))
                .await
                .unwrap();
            tokio::task::yield_now().await;
            s.announce("a", "A_ONLY").await.unwrap();
            s.announce("b", "WRONG_REQUEST").await.unwrap();
        }),
        b.scope(async {
            crate::transparency::resolve_contract("b", &json!({"chat_narration_allowed":true}))
                .await
                .unwrap();
            tokio::task::yield_now().await;
            s.announce("b", "SUPPRESSED").await.unwrap();
            crate::transparency::thinking("b", "B_ONLY", &json!({}))
                .await
                .unwrap();
        })
    );
    assert_eq!(
        rx_a.try_recv().unwrap()["choices"][0]["delta"]["content"],
        "A_ONLY\n\n"
    );
    assert!(rx_a.try_recv().is_err());
    let thought = rx_b.try_recv().unwrap();
    assert!(
        thought["choices"][0]["delta"]["reasoning_content"]
            .as_str()
            .unwrap()
            .contains("B_ONLY")
    );
    assert!(thought["choices"][0]["delta"].get("content").is_none());
    assert!(rx_b.try_recv().is_err());
}
