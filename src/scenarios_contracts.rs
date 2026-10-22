//! Client-contract and narration-free streaming regressions. Run when authorized.
use super::*;
use tokio::sync::{Semaphore, mpsc};

#[derive(Clone)]
struct Gated {
    responses: Script,
    started: mpsc::Sender<usize>,
    release: Arc<Semaphore>,
}

async fn gated(State(state): State<Gated>, Json(body): Json<Value>) -> Json<Value> {
    let (index, response) = {
        let mut script = state.responses.lock().unwrap();
        let index = script.1.len();
        script.1.push(body);
        (
            index,
            script.0.pop_front().expect("unexpected provider call"),
        )
    };
    state.started.send(index).await.unwrap();
    state.release.acquire().await.unwrap().forget();
    Json(response)
}

fn contract_state(url: &str) -> Arc<AppState> {
    let mut state = state(url, false);
    let config = Arc::make_mut(&mut Arc::make_mut(&mut state).config);
    config.upstream.first_response_timeout_ms = 5000;
    config.upstream.idle_timeout_ms = 5000;
    config.upstream.total_timeout_ms = 20_000;
    state
}

async fn read_until(response: &mut reqwest::Response, bytes: &mut Vec<u8>, needle: &str) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !String::from_utf8_lossy(bytes).contains(needle) {
            bytes.extend_from_slice(
                &response
                    .chunk()
                    .await
                    .unwrap()
                    .expect("stream ended before expected frame"),
            );
        }
    })
    .await
    .expect("initial stream frame was buffered behind the blocked operation");
}

fn chunks(text: &str) -> Vec<Value> {
    text.lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter(|raw| *raw != "[DONE]")
        .map(|raw| serde_json::from_str(raw).unwrap())
        .collect()
}

fn collected(text: &str) -> Value {
    let mut collector = crate::upstream::Collector::default();
    for chunk in chunks(text) {
        collector.feed(&chunk).unwrap();
    }
    collector.finish()["choices"][0]["message"].clone()
}

#[tokio::test]
async fn early_stream_keeps_progress_in_dashboard_only() {
    let script: Script = Arc::new(Mutex::new((VecDeque::new(), vec![])));
    let (started, mut calls) = mpsc::channel(8);
    let release = Arc::new(Semaphore::new(0));
    let provider = serve(
        Router::new()
            .route("/chat/completions", post(gated))
            .with_state(Gated {
                responses: script.clone(),
                started,
                release: release.clone(),
            }),
    )
    .await;
    let state = contract_state(&provider.url);
    let agent = insert(&state);
    script.lock().unwrap().0 = vec![
        message(
            &json!({"decision":"use","agent_id":agent.id,"note":"Fits this task."}).to_string(),
        ),
        message("report"),
        message("FINAL ANSWER"),
    ]
    .into();
    let server = proxy(state.clone()).await;
    let mut response = tokio::time::timeout(Duration::from_secs(2), request(&server, "task", true))
        .await
        .expect("headers waited for dispatch");
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["x-accel-buffering"], "no");
    let run = response.headers()["x-ccm-run-id"]
        .to_str()
        .unwrap()
        .to_string();
    let mut bytes = Vec::new();
    read_until(&mut response, &mut bytes, "\"role\":\"assistant\"").await;
    for (index, announcement) in [
        "Foreman: Reviewing",
        "Specialist 'writer': Working",
        "Foreman: Composing",
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), calls.recv())
                .await
                .unwrap(),
            Some(index)
        );
        let events = state.portfolio.trace_events(0, Some(&run)).unwrap();
        assert!(events.iter().any(|event| {
            event["kind"] == "operation.progress"
                && event["data"]["text"]
                    .as_str()
                    .is_some_and(|text| text.contains(announcement))
        }));
        assert!(!String::from_utf8_lossy(&bytes).contains("FINAL ANSWER"));
        assert!(calls.try_recv().is_err());
        release.add_permits(1);
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(chunk) = response.chunk().await.unwrap() {
            bytes.extend_from_slice(&chunk);
        }
    })
    .await
    .expect("stream senders were not released at completion");
    let text = String::from_utf8(bytes).unwrap();
    let frames = chunks(&text);
    let id = frames[0]["id"].clone();
    let timestamp = frames[0]["created"].clone();
    assert!(frames.iter().all(|c| c["id"] == id
        && c["created"] == timestamp
        && c["object"] == "chat.completion.chunk"));
    assert!(
        frames
            .iter()
            .all(|c| c.pointer("/choices/0/delta/reasoning").is_none())
    );
    let answer = collected(&text);
    let content = answer["content"].as_str().unwrap();
    assert_eq!(content, "FINAL ANSWER");
    assert!(text.ends_with("data: [DONE]\n\n"));
    let detail = state.portfolio.trace_detail(&run).unwrap();
    let root = detail["spans"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["kind"] == "request")
        .unwrap();
    assert_eq!(root["output"]["content"], answer["content"]);
}

#[tokio::test]
async fn streamed_tool_handoff_has_no_narration_and_resumes_owning_agent() {
    let (provider, script) = upstream(vec![]).await;
    let state = contract_state(&provider.url);
    let agent = insert(&state);
    script.lock().unwrap().0 = vec![
        message(&json!({"decision":"use","agent_id":agent.id}).to_string()),
        tool_message(),
        message("observed tool result"),
        message("FINAL ANSWER"),
    ]
    .into();
    let server = proxy(state.clone()).await;
    let client = reqwest::Client::new();
    let mut payload = tool_payload();
    payload["stream"] = json!(true);
    let first = client
        .post(format!("{}/v1/chat/completions", server.url))
        .json(&payload)
        .send()
        .await
        .unwrap();
    let run = first.headers()["x-ccm-run-id"].clone();
    let text = first.text().await.unwrap();
    assert!(!text.contains("Requesting client execution of tools:"));
    assert!(text.contains("\"finish_reason\":\"tool_calls\""));
    let assistant = collected(&text);
    let tool = assistant["tool_calls"][0]["id"].clone();
    assert_eq!(assistant["tool_calls"][0]["function"]["name"], "lookup");
    payload["messages"].as_array_mut().unwrap().extend([
        assistant,
        json!({"role":"tool","tool_call_id":tool,"content":"observation"}),
    ]);
    let second = client
        .post(format!("{}/v1/chat/completions", server.url))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(second.headers()["x-ccm-run-id"], run);
    let answer = collected(&second.text().await.unwrap());
    assert_eq!(answer["content"], "FINAL ANSWER");
    assert_eq!(script.lock().unwrap().1.len(), 4);
    let snapshot = state.portfolio.workforce(crate::observe::now_ms()).unwrap();
    let entity = snapshot["entities"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["id"] == agent.id)
        .unwrap();
    assert_eq!(entity["usage"]["1h"]["assignment"], 1);
    assert_eq!(entity["usage"]["1h"]["model_call"], 2);
}

#[tokio::test]
async fn disconnect_during_dispatch_cancels_the_inflight_foreman_call() {
    let script: Script = Arc::new(Mutex::new((vec![message("unused")].into(), vec![])));
    let (started, mut calls) = mpsc::channel(8);
    let release = Arc::new(Semaphore::new(0));
    let provider = serve(
        Router::new()
            .route("/chat/completions", post(gated))
            .with_state(Gated {
                responses: script.clone(),
                started,
                release,
            }),
    )
    .await;
    let state = contract_state(&provider.url);
    let server = proxy(state.clone()).await;
    let mut response = request(&server, "task", true).await;
    let run = response.headers()["x-ccm-run-id"]
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), calls.recv())
            .await
            .unwrap(),
        Some(0)
    );
    read_until(&mut response, &mut Vec::new(), "\"role\":\"assistant\"").await;
    drop(response);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let detail = state.portfolio.trace_detail(&run).unwrap();
            let cancelled = detail["spans"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|s| s["status"] == "cancelled")
                .count();
            if cancelled >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("disconnect left dispatch running");
    assert_eq!(script.lock().unwrap().1.len(), 1);
}

#[tokio::test]
async fn streaming_failure_has_no_narration_or_success_terminator() {
    let (provider, _) = upstream(vec![json!({"__status":503,"error":"fixture unavailable"})]).await;
    let server = proxy(contract_state(&provider.url)).await;
    let response = request(&server, "task", true).await;
    assert_eq!(response.status(), 200);
    let text = response.text().await.unwrap();
    assert!(!text.contains("Foreman: Reviewing"));
    assert!(chunks(&text).iter().any(|c| {
        c["error"]["message"]
            .as_str()
            .is_some_and(|s| s.contains("503"))
    }));
    assert!(!text.contains("[DONE]"));
    assert!(!text.contains("\"finish_reason\":\"stop\""));
}

#[tokio::test]
async fn strict_client_contracts_reach_routing_execution_queued_evaluation_and_composition() {
    for (instruction, answer, format) in [
        (
            "Generate ONLY a thread title for the user's message, not a story.",
            "A Short Story Request",
            None,
        ),
        (
            "Classify the user's message. Output ONLY JSON with a topic key.",
            "{\"topic\":\"story\"}",
            Some(json!({"type":"json_object"})),
        ),
    ] {
        for stream in [false, true] {
            let (provider, script) = upstream(vec![]).await;
            let state = contract_state(&provider.url);
            let agent = insert(&state);
            script.lock().unwrap().0 = vec![
                message(&json!({"decision":"use","agent_id":agent.id}).to_string()),
                message("report"),
                message(answer),
            ]
            .into();
            let server = proxy(state.clone()).await;
            let original = json!([
                {"role":"system","content":instruction},
                {"role":"developer","content":"Do not include a preamble or operational commentary."},
                {"role":"user","content":"Tell me a short story."}
            ]);
            let mut payload = json!({"messages":original,"stream":stream});
            if let Some(format) = &format {
                payload["response_format"] = format.clone();
            }
            let response = reqwest::Client::new()
                .post(format!("{}/v1/chat/completions", server.url))
                .json(&payload)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            let result = if stream {
                collected(&response.text().await.unwrap())
            } else {
                response.json::<Value>().await.unwrap()["choices"][0]["message"].clone()
            };
            assert_eq!(result["content"], answer);
            let calls = script.lock().unwrap();
            assert_eq!(calls.1.len(), 3);
            let routing = calls.1[0]["messages"][1]["content"].as_str().unwrap();
            assert!(routing.contains(instruction));
            assert!(routing.contains("developer"));
            assert!(
                calls.1[0]["messages"][0]["content"]
                    .as_str()
                    .unwrap()
                    .contains("RESPOND WITH ONLY STRICT JSON")
            );
            for message in original.as_array().unwrap() {
                assert!(calls.1[1]["messages"].as_array().unwrap().contains(message));
                assert!(calls.1[2]["messages"].as_array().unwrap().contains(message));
            }
            let task: String = state
                .portfolio
                .conn
                .lock()
                .unwrap()
                .query_row("SELECT task FROM improvement_jobs LIMIT 1", [], |r| {
                    r.get(0)
                })
                .unwrap();
            assert!(task.contains(instruction));
            assert_eq!(calls.1[2].get("response_format"), format.as_ref());
        }
    }
}

#[tokio::test]
async fn creation_paths_receive_the_original_client_contract() {
    for internal_tool in [false, true] {
        let (provider, script) = upstream(vec![]).await;
        let state = contract_state(&provider.url);
        let existing = insert(&state);
        let decision = if internal_tool {
            json!({"decision":"tool","name":"create_agent","arguments":{"requirements":"Reusable title generator"}})
        } else {
            json!({"decision":"create","requirements":"Reusable title generator","note":"Title the conversation, not write its story"})
        };
        let mut responses = vec![
            message(&decision.to_string()),
            message(&definition().to_string()),
        ];
        if internal_tool {
            responses.push(message(
                &json!({"decision":"use","agent_id":existing.id}).to_string(),
            ));
        }
        responses.extend([message("title report"), message("Story Request")]);
        script.lock().unwrap().0 = responses.into();
        let server = proxy(state).await;
        let instruction =
            "Output ONLY a thread title; treat the user message as material to title.";
        let response = reqwest::Client::new().post(format!("{}/v1/chat/completions",server.url))
            .json(&json!({"messages":[{"role":"system","content":instruction},{"role":"user","content":"Tell me a story"}]}))
            .send().await.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.json::<Value>().await.unwrap()["choices"][0]["message"]["content"],
            "Story Request"
        );
        let calls = script.lock().unwrap();
        assert!(
            calls.1[1]["messages"][1]["content"]
                .as_str()
                .unwrap()
                .contains(instruction)
        );
        if !internal_tool {
            assert!(
                calls.1[2]["messages"]
                    .to_string()
                    .contains("Title the conversation, not write its story")
            );
        }
    }
}

#[tokio::test]
async fn ordinary_chat_returns_only_the_composed_answer() {
    let (provider, script) = upstream(vec![]).await;
    let state = state(&provider.url, false);
    let agent = insert(&state);
    script.lock().unwrap().0 = vec![
        message(&json!({"decision":"use","agent_id":agent.id}).to_string()),
        message("report"),
        message("FINAL ANSWER"),
    ]
    .into();
    let server = proxy(state).await;
    let text = request(&server, "task", true).await.text().await.unwrap();
    assert_eq!(collected(&text)["content"], "FINAL ANSWER");
    assert!(text.ends_with("data: [DONE]\n\n"));
}

#[tokio::test]
async fn workflow_and_internal_tool_progress_stays_in_dashboard() {
    let (provider, script) = upstream(vec![]).await;
    let state = contract_state(&provider.url);
    let agent = insert(&state);
    let workflow = workflow(&agent.id);
    state.portfolio.workflow_insert(&workflow, 0).unwrap();
    script.lock().unwrap().0 = vec![
        message(r#"{"decision":"tool","name":"get_statistics","arguments":{}}"#),
        message(&json!({"decision":"use_workflow","workflow_id":workflow.id}).to_string()),
        message(r#"{"done":true}"#),
        message("workflow report"),
        message("FINAL ANSWER"),
    ]
    .into();
    let server = proxy(state.clone()).await;
    let response = request(&server, "task", true).await;
    let run = response.headers()["x-ccm-run-id"]
        .to_str()
        .unwrap()
        .to_string();
    let text = response.text().await.unwrap();
    assert_eq!(collected(&text)["content"], "FINAL ANSWER");
    let events = state.portfolio.trace_events(0, Some(&run)).unwrap();
    for (announcement, kind) in [
        (
            "Calling internal operation 'get_statistics'",
            "internal_tool",
        ),
        ("Starting stage 'work'", "workflow_stage"),
    ] {
        let before = events
            .iter()
            .find(|e| {
                e["kind"] == "operation.progress"
                    && e["data"]["text"]
                        .as_str()
                        .is_some_and(|s| s.contains(announcement))
            })
            .unwrap()["seq"]
            .as_i64()
            .unwrap();
        let operation = events
            .iter()
            .find(|e| e["kind"] == "span.started" && e["data"]["kind"] == kind)
            .unwrap()["seq"]
            .as_i64()
            .unwrap();
        assert!(before < operation);
    }
}
