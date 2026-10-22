//! Observability acceptance scenarios. Execution requires user authorization.
use super::*;
use crate::observe::Span;

#[tokio::test]
async fn sse_reconnect_honors_last_event_id_over_query_cursor() {
    use futures::StreamExt;
    let state = state("http://127.0.0.1:1", false);
    let run = state
        .portfolio
        .trace_begin("r", &json!({"messages":[]}), "external")
        .unwrap();
    let cursor = state
        .portfolio
        .trace_events(0, Some(&run))
        .unwrap()
        .last()
        .unwrap()["seq"]
        .as_i64()
        .unwrap();
    state
        .portfolio
        .trace_emit(
            &run,
            None,
            "after.disconnect",
            &json!({"evidence":"retained"}),
        )
        .unwrap();
    let server = serve(crate::web::routes().with_state(state)).await;
    let response = reqwest::Client::new()
        .get(format!("{}/api/events?after=0&run={run}", server.url))
        .header("last-event-id", cursor.to_string())
        .send()
        .await
        .unwrap();
    let mut stream = response.bytes_stream();
    let event = tokio::time::timeout(Duration::from_secs(2), async {
        let mut bytes = Vec::new();
        loop {
            bytes.extend_from_slice(&stream.next().await.unwrap().unwrap());
            let text = String::from_utf8_lossy(&bytes);
            if text.contains("\n\n") {
                let data = text
                    .lines()
                    .find_map(|line| line.strip_prefix("data: "))
                    .unwrap();
                break serde_json::from_str::<Value>(data).unwrap();
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(event["kind"], "after.disconnect");
    assert!(event["seq"].as_i64().unwrap() > cursor);
}

#[tokio::test]
async fn replay_after_process_epoch_change_resets_to_a_fresh_snapshot() {
    use futures::StreamExt;
    let state = state("http://127.0.0.1:1", false);
    let run = state
        .portfolio
        .trace_begin("epoch-test", &json!({"messages":[]}), "external")
        .unwrap();
    state
        .portfolio
        .trace_emit(&run, None, "retained", &json!({}))
        .unwrap();
    let epoch = state.portfolio.trace_epoch.clone();
    let server = serve(crate::web::routes().with_state(state)).await;
    let response = reqwest::Client::new()
        .get(format!("{}/api/events", server.url))
        .header("last-event-id", "previous-process:99999")
        .send()
        .await
        .unwrap();
    let mut stream = response.bytes_stream();
    let text = tokio::time::timeout(Duration::from_secs(2), async {
        let mut bytes = Vec::new();
        loop {
            bytes.extend_from_slice(&stream.next().await.unwrap().unwrap());
            if bytes.windows(2).any(|w| w == b"\n\n") {
                return String::from_utf8(bytes).unwrap();
            }
        }
    })
    .await
    .unwrap();
    assert!(text.contains("event: trace_reset"));
    assert!(text.contains(&format!("id: {epoch}:")));
    assert!(text.contains("memory_history_reset"));
}

#[test]
fn reset_retains_trace_versions_and_invalidates_outstanding_tools() {
    let state = state("http://127.0.0.1:1", true);
    let agent = insert(&state);
    let run = state
        .portfolio
        .trace_begin("first", &tool_payload(), "external")
        .unwrap();
    let version = state
        .portfolio
        .trace_snapshot(&agent.id, "agent", &json!(agent))
        .unwrap();
    let mut span = Span::start(
        state.portfolio.clone(),
        "first",
        "agent_execution",
        &agent.id,
        &json!({}),
        Some(&version),
        true,
    )
    .unwrap();
    let call = json!({"role":"assistant","tool_calls":[{"id":"owned-call","type":"function","function":{"name":"lookup","arguments":"{}"}}]});
    state.portfolio.trace_bind_tools("first", &call).unwrap();
    span.finish("waiting_tools", &call, None).unwrap();
    state
        .portfolio
        .trace_finish_request("first", "waiting_tools", &call)
        .unwrap();
    state.portfolio.reset().unwrap();
    assert!(state.portfolio.list().unwrap().is_empty());
    let detail = state.portfolio.trace_detail(&run).unwrap();
    assert_eq!(
        detail["versions"][0]["definition"]["system_prompt"],
        agent.system_prompt
    );
    assert!(
        detail["spans"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["kind"] == "client_tool" && s["status"] == "reset")
    );
    assert!(
        !state
            .portfolio
            .trace_events(0, Some(&run))
            .unwrap()
            .is_empty()
    );
    let resume =
        json!({"messages":[call,{"role":"tool","tool_call_id":"owned-call","content":"done"}]});
    assert!(
        state
            .portfolio
            .trace_begin("second", &resume, "external")
            .is_err()
    );
}

#[test]
fn continuation_requires_complete_batch_and_rejects_replay() {
    let state = state("http://127.0.0.1:1", false);
    let run = state
        .portfolio
        .trace_begin("first", &tool_payload(), "external")
        .unwrap();
    let call = json!({"role":"assistant","tool_calls":[{"id":"a","function":{"name":"lookup"}},{"id":"b","function":{"name":"lookup"}}]});
    state.portfolio.trace_bind_tools("first", &call).unwrap();
    state
        .portfolio
        .trace_finish_request("first", "waiting_tools", &call)
        .unwrap();
    let partial = json!({"messages":[call,{"role":"tool","tool_call_id":"a","content":"one"}]});
    assert!(
        state
            .portfolio
            .trace_begin("partial", &partial, "external")
            .is_err()
    );
    let complete = json!({"messages":[call,{"role":"tool","tool_call_id":"b","content":"two"},{"role":"tool","tool_call_id":"a","content":"one"}]});
    assert_eq!(
        state
            .portfolio
            .trace_begin("resume", &complete, "external")
            .unwrap(),
        run
    );
    assert!(
        state
            .portfolio
            .trace_begin("replay", &complete, "external")
            .is_err()
    );
    let detail = state.portfolio.trace_detail(&run).unwrap();
    let tools: Vec<_> = detail["spans"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["kind"] == "client_tool")
        .collect();
    assert_eq!(tools.len(), 2);
    assert!(
        tools
            .iter()
            .all(|s| s["status"] == "completed" && s["output"]["role"] == "tool")
    );
}

#[test]
fn durable_event_pages_replay_in_order_beyond_notification_capacity() {
    let state = state("http://127.0.0.1:1", false);
    let run = state
        .portfolio
        .trace_begin("r", &json!({"messages":[]}), "external")
        .unwrap();
    for index in 0..610 {
        state
            .portfolio
            .trace_emit(&run, None, "evidence", &json!({"index":index}))
            .unwrap();
    }
    let first = state.portfolio.trace_events(0, Some(&run)).unwrap();
    assert_eq!(first.len(), 500);
    let cursor = first.last().unwrap()["seq"].as_i64().unwrap();
    let second = state.portfolio.trace_events(cursor, Some(&run)).unwrap();
    assert_eq!(second.len(), 111);
    let sequences: Vec<_> = first
        .iter()
        .chain(&second)
        .map(|e| e["seq"].as_i64().unwrap())
        .collect();
    assert!(sequences.windows(2).all(|s| s[1] > s[0]));
    assert_eq!(second.last().unwrap()["data"]["index"], 609);
}

#[tokio::test]
async fn guidance_waits_for_target_and_pause_blocks_until_resume() {
    let (provider, script) = upstream(vec![message("guided")]).await;
    let state = state(&provider.url, false);
    let run = state
        .portfolio
        .trace_begin("r", &json!({"messages":[]}), "web")
        .unwrap();
    let mut group = Span::start(
        state.portfolio.clone(),
        "r",
        "agent_execution",
        "worker",
        &json!({}),
        None,
        true,
    )
    .unwrap();
    state
        .portfolio
        .control(&run, "worker", "guidance", "Use the revised constraint")
        .unwrap();
    state
        .portfolio
        .control(&run, "worker", "pause", "")
        .unwrap();
    let worker_state = state.clone();
    let task = tokio::spawn(async move {
        crate::upstream::call(
            &worker_state,
            "r",
            "agent_call",
            "worker",
            json!({"model":"test","messages":[{"role":"system","content":"work"}]}),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if state.portfolio.trace_runs().unwrap()[0]["status"] == "paused" {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(script.lock().unwrap().1.is_empty());
    state
        .portfolio
        .control(&run, "worker", "resume", "")
        .unwrap();
    assert!(task.await.unwrap().is_ok());
    let sent = script.lock().unwrap().1[0].clone();
    assert!(
        sent["messages"].as_array().unwrap().last().unwrap()["content"]
            .as_str()
            .unwrap()
            .contains("revised constraint")
    );
    let detail = state.portfolio.trace_detail(&run).unwrap();
    assert_eq!(detail["controls"][0]["status"], "delivered");
    assert!(detail["controls"][0]["span_id"].is_string());
    group.finish("completed", &json!({}), None).unwrap();
}

#[tokio::test]
async fn interrupt_restarts_active_model_with_guidance_and_keeps_partial_evidence() {
    let (provider, script) =
        upstream(vec![json!({"__stream_delay":true}), message("redirected")]).await;
    let state = state(&provider.url, false);
    let run = state
        .portfolio
        .trace_begin("r", &json!({"messages":[]}), "web")
        .unwrap();
    let worker = state.clone();
    let task = tokio::spawn(async move {
        crate::upstream::call(
            &worker,
            "r",
            "agent_call",
            "worker",
            json!({"messages":[{"role":"system","content":"work"}]}),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if state
                .portfolio
                .trace_events(0, Some(&run))
                .unwrap()
                .iter()
                .any(|e| e["kind"] == "model.delta")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    state
        .portfolio
        .control(&run, "worker", "interrupt", "Change direction")
        .unwrap();
    let result = task.await.unwrap().unwrap();
    assert_eq!(result["choices"][0]["message"]["content"], "redirected");
    let script = script.lock().unwrap();
    assert_eq!(script.1.len(), 2);
    assert!(
        script.1[1]["messages"].as_array().unwrap().last().unwrap()["content"]
            .as_str()
            .unwrap()
            .contains("Change direction")
    );
    let detail = state.portfolio.trace_detail(&run).unwrap();
    assert!(
        detail["spans"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["status"] == "interrupted")
    );
    assert!(
        state
            .portfolio
            .trace_events(0, Some(&run))
            .unwrap()
            .iter()
            .any(|e| e["kind"] == "model.delta"
                && e["data"]["choices"][0]["delta"]["content"] == "first")
    );
}

#[tokio::test]
async fn native_and_external_chat_share_traced_pipeline() {
    let (provider, script) = upstream(vec![]).await;
    let state = state(&provider.url, false);
    let agent = insert(&state);
    let decision = message(&json!({"decision":"use","agent_id":agent.id}).to_string());
    script.lock().unwrap().0 = vec![
        decision.clone(),
        message("report"),
        message("native answer"),
        decision,
        message("report"),
        message("external answer"),
    ]
    .into();
    let server = serve(
        Router::new()
            .merge(crate::web::routes())
            .route("/v1/chat/completions", post(handlers::chat_completions))
            .with_state(state.clone()),
    )
    .await;
    let client = reqwest::Client::new();
    let submitted: Value = client
        .post(format!("{}/api/chat", server.url))
        .json(&json!({"message":"native task"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let conversation = submitted["conversation_id"].as_str().unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let body: Value = client
                .get(format!("{}/api/conversations/{conversation}", server.url))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if body["messages"].as_array().unwrap().len() == 2 {
                assert_eq!(body["messages"][1]["content"], "native answer");
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let native_run = submitted["run_id"].as_str().unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.portfolio.improvements(native_run).unwrap()[0]["status"] != "pending_checkpoint"
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let events = state.portfolio.trace_events(0, Some(native_run)).unwrap();
    let published = events
        .iter()
        .find(|e| e["kind"] == "conversation.message")
        .unwrap()["seq"]
        .as_i64()
        .unwrap();
    let released = events
        .iter()
        .find(|e| e["kind"] == "improvement.changed" && e["data"]["status"] == "pending_checkpoint")
        .unwrap()["seq"]
        .as_i64()
        .unwrap();
    assert!(published < released);
    let response = client
        .post(format!("{}/v1/chat/completions", server.url))
        .header("x-ccm-conversation-id", "external-session")
        .json(&json!({"messages":[{"role":"user","content":"external task"}]}))
        .send()
        .await
        .unwrap();
    let run = response.headers()["x-ccm-run-id"]
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(
        response.json::<Value>().await.unwrap()["choices"][0]["message"]["content"],
        "external answer"
    );
    let runs = state.portfolio.trace_runs().unwrap();
    assert!(runs.as_array().unwrap().iter().any(|r| r["id"] == run
        && r["source"] == "external"
        && r["conversation_id"] == "external-session"));
    assert!(
        runs.as_array()
            .unwrap()
            .iter()
            .any(|r| r["id"] == submitted["run_id"] && r["source"] == "web")
    );
    let detail = state.portfolio.trace_detail(&run).unwrap();
    assert!(
        detail["spans"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["kind"] == "composition" && s["version_id"].is_string())
    );
}

#[test]
fn otlp_preserves_trace_parentage_and_definition_version() {
    let state = state("http://127.0.0.1:1", false);
    let run = state
        .portfolio
        .trace_begin("request", &json!({"messages":[]}), "external")
        .unwrap();
    let version = state
        .portfolio
        .trace_snapshot("worker", "agent", &json!({"system_prompt":"original"}))
        .unwrap();
    let mut span = Span::start(
        state.portfolio.clone(),
        "request",
        "agent_call",
        "worker",
        &json!({}),
        Some(&version),
        false,
    )
    .unwrap();
    span.finish("completed", &message("output"), None).unwrap();
    state
        .portfolio
        .trace_finish_request("request", "completed", &json!({}))
        .unwrap();
    let otlp = state.portfolio.otlp(&run, None).unwrap();
    let spans = otlp["resourceSpans"][0]["scopeSpans"][0]["spans"]
        .as_array()
        .unwrap();
    let child = spans.iter().find(|s| s["spanId"] == span.id).unwrap();
    assert_eq!(child["traceId"].as_str().unwrap().len(), 32);
    assert_eq!(child["spanId"].as_str().unwrap().len(), 16);
    assert!(spans.iter().any(|s| s["spanId"] == child["parentSpanId"]));
    assert!(
        child["attributes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["key"] == "ccm.definition.version" && a["value"]["stringValue"] == version)
    );
}
