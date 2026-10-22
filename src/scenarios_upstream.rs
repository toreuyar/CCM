//! Provider reliability regressions. Scripted HTTP only; run when authorized.
use super::*;
use tokio::sync::mpsc;

#[derive(Clone)]
struct Reply {
    status: u16,
    content_type: &'static str,
    retry_after: Option<&'static str>,
    headers_delay_ms: u64,
    chunks: Vec<(u64, Vec<u8>)>,
}

impl Reply {
    fn sse(text: &str) -> Self {
        Self {
            status: 200,
            content_type: "text/event-stream",
            retry_after: None,
            headers_delay_ms: 0,
            chunks: vec![(0, text.as_bytes().to_vec())],
        }
    }
    fn json(text: &str) -> Self {
        Self {
            content_type: "application/json",
            ..Self::sse(text)
        }
    }
    fn status(status: u16) -> Self {
        Self {
            status,
            ..Self::json("{}")
        }
    }
}

type Replies = Arc<Mutex<(VecDeque<Reply>, Vec<(Instant, Value)>)>>;
async fn respond(State(replies): State<Replies>, Json(body): Json<Value>) -> Response {
    let reply = {
        let mut replies = replies.lock().unwrap();
        replies.1.push((Instant::now(), body));
        replies.0.pop_front().expect("unexpected provider attempt")
    };
    tokio::time::sleep(Duration::from_millis(reply.headers_delay_ms)).await;
    let stream = async_stream::stream! {
        for (delay, chunk) in reply.chunks {
            if delay > 0 { tokio::time::sleep(Duration::from_millis(delay)).await; }
            yield Ok::<_, std::io::Error>(bytes::Bytes::from(chunk));
        }
    };
    let mut response = Response::builder()
        .status(reply.status)
        .header("content-type", reply.content_type)
        .header("x-request-id", "fixture-request");
    if let Some(value) = reply.retry_after {
        response = response.header("retry-after", value);
    }
    response.body(Body::from_stream(stream)).unwrap()
}

async fn fixture(replies: Vec<Reply>) -> (Server, Replies, Arc<AppState>) {
    let replies = Arc::new(Mutex::new((replies.into(), vec![])));
    let server = serve(
        Router::new()
            .route("/chat/completions", post(respond))
            .with_state(replies.clone()),
    )
    .await;
    let mut state = state(&server.url, false);
    Arc::make_mut(&mut Arc::make_mut(&mut state).config).upstream = crate::config::UpstreamConfig {
        first_response_timeout_ms: 1000,
        idle_timeout_ms: 1000,
        total_timeout_ms: 5000,
        max_attempts: 3,
        retry_base_ms: 5,
        ..Default::default()
    };
    (server, replies, state)
}

fn budgets(state: &mut Arc<AppState>) -> &mut crate::config::UpstreamConfig {
    &mut Arc::make_mut(&mut Arc::make_mut(state).config).upstream
}

fn input() -> Value {
    json!({"model":"test","messages":[{"role":"system","content":"Original instruction"},{"role":"user","content":"task"}]})
}

async fn call(state: &Arc<AppState>, sink: Option<mpsc::Sender<Value>>) -> Result<Value, String> {
    crate::upstream::call_with_sink(
        state,
        "provider-test",
        "foreman_decision",
        "foreman",
        input(),
        sink,
    )
    .await
}

fn detail(state: &AppState) -> Value {
    let run = state
        .portfolio
        .trace_context("provider-test")
        .unwrap()
        .unwrap()
        .0;
    state.portfolio.trace_detail(&run).unwrap()
}

fn failure(state: &AppState) -> Value {
    detail(state)["spans"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["output"]["upstream"].is_object())
        .unwrap()["output"]["upstream"]
        .clone()
}

const ANSWER: &str = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"answer\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
const REASONING: &str = "data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning\":\"thinking\"},\"finish_reason\":null}]}\n\n";

fn admission(state: &mut Arc<AppState>, limit: usize, cooldown: u64) {
    Arc::make_mut(state).providers = Arc::new(crate::provider_gate::Registry::new(
        &std::collections::BTreeMap::from([(
            String::new(),
            crate::model_config::Provider {
                max_concurrent_requests: Some(limit),
                recovery: crate::provider_gate::Recovery {
                    cooldown_ms: cooldown,
                    max_cooldown_ms: cooldown * 4,
                },
                ..Default::default()
            },
        )]),
    ));
}

#[tokio::test]
async fn provider_limit_covers_entire_stream_across_resolved_states() {
    let mut slow = Reply::sse(ANSWER);
    slow.chunks = vec![(80, ANSWER.as_bytes().to_vec())];
    let (_server, replies, mut state) = fixture(vec![slow, Reply::sse(ANSWER)]).await;
    admission(&mut state, 1, 25);
    let other = crate::model_config::with_config(&state, (*state.config).clone()).unwrap();
    let (a, b) = tokio::join!(
        crate::upstream::call(&state, "foreground", "composition", "foreman", input()),
        crate::upstream::call(&other, "background", "evaluation", "evaluator", input())
    );
    assert!(a.is_ok() && b.is_ok(), "{a:?} {b:?}");
    let calls = &replies.lock().unwrap().1;
    assert_eq!(calls.len(), 2);
    assert!(calls[1].0.duration_since(calls[0].0) >= Duration::from_millis(75));
}

#[tokio::test]
async fn separate_jobs_share_cooldown_after_nonretryable_provider_error() {
    let (_server, replies, mut state) = fixture(vec![Reply::status(401), Reply::sse(ANSWER)]).await;
    admission(&mut state, 1, 80);
    assert!(
        crate::upstream::call(&state, "failed-job", "evaluation", "evaluator", input())
            .await
            .is_err()
    );
    crate::upstream::call(&state, "next-job", "evaluation", "evaluator", input())
        .await
        .unwrap();
    let calls = &replies.lock().unwrap().1;
    assert_eq!(calls.len(), 2);
    assert!(calls[1].0.duration_since(calls[0].0) >= Duration::from_millis(75));
}

#[tokio::test]
async fn queued_disconnect_sends_no_http_and_releases_queue_position() {
    let (_server, replies, mut state) = fixture(vec![Reply::sse(ANSWER)]).await;
    admission(&mut state, 1, 25);
    let gate = state.providers.get("", &Default::default());
    let occupied = gate.acquire().await;
    let (tx, rx) = mpsc::channel(4);
    let s = state.clone();
    let queued = tokio::spawn(async move { call(&s, Some(tx)).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(rx);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), queued)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err()
            .contains("client disconnected")
    );
    assert!(replies.lock().unwrap().1.is_empty());
    drop(occupied);
    call(&state, None).await.unwrap();
    assert_eq!(replies.lock().unwrap().1.len(), 1);
}

#[tokio::test]
async fn active_reasoning_survives_multiple_idle_windows() {
    let mut reply = Reply::sse("");
    reply.chunks = (0..10)
        .map(|_| (70, REASONING.as_bytes().to_vec()))
        .collect();
    reply.chunks.push((0, ANSWER.as_bytes().to_vec()));
    let (_server, requests, mut state) = fixture(vec![reply]).await;
    budgets(&mut state).idle_timeout_ms = 300;
    let result = call(&state, None).await.unwrap();
    assert_eq!(result["choices"][0]["message"]["content"], "answer");
    assert!(
        result["choices"][0]["message"]["reasoning"]
            .as_str()
            .unwrap()
            .len()
            >= 70
    );
    assert_eq!(requests.lock().unwrap().1.len(), 1);
}

#[tokio::test]
async fn idle_failure_is_classified_and_retains_progress() {
    let mut reply = Reply::sse(REASONING);
    reply.chunks.push((1500, ANSWER.as_bytes().to_vec()));
    let (_server, _, mut state) = fixture(vec![reply]).await;
    budgets(&mut state).idle_timeout_ms = 150;
    budgets(&mut state).max_attempts = 1;
    assert!(
        call(&state, None)
            .await
            .unwrap_err()
            .contains("idle_timeout")
    );
    let error = failure(&state);
    assert_eq!(error["http_status"], 200);
    assert_eq!(error["provider_request_id"], "fixture-request");
    assert_eq!(error["events"], 1);
    assert!(error["last_activity_ms"].is_number());
    assert_eq!(error["client_output_sent"], false);
}

#[tokio::test]
async fn total_budget_stops_an_active_stream_without_retry() {
    let mut reply = Reply::sse("");
    reply.chunks = (0..100)
        .map(|_| (20, REASONING.as_bytes().to_vec()))
        .collect();
    let (_server, requests, mut state) = fixture(vec![reply]).await;
    budgets(&mut state).total_timeout_ms = 300;
    assert!(
        call(&state, None)
            .await
            .unwrap_err()
            .contains("total_timeout")
    );
    assert_eq!(requests.lock().unwrap().1.len(), 1);
    assert!(failure(&state)["events"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn delayed_headers_and_first_body_byte_have_separate_diagnostics() {
    for headers in [true, false] {
        let mut reply = Reply::sse(ANSWER);
        if headers {
            reply.headers_delay_ms = 1500;
        } else {
            reply.chunks[0].0 = 1500;
        }
        let (_server, _, mut state) = fixture(vec![reply]).await;
        budgets(&mut state).first_response_timeout_ms = 150;
        budgets(&mut state).max_attempts = 1;
        assert!(
            call(&state, None)
                .await
                .unwrap_err()
                .contains("first_response_timeout")
        );
        assert_eq!(
            failure(&state)["stage"],
            if headers {
                "headers"
            } else {
                "first_body_byte"
            }
        );
    }
}

#[tokio::test]
async fn raw_error_body_is_logged_before_decoding_and_correlated_with_request() {
    let mut reply = Reply::status(400);
    reply.chunks = vec![(0, vec![0xff, 0, b'!'])];
    let (_server, _, state) = fixture(vec![reply]).await;
    assert!(
        call(&state, None)
            .await
            .unwrap_err()
            .contains("http_status")
    );
    let events = state.portfolio.trace_events(0, None).unwrap();
    let raw = events
        .iter()
        .find(|e| e["kind"] == "upstream.body.chunk")
        .unwrap();
    assert_eq!(raw["data"]["body"]["bytes_hex"], "ff0021");
    assert_eq!(raw["data"]["request_id"], "provider-test");
    assert!(raw["data"]["body"]["text"].is_null());
    let request = events
        .iter()
        .find(|e| e["kind"] == "upstream.request")
        .unwrap();
    assert_eq!(request["span_id"], raw["span_id"]);
    assert_eq!(request["data"]["body"]["messages"], input()["messages"]);
    assert!(
        events
            .iter()
            .any(|e| e["kind"] == "upstream.response.headers" && e["data"]["status"] == 400)
    );
}

#[tokio::test]
async fn transient_status_retries_preserve_input_and_isolate_attempts() {
    let (_server, requests, state) = fixture(vec![
        Reply::status(503),
        Reply::status(429),
        Reply::sse(ANSWER),
    ])
    .await;
    assert!(call(&state, None).await.is_ok());
    let requests = requests.lock().unwrap();
    assert_eq!(requests.1.len(), 3);
    assert_eq!(requests.1[0].1, requests.1[2].1);
    assert_eq!(requests.1[0].1["messages"], input()["messages"]);
    let detail = detail(&state);
    let spans: Vec<_> = detail["spans"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["kind"] == "foreman_decision")
        .collect();
    assert_eq!(spans.len(), 3);
    assert_eq!(spans[0]["status"], "error");
    assert_eq!(spans[2]["status"], "completed");
}

#[tokio::test]
async fn retries_stop_at_attempt_limit_and_on_permanent_http_errors() {
    for status in [400, 401, 403, 422, 503] {
        let count = if status == 503 { 3 } else { 1 };
        let (_server, requests, state) = fixture(vec![Reply::status(status); count]).await;
        assert!(
            call(&state, None)
                .await
                .unwrap_err()
                .contains("http_status")
        );
        assert_eq!(requests.lock().unwrap().1.len(), count);
    }
}

#[tokio::test]
async fn retry_after_is_honored_and_cannot_escape_total_budget() {
    let throttled = Reply {
        retry_after: Some("1"),
        ..Reply::status(429)
    };
    let (_server, requests, state) = fixture(vec![throttled.clone(), Reply::sse(ANSWER)]).await;
    call(&state, None).await.unwrap();
    let elapsed = {
        let r = requests.lock().unwrap();
        r.1[1].0.duration_since(r.1[0].0)
    };
    assert!(elapsed >= Duration::from_secs(1));
    let (_server, requests, mut state) = fixture(vec![throttled]).await;
    budgets(&mut state).total_timeout_ms = 300;
    assert!(call(&state, None).await.is_err());
    assert_eq!(requests.lock().unwrap().1.len(), 1);
    assert_eq!(failure(&state)["retry_after_ms"], 1000);
}

#[tokio::test]
async fn partial_internal_output_is_discarded_before_retry() {
    let partial = "data: {\"choices\":[{\"delta\":{\"content\":\"discard me\",\"tool_calls\":[{\"index\":0,\"id\":\"partial\",\"function\":{\"name\":\"do_work\",\"arguments\":\"{\"}}]}}]}\n\n";
    let (_server, requests, state) = fixture(vec![Reply::sse(partial), Reply::sse(ANSWER)]).await;
    let result = call(&state, None).await.unwrap();
    assert_eq!(result["choices"][0]["message"]["content"], "answer");
    assert!(result["choices"][0]["message"].get("tool_calls").is_none());
    assert_eq!(requests.lock().unwrap().1.len(), 2);
}

#[tokio::test]
async fn partial_client_output_prevents_retry_even_for_reasoning_only() {
    let (_server, requests, state) = fixture(vec![Reply::sse(REASONING)]).await;
    let (tx, mut rx) = mpsc::channel(8);
    assert!(
        call(&state, Some(tx))
            .await
            .unwrap_err()
            .contains("premature_eof")
    );
    assert!(rx.recv().await.unwrap()["choices"][0]["delta"]["reasoning"].is_string());
    assert_eq!(requests.lock().unwrap().1.len(), 1);
    assert_eq!(failure(&state)["client_output_sent"], true);
}

#[tokio::test]
async fn malformed_and_incomplete_protocol_results_never_become_success() {
    for reply in [
        Reply::sse("data: {invalid}\n\n"),
        Reply::sse("data: [DONE]\n\n"),
        Reply::sse(&format!("{REASONING}data: [DONE]\n\n")),
        Reply::json("<html>bad gateway</html>"),
        Reply::json(r#"{"choices":[{"message":{"role":"assistant","content":"answer"}}]}"#),
        Reply::json(
            r#"{"choices":[{"message":{"role":"assistant","tool_calls":[{"id":"x","type":"function","function":{"name":"lookup","arguments":"{"}}]},"finish_reason":"tool_calls"}]}"#,
        ),
    ] {
        let (_server, requests, state) = fixture(vec![reply]).await;
        assert!(call(&state, None).await.is_err());
        assert_eq!(requests.lock().unwrap().1.len(), 1);
    }
}

#[tokio::test]
async fn multiline_crlf_and_split_utf8_tool_frames_are_assembled() {
    let frames = concat!(
        ": heartbeat\r\n\r\n",
        "data: {\"choices\":[\r\ndata: {\"index\":0,\"delta\":{\"content\":\"café\",\"tool_calls\":[{\"index\":0,\"id\":\"tool_1\",\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\":\"}}]},\"finish_reason\":null}]}\r\n\r\n",
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"\\\"Paris\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\r\r",
        "data: [DONE]\r\r"
    );
    let mut reply = Reply::sse("");
    // Byte-sized fragments exercise partial UTF-8 and split CRLF delimiters.
    reply.chunks = frames.as_bytes().iter().map(|&b| (0, vec![b])).collect();
    let (_server, _, state) = fixture(vec![reply]).await;
    let value = call(&state, None).await.unwrap();
    assert_eq!(value["choices"][0]["message"]["content"], "café");
    assert_eq!(
        value["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"],
        r#"{"q":"Paris"}"#
    );
}

#[tokio::test]
async fn nonstreaming_json_stall_uses_idle_budget_and_retry_recovers() {
    let mut stalled = Reply::json("{");
    stalled.chunks.push((1500, b"}".to_vec()));
    let (_server, requests, mut state) =
        fixture(vec![stalled, Reply::json(&message("complete").to_string())]).await;
    budgets(&mut state).idle_timeout_ms = 150;
    assert_eq!(
        call(&state, None).await.unwrap()["choices"][0]["message"]["content"],
        "complete"
    );
    assert_eq!(requests.lock().unwrap().1.len(), 2);
}

#[tokio::test]
async fn cancellation_during_backoff_does_not_send_another_attempt() {
    let (_server, requests, state) = fixture(vec![Reply {
        retry_after: Some("2"),
        ..Reply::status(503)
    }])
    .await;
    let worker = state.clone();
    let task = tokio::spawn(async move { call(&worker, None).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if state
                .portfolio
                .trace_events(0, None)
                .unwrap()
                .iter()
                .any(|e| e["kind"] == "upstream.retry" && e["data"]["scheduled"] == true)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(requests.lock().unwrap().1.len(), 1);
}

#[tokio::test]
async fn workforce_counts_each_attempt_without_new_assignment() {
    let (_server, requests, state) = fixture(vec![Reply::status(503), Reply::sse(ANSWER)]).await;
    let agent = insert(&state);
    crate::upstream::call(&state, "provider-test", "agent_call", &agent.id, input())
        .await
        .unwrap();
    assert_eq!(requests.lock().unwrap().1.len(), 2);
    let conn = state.portfolio.conn.lock().unwrap();
    let calls: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM workforce_usage WHERE entity_id=?1 AND kind='model_call'",
            [&agent.id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(calls, 2);
    let assignments: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM workforce_usage WHERE entity_id=?1 AND kind='assignment'",
            [&agent.id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(assignments, 0);
}

#[tokio::test]
async fn truncated_http_body_retains_underlying_transport_cause() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut input = Vec::new();
        loop {
            let mut bytes = [0; 4096];
            let n = socket.read(&mut bytes).await.unwrap();
            assert!(n > 0);
            input.extend_from_slice(&bytes[..n]);
            if let Some(end) = input.windows(4).position(|w| w == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&input[..end]).to_ascii_lowercase();
                let length: usize = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length:"))
                    .unwrap()
                    .trim()
                    .parse()
                    .unwrap();
                if input.len() >= end + 4 + length {
                    break;
                }
            }
        }
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 100000\r\nConnection: close\r\n\r\n").await.unwrap();
        socket.write_all(REASONING.as_bytes()).await.unwrap();
        socket.shutdown().await.unwrap();
    });
    let mut state = state(&url, false);
    budgets(&mut state).first_response_timeout_ms = 2000;
    assert!(
        call(&state, None)
            .await
            .unwrap_err()
            .contains("body_transport")
    );
    assert!(failure(&state)["causes"].as_array().unwrap().len() > 1);
    task.await.unwrap();
}
