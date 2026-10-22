//! One provider attempt: explicit budgets, bounded framing, and causal errors.
use super::Collector;
use crate::{observe::Span, state::AppState};
use futures::StreamExt;
use serde_json::{Value, json};
use std::{error::Error, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::Instant};

const MAX_BODY: usize = 16 * 1024 * 1024;
const MAX_EVENT: usize = 1024 * 1024;

#[derive(Debug)]
pub(super) struct Failure {
    pub category: &'static str,
    pub causes: Vec<String>,
    pub transient: bool,
    pub retry_after_ms: Option<u64>,
    pub delivered: bool,
    pub diagnostics: Value,
}

impl Failure {
    fn new(category: &'static str, message: impl Into<String>, transient: bool) -> Self {
        Self {
            category,
            causes: vec![message.into()],
            transient,
            retry_after_ms: None,
            delivered: false,
            diagnostics: Value::Null,
        }
    }

    pub fn local(message: String) -> Self {
        Self::new("local", message, false)
    }

    fn transport(error: reqwest::Error) -> Self {
        let category = if error.is_timeout() {
            if error.is_connect() {
                "connect_timeout"
            } else {
                "transport_timeout"
            }
        } else if error.is_connect() {
            "connection"
        } else if error.is_builder() || error.is_redirect() {
            "invalid_request"
        } else if error.is_request() {
            "request_transport"
        } else {
            "body_transport"
        };
        let transient = !error.is_builder()
            && !error.is_redirect()
            && (error.is_timeout()
                || error.is_connect()
                || error.is_request()
                || error.is_body()
                || error.is_decode());
        let error = error.without_url();
        let mut causes = vec![error.to_string()];
        let mut source = error.source();
        while let Some(cause) = source {
            causes.push(cause.to_string());
            source = cause.source();
        }
        Self {
            causes,
            ..Self::new(category, "", transient)
        }
    }

    pub fn summary(&self) -> String {
        format!(
            "upstream {}: {} (elapsed {} ms, chunks {}, last activity {} ms)",
            self.category,
            self.causes.join(": "),
            self.diagnostics["elapsed_ms"],
            self.diagnostics["chunks"],
            self.diagnostics["last_activity_ms"]
        )
    }
}

struct Progress {
    started: Instant,
    stage: &'static str,
    status: Option<u16>,
    content_type: Option<String>,
    provider_id: Option<String>,
    bytes: usize,
    chunks: usize,
    events: usize,
    first_byte_ms: Option<u64>,
    last_activity_ms: Option<u64>,
    delivered: bool,
    processing_us: u64,
    forward_wait_us: u64,
}

impl Progress {
    fn snapshot(&self) -> Value {
        json!({"stage":self.stage,"elapsed_ms":self.started.elapsed().as_millis() as u64,
            "http_status":self.status,"content_type":self.content_type,"provider_request_id":self.provider_id,
            "bytes":self.bytes,"chunks":self.chunks,"events":self.events,
            "first_byte_ms":self.first_byte_ms,"last_activity_ms":self.last_activity_ms,
            "client_output_sent":self.delivered,"local_processing_us":self.processing_us,
            "client_enqueue_wait_us":self.forward_wait_us})
    }
}

pub(super) async fn read_call(
    state: &Arc<AppState>,
    request: &str,
    kind: &str,
    original: &Value,
    deadline: Instant,
    span: &Span,
    sink: Option<&mpsc::Sender<Value>>,
) -> Result<Value, Failure> {
    let mut progress = Progress {
        started: Instant::now(),
        stage: "headers",
        status: None,
        content_type: None,
        provider_id: None,
        bytes: 0,
        chunks: 0,
        events: 0,
        first_byte_ms: None,
        last_activity_ms: None,
        delivered: false,
        processing_us: 0,
        forward_wait_us: 0,
    };
    let result = if Instant::now() >= deadline {
        Err(Failure::new(
            "total_timeout",
            "model call exhausted its total budget",
            false,
        ))
    } else {
        tokio::time::timeout_at(
            deadline,
            perform(state, request, kind, original, span, sink, &mut progress),
        )
        .await
        .unwrap_or_else(|_| {
            Err(Failure::new(
                "total_timeout",
                "model call exhausted its total budget",
                false,
            ))
        })
    };
    match result {
        Ok(value) => {
            span.emit("upstream.attempt.completed", &progress.snapshot())
                .map_err(Failure::local)?;
            Ok(value)
        }
        Err(mut error) => {
            error.delivered = progress.delivered;
            error.diagnostics = progress.snapshot();
            // Do not persist credentials echoed in a provider error or nested URL.
            if !state.config.api_key.is_empty() {
                error.causes = error
                    .causes
                    .into_iter()
                    .map(|s| s.replace(&state.config.api_key, "[redacted]"))
                    .collect();
            }
            error.diagnostics["category"] = json!(error.category);
            error.diagnostics["causes"] = json!(error.causes);
            error.diagnostics["transient"] = json!(error.transient);
            error.diagnostics["retry_after_ms"] = json!(error.retry_after_ms);
            Err(error)
        }
    }
}

async fn perform(
    state: &Arc<AppState>,
    request: &str,
    kind: &str,
    original: &Value,
    span: &Span,
    sink: Option<&mpsc::Sender<Value>>,
    progress: &mut Progress,
) -> Result<Value, Failure> {
    let config = &state.config.upstream;
    let mut body = original.clone();
    body["stream"] = json!(true);
    body["stream_options"] = json!({"include_usage":true});
    let outgoing = state
        .http
        .post(format!(
            "{}/chat/completions",
            state.config.upstream_base_url
        ))
        .header("traceparent", format!("00-{}-{}-01", span.run, span.id));
    let outgoing = if state.config.api_key.is_empty() {
        outgoing
    } else {
        outgoing.bearer_auth(&state.config.api_key)
    };
    let outgoing = outgoing.json(&body).build().map_err(Failure::transport)?;
    span.emit(
        "upstream.request",
        &json!({"request_id":request,"operation":kind,
        "method":outgoing.method().as_str(),"url":outgoing.url().as_str(),
        "headers":crate::traffic::headers(outgoing.headers()),"body":body,
        "wire_body":outgoing.body().and_then(|b|b.as_bytes()).map(crate::traffic::bytes)}),
    )
    .map_err(Failure::local)?;
    let response = tokio::time::timeout(
        Duration::from_millis(config.first_response_timeout_ms),
        state.http.execute(outgoing),
    )
    .await
    .map_err(|_| {
        Failure::new(
            "first_response_timeout",
            "no response headers within budget",
            true,
        )
    })?
    .map_err(Failure::transport)?;
    let status = response.status();
    span.emit(
        "upstream.response.headers",
        &json!({"request_id":request,"status":status.as_u16(),
        "headers":crate::traffic::headers(response.headers())}),
    )
    .map_err(Failure::local)?;
    progress.status = Some(status.as_u16());
    progress.content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.chars().take(256).collect());
    progress.provider_id = ["x-request-id", "request-id", "x-amzn-requestid", "cf-ray"]
        .iter()
        .find_map(|name| response.headers().get(*name).and_then(|v| v.to_str().ok()))
        .map(|s| s.chars().take(256).collect());
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(retry_after_ms);
    span.emit("upstream.response", &progress.snapshot())
        .map_err(Failure::local)?;
    let http_failure = if !status.is_success() {
        let mut error = Failure::new(
            "http_status",
            format!("provider returned HTTP {status}"),
            matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504),
        );
        error.retry_after_ms = retry_after;
        Some(error)
    } else {
        None
    };
    let sse = progress.content_type.as_deref().is_some_and(|s| {
        s.split(';')
            .next()
            .unwrap_or("")
            .trim()
            .eq_ignore_ascii_case("text/event-stream")
    });
    let mut stream = response.bytes_stream();
    let mut parser = EventParser::default();
    let mut json_body = Vec::new();
    let mut collected = Collector::default();
    let mut body_deadline =
        Instant::now() + Duration::from_millis(config.first_response_timeout_ms);
    loop {
        progress.stage = if progress.chunks == 0 {
            "first_body_byte"
        } else {
            "body"
        };
        let next = tokio::time::timeout_at(body_deadline, stream.next())
            .await
            .map_err(|_| {
                Failure::new(
                    if progress.chunks == 0 {
                        "first_response_timeout"
                    } else {
                        "idle_timeout"
                    },
                    "no provider body bytes within budget",
                    true,
                )
            })?;
        let Some(bytes) = next else { break };
        let bytes = bytes.map_err(Failure::transport)?;
        if bytes.is_empty() {
            continue;
        }
        let elapsed = progress.started.elapsed().as_millis() as u64;
        progress.first_byte_ms.get_or_insert(elapsed);
        progress.last_activity_ms = Some(elapsed);
        progress.chunks += 1;
        progress.bytes += bytes.len();
        let processing = Instant::now();
        span.emit(
            "upstream.body.chunk",
            &json!({"request_id":request,
            "chunk":progress.chunks,"offset":progress.bytes-bytes.len(),
            "elapsed_ms":elapsed,"body":crate::traffic::bytes(&bytes)}),
        )
        .map_err(Failure::local)?;
        if progress.bytes > MAX_BODY {
            return Err(Failure::new(
                "response_too_large",
                "provider body exceeds 16 MiB",
                false,
            ));
        }
        if http_failure.is_some() {
            progress.processing_us += processing.elapsed().as_micros() as u64;
            body_deadline = Instant::now() + Duration::from_millis(config.idle_timeout_ms);
            continue;
        }
        if !sse {
            json_body.extend_from_slice(&bytes);
            progress.processing_us += processing.elapsed().as_micros() as u64;
            body_deadline = Instant::now() + Duration::from_millis(config.idle_timeout_ms);
            continue;
        }
        progress.stage = "sse_decode";
        for raw in parser.feed(&bytes)? {
            if raw.trim() == "[DONE]" {
                let value = collected.finish();
                validate_response(&value)?;
                progress.processing_us += processing.elapsed().as_micros() as u64;
                return Ok(value);
            }
            let chunk: Value = serde_json::from_str(&raw)
                .map_err(|e| Failure::new("invalid_sse_json", e.to_string(), false))?;
            validate_chunk(&chunk)?;
            collected
                .feed(&chunk)
                .map_err(|e| Failure::new("invalid_response", e, false))?;
            progress.events += 1;
            for choice in chunk["choices"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|c| c["index"].as_u64().unwrap_or(0) == 0)
            {
                progress.delivered |=
                    crate::transparency::provider_delta(request, kind, &choice["delta"])
                        .await
                        .map_err(Failure::local)?;
            }
            span.emit("model.delta", &chunk).map_err(Failure::local)?;
            if let Some(sink) = sink {
                let mut visible = chunk;
                if let Some(choices) = visible["choices"].as_array_mut() {
                    for choice in choices {
                        choice["finish_reason"] = Value::Null;
                        crate::transparency::filter_client_delta(request, &mut choice["delta"]);
                    }
                }
                progress.stage = "client_forward";
                let forwarding = Instant::now();
                sink.send(visible).await.map_err(|_| {
                    Failure::new("client_disconnected", "client disconnected", false)
                })?;
                progress.forward_wait_us += forwarding.elapsed().as_micros() as u64;
                // Conservative: even a role/usage-only event prevents automatic replay.
                progress.delivered = true;
            }
        }
        progress.processing_us += processing.elapsed().as_micros() as u64;
        // Capacity pressure is local time, not provider inactivity. The total
        // call deadline still includes it.
        body_deadline = Instant::now() + Duration::from_millis(config.idle_timeout_ms);
    }
    span.emit("upstream.body.eof", &progress.snapshot())
        .map_err(Failure::local)?;
    if let Some(error) = http_failure {
        return Err(error);
    }
    if sse {
        return Err(Failure::new(
            "premature_eof",
            "upstream stream ended before DONE",
            true,
        ));
    }
    progress.stage = "json_decode";
    let value: Value = serde_json::from_slice(&json_body)
        .map_err(|e| Failure::new("invalid_json", e.to_string(), e.is_eof()))?;
    validate_response(&value)?;
    progress.delivered |=
        crate::transparency::provider_delta(request, kind, &value["choices"][0]["message"])
            .await
            .map_err(Failure::local)?;
    span.emit("model.output", &value).map_err(Failure::local)?;
    if let Some(sink) = sink {
        progress.stage = "client_forward";
        let mut message = value["choices"][0]["message"].clone();
        crate::transparency::filter_client_delta(request, &mut message);
        sink.send(json!({"choices":[{"index":0,"delta":message,"finish_reason":null}]}))
            .await
            .map_err(|_| Failure::new("client_disconnected", "client disconnected", false))?;
        progress.delivered = true;
    }
    Ok(value)
}

fn retry_after_ms(value: &str) -> Option<u64> {
    if let Ok(seconds) = value.trim().parse::<u64>() {
        return Some(seconds.saturating_mul(1000));
    }
    httpdate::parse_http_date(value).ok().map(|date| {
        date.duration_since(std::time::SystemTime::now())
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64
    })
}

fn validate_chunk(value: &Value) -> Result<(), Failure> {
    if let Some(error) = value.get("error").filter(|v| !v.is_null()) {
        let code = error["code"]
            .as_str()
            .or_else(|| error["type"].as_str())
            .unwrap_or("unknown");
        return Err(Failure::new(
            "provider_error",
            format!("provider stream error ({code})"),
            matches!(
                code,
                "rate_limit_exceeded" | "server_error" | "overloaded_error"
            ),
        ));
    }
    let choices = value["choices"].as_array().ok_or_else(|| {
        Failure::new("invalid_response", "SSE event missing choices array", false)
    })?;
    for choice in choices {
        if !choice.is_object()
            || (!choice["delta"].is_object() && !choice["finish_reason"].is_string())
        {
            return Err(Failure::new(
                "invalid_response",
                "invalid SSE choice/delta",
                false,
            ));
        }
        if !choice["finish_reason"].is_null() && !choice["finish_reason"].is_string() {
            return Err(Failure::new(
                "invalid_response",
                "invalid SSE finish reason",
                false,
            ));
        }
        let delta = &choice["delta"];
        for field in [
            "content",
            "reasoning",
            "reasoning_content",
            "refusal",
            "role",
        ] {
            if !delta[field].is_null() && !delta[field].is_string() {
                return Err(Failure::new(
                    "invalid_response",
                    format!("invalid SSE {field} delta"),
                    false,
                ));
            }
        }
        if let Some(calls) = delta.get("tool_calls").filter(|v| !v.is_null()) {
            let calls = calls.as_array().ok_or_else(|| {
                Failure::new("invalid_response", "invalid SSE tool_calls delta", false)
            })?;
            for call in calls {
                if call["index"].as_u64().is_none() {
                    return Err(Failure::new(
                        "invalid_response",
                        "tool delta missing index",
                        false,
                    ));
                }
                for pointer in ["/id", "/type", "/function/name", "/function/arguments"] {
                    if call
                        .pointer(pointer)
                        .is_some_and(|v| !v.is_null() && !v.is_string())
                    {
                        return Err(Failure::new(
                            "invalid_response",
                            "invalid tool delta field",
                            false,
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

fn validate_response(value: &Value) -> Result<(), Failure> {
    let invalid = |message| Failure::new("invalid_response", message, false);
    let choice = value
        .pointer("/choices/0")
        .ok_or_else(|| invalid("response missing choice"))?;
    let message = &choice["message"];
    if !message.is_object() || message["role"] != "assistant" {
        return Err(invalid("response missing assistant message"));
    }
    if choice["finish_reason"].as_str().is_none_or(str::is_empty) {
        return Err(invalid("response missing finish reason"));
    }
    if !message["content"].is_null() && !message["content"].is_string() {
        return Err(invalid("response content must be text or null"));
    }
    if let Some(calls) = message.get("tool_calls").filter(|v| !v.is_null()) {
        let calls = calls
            .as_array()
            .ok_or_else(|| invalid("tool_calls must be an array"))?;
        let mut ids = std::collections::HashSet::new();
        for call in calls {
            let id = call["id"]
                .as_str()
                .filter(|v| !v.is_empty())
                .ok_or_else(|| invalid("tool call missing id"))?;
            if !ids.insert(id)
                || call["type"] != "function"
                || call["function"]["name"].as_str().is_none_or(str::is_empty)
            {
                return Err(invalid("invalid or duplicate tool call"));
            }
            let args = call["function"]["arguments"]
                .as_str()
                .ok_or_else(|| invalid("tool arguments must be a JSON string"))?;
            if serde_json::from_str::<Value>(args).is_err() {
                return Err(invalid("incomplete or invalid tool arguments"));
            }
        }
    }
    if message["content"].as_str().is_none_or(str::is_empty)
        && message["tool_calls"].as_array().is_none_or(Vec::is_empty)
        && message["refusal"].as_str().is_none_or(str::is_empty)
    {
        return Err(invalid("response has no answer, tool call, or refusal"));
    }
    Ok(())
}

/// SSE framing across arbitrary UTF-8/line boundaries, including CRLF and CR.
#[derive(Default)]
struct EventParser {
    line: Vec<u8>,
    data: Vec<u8>,
    has_data: bool,
    skip_lf: bool,
}
impl EventParser {
    fn feed(&mut self, bytes: &[u8]) -> Result<Vec<String>, Failure> {
        let mut events = Vec::new();
        for &byte in bytes {
            if self.skip_lf {
                self.skip_lf = false;
                if byte == b'\n' {
                    continue;
                }
            }
            if byte == b'\n' || byte == b'\r' {
                self.skip_lf = byte == b'\r';
                if self.line.is_empty() {
                    if self.has_data {
                        self.data.pop(); // final newline between data fields
                        let data = std::mem::take(&mut self.data);
                        events.push(
                            String::from_utf8(data)
                                .map_err(|e| Failure::new("invalid_utf8", e.to_string(), false))?,
                        );
                        self.has_data = false;
                    }
                } else if self.line == b"data" || self.line.starts_with(b"data:") {
                    let data = self.line.get(5..).unwrap_or_default();
                    self.data
                        .extend_from_slice(data.strip_prefix(b" ").unwrap_or(data));
                    self.data.push(b'\n');
                    self.has_data = true;
                }
                self.line.clear();
            } else {
                self.line.push(byte);
            }
            if self.line.len() + self.data.len() > MAX_EVENT {
                return Err(Failure::new(
                    "event_too_large",
                    "SSE event exceeds 1 MiB",
                    false,
                ));
            }
        }
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sse_frames_survive_every_byte_boundary_and_multiline_data() {
        let mut parser = EventParser::default();
        let mut events = Vec::new();
        for byte in ": heartbeat\r\n\r\ndata: {\r\ndata: \"text\":\"café\"}\r\n\r\ndata: [DONE]\r\r"
            .as_bytes()
        {
            events.extend(parser.feed(&[*byte]).unwrap());
        }
        assert_eq!(events, vec!["{\n\"text\":\"café\"}", "[DONE]"]);
    }

    #[test]
    fn malformed_utf8_and_oversized_unterminated_events_are_bounded() {
        assert_eq!(
            EventParser::default()
                .feed(b"data: \xff\n\n")
                .unwrap_err()
                .category,
            "invalid_utf8"
        );
        assert_eq!(
            EventParser::default()
                .feed(&vec![b'x'; MAX_EVENT + 1])
                .unwrap_err()
                .category,
            "event_too_large"
        );
    }

    #[test]
    fn retry_after_accepts_seconds_and_http_dates() {
        assert_eq!(retry_after_ms("12"), Some(12_000));
        assert_eq!(retry_after_ms("invalid"), None);
        let date = httpdate::fmt_http_date(std::time::SystemTime::now() + Duration::from_secs(120));
        assert!((118_000..=120_000).contains(&retry_after_ms(&date).unwrap()));
        assert_eq!(retry_after_ms("Thu, 01 Jan 1970 00:00:00 GMT"), Some(0));
    }
}
