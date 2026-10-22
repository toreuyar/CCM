use crate::config::{McpServerConfig, McpTransportConfig, ToolingConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use std::{process::Stdio, time::Duration};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};

const PROTOCOL_VERSION: &str = "2025-03-26";

pub(super) fn function_name(server: &str, tool: &str) -> String {
    format!("cm{}_{}_{}", server.len(), server, tool)
}

pub(super) async fn call(
    config: &ToolingConfig,
    server: &McpServerConfig,
    tool_name: &str,
    arguments: &Value,
) -> Result<Value, String> {
    let allowlisted = server
        .tools
        .get(tool_name)
        .ok_or("MCP tool is not allowlisted")?;
    super::validate_arguments(&allowlisted.input_schema, arguments)?;
    match &server.transport {
        McpTransportConfig::Stdio {
            command,
            args,
            env,
            sandbox,
        } => {
            let sandbox_mode = super::resolve_sandbox_mode(config, sandbox.enabled)?;
            let scratch = super::sandbox::SandboxTempDir::new()?;
            let mut target_env = std::collections::BTreeMap::new();
            target_env.insert("PATH".into(), "/usr/bin:/bin".into());
            target_env.insert(
                "TMPDIR".into(),
                scratch.path().to_string_lossy().into_owned(),
            );
            target_env.insert("LC_ALL".into(), "C".into());
            for (name, source) in env {
                super::sandbox::validate_target_env_name(name)?;
                let value = std::env::var(source).map_err(|_| {
                    format!("MCP provider environment variable {source} is unavailable")
                })?;
                target_env.insert(name.clone(), value);
            }
            let process = super::sandbox::SandboxCommand::new(
                command,
                args,
                scratch.path(),
                &[scratch.path().to_path_buf()],
                config.sandbox_limits,
                target_env,
                sandbox_mode,
                Stdio::piped(),
                Stdio::null(),
            )?;
            call_stdio(
                process,
                tool_name,
                arguments,
                config.request_timeout_ms,
                config.max_input_bytes,
                config.max_result_bytes,
            )
            .await
        }
        McpTransportConfig::StreamableHttp {
            url,
            bearer_token_env,
        } => {
            call_http(
                url,
                bearer_token_env.as_deref(),
                tool_name,
                arguments,
                config.request_timeout_ms,
                config.max_result_bytes,
            )
            .await
        }
    }
}

fn rpc_request(id: u64, method: &str, params: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
}

fn initialize_request() -> Value {
    rpc_request(
        1,
        "initialize",
        json!({
            "protocolVersion":PROTOCOL_VERSION,
            "capabilities":{},
            "clientInfo":{"name":"CCM","version":env!("CARGO_PKG_VERSION")}
        }),
    )
}

fn call_request(id: u64, tool: &str, arguments: &Value) -> Value {
    rpc_request(id, "tools/call", json!({"name":tool,"arguments":arguments}))
}

fn validate_rpc_response(response: &Value, id: u64) -> Result<Value, String> {
    if response["jsonrpc"] != "2.0" || response["id"].as_u64() != Some(id) {
        return Err("MCP provider returned an invalid or mismatched JSON-RPC response".into());
    }
    if let Some(error) = response.get("error") {
        return Err(format!(
            "MCP provider returned an error: {}",
            bounded_text(error, 1024)
        ));
    }
    let result = response
        .get("result")
        .cloned()
        .ok_or("MCP JSON-RPC response has no result")?;
    if result.get("isError").and_then(Value::as_bool) == Some(true) {
        return Err(format!(
            "MCP tool returned an error: {}",
            bounded_text(&result, 1024)
        ));
    }
    Ok(result)
}

fn bounded_text(value: &Value, max_bytes: usize) -> String {
    let text = value.to_string();
    if text.len() <= max_bytes {
        text
    } else {
        let boundary = text
            .char_indices()
            .map(|(index, _)| index)
            .take_while(|index| *index <= max_bytes)
            .last()
            .unwrap_or(0);
        format!("{}…", &text[..boundary])
    }
}

fn bounded_result(value: Value, max_bytes: usize) -> Result<Value, String> {
    let encoded = serde_json::to_vec(&value).map_err(|error| error.to_string())?;
    if encoded.len() > max_bytes {
        return Err("MCP tool result exceeds tools.max_result_bytes".into());
    }
    Ok(value)
}

async fn read_line_bounded<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max_bytes: usize,
) -> Result<Vec<u8>, String> {
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf().await.map_err(|error| error.to_string())?;
        if available.is_empty() {
            return Err("MCP stdio provider closed before returning a response".into());
        }
        let length = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if line.len().saturating_add(length) > max_bytes {
            return Err("MCP stdio response exceeds tools.max_result_bytes".into());
        }
        let finished = available.get(length.saturating_sub(1)) == Some(&b'\n');
        line.extend_from_slice(&available[..length]);
        reader.consume(length);
        if finished {
            return Ok(line);
        }
    }
}

fn dispatch_notification(message: &Value) {
    if let Some(method) = message.get("method").and_then(Value::as_str) {
        // Notification payloads can contain provider-authored content. Keep them
        // separate from JSON-RPC responses without copying that content into logs.
        tracing::trace!(method, "MCP server notification received");
    }
}

fn response_for_id(message: Value, id: u64) -> Result<Option<Value>, String> {
    if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err("MCP provider sent a message with an invalid JSON-RPC version".into());
    }
    if message.get("id").is_none_or(Value::is_null) {
        if message.get("method").and_then(Value::as_str).is_some() {
            dispatch_notification(&message);
            return Ok(None);
        }
        return Err("MCP provider sent a message without an id or method".into());
    }
    if message.get("method").is_some() {
        return Err("MCP provider sent an unsupported JSON-RPC request".into());
    }
    if message.get("id").and_then(Value::as_u64) == Some(id) {
        Ok(Some(message))
    } else {
        tracing::trace!(
            expected_id = id,
            "ignoring MCP response for another request id"
        );
        Ok(None)
    }
}

async fn read_stdio_response<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    expected_id: u64,
    max_bytes: usize,
) -> Result<Value, String> {
    let mut total_bytes = 0usize;
    loop {
        let remaining = max_bytes.saturating_sub(total_bytes);
        if remaining == 0 {
            return Err("MCP stdio messages exceed tools.max_result_bytes".into());
        }
        let line = read_line_bounded(reader, remaining).await?;
        total_bytes = total_bytes.saturating_add(line.len());
        let mut line = line.as_slice();
        if line.ends_with(b"\n") {
            line = &line[..line.len() - 1];
        }
        if line.ends_with(b"\r") {
            line = &line[..line.len() - 1];
        }
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let message: Value = serde_json::from_slice(line)
            .map_err(|error| format!("MCP stdio message is malformed JSON: {error}"))?;
        if let Some(response) = response_for_id(message, expected_id)? {
            return Ok(response);
        }
    }
}

async fn send_stdio<W: AsyncWrite + Unpin, R: AsyncBufRead + Unpin>(
    stdin: &mut W,
    reader: &mut R,
    request: &Value,
    id: u64,
    max_input_bytes: usize,
    max_bytes: usize,
) -> Result<Value, String> {
    let mut serialized = serde_json::to_vec(request).map_err(|error| error.to_string())?;
    if serialized.len() > max_input_bytes {
        return Err("MCP stdio request exceeds tools.max_input_bytes".into());
    }
    serialized.push(b'\n');
    stdin
        .write_all(&serialized)
        .await
        .map_err(|error| error.to_string())?;
    stdin.flush().await.map_err(|error| error.to_string())?;
    let response = read_stdio_response(reader, id, max_bytes).await?;
    validate_rpc_response(&response, id)
}

async fn stdio_protocol<W: AsyncWrite + Unpin, R: AsyncBufRead + Unpin>(
    stdin: &mut W,
    reader: &mut R,
    tool: &str,
    arguments: &Value,
    max_input_bytes: usize,
    max_result_bytes: usize,
) -> Result<Value, String> {
    let init = send_stdio(
        stdin,
        reader,
        &initialize_request(),
        1,
        max_input_bytes,
        max_result_bytes,
    )
    .await?;
    if init["protocolVersion"].as_str() != Some(PROTOCOL_VERSION) {
        return Err("MCP provider negotiated an unsupported protocol version".into());
    }
    let notification = json!({
        "jsonrpc":"2.0",
        "method":"notifications/initialized"
    });
    let mut notification_bytes = serde_json::to_vec(&notification).map_err(|e| e.to_string())?;
    if notification_bytes.len() > max_input_bytes {
        return Err("MCP stdio notification exceeds tools.max_input_bytes".into());
    }
    notification_bytes.push(b'\n');
    stdin
        .write_all(&notification_bytes)
        .await
        .map_err(|e| e.to_string())?;
    stdin.flush().await.map_err(|e| e.to_string())?;
    let result = send_stdio(
        stdin,
        reader,
        &call_request(2, tool, arguments),
        2,
        max_input_bytes,
        max_result_bytes,
    )
    .await?;
    bounded_result(result, max_result_bytes)
}

async fn call_stdio(
    process: super::sandbox::SandboxCommand,
    tool: &str,
    arguments: &Value,
    timeout_ms: u64,
    max_input_bytes: usize,
    max_result_bytes: usize,
) -> Result<Value, String> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    let mut child = tokio::time::timeout_at(deadline, process.spawn(true))
        .await
        .map_err(|_| "MCP stdio provider timed out".to_string())??;
    let mut stdin = child.take_stdin().ok_or("MCP stdin unavailable")?;
    let stdout = child.take_stdout().ok_or("MCP stdout unavailable")?;
    let mut reader = BufReader::new(stdout);
    let result = match tokio::time::timeout_at(
        deadline,
        stdio_protocol(
            &mut stdin,
            &mut reader,
            tool,
            arguments,
            max_input_bytes,
            max_result_bytes,
        ),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err("MCP stdio provider timed out".into()),
    };
    drop(stdin);
    drop(reader);
    child.kill_and_wait().await;
    result
}

async fn http_post(
    client: &reqwest::Client,
    url: &str,
    token: Option<&str>,
    session: &mut Option<String>,
    body: &Value,
    expected_id: Option<u64>,
    max_bytes: usize,
) -> Result<Option<Value>, String> {
    let mut request = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header(
            reqwest::header::ACCEPT,
            "application/json, text/event-stream",
        )
        .header("MCP-Protocol-Version", PROTOCOL_VERSION)
        .json(body);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    if let Some(session_id) = session.as_deref() {
        request = request.header("Mcp-Session-Id", session_id);
    }
    let response = request.send().await.map_err(|error| error.to_string())?;
    if let Some(session_id) = response
        .headers()
        .get("Mcp-Session-Id")
        .and_then(|value| value.to_str().ok())
    {
        *session = Some(session_id.to_owned());
    }
    if response.status().is_redirection() {
        return Err("MCP Streamable HTTP redirects are not followed".into());
    }
    if !response.status().is_success() && response.status().as_u16() != 202 {
        return Err(format!(
            "MCP Streamable HTTP returned status {}",
            response.status()
        ));
    }
    read_http_body(response, expected_id, max_bytes).await
}

async fn read_http_body(
    response: reqwest::Response,
    expected_id: Option<u64>,
    max_bytes: usize,
) -> Result<Option<Value>, String> {
    if response
        .content_length()
        .is_some_and(|length| length > max_bytes as u64)
    {
        return Err("MCP Streamable HTTP body exceeds tools.max_result_bytes".into());
    }
    let is_sse = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|content_type| {
            content_type.split(';').next().is_some_and(|media_type| {
                media_type.trim().eq_ignore_ascii_case("text/event-stream")
            })
        });
    let mut stream = response.bytes_stream();
    if is_sse {
        let mut parser = SseParser::new(max_bytes);
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| error.to_string())?;
            if let Some(response) = parser.push(&chunk, expected_id)? {
                // Dropping the response stream here stops provider output as soon
                // as the matching JSON-RPC response has arrived.
                return Ok(Some(response));
            }
        }
        return parser.finish(expected_id);
    }

    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| error.to_string())?;
        if bytes.len().saturating_add(chunk.len()) > max_bytes {
            return Err("MCP Streamable HTTP body exceeds tools.max_result_bytes".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.is_empty() {
        return Ok(None);
    }
    let message: Value = serde_json::from_slice(&bytes)
        .map_err(|error| format!("MCP HTTP body is malformed JSON: {error}"))?;
    response_for_optional_id(message, expected_id)
}

fn response_for_optional_id(
    message: Value,
    expected_id: Option<u64>,
) -> Result<Option<Value>, String> {
    if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err("MCP provider sent a message with an invalid JSON-RPC version".into());
    }
    if message.get("id").is_none_or(Value::is_null) {
        if message.get("method").and_then(Value::as_str).is_some() {
            dispatch_notification(&message);
            return Ok(None);
        }
        return Err("MCP provider sent a message without an id or method".into());
    }
    if message.get("method").is_some() {
        return Err("MCP provider sent an unsupported JSON-RPC request".into());
    }
    if expected_id.is_some_and(|id| message.get("id").and_then(Value::as_u64) == Some(id)) {
        Ok(Some(message))
    } else {
        if let Some(id) = expected_id {
            tracing::trace!(
                expected_id = id,
                "ignoring MCP response for another request id"
            );
        }
        Ok(None)
    }
}

struct SseParser {
    pending: Vec<u8>,
    scan_from: usize,
    data_lines: Vec<Vec<u8>>,
    event_data_bytes: usize,
    total_bytes: usize,
    max_bytes: usize,
    completed: bool,
}

/// Returns the start and byte length of an SSE line terminator. A CR at the
/// end of a non-final chunk remains pending until the next byte disambiguates
/// bare CR from CRLF.
fn sse_line_ending(bytes: &[u8], from: usize, eof: bool) -> Option<(usize, usize)> {
    let mut index = from;
    while index < bytes.len() {
        match bytes[index] {
            b'\n' => return Some((index, 1)),
            b'\r' => match bytes.get(index + 1) {
                Some(b'\n') => return Some((index, 2)),
                Some(_) => return Some((index, 1)),
                None if eof => return Some((index, 1)),
                None => return None,
            },
            _ => index += 1,
        }
    }
    None
}

impl SseParser {
    fn new(max_bytes: usize) -> Self {
        Self {
            pending: Vec::new(),
            scan_from: 0,
            data_lines: Vec::new(),
            event_data_bytes: 0,
            total_bytes: 0,
            max_bytes,
            completed: false,
        }
    }

    fn push(&mut self, chunk: &[u8], expected_id: Option<u64>) -> Result<Option<Value>, String> {
        if self.completed {
            return Ok(None);
        }
        self.total_bytes = self.total_bytes.saturating_add(chunk.len());
        if self.total_bytes > self.max_bytes {
            return Err("MCP Streamable HTTP body exceeds tools.max_result_bytes".into());
        }
        self.pending.extend_from_slice(chunk);
        let mut consumed = 0usize;
        loop {
            let Some((end, terminator_len)) = sse_line_ending(&self.pending, self.scan_from, false)
            else {
                self.scan_from = if self.pending.last() == Some(&b'\r') {
                    self.pending.len() - 1
                } else {
                    self.pending.len()
                };
                break;
            };
            // `pending` can contain many complete lines in one network chunk.
            // `consumed` marks the start of this line; using `[..end]` would
            // prepend all earlier fields and corrupt later SSE events.
            let mut line = self.pending[consumed..end].to_vec();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            let response = self.process_line(&line, expected_id)?;
            consumed = end + terminator_len;
            self.scan_from = consumed;
            if response.is_some() {
                // Leave the parser in a coherent state even when the caller
                // chooses to finish it after receiving the matching response.
                self.pending.drain(..consumed);
                self.scan_from = 0;
                self.completed = true;
                return Ok(response);
            }
        }
        if consumed > 0 {
            self.pending.drain(..consumed);
            self.scan_from = 0;
        }
        if self.pending.len() > self.max_bytes {
            return Err("MCP SSE line exceeds tools.max_result_bytes".into());
        }
        Ok(None)
    }

    fn process_line(
        &mut self,
        line: &[u8],
        expected_id: Option<u64>,
    ) -> Result<Option<Value>, String> {
        if line.is_empty() {
            return self.finish_event(expected_id);
        }
        if line.starts_with(b":") {
            return Ok(None);
        }
        if let Some(data) = line.strip_prefix(b"data:") {
            let data = data.strip_prefix(b" ").unwrap_or(data);
            if self.data_lines.len() >= 4096 {
                return Err("MCP SSE event has too many data lines".into());
            }
            self.event_data_bytes = self
                .event_data_bytes
                .saturating_add(data.len().saturating_add(1));
            if self.event_data_bytes > self.max_bytes {
                return Err("MCP SSE event exceeds tools.max_result_bytes".into());
            }
            self.data_lines.push(data.to_vec());
        }
        Ok(None)
    }

    fn finish_event(&mut self, expected_id: Option<u64>) -> Result<Option<Value>, String> {
        if self.data_lines.is_empty() {
            return Ok(None);
        }
        self.event_data_bytes = 0;
        let mut data = Vec::new();
        for (index, line) in self.data_lines.drain(..).enumerate() {
            if index > 0 {
                data.push(b'\n');
            }
            data.extend_from_slice(&line);
        }
        if data == b"[DONE]" {
            return Ok(None);
        }
        let text = String::from_utf8(data).map_err(|_| "MCP SSE data is not UTF-8")?;
        let message: Value = serde_json::from_str(&text)
            .map_err(|error| format!("MCP SSE event is malformed JSON: {error}"))?;
        response_for_optional_id(message, expected_id)
    }

    fn finish(&mut self, expected_id: Option<u64>) -> Result<Option<Value>, String> {
        if self.completed {
            return Ok(None);
        }
        let mut consumed = 0usize;
        loop {
            let Some((end, terminator_len)) = sse_line_ending(&self.pending, self.scan_from, true)
            else {
                break;
            };
            let mut line = self.pending[consumed..end].to_vec();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if let Some(response) = self.process_line(&line, expected_id)? {
                consumed = end + terminator_len;
                self.pending.drain(..consumed);
                self.scan_from = 0;
                self.completed = true;
                return Ok(Some(response));
            }
            consumed = end + terminator_len;
            self.scan_from = consumed;
        }
        if consumed > 0 {
            self.pending.drain(..consumed);
            self.scan_from = 0;
        }
        if !self.pending.is_empty() {
            let line = std::mem::take(&mut self.pending);
            if let Some(response) = self.process_line(&line, expected_id)? {
                self.completed = true;
                return Ok(Some(response));
            }
        }
        let response = self.finish_event(expected_id)?;
        if response.is_some() {
            self.completed = true;
        }
        Ok(response)
    }
}

fn parse_http_json(body: &str, expected_id: Option<u64>) -> Result<Option<Value>, String> {
    if let Ok(message) = serde_json::from_str(body) {
        return response_for_optional_id(message, expected_id);
    }
    let mut parser = SseParser::new(body.len());
    let response = parser.push(body.as_bytes(), expected_id)?;
    response.map_or_else(|| parser.finish(expected_id), |response| Ok(Some(response)))
}

async fn call_http(
    url: &str,
    token_env: Option<&str>,
    tool: &str,
    arguments: &Value,
    timeout_ms: u64,
    max_result_bytes: usize,
) -> Result<Value, String> {
    let token = token_env
        .map(std::env::var)
        .transpose()
        .map_err(|_| "configured MCP bearer token environment variable is unavailable")?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| error.to_string())?;
    let total_deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    let cleanup_reserve = Duration::from_millis(
        (timeout_ms / 10)
            .clamp(1, 250)
            .min(timeout_ms.saturating_sub(1)),
    );
    let protocol_deadline = total_deadline - cleanup_reserve;
    let mut session = None;
    let protocol = tokio::time::timeout_at(protocol_deadline, async {
        let initialize = http_post(
            &client,
            url,
            token.as_deref(),
            &mut session,
            &initialize_request(),
            Some(1),
            max_result_bytes,
        )
        .await?
        .ok_or("MCP initialize returned no matching JSON-RPC response")?;
        let initialized = validate_rpc_response(&initialize, 1)?;
        if initialized["protocolVersion"].as_str() != Some(PROTOCOL_VERSION) {
            return Err("MCP provider negotiated an unsupported protocol version".into());
        }
        let notification = json!({"jsonrpc":"2.0","method":"notifications/initialized"});
        let _ = http_post(
            &client,
            url,
            token.as_deref(),
            &mut session,
            &notification,
            None,
            max_result_bytes,
        )
        .await?;
        let response = http_post(
            &client,
            url,
            token.as_deref(),
            &mut session,
            &call_request(2, tool, arguments),
            Some(2),
            max_result_bytes,
        )
        .await?
        .ok_or("MCP tools/call returned no matching JSON-RPC response")?;
        bounded_result(validate_rpc_response(&response, 2)?, max_result_bytes)
    })
    .await;
    let mut result = match protocol {
        Ok(result) => result,
        Err(_) => Err("MCP Streamable HTTP provider timed out".into()),
    };

    if let Some(session_id) = session {
        let mut request = client
            .delete(url)
            .header("Mcp-Session-Id", session_id)
            .header("MCP-Protocol-Version", PROTOCOL_VERSION);
        if let Some(token) = token.as_deref() {
            request = request.bearer_auth(token);
        }
        let cleanup = tokio::time::timeout_at(total_deadline, request.send()).await;
        let cleanup_error = match cleanup {
            Ok(Ok(response))
                if response.status().is_success()
                    || response.status() == reqwest::StatusCode::NOT_FOUND =>
            {
                None
            }
            Ok(Ok(response)) => Some(format!(
                "MCP session cleanup returned status {}",
                response.status()
            )),
            Ok(Err(error)) => Some(format!("MCP session cleanup failed: {error}")),
            Err(_) => Some("MCP session cleanup exceeded the total deadline".into()),
        };
        if let Some(error) = cleanup_error {
            result = match result {
                Ok(_) => Err(error),
                Err(primary) => Err(format!("{primary}; {error}")),
            };
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        body::Body,
        extract::State,
        http::{StatusCode, header},
        response::{IntoResponse, Response},
        routing::post,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    async fn mock_handler(Json(request): Json<Value>) -> Response<Body> {
        if request.get("id").is_none_or(Value::is_null) {
            return Response::builder()
                .status(StatusCode::ACCEPTED)
                .header("Mcp-Session-Id", "local-session")
                .body(Body::empty())
                .unwrap();
        }
        let id = request["id"].clone();
        let result = match request["method"].as_str() {
            Some("initialize") => json!({
                "protocolVersion":PROTOCOL_VERSION,
                "capabilities":{"tools":{}},
                "serverInfo":{"name":"local-mock","version":"1"}
            }),
            Some("tools/call") => json!({
                "content":[{"type":"text","text":request["params"]["arguments"]["message"]}],
                "isError":false
            }),
            _ => json!({}),
        };
        Response::builder()
            .status(StatusCode::OK)
            .header("Mcp-Session-Id", "local-session")
            .body(Body::from(
                json!({"jsonrpc":"2.0","id":id,"result":result}).to_string(),
            ))
            .unwrap()
    }

    async fn mock_delete() -> StatusCode {
        StatusCode::NO_CONTENT
    }

    #[tokio::test]
    async fn streamable_http_transport_uses_local_mock_and_bounds_result() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route("/mcp", post(mock_handler).delete(mock_delete));
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let schema = json!({
            "type":"object",
            "properties":{"message":{"type":"string","maxLength":64}},
            "required":["message"],
            "additionalProperties":false
        });
        let mcp = McpServerConfig {
            profile: crate::config::McpProviderProfile {
                reviewed: true,
                reach: crate::config::McpReach::Local,
                impact: crate::config::McpImpact::Low,
            },
            transport: McpTransportConfig::StreamableHttp {
                url: format!("http://{address}/mcp"),
                bearer_token_env: None,
            },
            tools: std::collections::BTreeMap::from([(
                "echo".into(),
                crate::config::McpToolConfig {
                    description: "echo".into(),
                    input_schema: schema,
                },
            )]),
        };
        let result = call_http(
            match &mcp.transport {
                McpTransportConfig::StreamableHttp { url, .. } => url,
                _ => unreachable!(),
            },
            None,
            "echo",
            &json!({"message":"local"}),
            2000,
            2048,
        )
        .await
        .unwrap();
        assert_eq!(result["content"][0]["text"], "local");
        assert!(
            call_http(
                match &mcp.transport {
                    McpTransportConfig::StreamableHttp { url, .. } => url,
                    _ => unreachable!(),
                },
                None,
                "echo",
                &json!({"message":"local"}),
                2000,
                1,
            )
            .await
            .is_err()
        );
        server.abort();
    }

    #[tokio::test]
    async fn stdio_transport_uses_json_rpc_and_rejects_malformed_responses() {
        let response = test_stdio_exchange(false).await.unwrap();
        assert_eq!(response["content"][0]["text"], "stdio-local");
        assert!(test_stdio_exchange(true).await.is_err());
    }

    #[cfg(any(
        all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64")
        ),
        target_os = "macos"
    ))]
    #[tokio::test]
    async fn mcp_stdio_disabled_policy_runs_unconfined() {
        let root =
            std::env::temp_dir().join(format!("ccm-mcp-unconfined-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let outside = root.join("outside.txt");
        std::fs::write(&outside, "stdio-unconfined").unwrap();
        let script = format!(
            "read -r init; printf '%s\\n' '{{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{{\"protocolVersion\":\"2025-03-26\",\"capabilities\":{{}},\"serverInfo\":{{\"name\":\"test\",\"version\":\"1\"}}}}}}'; read -r initialized; read -r request; value=$(cat '{}'); printf '{{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{{\"content\":[{{\"type\":\"text\",\"text\":\"%s\"}}],\"isError\":false}}}}\\n' \"$value\"",
            outside.display()
        );
        let schema = json!({
            "type":"object",
            "properties":{},
            "required":[],
            "additionalProperties":false
        });
        let server = McpServerConfig {
            profile: crate::config::McpProviderProfile {
                reviewed: true,
                reach: crate::config::McpReach::Local,
                impact: crate::config::McpImpact::Low,
            },
            transport: McpTransportConfig::Stdio {
                command: "/bin/sh".into(),
                args: vec!["-c".into(), script],
                env: Default::default(),
                sandbox: crate::config::McpSandboxConfig {
                    enabled: if cfg!(target_os = "linux") {
                        Some(false)
                    } else {
                        None
                    },
                    executable: String::new(),
                },
            },
            tools: std::collections::BTreeMap::from([(
                "read".into(),
                crate::config::McpToolConfig {
                    description: "read outside scratch in explicit unconfined mode".into(),
                    input_schema: schema,
                },
            )]),
        };
        let config = ToolingConfig {
            sandbox_enabled: if cfg!(target_os = "linux") {
                Some(true)
            } else {
                None
            },
            ..Default::default()
        };
        let result = call(&config, &server, "read", &json!({})).await.unwrap();
        assert_eq!(result["content"][0]["text"], "stdio-unconfined");
        std::fs::remove_dir_all(root).unwrap();
    }

    async fn test_stdio_exchange(malformed: bool) -> Result<Value, String> {
        let (client, server) = tokio::io::duplex(4096);
        let (client_read, mut client_write) = tokio::io::split(client);
        let (server_read, mut server_write) = tokio::io::split(server);
        let mut client_reader = BufReader::new(client_read);
        let server_task = tokio::spawn(async move {
            let mut server_reader = BufReader::new(server_read);
            loop {
                let mut line = String::new();
                if server_reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                    break;
                }
                let request: Value = serde_json::from_str(line.trim()).unwrap();
                if request["id"].is_null() {
                    continue;
                }
                if malformed && request["method"] == "tools/call" {
                    server_write.write_all(b"not-json\n").await.unwrap();
                    break;
                }
                let result = match request["method"].as_str() {
                    Some("initialize") => json!({
                        "protocolVersion":PROTOCOL_VERSION,
                        "capabilities":{"tools":{}},
                        "serverInfo":{"name":"stdio-test","version":"1"}
                    }),
                    Some("tools/call") => json!({
                        "content":[{"type":"text","text":request["params"]["arguments"]["message"]}],
                        "isError":false
                    }),
                    _ => json!({}),
                };
                let response = json!({"jsonrpc":"2.0","id":request["id"],"result":result});
                let notification = json!({
                    "jsonrpc":"2.0",
                    "method":"notifications/message",
                    "params":{"level":"info","data":"working"}
                });
                let unrelated = json!({
                    "jsonrpc":"2.0",
                    "id":request["id"].as_u64().unwrap() + 100,
                    "result":{"stale":true}
                });
                server_write
                    .write_all(format!("{notification}\n{unrelated}\n{response}\n").as_bytes())
                    .await
                    .unwrap();
            }
        });
        let result = stdio_protocol(
            &mut client_write,
            &mut client_reader,
            "echo",
            &json!({"message":"stdio-local"}),
            2048,
            2048,
        )
        .await;
        server_task.abort();
        result
    }

    #[test]
    fn sse_parser_skips_notifications_matches_ids_and_joins_multiline_data() {
        let body = concat!(
            ": keepalive\n\n",
            "event: message\n",
            "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/message\",\"params\":{\"level\":\"info\"}}\n\n",
            "event: message\n",
            "data: {\"jsonrpc\":\"2.0\",\n",
            "data: \"id\":99,\"result\":{\"stale\":true}}\n\n",
            "event: message\n",
            "data: {\"jsonrpc\":\"2.0\",\n",
            "data: \"id\":2,\"result\":{\"content\":[]}}\n\n",
        );
        let response = parse_http_json(body, Some(2)).unwrap().unwrap();
        assert!(validate_rpc_response(&response, 2).is_ok());
        assert_eq!(response["id"], 2);

        let mut parser = SseParser::new(body.len());
        let mut response = None;
        for chunk in body.as_bytes().chunks(7) {
            if let Some(matched) = parser.push(chunk, Some(2)).unwrap() {
                response = Some(matched);
                break;
            }
        }
        let response = response.or(parser.finish(Some(2)).unwrap()).unwrap();
        assert_eq!(
            response["id"], 2,
            "SSE frame may span arbitrary network chunks"
        );
    }

    #[test]
    fn sse_parser_accepts_cr_crlf_lf_and_ignores_bytes_after_the_matching_response() {
        let body = concat!(
            ": keepalive\r\n",
            "\r",
            "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/message\"}\r",
            "\r\n",
            "data: {\"jsonrpc\":\"2.0\",\"id\":99,\"result\":{}}\n\n",
            "data: {\"jsonrpc\":\"2.0\",\r\n",
            "data: \"id\":2,\"result\":{\"content\":[]}}\r",
            "\r",
            "data: trailing-not-json\r\r",
        );

        let response = parse_http_json(body, Some(2)).unwrap().unwrap();
        assert_eq!(response["id"], 2);

        let mut parser = SseParser::new(body.len());
        let mut response = None;
        for chunk in body.as_bytes().chunks(1) {
            if let Some(matched) = parser.push(chunk, Some(2)).unwrap() {
                response = Some(matched);
                break;
            }
        }
        let response = response.unwrap();
        assert_eq!(response["id"], 2);
        assert!(parser.finish(Some(2)).unwrap().is_none());
        assert!(
            parser
                .push(b"data: trailing-not-json\r\r", Some(2))
                .unwrap()
                .is_none()
        );
    }

    #[derive(Default)]
    struct SseState {
        deletes: AtomicUsize,
    }

    async fn sse_handler(
        State(_state): State<Arc<SseState>>,
        Json(request): Json<Value>,
    ) -> Response<Body> {
        let (status, body) = match request["method"].as_str() {
            Some("initialize") => (
                StatusCode::OK,
                concat!(
                    "event: message\n",
                    "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/message\"}\n\n",
                    "event: message\n",
                    "data: {\"jsonrpc\":\"2.0\",\n",
                    "data: \"id\":1,\"result\":{\"protocolVersion\":\"2025-03-26\",\"capabilities\":{},\"serverInfo\":{\"name\":\"sse\",\"version\":\"1\"}}}\n\n"
                )
                .to_owned(),
            ),
            Some("tools/call") => (
                StatusCode::OK,
                concat!(
                    "event: message\n",
                    "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progress\":1}}\n\n",
                    "event: message\n",
                    "data: {\"jsonrpc\":\"2.0\",\n",
                    "data: \"id\":44,\"result\":{\"stale\":true}}\n\n",
                    "event: message\n",
                    "data: {\"jsonrpc\":\"2.0\",\n",
                    "data: \"id\":2,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"sse-local\"}]}}\n\n"
                )
                .to_owned(),
            ),
            _ => {
                return Response::builder()
                    .status(StatusCode::ACCEPTED)
                    .header("Mcp-Session-Id", "sse-session")
                    .body(Body::empty())
                    .unwrap();
            }
        };
        if request["method"] == "tools/call" {
            let stream = async_stream::stream! {
                yield Ok::<_, std::io::Error>(bytes::Bytes::from(body));
                std::future::pending::<()>().await;
            };
            return Response::builder()
                .status(status)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .header("Mcp-Session-Id", "sse-session")
                .body(Body::from_stream(stream))
                .unwrap();
        }
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .header("Mcp-Session-Id", "sse-session")
            .body(Body::from(body))
            .unwrap()
    }

    async fn sse_delete(State(state): State<Arc<SseState>>) -> StatusCode {
        state.deletes.fetch_add(1, Ordering::SeqCst);
        StatusCode::NO_CONTENT
    }

    #[tokio::test]
    async fn streamable_http_sse_handles_notifications_multiline_and_deletes_session() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(SseState::default());
        let app = Router::new()
            .route("/mcp", post(sse_handler).delete(sse_delete))
            .with_state(state.clone());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let result = call_http(
            &format!("http://{address}/mcp"),
            None,
            "echo",
            &json!({"message":"local"}),
            2000,
            2048,
        )
        .await
        .unwrap();
        assert_eq!(result["content"][0]["text"], "sse-local");
        assert_eq!(state.deletes.load(Ordering::SeqCst), 1);
        server.abort();
    }

    async fn delayed_handler(
        State(_state): State<Arc<SseState>>,
        Json(request): Json<Value>,
    ) -> Response<Body> {
        tokio::time::sleep(Duration::from_millis(80)).await;
        let id = request["id"].clone();
        if request["method"] == "notifications/initialized" {
            return (
                StatusCode::ACCEPTED,
                [("Mcp-Session-Id", "delayed-session")],
                Body::empty(),
            )
                .into_response();
        }
        let result = if request["method"] == "initialize" {
            json!({"protocolVersion":PROTOCOL_VERSION,"capabilities":{},"serverInfo":{"name":"delayed","version":"1"}})
        } else {
            json!({"content":[]})
        };
        (
            StatusCode::OK,
            [("Mcp-Session-Id", "delayed-session")],
            Json(json!({"jsonrpc":"2.0","id":id,"result":result})),
        )
            .into_response()
    }

    #[tokio::test]
    async fn streamable_http_deadline_is_total_and_timeout_still_cleans_session() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(SseState::default());
        let app = Router::new()
            .route("/mcp", post(delayed_handler).delete(sse_delete))
            .with_state(state.clone());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let started = tokio::time::Instant::now();
        let result = call_http(
            &format!("http://{address}/mcp"),
            None,
            "echo",
            &json!({}),
            200,
            2048,
        )
        .await;
        let elapsed = started.elapsed();
        assert!(result.unwrap_err().contains("timed out"));
        assert!(
            elapsed < Duration::from_millis(350),
            "total deadline reset: {elapsed:?}"
        );
        assert_eq!(state.deletes.load(Ordering::SeqCst), 1);
        server.abort();
    }
}
