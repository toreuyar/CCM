//! Instrumented, steerable provider calls. Internal requests stream into the
//! durable trace while retaining the complete OpenAI message for the machinery.
use crate::{observe::Span, state::AppState};
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
#[path = "upstream_transport.rs"]
mod transport;
use transport::{Failure, read_call};

pub async fn call(
    state: &Arc<AppState>,
    request: &str,
    kind: &str,
    entity: &str,
    body: Value,
) -> Result<Value, String> {
    call_with_sink(state, request, kind, entity, body, None).await
}

#[allow(clippy::too_many_arguments)]
pub async fn call_with_sink(
    state: &Arc<AppState>,
    request: &str,
    kind: &str,
    entity: &str,
    mut body: Value,
    sink: Option<mpsc::Sender<Value>>,
) -> Result<Value, String> {
    // Retries remain one logical model call. The async root limit is enforced
    // before tracing/admission so no provider work starts after the hard cap.
    state.portfolio.async_ops.claim_model_call(request).await?;
    let workflow = body
        .as_object_mut()
        .and_then(|o| o.remove("__ccm_workflow"));
    let actor = if kind == "evaluation" {
        "evaluator"
    } else {
        entity
    };
    let name = body
        .as_object_mut()
        .and_then(|o| o.remove("__ccm_name"))
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| entity.to_string());
    let definition_model = if kind == "agent_call" && entity != crate::policy::FALLBACK_ID {
        body["model"].as_str()
    } else {
        None
    };
    let definition_model = definition_model.map(str::to_owned);
    let definition_provider = body
        .as_object_mut()
        .and_then(|o| o.remove("__ccm_provider"));
    let resolved = state.config.for_agent(
        actor,
        &name,
        definition_model.as_deref(),
        definition_provider.as_ref().and_then(Value::as_str),
    )?;
    let run_id = state
        .portfolio
        .trace_begin(request, &json!({"messages":[]}), "internal")?;
    let resolved = state.portfolio.call_config(&run_id, actor, &resolved)?;
    let state = crate::model_config::with_config(state, resolved)?;
    body["model"] = json!(state.config.inference_model());
    state.portfolio.trace_event(request, "model.resolved", &json!({
        "actor":actor,"exposed_model":state.config.model,"provider":state.config.routing.provider,
        "inference_model":state.config.inference_model(),"upstream":state.config.upstream
    }))?;
    let version = body
        .as_object_mut()
        .and_then(|o| o.remove("__ccm_version"))
        .and_then(|v| v.as_str().map(str::to_string));
    let system_prompt = if kind == "composition" {
        json!(crate::foreman::composition_contract())
    } else {
        body.pointer("/messages/0/content")
            .cloned()
            .unwrap_or(Value::Null)
    };
    let version = match version {
        Some(v) => v,
        None => state.portfolio.trace_snapshot(
            actor,
            "system",
            &json!({"system_prompt":system_prompt,"model":body["model"],"operation":kind}),
        )?,
    };
    let run = state
        .portfolio
        .trace_begin(request, &json!({"messages":[]}), "internal")?;
    // Metadata is CCM's correlation protocol, not provider configuration.
    body.as_object_mut().unwrap().remove("metadata");
    let config = &state.config.upstream;
    let mut deadline = None;
    let mut attempt_number = 0;
    let mut call_id = uuid::Uuid::new_v4().simple().to_string();
    loop {
        state.portfolio.boundary(&run, actor).await?;
        let deadline_at = *deadline.get_or_insert_with(|| {
            tokio::time::Instant::now() + Duration::from_millis(config.total_timeout_ms)
        });
        if tokio::time::Instant::now() >= deadline_at {
            state.portfolio.trace_event(
                request,
                "upstream.retry.cancelled",
                &json!({"call_id":call_id,"reason":"total_timeout"}),
            )?;
            return Err(
                "upstream total_timeout: model call exhausted its total budget before next attempt"
                    .into(),
            );
        }
        if sink.as_ref().is_some_and(mpsc::Sender::is_closed) {
            return Err("client disconnected".into());
        }
        attempt_number += 1;
        let mut notices = state.portfolio.trace_bus.subscribe();
        let epoch = state.portfolio.gate(&run, actor)?.1;
        let mut span = Span::start(
            state.portfolio.clone(),
            request,
            kind,
            actor,
            &body,
            Some(&version),
            false,
        )?;
        let guidance = state.portfolio.take_guidance(&run, actor, &span.id)?;
        body["messages"]
            .as_array_mut()
            .ok_or("missing call messages")?
            .extend(guidance);
        span.input(&body)?;
        span.emit("upstream.attempt.started", &json!({"call_id":call_id,"attempt":attempt_number,
            "max_attempts":config.max_attempts,"total_timeout_ms":config.total_timeout_ms,
            "remaining_ms":deadline_at.saturating_duration_since(tokio::time::Instant::now()).as_millis() as u64,
            "idle_timeout_ms":config.idle_timeout_ms,"first_response_timeout_ms":config.first_response_timeout_ms}))?;
        let announcement = match kind {
            "foreman_decision" => {
                Some("Foreman: Reviewing the request and selecting the next operation.".to_string())
            }
            "meta_agent_creation" => {
                Some("Agent-generator: Creating a specialist definition.".to_string())
            }
            "meta_workflow_creation" => {
                Some("Workflow-generator: Creating a workflow definition.".to_string())
            }
            "agent_call" => Some(format!("Specialist '{name}': Working on the task.")),
            "evaluation" => Some(format!(
                "Evaluator: Reviewing the result and definition of '{entity}'."
            )),
            "composition" => Some("Foreman: Composing the response.".to_string()),
            _ => None,
        };
        if let Some(text) = announcement {
            state.announce(request, &text).await?;
        }
        let started = Instant::now();
        if kind == "agent_call" {
            state
                .portfolio
                .workforce_usage(entity, "model_call", &span.id, request)?;
            if let Some(workflow) = workflow.as_ref().and_then(Value::as_str) {
                state
                    .portfolio
                    .workforce_usage(workflow, "model_call", &span.id, request)?;
            }
        }
        let thinking_checkpoint = crate::transparency::attempt_checkpoint(request).await;
        let result = {
            let attempt = async {
                let provider = state
                    .config
                    .routing
                    .providers
                    .get(&state.config.routing.provider)
                    .cloned()
                    .unwrap_or_default();
                let gate = state
                    .providers
                    .get(&state.config.routing.provider, &provider);
                span.emit(
                    "provider.admission.wait",
                    &json!({"provider":state.config.routing.provider}),
                )
                .map_err(Failure::local)?;
                let lease = tokio::time::timeout_at(deadline_at, gate.acquire()).await
                    .map_err(|_| Failure::local("provider admission timeout: total model-call budget exhausted while waiting for capacity or recovery".into()))?;
                span.emit(
                    "provider.admission.acquired",
                    &json!({"provider":state.config.routing.provider}),
                )
                .map_err(Failure::local)?;
                let result = read_call(
                    &state,
                    request,
                    kind,
                    &body,
                    deadline_at,
                    &span,
                    sink.as_ref(),
                )
                .await;
                let failed = result
                    .as_ref()
                    .err()
                    .is_some_and(|e| e.transient || e.category == "http_status");
                let retry_after = result.as_ref().err().and_then(|e| e.retry_after_ms);
                // Dropping the lease releases capacity before any retry backoff.
                let cooldown = if result.is_ok() || failed {
                    lease.finish(failed, retry_after)
                } else {
                    drop(lease);
                    None
                };
                if let Some(delay) = cooldown {
                    span.emit(
                        "provider.cooldown",
                        &json!({"provider":state.config.routing.provider,"cooldown_ms":delay}),
                    )
                    .map_err(Failure::local)?;
                }
                result
            };
            tokio::pin!(attempt);
            loop {
                tokio::select! {
                    result=&mut attempt=>break Some(result),
                    _ = async {
                        if let Some(sink) = &sink { sink.closed().await; }
                        else { std::future::pending::<()>().await; }
                    } => break Some(Err(Failure::local("client disconnected".into()))),
                    _=notices.recv()=>{
                        match state.portfolio.gate(&run,actor) {
                            Ok((_,current)) if current!=epoch=>break None,
                             Err(error)=>break Some(Err(Failure::local(error))),
                            _=>{}
                        }
                    }
                }
            }
        };
        let interrupted = result.is_none()
            || state
                .portfolio
                .gate(&run, actor)
                .is_ok_and(|(_, current)| current != epoch);
        if interrupted {
            crate::transparency::discard_buffered_attempt(request, thinking_checkpoint).await?;
            span.finish(
                "interrupted",
                &json!({"reason":"operator redirect; partial output remains in trace"}),
                None,
            )?;
            state.portfolio.event(
                request,
                kind,
                entity,
                "interrupted",
                started.elapsed().as_millis() as u64,
                None,
                "",
            )?;
            state
                .announce(
                    request,
                    "Foreman: Restarting this operation with the operator’s guidance.",
                )
                .await?;
            // Operator-directed restart is a new call, not an automatic retry.
            deadline = None;
            attempt_number = 0;
            call_id = uuid::Uuid::new_v4().simple().to_string();
            continue;
        }
        let result = result.unwrap();
        match &result {
            Ok(value) => {
                span.finish("completed", value, value.get("usage"))?;
                state.portfolio.event(
                    request,
                    kind,
                    entity,
                    "ok",
                    started.elapsed().as_millis() as u64,
                    value.get("usage"),
                    "",
                )?;
            }
            Err(error) => {
                crate::transparency::discard_buffered_attempt(request, thinking_checkpoint).await?;
                span.emit("upstream.attempt.failed", &error.diagnostics)?;
                span.finish(
                    "error",
                    &json!({"error":error.summary(),"upstream":error.diagnostics}),
                    None,
                )?;
                state.portfolio.event(
                    request,
                    kind,
                    entity,
                    "error",
                    started.elapsed().as_millis() as u64,
                    None,
                    &error.summary(),
                )?;
            }
        }
        // Pause means stopping progression at the next execution boundary.
        state.portfolio.boundary(&run, actor).await?;
        if let Err(error) = &result {
            let backoff = config
                .retry_base_ms
                .saturating_mul(1u64 << (attempt_number - 1).min(9));
            let jitter = (uuid::Uuid::new_v4().as_u128() % (backoff / 2 + 1) as u128) as u64;
            let delay_ms = backoff
                .saturating_add(jitter)
                .max(error.retry_after_ms.unwrap_or(0));
            let remaining = deadline_at.saturating_duration_since(tokio::time::Instant::now());
            let retry = error.transient
                && !error.delivered
                && attempt_number < config.max_attempts
                && Duration::from_millis(delay_ms) < remaining;
            span.emit(
                "upstream.retry",
                &json!({"attempt":attempt_number,"scheduled":retry,
                "delay_ms":delay_ms,"category":error.category,"client_output_sent":error.delivered,
                "remaining_ms":remaining.as_millis() as u64}),
            )?;
            if retry {
                let sleep = tokio::time::sleep(Duration::from_millis(delay_ms));
                tokio::pin!(sleep);
                loop {
                    tokio::select! {
                        _ = &mut sleep => break,
                        _ = async {
                            if let Some(sink) = &sink { sink.closed().await; }
                            else { std::future::pending::<()>().await; }
                        } => return Err("client disconnected".into()),
                        _ = notices.recv() => {
                            if state.portfolio.gate(&run, actor)?.1 != epoch {
                                deadline = None;
                                attempt_number = 0;
                                call_id = uuid::Uuid::new_v4().simple().to_string();
                                break;
                            }
                        }
                    }
                }
                continue;
            }
        }
        return result.map_err(|error| error.summary());
    }
}

#[derive(Default)]
pub(crate) struct Collector {
    content: String,
    reasoning: String,
    refusal: String,
    calls: std::collections::BTreeMap<u64, Value>,
    usage: Option<Value>,
    finish_reason: Option<Value>,
}
impl Collector {
    pub(crate) fn feed(&mut self, chunk: &Value) -> Result<(), String> {
        if chunk.get("error").is_some_and(|v| !v.is_null()) {
            return Err(format!("upstream stream error: {}", chunk["error"]));
        }
        if let Some(usage) = chunk.get("usage").filter(|v| !v.is_null()) {
            self.usage = Some(usage.clone());
        }
        for choice in chunk["choices"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|c| c["index"].as_u64().unwrap_or(0) == 0)
        {
            if let Some(reason) = choice.get("finish_reason").filter(|v| !v.is_null()) {
                self.finish_reason = Some(reason.clone());
            }
            let delta = &choice["delta"];
            if let Some(text) = delta["refusal"].as_str() {
                self.refusal.push_str(text);
            }
            if let Some(text) = delta["content"].as_str() {
                self.content.push_str(text);
            }
            if let Some(text) = delta["reasoning"]
                .as_str()
                .or_else(|| delta["reasoning_content"].as_str())
            {
                self.reasoning.push_str(text);
            }
            for call in delta["tool_calls"].as_array().into_iter().flatten() {
                let index = call["index"].as_u64().ok_or("tool delta missing index")?;
                let entry = self.calls.entry(index).or_insert_with(
                    || json!({"id":"","type":"function","function":{"name":"","arguments":""}}),
                );
                for pointer in ["/id", "/function/name", "/function/arguments"] {
                    if let Some(text) = call.pointer(pointer).and_then(Value::as_str) {
                        let target = entry.pointer_mut(pointer).unwrap();
                        let mut value = target.as_str().unwrap_or("").to_string();
                        value.push_str(text);
                        *target = json!(value);
                    }
                }
            }
        }
        Ok(())
    }
    pub(crate) fn finish(self) -> Value {
        let mut message = json!({"role":"assistant","content":self.content});
        if !self.reasoning.is_empty() {
            message["reasoning"] = json!(self.reasoning);
        }
        if !self.refusal.is_empty() {
            message["refusal"] = json!(self.refusal);
        }
        if !self.calls.is_empty() {
            message["tool_calls"] = json!(self.calls.into_values().collect::<Vec<_>>());
        }
        json!({"choices":[{"index":0,"message":message,"finish_reason":self.finish_reason}],"usage":self.usage})
    }
}

pub fn content(value: &Value) -> Result<&str, String> {
    let message = value
        .pointer("/choices/0/message")
        .ok_or("response missing message")?;
    crate::brief::model_output(message).ok_or_else(|| "response empty".into())
}
pub fn object(value: &Value) -> Result<Value, String> {
    let json =
        crate::brief::extract_json_object(content(value)?).ok_or("response has no JSON object")?;
    serde_json::from_str(&json).map_err(|e| e.to_string())
}
