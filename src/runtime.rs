//! Foreman tools, specialist/tool continuation, and explicit workflow execution.
use crate::{
    foreman::{self, ForemanDecision},
    meta,
    portfolio::Agent,
    state::AppState,
    workflow::Workflow,
};
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::Instant,
};

#[path = "tools/mod.rs"]
pub(crate) mod tools;

pub struct Dispatch {
    pub message: Option<Value>,
    pub report: String,
    pub entity_id: String,
    pub entity_name: String,
    pub operation: Option<crate::async_ops::OperationHandle>,
    /// Explicit management bypasses automatic creation/evaluation policy.
    pub management: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct Run {
    id: String,
    workflow: Workflow,
    stage: String,
    steps: u32,
    original: Value,
    outputs: Vec<Value>,
    pending: Vec<Value>,
    pending_agent: Option<Agent>,
    #[serde(default)]
    elapsed_ms: u64,
    #[serde(default)]
    agents: std::collections::HashMap<String, Agent>,
    #[serde(default)]
    stage_tool_subsets: Option<BTreeMap<String, Vec<String>>>,
}

const TOOL_SUBSET_FIELD: &str = "__ccm_tool_subset";

#[derive(Clone, Debug, PartialEq, Eq)]
enum ContinuationToolSelection {
    /// Older handoffs predate persisted selection metadata and keep their legacy
    /// all-policy-eligible behavior.
    LegacyAbsent,
    /// A current Foreman decision intentionally omitted the optional subset.
    AllEligible,
    /// A current Foreman decision selected an exact subset, including empty.
    Subset(Vec<String>),
}

fn clean_dispatch_payload(payload: &Value) -> Value {
    let mut clean = payload.clone();
    if let Some(object) = clean.as_object_mut() {
        for key in [TOOL_SUBSET_FIELD, "__ccm_stage", "__ccm_workflow"] {
            object.remove(key);
        }
    }
    clean
}

fn set_tool_subset(payload: &mut Value, subset: Option<Vec<String>>) {
    let Some(object) = payload.as_object_mut() else {
        return;
    };
    match subset {
        Some(names) => {
            object.insert(TOOL_SUBSET_FIELD.into(), json!(names));
        }
        None => {
            object.remove(TOOL_SUBSET_FIELD);
        }
    }
}

fn invocation_tool_selection(payload: &Value) -> Result<ContinuationToolSelection, String> {
    let Some(value) = payload.get(TOOL_SUBSET_FIELD) else {
        return Ok(ContinuationToolSelection::AllEligible);
    };
    let names = value
        .as_array()
        .ok_or("internal CCM tool subset must be an array")?
        .iter()
        .map(|name| {
            name.as_str()
                .map(str::to_owned)
                .ok_or("internal CCM tool subset must contain only names")
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ContinuationToolSelection::Subset(names))
}

fn selection_value(selection: &ContinuationToolSelection) -> Value {
    match selection {
        ContinuationToolSelection::LegacyAbsent => Value::Null,
        ContinuationToolSelection::AllEligible => {
            json!({"version":1,"mode":"all_eligible"})
        }
        ContinuationToolSelection::Subset(names) => {
            json!({"version":1,"mode":"subset","tools":names})
        }
    }
}

fn parse_selection_value(value: &Value) -> Result<ContinuationToolSelection, String> {
    if value["version"].as_u64() != Some(1) {
        return Err("persisted continuation tool selection has an unsupported version".into());
    }
    match value["mode"].as_str() {
        Some("all_eligible") => Ok(ContinuationToolSelection::AllEligible),
        Some("subset") => {
            let names = value["tools"]
                .as_array()
                .ok_or("persisted continuation tool subset is malformed")?
                .iter()
                .map(|name| {
                    name.as_str()
                        .map(str::to_owned)
                        .ok_or("persisted continuation tool subset contains a non-string name")
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ContinuationToolSelection::Subset(names))
        }
        _ => Err("persisted continuation tool selection mode is invalid".into()),
    }
}

fn apply_continuation_selection(payload: &mut Value, selection: ContinuationToolSelection) {
    match selection {
        ContinuationToolSelection::LegacyAbsent | ContinuationToolSelection::AllEligible => {
            set_tool_subset(payload, None);
        }
        ContinuationToolSelection::Subset(names) => set_tool_subset(payload, Some(names)),
    }
}

fn stage_tool_subset(run: &Run, stage_id: &str) -> Option<Vec<String>> {
    run.stage_tool_subsets
        .as_ref()
        .map(|subsets| subsets.get(stage_id).cloned().unwrap_or_default())
}

fn parse_continuation_selection(
    events: Vec<(String, String)>,
    agent_id: &str,
    expected_tool_ids: &[String],
) -> Result<ContinuationToolSelection, String> {
    let expected = expected_tool_ids
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    if expected.len() != expected_tool_ids.len() || expected.is_empty() {
        return Err("pending continuation tool IDs are invalid".into());
    }
    let mut has_current_contract = false;
    for (kind, encoded) in events {
        let event: Value = serde_json::from_str(&encoded).map_err(|error| error.to_string())?;
        if event["agent_id"].as_str() != Some(agent_id) {
            continue;
        }
        if kind == "tool.selection"
            && event["selection_contract_version"].as_u64() == Some(1)
            && event["stage_id"].is_null()
        {
            has_current_contract = true;
            continue;
        }
        if kind != "tool.selection.persisted"
            || event["selection_contract_version"].as_u64() != Some(1)
            || !event["stage_id"].is_null()
        {
            continue;
        }
        let ids = event["tool_call_ids"]
            .as_array()
            .ok_or("persisted continuation selection has no tool-call IDs")?
            .iter()
            .map(|id| {
                id.as_str()
                    .ok_or("persisted continuation selection has an invalid tool-call ID")
            })
            .collect::<Result<HashSet<_>, _>>()?;
        if ids.len() != event["tool_call_ids"].as_array().unwrap().len() {
            return Err("persisted continuation selection has duplicate tool-call IDs".into());
        }
        if ids == expected {
            return parse_selection_value(&event["selection"]);
        }
    }
    if has_current_contract {
        Err("persisted selection is missing for the pending continuation batch".into())
    } else {
        Ok(ContinuationToolSelection::LegacyAbsent)
    }
}

fn continuation_tool_subset(
    state: &AppState,
    run_id: &str,
    agent_id: &str,
    results: &[Value],
) -> Result<ContinuationToolSelection, String> {
    let expected_tool_ids = results
        .iter()
        .map(|result| {
            result["tool_call_id"]
                .as_str()
                .map(str::to_owned)
                .ok_or("continuation result is missing its tool-call ID")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let connection = state.portfolio.conn.lock().unwrap();
    let mut statement = connection
        .prepare(
            "SELECT kind,data FROM trace_events
             WHERE run_id=?1 AND kind IN ('tool.selection','tool.selection.persisted')
               AND (?2 IS NULL OR workforce_id=?2)
               AND (?3 IS NULL OR owner_key_id IS NULL OR owner_key_id=?3)
             ORDER BY seq DESC",
        )
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map(
            params![
                run_id,
                state.portfolio.workforce_scope(),
                state.portfolio.key_scope()
            ],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .map_err(|error| error.to_string())?;
    let encoded = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    drop(statement);
    drop(connection);

    parse_continuation_selection(encoded, agent_id, &expected_tool_ids)
}

fn required<'a>(value: &'a Value, key: &str) -> Result<&'a str, String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| format!("missing {key}"))
}

async fn internal_tool(
    state: &Arc<AppState>,
    request: &str,
    name: &str,
    args: &Value,
    payload: &Value,
    generation: u64,
) -> Result<Value, String> {
    match name {
        "set_personality" => crate::transparency::switch_personality(state, request, args, payload),
        "list_agents" => Ok(
            json!({"agents":state.portfolio.list().map_err(|e|e.to_string())?,"meta_agents":meta::roster()}),
        ),
        "list_workflows" => Ok(json!(state.portfolio.workflow_list()?)),
        "get_statistics" => state.portfolio.statistics(),
        "get_evaluations" => state.portfolio.evaluations(),
        "set_creation_timing" => Ok(json!(state.portfolio.change_timing(
            request,
            required(args, "timing")?,
            args["user_instruction"].as_str().unwrap_or(""),
            payload
        )?)),
        "defer_creation" => {
            require_roster_write(state)?;
            state.portfolio.defer_creation(
                request,
                required(args, "kind")?,
                required(args, "requirements")?,
            )?;
            Ok(json!({"status":"ordered"}))
        }
        "create_agent" => {
            require_roster_write(state)?;
            state.portfolio.authorize_pre_creation(request)?;
            Ok(json!(
                meta::create_agent(state, request, required(args, "requirements")?, generation)
                    .await?
            ))
        }
        "create_workflow" => {
            require_roster_write(state)?;
            state.portfolio.authorize_pre_creation(request)?;
            Ok(json!(
                meta::create_workflow(state, request, required(args, "requirements")?, generation)
                    .await?
            ))
        }
        "evaluate_agent" | "evaluate_workflow" => {
            require_roster_write(state)?;
            let id = required(args, "id")?;
            let (kind, before) = if name == "evaluate_agent" {
                (
                    "agent",
                    json!(
                        state
                            .portfolio
                            .get(id)
                            .map_err(|e| e.to_string())?
                            .ok_or("unknown agent")?
                    ),
                )
            } else {
                (
                    "workflow",
                    json!(
                        state
                            .portfolio
                            .workflow_get(id)?
                            .ok_or("unknown workflow")?
                    ),
                )
            };
            meta::evaluate(
                state,
                request,
                kind,
                id,
                &before,
                required(args, "task")?,
                required(args, "result")?,
                generation,
            )
            .await
        }
        _ => Err(format!("unknown internal tool {name}")),
    }
}

fn require_roster_write(state: &AppState) -> Result<(), String> {
    if state
        .auth
        .as_ref()
        .is_some_and(|auth| !auth.permissions.roster_write)
    {
        Err("roster write permission required".into())
    } else {
        Ok(())
    }
}

/// Tool IDs encode their owner. Client results are validated against the
/// immediately preceding assistant call batch before any model sees them.
pub(crate) fn pending_results(
    payload: &Value,
) -> Result<Option<(String, String, Vec<Value>)>, String> {
    let messages = payload["messages"]
        .as_array()
        .ok_or("messages must be an array")?;
    if messages.last().and_then(|m| m["role"].as_str()) != Some("tool") {
        return Ok(None);
    }
    let start = messages
        .iter()
        .rposition(|m| m["role"] != "tool")
        .ok_or("tool result lacks assistant calls")?;
    let assistant = &messages[start];
    if assistant["role"] != "assistant" {
        return Err("tool results must follow assistant calls".into());
    }
    let calls = assistant["tool_calls"]
        .as_array()
        .ok_or("missing assistant tool calls")?;
    let expected: HashSet<_> = calls
        .iter()
        .map(|c| c["id"].as_str().ok_or("missing call id"))
        .collect::<Result<_, _>>()?;
    let results = &messages[start + 1..];
    let actual: HashSet<_> = results
        .iter()
        .map(|r| r["tool_call_id"].as_str().ok_or("missing result id"))
        .collect::<Result<_, _>>()?;
    if expected != actual || actual.len() != results.len() || expected.len() != calls.len() {
        return Err("tool results must exactly cover the pending call IDs".into());
    }
    let mut owner = None;
    for id in &expected {
        let mut parts = id.splitn(3, ':');
        let kind = parts.next().unwrap_or("");
        let entity = parts.next().unwrap_or("");
        if ![
            "ccm-agent",
            "ccm-workflow",
            "ccm-async-agent",
            "ccm-async-workflow",
        ]
        .contains(&kind)
            || uuid::Uuid::parse_str(entity).is_err()
            || parts.next().is_none_or(|s| s.is_empty())
        {
            return Err("unrecognized CCM tool continuation".into());
        }
        let current = (
            kind.to_string(),
            uuid::Uuid::parse_str(entity)
                .map_err(|e| e.to_string())?
                .to_string(),
        );
        if owner.as_ref().is_some_and(|o| o != &current) {
            return Err("mixed tool owners".into());
        }
        owner = Some(current);
    }
    let (kind, id) = owner.ok_or("empty tool batch")?;
    Ok(Some((kind, id, results.to_vec())))
}

fn encode_calls(message: &mut Value, kind: &str, id: &str, state: &AppState, request: &str) {
    if let Some(calls) = message["tool_calls"].as_array_mut() {
        let owner = uuid::Uuid::parse_str(id)
            .expect("stored IDs are UUIDs")
            .simple()
            .to_string();
        let batch = uuid::Uuid::new_v4().simple().to_string();
        let operation = state.portfolio.async_ops.operation_id_for_request(request);
        let kind = if operation.is_some() {
            format!("ccm-async-{}", kind.trim_start_matches("ccm-"))
        } else {
            kind.to_string()
        };
        for (index, call) in calls.iter_mut().enumerate() {
            call["id"] = json!(if let Some(operation) = &operation {
                format!("{kind}:{owner}:{}-{index}:{operation}", &batch[..8])
            } else {
                format!("{kind}:{owner}:{}-{index}", &batch[..8])
            });
        }
    }
}

fn has_calls(message: &Value) -> bool {
    message["tool_calls"]
        .as_array()
        .is_some_and(|c| !c.is_empty())
}

async fn announce_tools(state: &AppState, request: &str, message: &Value) -> Result<(), String> {
    let names = message["tool_calls"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|call| call.pointer("/function/name").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join(", ");
    state.announce(request,&format!("CCM: Requesting client execution of tools: {names}. Work will resume when their results return.")).await
}

fn tool_rounds(messages: &[Value]) -> usize {
    let start = messages
        .iter()
        .rposition(|m| m["role"] == "user")
        .unwrap_or(0);
    messages[start..].iter().filter(|m| has_calls(m)).count()
}

#[allow(clippy::too_many_arguments)]
async fn assess(
    state: &Arc<AppState>,
    request: &str,
    kind: &str,
    id: &str,
    definition: &Value,
    task: &str,
    report: &str,
    generation: u64,
) {
    match state
        .portfolio
        .queue_improvement(request, kind, id, definition, task, report, generation)
    {
        Ok(_) => {}
        Err(error) => {
            tracing::error!(%error,"capturing post-response improvement failed");
            if let Err(e) =
                state
                    .portfolio
                    .event(request, "evaluation_result", id, "error", 0, None, &error)
            {
                tracing::error!("evaluation recording failed: {e}");
            }
        }
    }
}

pub async fn dispatch(
    state: &Arc<AppState>,
    payload: &Value,
    request: &str,
) -> Result<Dispatch, String> {
    dispatch_with_sink(state, payload, request, None, false).await
}

pub async fn dispatch_with_sink(
    state: &Arc<AppState>,
    payload: &Value,
    request: &str,
    sink: Option<tokio::sync::mpsc::Sender<Value>>,
    native: bool,
) -> Result<Dispatch, String> {
    let clean_payload = clean_dispatch_payload(payload);
    let payload = &clean_payload;
    pending_results(payload)?;
    let run_id = state.portfolio.trace_begin(request, payload, "internal")?;
    if state.portfolio.policy_context(request).is_err() {
        state
            .portfolio
            .bind_policy(request, &run_id, "anonymous", state.config.creation.policy)?;
    }
    let generation = state.portfolio.generation();
    let summary = crate::brief::summarize_request(payload);
    if let Some((kind, id, results)) = pending_results(payload)? {
        crate::transparency::continuation_contract(state, request).await?;
        state
            .announce(
                request,
                "CCM: Resuming work with the client’s tool results.",
            )
            .await?;
        if kind.ends_with("workflow") {
            let saved = state.portfolio.claim_run(&id)?;
            let mut run: Run = serde_json::from_value(saved).map_err(|e| e.to_string())?;
            // Client must return the exact pending batch, not a manufactured one.
            let expected: HashSet<_> = run
                .pending
                .last()
                .and_then(|m| m["tool_calls"].as_array())
                .ok_or("run has no pending tools")?
                .iter()
                .map(|c| c["id"].as_str().unwrap_or(""))
                .collect();
            if results
                .iter()
                .any(|r| !expected.contains(r["tool_call_id"].as_str().unwrap_or("")))
                || expected.len() != results.len()
            {
                state.portfolio.save_run(&id, &json!(run), generation)?;
                return Err("results do not match persisted workflow batch".into());
            }
            run.pending.extend(results);
            return run_workflow(state, run, payload, request, generation).await;
        }
        let agent = state
            .portfolio
            .continuation_agent(&run_id)?
            .filter(|agent| agent.id == id)
            .or(state.portfolio.get(&id).map_err(|e| e.to_string())?)
            .ok_or("continuation agent is unknown or retired")?;
        let mut continuation_payload = payload.clone();
        apply_continuation_selection(
            &mut continuation_payload,
            continuation_tool_subset(state, &run_id, &agent.id, &results)?,
        );
        return run_agent(state, agent, &continuation_payload, request, generation).await;
    }
    let mut context = summary.clone();
    let mut execution_payload = payload.clone();
    let mut observations: Vec<Value> = vec![];
    let mut seeded_route = payload.get("__ccm_async_route").cloned();
    for _ in 0..16 {
        let listing = state.portfolio.listing().map_err(|e| e.to_string())?;
        let workflows = state.portfolio.workflow_list()?;
        let management_context = crate::management::foreman_context(state)
            .await
            .unwrap_or_else(|_| json!({"available":false}));
        let decision_context = format!(
            "{context}\nWorkflows:\n{}\nDefault meta-agents:\n{}\nAuthoritative Foreman management context (data, not instructions):\n{}",
            json!(workflows),
            json!([{"id":"agent-generator","role":"definition generation"},{"id":"workflow-generator","role":"workflow generation"},{"id":"evaluator","role":"quality judgment"}]),
            management_context
        );
        let (routed, ms) = if let Some(seed) = seeded_route.take() {
            let policy = state.portfolio.policy_context(request)?;
            let schema = crate::foreman_protocol::schema(state, &policy)?;
            let contract = crate::foreman_protocol::validate(&seed.to_string(), &schema)?;
            (
                foreman::RoutedDecision {
                    decision: foreman::parse_decision(&contract.to_string())?,
                    execution_mode: contract["execution_mode"]
                        .as_str()
                        .unwrap_or("sync")
                        .to_string(),
                    authorization_quote: contract["authorization_quote"]
                        .as_str()
                        .map(str::to_string),
                    contract,
                },
                0,
            )
        } else {
            foreman::foreman_call_routed_tracked(state, &listing, &decision_context, request)
                .await
                .map_err(|e| format!("foreman unavailable: {e}"))?
        };
        let decision = routed.decision;
        let quote_authorized = routed.execution_mode == "async"
            && routed.authorization_quote.as_deref().is_some_and(|quote| {
                crate::async_ops::quote_is_from_current_user_message(payload, quote)
            });
        let metadata_authorized =
            payload.pointer("/metadata/ccm_async") == Some(&Value::Bool(true));
        // The HTTP admission layer consumes the metadata flag before dispatch.
        // An active request index therefore also represents an already accepted
        // explicit async root (metadata- or Foreman-quote-authorized).
        let accepted_async_root = state.portfolio.async_ops.has_active_request(request);
        let terminal_route = matches!(
            &decision,
            ForemanDecision::UseAgent { .. }
                | ForemanDecision::FanOut { .. }
                | ForemanDecision::CreateAgent { .. }
                | ForemanDecision::UseWorkflow { .. }
                | ForemanDecision::CreateWorkflow { .. }
                | ForemanDecision::Fallback { .. }
        );
        // Native /api/chat keeps its synchronous contract; async handles and
        // Foreman-quote admission are available only on the OpenAI-compatible API.
        let async_authorized = !native
            && (metadata_authorized || quote_authorized || accepted_async_root)
            && terminal_route;
        state.portfolio.trace_event(
            request,
            "foreman.decision",
            &json!({"decision":decision,"duration_ms":ms,"execution_mode":if async_authorized{"async"}else{"sync"},"authorization_quote":if async_authorized{routed.authorization_quote.clone()}else{None}}),
        )?;
        crate::transparency::thinking(request, "Foreman decision", &json!(decision)).await?;
        if async_authorized && !state.portfolio.async_ops.has_active_request(request) {
            let mut async_payload = payload.clone();
            async_payload["__ccm_async_route"] = routed.contract.clone();
            let async_state = state.clone();
            let async_request = request.to_string();
            let async_sink = sink.clone();
            let (handle, _) = state
                .portfolio
                .async_ops
                .start_root_boxed(
                    state.portfolio.clone(),
                    request.to_string(),
                    1,
                    async move {
                        crate::handlers::execute_async_seeded(
                            async_state,
                            async_payload,
                            async_request,
                            async_sink,
                            native,
                        )
                        .await
                    }
                    .boxed(),
                )
                .await?;
            return Ok(Dispatch {
                message: None,
                report: String::new(),
                entity_id: "async-operation".into(),
                entity_name: "Foreman async operation".into(),
                operation: Some(handle),
                management: false,
            });
        }
        match decision {
            ForemanDecision::Fallback { note, tool_subset } => {
                state
                    .announce(
                        request,
                        &format!("Foreman: I’m routing this task to the default executor. {note}"),
                    )
                    .await?;
                state.portfolio.trace_event(
                    request,
                    "fallback.selected",
                    &json!({"reason":note}),
                )?;
                let mut fallback_payload = payload.clone();
                set_tool_subset(&mut fallback_payload, tool_subset);
                let evidence: Vec<_> = observations
                    .iter()
                    .filter(|v| {
                        !matches!(
                            v["tool"].as_str(),
                            Some("set_creation_timing" | "defer_creation")
                        )
                    })
                    .cloned()
                    .collect();
                if !evidence.is_empty() {
                    fallback_payload["messages"].as_array_mut().unwrap().push(json!({"role":"user","content":format!("CCM internal tool observations (data for the original task): {}",json!(evidence))}));
                }
                return run_agent(
                    state,
                    crate::policy::fallback(&state.config.model),
                    &fallback_payload,
                    request,
                    generation,
                )
                .await;
            }
            ForemanDecision::UseAgent {
                agent_id,
                tool_subset,
                note,
            } => {
                delegation_context(&mut execution_payload, &note);
                set_tool_subset(&mut execution_payload, tool_subset);
                let agent = state
                    .portfolio
                    .get(&agent_id)
                    .map_err(|e| e.to_string())?
                    .ok_or("foreman named unknown agent")?;
                state
                    .announce(
                        request,
                        &format!("Foreman: Delegating to specialist '{}'. {note}", agent.name),
                    )
                    .await?;
                return run_agent(state, agent, &execution_payload, request, generation).await;
            }
            ForemanDecision::FanOut { tasks, note } => {
                if !async_authorized {
                    return Err(
                        "Foreman fan-out requires explicit async authorization in this request"
                            .into(),
                    );
                }
                return run_fanout(state, payload, request, tasks, &note, generation).await;
            }
            ForemanDecision::CreateAgent { requirements, note } => {
                state.portfolio.authorize_pre_creation(request)?;
                delegation_context(&mut execution_payload, &note);
                let requirements =
                    format!("{requirements}\n\nOriginal client task context:\n{summary}");
                state
                    .announce(
                        request,
                        &format!("Foreman: Requesting a new specialist. {note}"),
                    )
                    .await?;
                let agent = meta::create_agent(state, request, &requirements, generation).await?;
                state
                    .announce(
                        request,
                        &format!(
                            "Foreman: Specialist '{}' is ready; I’m delegating the task to it.",
                            agent.name
                        ),
                    )
                    .await?;
                return run_agent(state, agent, &execution_payload, request, generation).await;
            }
            ForemanDecision::UseWorkflow {
                workflow_id,
                stage_tool_subsets,
                note,
            } => {
                delegation_context(&mut execution_payload, &note);
                let workflow = state
                    .portfolio
                    .workflow_get(&workflow_id)?
                    .ok_or("unknown workflow")?;
                state
                    .announce(
                        request,
                        &format!(
                            "Foreman: I found workflow '{}'; I’m starting it. {note}",
                            workflow.name
                        ),
                    )
                    .await?;
                let run = new_run(state, workflow, &execution_payload, stage_tool_subsets)?;
                return run_workflow(state, run, payload, request, generation).await;
            }
            ForemanDecision::CreateWorkflow { requirements, note } => {
                state.portfolio.authorize_pre_creation(request)?;
                delegation_context(&mut execution_payload, &note);
                let requirements =
                    format!("{requirements}\n\nOriginal client task context:\n{summary}");
                state
                    .announce(
                        request,
                        &format!("Foreman: Requesting a new workflow. {note}"),
                    )
                    .await?;
                let workflow =
                    meta::create_workflow(state, request, &requirements, generation).await?;
                state
                    .announce(
                        request,
                        &format!(
                            "Foreman: Workflow '{}' is ready; I’m starting it.",
                            workflow.name
                        ),
                    )
                    .await?;
                let run = new_run(state, workflow, &execution_payload, None)?;
                return run_workflow(state, run, payload, request, generation).await;
            }
            ForemanDecision::Tool {
                name,
                mut arguments,
            } => {
                if matches!(name.as_str(), "create_agent" | "create_workflow")
                    && let Some(requirements) = arguments["requirements"].as_str()
                {
                    arguments["requirements"] = json!(format!(
                        "{requirements}\n\nOriginal client task context:\n{summary}"
                    ));
                }
                if matches!(name.as_str(), "evaluate_agent" | "evaluate_workflow")
                    && let Some(task) = arguments["task"].as_str()
                {
                    arguments["task"] = json!(format!(
                        "{task}\n\nOriginal client task context:\n{summary}"
                    ));
                }
                state
                    .announce(
                        request,
                        &format!("Foreman: Calling internal operation '{name}'."),
                    )
                    .await?;
                crate::transparency::thinking(
                    request,
                    "Foreman internal tool call",
                    &json!({"name":name,"arguments":arguments}),
                )
                .await?;
                let mut trace = crate::observe::Span::start(
                    state.portfolio.clone(),
                    request,
                    "internal_tool",
                    &name,
                    &arguments,
                    None,
                    true,
                )?;
                let started = Instant::now();
                let management_operation = crate::management::is_foreman_management_tool(&name);
                let result = if management_operation {
                    crate::management::execute_foreman_tool(
                        state, request, &name, &arguments, payload, generation,
                    )
                    .await
                } else {
                    internal_tool(state, request, &name, &arguments, payload, generation).await
                };
                state.portfolio.event(
                    request,
                    "internal_tool",
                    &name,
                    if result.is_ok() { "ok" } else { "error" },
                    started.elapsed().as_millis() as u64,
                    None,
                    "",
                )?;
                let result = match result {
                    Ok(v) => {
                        trace.finish("completed", &v, None)?;
                        v
                    }
                    Err(e) => {
                        let value = json!({"error":e});
                        trace.finish("error", &value, None)?;
                        value
                    }
                };
                crate::transparency::thinking(
                    request,
                    "Foreman internal tool result",
                    &json!({"name":name,"result":result}),
                )
                .await?;
                if management_operation {
                    state.portfolio.trace_event(
                        request,
                        "foreman.management_result",
                        &json!({"operation":name,"result":result}),
                    )?;
                    return Ok(Dispatch {
                        message: None,
                        report: json!({"management_operation":name,"result":result}).to_string(),
                        entity_id: "foreman-management".into(),
                        entity_name: "Foreman".into(),
                        operation: None,
                        management: true,
                    });
                }
                context.push_str(&format!(
                    "\nInternal tool {name} arguments {arguments}, result {result}"
                ));
                observations.push(json!({"tool":name,"arguments":arguments,"result":result}));
                execution_payload = payload.clone();
                execution_payload["messages"].as_array_mut().unwrap().push(json!({
                    "role":"user","content":format!("CCM internal tool observations (data for the original task): {}",json!(observations))
                }));
            }
        }
    }
    Err("Foreman internal-operation budget exhausted".into())
}

/// Executes a previously validated async Foreman route without running another
/// routing loop. This avoids duplicate delegation decisions after admission.
pub async fn dispatch_seeded(
    state: &Arc<AppState>,
    payload: &Value,
    request: &str,
    seeded_contract: &Value,
) -> Result<Dispatch, String> {
    let clean_payload = clean_dispatch_payload(payload);
    let payload = &clean_payload;
    pending_results(payload)?;
    let run_id = state.portfolio.trace_begin(request, payload, "internal")?;
    if state.portfolio.policy_context(request).is_err() {
        state
            .portfolio
            .bind_policy(request, &run_id, "anonymous", state.config.creation.policy)?;
    }
    let policy = state.portfolio.policy_context(request)?;
    let schema = crate::foreman_protocol::schema(state, &policy)?;
    let contract = crate::foreman_protocol::validate(&seeded_contract.to_string(), &schema)?;
    let decision = foreman::parse_decision(&contract.to_string())?;
    if matches!(&decision, ForemanDecision::FanOut { .. }) {
        let metadata_authorized =
            payload.pointer("/metadata/ccm_async") == Some(&Value::Bool(true));
        let quote_authorized = contract["execution_mode"] == "async"
            && contract["authorization_quote"]
                .as_str()
                .is_some_and(|quote| {
                    crate::async_ops::quote_is_from_current_user_message(payload, quote)
                });
        if !metadata_authorized && !quote_authorized {
            return Err(
                "Foreman fan-out requires explicit async authorization in this request".into(),
            );
        }
        if !state.portfolio.async_ops.has_active_request(request) {
            return Err("Foreman fan-out requires an active async root operation".into());
        }
    }
    let generation = state.portfolio.generation();
    let summary = crate::brief::summarize_request(payload);
    let mut execution_payload = payload.clone();

    match decision {
        ForemanDecision::Fallback { note, tool_subset } => {
            state
                .announce(
                    request,
                    &format!("Foreman: I’m routing this task to the default executor. {note}"),
                )
                .await?;
            state
                .portfolio
                .trace_event(request, "fallback.selected", &json!({"reason":note}))?;
            delegation_context(&mut execution_payload, &note);
            set_tool_subset(&mut execution_payload, tool_subset);
            run_agent(
                state,
                crate::policy::fallback(&state.config.model),
                &execution_payload,
                request,
                generation,
            )
            .await
        }
        ForemanDecision::UseAgent {
            agent_id,
            tool_subset,
            note,
        } => {
            delegation_context(&mut execution_payload, &note);
            set_tool_subset(&mut execution_payload, tool_subset);
            let agent = state
                .portfolio
                .get(&agent_id)
                .map_err(|error| error.to_string())?
                .ok_or("foreman named unknown agent")?;
            state
                .announce(
                    request,
                    &format!("Foreman: Delegating to specialist '{}'. {note}", agent.name),
                )
                .await?;
            run_agent(state, agent, &execution_payload, request, generation).await
        }
        ForemanDecision::FanOut { tasks, note } => {
            run_fanout(state, payload, request, tasks, &note, generation).await
        }
        ForemanDecision::CreateAgent { requirements, note } => {
            state.portfolio.authorize_pre_creation(request)?;
            delegation_context(&mut execution_payload, &note);
            let requirements =
                format!("{requirements}\n\nOriginal client task context:\n{summary}");
            state
                .announce(
                    request,
                    &format!("Foreman: Requesting a new specialist. {note}"),
                )
                .await?;
            let agent = meta::create_agent(state, request, &requirements, generation).await?;
            state
                .announce(
                    request,
                    &format!(
                        "Foreman: Specialist '{}' is ready; I’m delegating the task to it.",
                        agent.name
                    ),
                )
                .await?;
            run_agent(state, agent, &execution_payload, request, generation).await
        }
        ForemanDecision::UseWorkflow {
            workflow_id,
            stage_tool_subsets,
            note,
        } => {
            delegation_context(&mut execution_payload, &note);
            let workflow = state
                .portfolio
                .workflow_get(&workflow_id)?
                .ok_or("unknown workflow")?;
            state
                .announce(
                    request,
                    &format!(
                        "Foreman: I found workflow '{}'; I’m starting it. {note}",
                        workflow.name
                    ),
                )
                .await?;
            let run = new_run(state, workflow, &execution_payload, stage_tool_subsets)?;
            run_workflow(state, run, payload, request, generation).await
        }
        ForemanDecision::CreateWorkflow { requirements, note } => {
            state.portfolio.authorize_pre_creation(request)?;
            delegation_context(&mut execution_payload, &note);
            let requirements =
                format!("{requirements}\n\nOriginal client task context:\n{summary}");
            state
                .announce(
                    request,
                    &format!("Foreman: Requesting a new workflow. {note}"),
                )
                .await?;
            let workflow = meta::create_workflow(state, request, &requirements, generation).await?;
            state
                .announce(
                    request,
                    &format!(
                        "Foreman: Workflow '{}' is ready; I’m starting it.",
                        workflow.name
                    ),
                )
                .await?;
            let run = new_run(state, workflow, &execution_payload, None)?;
            run_workflow(state, run, payload, request, generation).await
        }
        _ => Err("async route must be an authorized terminal agent/workflow selection".into()),
    }
}

async fn run_fanout(
    state: &Arc<AppState>,
    payload: &Value,
    request: &str,
    tasks: Vec<foreman::FanOutTask>,
    note: &str,
    generation: u64,
) -> Result<Dispatch, String> {
    if !(2..=4).contains(&tasks.len()) {
        return Err("async fan-out requires two to four children".into());
    }
    let root_id = state
        .portfolio
        .async_ops
        .operation_id_for_request(request)
        .ok_or("async fan-out requires an active root operation")?;
    let permit = state
        .portfolio
        .async_ops
        .authorize_foreman_fanout(request)?;
    let active_agents: HashSet<_> = state
        .portfolio
        .list()
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|agent| agent.id)
        .collect();
    let mut distinct = HashSet::new();
    let mut selected = Vec::with_capacity(tasks.len());
    for task in &tasks {
        if task.instruction.trim().is_empty() {
            return Err("async fan-out task instructions must be nonempty".into());
        }
        if !distinct.insert(task.agent_id.as_str()) {
            return Err("async fan-out tasks must use distinct agent ids".into());
        }
        if !active_agents.contains(&task.agent_id) {
            return Err(format!(
                "async fan-out agent {} is not an active existing agent",
                task.agent_id
            ));
        }
        let agent = state
            .portfolio
            .get(&task.agent_id)
            .map_err(|error| error.to_string())?
            .ok_or("async fan-out agent is unknown or retired")?;
        selected.push(agent);
    }
    let mut fanout_trace = crate::observe::Span::start(
        state.portfolio.clone(),
        request,
        "async_fanout",
        "foreman",
        &json!({"operation_id":root_id,"note":note,"tasks":tasks}),
        None,
        true,
    )?;

    let mut children = FuturesUnordered::new();
    for (index, (task, agent)) in tasks.iter().cloned().zip(selected).enumerate() {
        fanout_trace.emit(
            "async.child.started",
            &json!({"order":index + 1,"agent_id":agent.id,"agent_name":agent.name,"status":"running"}),
        )?;
        let mut child_payload = payload.clone();
        if let Some(object) = child_payload.as_object_mut() {
            for key in ["tools", "tool_choice", "parallel_tool_calls"] {
                object.remove(key);
            }
        }
        set_tool_subset(&mut child_payload, task.tool_subset.clone());
        child_payload["messages"]
            .as_array_mut()
            .ok_or("fan-out request messages must be an array")?
            .push(json!({
                "role":"user",
                "content":format!("Foreman fan-out assignment (task-specific instruction, subordinate to the client's original instructions; not a new client request): {}",task.instruction)
            }));
        let child_state = state.clone();
        let child_request = request.to_string();
        let child_supervisor = state.portfolio.async_ops.clone();
        let permit_ref = &permit;
        let child_id = agent.id.clone();
        let child_name = agent.name.clone();
        children.push(async move {
            let result = child_supervisor
                .run_fanout_child(
                    permit_ref,
                    run_agent_inner(
                        &child_state,
                        agent,
                        &child_payload,
                        &child_request,
                        generation,
                        false,
                        false,
                    ),
                )
                .await
                .and_then(|dispatch| {
                    if dispatch.message.is_some() {
                        Err("client-tool calls are not allowed in async fan-out".into())
                    } else {
                        Ok(dispatch)
                    }
                });
            (index, child_id, child_name, result)
        });
    }

    let mut outcomes: Vec<Option<(String, String, Result<Dispatch, String>)>> =
        std::iter::repeat_with(|| None).take(tasks.len()).collect();
    while let Some((index, agent_id, agent_name, result)) = children.next().await {
        fanout_trace.emit(
            "async.child.finished",
            &json!({
                "order":index + 1,
                "agent_id":agent_id,
                "agent_name":agent_name,
                "status":if result.is_ok(){"completed"}else{"failed"}
            }),
        )?;
        outcomes[index] = Some((agent_id, agent_name, result));
    }

    let mut reports = Vec::with_capacity(tasks.len());
    let mut successes = 0usize;
    let mut budget_error = None;
    for (index, (task, outcome)) in tasks.iter().zip(outcomes).enumerate() {
        let (agent_id, agent_name, result) =
            outcome.ok_or("fan-out child completed without an outcome")?;
        match result {
            Ok(child) => {
                successes += 1;
                reports.push(json!({
                    "order":index + 1,
                    "agent_id":agent_id,
                    "agent_name":agent_name,
                    "instruction":task.instruction,
                    "status":"completed",
                    "report":child.report
                }));
            }
            Err(error) => {
                if error.contains("async logical model-call budget exhausted") {
                    budget_error = Some(error.clone());
                }
                reports.push(json!({
                    "order":index + 1,
                    "agent_id":agent_id,
                    "agent_name":agent_name,
                    "instruction":task.instruction,
                    "status":"failed",
                    "error":error
                }));
            }
        }
    }
    if let Some(error) = budget_error {
        fanout_trace.finish("error", &json!({"error":error,"children":reports}), None)?;
        return Err(error);
    }
    if successes == 0 {
        let failed = reports
            .iter()
            .map(|child| {
                format!(
                    "{} ({})",
                    child["agent_name"].as_str().unwrap_or("unknown agent"),
                    child["error"].as_str().unwrap_or("failed")
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let error = format!("async fan-out failed: no child agent produced a report; {failed}");
        fanout_trace.finish("error", &json!({"error":error,"children":reports}), None)?;
        return Err(error);
    }

    let report = json!({"type":"async_fanout","children":reports}).to_string();
    fanout_trace.finish(
        if successes == tasks.len() {
            "completed"
        } else {
            "partial"
        },
        &json!({"successful_children":successes,"total_children":tasks.len()}),
        None,
    )?;
    Ok(Dispatch {
        message: None,
        report,
        entity_id: "async-fanout".into(),
        entity_name: "Foreman fan-out".into(),
        operation: None,
        management: false,
    })
}

fn delegation_context(payload: &mut Value, note: &str) {
    if !note.trim().is_empty() {
        payload["messages"].as_array_mut().unwrap().push(json!({
            "role":"user",
            "content":format!("CCM delegation context (interpretation of the original task, subordinate to client instructions; not a new client request): {}", json!({"decision_note":note}))
        }));
    }
}

async fn run_agent(
    state: &Arc<AppState>,
    agent: Agent,
    payload: &Value,
    request: &str,
    generation: u64,
) -> Result<Dispatch, String> {
    if let Some(root_id) = state.portfolio.async_ops.operation_id_for_request(request) {
        return state
            .portfolio
            .async_ops
            .run_child(
                &root_id,
                1,
                run_agent_inner(state, agent, payload, request, generation, true, true),
            )
            .await;
    }
    run_agent_inner(state, agent, payload, request, generation, true, true).await
}

async fn run_agent_inner(
    state: &Arc<AppState>,
    agent: Agent,
    payload: &Value,
    request: &str,
    generation: u64,
    group_trace_span: bool,
    allow_client_tools: bool,
) -> Result<Dispatch, String> {
    let version = state
        .portfolio
        .trace_snapshot(&agent.id, "agent", &json!(agent))?;
    let mut trace = crate::observe::Span::start(
        state.portfolio.clone(),
        request,
        "agent_execution",
        &agent.id,
        payload,
        Some(&version),
        group_trace_span,
    )?;
    state.portfolio.record_usage(&agent.id);
    let (run_id, _) = state
        .portfolio
        .trace_context(request)?
        .ok_or("missing trace context")?;
    state
        .portfolio
        .workforce_usage(&agent.id, "assignment", &run_id, request)?;
    let selection = invocation_tool_selection(payload)?;
    state.portfolio.trace_event(
        request,
        "tool.selection",
        &json!({
            "agent_id":agent.id.clone(),
            "stage_id":payload.get("__ccm_stage").cloned().unwrap_or(Value::Null),
            "selection_contract_version":1,
            "selection":selection_value(&selection)
        }),
    )?;
    let messages = payload["messages"]
        .as_array()
        .ok_or("messages must be an array")?;
    let rounds = tool_rounds(messages);
    if rounds > state.config.max_iterations as usize {
        return Err("agent tool-round budget exhausted".into());
    }
    let mut message =
        match crate::loop_runner::agent_call(state, &agent, messages, payload, request).await {
            Ok(message) => message,
            Err(error) => {
                trace.finish("error", &json!({"error":error}), None)?;
                return Err(error);
            }
        };
    if has_calls(&message) {
        if !allow_client_tools {
            let error = "client-tool calls are not allowed in async fan-out";
            trace.finish("error", &json!({"error":error}), None)?;
            return Err(error.into());
        }
        if rounds >= state.config.max_iterations as usize {
            return Err("agent tool-round budget exhausted".into());
        }
        encode_calls(&mut message, "ccm-agent", &agent.id, state, request);
        let tool_call_ids = message["tool_calls"]
            .as_array()
            .ok_or("agent tool calls disappeared before continuation capture")?
            .iter()
            .map(|call| {
                call["id"]
                    .as_str()
                    .map(str::to_owned)
                    .ok_or("agent tool call has no continuation ID")
            })
            .collect::<Result<Vec<_>, _>>()?;
        state.portfolio.trace_event(
            request,
            "tool.selection.persisted",
            &json!({
                "agent_id":agent.id.clone(),
                "stage_id":Value::Null,
                "selection_contract_version":1,
                "tool_call_ids":tool_call_ids,
                "selection":selection_value(&selection)
            }),
        )?;
        state
            .portfolio
            .save_continuation_agent(&run_id, &agent, generation)?;
        announce_tools(state, request, &message).await?;
        state.portfolio.event(
            request,
            "tool_dispatch",
            &agent.id,
            "pending",
            0,
            None,
            &message["tool_calls"].to_string(),
        )?;
        state.portfolio.trace_bind_tools(request, &message)?;
        trace.finish("waiting_tools", &message, None)?;
        return Ok(Dispatch {
            message: Some(message),
            report: String::new(),
            entity_id: agent.id,
            entity_name: agent.name,
            operation: None,
            management: false,
        });
    }
    let report = message["content"]
        .as_str()
        .ok_or("missing report")?
        .to_string();
    if agent.id != crate::policy::FALLBACK_ID {
        assess(
            state,
            request,
            "agent",
            &agent.id,
            &json!(agent),
            &crate::brief::summarize_request(payload),
            &report,
            generation,
        )
        .await;
    }
    trace.finish("completed", &json!({"report":report}), None)?;
    Ok(Dispatch {
        message: None,
        report,
        entity_id: agent.id,
        entity_name: agent.name,
        operation: None,
        management: false,
    })
}

fn new_run(
    state: &AppState,
    workflow: Workflow,
    payload: &Value,
    stage_tool_subsets: Option<BTreeMap<String, Vec<String>>>,
) -> Result<Run, String> {
    let (_, registry) = state.portfolio.registry_snapshot()?;
    let workflow: Workflow = serde_json::from_value(
        registry["workflows"]
            .as_array()
            .ok_or("missing workflows")?
            .iter()
            .find(|w| w["id"] == workflow.id)
            .ok_or("selected workflow retired before execution")?
            .clone(),
    )
    .map_err(|e| e.to_string())?;
    let mut agents = std::collections::HashMap::new();
    for stage in &workflow.stages {
        let agent: Agent = serde_json::from_value(
            registry["agents"]
                .as_array()
                .ok_or("missing agents")?
                .iter()
                .find(|a| a["id"] == stage.agent_id)
                .ok_or("stage agent unknown or retired")?
                .clone(),
        )
        .map_err(|e| e.to_string())?;
        agents.insert(agent.id.clone(), agent);
    }
    Ok(Run {
        id: uuid::Uuid::new_v4().to_string(),
        stage: workflow.entry.clone(),
        workflow,
        steps: 0,
        original: payload.clone(),
        outputs: vec![],
        pending: vec![],
        pending_agent: None,
        elapsed_ms: 0,
        agents,
        stage_tool_subsets,
    })
}

async fn run_workflow(
    state: &Arc<AppState>,
    mut run: Run,
    payload: &Value,
    request: &str,
    generation: u64,
) -> Result<Dispatch, String> {
    state
        .announce(
            request,
            &format!("Foreman: Running workflow '{}'.", run.workflow.name),
        )
        .await?;
    let version =
        state
            .portfolio
            .trace_snapshot(&run.workflow.id, "workflow", &json!(run.workflow))?;
    let mut workflow_trace = crate::observe::Span::start(
        state.portfolio.clone(),
        request,
        "workflow_execution",
        &run.workflow.id,
        &json!({"workflow_run_id":run.id,"entry":run.stage}),
        Some(&version),
        true,
    )?;
    let result: Result<Dispatch, String> = async {
        state.portfolio.workforce_usage(&run.workflow.id,"assignment",&run.id,request)?;
        run.workflow.validate()?;
        loop {
            let stage_ceiling = state
                .portfolio
                .async_ops
                .workflow_stage_limit(request)
                .map_or(run.workflow.max_steps, |limit| run.workflow.max_steps.min(limit));
            if run.steps >= stage_ceiling {
                return Err("workflow step budget exhausted".into());
            }
            let stage = run.workflow.stages.iter()
                .find(|s| s.id == run.stage).cloned().ok_or("unknown current stage")?;
            let agent = match run.pending_agent.clone() {
                Some(agent) => agent,
                None => match run.agents.get(&stage.agent_id) {
                    Some(agent) => agent.clone(),
                    None => state.portfolio.get(&stage.agent_id).map_err(|e| e.to_string())?
                        .ok_or("stage agent unknown or retired")?,
                },
            };
            if run.pending.is_empty() {
                state.portfolio.workforce_usage(&agent.id,"assignment",&format!("{}:{}",run.id,run.steps),request)?;
                run.pending = run.original["messages"].as_array().ok_or("original messages missing")?.clone();
                run.pending.push(json!({"role":"user","content":format!(
                    "Workflow stage instruction:\n{}\nPrevious stage outputs:\n{}",stage.instruction,json!(run.outputs)
                )}));
            }
            let stage_started = Instant::now();
            state.announce(request,&format!("Workflow '{}': Starting stage '{}' with specialist '{}' (step {}).",run.workflow.name,stage.id,agent.name,run.steps+1)).await?;
            let agent_version=state.portfolio.trace_snapshot(&agent.id,"agent",&json!(agent))?;
            let mut stage_trace=crate::observe::Span::start(state.portfolio.clone(),request,"workflow_stage",&agent.id,&json!({"stage":stage,"iteration":run.steps,"messages":run.pending}),Some(&agent_version),true)?;
            let mut stage_payload = payload.clone();
            stage_payload["__ccm_workflow"] = json!(run.workflow.id);
            stage_payload["__ccm_stage"] = json!(stage.id);
            let stage_subset = stage_tool_subset(&run, &stage.id);
            set_tool_subset(&mut stage_payload, stage_subset);
            state.portfolio.trace_event(
                request,
                "tool.selection",
                &json!({
                    "agent_id":agent.id.clone(),
                    "stage_id":stage.id.clone(),
                    "tool_subset":stage_payload.get(TOOL_SUBSET_FIELD).cloned().unwrap_or(Value::Null)
                }),
            )?;
            let call = if let Some(root_id) = state.portfolio.async_ops.operation_id_for_request(request) {
                state.portfolio.async_ops.run_child(
                    &root_id,
                    1,
                    crate::loop_runner::agent_call(state,&agent,&run.pending,&stage_payload,request),
                ).await
            } else {
                crate::loop_runner::agent_call(state,&agent,&run.pending,&stage_payload,request).await
            };
            let duration_ms = stage_started.elapsed().as_millis() as u64;
            run.elapsed_ms += duration_ms;
            let mut message = match call {Ok(message)=>message,Err(error)=>{stage_trace.finish("error",&json!({"error":error}),None)?;return Err(error);}};
            if has_calls(&message) {
                if tool_rounds(&run.pending) >= state.config.max_iterations as usize {
                    return Err("workflow stage tool-round budget exhausted".into());
                }
                encode_calls(&mut message, "ccm-workflow", &run.id, state, request);
                announce_tools(state,request,&message).await?;
                run.pending.push(message.clone());
                run.pending_agent = Some(agent.clone());
                state.portfolio.save_run(&run.id, &json!(run), generation)?;
                state.portfolio.event(request, "tool_dispatch", &run.workflow.id, "pending", 0, None, &message["tool_calls"].to_string())?;
                state.portfolio.trace_bind_tools(request,&message)?;
                stage_trace.finish("waiting_tools",&message,None)?;
                return Ok(Dispatch {
                    message: Some(message), report: String::new(),
                    entity_id: run.workflow.id.clone(), entity_name: run.workflow.name.clone(),
                    operation: None,
                    management: false,
                });
            }
            let report = message["content"].as_str().ok_or("stage report missing")?.to_string();
            stage_trace.finish("completed",&json!({"report":report}),None)?;
            run.steps += 1;
            state.portfolio.record_usage(&agent.id);
            state.portfolio.event(request, "workflow_stage", &run.workflow.id, "ok", duration_ms, None, &stage.id)?;
            // Stage quality is evaluated after completion, never by Foreman.
            // Defer definition mutations until the whole workflow ends so a loop
            // does not lose its own executor midway through the run.
            run.outputs.push(json!({"stage":stage.id,"agent":agent,"instruction":stage.instruction,"report":report}));
            run.pending.clear();
            run.pending_agent = None;
            let transition=run.workflow.transition(&stage, &report)?;
            state.portfolio.trace_event(request,"workflow.transition",&json!({"workflow_id":run.workflow.id,"stage":stage.id,"next":transition,"step":run.steps}))?;
            match transition {
                Some(next) => { run.stage = next; }
                None => {
                    for output in &run.outputs {
                        let agent = &output["agent"];
                        assess(state, request, "agent", agent["id"].as_str().unwrap(), agent,
                             &format!("{}\nAssigned intermediate stage instruction:\n{}",crate::brief::summarize_request(&run.original),output["instruction"].as_str().unwrap()), output["report"].as_str().unwrap(),
                             generation).await;
                    }
                    let reports = run.outputs.iter().map(|o| json!({"stage":o["stage"],"report":o["report"]})).collect::<Vec<_>>();
                    let report = json!(reports).to_string();
                    assess(state, request, "workflow", &run.workflow.id, &json!(run.workflow),
                         &crate::brief::summarize_request(&run.original), &report, generation).await;
                    state.portfolio.finish_run(&run.id)?;
                    state.portfolio.event(request, "workflow_run", &run.workflow.id, "ok", run.elapsed_ms, None, &run.id)?;
                    return Ok(Dispatch {
                        message: None, report, entity_id: run.workflow.id.clone(),
                        entity_name: run.workflow.name.clone(),
                        operation: None,
                        management: false,
                    });
                }
            }
        }
    }.await;
    match &result {
        Ok(output) => workflow_trace.finish(
            if output.message.is_some() {
                "waiting_tools"
            } else {
                "completed"
            },
            &json!({"report":output.report,"message":output.message}),
            None,
        )?,
        Err(error) => workflow_trace.finish("error", &json!({"error":error}), None)?,
    }
    if let Err(error) = &result {
        state.portfolio.finish_run(&run.id)?;
        state.portfolio.event(
            request,
            "workflow_run",
            &run.workflow.id,
            "error",
            run.elapsed_ms,
            None,
            error,
        )?;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_stage_subset_and_continuation_selection_survive_dispatch_boundaries() {
        let run = Run {
            id: "run".into(),
            workflow: Workflow {
                id: "workflow".into(),
                name: "workflow".into(),
                purpose: "test".into(),
                entry: "write".into(),
                max_steps: 2,
                stages: vec![],
            },
            stage: "write".into(),
            steps: 0,
            original: json!({"messages":[]}),
            outputs: vec![],
            pending: vec![],
            pending_agent: None,
            elapsed_ms: 0,
            agents: Default::default(),
            stage_tool_subsets: Some(BTreeMap::from([
                ("write".into(), vec!["ccm_file_write".into()]),
                ("review".into(), vec![]),
            ])),
        };
        let saved = serde_json::to_value(&run).unwrap();
        let run: Run = serde_json::from_value(saved).unwrap();
        assert_eq!(
            stage_tool_subset(&run, "write"),
            Some(vec!["ccm_file_write".into()])
        );
        assert_eq!(stage_tool_subset(&run, "review"), Some(vec![]));
        assert_eq!(stage_tool_subset(&run, "missing"), Some(vec![]));

        let expected_ids = vec!["pending-call-1".to_owned()];
        let mut events = Vec::new();
        // The matching selection is older than the former 100-selection window.
        for index in 0..125 {
            events.push((
                "tool.selection.persisted".into(),
                json!({
                    "agent_id":"agent",
                    "stage_id":null,
                    "selection_contract_version":1,
                    "tool_call_ids":[format!("later-call-{index}")],
                    "selection":{"version":1,"mode":"all_eligible"}
                })
                .to_string(),
            ));
        }
        events.push((
            "tool.selection.persisted".into(),
            json!({
                "agent_id":"agent",
                "stage_id":null,
                "selection_contract_version":1,
                "tool_call_ids":expected_ids.clone(),
                "selection":{"version":1,"mode":"subset","tools":["ccm_file_read"]}
            })
            .to_string(),
        ));
        events.push((
            "tool.selection".into(),
            json!({
                "agent_id":"agent",
                "stage_id":null,
                "selection_contract_version":1,
                "selection":{"version":1,"mode":"subset","tools":["ccm_file_read"]}
            })
            .to_string(),
        ));
        let selection = parse_continuation_selection(events, "agent", &expected_ids).unwrap();
        assert_eq!(
            selection,
            ContinuationToolSelection::Subset(vec!["ccm_file_read".into()])
        );

        let mut payload = json!({"messages":[],"__ccm_tool_subset":["client-injected"]});
        payload = clean_dispatch_payload(&payload);
        assert!(payload.get(TOOL_SUBSET_FIELD).is_none());
        apply_continuation_selection(&mut payload, selection);
        assert_eq!(payload[TOOL_SUBSET_FIELD], json!(["ccm_file_read"]));
        let eligible = vec!["ccm_file_read".to_owned(), "ccm_file_write".to_owned()];
        crate::runtime::tools::validate_tool_subset(Some(&["ccm_file_read".to_owned()]), &eligible)
            .unwrap();

        let explicit_omission = parse_continuation_selection(
            vec![(
                "tool.selection.persisted".into(),
                json!({
                    "agent_id":"agent",
                    "stage_id":null,
                    "selection_contract_version":1,
                    "tool_call_ids":["pending-call-1"],
                    "selection":{"version":1,"mode":"all_eligible"}
                })
                .to_string(),
            )],
            "agent",
            &["pending-call-1".into()],
        )
        .unwrap();
        assert_eq!(explicit_omission, ContinuationToolSelection::AllEligible);
        apply_continuation_selection(&mut payload, explicit_omission);
        assert!(payload.get(TOOL_SUBSET_FIELD).is_none());

        let explicit_empty = parse_continuation_selection(
            vec![(
                "tool.selection.persisted".into(),
                json!({
                    "agent_id":"agent",
                    "stage_id":null,
                    "selection_contract_version":1,
                    "tool_call_ids":["empty-call"],
                    "selection":{"version":1,"mode":"subset","tools":[]}
                })
                .to_string(),
            )],
            "agent",
            &["empty-call".into()],
        )
        .unwrap();
        assert_eq!(explicit_empty, ContinuationToolSelection::Subset(vec![]));
        apply_continuation_selection(&mut payload, explicit_empty);
        assert_eq!(payload[TOOL_SUBSET_FIELD], json!([]));

        assert_eq!(
            parse_continuation_selection(vec![], "agent", &["legacy-call".into()]).unwrap(),
            ContinuationToolSelection::LegacyAbsent
        );
        assert!(
            parse_continuation_selection(
                vec![(
                    "tool.selection".into(),
                    json!({
                        "agent_id":"agent",
                        "stage_id":null,
                        "selection_contract_version":1,
                        "selection":{"version":1,"mode":"subset","tools":[]}
                    })
                    .to_string(),
                )],
                "agent",
                &["missing-persisted-call".into()]
            )
            .is_err()
        );
    }

    #[test]
    fn continuation_rejects_mismatched_results() {
        let id = format!("ccm-agent:{}:call", uuid::Uuid::new_v4());
        let mut payload = json!({"messages":[{"role":"assistant","tool_calls":[{"id":id}]},{"role":"tool","tool_call_id":id,"content":"ok"}]});
        assert!(pending_results(&payload).unwrap().is_some());
        payload["messages"][1]["tool_call_id"] = json!("other");
        assert!(pending_results(&payload).is_err());
    }
}
